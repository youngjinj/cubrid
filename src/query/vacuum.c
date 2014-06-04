/*
 * Copyright (C) 2008 Search Solution Corporation. All rights reserved by Search Solution.
 *
 *   This program is free software; you can redistribute it and/or modify
 *   it under the terms of the GNU General Public License as published by
 *   the Free Software Foundation; either version 2 of the License, or
 *   (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program; if not, write to the Free Software
 *  Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA
 *
 */

/*
 * vacuum.c - Vacuuming system implementation.
 *
 */
#include "vacuum.h"
#include "thread.h"
#include "mvcc.h"
#include "page_buffer.h"
#include "heap_file.h"
#include "boot_sr.h"
#include "system_parameter.h"
#include "btree.h"
#include "log_compress.h"
#include "overflow_file.h"
#include "lock_free.h"
#include "dbtype.h"

/* The maximum number of slots in a page if all of them are empty.
 * IO_MAX_PAGE_SIZE is used for page size and any headers are ignored (it
 * wouldn't bring a significant difference).
 */
#define MAX_SLOTS_IN_PAGE (IO_MAX_PAGE_SIZE / sizeof (SPAGE_SLOT))

/* The maximum size of OID buffer where the records vacuumed from heap file
 * are collected to be removed from blockid b-trees.
 */
#define REMOVE_OIDS_BUFFER_SIZE (16 * IO_DEFAULT_PAGE_SIZE / OR_OID_SIZE)

/* The default number of cached entries in a vacuum statistics cache */
#define VACUUM_STATS_CACHE_SIZE 100



/* Get first log page identifier in a log block */
#define VACUUM_FIRST_LOG_PAGEID_IN_BLOCK(blockid) \
  ((blockid) * VACUUM_LOG_PAGES_PER_BLOCK)
/* Get last log page identifier in a log block */
#define VACUUM_LAST_LOG_PAGEID_IN_BLOCK(blockid) \
  (VACUUM_FIRST_LOG_PAGEID_IN_BLOCK (blockid + 1) - 1)
/* Get for a log page the identifier for the block to which it belongs. */
#define VACUUM_GET_LOG_BLOCKID_FOR_PAGE(pageid) \
  (pageid / VACUUM_LOG_PAGES_PER_BLOCK)

/*
 * Vacuum data section.
 * Vacuum data contains useful information for the vacuum process. There are
 * several fields, among which a table of entries which describe the progress
 * of processing log data for vacuum.
 * This information is kept on disk too and requires redo logging.
 */

/* Macro's for locking/unlocking vacuum data */
#define VACUUM_LOCK_DATA() \
  pthread_mutex_lock (&vacuum_Data_mutex)
#define VACUUM_UNLOCK_DATA() \
  pthread_mutex_unlock (&vacuum_Data_mutex)

/* Vacuum log block data.
 *
 * Stores information on a block of log data relevant for vacuum.c
 */
typedef struct vacuum_data_entry VACUUM_DATA_ENTRY;
struct vacuum_data_entry
{
  VACUUM_LOG_BLOCKID blockid;
  LOG_LSA start_lsa;
  MVCCID oldest_mvccid;
  MVCCID newest_mvccid;
};

/* One flag is required for entries currently being vacuumed. In order to
 * avoid using an extra-field and because blockid will not use all its 64 bits
 * first bit will be used for this flag.
 */
/* Bits used for flag */
#define VACUUM_DATA_ENTRY_FLAG_MASK	  0x8000000000000000
/* Bits used for blockid */
#define VACUUM_DATA_ENTRY_BLOCKID_MASK	  0x3FFFFFFFFFFFFFFF

/* Flags */
/* The represented block is being vacuumed */
#define VACUUM_BLOCK_STATUS_MASK	       0xC000000000000000
#define VACUUM_BLOCK_STATUS_VACUUMED	       0xC000000000000000
#define VACUUM_BLOCK_STATUS_REQUESTED_VACUUM   0x4000000000000000
#define VACUUM_BLOCK_STATUS_RUNNING_VACUUM     0x8000000000000000
#define VACUUM_BLOCK_STATUS_AVAILABLE	       0x0000000000000000

/* Vacuum data.
 *
 * Stores data required for vacuum. It is also stored on disk in the first
 * database volume.
 */
typedef struct vacuum_data VACUUM_DATA;
struct vacuum_data
{
  LOG_LSA crt_lsa;		/* Data is stored on disk and requires
				 * logging, therefore it also requires a log
				 * lsa.
				 */

  VACUUM_LOG_BLOCKID last_blockid;	/* Block id for last vacuum data entry...
					 * This entry is actually the id of last
					 * added block which may not even in
					 * vacuum data (being already vacuumed).
					 */
  MVCCID oldest_mvccid;		/* Oldest MVCCID found in current vacuum
				 * data.
				 */
  MVCCID newest_mvccid;		/* Newest MVCCID found in current vacuum
				 * data.
				 */

  int n_table_entries;		/* Number of entries in log block table */

  /* NOTE: Leave the table at the end of vacuum data structure */
  VACUUM_DATA_ENTRY vacuum_data_table[1];	/* Array containing log block
						 * entries and skipped blocks
						 * entries. This is a dynamic array,
						 * new blocks will be appended at
						 * the end, while vacuumed blocks
						 * will be removed when finished.
						 */
};

/* Pointer to vacuum data */
VACUUM_DATA *vacuum_Data = NULL;

/* VPID of the first vacuum data page */
VPID vacuum_Data_vpid;

/* The maximum allowed size for vacuum data */
int vacuum_Data_max_size;
/* Mutex used to synchronize vacuum data for auto-vacuum master and vacuum
 * workers.
 */
pthread_mutex_t vacuum_Data_mutex;

/* Size of vacuum data header (all data that precedes log block data table) */
#define VACUUM_DATA_HEADER_SIZE \
  ((int) offsetof (VACUUM_DATA, vacuum_data_table))
/* The maximum allowed size for the log block table */
#define VACUUM_DATA_TABLE_MAX_SIZE	\
  ((vacuum_Data_max_size - VACUUM_DATA_HEADER_SIZE) \
   / ((int) sizeof (VACUUM_DATA_ENTRY)))

/* Get vacuum data entry table at the given index */
#if 0
#define VACUUM_DATA_GET_ENTRY(block_index) \
  (&vacuum_Data->vacuum_data_table[block_index])
#else
static VACUUM_DATA_ENTRY *
vacuum_data_get_entry (int block_index)
{
  if (block_index < 0)
    {
      assert_release (false);
    }
  if (block_index >= VACUUM_DATA_TABLE_MAX_SIZE)
    {
      assert_release (false);
    }
  return &vacuum_Data->vacuum_data_table[block_index];
}

#define VACUUM_DATA_GET_ENTRY(block_index) \
  vacuum_data_get_entry (block_index)
#endif

/* Access fields in a vacuum data table entry */
/* Get blockid (use mask to cancel flag bits) */
#define VACUUM_DATA_ENTRY_BLOCKID(entry) \
  (((entry)->blockid) & VACUUM_DATA_ENTRY_BLOCKID_MASK)
/* Get start vacuum lsa */
#define VACUUM_DATA_ENTRY_START_LSA(entry) \
  ((entry)->start_lsa)
#define VACUUM_DATA_ENTRY_OLDEST_MVCCID(entry) \
  ((entry)->oldest_mvccid)
#define VACUUM_DATA_ENTRY_NEWEST_MVCCID(entry) \
  ((entry)->newest_mvccid)

/* Get data entry flags */
#define VACUUM_DATA_ENTRY_FLAG(entry) \
  (((entry)->blockid) & VACUUM_DATA_ENTRY_FLAG_MASK)

/* Vacuum block status: requested means that vacuum data has assigned it as
 * a job, but no worker started it yet; running means that a work is currently
 * vacuuming based on this entry's block.
 */
/* Get vacuum block status */
#define VACUUM_BLOCK_STATUS(entry) \
  (((entry)->blockid) & VACUUM_BLOCK_STATUS_MASK)

/* Check vacuum block status */
#define VACUUM_BLOCK_STATUS_IS_VACUUMED(entry) \
  (VACUUM_BLOCK_STATUS (entry) == VACUUM_BLOCK_STATUS_VACUUMED)
#define VACUUM_BLOCK_STATUS_IS_RUNNING(entry) \
  (VACUUM_BLOCK_STATUS (entry) == VACUUM_BLOCK_STATUS_RUNNING_VACUUM)
#define VACUUM_BLOCK_STATUS_IS_REQUESTED(entry) \
  (VACUUM_BLOCK_STATUS (entry) == VACUUM_BLOCK_STATUS_REQUESTED_VACUUM)
#define VACUUM_BLOCK_STATUS_IS_AVAILABLE(entry) \
  (VACUUM_BLOCK_STATUS (entry) == VACUUM_BLOCK_STATUS_AVAILABLE)

/* Set vacuum block status */
#define VACUUM_BLOCK_STATUS_SET_VACUUMED(entry) \
  ((entry)->blockid = \
  ((entry)->blockid & ~VACUUM_BLOCK_STATUS_MASK) \
  | VACUUM_BLOCK_STATUS_VACUUMED)
#define VACUUM_BLOCK_STATUS_SET_RUNNING(entry) \
  ((entry)->blockid = \
   ((entry)->blockid & ~VACUUM_BLOCK_STATUS_MASK) \
   | VACUUM_BLOCK_STATUS_RUNNING_VACUUM)
#define VACUUM_BLOCK_STATUS_SET_REQUESTED(entry) \
  ((entry)->blockid = \
  ((entry)->blockid & ~VACUUM_BLOCK_STATUS_MASK) \
  | VACUUM_BLOCK_STATUS_REQUESTED_VACUUM)
#define VACUUM_BLOCK_STATUS_SET_AVAILABLE(entry) \
  ((entry)->blockid = \
  ((entry)->blockid & ~VACUUM_BLOCK_STATUS_MASK) \
  | VACUUM_BLOCK_STATUS_AVAILABLE)

/* A block entry in vacuum data can be vacuumed if:
 * 1. block is marked as available (is not assigned to any thread and no
 *    worker is currently processing the block).
 * 2. the newest block MVCCID is older than the oldest active MVCCID (meaning
 *    that all changes logged in block are now "all visible").
 * 3. for safety: make sure that there is at least a page between start_lsa
 *    and the page currently used for logging. It is possible that the log
 *    entry at start_lsa is "spilled" on the next page. Fetching the currently
 *    logged page requires an exclusive log lock which is prohibited for
 *    vacuum workers.
 */
#define VACUUM_LOG_BLOCK_CAN_VACUUM(entry, mvccid) \
  (VACUUM_BLOCK_STATUS_IS_AVAILABLE (entry) \
   && mvcc_id_precedes (VACUUM_DATA_ENTRY_NEWEST_MVCCID (entry), mvccid) \
   && entry->start_lsa.pageid + 1 < log_Gl.append.prev_lsa.pageid)

/*
 * Vacuum oldest not flushed lsa section.
 * Server must keep track of the oldest change on vacuum data that is not
 * yet flushed to disk. Used for log checkpoint to give vacuum data logging
 * a behavior similar to logging database page changes.
 */
LOG_LSA vacuum_Data_oldest_not_flushed_lsa;

/* Macro called after logging any changes on vacuum data. Updates oldest
 * lsa for vacuum data that is not flushed to disk.
 */
#define VACUUM_UPDATE_OLDEST_NOT_FLUSHED_LSA() \
  do									      \
    {									      \
      if (LSA_ISNULL (&vacuum_Data_oldest_not_flushed_lsa))		      \
	{								      \
	  LSA_COPY (&vacuum_Data_oldest_not_flushed_lsa, &vacuum_Data->crt_lsa); \
	}								      \
    }									      \
  while (0)

/* A lock-free buffer used for communication between logger transactions and
 * auto-vacuum master. It is advisable to avoid synchronizing running
 * transactions with vacuum threads and for this reason the block data is not
 * added directly to vacuum data.
 */
LOCK_FREE_CIRCULAR_QUEUE *vacuum_Block_data_buffer = NULL;
#define VACUUM_BLOCK_DATA_BUFFER_CAPACITY 1024

/*
 * Dropped classes/indexes section.
 */

typedef enum
{
  DROPPED_CLASS,
  DROPPED_INDEX
} DROPPED_TYPE;

#define VACUUM_DROPPED_ENTRIES_EXTEND_FILE_NPAGES 5

/* Identifiers for dropped classes/indexes files */
static VFID vacuum_Dropped_classes_vfid;
static VFID vacuum_Dropped_indexes_vfid;
#define VACUUM_DROPPED_ENTRIES_VFID_PTR(dropped_type) \
  ((dropped_type) == DROPPED_CLASS ? \
  &vacuum_Dropped_classes_vfid : &vacuum_Dropped_indexes_vfid)

/* Identifiers for first pages in dropped classes/indexes files */
static VPID vacuum_Dropped_classes_vpid;
static VPID vacuum_Dropped_indexes_vpid;
#define VACUUM_DROPPED_ENTRIES_VPID_PTR(dropped_type) \
  ((dropped_type) == DROPPED_CLASS ? \
   &vacuum_Dropped_classes_vpid : &vacuum_Dropped_indexes_vpid)

/* Total count of dropped classes/indexes */
static INT32 vacuum_Dropped_classes_count;
static INT32 vacuum_Dropped_indexes_count;
#define VACUUM_DROPPED_ENTRIES_COUNT_PTR(dropped_type) \
  ((dropped_type) == DROPPED_CLASS ? \
   &vacuum_Dropped_classes_count : &vacuum_Dropped_indexes_count)

/* Temporary buffer used to move memory data from the first page of dropped
 * classes/indexes. An exclusive latch on this first page is required to
 * synchronize buffer usage.
 * For other pages, memmove should be used.
 */
static char vacuum_Dropped_classes_tmp_buffer[IO_MAX_PAGE_SIZE];
static char vacuum_Dropped_indexes_tmp_buffer[IO_MAX_PAGE_SIZE];
#define VACUUM_DROPPED_ENTRIES_TMP_BUFFER(dropped_type) \
  ((dropped_type) == DROPPED_CLASS ? \
   vacuum_Dropped_classes_tmp_buffer : vacuum_Dropped_indexes_tmp_buffer)

/* Union of class and index identifiers */
typedef union vacuum_dropped_cls_btid VACUUM_DROPPED_CLS_BTID;
union vacuum_dropped_cls_btid
{
  OID class_oid;
  BTID btid;
};

/* Dropped class/index entry */
typedef struct vacuum_dropped_entry VACUUM_DROPPED_ENTRY;
struct vacuum_dropped_entry
{
  VACUUM_DROPPED_CLS_BTID dropped_cls_btid;
  MVCCID mvccid;
};

/* A page of dropped classes/indexes entries */
typedef struct vacuum_dropped_entries_page VACUUM_DROPPED_ENTRIES_PAGE;
struct vacuum_dropped_entries_page
{
  VPID next_page;		/* VPID of next dropped classes
				 * or indexes page.
				 */
  INT16 n_dropped_entries;	/* Number of entries on page */

  /* Leave the dropped entries at the end of the structure */
  VACUUM_DROPPED_ENTRY dropped_entries[1];	/* Dropped classes or indexes. It
						 * has a variable size and can
						 * fill up to a maximum of
						 * database page.
						 */
};

/* Set a dropped class or index value */
#define VACUUM_DROPPED_CLS_BTID_SET_VALUE(dropped_cls_btid, val, type) \
  do \
    { \
      if ((type) == DROPPED_CLASS) \
	{ \
	  COPY_OID (&((dropped_cls_btid)->class_oid), (OID *) (val)); \
	} \
      else \
	{ \
	  BTID_COPY (&((dropped_cls_btid)->btid), (BTID *) (val)); \
	} \
    } while (0)

/* Size of dropped class/index page header */
#define VACUUM_DROPPED_ENTRIES_PAGE_HEADER_SIZE \
  (offsetof (VACUUM_DROPPED_ENTRIES_PAGE, dropped_entries))

/* Capacity of dropped class/index page */
#define VACUUM_DROPPED_ENTRIES_PAGE_CAPACITY \
  ((INT16) ((DB_PAGESIZE - VACUUM_DROPPED_ENTRIES_PAGE_HEADER_SIZE) \
	    / sizeof (VACUUM_DROPPED_ENTRY)))
/* Maximum possible capacity of dropped class/index page */
#define VACUUM_DROPPED_ENTRIES_PAGE_MAX_CAPACITY \
  ((IO_MAX_PAGE_SIZE - VACUUM_DROPPED_ENTRIES_PAGE_HEADER_SIZE) \
  / sizeof (VACUUM_DROPPED_ENTRY))

/* Overwritten versions of pgbuf_fix, pgbuf_unfix and pgbuf_set_dirty, 
 * adapted for the needs of vacuum and its dropped classes/indexes pages.
 */
#define vacuum_fix_dropped_entries_page(thread_p, vpidp, latch) \
  ((VACUUM_DROPPED_ENTRIES_PAGE *) pgbuf_fix (thread_p, vpidp, OLD_PAGE, \
					      latch, \
					      PGBUF_UNCONDITIONAL_LATCH))
#define vacuum_unfix_dropped_entries_page(thread_p, dropped_page) \
  do \
    { \
      pgbuf_unfix (thread_p, (PAGE_PTR) (dropped_page)); \
      (dropped_page) = NULL; \
    } while (0)
#define vacuum_set_dirty_dropped_entries_page(thread_p, dropped_page, free) \
  do \
    { \
      pgbuf_set_dirty (thread_p, (PAGE_PTR) (dropped_page), free); \
      if ((free) == FREE) \
	{ \
	  (dropped_page) = NULL; \
	} \
    } while (0)

#if !defined (NDEBUG)
/* Track pages allocated for dropped classes and indexes. Used for debugging
 * only, to observe easily the lists of dropped classes and indexes at any
 * time.
 */
typedef struct vacuum_track_dropped_entries VACUUM_TRACK_DROPPED_ENTRIES;
struct vacuum_track_dropped_entries
{
  VACUUM_TRACK_DROPPED_ENTRIES *next_tracked_page;
  VACUUM_DROPPED_ENTRIES_PAGE dropped_data_page;
};
VACUUM_TRACK_DROPPED_ENTRIES *vacuum_Track_dropped_classes;
VACUUM_TRACK_DROPPED_ENTRIES *vacuum_Track_dropped_indexes;
#define VACUUM_DROPPED_CLS_BTID_TRACK_PTR(dropped_type) \
  ((dropped_type) == DROPPED_CLASS ? \
   vacuum_Track_dropped_classes : vacuum_Track_dropped_indexes)
#define VACUUM_TRACK_DROPPED_ENTRIES_SIZE \
  (DB_PAGESIZE + sizeof (VACUUM_TRACK_DROPPED_ENTRIES *))
#endif

typedef struct vacuum_drop_data_buffer VACUUM_DROP_DATA_BUFFER;
struct vacuum_drop_data_buffer
{
  VACUUM_DROPPED_ENTRY *dropped_entries;
  int n_dropped_entries;
  int capacity;
  pthread_mutex_t mutex;
};

VACUUM_DROP_DATA_BUFFER *vacuum_Dropped_class_buffer = NULL;
VACUUM_DROP_DATA_BUFFER *vacuum_Dropped_index_buffer = NULL;
#define VACUUM_DROPPED_DATA_BUFFER_CAPACITY 1024

static void vacuum_log_vacuum_oids_from_heap_page (THREAD_ENTRY * thread_p,
						   PAGE_PTR page_p,
						   HFID * hfid,
						   PGSLOTID * slots,
						   int n_slots);

static int vacuum_oids_from_heap (THREAD_ENTRY * thread_p, OID * oids,
				  int n_oids, MVCCID oldest_mvccid);
static int vacuum_oids_from_heap_page (THREAD_ENTRY * thread_p, OID * oids,
				       int n_oids,
				       MVCCID oldest_active_mvccid);
static void vacuum_oids_from_heap_page_log_and_reset (THREAD_ENTRY * thread_p,
						      PAGE_PTR * page_p,
						      HFID * hfid,
						      PGSLOTID *
						      vacuumed_slots,
						      int *n_vacuumed_slots);

static void vacuum_process_vacuum_data (THREAD_ENTRY * thread_p);
static int vacuum_process_log_block (THREAD_ENTRY * thread_p,
				     VACUUM_DATA_ENTRY * block_data);
static void vacuum_finished_block_vacuum (THREAD_ENTRY * thread_p,
					  VACUUM_DATA_ENTRY * block_data,
					  bool is_vacuum_complete);

static int vacuum_get_mvcc_delete_undo_data (THREAD_ENTRY * thread_p,
					     LOG_LSA * log_lsa_p,
					     LOG_PAGE * log_page_p,
					     struct log_data *log_record_data,
					     MVCCID * mvccid,
					     LOG_ZIP * log_unzip_ptr,
					     char **undo_data_buffer,
					     int *undo_data_buffer_size,
					     char **undo_data_ptr,
					     int *undo_data_size);
static int vacuum_load_data_from_disk (THREAD_ENTRY * thread_p,
				       VFID * vacuum_data_vfid);
static int vacuum_load_dropped_classes_indexes_from_disk (THREAD_ENTRY *
							  thread_p,
							  VFID * vfid,
							  DROPPED_TYPE type);
static int vacuum_compare_data_entries (const void *ptr1, const void *ptr2);
static VACUUM_DATA_ENTRY *vacuum_get_vacuum_data_entry (VACUUM_LOG_BLOCKID
							blockid);
static bool vacuum_is_work_in_progress (THREAD_ENTRY * thread_p);
static void vacuum_data_remove_finished_entries (THREAD_ENTRY * thread_p);
static void vacuum_data_remove_entries (THREAD_ENTRY * thread_p,
					int n_removed_entries,
					int * removed_entries);

static void vacuum_log_remove_data_entries (THREAD_ENTRY * thread_p,
					    int * removed_indexes,
					    int n_removed_indexes);
static void vacuum_log_append_block_data (THREAD_ENTRY * thread_p,
					  VACUUM_DATA_ENTRY * new_entries,
					  int n_new_entries);
static void vacuum_log_update_block_data (THREAD_ENTRY * thread_p,
					  VACUUM_DATA_ENTRY * block_datad);

static int vacuum_compare_dropped_cls_btid (const void *a, const void *b,
					    DROPPED_TYPE type);
static int vacuum_add_dropped_cls_btid (THREAD_ENTRY * thread_p,
					void *cls_btid, MVCCID mvccid,
					DROPPED_TYPE type);
static int vacuum_cleanup_dropped_cls_btid (THREAD_ENTRY * thread_p,
					    DROPPED_TYPE type);
static bool vacuum_find_dropped_cls_btid (THREAD_ENTRY * thread_p,
					  void *cls_btid, MVCCID mvccid,
					  DROPPED_TYPE type);
static void vacuum_log_add_dropped_cls_btid (THREAD_ENTRY * thread_p,
					     PAGE_PTR page_p,
					     VACUUM_DROPPED_ENTRY *
					     dropped_cls_btid,
					     INT16 position,
					     DROPPED_TYPE type,
					     bool is_duplicate);
static void vacuum_log_cleanup_dropped_cls_btid (THREAD_ENTRY * thread_p,
						 PAGE_PTR page_p,
						 INT16 * indexes,
						 INT16 n_indexes,
						 DROPPED_TYPE type);
static void vacuum_log_set_next_page_dropped_cls_btid (THREAD_ENTRY *
						       thread_p,
						       PAGE_PTR page_p,
						       VPID * next_page,
						       DROPPED_TYPE type);

static void vacuum_update_oldest_mvccid (THREAD_ENTRY * thread_p);
static void vacuum_consume_dropped_index_buffer_log_blocks (THREAD_ENTRY *
							    thread_p);
static void vacuum_consume_dropped_class_buffer_log_blocks (THREAD_ENTRY *
							    thread_p);
static int vacuum_is_dropped_cls_btid_in_buffer (THREAD_ENTRY * thread_p,
						 void *cls_btid,
						 MVCCID mvccid,
						 DROPPED_TYPE type);

/*
 * xvacuum () - Server function for vacuuming classes
 *
 * return	    : Error code.
 * thread_p (in)    : Thread entry.
 * num_classes (in) : Number of classes in class_oids array.
 * class_oids (in)  : Class OID array.
 */
int
xvacuum (THREAD_ENTRY * thread_p, int num_classes, OID * class_oids)
{
  /* TODO: Vacuum statement */
  return NO_ERROR;
}

/*
 * vacuum_initialize () - Initialize necessary structures for vacuum and
 *			  auto-vacuum.
 *
 * return	 : Void.
 * thread_p (in) : Thread entry.
 */
int
vacuum_initialize (THREAD_ENTRY * thread_p)
{
  int error_code = NO_ERROR;

  if (prm_get_bool_value (PRM_ID_DISABLE_VACUUM))
    {
      return NO_ERROR;
    }

  /* Initialize mutex for vacuum data */
  (void) pthread_mutex_init (&vacuum_Data_mutex, NULL);

  vacuum_Block_data_buffer =
    lock_free_circular_queue_create (VACUUM_BLOCK_DATA_BUFFER_CAPACITY,
				     sizeof (VACUUM_DATA_ENTRY),
				     false, false);
  if (vacuum_Block_data_buffer == NULL)
    {
      goto error;
    }

  vacuum_Dropped_class_buffer = (VACUUM_DROP_DATA_BUFFER *)
    malloc (sizeof (VACUUM_DROP_DATA_BUFFER));
  if (vacuum_Dropped_class_buffer == NULL)
    {
      goto error;
    }
  vacuum_Dropped_class_buffer->n_dropped_entries = 0;
  pthread_mutex_init (&vacuum_Dropped_class_buffer->mutex, NULL);
  vacuum_Dropped_class_buffer->dropped_entries =
    (VACUUM_DROPPED_ENTRY *) malloc (VACUUM_DROPPED_DATA_BUFFER_CAPACITY
				     * sizeof (VACUUM_DROPPED_ENTRY));
  if (vacuum_Dropped_class_buffer->dropped_entries == NULL)
    {
      goto error;
    }
  vacuum_Dropped_class_buffer->capacity = VACUUM_DROPPED_DATA_BUFFER_CAPACITY;

  vacuum_Dropped_index_buffer = (VACUUM_DROP_DATA_BUFFER *)
    malloc (sizeof (VACUUM_DROP_DATA_BUFFER));
  if (vacuum_Dropped_index_buffer == NULL)
    {
      goto error;
    }
  vacuum_Dropped_index_buffer->n_dropped_entries = 0;
  pthread_mutex_init (&vacuum_Dropped_index_buffer->mutex, NULL);
  vacuum_Dropped_index_buffer->dropped_entries =
    (VACUUM_DROPPED_ENTRY *) malloc (VACUUM_DROPPED_DATA_BUFFER_CAPACITY
				     * sizeof (VACUUM_DROPPED_ENTRY));
  if (vacuum_Dropped_index_buffer->dropped_entries == NULL)
    {
      goto error;
    }
  vacuum_Dropped_index_buffer->capacity = VACUUM_DROPPED_DATA_BUFFER_CAPACITY;

  return NO_ERROR;

error:
  vacuum_finalize (thread_p);
  return (error_code == NO_ERROR) ? ER_FAILED : error_code;
}

/*
* vacuum_finalize () - Finalize necessary structures for vacuum and
*		       auto-vacuum.
*
* return	: Void.
* thread_p (in) : Thread entry.
 */
void
vacuum_finalize (THREAD_ENTRY * thread_p)
{
  if (prm_get_bool_value (PRM_ID_DISABLE_VACUUM))
    {
      return;
    }
  VACUUM_LOCK_DATA ();

#if defined(SERVER_MODE)
  while (vacuum_is_work_in_progress (thread_p))
    {
      /* Must wait for vacuum workers to finish */
      VACUUM_UNLOCK_DATA ();
      thread_sleep (0);
      VACUUM_LOCK_DATA ();
    }
#endif

  if (vacuum_Data != NULL)
    {
      /* Flush vacuum data to disk */
      (void) vacuum_flush_data (thread_p, NULL, NULL, NULL, true);

      if (!LOCK_FREE_CIRCULAR_QUEUE_IS_EMPTY (vacuum_Block_data_buffer))
	{
	  /* Block data is lost, this should never happen */
	  /* TODO: Make sure vacuum data is big enough to handle all buffered
	   *       blocks.
	   */
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_GENERIC_ERROR, 0);
	  assert (0);
	}

      /* Free vacuum data */
      if (vacuum_Data != NULL)
	{
	  free_and_init (vacuum_Data);
	}
    }

  /* Destroy log blocks data buffer */
  if (vacuum_Block_data_buffer != NULL)
    {
      lock_free_circular_queue_destroy (vacuum_Block_data_buffer);
      vacuum_Block_data_buffer = NULL;
    }

  /* Destroy the dropped classes buffer */
  if (vacuum_Dropped_class_buffer != NULL)
    {
      if (vacuum_Dropped_class_buffer->dropped_entries != NULL)
	{
	  free_and_init (vacuum_Dropped_class_buffer->dropped_entries);
	}
      pthread_mutex_destroy (&vacuum_Dropped_class_buffer->mutex);
      free_and_init (vacuum_Dropped_class_buffer);
    }

  /* Destroy the dropped indexes buffer */
  if (vacuum_Dropped_index_buffer != NULL)
    {
      if (vacuum_Dropped_index_buffer->dropped_entries != NULL)
	{
	  free_and_init (vacuum_Dropped_index_buffer->dropped_entries);
	}
      pthread_mutex_destroy (&vacuum_Dropped_index_buffer->mutex);
      free_and_init (vacuum_Dropped_index_buffer);
    }

  /* Unlock data */
  VACUUM_UNLOCK_DATA ();
}

/*
 * vacuum_oids_from_heap () - Vacuum all objects in the array from heap files.
 *
 * return	      : Error code.
 * thread_p (in)      : Thread entry.
 * oids (in)	      : Array of OID's to be vacuumed.
 * n_oids (in)	      : Number of objects in the array.
 * oldest_mvccid (in) : Threshold MVCCID used for the vacuum test.
 */
static int
vacuum_oids_from_heap (THREAD_ENTRY * thread_p, OID * oids, int n_oids,
		       MVCCID oldest_mvccid)
{
  int i, start_index, error_code = NO_ERROR;

  /* Sort OID's to have OID's belonging to the same page on successive
   * positions.
   */
  qsort (oids, n_oids, sizeof (OID), oid_compare);

  /* We will always save start_index the first OID in a new page. When next
   * OID is on a different page, vacuum current batch of OID's (which should
   * all be on the same page.
   */
  for (i = 0, start_index = 0; i < n_oids; i++)
    {
      if (			/* The end of the OID array is reached, vacuum last batch */
	   i == n_oids - 1
	   /* Next OID is on a different page, vacuum current batch */
	   || !VPID_EQ_FOR_OIDS (&oids[i], &oids[i + 1]))
	{
	  error_code =
	    vacuum_oids_from_heap_page (thread_p, &oids[start_index],
					i - start_index + 1, oldest_mvccid);
	  if (error_code != NO_ERROR)
	    {
	      return error_code;
	    }
	  start_index = i + 1;
	}
      else
	{
	  /* Next OID is on the same page, continue "collecting" OID's */
	}
    }
  return NO_ERROR;
}

/*
 * vacuum_oids_from_heap_page () - Vacuum the given OID's from current page.
 *				   It is supposed that all OID's belong to
 *				   this page.
 *
 * return		     : Error code.
 * thread_p (in)	     : Thread entry.
 * oids (in)		     : Array of OID's to vacuum.
 * n_oids (in)		     : The number of OID's.
 * oldest_active_mvccid (in) : Oldest active MVCCID used to check record for
 *			       vacuum.
 */
static int
vacuum_oids_from_heap_page (THREAD_ENTRY * thread_p, OID * oids, int n_oids,
			    MVCCID oldest_active_mvccid)
{
  int i, error_code = NO_ERROR;
  RECDES recdes, forward_recdes, temp_recdes;
  OID forward_oid;
  PAGE_PTR forward_page = NULL, page = NULL;
  SPAGE_SLOT *slot_p = NULL;
  VPID page_vpid, forward_vpid;
  MVCC_REC_HEADER mvcc_rec_header;
  OID class_oid;
  HFID hfid;
  PGSLOTID vacuumed_slots[MAX_SLOTS_IN_PAGE];
  int n_vacuumed_slots = 0;
  int lock_ret;
  int old_header_size, new_header_size;
  MVCC_SATISFIES_VACUUM_RESULT result;
  DISK_ISVALID valid;
  char buffer[IO_MAX_PAGE_SIZE];
  bool is_bigone = false;
  bool is_class_locked = false;

  if (oids == NULL || n_oids <= 0)
    {
      /* nothing to do */
      return NO_ERROR;
    }

  /* Get page VPID and fix page */
  VPID_GET_FROM_OID (&page_vpid, &oids[0]);
  valid = disk_isvalid_page (thread_p, page_vpid.volid, page_vpid.pageid);
  if (valid != DISK_VALID)
    {
      /* Page is no longer valid. This can happen if the heap file was
       * destroyed.
       */
      assert (valid == DISK_INVALID);
      vacuum_er_log (VACUUM_ER_LOG_HEAP | VACUUM_ER_LOG_WARNING
		     | VACUUM_ER_LOG_WORKER,
		     "VACUUM WARNING: thread(%d): "
		     "page(%d, %d) could not be found\n",
		     thread_get_current_entry_index (),
		     page_vpid.pageid, page_vpid.volid);
      return NO_ERROR;
    }
  page =
    pgbuf_fix (thread_p, &page_vpid, OLD_PAGE, PGBUF_LATCH_WRITE,
	       PGBUF_UNCONDITIONAL_LATCH);
  if (page == NULL)
    {
      /* Page fix failed */
      valid = disk_isvalid_page (thread_p, page_vpid.volid, page_vpid.pageid);
      if (valid != DISK_VALID)
	{
	  /* Page is no longer valid. This can happen if the heap file was
	   * destroyed.
	   */
	  assert (valid == DISK_INVALID);
	  vacuum_er_log (VACUUM_ER_LOG_HEAP | VACUUM_ER_LOG_WARNING
			 | VACUUM_ER_LOG_WORKER,
			 "VACUUM WARNING: thread(%d): "
			 "page(%d, %d) could not be found\n",
			 thread_get_current_entry_index (),
			 page_vpid.pageid, page_vpid.volid);
	  return NO_ERROR;
	}
      assert (false);
      return ER_FAILED;
    }

  if (!pgbuf_check_page_type_no_error (thread_p, page, PAGE_HEAP))
    {
      goto end;
    }

  /* Get class OID */
  if (heap_get_class_oid_from_page (thread_p, page, &class_oid) != NO_ERROR)
    {
      error_code = ER_FAILED;
      goto end;
    }
  /* Lock class */
  lock_ret =
    lock_object (thread_p, &class_oid, oid_Root_class_oid, SCH_S_LOCK,
		 LK_COND_LOCK);
  if (lock_ret != LK_GRANTED)
    {
      /* Usually page is fixed after locking class object. In order to avoid
       * a dead lock, unfix page now, lock class object and then retry page
       * fix.
       */
      pgbuf_unfix_and_init (thread_p, page);
      lock_ret =
	lock_object (thread_p, &class_oid, oid_Root_class_oid, SCH_S_LOCK,
		     LK_UNCOND_LOCK);
      if (lock_ret != LK_GRANTED)
	{
	  /* Lock was not granted */
	  error_code = ER_FAILED;
	  goto end;
	}
      is_class_locked = true;
      valid = disk_isvalid_page (thread_p, page_vpid.volid, page_vpid.pageid);
      if (valid != DISK_VALID)
	{
	  assert (valid == DISK_INVALID);
	  vacuum_er_log (VACUUM_ER_LOG_HEAP | VACUUM_ER_LOG_WARNING
			 | VACUUM_ER_LOG_WORKER,
			 "VACUUM WARNING: thread(%d): "
			 "page(%d, %d) could not be found\n",
			 thread_get_current_entry_index (),
			 page_vpid.pageid, page_vpid.volid);
	  return NO_ERROR;
	}
      page =
	pgbuf_fix (thread_p, &page_vpid, OLD_PAGE, PGBUF_LATCH_WRITE,
		   PGBUF_UNCONDITIONAL_LATCH);
      if (page == NULL)
	{
	  /* Page fix failed */
	  error_code = ER_FAILED;
	  assert (false);
	  goto end;
	}
    }
  is_class_locked = true;
  /* Get HFID (required for logging) */
  if (heap_get_hfid_from_class_oid (thread_p, &class_oid, &hfid) != NO_ERROR)
    {
      /* Class was probably deleted. */
      /* TODO: Is there a way to know this? */
      goto end;
    }
  for (i = 0; i < n_oids; i++)
    {
      /* Safety check */
      assert (page_vpid.pageid == oids[i].pageid
	      && page_vpid.volid == oids[i].volid);
      if (page == NULL)
	{
	  page =
	    pgbuf_fix (thread_p, &page_vpid, OLD_PAGE, PGBUF_LATCH_WRITE,
		       PGBUF_UNCONDITIONAL_LATCH);
	}
      slot_p = spage_get_slot (page, oids[i].slotid);
      if (slot_p == NULL)
	{
	  /* Couldn't find object slot. Probably the page was reused until
	   * vacuum reached to clean it and the old object is no longer here.
	   * Proceed to next object.
	   */
	  continue;
	}
      switch (slot_p->record_type)
	{
	case REC_MARKDELETED:
	case REC_DELETED_WILL_REUSE:
	case REC_MVCC_NEXT_VERSION:
	  /* Must be already vacuumed */
	  break;

	case REC_ASSIGN_ADDRESS:
	  /* Cannot vacuum */
	  break;

	case REC_RELOCATION:
	  /* Must also vacuum REC_NEWHOME */

	  /* Get forward page */
	  forward_recdes.area_size = OR_OID_SIZE;
	  forward_recdes.length = OR_OID_SIZE;
	  forward_recdes.data = (char *) &forward_oid;

	  if (spage_get_record (page, oids[i].slotid, &forward_recdes, COPY)
	      != S_SUCCESS)
	    {
	      /* TODO: add vacuum error log */
	      er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE,
		      ER_HEAP_BAD_OBJECT_TYPE, 3, oids[i].volid,
		      oids[i].pageid, oids[i].slotid);
	      break;
	    }

	  VPID_GET_FROM_OID (&forward_vpid, &forward_oid);
	  assert (forward_page == NULL);
	  forward_page =
	    pgbuf_fix (thread_p, &forward_vpid, OLD_PAGE, PGBUF_LATCH_WRITE,
		       PGBUF_CONDITIONAL_LATCH);
	  if (forward_page == NULL)
	    {
	      /* Couldn't obtain conditional latch, free original page and
	       * retry.
	       */
	      vacuum_oids_from_heap_page_log_and_reset (thread_p, &page,
							&hfid,
							vacuumed_slots,
							&n_vacuumed_slots);
	      forward_page =
		pgbuf_fix (thread_p, &forward_vpid, OLD_PAGE,
			   PGBUF_LATCH_WRITE, PGBUF_UNCONDITIONAL_LATCH);
	      if (forward_page == NULL)
		{
		  /* TODO: Add vacuum error log */
		  break;
		}
	    }
	  if (spage_get_record (forward_page, forward_oid.slotid, &recdes,
				PEEK) != S_SUCCESS)
	    {
	      /* TODO: Add vacuum error log */
	      break;
	    }
	  if (recdes.type != REC_NEWHOME)
	    {
	      /* TODO: Add vacuum error log */
	      er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE,
		      ER_HEAP_BAD_OBJECT_TYPE, 3, oids[i].volid,
		      oids[i].pageid, oids[i].slotid);
	      break;
	    }
	  or_mvcc_get_header (&recdes, &mvcc_rec_header);
	  result =
	    mvcc_satisfies_vacuum (thread_p, &mvcc_rec_header,
				   oldest_active_mvccid);
	  switch (result)
	    {
	    case VACUUM_RECORD_REMOVE:
	      /* Clean both relocation and new home slots */
	      /* First must clean the relocation slot */
	      /* MVCC next version is set on REC_RELOCATION slot.
	       */

	      /* Clean REC_RELOCATION slot */
	      if (page == NULL)
		{
		  /* Fix page */
		  error_code =
		    pgbuf_fix_when_other_is_fixed (thread_p, &page_vpid,
						   OLD_PAGE,
						   PGBUF_LATCH_WRITE, &page,
						   &forward_page);
		}
	      if (error_code == ER_FAILED)
		{
		  /* Failed fix */
		  goto end;
		}
	      assert (page != NULL);
	      spage_vacuum_slot (thread_p, page, oids[i].slotid,
				 &MVCC_GET_NEXT_VERSION (&mvcc_rec_header));
	      vacuumed_slots[n_vacuumed_slots++] = oids[i].slotid;

	      /* Clean new home slot */
	      if (forward_page == NULL)
		{
		  forward_page =
		    pgbuf_fix (thread_p, &forward_vpid, OLD_PAGE,
			       PGBUF_LATCH_WRITE, PGBUF_CONDITIONAL_LATCH);
		  if (forward_page == NULL)
		    {
		      vacuum_oids_from_heap_page_log_and_reset (thread_p,
								&page,
								&hfid,
								vacuumed_slots,
								&n_vacuumed_slots);
		      forward_page =
			pgbuf_fix (thread_p, &forward_vpid, OLD_PAGE,
				   PGBUF_LATCH_WRITE,
				   PGBUF_UNCONDITIONAL_LATCH);
		      if (forward_page == NULL)
			{
			  /* TODO: Add vacuum error log */
			  break;
			}
		    }
		}
	      assert (forward_page != NULL);
	      spage_vacuum_slot (thread_p, forward_page, forward_oid.slotid,
				 NULL);
	      pgbuf_set_dirty (thread_p, forward_page, DONT_FREE);
	      pgbuf_unfix_and_init (thread_p, forward_page);
	      break;
	    case VACUUM_RECORD_DELETE_INSID:
	      /* Remove insert MVCCID */
	      assert (MVCC_IS_FLAG_SET (&mvcc_rec_header,
					OR_MVCC_FLAG_VALID_INSID));
	      /* Get old header size */
	      old_header_size =
		or_mvcc_header_size_from_flags (MVCC_GET_FLAG
						(&mvcc_rec_header));
	      /* Clear flag for valid insert MVCCID and get new header size */
	      MVCC_CLEAR_FLAG_BITS (&mvcc_rec_header,
				    OR_MVCC_FLAG_VALID_INSID);
	      new_header_size =
		or_mvcc_header_size_from_flags (MVCC_GET_FLAG
						(&mvcc_rec_header));
	      /* Create record and add new header */
	      temp_recdes.data = buffer;
	      temp_recdes.area_size = IO_MAX_PAGE_SIZE;
	      temp_recdes.type = recdes.type;
	      memcpy (temp_recdes.data, recdes.data, recdes.length);
	      temp_recdes.length = recdes.length;
	      error_code =
		or_mvcc_set_header (&temp_recdes, &mvcc_rec_header);
	      if (error_code != NO_ERROR)
		{
		  /* TODO: Vacuum log error */
		  pgbuf_unfix_and_init (thread_p, forward_page);
		  error_code = NO_ERROR;
		  break;
		}
	      /* Copy data from old record */
	      memcpy (temp_recdes.data + new_header_size,
		      recdes.data + old_header_size,
		      recdes.length - old_header_size);
	      /* Update slot */
	      if (spage_update (thread_p, forward_page, forward_oid.slotid,
				&temp_recdes) != SP_SUCCESS)
		{
		  /* TODO: Vacuum log error */
		  break;
		}
	      pgbuf_set_dirty (thread_p, forward_page, DONT_FREE);
	      pgbuf_unfix_and_init (thread_p, forward_page);
	      /* Add relocation slot to vacuumed slots */
	      vacuumed_slots[n_vacuumed_slots++] = oids[i].slotid;
	      break;
	    case VACUUM_RECORD_CANNOT_VACUUM:
	    default:
	      break;
	    }
	  break;

	case REC_BIGONE:
	  is_bigone = true;
	  /* Fall through */
	case REC_NEWHOME:
	case REC_HOME:
	  if (is_bigone)
	    {
	      /* Get overflow page */
	      forward_recdes.area_size = OR_OID_SIZE;
	      forward_recdes.length = OR_OID_SIZE;
	      forward_recdes.data = (char *) &forward_oid;
	      if (spage_get_record (page, oids[i].slotid, &forward_recdes,
				    COPY) != S_SUCCESS)
		{
		  /* TODO: Add vacuum error log */
		  break;
		}
	      VPID_GET_FROM_OID (&forward_vpid, &forward_oid);
	      assert (forward_page == NULL);
	      forward_page =
		pgbuf_fix (thread_p, &forward_vpid, OLD_PAGE,
			   PGBUF_LATCH_WRITE, PGBUF_UNCONDITIONAL_LATCH);
	      if (forward_page == NULL)
		{
		  /* TODO: Add vacuum error log */
		  break;
		}
	      /* Get MVCC header from overflow page */
	      heap_get_mvcc_rec_header_from_overflow (forward_page,
						      &mvcc_rec_header, NULL);
	    }
	  else
	    {
	      /* Get MVCC header */
	      if (spage_get_record (page, oids[i].slotid, &recdes, PEEK)
		  != S_SUCCESS)
		{
		  /* TODO: Add vacuum error log */
		  break;
		}
	      or_mvcc_get_header (&recdes, &mvcc_rec_header);
	    }
	  /* Check record for vacuum */

	  result =
	    mvcc_satisfies_vacuum (thread_p, &mvcc_rec_header,
				   oldest_active_mvccid);
	  switch (result)
	    {
	    case VACUUM_RECORD_REMOVE:
	      /* Vacuum record */
	      spage_vacuum_slot (thread_p, page, oids[i].slotid,
				 &MVCC_GET_NEXT_VERSION (&mvcc_rec_header));
	      vacuumed_slots[n_vacuumed_slots++] = oids[i].slotid;
	      if (is_bigone)
		{
		  /* Delete overflow pages */
		  if (heap_ovf_delete (thread_p, &hfid, &forward_oid) == NULL)
		    {
		      /* TODO: Add vacuum error log */
		      break;
		    }
		}
	      break;

	    case VACUUM_RECORD_DELETE_INSID:
	      /* Remove insert MVCCID */
	      assert (MVCC_IS_FLAG_SET (&mvcc_rec_header,
					OR_MVCC_FLAG_VALID_INSID));
	      if (is_bigone)
		{
		  /* Replace current insert MVCCID with MVCCID_ALL_VISIBLE */
		  MVCC_SET_INSID (&mvcc_rec_header, MVCCID_ALL_VISIBLE);
		  /* Set new header into overflow page - header size is not
		   * changed.
		   */
		  heap_set_mvcc_rec_header_on_overflow (forward_page,
							&mvcc_rec_header);
		  pgbuf_set_dirty (thread_p, forward_page, DONT_FREE);
		  pgbuf_unfix_and_init (thread_p, forward_page);
		}
	      else
		{
		  /* Get old header size */
		  old_header_size =
		    or_mvcc_header_size_from_flags (MVCC_GET_FLAG
						    (&mvcc_rec_header));
		  /* Clear valid insert MVCCID flag */
		  MVCC_CLEAR_FLAG_BITS (&mvcc_rec_header,
					OR_MVCC_FLAG_VALID_INSID);
		  /* Get new header size */
		  new_header_size =
		    or_mvcc_header_size_from_flags (MVCC_GET_FLAG
						    (&mvcc_rec_header));
		  /* Create record and add new header */
		  temp_recdes.data = buffer;
		  temp_recdes.area_size = IO_MAX_PAGE_SIZE;
		  temp_recdes.type = recdes.type;
		  memcpy (temp_recdes.data, recdes.data, recdes.length);
		  temp_recdes.length = recdes.length;
		  or_mvcc_set_header (&temp_recdes, &mvcc_rec_header);
		  /* Update slot */
		  if (spage_update (thread_p, page, oids[i].slotid,
				    &temp_recdes) != SP_SUCCESS)
		    {
		      /* TODO: Vacuum log error */
		      break;
		    }
		  /* Add to vacuumed slots. Page will be set as dirty later,
		   * before releasing latch on page.
		   */
		  vacuumed_slots[n_vacuumed_slots++] = oids[i].slotid;
		}
	      break;
	    case VACUUM_RECORD_CANNOT_VACUUM:
	    default:
	      break;
	    }
	  break;

	case REC_DEAD:
	  /* TODO: REC_DEAD is probably going to be removed */
	  spage_vacuum_slot (thread_p, page, oids[i].slotid, NULL);
	  vacuumed_slots[n_vacuumed_slots++] = oids[i].slotid;
	  break;

	default:
	  /* Unhandled case */
	  assert (0);
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_GENERIC_ERROR, 0);
	  break;
	}
      if (forward_page != NULL)
	{
	  pgbuf_unfix_and_init (thread_p, forward_page);
	}
    }

end:
  if (forward_page != NULL)
    {
      pgbuf_unfix_and_init (thread_p, forward_page);
    }
  if (page != NULL)
    {
      vacuum_oids_from_heap_page_log_and_reset (thread_p, &page, &hfid,
						vacuumed_slots,
						&n_vacuumed_slots);
    }
  else
    {
      /* Make all changes were logged */
      assert (n_vacuumed_slots == 0);
    }

  if (page != NULL)
    {
      pgbuf_unfix_and_init (thread_p, page);
    }
  if (is_class_locked)
    {
      lock_unlock_object (thread_p, &class_oid, oid_Root_class_oid,
			  SCH_S_LOCK, true);
    }

#if !defined (NDEBUG)
  /* Unfix all pages now. Normally all pages should already be unfixed. */
  pgbuf_unfix_all (thread_p);
#endif
  return error_code;
}

/*
 * vacuum_oids_from_heap_page_log_and_reset () - Logs the vacuumed slots from
 *						 page and reset page pointer
 *						 and number of vacuumed slots.
 *
 * return		     : Void.
 * thread_p (in)	     : Thread entry.
 * page_p (in/out)	     : Pointer to page (where the vacuumed slots are
 *			       found).
 * hfid (in)		     : Heap file identifier.
 * vacuumed_slots (in)	     : Array of vacuumed slots.
 * n_vacuumed_slots (in/out) : Number of vacuumed slots.
 *
 * NOTE: This is supposed to be called by vacuum_oids_from_heap_page whenever
 *	 the vacuumed page must be unfixed.
 */
static void
vacuum_oids_from_heap_page_log_and_reset (THREAD_ENTRY * thread_p,
					  PAGE_PTR * page_p,
					  HFID * hfid,
					  PGSLOTID * vacuumed_slots,
					  int *n_vacuumed_slots)
{
  assert (page_p != NULL && vacuumed_slots != NULL
	  && n_vacuumed_slots != NULL);
  if (*n_vacuumed_slots <= 0)
    {
      /* Nothing to do */
      if (*page_p != NULL)
	{
	  pgbuf_unfix_and_init (thread_p, *page_p);
	}
      return;
    }
  assert (*page_p != NULL);

  /* Compact page data */
  spage_compact (*page_p);

  /* Log vacuumed slots */
  vacuum_log_vacuum_oids_from_heap_page (thread_p, *page_p, hfid,
					 vacuumed_slots, *n_vacuumed_slots);
  /* Mark page as dirty and unfix */
  pgbuf_set_dirty (thread_p, *page_p, DONT_FREE);
  pgbuf_unfix_and_init (thread_p, *page_p);
  /* Reset the number of vacuumed slots */
  *n_vacuumed_slots = 0;
}


/*
 * vacuum_log_vacuum_oids_from_heap_page () - Log removing OID's from heap
 *					      page.
 *
 * return	 : Error code.
 * thread_p (in) : Thread entry.
 * page_p (in)	 : Page pointer.
 * hfid (in)	 : Heap file identifier.
 * slots (in)	 : Array of slots removed from heap page.
 * n_slots (in)  : OID count in slots.
 */
static void
vacuum_log_vacuum_oids_from_heap_page (THREAD_ENTRY * thread_p,
				       PAGE_PTR page_p, HFID * hfid,
				       PGSLOTID * slots, int n_slots)
{
  LOG_DATA_ADDR addr;
  LOG_CRUMB crumbs[2];

  /* Add number of slots to crumbs */
  crumbs[0].length = sizeof (int);
  crumbs[0].data = &n_slots;

  /* Add slots to crumbs */
  crumbs[1].length = n_slots * sizeof (PGSLOTID);
  crumbs[1].data = slots;

  /* Initialize addr */
  addr.pgptr = page_p;
  addr.offset = -1;
  addr.vfid = &hfid->vfid;

  /* Append new redo log record */
  log_append_redo_crumbs (thread_p, RVVAC_REMOVE_HEAP_OIDS, &addr, 2, crumbs);
}

/*
 * vacuum_rv_redo_remove_oids_from_heap_page () - Redo vacuum remove oids from
 *						  heap page.
 *
 * return	 : Error code.
 * thread_p (in) : Thread entry.
 * rcv (in)	 : Recovery structure.
 */
int
vacuum_rv_redo_remove_oids_from_heap_page (THREAD_ENTRY * thread_p,
					   LOG_RCV * rcv)
{
  int n_slots;
  PGSLOTID *slotids = NULL;
  PAGE_PTR page_p = NULL;
  PGSLOTID *slotid_p = NULL;

  page_p = rcv->pgptr;

  /* Get logged number of slots */
  n_slots = *((int *) rcv->data);

  /* Safety check */
  assert (rcv->length == (sizeof (int) + n_slots * sizeof (PGSLOTID)));

  /* Get logged slots */
  slotids = (PGSLOTID *) (((char *) rcv->data) + sizeof (int));

  /* Vacuum slots */
  for (slotid_p = slotids; slotid_p < slotids + n_slots; slotid_p++)
    {
      if (spage_vacuum_slot (thread_p, page_p, *slotid_p, NULL) != NO_ERROR)
	{
	  return ER_FAILED;
	}
    }
  pgbuf_set_dirty (thread_p, page_p, DONT_FREE);

  return NO_ERROR;
}

/*
 * vacuum_produce_log_block_data () - After logging a block of log data,
 *				      useful information for vacuum is passed
 *				      by log manager and should be saved in
 *				      lock-free buffer.
 *
 * return	      : Void. 
 * thread_p (in)      : Thread entry.
 * start_lsa (in)     : Log block starting LSA.
 * oldest_mvccid (in) : Log block oldest MVCCID.
 * newest_mvccid (in) : Log block newest MVCCID.
 */
void
vacuum_produce_log_block_data (THREAD_ENTRY * thread_p, LOG_LSA * start_lsa,
			       MVCCID oldest_mvccid, MVCCID newest_mvccid)
{
  VACUUM_DATA_ENTRY block_data;

  if (prm_get_bool_value (PRM_ID_DISABLE_VACUUM))
    {
      return;
    }

  if (vacuum_Block_data_buffer == NULL)
    {
      /* TODO: Right now, the vacuum is not working when a database is
       *       created, which means we will "leak" some MVCC operations.
       * There are two possible solutions:
       * 1. Initialize vacuum just to collect information on MVCC operations
       *    done while creating the database.
       * 2. Disable MVCC operation while creating database. No concurrency,
       *    no MVCC is required.
       * Option #2 is best, however the dynamic MVCC headers for heap records
       * are required. Postpone solution until then, and just set a warning
       * here.
       * Update:
       * Alex is going disable MVCC when the server will work in stand-alone
       * mode with the implementation for Dynamic MVCC header for heap.
       */
      er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE, ER_GENERIC_ERROR, 0);
      return;
    }

  /* Set blockid */
  block_data.blockid = VACUUM_GET_LOG_BLOCKID (start_lsa->pageid);

  /* Check the blockid data is not corrupted */
  assert (block_data.blockid >= 0);
  assert (MVCCID_IS_VALID (oldest_mvccid));
  assert (MVCCID_IS_VALID (newest_mvccid));
  assert (!mvcc_id_precedes (newest_mvccid, oldest_mvccid));

  /* Set start lsa for block */
  LSA_COPY (&block_data.start_lsa, start_lsa);
  /* Set oldest and newest MVCCID */
  block_data.oldest_mvccid = oldest_mvccid;
  block_data.newest_mvccid = newest_mvccid;

  vacuum_er_log (VACUUM_ER_LOG_LOGGING | VACUUM_ER_LOG_VACUUM_DATA,
		 "VACUUM, thread %d calls: "
		 "vacuum_produce_log_block_data: blockid=(%lld) "
		 "start_lsa=(%lld, %d) old_mvccid=(%llu) new_mvccid=(%llu)\n",
		 thread_get_current_entry_index (),
		 block_data.blockid,
		 (long long int) block_data.start_lsa.pageid,
		 (int) block_data.start_lsa.offset, block_data.oldest_mvccid,
		 block_data.newest_mvccid);

  /* Push new block into block data buffer */
  if (!lock_free_circular_queue_push (vacuum_Block_data_buffer, &block_data))
    {
      /* Push failed, the buffer must be full */
      /* TODO: Set a new message error for full block data buffer */
      /* TODO: Probably this case should be avoided... Make sure that we
       *       do not lose vacuum data so there has to be enough space to
       *       keep it.
       */
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_GENERIC_ERROR, 0);
      return;
    }
}

/*
 * vacuum_produce_log_block_dropped_classes () - After logging a block of log data,
 *				      useful information for vacuum is passed
 *				      by log manager and should be saved in
 *				      lock-free buffer.
 *
 * return	      : error
 * thread_p (in)      : Thread entry.
 * entries (in)	      : vacuum dropped class entries to be added to buffer.
 */
int
vacuum_produce_log_block_dropped_classes (THREAD_ENTRY * thread_p,
					  void * entries)
{
  int n_dropped_entries = 0;
  VACUUM_DROPPED_ENTRY *new_entry = NULL;
  LOG_DROPPED_CLS_BTID_ENTRY *entry = NULL;
  VACUUM_DROPPED_ENTRY *new_buffer = NULL;
  int n_new_entries = 0;

  if (prm_get_bool_value (PRM_ID_DISABLE_VACUUM))
    {
      return NO_ERROR;
    }

  if (vacuum_Dropped_class_buffer == NULL
      || vacuum_Dropped_class_buffer->dropped_entries == NULL)
    {
      er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE, ER_GENERIC_ERROR, 0);
      return ER_FAILED;
    }

  for (entry = (LOG_DROPPED_CLS_BTID_ENTRY *) entries; entry != NULL;
       entry = entry->next)
    {
      n_new_entries++;
    }

  /* Push new block into block data buffer */
  (void) pthread_mutex_lock (&vacuum_Dropped_class_buffer->mutex);

  n_dropped_entries = vacuum_Dropped_class_buffer->n_dropped_entries;
  if (n_dropped_entries + n_new_entries >
      vacuum_Dropped_class_buffer->capacity)
    {
      new_buffer = (VACUUM_DROPPED_ENTRY *)
	realloc (vacuum_Dropped_class_buffer->dropped_entries,
		 (vacuum_Dropped_class_buffer->capacity
		  + VACUUM_DROPPED_DATA_BUFFER_CAPACITY)
		 * sizeof (VACUUM_DROPPED_ENTRY));
      if (new_buffer == NULL)
	{
	  pthread_mutex_unlock (&vacuum_Dropped_class_buffer->mutex);
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_GENERIC_ERROR, 0);
	  return ER_FAILED;
	}
      vacuum_Dropped_class_buffer->capacity +=
	VACUUM_DROPPED_DATA_BUFFER_CAPACITY;
      vacuum_Dropped_class_buffer->dropped_entries = new_buffer;
    }

  for (entry = (LOG_DROPPED_CLS_BTID_ENTRY *) entries; entry != NULL;
       entry = entry->next)
    {
      new_entry =
	&vacuum_Dropped_class_buffer->dropped_entries[n_dropped_entries];
      if (new_entry == NULL)
	{
	  pthread_mutex_unlock (&vacuum_Dropped_class_buffer->mutex);
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_GENERIC_ERROR, 0);
	  return ER_FAILED;
	}

      new_entry->mvccid = entry->mvccid;
      COPY_OID (&new_entry->dropped_cls_btid.class_oid, &entry->id.class_oid);

      n_dropped_entries++;
    }
  vacuum_Dropped_class_buffer->n_dropped_entries = n_dropped_entries;
  pthread_mutex_unlock (&vacuum_Dropped_class_buffer->mutex);

  return NO_ERROR;
}

/*
 * vacuum_produce_log_block_dropped_indexes () - After logging a block of log data,
 *				      useful information for vacuum is passed
 *				      by log manager and should be saved in
 *				      lock-free buffer.
 *
 * return	      : Void. 
 * thread_p (in)      : Thread entry.
 * entries (in)	      : vacuum dropped index entries to be added to buffer.
 */
int
vacuum_produce_log_block_dropped_indexes (THREAD_ENTRY * thread_p,
					  void * entries)
{
  int n_dropped_entries = 0;
  VACUUM_DROPPED_ENTRY *new_entry = NULL;
  LOG_DROPPED_CLS_BTID_ENTRY *entry = NULL;
  VACUUM_DROPPED_ENTRY *new_buffer = NULL;
  int n_new_entries = 0;

  if (prm_get_bool_value (PRM_ID_DISABLE_VACUUM))
    {
      return NO_ERROR;
    }

  if (vacuum_Dropped_index_buffer == NULL
      || vacuum_Dropped_index_buffer->dropped_entries == NULL)
    {
      er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE, ER_GENERIC_ERROR, 0);
      return ER_FAILED;
    }

  for (entry = (LOG_DROPPED_CLS_BTID_ENTRY *) entries; entry != NULL;
       entry = entry->next)
    {
      n_new_entries++;
    }

  /* Push new block into block data buffer */
  (void) pthread_mutex_lock (&vacuum_Dropped_index_buffer->mutex);

  n_dropped_entries = vacuum_Dropped_index_buffer->n_dropped_entries;
  if (n_dropped_entries + n_new_entries >
      vacuum_Dropped_index_buffer->capacity)
    {
      new_buffer = (VACUUM_DROPPED_ENTRY *)
	realloc (vacuum_Dropped_index_buffer->dropped_entries,
		 (vacuum_Dropped_index_buffer->capacity
		  + VACUUM_DROPPED_DATA_BUFFER_CAPACITY)
		 * sizeof (VACUUM_DROPPED_ENTRY));
      if (new_buffer == NULL)
	{
	  pthread_mutex_unlock (&vacuum_Dropped_index_buffer->mutex);
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_GENERIC_ERROR, 0);
	  return ER_FAILED;
	}
      vacuum_Dropped_index_buffer->capacity +=
	VACUUM_DROPPED_DATA_BUFFER_CAPACITY;
      vacuum_Dropped_index_buffer->dropped_entries = new_buffer;
    }

  for (entry = (LOG_DROPPED_CLS_BTID_ENTRY *) entries; entry != NULL;
       entry = entry->next)
    {
      new_entry =
	&vacuum_Dropped_index_buffer->dropped_entries[n_dropped_entries];
      if (new_entry == NULL)
	{
	  pthread_mutex_unlock (&vacuum_Dropped_index_buffer->mutex);
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_GENERIC_ERROR, 0);
	  return ER_FAILED;
	}

      new_entry->mvccid = entry->mvccid;
      BTID_COPY (&new_entry->dropped_cls_btid.btid, &entry->id.btid);

      n_dropped_entries++;
    }
  vacuum_Dropped_index_buffer->n_dropped_entries = n_dropped_entries;
  pthread_mutex_unlock (&vacuum_Dropped_index_buffer->mutex);

  return NO_ERROR;
}

/*
 * vacuum_master_start () - Base function for auto vacuum routine. It will
 *			  process vacuum data and assign vacuum jobs to
 *			  workers. Each job will process a block of log data
 *			  and vacuum records from b-trees and heap files.
 *
 * return : Void.
 *
 * TODO: Current vacuum algorithm cannot handle REC_MVCC_NEXT_VERSION slot
 *	 types. We may need to add a new routine that will only vacuum
 *	 classes with referable objects. Currently we can only do this by
 *	 scanning the entire heap file and update REC_MVCC_NEXT_VERSION slots.
 *	 It should be called rarely and may require keeping some statistics.
 */
void
vacuum_master_start (void)
{
  THREAD_ENTRY *thread_p = thread_get_thread_entry_info ();

  /* Start a new vacuum iteration that processes log to create vacuum jobs */
  vacuum_process_vacuum_data (thread_p);
}

/*
 * vacuum_process_vacuum_data () - Start a new vacuum iteration that processes
 *				   vacuum data and identifies blocks candidate
 *				   to assign as jobs for vacuum workers.
 *
 * return	 : Void.
 * thread_p (in) : Thread entry.
 */
static void
vacuum_process_vacuum_data (THREAD_ENTRY * thread_p)
{
  int i;
  VACUUM_DATA_ENTRY *entry = NULL;
  VACUUM_LOG_BLOCKID blockid;
  MVCCID global_oldest_mvccid;

  if (vacuum_Data == NULL
      || (vacuum_Data->n_table_entries <= 0
	  && LOCK_FREE_CIRCULAR_QUEUE_IS_EMPTY (vacuum_Block_data_buffer)))
    {
      /* Vacuum data was not loaded yet from disk or it doesn't have any
       * entries.
       */
      return;
    }

  if (prm_get_bool_value (PRM_ID_DISABLE_VACUUM))
    {
      return;
    }

  global_oldest_mvccid = logtb_get_lowest_active_mvccid (thread_p);

  /* Lock vacuum data */
  VACUUM_LOCK_DATA ();

  /* Remove vacuumed entries */
  vacuum_data_remove_finished_entries (thread_p);

  /* Append newly logged blocks at the end of the vacuum data table */
  vacuum_consume_buffer_log_blocks (thread_p, false);
  vacuum_consume_dropped_class_buffer_log_blocks (thread_p);
  vacuum_consume_dropped_index_buffer_log_blocks (thread_p);

  /* Search for blocks ready to be vacuumed */
  for (i = 0; i < vacuum_Data->n_table_entries; i++)
    {
      entry = VACUUM_DATA_GET_ENTRY (i);
      if (!VACUUM_LOG_BLOCK_CAN_VACUUM (entry, global_oldest_mvccid))
	{
	  /* This block cannot be vacuumed */
	  continue;
	}
      /* Flag block as being vacuumed in order to avoid re-run vacuum on next
       * iteration.
       */
      VACUUM_BLOCK_STATUS_SET_REQUESTED (entry);
      blockid = VACUUM_DATA_ENTRY_BLOCKID (entry);
#if defined (SERVER_MODE)
      if (!thread_wakeup_vacuum_worker_thread ((void *) &blockid))
	{
	  /* All vacuum worker threads are busy, stop this iteration */ ;
	  /* Clear being vacuumed flag */
	  VACUUM_BLOCK_STATUS_SET_AVAILABLE (entry);

	  vacuum_er_log (VACUUM_ER_LOG_MASTER,
			 "VACUUM: thread(%d): request vacuum job on block "
			 "(%lld)\n", thread_get_current_entry_index (),
			 blockid);

	  /* Unlock data */
	  goto end;
	}
#endif /* SERVER_MODE */
      /* TODO: Add stand-alone mode */
    }

#if defined (SERVER_MODE)
end:
#endif /* SERVER_MODE */

  /* Unlock data */
  VACUUM_UNLOCK_DATA ();
}

/*
 * vacuum_process_log_block () - Vacuum heap and b-tree entries using log
 *				 information found in a block of pages.
 *
 * return	   : Error code.
 * thread_p (in)   : Thread entry.
 * data (in) : Block data.
 */
static int
vacuum_process_log_block (THREAD_ENTRY * thread_p, VACUUM_DATA_ENTRY * data)
{
#define VACUUM_MAX_OID_BUFFER_SIZE  4000
  LOG_ZIP *log_zip_ptr = NULL;
  LOG_LSA log_lsa;
  LOG_PAGEID first_block_pageid =
    VACUUM_FIRST_LOG_PAGEID_IN_BLOCK (VACUUM_DATA_ENTRY_BLOCKID (data));
  int error_code = NO_ERROR;
  struct log_data log_record_data;
  char *undo_data_buffer = NULL, *undo_data = NULL;
  int undo_data_buffer_size, undo_data_size;
  char log_pgbuf[IO_MAX_PAGE_SIZE + MAX_ALIGNMENT];
  LOG_PAGE *log_page_p = NULL;
  BTID_INT btid_int;
  BTID sys_btid;
  DB_VALUE key_value;
  OID class_oid, oid;
  MVCCID threshold_mvccid = logtb_get_lowest_active_mvccid (thread_p);
  int unique;
  OID oid_buffer[VACUUM_MAX_OID_BUFFER_SIZE];
  int n_oids = 0;
  MVCC_BTREE_OP_ARGUMENTS mvcc_args;
  MVCC_REC_HEADER mvcc_header;
  MVCCID mvccid;
  bool vacuum_complete = false, class_locked = false, is_ghost = false;

  if (prm_get_bool_value (PRM_ID_DISABLE_VACUUM))
    {
      return NO_ERROR;
    }

  /* Allocate space to unzip log data */
  log_zip_ptr = log_zip_alloc (IO_PAGESIZE, false);
  if (log_zip_ptr == NULL)
    {
      logpb_fatal_error (thread_p, true, ARG_FILE_LINE,
			 "vacuum_process_log_block");
      error_code = ER_FAILED;
      goto end;
    }

  /* Initialize key value as NULL */
  DB_MAKE_NULL (&key_value);

  /* Set sys_btid pointer for internal b-tree block */
  btid_int.sys_btid = &sys_btid;

  /* Check starting lsa is not null and that it really belong to this block */
  assert (!LSA_ISNULL (&VACUUM_DATA_ENTRY_START_LSA (data))
	  && (VACUUM_DATA_ENTRY_BLOCKID (data)
	      ==
	      VACUUM_GET_LOG_BLOCKID (VACUUM_DATA_ENTRY_START_LSA (data).
				      pageid)));

  /* Fetch the page where start_lsa is located */
  log_page_p = (LOG_PAGE *) PTR_ALIGN (log_pgbuf, MAX_ALIGNMENT);
  log_page_p->hdr.logical_pageid = NULL_PAGEID;
  log_page_p->hdr.offset = NULL_OFFSET;

  vacuum_er_log (VACUUM_ER_LOG_WORKER,
		 "VACUUM: thread(%d): vacuum_process_log_block ():"
		 "blockid(%lld) start_lsa(%lld, %d) old_mvccid(%llu)"
		 " new_mvccid(%llu)\n", thread_get_current_entry_index (),
		 data->blockid, (long long int) data->start_lsa.pageid,
		 (int) data->start_lsa.offset, data->oldest_mvccid,
		 data->newest_mvccid);

  /* Follow the linked records starting with start_lsa */
  for (LSA_COPY (&log_lsa, &VACUUM_DATA_ENTRY_START_LSA (data));
       !LSA_ISNULL (&log_lsa) && log_lsa.pageid >= first_block_pageid;
       /* Get log lsa for previous MVCC operation */
       (void) or_unpack_log_lsa (undo_data, &log_lsa))
    {
#if defined(SERVER_MODE)
      if (thread_p->shutdown)
	{
	  /* Server shutdown was requested, stop vacuuming. */
	  goto end;
	}
#endif /* SERVER_MODE */
      vacuum_er_log (VACUUM_ER_LOG_WORKER,
		     "VACUUM: thread(%d): progess log entry at log_lsa "
		     "(%lld, %d)\n", thread_get_current_entry_index (),
		     (long long int) log_lsa.pageid, (int) log_lsa.offset);

      thread_set_is_process_log_phase (thread_p, true);
      if (log_page_p->hdr.logical_pageid != log_lsa.pageid)
	{
	  if (logpb_fetch_page (thread_p, log_lsa.pageid, log_page_p) == NULL)
	    {
	      logpb_fatal_error (thread_p, true, ARG_FILE_LINE,
				 "vacuum_process_log_block");
	      thread_set_is_process_log_phase (thread_p, false);
	      goto end;
	    }
	}
      /* Get undo data */
      error_code =
	vacuum_get_mvcc_delete_undo_data (thread_p, &log_lsa, log_page_p,
					  &log_record_data, &mvccid,
					  log_zip_ptr, &undo_data_buffer,
					  &undo_data_buffer_size, &undo_data,
					  &undo_data_size);
      if (error_code != NO_ERROR)
	{
	  thread_set_is_process_log_phase (thread_p, false);
	  goto end;
	}
      assert (undo_data != NULL);

      thread_set_is_process_log_phase (thread_p, false);

      if (mvcc_id_follow_or_equal (mvccid, threshold_mvccid))
	{
	  /* Object cannot be vacuumed */
	  /* Shouldn't happen. The block should be vacuumed only if it's
	   * oldest MVCCID precedes the global oldest active MVCCID.
	   */
	  assert (0);
	  logpb_fatal_error (thread_p, true, ARG_FILE_LINE,
			     "vacuum_process_log_block");
	  goto end;
	}

      if (LOG_IS_MVCC_HEAP_OPERATION (log_record_data.rcvindex))
	{
	  /* Get record OID and add it to oid_buffer */
	  oid.pageid = log_record_data.pageid;
	  oid.volid = log_record_data.volid;
	  oid.slotid = log_record_data.offset;

	  if (n_oids < VACUUM_MAX_OID_BUFFER_SIZE)
	    {
	      COPY_OID (&oid_buffer[n_oids], &oid);
	      n_oids++;
	    }
	  else
	    {
	      /* Full OID buffer, vacuum current objects */
	      error_code =
		vacuum_oids_from_heap (thread_p, oid_buffer, n_oids,
				       threshold_mvccid);
	      if (error_code != NO_ERROR)
		{
		  goto end;
		}
	      n_oids = 0;
	      break;
	    }
	}
      else if (LOG_IS_MVCC_BTREE_OPERATION (log_record_data.rcvindex))
	{
	  MVCCID save_mvccid = mvccid;
	  /* TODO: is mvccid really required to be stored? it can also be
	   *       obtained from undoredo data.
	   */
	  btree_rv_read_keyval_info_nocopy (thread_p, undo_data,
					    undo_data_size, &btid_int,
					    &class_oid, &oid, &mvcc_header,
					    &key_value, true, true, &is_ghost,
					    &class_locked);
	  if (is_ghost)
	    {
	      /* Index was deleted? Vacuum cannot do anything here, proceed
	       * to previous MVCC operation.
	       */
	      vacuum_er_log (VACUUM_ER_LOG_BTREE | VACUUM_ER_LOG_WARNING
			     | VACUUM_ER_LOG_WORKER,
			     "VACUUM WARNING: thread(%d): "
			     "index couldn't be found\n",
			     thread_get_current_entry_index ());
	      if (class_locked)
		{
		  lock_unlock_object (thread_p, &class_oid,
				      oid_Root_class_oid, SCH_S_LOCK, true);
		  class_locked = false;
		}
	      continue;
	    }
	  assert (!OID_ISNULL (&oid) && !OID_ISNULL (&class_oid));

	  if (!class_locked)
	    {
	      vacuum_er_log (VACUUM_ER_LOG_ERROR | VACUUM_ER_LOG_BTREE
			     | VACUUM_ER_LOG_WORKER,
			     "VACUUM ERROR: thread(%d): failed locking "
			     "class(%d, %d, %d)\n",
			     thread_get_current_entry_index (),
			     class_oid.volid, class_oid.pageid,
			     class_oid.slotid);
	    }

	  /* Vacuum b-tree */
	  unique = BTREE_IS_UNIQUE (btid_int.unique_pk);
	  /* Set btree_delete purpose: vacuum object if it was deleted or
	   * vacuum insert MVCCID if it was inserted.
	   */
	  if (log_record_data.rcvindex == RVBT_KEYVAL_INS_LFRECORD_MVCC_DELID)
	    {
	      mvcc_args.purpose = MVCC_BTREE_VACUUM_OBJECT;
	      mvcc_args.delete_mvccid = MVCC_GET_DELID (&mvcc_header);
	    }
	  else
	    {
	      mvcc_args.purpose = MVCC_BTREE_VACUUM_INSID;
	      mvcc_args.insert_mvccid = MVCC_GET_INSID (&mvcc_header);
	    }

	  vacuum_er_log (VACUUM_ER_LOG_BTREE | VACUUM_ER_LOG_WORKER,
			 "VACUUM: thread(%d): vacuum from b-tree: "
			 "btidp(%d, (%d %d)) oid(%d, %d, %d) "
			 "class_oid(%d, %d, %d), purpose=%s, mvccid=%lld\n",
			 thread_get_current_entry_index (),
			 btid_int.sys_btid->root_pageid,
			 btid_int.sys_btid->vfid.fileid,
			 btid_int.sys_btid->vfid.volid,
			 oid.volid, oid.pageid, oid.slotid,
			 class_oid.volid, class_oid.pageid, class_oid.slotid,
			 mvcc_args.purpose == MVCC_BTREE_VACUUM_OBJECT ?
			 "rem_object" : "rem_insid", mvccid);
	  (void) btree_delete (thread_p, btid_int.sys_btid, &key_value,
			       &class_oid, &oid, BTREE_NO_KEY_LOCKED, &unique,
			       SINGLE_ROW_DELETE, NULL, &mvcc_args);
	  error_code = er_errid ();
	  if (error_code != NO_ERROR)
	    {
	      /* TODO:
	       * Right now, errors may occur. For instance, the object or the
	       * key may not be found (they may have been already vacuumed or
	       * they may not be found due to ghosting).
	       * For now, continue vacuuming and log these errors. Must
	       * investigate and see if these cases can be isolated.
	       */
	      vacuum_er_log (VACUUM_ER_LOG_BTREE | VACUUM_ER_LOG_WORKER,
			     "VACUUM: thread(%d): Error deleting object or "
			     "insert MVCCID: error_code=%d",
			     thread_get_current_entry_index (), error_code);
	      er_clear ();
	      error_code = NO_ERROR;
	    }
	  /* Unlock class */
	  lock_unlock_object (thread_p, &class_oid, oid_Root_class_oid,
			      SCH_S_LOCK, true);
	  class_locked = false;

	  /* Clear key value */
	  db_value_clear (&key_value);
	}
      else
	{
	  /* Safeguard code */
	  assert (false);
	}
    }

  vacuum_complete = true;

end:
  if (class_locked)
    {
      assert (!OID_ISNULL (&class_oid));
      lock_unlock_object (thread_p, &class_oid, oid_Root_class_oid, IX_LOCK,
			  true);
    }
  if (n_oids != 0)
    {
      error_code =
	vacuum_oids_from_heap (thread_p, oid_buffer, n_oids,
			       threshold_mvccid);
      if (error_code != NO_ERROR)
	{
	  vacuum_complete = false;
	}
    }

  /* TODO: Check that if start_lsa can be set to a different value when
   *       vacuum is not complete, to avoid processing the same log data
   *       again.
   */
  vacuum_finished_block_vacuum (thread_p, data, vacuum_complete);
  if (log_zip_ptr != NULL)
    {
      log_zip_free (log_zip_ptr);
    }
  if (undo_data_buffer != NULL)
    {
      free (undo_data_buffer);
    }
  db_value_clear (&key_value);

  /* Unfix all pages now. Normally all pages should already be unfixed. */
  pgbuf_unfix_all (thread_p);

  return error_code;
}

/*
 * vacuum_start_new_job () - Start a vacuum job which process one block of
 *			     log data.
 *
 * return	 : Void.
 * thread_p (in) : Thread entry.
 * blockid (in)	 : Block of log data identifier.
 */
void
vacuum_start_new_job (THREAD_ENTRY * thread_p, VACUUM_LOG_BLOCKID blockid)
{
  VACUUM_DATA_ENTRY *entry = NULL;
  VACUUM_DATA_ENTRY local_entry;
  LOG_TDES *tdes = NULL;
  int tran_index = NULL_TRAN_INDEX;

  /* The table where data about log blocks is kept can be modified.
   * Lock vacuum data, identify the right block data and copy them.
   */
  VACUUM_LOCK_DATA ();
  entry = vacuum_get_vacuum_data_entry (blockid);
  if (entry == NULL)
    {
      /* This is not supposed to happen!!! */
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_GENERIC_ERROR, 0);
      assert (false);
      return;
    }
  assert (VACUUM_BLOCK_STATUS_IS_REQUESTED (entry));
  VACUUM_BLOCK_STATUS_SET_RUNNING (entry);
  memcpy (&local_entry, entry, sizeof (VACUUM_DATA_ENTRY));

  /* TODO: Due to the fact that there several vacuum workers and because in
   *       some cases threads are identified by their transaction index
   *       (e.g. locking), the worker is assigned a transaction index. Note
   *       however that this isn't really a transaction, it is a cleanup tool
   *       that may sometimes use resources common to regular transactions.
   *       Maybe there is an alternative way to handle these problems.
   */
  tran_index =
    logtb_assign_tran_index (thread_p, NULL_TRANID, TRAN_ACTIVE, NULL, NULL,
			     TRAN_LOCK_INFINITE_WAIT, TRAN_DEFAULT_ISOLATION);
  if (tran_index == NULL_TRAN_INDEX)
    {
      vacuum_er_log (VACUUM_ER_LOG_WORKER | VACUUM_ER_LOG_ERROR,
		     "VACUUM ERROR: thread(%d): could not assign a"
		     "transaction index\n",
		     thread_get_current_entry_index ());
      VACUUM_BLOCK_STATUS_SET_AVAILABLE (entry);
      VACUUM_UNLOCK_DATA ();
      return;
    }
  VACUUM_UNLOCK_DATA ();

  /* Run vacuum */
  (void) vacuum_process_log_block (thread_p, &local_entry);

  if (tran_index != NULL_TRAN_INDEX)
    {
      log_commit (thread_p, tran_index, false);
      logtb_free_tran_index (thread_p, tran_index);
    }
}

/*
 * vacuum_finished_block_vacuum () - Called when vacuuming a block is stopped.
 *
 * return		   : Void.
 * thread_p (in)	   : Thread entry.
 * data (in)		   : Vacuum block data.
 * is_vacuum_complete (in) : True if the entire block was processed.
 *
 * NOTE: The block data received here is not a block in vacuum data table.
 *	 It is just a copy (because the table can be changed and the data
 *	 can be moved). First obtain the block data in the table and copy the
 *	 block data received as argument.
 */
static void
vacuum_finished_block_vacuum (THREAD_ENTRY * thread_p,
			      VACUUM_DATA_ENTRY * data,
			      bool is_vacuum_complete)
{
  VACUUM_DATA_ENTRY *table_entry = NULL;
  VACUUM_LOG_BLOCKID blockid;

  /* Lock vacuum data to add changes */
  VACUUM_LOCK_DATA ();

  /* Clear running flag */
  VACUUM_BLOCK_STATUS_SET_AVAILABLE (data);

  /* Find the equivalent block in vacuum data table */
  blockid = VACUUM_DATA_ENTRY_BLOCKID (data);
  table_entry = vacuum_get_vacuum_data_entry (blockid);
  if (table_entry == NULL)
    {
      /* Impossible, safeguard code */
      assert (0);
      VACUUM_UNLOCK_DATA ();
      return;
    }
  assert (VACUUM_BLOCK_STATUS_IS_RUNNING (table_entry));

  if (is_vacuum_complete)
    {
      /* Set status as vacuumed. Vacuum master will remove it from table */
      VACUUM_BLOCK_STATUS_SET_VACUUMED (table_entry);
    }
  else
    {
      /* Vacuum will have to be re-run */
      VACUUM_BLOCK_STATUS_SET_AVAILABLE (table_entry);
      /* Copy new block data */
      /* The only relevant information is in fact the updated start_lsa
       * if it has changed.
       */
    }

  /* Unlock vacuum data */
  VACUUM_UNLOCK_DATA ();
}

/*
 * vacuum_get_mvcc_delete_undo_data () - Get undo data for an MVCC delete
 *					 log record.
 *
 * return			  : Error code.
 * thread_p (in)		  : Thread entry.
 * undoredo (in)		  : Undoredo data.
 * log_lsa_p (in/out)		  : Input is the start of undo data. Output is
 *				    the end of undo data.
 * log_page_p (in/out)		  : The log page for log_lsa_p.
 * log_unzip_ptr (in/out)	  : Used to unzip compressed data.
 * undo_data_buffer (in/out)	  : Buffer for undo data. If it not large
 *				    enough, it is automatically extended.
 * undo_data_buffer_size (in/out) : Size of undo data buffer.
 * undo_data_ptr (out)		  : Undo data pointer.
 * undo_data_size (out)		  : Undo data size.
 */
static int
vacuum_get_mvcc_delete_undo_data (THREAD_ENTRY * thread_p,
				  LOG_LSA * log_lsa_p,
				  LOG_PAGE * log_page_p,
				  struct log_data *log_record_data,
				  MVCCID * mvccid,
				  LOG_ZIP * log_unzip_ptr,
				  char **undo_data_buffer,
				  int *undo_data_buffer_size,
				  char **undo_data_ptr, int *undo_data_size)
{
  LOG_RECORD_HEADER *log_rec_header = NULL;
  struct log_undoredo *undoredo = NULL;
  struct log_undo *undo = NULL;
  int ulength;
  char *new_undo_data_buffer = NULL;
  bool is_zipped = false;

  assert (log_lsa_p != NULL && log_page_p != NULL && log_unzip_ptr != NULL);
  assert (undo_data_buffer != NULL && undo_data_buffer_size != NULL
	  && undo_data_ptr != NULL && undo_data_size != NULL);

  /* Get log record header */
  log_rec_header = LOG_GET_LOG_RECORD_HEADER (log_page_p, log_lsa_p);
  LOG_READ_ADD_ALIGN (thread_p, sizeof (*log_rec_header), log_lsa_p,
		      log_page_p);
  *mvccid = log_rec_header->mvcc_id;

  if (log_rec_header->type == LOG_UNDO_DATA)
    {
      /* Get log record undo information */
      LOG_READ_ADVANCE_WHEN_DOESNT_FIT (thread_p, sizeof (*undo), log_lsa_p,
					log_page_p);
      undo = (struct log_undo *) (log_page_p->area + log_lsa_p->offset);
      ulength = undo->length;
      *log_record_data = undo->data;
      LOG_READ_ADD_ALIGN (thread_p, sizeof (*undo), log_lsa_p, log_page_p);
    }
  else if (log_rec_header->type == LOG_UNDOREDO_DATA)
    {
      /* Get log record undoredo information */
      LOG_READ_ADVANCE_WHEN_DOESNT_FIT (thread_p, sizeof (*undoredo),
					log_lsa_p, log_page_p);
      undoredo =
	(struct log_undoredo *) (log_page_p->area + log_lsa_p->offset);
      ulength = undoredo->ulength;
      *log_record_data = undoredo->data;
      LOG_READ_ADD_ALIGN (thread_p, sizeof (*undoredo), log_lsa_p,
			  log_page_p);
    }
  else
    {
      /* Unexpected case */
      assert (false);
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_GENERIC_ERROR, 0);
      return ER_FAILED;
    }

  if (ZIP_CHECK (ulength))
    {
      /* Get real size */
      *undo_data_size = (int) GET_ZIP_LEN (ulength);
      is_zipped = true;
    }
  else
    {
      *undo_data_size = ulength;
    }

  if (log_lsa_p->offset + *undo_data_size < (int) LOGAREA_SIZE)
    {
      /* Set undo data pointer directly to log data */
      *undo_data_ptr = (char *) log_page_p->area + log_lsa_p->offset;
    }
  else
    {
      /* Undo data is found on several pages and needs to be copied to a
       * contiguous area.
       */
      if (*undo_data_buffer == NULL)
	{
	  /* Allocate new undo data buffer */
	  *undo_data_buffer_size = *undo_data_size;
	  *undo_data_buffer = malloc (*undo_data_buffer_size);
	  if (*undo_data_buffer == NULL)
	    {
	      logpb_fatal_error (thread_p, true, ARG_FILE_LINE,
				 "vacuum_get_mvcc_delete_undo_data");
	      return ER_FAILED;
	    }
	}
      else if (*undo_data_buffer_size < *undo_data_size)
	{
	  /* Must extend buffer so all undo data can fit */
	  *undo_data_buffer_size = *undo_data_size;
	  new_undo_data_buffer =
	    realloc (*undo_data_buffer, *undo_data_buffer_size);
	  if (new_undo_data_buffer == NULL)
	    {
	      logpb_fatal_error (thread_p, true, ARG_FILE_LINE,
				 "vacuum_get_mvcc_delete_undo_data");
	      return ER_FAILED;
	    }
	  *undo_data_buffer = new_undo_data_buffer;
	}
      *undo_data_ptr = *undo_data_buffer;
      /* Copy data to buffer */
      logpb_copy_from_log (thread_p, *undo_data_ptr, *undo_data_size,
			   log_lsa_p, log_page_p);
    }

  if (is_zipped)
    {
      /* Unzip data */
      if (log_unzip (log_unzip_ptr, *undo_data_size, *undo_data_ptr))
	{
	  *undo_data_size = (int) log_unzip_ptr->data_length;
	  *undo_data_ptr = (char *) log_unzip_ptr->log_data;
	}
      else
	{
	  logpb_fatal_error (thread_p, true, ARG_FILE_LINE,
			     "vacuum_get_mvcc_delete_undo_data");
	  return ER_FAILED;
	}
    }

  if ((prm_get_integer_value (PRM_ID_ER_LOG_VACUUM) & VACUUM_ER_LOG_WORKER) !=
      0)
    {
      LOG_LSA prev_mvcc_log_lsa;
      (void) or_unpack_log_lsa (*undo_data_ptr, &prev_mvcc_log_lsa);
      vacuum_er_log (VACUUM_ER_LOG_WORKER,
		     "VACUUM: thread(%d): unpacked log entry:"
		     "mvccid(%lld) rec_type(%s) rcvindex(%d) - "
		     "volid(%d) pageid(%d) offset(%d) - "
		     "prev_mvcc_log_lsa (%lld, %d)\n",
		     thread_get_current_entry_index (),
		     log_rec_header->mvcc_id,
		     (log_rec_header->type == LOG_UNDO_DATA) ?
		     "undo" : "undoredo",
		     log_record_data->rcvindex, log_record_data->volid,
		     log_record_data->pageid, log_record_data->offset,
		     (long long int) prev_mvcc_log_lsa.pageid,
		     (int) prev_mvcc_log_lsa.offset);
    }
  return NO_ERROR;
}

/*
 * vacuum_get_vacuum_data_entry () - Search for blockid in vacuum data table
 *				     and return pointer to the equivalent
 *				     entry.
 *
 * return	: Pointer to data entry if blockid is found, NULL otherwise.
 * blockid (in) : Log block identifier.
 */
static VACUUM_DATA_ENTRY *
vacuum_get_vacuum_data_entry (VACUUM_LOG_BLOCKID blockid)
{
  return (VACUUM_DATA_ENTRY *) bsearch (&blockid,
					vacuum_Data->vacuum_data_table,
					vacuum_Data->n_table_entries,
					sizeof (VACUUM_DATA_ENTRY),
					vacuum_compare_data_entries);
}

/*
 * vacuum_compare_data_entries () - Comparator function for vacuum data
 *				    entries. The entries' block id's are
 *				    compared.
 *
 * return    : 0 if entries are equal, negative if first entry is smaller and
 *	       positive if first entry is bigger.
 * ptr1 (in) : Pointer to first vacuum data entry.
 * ptr2 (in) : Pointer to second vacuum data entry.
 */
static int
vacuum_compare_data_entries (const void *ptr1, const void *ptr2)
{
  return (int) (VACUUM_DATA_ENTRY_BLOCKID ((VACUUM_DATA_ENTRY *) ptr1)
		- VACUUM_DATA_ENTRY_BLOCKID ((VACUUM_DATA_ENTRY *) ptr2));
}

/*
 * vacuum_load_data_from_disk () - Loads vacuum data from disk.
 *
 * return	 : Error code.
 * thread_p (in) : Thread entry.
 *
 * NOTE: Loading vacuum data should be done when the database is started,
 *	 before starting other vacuum routines.
 */
static int
vacuum_load_data_from_disk (THREAD_ENTRY * thread_p, VFID * vacuum_data_vfid)
{
  int error_code = NO_ERROR, i;
  int vacuum_data_npages = prm_get_integer_value (PRM_ID_VACUUM_DATA_PAGES);
  int vol_fd;
  VACUUM_DATA_ENTRY *entry = NULL;

  assert (vacuum_Data == NULL);

  /* Data is being loaded from disk so all data that is flushed */
  LSA_SET_NULL (&vacuum_Data_oldest_not_flushed_lsa);

  vacuum_Data_max_size = IO_PAGESIZE * vacuum_data_npages;
  vacuum_Data = (VACUUM_DATA *) malloc (vacuum_Data_max_size);
  if (vacuum_Data == NULL)
    {
      assert (false);
      return ER_OUT_OF_VIRTUAL_MEMORY;
    }

  /* Get the first vacuum data page vpid */
  if (file_find_nthpages (thread_p, vacuum_data_vfid, &vacuum_Data_vpid, 0, 1)
      < 1)
    {
      assert (false);
      return ER_FAILED;
    }
  assert (!VPID_ISNULL (&vacuum_Data_vpid));
  vol_fd = fileio_get_volume_descriptor (vacuum_Data_vpid.volid);
  /* Read vacuum data from disk */
  /* Do not use page buffer */
  /* All vacuum data pages are contiguous */
  if (fileio_read_pages (thread_p, vol_fd, (char *) vacuum_Data,
			 vacuum_Data_vpid.pageid, vacuum_data_npages,
			 IO_PAGESIZE) == NULL)
    {
      assert (false);
      return ER_FAILED;
    }

  /* Make sure no entry is marked as being vacuumed. */
  /* When vacuum data was last flushed, it is possible that some blocks
   * were being vacuumed.
   */
  for (i = 0; i < vacuum_Data->n_table_entries; i++)
    {
      VACUUM_BLOCK_STATUS_SET_AVAILABLE (VACUUM_DATA_GET_ENTRY (i));
    }

  return NO_ERROR;
}

/*
 * vacuum_load_dropped_classes_indexes_from_disk () - Loads dropped classes or
 *						      indexes from disk.
 *
 * return	 : Error code.
 * thread_p (in) : Thread entry.
 * vfid (in)	 : File identifier.
 * type (in)	 : Dropped class or index.
 */
static int
vacuum_load_dropped_classes_indexes_from_disk (THREAD_ENTRY * thread_p,
					       VFID * vfid, DROPPED_TYPE type)
{
  VACUUM_DROPPED_ENTRIES_PAGE *page = NULL;
  VPID vpid;
  VPID *dropped_cls_btid_vpidp = VACUUM_DROPPED_ENTRIES_VPID_PTR (type);
  INT16 page_count;
  INT32 *countp = VACUUM_DROPPED_ENTRIES_COUNT_PTR (type);
#if !defined (NDEBUG)
  VACUUM_TRACK_DROPPED_ENTRIES *track_head = NULL, *track_tail = NULL;
  VACUUM_TRACK_DROPPED_ENTRIES *track_new = NULL;
#endif

  assert (vfid != NULL);
  assert (type == DROPPED_CLASS || type == DROPPED_INDEX);

  /* Save vfid */
  VFID_COPY (VACUUM_DROPPED_ENTRIES_VFID_PTR (type), vfid);

  /* Save first page vpid. */
  if (file_find_nthpages (thread_p, vfid, dropped_cls_btid_vpidp, 0, 1) < 1)
    {
      assert (false);
      return ER_FAILED;
    }
  assert (!VPID_ISNULL (dropped_cls_btid_vpidp));

  /* Save total count. */
  *countp = 0;
  VPID_COPY (&vpid, dropped_cls_btid_vpidp);
  for (; !VPID_ISNULL (&vpid);)
    {
      page =
	vacuum_fix_dropped_entries_page (thread_p, &vpid, PGBUF_LATCH_READ);
      if (page == NULL)
	{
	  assert (false);
	  return ER_FAILED;
	}
      /* Get next page VPID and current page count */
      VPID_COPY (&vpid, &page->next_page);
      page_count = page->n_dropped_entries;
      *countp += (INT32) page_count;

#if !defined (NDEBUG)
      track_new =
	(VACUUM_TRACK_DROPPED_ENTRIES *)
	malloc (VACUUM_TRACK_DROPPED_ENTRIES_SIZE);
      if (track_new == NULL)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY,
		  1, VACUUM_TRACK_DROPPED_ENTRIES_SIZE);
	  /* TODO: Free already allocated tracked pages */
	  vacuum_unfix_dropped_entries_page (thread_p, page);
	  return ER_OUT_OF_VIRTUAL_MEMORY;
	}
      memcpy (&track_new->dropped_data_page, page, DB_PAGESIZE);
      track_new->next_tracked_page = NULL;
      if (track_head == NULL)
	{
	  track_head = track_tail = track_new;
	}
      else
	{
	  assert (track_tail != NULL);
	  track_tail->next_tracked_page = track_new;
	  track_tail = track_new;
	}
#endif
      vacuum_unfix_dropped_entries_page (thread_p, page);
    }

#if !defined(NDEBUG)
  if (type == DROPPED_CLASS)
    {
      vacuum_Track_dropped_classes = track_head;
    }
  else
    {
      vacuum_Track_dropped_indexes = track_head;
    }
#endif

  return NO_ERROR;
}

/*
 * vacuum_load_from_disk () - Load vacuum data and dropped classes/indexes
 *			      from disk.
 *
 * return		     : Error code.
 * thread_p (in)	     : Thread entry.
 * vacuum_data_vfid (in)     : Vacuum data file identifier.
 * dropped_classes_vfid (in) : Dropped classes file identifier.
 * dropped_indexes_vfid (in) : Dropped indexes file identifier.
 */
int
vacuum_load_from_disk (THREAD_ENTRY * thread_p, VFID * vacuum_data_vfid,
		       VFID * dropped_classes_vfid,
		       VFID * dropped_indexes_vfid)
{
  int error_code;

  if (prm_get_bool_value (PRM_ID_DISABLE_VACUUM))
    {
      return NO_ERROR;
    }

  error_code = vacuum_load_data_from_disk (thread_p, vacuum_data_vfid);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }
  error_code =
    vacuum_load_dropped_classes_indexes_from_disk (thread_p,
						   dropped_classes_vfid,
						   DROPPED_CLASS);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }
  error_code =
    vacuum_load_dropped_classes_indexes_from_disk (thread_p,
						   dropped_indexes_vfid,
						   DROPPED_INDEX);
  return error_code;
}


/*
 * vacuum_flush_data () - Flush vacuum data to disk. Only used pages are
 *			  flushed.
 *
 * return			: Error code.
 * thread_p (in)		: Thread entry.
 * flush_to_lsa (in)		: New target checkpoint lsa for flush.
 * prev_chkpt_lsa (in)		: Previous checkpoint lsa.
 * oldest_not_flushed_lsa (out) : If flush is failed, the oldest lsa that is
 *				  not yet flushed to disk must be sent to
 *				  caller.
 * is_vacuum_data_locked (in)	: True if vacuum data is locked when this
 *				  function is called. If false, it will first
 *				  lock vacuum data, and will unlock it before
 *				  exiting the function.
 */
int
vacuum_flush_data (THREAD_ENTRY * thread_p, LOG_LSA * flush_to_lsa,
		   LOG_LSA * prev_chkpt_lsa, LOG_LSA * oldest_not_flushed_lsa,
		   bool is_vacuum_data_locked)
{
  int n_pages;
  int vacuum_data_actual_size;
  int error_code = NO_ERROR;
  int vol_fd;

  if (vacuum_Data == NULL)
    {
      /* The vacuum data is not loaded yet, therefore there are no changes */
      return NO_ERROR;
    }

  if (prm_get_bool_value (PRM_ID_DISABLE_VACUUM))
    {
      return NO_ERROR;
    }

  if (!is_vacuum_data_locked)
    {
      VACUUM_LOCK_DATA ();
    }

  /* Make sure that vacuum data is up to date before flushing it */
  vacuum_consume_buffer_log_blocks (thread_p, false);

  if (LSA_ISNULL (&vacuum_Data_oldest_not_flushed_lsa))
    {
      if (!is_vacuum_data_locked)
	{
	  VACUUM_UNLOCK_DATA ();
	}
      /* No changes, nothing to flush */
      return NO_ERROR;
    }

  if (flush_to_lsa != NULL
      && LSA_GT (&vacuum_Data_oldest_not_flushed_lsa, flush_to_lsa))
    {
      if (!is_vacuum_data_locked)
	{
	  VACUUM_UNLOCK_DATA ();
	}
      /* Skip flushing */
      return NO_ERROR;
    }

  if (prev_chkpt_lsa != NULL
      && LSA_LE (&vacuum_Data_oldest_not_flushed_lsa, prev_chkpt_lsa))
    {
      if (!is_vacuum_data_locked)
	{
	  VACUUM_UNLOCK_DATA ();
	}
      /* Conservative safety check */
      assert (false);
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_GENERIC_ERROR, 0);
      return ER_FAILED;
    }

  vacuum_data_actual_size =
    VACUUM_DATA_HEADER_SIZE +
    (vacuum_Data->n_table_entries * sizeof (VACUUM_DATA_ENTRY));
  n_pages = CEIL_PTVDIV (vacuum_data_actual_size, IO_PAGESIZE);

  /* Flush to disk */
  vol_fd = fileio_get_volume_descriptor (vacuum_Data_vpid.volid);
  if (fileio_write_pages (thread_p, vol_fd, (char *) vacuum_Data,
			  vacuum_Data_vpid.pageid, n_pages, IO_PAGESIZE)
      == NULL)
    {
      if (oldest_not_flushed_lsa != NULL
	  && (LSA_ISNULL (oldest_not_flushed_lsa)
	      || LSA_LT (&vacuum_Data_oldest_not_flushed_lsa,
			 oldest_not_flushed_lsa)))
	{
	  /* If flush is failed, noticing the caller may be required */
	  LSA_COPY (oldest_not_flushed_lsa,
		    &vacuum_Data_oldest_not_flushed_lsa);
	}
      if (!is_vacuum_data_locked)
	{
	  VACUUM_UNLOCK_DATA ();
	}
      return ER_FAILED;
    }

  /* Successful flush, reset vacuum_Data_oldest_not_flushed_lsa */
  LSA_SET_NULL (&vacuum_Data_oldest_not_flushed_lsa);
  if (!is_vacuum_data_locked)
    {
      VACUUM_UNLOCK_DATA ();
    }
  return NO_ERROR;
}

/*
 * vacuum_init_vacuum_files () - Initialize information in the files used by
 *				 vacuum: vacuum data file, dropped classes
 *				 file and dropped indexes file.
 *
 * return		     : Error code.
 * thread_p (in)	     : Thread entry.
 * vacuum_data_vfid (in)     : 
 * dropped_classes_vfid (in) :
 * dropped_indexes_vfid (in) :
 */
int
vacuum_init_vacuum_files (THREAD_ENTRY * thread_p, VFID * vacuum_data_vfid,
			  VFID * dropped_classes_vfid,
			  VFID * dropped_indexes_vfid)
{
  VPID vpid;
  int vol_fd;
  VACUUM_DATA *vacuum_data_p = NULL;
  VACUUM_DROPPED_ENTRIES_PAGE *dropped_cls_btid_page = NULL;
  char page_buf[IO_MAX_PAGE_SIZE];

  assert (mvcc_Enabled);
  assert (vacuum_data_vfid != NULL && dropped_classes_vfid != NULL
	  && dropped_indexes_vfid != NULL);

  /* Initialize vacuum data file */
  /* Get first page in file */
  if (file_find_nthpages (thread_p, vacuum_data_vfid, &vpid, 0, 1) < 0)
    {
      assert (false);
      return ER_FAILED;
    }
  assert (!VPID_ISNULL (&vpid));
  /* Do not use page buffer to load vacuum data page */
  vol_fd = fileio_get_volume_descriptor (vpid.volid);
  if (fileio_read (thread_p, vol_fd, page_buf, vpid.pageid, IO_PAGESIZE)
      == NULL)
    {
      assert (false);
      return ER_FAILED;
    }
  vacuum_data_p = (VACUUM_DATA *) page_buf;
  LSA_SET_NULL (&vacuum_data_p->crt_lsa);
  vacuum_data_p->last_blockid = VACUUM_NULL_LOG_BLOCKID;
  vacuum_data_p->newest_mvccid = MVCCID_NULL;
  vacuum_data_p->oldest_mvccid = MVCCID_NULL;
  vacuum_data_p->n_table_entries = 0;
  if (fileio_write (thread_p, vol_fd, page_buf, vpid.pageid, IO_PAGESIZE)
      == NULL)
    {
      assert (false);
      return ER_FAILED;
    }

  /* Initialize dropped classes */
  if (file_find_nthpages (thread_p, dropped_classes_vfid, &vpid, 0, 1) < 0)
    {
      assert (false);
      return ER_FAILED;
    }
  dropped_cls_btid_page =
    vacuum_fix_dropped_entries_page (thread_p, &vpid, PGBUF_LATCH_WRITE);
  if (dropped_cls_btid_page == NULL)
    {
      assert (false);
      return ER_FAILED;
    }
  /* Pack VPID of next page as NULL OID and count as 0 */
  VPID_SET_NULL (&dropped_cls_btid_page->next_page);
  dropped_cls_btid_page->n_dropped_entries = 0;
  /* Set dirty page and free */
  vacuum_set_dirty_dropped_entries_page (thread_p, dropped_cls_btid_page,
					 FREE);

  /* Initialize dropped indexes */
  if (file_find_nthpages (thread_p, dropped_indexes_vfid, &vpid, 0, 1) < 0)
    {
      assert (false);
      return ER_FAILED;
    }
  dropped_cls_btid_page =
    vacuum_fix_dropped_entries_page (thread_p, &vpid, PGBUF_LATCH_WRITE);
  if (dropped_cls_btid_page == NULL)
    {
      assert (false);
      return ER_FAILED;
    }
  /* Pack VPID of next page as NULL OID and count as 0 */
  VPID_SET_NULL (&dropped_cls_btid_page->next_page);
  dropped_cls_btid_page->n_dropped_entries = 0;
  /* Set dirty page and free */
  vacuum_set_dirty_dropped_entries_page (thread_p, dropped_cls_btid_page,
					 FREE);

  return NO_ERROR;
}


/*
 * vacuum_set_vacuum_data_lsa () - Called by log manager, sets vacuum data log
 *				   lsa whenever changes on vacuum data are
 *				   logged.
 *
 * return		: Void.
 * thread_p (in)	: Thread entry.
 * vacuum_data_lsa (in) : Log lsa for the latest change on vacuum data.
 */
void
vacuum_set_vacuum_data_lsa (THREAD_ENTRY * thread_p,
			    LOG_LSA * vacuum_data_lsa)
{
  if (prm_get_bool_value (PRM_ID_DISABLE_VACUUM))
    {
      return;
    }
  assert (vacuum_data_lsa != NULL);
  LSA_COPY (&vacuum_Data->crt_lsa, vacuum_data_lsa);
}

/*
 * vacuum_get_vacuum_data_lsa () - Called by log manager to check vacuum data
 *				   lsa for recovery.
 *
 * return		 : Void.
 * thread_p (in)	 : Thread entry.
 * vacuum_data_lsa (out) : Pointer to log lsa where vacuum data lsa is saved.
 */
void
vacuum_get_vacuum_data_lsa (THREAD_ENTRY * thread_p,
			    LOG_LSA * vacuum_data_lsa)
{
  if (prm_get_bool_value (PRM_ID_DISABLE_VACUUM))
    {
      LSA_SET_NULL (vacuum_data_lsa);
      return;
    }
  assert (vacuum_data_lsa != NULL);
  LSA_COPY (vacuum_data_lsa, &vacuum_Data->crt_lsa);
}

/*
 * vacuum_is_work_in_progress () - Returns true if there are any vacuum jobs
 *				   running.
 *
 * return	 : True if there is any job in progress, false otherwise.
 * thread_p (in) : Thread entry.
 *
 * NOTE: If this is not called by the auto vacuum master thread, it is
 *	 recommended to obtain lock on vacuum data first.
 */
static bool
vacuum_is_work_in_progress (THREAD_ENTRY * thread_p)
{
  int i;
  VACUUM_DATA_ENTRY *entry;

  if (vacuum_Data == NULL)
    {
      /* Vacuum data was not loaded */
      return false;
    }

  for (i = 0; i < vacuum_Data->n_table_entries; i++)
    {
      entry = VACUUM_DATA_GET_ENTRY (i);
      if (VACUUM_BLOCK_STATUS_IS_RUNNING (entry))
	{
	  /* Found a running job, return true */
	  return true;
	}
      else if (VACUUM_BLOCK_STATUS_IS_REQUESTED (entry))
	{
	  VACUUM_BLOCK_STATUS_SET_AVAILABLE (entry);
	}
    }
  /* No running jobs, return false */
  return false;
}

/*
 * vacuum_data_remove_entries () - Remove given vacuum data entries.
 *
 * return		  : Void.
 * thread_p (in)	  : Thread entry.
 * n_removed_entries (in) : Number of entries to be removed.
 * removed_entries (in)	  : Indexes of entries to be removed.
 *
 * NOTE: Another task besides removing entries from vacuum data is to update
 *	 the oldest vacuum data MVCCID.
 */
static void
vacuum_data_remove_entries (THREAD_ENTRY * thread_p, int n_removed_entries,
			    int * removed_entries)
{
#define TEMP_BUFFER_SIZE 1024
  VACUUM_DATA_ENTRY temp_buffer[TEMP_BUFFER_SIZE], *entry = NULL;
  int mem_size = 0, temp_buffer_mem_size = sizeof (temp_buffer);
  int i, table_index;
  int start_index = 0, n_successive = 1;
  bool update_oldest_mvccid = (vacuum_Data->oldest_mvccid == MVCCID_NULL);

  if (n_removed_entries == 0)
    {
      return;
    }

  for (i = 0; i < n_removed_entries; i++)
    {
      /* Get table index of entry being removed */
      table_index = removed_entries[i];

      /* Make sure that indexes in removed entries are in descending order */
      assert (i == (n_removed_entries - 1)
	      || table_index > removed_entries[i + 1]);

      /* Get entry at table_index */
      entry = VACUUM_DATA_GET_ENTRY (table_index);
      /* If entry oldest MVCCID is equal to vacuum data oldest MVCCID, this
       * may need to be updated.
       */
      update_oldest_mvccid =
	update_oldest_mvccid
	|| (vacuum_Data->oldest_mvccid ==
	    VACUUM_DATA_ENTRY_OLDEST_MVCCID (entry));

      if (i < n_removed_entries - 1
	  && table_index == removed_entries[i + 1] + 1)
	{
	  /* Successive entries are being removed. Group their removal. */
	  n_successive++;
	  continue;
	}
      /* Get starting index for current group. If no successive entries were
       * found, starting index will be same as table index and n_successive
       * will be 1.
       */
      start_index = table_index;

      /* Compute the size of memory data being moved */
      mem_size = (vacuum_Data->n_table_entries - (start_index + n_successive))
	* sizeof (VACUUM_DATA_ENTRY);
      assert (mem_size >= 0);
      if (mem_size > 0)
	{
	  /* Move memory data */
	  if (mem_size <= temp_buffer_mem_size)
	    {
	      /* Use temporary buffer to move memory data */
	      memcpy (temp_buffer,
		      VACUUM_DATA_GET_ENTRY (start_index + n_successive),
		      mem_size);
	      memcpy (VACUUM_DATA_GET_ENTRY (start_index), temp_buffer,
		      mem_size);
	    }
	  else
	    {
	      /* Use memmove */
	      memmove (VACUUM_DATA_GET_ENTRY (start_index),
		       VACUUM_DATA_GET_ENTRY (start_index + n_successive),
		       mem_size);
	    }
	}
      vacuum_Data->n_table_entries -= n_successive;

      /* Reset successive removed entries to 1 */
      n_successive = 1;
    }

  if (update_oldest_mvccid)
    {
      vacuum_update_oldest_mvccid (thread_p);
    }
}

/*
 * vacuum_data_remove_finished_entries () - Remove vacuumed entries from
 *					    vacuum data table.
 *
 * return	 : Void.
 * thread_p (in) : Index of entry being removed.
 */
static void
vacuum_data_remove_finished_entries (THREAD_ENTRY * thread_p)
{
#define TEMP_BUFFER_SIZE 1024
  int removed_indexes[TEMP_BUFFER_SIZE];
  int index, n_removed_indexes = 0;
  int n_removed_indexes_capacity = TEMP_BUFFER_SIZE;
  int *removed_indexes_p = NULL, *new_removed_indexes = NULL;
  int mem_size;

  removed_indexes_p = removed_indexes;

  /* Search for vacuumed blocks and remove them for vacuum data */
  for (index = vacuum_Data->n_table_entries - 1; index >= 0; index--)
    {
      if (VACUUM_BLOCK_STATUS_IS_VACUUMED (VACUUM_DATA_GET_ENTRY (index)))
	{
	  /* Save index of entry to be removed */
	  if (n_removed_indexes >= n_removed_indexes_capacity)
	    {
	      /* Realloc removed indexes buffer */
	      mem_size = sizeof (int) * 2 * n_removed_indexes_capacity;
	      if (removed_indexes == removed_indexes_p)
		{
		  new_removed_indexes = (int *) malloc (mem_size);
		}
	      else
		{
		  new_removed_indexes =
		    (int *) realloc (removed_indexes_p, mem_size);
		}
	      if (new_removed_indexes == NULL)
		{
		  assert_release (false);
		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			  ER_OUT_OF_VIRTUAL_MEMORY, 1,
			  sizeof (int) * 2 * n_removed_indexes_capacity);
		  goto end;
		}
	      removed_indexes_p = new_removed_indexes;
	      n_removed_indexes_capacity *= 2;
	    }
	  removed_indexes_p[n_removed_indexes++] = index;
	}
    }

  if (n_removed_indexes > 0)
    {
      /* Remove entries from vacuum data */
      vacuum_data_remove_entries (thread_p, n_removed_indexes,
				  removed_indexes_p);

      /* Log removed data entries */
      vacuum_log_remove_data_entries (thread_p, removed_indexes_p,
				      n_removed_indexes);
    }

end:
  if (removed_indexes_p != removed_indexes)
    {
      free (removed_indexes_p);
    }
}

/*
 * vacuum_consume_buffer_log_blocks () - Append new blocks from log block
 *					     data from buffer (if any).
 *
 * return	 : Void.
 * thread_p (in) : Thread entry.
 *
 * NOTE: In order to avoid synchronizing access on vacuum data for log
 *	 manager, information on new blocks is appended into a lock-free
 *	 buffer. This information can be later obtained and appended to
 *	 vacuum data.
 *	 The function can be used under two circumstances:
 *	 1. During the auto-vacuum process, when expected blocks are in
 *	    ascending order.
 *	 2. During recovery after crash, when the lost buffer is rebuild.
 *	    Some blocks may already exist in the recovered vacuum data.
 *	    Duplicates are simply ignored.
 */
void
vacuum_consume_buffer_log_blocks (THREAD_ENTRY * thread_p,
				  bool ignore_duplicates)
{
  int save_n_entries;
  VACUUM_DATA_ENTRY *entry = NULL;

  if (prm_get_bool_value (PRM_ID_DISABLE_VACUUM))
    {
      return;
    }

  /* Consume blocks data from buffer and append them to vacuum data */
  save_n_entries = vacuum_Data->n_table_entries;
  do
    {
      /* The vacuum data capacity should cover all blocks */
      assert (vacuum_Data->n_table_entries < VACUUM_DATA_TABLE_MAX_SIZE
	      ||
	      LOCK_FREE_CIRCULAR_QUEUE_IS_EMPTY (vacuum_Block_data_buffer));
      /* Position entry pointer at the end of the vacuum data table */
      entry = VACUUM_DATA_GET_ENTRY (vacuum_Data->n_table_entries);
      /* Get a block from buffer */
      if (!lock_free_circular_queue_pop (vacuum_Block_data_buffer, entry))
	{
	  /* Buffer is empty */
	  break;
	}
      if (VACUUM_DATA_ENTRY_BLOCKID (entry) <= vacuum_Data->last_blockid)
	{
	  if (ignore_duplicates)
	    {
	      /* Ignore duplicates */
	      continue;
	    }
	  else
	    {
	      /* Duplicates are not expected, something is wrong */
	      assert (false);
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_GENERIC_ERROR, 0);
	      return;
	    }
	}
      vacuum_er_log (VACUUM_ER_LOG_MASTER | VACUUM_ER_LOG_VACUUM_DATA,
		     "VACUUM: thread(%d) calls: "
		     "vacuum_consume_buffer_log_blocks ():"
		     "blockid=(%lld) start_lsa=(%lld, %d) old_mvccid=(%llu)"
		     "new_mvccid=(%llu)\n",
		     thread_get_current_entry_index (),
		     entry->blockid, (long long int) entry->start_lsa.pageid,
		     (int) entry->start_lsa.offset, entry->oldest_mvccid,
		     entry->newest_mvccid);
      vacuum_Data->n_table_entries++;
      assert (vacuum_Data->last_blockid < entry->blockid);
      vacuum_Data->last_blockid = entry->blockid;
      if (mvcc_id_precedes (vacuum_Data->newest_mvccid, entry->newest_mvccid))
	{
	  vacuum_Data->newest_mvccid = entry->newest_mvccid;
	}
    }
  while (true);

  if (save_n_entries < vacuum_Data->n_table_entries)
    {
      if (save_n_entries == 0)
	{
	  /* Update oldest vacuum data MVCCID */
	  vacuum_update_oldest_mvccid (thread_p);
	}

      /* New blocks have been appended and must be logged */
      vacuum_log_append_block_data (thread_p,
				    VACUUM_DATA_GET_ENTRY (save_n_entries),
				    vacuum_Data->n_table_entries -
				    save_n_entries);
    }
}

/*
 * vacuum_consume_dropped_class_buffer_log_blocks () - Append new blocks from
 *                                  log block data from buffer (if any).
 *
 * return	 : Void.
 * thread_p (in) : Thread entry.
 *
 */
static void
vacuum_consume_dropped_class_buffer_log_blocks (THREAD_ENTRY * thread_p)
{
  int i = 0;
  VACUUM_DROPPED_ENTRY *drop_data_entry = NULL;

  if (vacuum_Dropped_class_buffer == NULL
      || vacuum_Dropped_class_buffer->dropped_entries == NULL
      || vacuum_Dropped_class_buffer->n_dropped_entries == 0)
    {
      return;
    }

  (void) pthread_mutex_lock (&vacuum_Dropped_class_buffer->mutex);

  /* Consume blocks data from buffer and append them to log */
  for (i = 0; i < vacuum_Dropped_class_buffer->n_dropped_entries; i++)
    {
      drop_data_entry = &vacuum_Dropped_class_buffer->dropped_entries[i];
      if (drop_data_entry == NULL)
	{
	  pthread_mutex_unlock (&vacuum_Dropped_class_buffer->mutex);
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_GENERIC_ERROR, 0);
	  return;
	}

      vacuum_add_dropped_class (thread_p,
				&drop_data_entry->dropped_cls_btid.class_oid,
				drop_data_entry->mvccid);
    }
  vacuum_Dropped_class_buffer->n_dropped_entries = 0;
  pthread_mutex_unlock (&vacuum_Dropped_class_buffer->mutex);

  return;
}

/*
 * vacuum_consume_dropped_index_buffer_log_blocks () - Append new blocks from
 *                                  log block data from buffer (if any).
 *
 * return	 : Void.
 * thread_p (in) : Thread entry.
 *
 */
static void
vacuum_consume_dropped_index_buffer_log_blocks (THREAD_ENTRY * thread_p)
{
  int i = 0;
  VACUUM_DROPPED_ENTRY *drop_data_entry = NULL;

  if (vacuum_Dropped_index_buffer == NULL
      || vacuum_Dropped_index_buffer->dropped_entries == NULL
      || vacuum_Dropped_index_buffer->n_dropped_entries == 0)
    {
      return;
    }

  (void) pthread_mutex_lock (&vacuum_Dropped_index_buffer->mutex);

  /* Consume blocks data from buffer and append them to log */
  for (i = 0; i < vacuum_Dropped_index_buffer->n_dropped_entries; i++)
    {
      drop_data_entry = &vacuum_Dropped_index_buffer->dropped_entries[i];
      if (drop_data_entry == NULL)
	{
	  pthread_mutex_unlock (&vacuum_Dropped_index_buffer->mutex);
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_GENERIC_ERROR, 0);
	  return;
	}

      vacuum_add_dropped_index (thread_p,
				&drop_data_entry->dropped_cls_btid.btid,
				drop_data_entry->mvccid);
    }
  vacuum_Dropped_index_buffer->n_dropped_entries = 0;
  pthread_mutex_unlock (&vacuum_Dropped_index_buffer->mutex);

  return;
}

/*
 * vacuum_data_get_first_log_pageid () - Get the first pageid in first block
 *					 found in vacuum data. If vacuum has
 *					 no entries return NULL_PAGEID.
 *
 * return	 : LOG Page identifier for first log page that should be
 *		   processed by vacuum.
 * thread_p (in) : Thread entry.
 */
LOG_PAGEID
vacuum_data_get_first_log_pageid (THREAD_ENTRY * thread_p)
{
  /* Return first pageid from first block in vacuum data table */
  VACUUM_LOG_BLOCKID blockid = VACUUM_NULL_LOG_BLOCKID;
  if (prm_get_bool_value (PRM_ID_DISABLE_VACUUM))
    {
      return NULL_PAGEID;
    }
  if (vacuum_Data->n_table_entries == 0)
    {
      /* No entries, no log pageid */
      return NULL_PAGEID;
    }
  /* Get blockid of first entry in vacuum data table */
  blockid = VACUUM_DATA_ENTRY_BLOCKID (vacuum_Data->vacuum_data_table);
  /* Return first pageid for blockid */
  return VACUUM_FIRST_LOG_PAGEID_IN_BLOCK (blockid);
}

/*
 * vacuum_data_get_last_log_pageid () - Get the last pageid in the last block
 *					found in vacuum data. Used for
 *					recovery.
 *
 * return	 : The page identifier for the last log page which is has
 *		   the statistics stored in vacuum data.
 * thread_p (in) : Thread entry.
 *
 * NOTE: Used for recovery (to know where to start the vacuum data recovery).
 */
LOG_PAGEID
vacuum_data_get_last_log_pageid (THREAD_ENTRY * thread_p)
{
  /* Return last pageid from last block in vacuum data table */
  if (prm_get_bool_value (PRM_ID_DISABLE_VACUUM))
    {
      return NULL_PAGEID;
    }
  assert (vacuum_Data != NULL);
  return VACUUM_LAST_LOG_PAGEID_IN_BLOCK (vacuum_Data->last_blockid);
}

/*
 * vacuum_log_remove_data_entries () - Log when an entry is removed from vacuum data
 *				(after being vacuumed).
 *
 * return		  : Void.
 * thread_p (in)	  : Thread entry.
 * removed_indexes (in)	  : Indexes of removed vacuum data entries.
 * n_removed_indexes (in) : Removed entries number.
 */
static void
vacuum_log_remove_data_entries (THREAD_ENTRY * thread_p,
				int * removed_indexes,
				int n_removed_indexes)
{
#define MAX_LOG_DISCARD_BLOCK_DATA_CRUMBS 2

  VFID null_vfid;
  LOG_DATA_ADDR addr;
  LOG_CRUMB redo_crumbs[MAX_LOG_DISCARD_BLOCK_DATA_CRUMBS];
  int n_redo_crumbs = 0;

  /* Initialize addr */
  addr.pgptr = NULL;
  addr.offset = 0;
  VFID_SET_NULL (&null_vfid);
  addr.vfid = &null_vfid;

  /* Append the number of removed blocks to crumbs */
  redo_crumbs[n_redo_crumbs].data = &n_removed_indexes;
  redo_crumbs[n_redo_crumbs].length = sizeof (n_removed_indexes);
  n_redo_crumbs++;

  /* Append removed blocks indexes to crumbs */
  redo_crumbs[n_redo_crumbs].data = removed_indexes;
  redo_crumbs[n_redo_crumbs].length =
    n_removed_indexes * sizeof (*removed_indexes);
  n_redo_crumbs++;

  /* Safeguard check */
  assert (n_redo_crumbs <= MAX_LOG_DISCARD_BLOCK_DATA_CRUMBS);

  /* Append redo log record */
  log_append_redo_crumbs (thread_p, RVVAC_LOG_BLOCK_REMOVE, &addr,
			  n_redo_crumbs, redo_crumbs);

  VACUUM_UPDATE_OLDEST_NOT_FLUSHED_LSA ();
}

/*
 * vacuum_rv_redo_remove_data_entries () - Redo removing entry from vacuum data.
 *
 * return	 : Error code.
 * thread_p (in) : Thread entry.
 * rcv (in)	 : Log recovery data.
 */
int
vacuum_rv_redo_remove_data_entries (THREAD_ENTRY * thread_p, LOG_RCV * rcv)
{
  int *removed_entries = NULL;
  int n_removed_entries;
  int offset = 0;

  if (vacuum_Data == NULL)
    {
      /* Vacuum data is not initialized */
      assert (0);
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_GENERIC_ERROR, 0);
      return ER_GENERIC_ERROR;
    }

  /* Get the number of removed entries */
  n_removed_entries = *((int *) rcv->data);
  offset += sizeof (n_removed_entries);

  /* Get the removed entries indexes */
  removed_entries = (int *) (rcv->data + offset);
  offset += sizeof (*removed_entries) * n_removed_entries;

  /* Safeguard */
  assert (rcv->length == offset);

  if (VACUUM_IS_ER_LOG_LEVEL_SET (VACUUM_ER_LOG_RECOVERY
				  | VACUUM_ER_LOG_VACUUM_DATA))
    {
      int i;
      VACUUM_DATA_ENTRY *entry = NULL;
      for (i = 0; i < n_removed_entries; i++)
	{
	  entry = VACUUM_DATA_GET_ENTRY (removed_entries[i]);
	  vacuum_er_log (VACUUM_ER_LOG_RECOVERY | VACUUM_ER_LOG_VACUUM_DATA,
			 "Recovery vacuum data: remove block = "
			 "{start_lsa=(%d, %d), oldest_blockid=%lld, "
			 "newest_blockid=%lld, blockid=%lld}.",
			 (long long int) entry->start_lsa.pageid,
			 (int) entry->start_lsa.offset,
			 entry->oldest_mvccid, entry->newest_mvccid,
			 entry->blockid);
	}
    }

  /* Is lock required during recovery? */
  VACUUM_LOCK_DATA ();

  vacuum_data_remove_entries (thread_p, n_removed_entries, removed_entries);

  VACUUM_UNLOCK_DATA ();

  return NO_ERROR;
}

/*
 * vacuum_log_append_block_data () - Log append of new blocks data.
 *
 * return	   : Void.
 * thread_p (in)   : Thread entry.
 * data (in) : Appended block data.
 */
static void
vacuum_log_append_block_data (THREAD_ENTRY * thread_p,
			      VACUUM_DATA_ENTRY * new_entries,
			      int n_new_entries)
{
#define MAX_LOG_APPEND_BLOCK_DATA_CRUMBS 1

  VFID null_vfid;
  LOG_DATA_ADDR addr;
  LOG_CRUMB redo_crumbs[MAX_LOG_APPEND_BLOCK_DATA_CRUMBS];
  int n_redo_crumbs = 0;

  /* Initialize addr */
  addr.pgptr = NULL;
  addr.offset = 0;
  VFID_SET_NULL (&null_vfid);
  addr.vfid = &null_vfid;

  /* Append block data to crumbs */
  redo_crumbs[n_redo_crumbs].data = new_entries;
  redo_crumbs[n_redo_crumbs].length = sizeof (*new_entries) * n_new_entries;
  n_redo_crumbs++;

  assert (n_redo_crumbs <= MAX_LOG_APPEND_BLOCK_DATA_CRUMBS);

  log_append_redo_crumbs (thread_p, RVVAC_LOG_BLOCK_APPEND, &addr,
			  n_redo_crumbs, redo_crumbs);

  VACUUM_UPDATE_OLDEST_NOT_FLUSHED_LSA ();
}

/*
 * vacuum_rv_redo_append_block_data () - Redo append new blocks data.
 *
 * return	 : Error code.
 * thread_p (in) : Thread entry.
 * rcv (in)	 : Recovery data.
 */
int
vacuum_rv_redo_append_block_data (THREAD_ENTRY * thread_p, LOG_RCV * rcv)
{
  int n_entries, i;

  if (vacuum_Data == NULL)
    {
      /* Vacuum data is not initialized */
      assert (0);
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_GENERIC_ERROR, 0);
      return ER_GENERIC_ERROR;
    }

  /* rcv->data contains only VACUUM_DATA_ENTRY entries */
  n_entries = rcv->length / sizeof (VACUUM_DATA_ENTRY);

  /* Is lock required during recovery? */
  VACUUM_LOCK_DATA ();
  /* Append new blocks at the end of log block table */
  memcpy (VACUUM_DATA_GET_ENTRY (vacuum_Data->n_table_entries), rcv->data,
	  rcv->length);
  /* Update log block table number of entries */
  vacuum_Data->n_table_entries += n_entries;

  /* Update last_blockid as the blockid of the last entry */
  vacuum_Data->last_blockid =
    VACUUM_DATA_ENTRY_BLOCKID (VACUUM_DATA_GET_ENTRY
			       (vacuum_Data->n_table_entries - 1));

  /* Update newest MVCCID */
  for (i = vacuum_Data->n_table_entries - n_entries;
       i < vacuum_Data->n_table_entries; i++)
    {
      vacuum_er_log (VACUUM_ER_LOG_RECOVERY | VACUUM_ER_LOG_VACUUM_DATA,
		     "Recovery vacuum data: append block = "
		     "{start_lsa=(%lld, %d), oldest_mvccid=%lld, "
		     "newest_mvccid=%lld, blockid=%lld",
		     (long long int)
		     (VACUUM_DATA_GET_ENTRY (i)->start_lsa.pageid),
		     (int) (VACUUM_DATA_GET_ENTRY (i)->start_lsa.offset),
		     VACUUM_DATA_GET_ENTRY (i)->oldest_mvccid,
		     VACUUM_DATA_GET_ENTRY (i)->newest_mvccid,
		     VACUUM_DATA_GET_ENTRY (i)->blockid);
      if (mvcc_id_precedes (vacuum_Data->newest_mvccid,
			    VACUUM_DATA_ENTRY_NEWEST_MVCCID
			    (VACUUM_DATA_GET_ENTRY (i))))
	{
	  vacuum_Data->newest_mvccid =
	    VACUUM_DATA_ENTRY_NEWEST_MVCCID (VACUUM_DATA_GET_ENTRY (i));
	}
    }
  VACUUM_UNLOCK_DATA ();

  return NO_ERROR;
}

/*
 * vacuum_log_update_block_data () - Log update vacuum data for a block.
 *
 * return	   : 
 * thread_p (in)   :
 * data (in) :
 * blockid (in)	   :
 */
static void
vacuum_log_update_block_data (THREAD_ENTRY * thread_p,
			      VACUUM_DATA_ENTRY * block_data)
{
#define MAX_LOG_UPDATE_BLOCK_DATA_CRUMBS 1

  VFID null_vfid;
  LOG_DATA_ADDR addr;
  LOG_CRUMB redo_crumbs[MAX_LOG_UPDATE_BLOCK_DATA_CRUMBS];
  int n_redo_crumbs = 0;

  /* Initialize addr */
  addr.pgptr = NULL;
  addr.offset = 0;
  VFID_SET_NULL (&null_vfid);
  addr.vfid = &null_vfid;

  /* Add block data to crumbs */
  redo_crumbs[n_redo_crumbs].data = &block_data;
  redo_crumbs[n_redo_crumbs].length = sizeof (*block_data);
  n_redo_crumbs++;

  /* Add block blockid to crumbs */

  /* Safety check */
  assert (n_redo_crumbs <= MAX_LOG_UPDATE_BLOCK_DATA_CRUMBS);

  log_append_redo_crumbs (thread_p, RVVAC_LOG_BLOCK_MODIFY, &addr,
			  n_redo_crumbs, redo_crumbs);

  VACUUM_UPDATE_OLDEST_NOT_FLUSHED_LSA ();
}

/*
 * vacuum_rv_redo_update_block_data () - Redo updating data for one block in
 *					 in vacuum data.
 *
 * return	 : Error code.
 * thread_p (in) : Thread entry.
 * rcv (in)	 : Recovery data.
 */
int
vacuum_rv_redo_update_block_data (THREAD_ENTRY * thread_p, LOG_RCV * rcv)
{
  VACUUM_DATA_ENTRY *table_entry;
  VACUUM_LOG_BLOCKID rv_blockid;
  int offset = 0;

  if (vacuum_Data == NULL)
    {
      /* Vacuum data is not initialized */
      assert (0);
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_GENERIC_ERROR, 0);
      return ER_GENERIC_ERROR;
    }

  /* Is lock required during recovery? */
  VACUUM_LOCK_DATA ();

  /* Get blockid */
  rv_blockid = *((VACUUM_LOG_BLOCKID *) rcv->data);
  offset += sizeof (rv_blockid);

  /* Recover table entry data */
  table_entry = vacuum_get_vacuum_data_entry (rv_blockid);
  memcpy (table_entry, rcv->data + offset, sizeof (*table_entry));
  offset += sizeof (*table_entry);

  /* Consistency check */
  assert (offset == rcv->length);

  VACUUM_UNLOCK_DATA ();

  return NO_ERROR;
}

/*
 * vacuum_update_oldest_mvccid () - Obtains the oldest mvccid from all blocks
 *				    found in vacuum data.
 *
 * return	 : Void.
 * thread_p (in) : Update oldest MVCCID.
 */
static void
vacuum_update_oldest_mvccid (THREAD_ENTRY * thread_p)
{
  int i;
  MVCCID oldest_mvccid = MVCCID_NULL;

  if (vacuum_Data->n_table_entries == 0)
    {
      return;
    }

  oldest_mvccid = VACUUM_DATA_ENTRY_OLDEST_MVCCID (VACUUM_DATA_GET_ENTRY (0));
  for (i = 1; i < vacuum_Data->n_table_entries; i++)
    {
      if (mvcc_id_precedes (VACUUM_DATA_ENTRY_OLDEST_MVCCID
			    (VACUUM_DATA_GET_ENTRY (i)), oldest_mvccid))
	{
	  oldest_mvccid =
	    VACUUM_DATA_ENTRY_OLDEST_MVCCID (VACUUM_DATA_GET_ENTRY (i));
	}
    }
  if (vacuum_Data->oldest_mvccid != oldest_mvccid)
    {
      /* Oldest MVCCID has changed. Update it and run cleanup on dropped
       * classes/indexes.
       */
      vacuum_Data->oldest_mvccid = oldest_mvccid;
      vacuum_cleanup_dropped_cls_btid (thread_p, DROPPED_CLASS);
      vacuum_cleanup_dropped_cls_btid (thread_p, DROPPED_INDEX);
    }
}

/*
 * vacuum_compare_dropped_cls_btid () - Compare function used for dropped
 *					classes and indexes.
 *
 * return    : Positive if the first argument is bigger, negative if it is
 *	       smaller and 0 if arguments are equal.
 * a (in)    : Pointer to a class OID or to a BTID.
 * b (in)    : Pointer to a class OID or to a BTID.
 * type (in) : Dropped class or dropped index.
 */
static int
vacuum_compare_dropped_cls_btid (const void *a, const void *b,
				 DROPPED_TYPE type)
{
  assert (a != NULL && b != NULL);
  assert (type == DROPPED_CLASS || type == DROPPED_INDEX);

  if (type == DROPPED_CLASS)
    {
      return oid_compare (a, b);
    }
  else
    {
      return btree_compare_btids (a, b);
    }
}

/*
 * vacuum_add_dropped_class () - Tracks newly dropped classes.
 *
 * return	  : Error code.
 * thread_p (in)  : Thread entry.
 * class_oid (in) : Class identifier.
 * mvccid (in)	  : MVCCID for the transaction which dropped the class.
 *
 * NOTE: Vacuum keeps track of recently dropped classes so it can skip
 *	 vacuuming their heap files and b-trees. Once all the records
 *	 belonging to this class have been vacuumed or skipped, the class
 *	 can be removed.
 */
int
vacuum_add_dropped_class (THREAD_ENTRY * thread_p, OID * class_oid,
			  MVCCID mvccid)
{
  return vacuum_add_dropped_cls_btid (thread_p, class_oid, mvccid,
				      DROPPED_CLASS);
}

/*
 * vacuum_add_dropped_index () - Tracks newly dropped indexes.
 *
 * return	  : Error code.
 * thread_p (in)  : Thread entry.
 * btidp (in)	  : B-tree identifier.
 * mvccid (in)	  : MVCCID for the transaction which dropped the index.
 */
int
vacuum_add_dropped_index (THREAD_ENTRY * thread_p, BTID * btidp,
			  MVCCID mvccid)
{
  return vacuum_add_dropped_cls_btid (thread_p, btidp, mvccid, DROPPED_INDEX);
}

/*
 * vacuum_add_dropped_cls_btid () - Add new dropped class/index.
 *
 * return	 : Error code.
 * thread_p (in) : Thread entry.
 * cls_btid (in) : Class OID or B-tree identifier.
 * mvccid (in)	 : MVCCID.
 * type (in)	 : Dropped class or dropped index.
 */
static int
vacuum_add_dropped_cls_btid (THREAD_ENTRY * thread_p, void *cls_btid,
			     MVCCID mvccid, DROPPED_TYPE type)
{
  VPID vpid, prev_vpid;
  INT32 *countp = VACUUM_DROPPED_ENTRIES_COUNT_PTR (type);
  int page_count, mem_size, npages = 0, compare;
  char *ptr = NULL;
  char *temp_buf = VACUUM_DROPPED_ENTRIES_TMP_BUFFER (type);
  VFID *file_vfidp = NULL;
  VACUUM_DROPPED_ENTRIES_PAGE *page = NULL;
  INT16 min, max, mid, position;

#if !defined (NDEBUG)
  VACUUM_TRACK_DROPPED_ENTRIES *track_page =
    VACUUM_DROPPED_CLS_BTID_TRACK_PTR (type);
  VACUUM_TRACK_DROPPED_ENTRIES *new_track_page = NULL;
#endif

  VPID_COPY (&vpid, VACUUM_DROPPED_ENTRIES_VPID_PTR (type));
  while (!VPID_ISNULL (&vpid))
    {
      /* Unfix previous page */
      if (page != NULL)
	{
	  vacuum_unfix_dropped_entries_page (thread_p, page);
	}
      /* Fix current page */
      page =
	vacuum_fix_dropped_entries_page (thread_p, &vpid, PGBUF_LATCH_WRITE);
      if (page == NULL)
	{
	  assert (false);
	  return ER_FAILED;
	}
      /* Save current vpid to prev_vpid */
      VPID_COPY (&prev_vpid, &vpid);
      /* Get next vpid and page count */
      VPID_COPY (&vpid, &page->next_page);
      page_count = page->n_dropped_entries;

      /* Dropped entries must be ordered. Look for the right position for the
       * new entry.
       * The algorithm considers possible to have duplicate values in case of
       * dropped indexes, because btid may be reused. Although unlikely, it
       * is theoretically possible to drop index with same btid in a
       * relatively short time, without being removed from the list of dropped
       * indexes.
       *
       * Set position variable for adding new dropped entry.
       */
      if (page_count > 0)
	{
	  if (VACUUM_DROPPED_ENTRIES_PAGE_CAPACITY <= page_count)
	    {
	      /* No room left for new entries, try next page */
	      npages++;
#if !defined (NDEBUG)
	      if (!VPID_ISNULL (&vpid))
		{
		  /* Don't advance from last track page. A new page will be
		   * added and we need to set a link between last track page
		   * and new track page.
		   */
		  track_page = track_page->next_tracked_page;
		}
#endif
	      continue;
	    }
	  /* Dropped entries are kept in ascending order */
	  /* Use a binary search to find the right position for the new
	   * dropped entry. If a duplicate is found, just replace the previous
	   * MVCCID.
	   */
	  min = 0;
	  max = page_count;
	  /* Initialize compare with a non-zero value. If page_count is 1,
	   * the while loop is skipped and then we must compare with the new
	   * entry with the only existing value (which is only done if compare
	   * is not 0).
	   */
	  compare = -1;
	  while ((min + 1) < max)
	    {
	      /* Stop when next mid is the same with min */
	      mid = (min + max) / 2;

	      /* Compare with mid value */
	      compare =
		vacuum_compare_dropped_cls_btid (cls_btid,
						 &page->dropped_entries[mid].
						 dropped_cls_btid, type);
	      if (compare == 0)
		{
		  /* Duplicate found, break loop */
		  break;
		}
	      if (compare < 0)
		{
		  /* Keep searching in the min-mid range */
		  max = mid;
		}
	      else		/* compare > 0 */
		{
		  /* Keep searching in mid-max range */
		  min = mid;
		}
	    }
	  if (compare != 0 && min == 0)
	    {
	      /* This code can be reached in two cases:
	       * 1. There was previously only one value and the while loop was
	       *    skipped.
	       * 2. All compared entries have been bigger than the new value.
	       *    The last remaining range is min = 0, max = 1 and the new
	       *    value still must be compared with the first entry.
	       */
	      compare =
		vacuum_compare_dropped_cls_btid (cls_btid,
						 &page->dropped_entries[0].
						 dropped_cls_btid, type);
	      if (compare == 0)
		{
		  /* Set mid to 0 to replace the MVCCID */
		  mid = 0;
		}
	      else if (compare < 0)
		{
		  /* Add new entry before all existing entries */
		  position = 0;
		}
	      else		/* compare > 0 */
		{
		  /* Add new entry after the first entry */
		  position = 1;
		}
	    }
	  else
	    {
	      /* min is certainly smaller the the new entry. max is either
	       * bigger or is the equal to the number of existing entries.
	       * The position of the new entry must be max.
	       */
	      position = max;
	    }
	  if (compare == 0)
	    {
	      /* Same entry was already dropped, replace previous MVCCID */
	      /* The equal entry must be at the current mid value */
	      if (type != DROPPED_INDEX)
		{
		  /* this case is possible when you create and drop the same
		   * class multiple times in the same transaction
		   */

		  vacuum_er_log (VACUUM_ER_LOG_WARNING
				 | VACUUM_ER_LOG_DROPPED_CLASSES,
				 "VACUUM ERROR: thread(%d): dropped class "
				 "duplicate: class_oid(%d, %d, %d)\n",
				 thread_get_current_entry_index (),
				 ((OID *) cls_btid)->volid,
				 ((OID *) cls_btid)->pageid,
				 ((OID *) cls_btid)->slotid);
		  vacuum_unfix_dropped_entries_page (thread_p, page);
		  return NO_ERROR;
		}

	      /* Replace MVCCID */
	      page->dropped_entries[mid].mvccid = mvccid;

	      vacuum_log_add_dropped_cls_btid (thread_p, (PAGE_PTR) page,
					       &page->dropped_entries[mid],
					       mid, type, true);

#if !defined (NDEBUG)
	      memcpy (&track_page->dropped_data_page, page, DB_PAGESIZE);
#endif
	      vacuum_er_log (VACUUM_ER_LOG_DROPPED_INDEXES,
			     "VACUUM: thread(%d): add dropped btid: found"
			     "duplicate btid(%d, %d %d) at position=%d, "
			     "replace mvccid=%lld\n",
			     thread_get_current_entry_index (),
			     page->dropped_entries[mid].dropped_cls_btid.btid.
			     root_pageid,
			     page->dropped_entries[mid].dropped_cls_btid.btid.
			     vfid.volid,
			     page->dropped_entries[mid].dropped_cls_btid.btid.
			     vfid.fileid, mid,
			     page->dropped_entries[mid].mvccid);
	      vacuum_set_dirty_dropped_entries_page (thread_p, page, FREE);
	      return NO_ERROR;
	    }
	}
      else
	{
	  /* Set position to 0 */
	  position = 0;
	}

      /* Add new entry at position */
      mem_size = (page_count - position) * sizeof (VACUUM_DROPPED_ENTRY);
      if (mem_size > 0)
	{
	  if (npages == 0)
	    {
	      /* Use temporary buffer to move data. */
	      /* Copy from max position to buffer. */
	      memcpy (temp_buf, &page->dropped_entries[position], mem_size);
	      /* Copy from buffer to (max + 1) position. */
	      memcpy (&page->dropped_entries[position + 1], temp_buf,
		      mem_size);
	    }
	  else
	    {
	      /* Buffer can be used only when exclusive latch on the first
	       * page is held. Use memmove instead.
	       */
	      memmove (&page->dropped_entries[position + 1],
		       &page->dropped_entries[position], mem_size);
	    }
	}
      /* Increment page count */
      page->n_dropped_entries++;
      /* Increment total count */
      ATOMIC_INC_32 (countp, 1);

      VACUUM_DROPPED_CLS_BTID_SET_VALUE (&page->dropped_entries[position].
					 dropped_cls_btid, cls_btid, type);
      page->dropped_entries[position].mvccid = mvccid;

      vacuum_log_add_dropped_cls_btid (thread_p, (PAGE_PTR) page,
				       &page->dropped_entries[position],
				       position, type, false);

#if !defined (NDEBUG)
      memcpy (&track_page->dropped_data_page, page, DB_PAGESIZE);
#endif
      if (type == DROPPED_CLASS)
	{
	  vacuum_er_log (VACUUM_ER_LOG_DROPPED_CLASSES,
			 "VACUUM: thread(%d): added new dropped "
			 "class(%d, %d, %d) and mvccid=%lld to page=%d at"
			 "position=%d\n", thread_get_current_entry_index (),
			 page->dropped_entries[position].dropped_cls_btid.
			 class_oid.volid,
			 page->dropped_entries[position].dropped_cls_btid.
			 class_oid.pageid,
			 page->dropped_entries[position].dropped_cls_btid.
			 class_oid.slotid,
			 page->dropped_entries[position].mvccid, npages,
			 position);
	}
      else
	{
	  vacuum_er_log (VACUUM_ER_LOG_DROPPED_INDEXES,
			 "VACUUM: thread(%d): added new dropped "
			 "index(%d, %d %d) and mvccid=%lld to page=%d at"
			 "position=%d\n", thread_get_current_entry_index (),
			 page->dropped_entries[position].dropped_cls_btid.
			 btid.root_pageid,
			 page->dropped_entries[position].dropped_cls_btid.
			 btid.vfid.volid,
			 page->dropped_entries[position].dropped_cls_btid.
			 btid.vfid.fileid,
			 page->dropped_entries[position].mvccid, npages,
			 position);
	}
      vacuum_set_dirty_dropped_entries_page (thread_p, page, FREE);
      return NO_ERROR;
    }
  /* The entry couldn't fit in any of the current pages. */
  /* Allocate a new page */
  /* Last page must be fixed */
  assert (page != NULL);

  file_vfidp = VACUUM_DROPPED_ENTRIES_VFID_PTR (type);
  if (npages >= file_get_numpages (thread_p, file_vfidp))
    {
      /* Extend file */
      if (file_alloc_pages (thread_p, file_vfidp, &vpid,
			    VACUUM_DROPPED_ENTRIES_EXTEND_FILE_NPAGES,
			    &prev_vpid, NULL, NULL) == NULL)
	{
	  assert (false);
	  vacuum_unfix_dropped_entries_page (thread_p, page);
	  return ER_FAILED;
	}
    }
  if (file_find_nthpages (thread_p, file_vfidp, &vpid, npages, 1) < 1)
    {
      /* Get next page in file */
      assert (false);
      vacuum_unfix_dropped_entries_page (thread_p, page);
      return ER_FAILED;
    }
  /* Save a link to the new page in last page */
  VPID_COPY (&page->next_page, &vpid);
  vacuum_log_set_next_page_dropped_cls_btid (thread_p, (PAGE_PTR) page, &vpid,
					     type);

#if !defined (NDEBUG)
  memcpy (&track_page->dropped_data_page, page, DB_PAGESIZE);
#endif
  vacuum_set_dirty_dropped_entries_page (thread_p, page, FREE);

  /* Add new entry to new page */
  page = vacuum_fix_dropped_entries_page (thread_p, &vpid, PGBUF_LATCH_WRITE);
  if (page == NULL)
    {
      assert (false);
      return ER_FAILED;
    }
  /* Set page header: next page as NULL and count as 1 */
  VPID_SET_NULL (&page->next_page);
  page->n_dropped_entries = 1;

  /* Set class OID or btidp */
  VACUUM_DROPPED_CLS_BTID_SET_VALUE (&page->dropped_entries[0].
				     dropped_cls_btid, cls_btid, type);
  /* Set MVCCID */
  page->dropped_entries[0].mvccid = mvccid;

  vacuum_log_add_dropped_cls_btid (thread_p, (PAGE_PTR) page,
				   &page->dropped_entries[0], 0, type, false);
#if !defined(NDEBUG)
  if (track_page->next_tracked_page == NULL)
    {
      new_track_page =
	(VACUUM_TRACK_DROPPED_ENTRIES *)
	malloc (VACUUM_TRACK_DROPPED_ENTRIES_SIZE);
      if (new_track_page == NULL)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY,
		  1, VACUUM_TRACK_DROPPED_ENTRIES_SIZE);
	  vacuum_unfix_dropped_entries_page (thread_p, page);
	  return ER_FAILED;
	}
    }
  else
    {
      new_track_page = track_page->next_tracked_page;
    }
  memcpy (&new_track_page->dropped_data_page, page, DB_PAGESIZE);
  new_track_page->next_tracked_page = NULL;
  track_page->next_tracked_page = new_track_page;
#endif

  if (type == DROPPED_CLASS)
    {
      vacuum_er_log (VACUUM_ER_LOG_DROPPED_CLASSES,
		     "VACUUM: thread(%d): added new dropped "
		     "class(%d, %d, %d) and mvccid=%lld to page=%d at"
		     "position=%d\n",
		     thread_get_current_entry_index (),
		     page->dropped_entries[0].dropped_cls_btid.
		     class_oid.volid,
		     page->dropped_entries[0].dropped_cls_btid.
		     class_oid.pageid,
		     page->dropped_entries[0].dropped_cls_btid.
		     class_oid.slotid,
		     page->dropped_entries[0].mvccid, npages, 0);
    }
  else
    {
      vacuum_er_log (VACUUM_ER_LOG_DROPPED_INDEXES,
		     "VACUUM: thread(%d): added new dropped "
		     "index(%d, %d %d) and mvccid=%lld to page=%d at"
		     "position=%d\n",
		     thread_get_current_entry_index (),
		     page->dropped_entries[0].dropped_cls_btid.
		     btid.root_pageid,
		     page->dropped_entries[0].dropped_cls_btid.
		     btid.vfid.volid,
		     page->dropped_entries[0].dropped_cls_btid.
		     btid.vfid.fileid,
		     page->dropped_entries[0].mvccid, npages, 0);
    }
  vacuum_set_dirty_dropped_entries_page (thread_p, page, FREE);
  return NO_ERROR;
}

/*
 * vacuum_cleanup_dropped_cls_btid () - Clean unnecessary dropped entries.
 *
 * return	 : Error code.
 * thread_p (in) : Thread entry.
 * type (in)	 : Dropped class or dropped index.
 *
 * NOTE: All entries with an MVCCID older than vacuum_Data->oldest_mvccid are
 *	 removed. All records belonging to these entries must be either
 *	 vacuumed or skipped after drop.
 */
static int
vacuum_cleanup_dropped_cls_btid (THREAD_ENTRY * thread_p, DROPPED_TYPE type)
{
  VPID vpid;
  VACUUM_DROPPED_ENTRIES_PAGE *page = NULL;
  int page_count, mem_size;
  char *temp_buffer = VACUUM_DROPPED_ENTRIES_TMP_BUFFER (type);
  INT32 *countp = VACUUM_DROPPED_ENTRIES_COUNT_PTR (type);
  VPID last_page_vpid, last_non_empty_page_vpid;
  INT16 removed_entries[VACUUM_DROPPED_ENTRIES_PAGE_MAX_CAPACITY];
  INT16 n_removed_entries, i;
  bool is_first_page = true;
#if !defined (NDEBUG)
  VACUUM_TRACK_DROPPED_ENTRIES *track_page =
    VACUUM_DROPPED_CLS_BTID_TRACK_PTR (type);
#endif

  assert (type == DROPPED_CLASS || type == DROPPED_INDEX);

  if ((*countp) == 0)
    {
      /* Nothing to clean */
      return NO_ERROR;
    }

  /* Clean each page of dropped entries */
  VPID_COPY (&vpid, VACUUM_DROPPED_ENTRIES_VPID_PTR (type));
  while (!VPID_ISNULL (&vpid))
    {
      /* Reset n_removed_entries */
      n_removed_entries = 0;

      /* Track the last page found */
      VPID_COPY (&last_page_vpid, &vpid);

      /* Fix current page */
      page =
	vacuum_fix_dropped_entries_page (thread_p, &vpid, PGBUF_LATCH_WRITE);
      if (page == NULL)
	{
	  assert (false);
	  return ER_FAILED;
	}
      /* Get next page VPID */
      VPID_COPY (&vpid, &page->next_page);
      page_count = page->n_dropped_entries;
      if (page_count == 0)
	{
	  /* Page is empty */
	  vacuum_unfix_dropped_entries_page (thread_p, page);
	  continue;
	}
      /* Page is not empty, track the last non-empty page found */
      VPID_COPY (&last_non_empty_page_vpid, &vpid);

      /* Check entries for cleaning. Start from the end of the array */
      for (i = page_count - 1; i >= 0; i--)
	{
	  if (mvcc_id_precedes (page->dropped_entries[i].mvccid,
				vacuum_Data->oldest_mvccid))
	    {
	      /* Remove entry */
	      removed_entries[n_removed_entries++] = i;
	      if (i < page_count - 1)
		{
		  mem_size =
		    (page_count - i - 1) * sizeof (VACUUM_DROPPED_ENTRY);
		  if (is_first_page)
		    {
		      /* User buffer to move data */
		      memcpy (temp_buffer, &page->dropped_entries[i + 1],
			      mem_size);
		      memcpy (&page->dropped_entries[i], temp_buffer,
			      mem_size);
		    }
		  else
		    {
		      /* Buffer can only be used if exclusive latch on first
		       * page is held. Use memmove instead.
		       */
		      memmove (&page->dropped_entries[i],
			       &page->dropped_entries[i + 1], mem_size);
		    }
		}
	    }
	}
      if (n_removed_entries > 0)
	{
	  /* Update dropped entries global counter */
	  ATOMIC_INC_32 (countp, -n_removed_entries);

	  /* Update dropped entries page counter */
	  page->n_dropped_entries -= n_removed_entries;

	  /* Log changes */
	  vacuum_log_cleanup_dropped_cls_btid (thread_p, (PAGE_PTR) page,
					       removed_entries,
					       n_removed_entries, type);

#if !defined (NDEBUG)
	  /* Copy changes to tracker */
	  memcpy (&track_page->dropped_data_page, page, DB_PAGESIZE);
#endif
	  vacuum_set_dirty_dropped_entries_page (thread_p, page, FREE);
	}
      else
	{
	  /* No changes */
	  vacuum_unfix_dropped_entries_page (thread_p, page);
	}

#if !defined (NDEBUG)
      track_page = track_page->next_tracked_page;
#endif
    }

  if (!VPID_ISNULL (&last_non_empty_page_vpid)
      && !VPID_EQ (&last_non_empty_page_vpid, &last_page_vpid))
    {
      /* Update next page link in the last non-empty page to NULL, to avoid
       * fixing empty pages in the future.
       */
      page =
	vacuum_fix_dropped_entries_page (thread_p, &last_non_empty_page_vpid,
					 PGBUF_LATCH_WRITE);
      if (page == NULL)
	{
	  assert (false);
	  return ER_FAILED;
	}
      VPID_SET_NULL (&page->next_page);
      vacuum_log_set_next_page_dropped_cls_btid (thread_p, (PAGE_PTR) page,
						 &page->next_page, type);
      vacuum_set_dirty_dropped_entries_page (thread_p, page, FREE);
    }
  return NO_ERROR;
}

/*
 * vacuum_is_class_dropped () - Check whether class is considered dropped in
 *				respect to MVCCID.
 *
 * return	  : True if class is considered dropped.
 * thread_p (in)  : Thread entry.
 * class_oid (in) : Class object identifier.
 * mvccid (in)	  : MVCCID.
 */
bool
vacuum_is_class_dropped (THREAD_ENTRY * thread_p, OID * class_oid,
			 MVCCID mvccid)
{
  if (prm_get_bool_value (PRM_ID_DISABLE_VACUUM))
    {
      return false;
    }
  if (vacuum_find_dropped_cls_btid
      (thread_p, class_oid, mvccid, DROPPED_CLASS) == false)
    {
      return vacuum_is_dropped_cls_btid_in_buffer (thread_p, class_oid,
						   mvccid, DROPPED_CLASS);
    }
  return true;
}

/*
 * vacuum_is_index_dropped () - Check whether index is considered dropped in
 *				respect to MVCCID.
 *
 * return	  : True if class is considered dropped.
 * thread_p (in)  : Thread entry.
 * class_oid (in) : Class object identifier.
 * mvccid (in)	  : MVCCID.
 */
bool
vacuum_is_index_dropped (THREAD_ENTRY * thread_p, BTID * btid, MVCCID mvccid)
{
  if (prm_get_bool_value (PRM_ID_DISABLE_VACUUM))
    {
      return false;
    }
  if (vacuum_find_dropped_cls_btid (thread_p, btid, mvccid, DROPPED_INDEX)
      == false)
    {
      return vacuum_is_dropped_cls_btid_in_buffer (thread_p, btid, mvccid,
						   DROPPED_INDEX);
    }
  return true;
}

/*
 * vacuum_find_dropped_cls_btid () - Find the dropped class or index and check
 *				     whether the given MVCCID is older than or
 *				     equal to the MVCCID of dropped class.
 *				     Used by vacuum to detect records that
 *				     belong to dropped classes and indexes.
 *
 * return	 : True if record belong to a dropped class or index.
 * thread_p (in) : Thread entry.
 * cls_btid (in) : Class identifier or B-tree identifier.
 * mvccid (in)	 : MVCCID of checked record.
 * type (in)	 : Dropped class or dropped index.
 */
static bool
vacuum_find_dropped_cls_btid (THREAD_ENTRY * thread_p, void *cls_btid,
			      MVCCID mvccid, DROPPED_TYPE type)
{
  VACUUM_DROPPED_ENTRIES_PAGE *page = NULL;
  VACUUM_DROPPED_ENTRY *dropped_entry = NULL;
  VPID vpid;
  INT32 *countp = VACUUM_DROPPED_ENTRIES_COUNT_PTR (type);
  int mvccid_offset =
    (type == DROPPED_CLASS) ? OR_OID_SIZE : OR_BTID_ALIGNED_SIZE;
  INT16 page_count;

  if (*countp == 0)
    {
      /* No dropped entries */
      return false;
    }

  /* Search for dropped entry in all pages. */
  VPID_COPY (&vpid, VACUUM_DROPPED_ENTRIES_VPID_PTR (type));
  while (!VPID_ISNULL (&vpid))
    {
      /* Fix current page */
      page =
	vacuum_fix_dropped_entries_page (thread_p, &vpid, PGBUF_LATCH_READ);
      if (page == NULL)
	{
	  assert (false);
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_GENERIC_ERROR, 0);
	  return false;
	}
      /* Copy next page VPID */
      VPID_COPY (&vpid, &page->next_page);
      page_count = page->n_dropped_entries;
      if (type == DROPPED_CLASS)
	{
	  /* Use oid_compare to find matching entry */
	  dropped_entry =
	    (VACUUM_DROPPED_ENTRY *) bsearch (cls_btid, page->dropped_entries,
					      page_count,
					      sizeof (VACUUM_DROPPED_ENTRY),
					      oid_compare);
	}
      else
	{
	  /* Use btree_compare_btids to find matching entries */
	  dropped_entry =
	    (VACUUM_DROPPED_ENTRY *) bsearch (cls_btid, page->dropped_entries,
					      page_count,
					      sizeof (VACUUM_DROPPED_ENTRY),
					      btree_compare_btids);
	}
      if (dropped_entry != NULL)
	{
	  /* Found matching entry.
	   * Compare the given MVCCID with the MVCCID of dropped entry.
	   */
	  if (!mvcc_id_precedes (dropped_entry->mvccid, mvccid))
	    {
	      /* The record must belong to the dropped entry */
	      vacuum_unfix_dropped_entries_page (thread_p, page);
	      return true;
	    }
	  else
	    {
	      /* The record belongs to an entry with the same identifier, but
	       * is newer.
	       */
	      vacuum_unfix_dropped_entries_page (thread_p, page);
	      return false;
	    }
	}
      vacuum_unfix_dropped_entries_page (thread_p, page);
    }

  /* Entry not found */
  return false;
}

/*
 * vacuum_log_add_dropped_cls_btid () - Log adding dropped entries.
 *
 * return		 : Void.
 * thread_p (in)	 : Thread entry.
 * page_p (in)		 : Page pointer.
 * dropped_cls_btid (in) : Newly added dropped entry.
 * position (in)	 : Position where the new dropped entry was added.
 * type (in)		 : Dropped class or b-tree identifier.
 * is_duplicate (in)	 : True if a duplicate of new dropped entry exists.
 *			   In this case only replacing MVCCID is required.
 */
static void
vacuum_log_add_dropped_cls_btid (THREAD_ENTRY * thread_p, PAGE_PTR page_p,
				 VACUUM_DROPPED_ENTRY * dropped_cls_btid,
				 INT16 position, DROPPED_TYPE type,
				 bool is_duplicate)
{
#define VACUUM_ADD_DROPPED_CLS_BTID_MAX_REDO_CRUMBS 3
  LOG_DATA_ADDR addr;
  LOG_CRUMB redo_crumbs[VACUUM_ADD_DROPPED_CLS_BTID_MAX_REDO_CRUMBS];
  int n_redo_crumbs = 0;

  /* Add dropped entry */
  redo_crumbs[n_redo_crumbs].data = dropped_cls_btid;
  redo_crumbs[n_redo_crumbs++].length = sizeof (*dropped_cls_btid);

  /* Add dropped type */
  redo_crumbs[n_redo_crumbs].data = &type;
  redo_crumbs[n_redo_crumbs++].length = sizeof (type);

  /* Add is_duplicate */
  redo_crumbs[n_redo_crumbs].data = &is_duplicate;
  redo_crumbs[n_redo_crumbs++].length = sizeof (is_duplicate);

  assert (n_redo_crumbs <= VACUUM_ADD_DROPPED_CLS_BTID_MAX_REDO_CRUMBS);

  /* Initialize log data address */
  addr.pgptr = page_p;
  addr.vfid = VACUUM_DROPPED_ENTRIES_VFID_PTR (type);
  addr.offset = position;

  /* Append redo log data */
  log_append_redo_crumbs (thread_p, RVVAC_DROPPED_CLS_BTID_ADD, &addr,
			  n_redo_crumbs, redo_crumbs);
}

/*
 * vacuum_rv_redo_add_dropped_cls_btid () - Recover adding dropped entry.
 *
 * return	 : Error code.
 * thread_p (in) : Thread entry.
 * rcv (in)	 : Recovery data.
 */
int
vacuum_rv_redo_add_dropped_cls_btid (THREAD_ENTRY * thread_p, LOG_RCV * rcv)
{
  VACUUM_DROPPED_ENTRIES_PAGE *page =
    (VACUUM_DROPPED_ENTRIES_PAGE *) rcv->pgptr;
  VACUUM_DROPPED_ENTRY *new_entry = NULL;
  INT32 *countp = NULL;
  char *tmp_buffer = NULL;
  int offset = 0, mem_size;
  DROPPED_TYPE type;
  INT16 position = rcv->offset;
  bool is_duplicate;

  /* Get recovery data */

  /* Get dropped entry data */
  new_entry = (VACUUM_DROPPED_ENTRY *) rcv->data;
  offset = sizeof (*new_entry);

  /* Get dropped entry type */
  type = *((DROPPED_TYPE *) (rcv->data + offset));
  offset += sizeof (type);

  /* Get is duplicate */
  is_duplicate = *((char *) (rcv->data + offset));
  offset += sizeof (is_duplicate);

  /* Check that all expected data was processed */
  assert (offset == rcv->length);

  /* Get count pointer and temporary buffer according to dropped type */
  countp = VACUUM_DROPPED_ENTRIES_COUNT_PTR (type);
  tmp_buffer = VACUUM_DROPPED_ENTRIES_TMP_BUFFER (type);

  if (type == DROPPED_CLASS)
    {
      vacuum_er_log (VACUUM_ER_LOG_RECOVERY | VACUUM_ER_LOG_DROPPED_CLASSES,
		     "Recovery of dropped classes: add class(%d, %d, %d), "
		     "mvccid=%lld at position %d. (%s)",
		     (int) new_entry->dropped_cls_btid.class_oid.volid,
		     (int) new_entry->dropped_cls_btid.class_oid.pageid,
		     (int) new_entry->dropped_cls_btid.class_oid.slotid,
		     new_entry->mvccid, (int) position,
		     is_duplicate ? "is duplicate" : "is not duplicate");
    }
  else
    {
      assert (type == DROPPED_INDEX);
      vacuum_er_log (VACUUM_ER_LOG_RECOVERY | VACUUM_ER_LOG_DROPPED_INDEXES,
		     "Recovery of dropped indexes: add btid(%d, %d %d), "
		     "mvccid=%lld at position %d. (%s)",
		     (int) new_entry->dropped_cls_btid.btid.root_pageid,
		     (int) new_entry->dropped_cls_btid.btid.vfid.volid,
		     (int) new_entry->dropped_cls_btid.btid.vfid.fileid,
		     new_entry->mvccid, (int) position,
		     is_duplicate ? "is duplicate" : "is not duplicate");
    }

  if (position < page->n_dropped_entries && !is_duplicate)
    {
      /* Move all entries starting with position to the right */
      /* Use temporary buffer to move memory data */
      mem_size =
	(page->n_dropped_entries - position) * sizeof (VACUUM_DROPPED_ENTRY);
      memcpy (tmp_buffer, &page->dropped_entries[position], mem_size);
      memcpy (&page->dropped_entries[position + 1], tmp_buffer, mem_size);
    }
  /* Add new entry at position */
  memcpy (&page->dropped_entries[position], new_entry,
	  sizeof (VACUUM_DROPPED_ENTRY));
  if (!is_duplicate)
    {
      /* Increment dropped entries page counter */
      page->n_dropped_entries++;
      /* Increment dropped entries global counter */
      (*countp)++;
    }

  pgbuf_set_dirty (thread_p, rcv->pgptr, DONT_FREE);

  return NO_ERROR;
}

/*
 * vacuum_log_cleanup_dropped_cls_btid () - Log dropped entries cleanup.
 *
 * return	  : Void.
 * thread_p (in)  : Thread entry.
 * page_p (in)	  : Page pointer.
 * indexes (in)	  : Indexes of cleaned up dropped entries.
 * n_indexes (in) : Total count of dropped entries.
 * type (in)	  : Dropped class or b-tree identifier.
 *
 * NOTE: Consider not logging cleanup. Cleanup can be done at database
 *	 restart.
 */
static void
vacuum_log_cleanup_dropped_cls_btid (THREAD_ENTRY * thread_p, PAGE_PTR page_p,
				     INT16 * indexes, INT16 n_indexes,
				     DROPPED_TYPE type)
{
#define VACUUM_CLEANUP_DROPPED_CLS_BTID_MAX_REDO_CRUMBS 3
  LOG_CRUMB redo_crumbs[VACUUM_CLEANUP_DROPPED_CLS_BTID_MAX_REDO_CRUMBS];
  LOG_DATA_ADDR addr;
  int n_redo_crumbs = 0;

  /* Add n_indexes */
  redo_crumbs[n_redo_crumbs].data = &n_indexes;
  redo_crumbs[n_redo_crumbs++].length = sizeof (n_indexes);

  /* Add indexes */
  redo_crumbs[n_redo_crumbs].data = indexes;
  redo_crumbs[n_redo_crumbs++].length = n_indexes * sizeof (*indexes);

  /* Add dropped type */
  redo_crumbs[n_redo_crumbs].data = &type;
  redo_crumbs[n_redo_crumbs++].length = sizeof (type);

  assert (n_redo_crumbs <= VACUUM_CLEANUP_DROPPED_CLS_BTID_MAX_REDO_CRUMBS);

  /* Initialize log data address */
  addr.pgptr = page_p;
  addr.vfid = VACUUM_DROPPED_ENTRIES_VFID_PTR (type);
  addr.offset = 0;

  log_append_redo_crumbs (thread_p, RVVAC_DROPPED_CLS_BTID_CLEANUP, &addr,
			  n_redo_crumbs, redo_crumbs);
}

/*
 * vacuum_rv_redo_cleanup_dropped_cls_btid () - Recover dropped entries
 *						cleanup.
 *
 * return	 : Error code.
 * thread_p (in) : Thread entry,
 * rcv (in)	 : Recovery data.
 *
 * NOTE: Consider not logging cleanup. Cleanup can be done at database
 *	 restart.
 */
int
vacuum_rv_redo_cleanup_dropped_cls_btid (THREAD_ENTRY * thread_p,
					 LOG_RCV * rcv)
{
  int offset = 0, mem_size;
  VACUUM_DROPPED_ENTRIES_PAGE *page =
    (VACUUM_DROPPED_ENTRIES_PAGE *) rcv->pgptr;
  INT32 *countp = NULL;
  char *tmp_buffer = NULL;
  INT16 *indexes;
  INT16 n_indexes, i;
  DROPPED_TYPE type;

  /* Get recovery information */

  /* Get n_indexes */
  n_indexes = *((INT16 *) rcv->data);
  offset += sizeof (n_indexes);

  /* Get indexes */
  indexes = (INT16 *) (rcv->data + offset);
  offset += sizeof (*indexes) * n_indexes;

  /* Get dropped type */
  type = *((DROPPED_TYPE *) (rcv->data + offset));
  offset += sizeof (type);

  /* Check that all recovery data has been processed */
  assert (offset == rcv->length);

  /* Get count pointer and temporary buffer according to dropped type */
  countp = VACUUM_DROPPED_ENTRIES_COUNT_PTR (type);
  tmp_buffer = VACUUM_DROPPED_ENTRIES_TMP_BUFFER (type);

  /* Cleanup starting from last entry */
  for (i = 0; i < n_indexes; i++)
    {
      /* Remove entry at indexes[i] */
      if (type == DROPPED_CLASS)
	{
	  vacuum_er_log (VACUUM_ER_LOG_RECOVERY
			 | VACUUM_ER_LOG_DROPPED_CLASSES,
			 "Recovery of dropped classes: remove "
			 "class(%d, %d, %d), mvccid=%lld at position %d.",
			 (int) page->dropped_entries[indexes[i]].
			 dropped_cls_btid.class_oid.volid,
			 (int) page->dropped_entries[indexes[i]].
			 dropped_cls_btid.class_oid.pageid,
			 (int) page->dropped_entries[indexes[i]].
			 dropped_cls_btid.class_oid.slotid,
			 page->dropped_entries[indexes[i]].mvccid,
			 (int) indexes[i]);
	}
      else
	{
	  assert (type == DROPPED_INDEX);
	  vacuum_er_log (VACUUM_ER_LOG_RECOVERY
			 | VACUUM_ER_LOG_DROPPED_INDEXES,
			 "Recovery of dropped indexes: remove "
			 "btid(%d, %d %d), mvccid=%lld at position %d.",
			 (int) page->dropped_entries[indexes[i]].
			 dropped_cls_btid.btid.root_pageid,
			 (int) page->dropped_entries[indexes[i]].
			 dropped_cls_btid.btid.vfid.volid,
			 (int) page->dropped_entries[indexes[i]].
			 dropped_cls_btid.btid.vfid.fileid,
			 page->dropped_entries[indexes[i]].mvccid,
			 (int) indexes[i]);
	}
      mem_size =
	(page->n_dropped_entries -
	 indexes[i]) * sizeof (VACUUM_DROPPED_ENTRY);
      assert (mem_size >= 0);
      if (mem_size > 0)
	{
	  memcpy (tmp_buffer, &page->dropped_entries[indexes[i] + 1],
		  mem_size);
	  memcpy (&page->dropped_entries[indexes[i]], tmp_buffer, mem_size);
	}

      /* Update dropped entries page counter */
      page->n_dropped_entries--;
    }

  /* Update dropped entries global counter */
  (*countp) -= n_indexes;

  pgbuf_set_dirty (thread_p, rcv->pgptr, DONT_FREE);

  return NO_ERROR;
}

/*
 * vacuum_log_set_next_page_dropped_cls_btid () - Log changing link to next
 *						  page for dropped entries.
 *
 * return	  : Void.
 * thread_p (in)  : Thread entry.
 * page_p (in)	  : Page pointer.
 * next_page (in) : Next page VPID.
 * type (in)	  : Dropped class or b-tree identifier.
 */
static void
vacuum_log_set_next_page_dropped_cls_btid (THREAD_ENTRY * thread_p,
					   PAGE_PTR page_p, VPID * next_page,
					   DROPPED_TYPE type)
{
  LOG_DATA_ADDR addr;

  /* Initialize log data address */
  addr.pgptr = page_p;
  addr.vfid = VACUUM_DROPPED_ENTRIES_VFID_PTR (type);
  addr.offset = 0;

  /* Append log redo */
  log_append_redo_data (thread_p, RVVAC_DROPPED_CLS_BTID_NEXT_PAGE, &addr,
			sizeof (*next_page), next_page);
}

/*
 * vacuum_rv_set_next_page_dropped_cls_btid () - Recover setting link to next
 *						 page for dropped entries.
 *
 * return	 : Error code.
 * thread_p (in) : Thread entry.
 * rcv (in)	 : Recovery data.
 */
int
vacuum_rv_set_next_page_dropped_cls_btid (THREAD_ENTRY * thread_p,
					  LOG_RCV * rcv)
{
  VACUUM_DROPPED_ENTRIES_PAGE *page =
    (VACUUM_DROPPED_ENTRIES_PAGE *) rcv->pgptr;

  /* Set next page VPID */
  VPID_COPY (&page->next_page, (VPID *) rcv->data);

  /* Check recovery data is as expected */
  assert (rcv->length = sizeof (VPID));

  vacuum_er_log (VACUUM_ER_LOG_RECOVERY,
		 "Set link for dropped entries from page(%d, %d) to "
		 "page(%d, %d).", pgbuf_get_vpid_ptr (rcv->pgptr)->pageid,
		 pgbuf_get_vpid_ptr (rcv->pgptr)->volid,
		 page->next_page.volid, page->next_page.pageid);

  pgbuf_set_dirty (thread_p, rcv->pgptr, DONT_FREE);

  return NO_ERROR;
}

/*
 * vacuum_is_dropped_cls_btid_in_buffer () - Find the if the class or index
 *		    is in the dropped classes/indexes buffers that have not yet
 *		    been consumed
 *
 * return	 : True if found
 * thread_p (in) : Thread entry.
 * cls_btid (in) : Class identifier or B-tree identifier.
 * mvccid (in)	 : MVCCID of checked record.
 * type (in)	 : Dropped class or dropped index.
 */
static int
vacuum_is_dropped_cls_btid_in_buffer (THREAD_ENTRY * thread_p, void *cls_btid,
				      MVCCID mvccid, DROPPED_TYPE type)
{
  VACUUM_DROPPED_ENTRY *entry = NULL;
  int i = 0;

  if (type == DROPPED_CLASS)
    {
      OID *cls_oid = (OID *) cls_btid;

      (void) pthread_mutex_lock (&vacuum_Dropped_class_buffer->mutex);

      for (i = 0; i < vacuum_Dropped_class_buffer->n_dropped_entries; i++)
	{
	  entry = &vacuum_Dropped_class_buffer->dropped_entries[i];
	  if (OID_EQ (cls_oid, &entry->dropped_cls_btid.class_oid))
	    {
	      if (!mvcc_id_precedes (entry->mvccid, mvccid))
		{
		  (void) pthread_mutex_unlock (&vacuum_Dropped_class_buffer->
					       mutex);
		  return true;
		}
	      else
		{
		  (void) pthread_mutex_unlock (&vacuum_Dropped_class_buffer->
					       mutex);
		  return false;
		}
	    }
	}
      (void) pthread_mutex_unlock (&vacuum_Dropped_class_buffer->mutex);
    }
  else
    {
      BTID *btid = (BTID *) cls_btid;

      (void) pthread_mutex_lock (&vacuum_Dropped_index_buffer->mutex);

      for (i = 0; i < vacuum_Dropped_index_buffer->n_dropped_entries; i++)
	{
	  entry = &vacuum_Dropped_index_buffer->dropped_entries[i];
	  if (BTID_IS_EQUAL (btid, &entry->dropped_cls_btid.btid))
	    {
	      if (!mvcc_id_precedes (entry->mvccid, mvccid))
		{
		  (void) pthread_mutex_unlock (&vacuum_Dropped_index_buffer->
					       mutex);
		  return true;
		}
	      else
		{
		  (void) pthread_mutex_unlock (&vacuum_Dropped_index_buffer->
					       mutex);
		  return false;
		}
	    }
	}
      (void) pthread_mutex_unlock (&vacuum_Dropped_index_buffer->mutex);
    }

  return false;
}
