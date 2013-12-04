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

/* The maximum number of slots in a page if all of them are empty.
 * IO_MAX_PAGE_SIZE is used for page size and any headers are ignored (it
 * wouldn't bring a significant difference).
 */
#define MAX_SLOTS_IN_PAGE (IO_MAX_PAGE_SIZE / sizeof (SPAGE_SLOT))

/* The maximum size of OID buffer where the records vacuumed from heap file
 * are collected to be removed from index b-trees.
 */
#define REMOVE_OIDS_BUFFER_SIZE (16 * IO_DEFAULT_PAGE_SIZE / OR_OID_SIZE)

/* The default number of cached entries in a vacuum statistics cache */
#define VACUUM_STATS_CACHE_SIZE 100

#define db_private_free_and_init_class_oid_list(thrd_p, ptr) \
  do \
  { \
    db_private_free_class_oid_list(thrd_p, ptr); \
    ptr = NULL;	\
  } while (0)

/* Structure that stores necessary statistics for auto-vacuuming */
typedef struct vacuum_statistics VACUUM_STATISTICS;
struct vacuum_statistics
{
  OID class_oid;		/* Class object identifier */
  HFID hfid;			/* Heap file identifier */
  int n_total_records;		/* Counts the total number of records */
  int n_dead_records;		/* Counts the number of dead records */

  int option_dead_threshold;	/* TODO: class may overwrite auto-vacuum options */
  float option_dead_ratio;

  VACUUM_STATISTICS *next;
};

/* Structure used to cache vacuum statistics entries */
typedef struct vacuum_stats_cache VACUUM_STATS_CACHE;
struct vacuum_stats_cache
{
  VACUUM_STATISTICS cached_entries[VACUUM_STATS_CACHE_SIZE];
  VACUUM_STATS_CACHE *next;
};

/* Hash table for vacuum statistics entries */
typedef struct vacuum_stats_table VACUUM_STATS_TABLE;
struct vacuum_stats_table
{
  MHT_TABLE *hash_table;	/* Hash table to keep vacuum statistics.
				 * The key is a class oid, data is
				 * a vacuum statistics entry.
				 */
  VACUUM_STATS_CACHE *mem_cache;	/* A list of preallocated caches for
					 * vacuum statistics entries.
					 */
  VACUUM_STATISTICS *free_entries;	/* A list of usable vacuum statistics
					 * entries.
					 */
};

/* Structure that helps storing a list of OID's. Use by auto-vacuum */
typedef struct class_oid_list CLASS_OID_LIST;
struct class_oid_list
{
  OID class_oid;
  CLASS_OID_LIST *next;
};

/* Structure used for tracking running vacuums. It is not desirable to have
 * two vacuum processes on the same class at once.
 */
typedef struct running_vacuum RUNNING_VACUUM;
struct running_vacuum
{
  OID class_oid;
  bool interrupt;
  bool abort;

  pthread_mutex_t mutex;

  RUNNING_VACUUM *next;
};

/* Static vacuum statistics table */
VACUUM_STATS_TABLE vacuum_Stats;

/* Running vacuums variables */
/* List of currently running vacuums */
RUNNING_VACUUM *current_running_vacuums;
/* Cached entries to be reused when a new vacuum process starts */
RUNNING_VACUUM *running_vacuum_pool;
/* Mutex to synchronize access in the list of currently running vacuums */
pthread_mutex_t running_vacuums_mutex = PTHREAD_MUTEX_INITIALIZER;

/* Blocks/allows a new vacuum to start */
bool is_vacuum_allowed = false;

static void db_private_free_class_oid_list (THREAD_ENTRY * thread_p,
					    CLASS_OID_LIST * cls_oid_list);

static int vacuum_class (THREAD_ENTRY * thread_p, OID * class_oid,
			 MVCCID lowest_active_mvccid);
static int auto_vacuum_validate_class (THREAD_ENTRY * thread_p, void *data,
				       void *args);
static int vacuum_flush_statistics (THREAD_ENTRY * thread_p, void *data,
				    void *args);
static int vacuum_expand_stats_table (THREAD_ENTRY * thread_p);
static int vacuum_free_stats_table_entry (const void *key, void *data,
					  void *args);
static void vacuum_free_stats_entry (VACUUM_STATISTICS * vacuum_stats_entry);
static int vacuum_stats_table_initialize (THREAD_ENTRY * thread_p);
static void vacuum_stats_table_finalize (THREAD_ENTRY * thread_p);
static void vacuum_running_vacuums_initialize (THREAD_ENTRY * thread_p);
static void vacuum_running_vacuums_finalize (THREAD_ENTRY * thread_p);
static void vacuum_running_vacuums_abort_all (THREAD_ENTRY * thread_p);
static RUNNING_VACUUM *vacuum_running_vacuums_allocate_entry (THREAD_ENTRY *
							      thread_p,
							      OID *
							      class_oid);
static void vacuum_running_vacuum_free_entry (THREAD_ENTRY * thread_p,
					      RUNNING_VACUUM * entry);
static bool vacuum_is_class_vacuumed (THREAD_ENTRY * thread_p,
				      const OID * class_oid);
static int vacuum_indexes (THREAD_ENTRY * thread_p, OR_CLASSREP * class_rep,
			   HFID * hfid, OID * remove_oids, int n_remove_oids);
static int vacuum_remove_oids_from_heap (THREAD_ENTRY * thread_p, HFID * hfid,
					 OID * remove_oids,
					 int n_remove_oids);
static void vacuum_log_remove_oids (THREAD_ENTRY * thread_p, PAGE_PTR page_p,
				    HFID * hfid, PGSLOTID * slots,
				    int n_slots);

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
  int i, error = NO_ERROR;
  OID cls_oid;
  MVCCID lowest_active_mvccid = logtb_get_lowest_active_mvccid (thread_p);

  for (i = 0; i < num_classes; i++)
    {
      /* Get current class OID */
      COPY_OID (&cls_oid, &class_oids[i]);

      /* Vacuum the heap file */
      /* do not lock class, only lock heap pages
       * Index key/OID? locks still be required though.
       */
      error = vacuum_class (thread_p, &cls_oid, lowest_active_mvccid);
      if (error != NO_ERROR)
	{
	  return ER_FAILED;
	}
    }
  return NO_ERROR;
}

/*
 * vacuum_class () - Function that cleans up one class (heap file and all
 *		     indexes). Used in the context of MVCC.
 *
 * return		     : Error code.
 * thread_p (in)	     : Thread entry.
 * class_oid (in)	     : Class OID.
 * lowest_active_mvccid (in) : MVCCID for the oldest active transaction
 *			       when vacuuming was started. 
 */
static int
vacuum_class (THREAD_ENTRY * thread_p, OID * class_oid,
	      MVCCID lowest_active_mvccid)
{
  VPID vpid, next_vpid;
  PAGE_PTR page_p = NULL;
  HFID hfid;
  VACUUM_STATISTICS *vacuum_stats_entry = NULL;
  VACUUM_PAGE_DATA vacuum_page_data;
  RUNNING_VACUUM *running_vacuum_entry = NULL;
  int n_total_cleaned_records;
  int lock_result;
  OID remove_oids[REMOVE_OIDS_BUFFER_SIZE];
  OID oid;
  int n_remove_oids = 0, i, idx_incache = -1, error_code = NO_ERROR;
  OR_CLASSREP *classrep = NULL;
  bool has_lock_on_class = false;
  bool has_index;

  /* Check if vacuum already runs on current class */
  pthread_mutex_lock (&running_vacuums_mutex);
  if (!is_vacuum_allowed)
    {
      /* Database is shutting down */
      pthread_mutex_unlock (&running_vacuums_mutex);
      return NO_ERROR;
    }
  if (vacuum_is_class_vacuumed (thread_p, class_oid))
    {
      /* TODO: add new error message */
      pthread_mutex_unlock (&running_vacuums_mutex);
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_GENERIC_ERROR, 0);
      return ER_GENERIC_ERROR;
    }

  /* Create a new entry for current running vacuum */
  running_vacuum_entry =
    vacuum_running_vacuums_allocate_entry (thread_p, class_oid);
  pthread_mutex_unlock (&running_vacuums_mutex);

  /* Get heap file identifier for this class */
  error_code = heap_get_hfid_from_class_oid (thread_p, class_oid, &hfid);
  if (error_code != NO_ERROR)
    {
      goto end;
    }

  /* Get class representation - information about indexes is required */
  classrep =
    heap_classrepr_get (thread_p, class_oid, NULL, 0, &idx_incache, true);
  has_index = classrep->n_indexes > 0;

  /* Get first page */
  vpid.pageid = hfid.hpgid;
  vpid.volid = hfid.vfid.volid;

  /* Allocate enough space in vacuum page data to handle any heap page */
  error_code =
    mvcc_allocate_vacuum_data (thread_p, &vacuum_page_data,
			       MAX_SLOTS_IN_PAGE);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  /* Initialize the count of vacuumed records */
  n_total_cleaned_records = 0;

  /* Loop through all heap file pages */
  while (!VPID_ISNULL (&vpid))
    {
      /* Fix current page */
      page_p =
	pgbuf_fix (thread_p, &vpid, OLD_PAGE, PGBUF_LATCH_READ,
		   PGBUF_UNCONDITIONAL_LATCH);
      if (page_p == NULL)
	{
	  error_code = ER_FAILED;
	  goto end;
	}

      /* Save the vpid of next page */
      if (heap_vpid_next (&hfid, page_p, &next_vpid) != NO_ERROR)
	{
	  error_code = ER_FAILED;
	  goto end;
	}

      /* Before starting to vacuum page, must first check running_vacuum entry
       * maybe somebody requested an interrupt or even an abort.
       */
      pthread_mutex_lock (&running_vacuum_entry->mutex);
      if (running_vacuum_entry->abort)
	{
	  /* Just abort vacuum */
	  pthread_mutex_unlock (&running_vacuum_entry->mutex);
	  goto end;
	}
      else if (running_vacuum_entry->interrupt)
	{
	  /* TODO: Implement an interrupt/resume system */
	}
      pthread_mutex_unlock (&running_vacuum_entry->mutex);

      /* Get an IX_LOCK on class so no one can alter the class while the page
       * is vacuumed.
       */
      lock_result =
	lock_object (thread_p, class_oid, oid_Root_class_oid, IX_LOCK,
		     LK_UNCOND_LOCK);
      if (lock_result != LK_GRANTED)
	{
	  error_code = er_errid ();
	  goto end;
	}
      has_lock_on_class = true;

      /* TODO: check page visibility */

      /* Clean current page */
      error_code =
	heap_vacuum_page (thread_p, &hfid, &page_p, lowest_active_mvccid,
			  false, has_index, &vacuum_page_data);
      if (error_code != NO_ERROR)
	{
	  goto end;
	}
      if (page_p != NULL)
	{
	  /* Unfix heap page */
	  pgbuf_unfix_and_init (thread_p, page_p);
	}

      /* TODO: update visibility map
       * If the last change on page is before lowest_active_mvccid, the page
       * can be marked as all visible.
       */
      /* Consider an optimization to pass heap_vacuum_page that heap_page has
       * no indexes, therefore all dead records can be marked as unused
       * directly. Even better, consider setting a flag in the heap page.
       * When an index is created, all pages must be scanned for OID's and
       * can be marked as using index.
       */

      /* Initialize pageid and volid from OID's in current page */
      oid.pageid = vpid.pageid;
      oid.volid = vpid.volid;

      if (has_index)
	{
	  /* Vacuumed records should be removed from b-trees too */
	  /* Collect them in a buffer */
	  for (i = 0; i < vacuum_page_data.n_dead; i++)
	    {
	      oid.slotid = vacuum_page_data.dead_slots[i];
	      COPY_OID (&remove_oids[n_remove_oids], &oid);

	      if (++n_remove_oids == REMOVE_OIDS_BUFFER_SIZE)
		{
		  /* The buffer is full, run vacuum on indexes */
		  error_code =
		    vacuum_indexes (thread_p, classrep, &hfid, remove_oids,
				    n_remove_oids);
		  if (error_code != NO_ERROR)
		    {
		      goto end;
		    }
		  /* Statistics will have to be updated too */
		  n_total_cleaned_records += n_remove_oids;
		  /* Empty buffer */
		  n_remove_oids = 0;
		}
	    }
	}
      else
	{
	  /* Records have been marked as deleted */
	  n_total_cleaned_records += vacuum_page_data.n_vacuumed_records;
	}

      /* Be lazy, unlock class to allow other transactions lock it */
      lock_unlock_object (thread_p, class_oid, oid_Root_class_oid, IX_LOCK,
			  true);
      has_lock_on_class = false;

      /* Advance to next heap page */
      VPID_COPY (&vpid, &next_vpid);
    }

  if (has_index && n_remove_oids > 0)
    {
      /* Vacuum indexes */
      lock_result =
	lock_object (thread_p, class_oid, oid_Root_class_oid, IX_LOCK,
		     LK_UNCOND_LOCK);
      if (lock_result != LK_GRANTED)
	{
	  error_code = er_errid ();
	  goto end;
	}
      has_lock_on_class = true;
      error_code =
	vacuum_indexes (thread_p, classrep, &hfid, remove_oids,
			n_remove_oids);
      if (error_code != NO_ERROR)
	{
	  goto end;
	}
      lock_unlock_object (thread_p, class_oid, oid_Root_class_oid, IX_LOCK,
			  true);
      has_lock_on_class = false;
      n_total_cleaned_records += n_remove_oids;
    }

  /* Update vacuum statistics */
  error_code =
    vacuum_stats_table_update_entry (thread_p, class_oid, &hfid,
				     -n_total_cleaned_records,
				     -n_total_cleaned_records);

end:
  /* Make sure class is not locked */
  if (has_lock_on_class)
    {
      lock_unlock_object (thread_p, class_oid, oid_Root_class_oid, IX_LOCK,
			  true);
    }

  /* Unfix the last page */
  if (page_p != NULL)
    {
      pgbuf_unfix_and_init (thread_p, page_p);
    }

  /* Clean vacuum data */
  mvcc_finalize_vacuum_data (thread_p, &vacuum_page_data);

  /* Free running vacuum entry */
  pthread_mutex_lock (&running_vacuums_mutex);
  vacuum_running_vacuum_free_entry (thread_p, running_vacuum_entry);
  pthread_mutex_unlock (&running_vacuums_mutex);

  return error_code;
}

/*
 * vacuum_indexes () - Remove vacuumed OID's from 
 *
 * return	      : Error code.
 * thread_p (in)      : Thread entry.
 * class_rep (in)     : Class representation.
 * remove_oids (in)   : Arrays of OID's to remove.
 * n_remove_oids (in) : OID count in remove_oids.
 */
static int
vacuum_indexes (THREAD_ENTRY * thread_p, OR_CLASSREP * class_rep, HFID * hfid,
		OID * remove_oids, int n_remove_oids)
{
  int i;
  int error_code = NO_ERROR;

  /* TODO: If there are no indexes, we shouldn't even be here. Must change
   *       heap_vacuum_page to remove records completely directly. Must
   *       also consider the page vacuuming during sequential scan.
   */

  /* First sort the OID buffer in order to do binary search later */
  qsort (remove_oids, n_remove_oids, OR_OID_SIZE, oid_compare);

  /* Vacuum all b-trees belonging to this class by removing the OID's found
   * in buffer.
   */
  for (i = 0; i < class_rep->n_indexes; i++)
    {
      error_code =
	btree_vacuum (thread_p, &class_rep->indexes[i].btid,
		      remove_oids, n_remove_oids);
      if (error_code != NO_ERROR)
	{
	  return ER_FAILED;
	}
    }

  /* Now mark the objects in heap file accordingly */
  return vacuum_remove_oids_from_heap (thread_p, hfid, remove_oids,
				       n_remove_oids);
}

/*
 * vacuum_remove_oids_from_heap () - Remove OID's from heap file.
 *
 * return	      : Error code.
 * thread_p (in)      : Thread entry.
 * hfid (in)	      : Heap file identifier.
 * remove_oids (in)   : Array of OID's to remove.
 * n_remove_oids (in) : OID count in remove_oids.
 *
 * NOTE: This must be called after these OID's have been removed from all
 *	 indexes. This way, next vacuum will know not they were already
 *	 removed from index.
 */
static int
vacuum_remove_oids_from_heap (THREAD_ENTRY * thread_p, HFID * hfid,
			      OID * remove_oids, int n_remove_oids)
{
  VPID crt_vpid, fixed_vpid;
  PAGE_PTR page_p = NULL;
  OID *crt_oidp, *oid_buffer_end;
  PGSLOTID slotids[MAX_SLOTS_IN_PAGE];
  int n_slots;

  oid_buffer_end = remove_oids + n_remove_oids;
  VPID_SET_NULL (&fixed_vpid);

  for (crt_oidp = remove_oids; crt_oidp < oid_buffer_end; crt_oidp++)
    {
      VPID_GET_FROM_OID (&crt_vpid, crt_oidp);
      if (!VPID_EQ (&crt_vpid, &fixed_vpid))
	{
	  /* Page is changed */

	  if (page_p != NULL)
	    {
	      assert (n_slots > 0);

	      /* Log changes in old page */
	      vacuum_log_remove_oids (thread_p, page_p, hfid, slotids,
				      n_slots);
	      pgbuf_set_dirty (thread_p, page_p, DONT_FREE);

	      /* Unfix old page */
	      pgbuf_unfix_and_init (thread_p, page_p);
	    }

	  /* Fix new page */
	  VPID_COPY (&fixed_vpid, &crt_vpid);
	  page_p =
	    pgbuf_fix (thread_p, &fixed_vpid, OLD_PAGE, PGBUF_LATCH_WRITE,
		       PGBUF_UNCONDITIONAL_LATCH);
	  if (page_p == NULL)
	    {
	      return ER_FAILED;
	    }
	  n_slots = 0;
	}

      /* Mark current object as deleted */
      if (spage_mark_deleted_after_vacuum (thread_p, page_p, crt_oidp->slotid)
	  != NO_ERROR)
	{
	  pgbuf_unfix (thread_p, page_p);
	  return ER_FAILED;
	}
      slotids[n_slots++] = crt_oidp->slotid;
    }
  /* Log changes in last page */
  vacuum_log_remove_oids (thread_p, page_p, hfid, slotids, n_slots);
  pgbuf_set_dirty (thread_p, page_p, DONT_FREE);

  /* Unfix last page */
  pgbuf_unfix_and_init (thread_p, page_p);

  return NO_ERROR;
}

/*
 * vacuum_log_remove_oids () - Log removing OID's from heap page.
 *
 * return	 : Error code.
 * thread_p (in) : Thread entry.
 * page_p (in)	 : Page pointer.
 * hfid (in)	 : Heap file identifier.
 * slots (in)	 : Array of slots removed from heap page.
 * n_slots (in)  : OID count in slots.
 */
static void
vacuum_log_remove_oids (THREAD_ENTRY * thread_p, PAGE_PTR page_p,
			HFID * hfid, PGSLOTID * slots, int n_slots)
{
  LOG_DATA_ADDR addr;
  LOG_CRUMB crumbs[2];

  crumbs[0].length = sizeof (int);
  crumbs[0].data = &n_slots;

  crumbs[1].length = n_slots * sizeof (PGSLOTID);
  crumbs[1].data = slots;

  addr.pgptr = page_p;
  addr.offset = -1;
  addr.vfid = &hfid->vfid;

  log_append_redo_crumbs (thread_p, RVHF_MVCC_VACUUM_REMOVE_OIDS, &addr,
			  2, crumbs);
}

/*
 * vacuum_rv_redo_remove_oids () - Redo vacuum remove oids from heap page.
 *
 * return	 : Error code.
 * thread_p (in) : Thread entry.
 * rcv (in)	 : Recovery structure.
 */
int
vacuum_rv_redo_remove_oids (THREAD_ENTRY * thread_p, LOG_RCV * rcv)
{
  int n_slots;
  PGSLOTID *slotids = NULL;
  PAGE_PTR page_p = NULL;
  PGSLOTID *slotid_p = NULL;

  page_p = rcv->pgptr;
  n_slots = *((int *) rcv->data);

  assert (rcv->length == (sizeof (int) + n_slots * sizeof (PGSLOTID)));
  slotids = (PGSLOTID *) (((char *) rcv->data) + sizeof (int));

  for (slotid_p = slotids; slotid_p < slotids + n_slots; slotid_p++)
    {
      if (spage_mark_deleted_after_vacuum (thread_p, page_p, *slotid_p)
	  != NO_ERROR)
	{
	  return ER_FAILED;
	}
    }
  pgbuf_set_dirty (thread_p, page_p, DONT_FREE);

  return NO_ERROR;
}

/*
 * auto_vacuum_start () - Base function for auto vacuum process. Checks all
 *			  classes and creates a list of classes to vacuum.
 *			  Then it runs vacuum on each class sequentially.
 *
 * return : Void.
 */
void
auto_vacuum_start (void)
{
  THREAD_ENTRY *thread_p = thread_get_thread_entry_info ();
  CLASS_OID_LIST *cls_oid_list = NULL, *entry = NULL;
  MVCCID lowest_active_mvccid;

  if (csect_enter_as_reader (thread_p, CSECT_VACUUM_STATS, INF_WAIT)
      != NO_ERROR)
    {
      assert (0);
      return;
    }

  /* Create a list of classes that need vacuuming */
  (void) mht_map_no_key (thread_p, vacuum_Stats.hash_table,
			 auto_vacuum_validate_class, &cls_oid_list);
  csect_exit (thread_p, CSECT_VACUUM_STATS);

  /* Run vacuum on each class in cls_oid_list */
  for (entry = cls_oid_list; entry != NULL; entry = entry->next)
    {
      lowest_active_mvccid = logtb_get_lowest_active_mvccid (thread_p);
      (void) vacuum_class (thread_p, &entry->class_oid, lowest_active_mvccid);
    }

  /* Free cls_oid_list */
  db_private_free_and_init_class_oid_list (thread_p, cls_oid_list);
}

/*
 * auto_vacuum_validate_class () - Validates class for vacuum. Function is
 *				   supposed to be mapped on
 *				   vacuum_Stats.hash_table.
 *
 * return	  : True if class needs vacuum, false otherwise.
 * thread_p (in)  : Thread entry.
 * data (in)	  : VACUUM_STATISTICS entry.
 * args (out)	  : Keeps a list of class oids that need vacuuming.
 *
 * NOTE: For each class in vacuum_Stats.hash_table, compare statistics with the
 *	 auto vacuum options for that class. If the class should be vacuumed
 *	 add class to the list given as argument.
 */
static int
auto_vacuum_validate_class (THREAD_ENTRY * thread_p, void *data, void *args)
{
  VACUUM_STATISTICS *vacuum_stats = (VACUUM_STATISTICS *) data;
  CLASS_OID_LIST **cls_oid_list = (CLASS_OID_LIST **) args;
  int option_dead_threshold =
    prm_get_integer_value (PRM_ID_AUTO_VACUUM_THRESHOLD);
  float option_dead_ratio = prm_get_float_value (PRM_ID_AUTO_VACUUM_RATIO);

  assert (vacuum_stats != NULL && cls_oid_list != NULL);

  if (((float) vacuum_stats->n_dead_records) >=
      ((float) vacuum_stats->n_total_records) * option_dead_ratio +
      ((float) option_dead_threshold))
    {
      /* Class needs vacuuming */
      /* Allocate new entry for class oid list */
      CLASS_OID_LIST *new_entry =
	(CLASS_OID_LIST *) db_private_alloc (thread_p,
					     sizeof (CLASS_OID_LIST));
      if (new_entry == NULL)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY,
		  1, sizeof (CLASS_OID_LIST));
	  return ER_OUT_OF_VIRTUAL_MEMORY;
	}
      COPY_OID (&new_entry->class_oid, &vacuum_stats->class_oid);

      /* Add the entry to class oid list */
      new_entry->next = *cls_oid_list;
      *cls_oid_list = new_entry;
    }
  return NO_ERROR;
}

/*
 * vacuum_stats_table_initialize () - Initialize vacuum statistics table.
 *
 * return	 : Error code.
 * thread_p (in) : Thread entry.
 */
static int
vacuum_stats_table_initialize (THREAD_ENTRY * thread_p)
{
  vacuum_Stats.hash_table = NULL;
  vacuum_Stats.mem_cache = NULL;
  vacuum_Stats.free_entries = NULL;

  /* Allocate vacuum statistics entries cache */
  vacuum_expand_stats_table (thread_p);
  if (vacuum_Stats.mem_cache == NULL || vacuum_Stats.free_entries == NULL)
    {
      /* ER_OUT_OF_VIRTUAL_MEMORY */
      return er_errid ();
    }

  if (csect_enter (thread_p, CSECT_VACUUM_STATS, INF_WAIT) != NO_ERROR)
    {
      assert (0);
      return ER_FAILED;
    }
  vacuum_Stats.hash_table =
    mht_create ("Class OID to Auto vacuum statistics",
		VACUUM_STATS_CACHE_SIZE, oid_hash, oid_compare_equals);
  if (vacuum_Stats.hash_table == NULL)
    {
      /* ER_OUT_OF_VIRTUAL_MEMORY */
      free_and_init (vacuum_Stats.mem_cache);
      csect_exit (thread_p, CSECT_VACUUM_STATS);
      return er_errid ();
    }

  /* Vacuum stats table was initialized successfully */
  csect_exit (thread_p, CSECT_VACUUM_STATS);
  return NO_ERROR;
}

/*
 * vacuum_allocate_stats_table_entry () - Returns a preallocated vacuum
 *					  statistics entry.
 *
 * return	 : Vacuum statistics entry or NULL.
 * thread_p (in) : Thread entry.
 */
static VACUUM_STATISTICS *
vacuum_allocate_stats_table_entry (THREAD_ENTRY * thread_p)
{
  VACUUM_STATISTICS *vacuum_stats_entry = NULL;

  if (vacuum_Stats.free_entries == NULL)
    {
      /* Must allocate more entries */
      if (vacuum_expand_stats_table (thread_p) != NO_ERROR)
	{
	  return NULL;
	}
    }
  assert (vacuum_Stats.free_entries != NULL);

  /* Pop the first entry in free entries list */
  vacuum_stats_entry = vacuum_Stats.free_entries;
  vacuum_Stats.free_entries = vacuum_Stats.free_entries->next;

  /* Remove link to free entries list */
  vacuum_stats_entry->next = NULL;

  return vacuum_stats_entry;
}

/*
 * vacuum_expand_stats_table () - Expands vacuum statistics table.
 *
 * return	 : Error code.
 * thread_p (in) : Thread entry.
 */
static int
vacuum_expand_stats_table (THREAD_ENTRY * thread_p)
{
  VACUUM_STATS_CACHE *stats_cache =
    (VACUUM_STATS_CACHE *) malloc (sizeof (VACUUM_STATS_CACHE));
  int i;

  if (stats_cache == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1,
	      sizeof (VACUUM_STATS_CACHE));
      return ER_OUT_OF_VIRTUAL_MEMORY;
    }

  for (i = 0; i < VACUUM_STATS_CACHE_SIZE; i++)
    {
      /* Add to free entries */
      vacuum_free_stats_entry (&stats_cache->cached_entries[i]);
    }

  /* Add to memory caches */
  stats_cache->next = vacuum_Stats.mem_cache;
  vacuum_Stats.mem_cache = stats_cache;

  return NO_ERROR;
}

/*
 * vacuum_stats_table_finalize () - Free vacuum statistics table.
 *
 * return	 : Void.
 * thread_p (in) : Thread entry.
 */
static void
vacuum_stats_table_finalize (THREAD_ENTRY * thread_p)
{
  VACUUM_STATS_CACHE *stats_cache = NULL, *save_next = NULL;
  if (vacuum_Stats.hash_table != NULL)
    {
      (void) mht_map_no_key (thread_p, vacuum_Stats.hash_table,
			     vacuum_flush_statistics, NULL);
      mht_destroy (vacuum_Stats.hash_table);
      vacuum_Stats.hash_table = NULL;
    }

  /* Free vacuum stats entries cache */
  for (stats_cache = vacuum_Stats.mem_cache; stats_cache != NULL;
       stats_cache = save_next)
    {
      save_next = stats_cache->next;
      free_and_init (stats_cache);
    }
  vacuum_Stats.mem_cache = NULL;
  vacuum_Stats.free_entries = NULL;
}

/*
 * vacuum_flush_statistics () - Function to map on vacuum statistics table.
 *				It will update statistics in heap files
 *				for each table entry.
 *
 * return	 : Error code.
 * thread_p (in) : Thread entry.
 * data (in)	 : VACUUM_STATISTICS structure.
 * args (in)	 : Unused.
 */
static int
vacuum_flush_statistics (THREAD_ENTRY * thread_p, void *data, void *args)
{
  VACUUM_STATISTICS *vacuum_stats = (VACUUM_STATISTICS *) data;
  if (vacuum_stats == NULL)
    {
      return NO_ERROR;
    }
  (void) heap_stats_set_records_count (thread_p, &vacuum_stats->hfid,
				       vacuum_stats->n_total_records,
				       vacuum_stats->n_dead_records, true);
  return NO_ERROR;
}

/*
 * vacuum_stats_table_update_entry () - Update total records count and dead records
 *				 count.
 *
 * return	   : Error code.
 * thread_p (in)   : Thread entry.
 * class_oid (in)  : Class object identifier.
 * hfid (in)	   : Heap file identifier.
 * n_inserted (in) : Number of inserted records (will update total count).
 * n_deleted (in)  : Number of deleted records (will update dead count).
 *
 * NOTE: First it looks in the hash table if an entry for class already exists
 *	 and then update statistics.
 *	 For now this shouldn't be called for system classes.
 *
 * TODO: Overwrite dead_threshold and dead_ratio values for tables.
 */
int
vacuum_stats_table_update_entry (THREAD_ENTRY * thread_p,
				 const OID * class_oid, HFID * hfid,
				 int n_inserted, int n_deleted)
{
  VACUUM_STATISTICS *vacuum_stats_entry = NULL;
  VACUUM_STATISTICS *another_vacuum_stats_entry = NULL;
  int error = NO_ERROR;

  assert (class_oid != NULL && !OID_ISNULL (class_oid)
	  && hfid != NULL && !HFID_IS_NULL (hfid));

  if (vacuum_Stats.hash_table == NULL)
    {
      return NO_ERROR;
    }

  if (n_inserted == 0 && n_deleted == 0)
    {
      /* nothing to update */
      return NO_ERROR;
    }

#if !defined (NDEBUG)
  {
    /* Check that there is no system class in vacuum statistics table */
    bool is_system_class = false;
    error = class_is_system_class (thread_p, class_oid, &is_system_class);
    if (error != NO_ERROR || is_system_class)
      {
	assert (0);
	return ER_FAILED;
      }
  }
#endif /* !NDEBUG */

  if (csect_enter (thread_p, CSECT_VACUUM_STATS, INF_WAIT) != NO_ERROR)
    {
      assert (0);
      return ER_FAILED;
    }
  vacuum_stats_entry = mht_get (vacuum_Stats.hash_table, (void *) class_oid);
  if (vacuum_stats_entry == NULL)
    {
      /* No is no entry for this class in statistics table, a new entry must
       * be added.
       */

      /* Heap file root page must be fixed in order to obtain statistics.
       * Exit critical section in order to avoid dead locks.
       */
      csect_exit (thread_p, CSECT_VACUUM_STATS);

      /* Create new entry */
      vacuum_stats_entry = vacuum_allocate_stats_table_entry (thread_p);
      if (vacuum_stats_entry == NULL)
	{
	  return ER_FAILED;
	}
      COPY_OID (&vacuum_stats_entry->class_oid, class_oid);
      HFID_COPY (&vacuum_stats_entry->hfid, hfid);
      if (heap_stats_get_records_count (thread_p, &vacuum_stats_entry->hfid,
					&vacuum_stats_entry->n_total_records,
					&vacuum_stats_entry->n_dead_records)
	  != NO_ERROR)
	{
	  vacuum_free_stats_entry (vacuum_stats_entry);
	  return ER_FAILED;
	}
      if (csect_enter (thread_p, CSECT_VACUUM_STATS, INF_WAIT) != NO_ERROR)
	{
	  assert (0);
	  return ER_FAILED;
	}

      /* Somebody else may have updated this entry in the meanwhile */
      another_vacuum_stats_entry =
	mht_get (vacuum_Stats.hash_table, (void *) class_oid);
      if (another_vacuum_stats_entry != NULL)
	{
	  /* Use the existing entry */
	  vacuum_free_stats_entry (vacuum_stats_entry);
	  vacuum_stats_entry = another_vacuum_stats_entry;
	}
      else
	{
	  /* Add new entry to statistics table */
	  if (mht_put (vacuum_Stats.hash_table, (void *) vacuum_stats_entry,
		       (void *) vacuum_stats_entry) == NULL)
	    {
	      return ER_FAILED;
	    }
	}
    }
  assert (vacuum_stats_entry != NULL);

  vacuum_stats_entry->n_total_records += n_inserted;
  vacuum_stats_entry->n_dead_records += n_deleted;

#if !defined (NDEBUG)
  if (vacuum_stats_entry->n_total_records < 0)
    {
      er_log_debug (ARG_FILE_LINE, "Warning: vacuum statistics are off: "
		    "n_total_records = %d",
		    vacuum_stats_entry->n_total_records);
      vacuum_stats_entry->n_total_records = 0;
    }
  if (vacuum_stats_entry->n_dead_records < 0)
    {
      er_log_debug (ARG_FILE_LINE, "Warning: vacuum statistics are off: "
		    "n_dead_records = %d",
		    vacuum_stats_entry->n_dead_records);
      vacuum_stats_entry->n_dead_records = 0;
    }
  if (vacuum_stats_entry->n_dead_records
      > vacuum_stats_entry->n_total_records)
    {
      er_log_debug (ARG_FILE_LINE, "Warning: vacuum statistics are off: "
		    "n_dead_records = %d, n_total_records = %d",
		    vacuum_stats_entry->n_dead_records,
		    vacuum_stats_entry->n_total_records);
    }
#endif /* !NDEBUG */

  /* TODO: Sometimes we need to flush these statistics to heap page */

  csect_exit (thread_p, CSECT_VACUUM_STATS);
  return NO_ERROR;
}

/*
 * vacuum_stats_table_remove_entry () - Removes an entry from vacuum stats
 *					table (e.g. when a class is dropped).
 *
 * return	  : Error code.
 * thread_p (in)  : Thread entry.
 * class_oid (in) : Class object identifier (and key for table entry).
 */
int
vacuum_stats_table_remove_entry (OID * class_oid)
{
  assert (class_oid != NULL && !OID_ISNULL (class_oid));

  if (csect_enter (NULL, CSECT_VACUUM_STATS, INF_WAIT) != NO_ERROR)
    {
      assert (0);
      return ER_FAILED;
    }
  if (vacuum_Stats.hash_table == NULL)
    {
      /* No vacuum statistics table */
      return NO_ERROR;
    }

  (void) mht_rem (vacuum_Stats.hash_table, (void *) class_oid,
		  vacuum_free_stats_table_entry, NULL);
  csect_exit (NULL, CSECT_VACUUM_STATS);

  return NO_ERROR;
}

/*
 * vacuum_free_stats_table_entry () - Function that will free an entry in
 *				      vacuum stats table. The entry is added
 *				      to vacuum_Stats.free_entries.
 *
 * return    : Error code. 
 * key (in)  : Pointer to key value.
 * data (in) : Pointer to data.
 * args (in) : Unused.
 */
static int
vacuum_free_stats_table_entry (const void *key, void *data, void *args)
{
  VACUUM_STATISTICS *vacuum_stats_entry = NULL;
  assert (key != NULL && data != NULL);
  assert (key == data);

  vacuum_stats_entry = (VACUUM_STATISTICS *) data;
  vacuum_free_stats_entry (vacuum_stats_entry);
  return NO_ERROR;
}

/*
 * vacuum_free_stats_entry () - Adds entry to free_entries list.
 *
 * return		   : Void
 * vacuum_stats_entry (in) : Vacuum statistics entry to free.
 */
static void
vacuum_free_stats_entry (VACUUM_STATISTICS * vacuum_stats_entry)
{
  assert (vacuum_stats_entry != NULL);
  vacuum_stats_entry->next = vacuum_Stats.free_entries;
  vacuum_Stats.free_entries = vacuum_stats_entry;
}

/*
 * db_private_free_class_oid_list () - Free a list of class oids.
 *
 * return	     : Void.
 * cls_oid_list (in) : List of class oid's.
 */
static void
db_private_free_class_oid_list (THREAD_ENTRY * thread_p,
				CLASS_OID_LIST * cls_oid_list)
{
  if (cls_oid_list == NULL)
    {
      return;
    }
  if (cls_oid_list->next != NULL)
    {
      db_private_free_class_oid_list (thread_p, cls_oid_list->next);
    }
  db_private_free_and_init (thread_p, cls_oid_list);
}

/*
 * vacuum_running_vacuums_initialize () - Init running vacuum structures.
 *
 * return	 : Void.
 * thread_p (in) : Thread entry.
 */
static void
vacuum_running_vacuums_initialize (THREAD_ENTRY * thread_p)
{
  current_running_vacuums = NULL;
  running_vacuum_pool = NULL;
  is_vacuum_allowed = true;
  pthread_mutex_init (&running_vacuums_mutex, NULL);
}

/*
 * vacuum_running_vacuums_finalize () - Finalize running vacuum structures.
 *
 * return	 : Void.
 * thread_p (in) : Thread entry.
 */
static void
vacuum_running_vacuums_finalize (THREAD_ENTRY * thread_p)
{
  RUNNING_VACUUM *entry = NULL, *save_next = NULL;

  /* Abort running vacuums */
  vacuum_running_vacuums_abort_all (thread_p);
  assert (current_running_vacuums == NULL);

  pthread_mutex_lock (&running_vacuums_mutex);

  /* Block any newly starting vacuums */
  is_vacuum_allowed = false;

  for (entry = running_vacuum_pool; entry != NULL; entry = save_next)
    {
      save_next = entry->next;
      pthread_mutex_destroy (&entry->mutex);
      free_and_init (entry);
    }
  running_vacuum_pool = NULL;

  pthread_mutex_unlock (&running_vacuums_mutex);
  pthread_mutex_destroy (&running_vacuums_mutex);
}

/*
 * vacuum_running_vacuums_abort_all () - Notify all running vacuums to abort
 *					 and wait until they are all aborted.
 *
 * return	 : Void.
 * thread_p (in) : Thread entry.
 */
static void
vacuum_running_vacuums_abort_all (THREAD_ENTRY * thread_p)
{
#if defined (SERVER_MODE)
  RUNNING_VACUUM *entry = NULL;

  /* Lock mutex to access current_running_vacuums */
  pthread_mutex_lock (&running_vacuums_mutex);
  for (entry = current_running_vacuums; entry != NULL; entry = entry->next)
    {
      /* Lock mutex on running vacuum entry */
      pthread_mutex_lock (&entry->mutex);
      /* Notify to abort */
      entry->abort = true;
      /* Unlock mutex on running vacuum entry */
      pthread_mutex_unlock (&entry->mutex);
    }

  /* Now wait for all running vacuums to abort. When all are aborted,
   * current_running_vacuums should be NULL.
   */
  while (current_running_vacuums != NULL)
    {
      /* There are still running vacuums */
      pthread_mutex_unlock (&running_vacuums_mutex);
      thread_sleep (50);
      pthread_mutex_lock (&running_vacuums_mutex);
    }

  /* All vacuums have been aborted */
  pthread_mutex_unlock (&running_vacuums_mutex);
#endif /* SERVER_MODE */
}

/*
 * vacuum_running_vacuums_allocate_entry () - Allocate a new entry for a
 *					      running vacuum.
 *
 * return	 : New running vacuum entry.
 * thread_p (in) : Thread entry.
 *
 * NOTE: First will try to find a free entry in the running vacuum pool.
 */
static RUNNING_VACUUM *
vacuum_running_vacuums_allocate_entry (THREAD_ENTRY * thread_p,
				       OID * class_oid)
{
  RUNNING_VACUUM *new_entry = NULL;

  if (running_vacuum_pool != NULL)
    {
      new_entry = running_vacuum_pool;
      running_vacuum_pool = running_vacuum_pool->next;
    }
  else
    {
      new_entry = (RUNNING_VACUUM *) malloc (sizeof (RUNNING_VACUUM));
      /* This entry will be used for the first time, must initialize mutex */
      pthread_mutex_init (&new_entry->mutex, NULL);
    }

  /* Initialize entry */
  COPY_OID (&new_entry->class_oid, class_oid);
  new_entry->interrupt = false;
  new_entry->abort = false;

  /* Link entry to current running vacuums */
  new_entry->next = current_running_vacuums;
  current_running_vacuums = new_entry;

  return new_entry;
}

/*
 * vacuum_running_vacuum_free_entry () - Frees a running vacuum entry when
 *					 a vacuum is finished. The entry is
 *					 removed from current_running_vacuums
 *					 and added to running_vacuum_pool.
 *
 * return	 : Void.
 * thread_p (in) : Thread entry.
 * entry (in)	 : Running vacuum entry.
 *
 * NOTE: Make sure lock on entry is not held when calling free.
 */
static void
vacuum_running_vacuum_free_entry (THREAD_ENTRY * thread_p,
				  RUNNING_VACUUM * entry)
{
  RUNNING_VACUUM *prev_entry = NULL, *crt_entry = NULL, *save_next = NULL;

  for (crt_entry = current_running_vacuums; crt_entry != NULL;
       crt_entry = crt_entry->next)
    {
      if (crt_entry != NULL)
	{
	  break;
	}
      prev_entry = crt_entry;
    }
  if (crt_entry == NULL)
    {
      assert (false);
      return;
    }

  pthread_mutex_lock (&entry->mutex);
  if (prev_entry == NULL)
    {
      current_running_vacuums = entry->next;
    }
  else
    {
      prev_entry->next = entry->next;
    }
  entry->next = running_vacuum_pool;
  running_vacuum_pool = entry;

  pthread_mutex_unlock (&entry->mutex);
}

/*
 * vacuum_is_class_vacuumed () - Checks if a vacuum on class_oid is already
 *				 running.
 *
 * return	  : True if vacuum on class_oid is already running.
 * thread_p (in)  : Thread entry.
 * class_oid (in) : Class object identifier.
 */
static bool
vacuum_is_class_vacuumed (THREAD_ENTRY * thread_p, const OID * class_oid)
{
  RUNNING_VACUUM *entry = NULL;

  assert (class_oid != NULL);

  for (entry = current_running_vacuums; entry != NULL; entry = entry->next)
    {
      if (OID_EQ (&entry->class_oid, class_oid))
	{
	  return true;
	}
    }
  return false;
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

  error_code = vacuum_stats_table_initialize (thread_p);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }
  vacuum_running_vacuums_initialize (thread_p);
  return error_code;
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
  vacuum_stats_table_finalize (thread_p);
  vacuum_running_vacuums_finalize (thread_p);
}
