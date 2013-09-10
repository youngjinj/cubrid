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
#include "mvcc.h"
#include "page_buffer.h"
#include "heap_file.h"
#include "boot_sr.h"
#include "system_parameter.h"

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

/* Heap table for vacuum statistics entries */
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

VACUUM_STATS_TABLE vacuum_Stats;

static void db_private_free_class_oid_list (THREAD_ENTRY * thread_p,
					    CLASS_OID_LIST * cls_oid_list);

static int vacuum_class (THREAD_ENTRY * thread_p, const OID * class_oid,
			 MVCCID lowest_active_mvccid);
static int auto_vacuum_validate_class (THREAD_ENTRY * thread_p, void *data,
				       void *args);
static int vacuum_flush_statistics (THREAD_ENTRY * thread_p, void *data,
				    void *args);
static int vacuum_expand_stats_table (THREAD_ENTRY * thread_p);
static int vacuum_free_stats_table_entry (const void *key, void *data,
					  void *args);
static void vacuum_free_stats_entry (VACUUM_STATISTICS * vacuum_stats_entry);

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
vacuum_class (THREAD_ENTRY * thread_p, const OID * class_oid,
	      MVCCID lowest_active_mvccid)
{
  VPID vpid;
  PAGE_PTR page_p = NULL;
  HFID hfid;
  int error;
  VACUUM_STATISTICS *vacuum_stats_entry = NULL;
  VACUUM_PAGE_DATA vacuum_page_data;
  int n_total_cleaned_records;

  /* Get heap file identifier for this class */
  error = heap_get_hfid_from_class_oid (thread_p, class_oid, &hfid);
  if (error != NO_ERROR)
    {
      return error;
    }

  /* Get first page */
  vpid.pageid = hfid.hpgid;
  vpid.volid = hfid.vfid.volid;

  n_total_cleaned_records = 0;
  while (!VPID_ISNULL (&vpid))
    {
      /* Unfix last page */
      if (page_p != NULL)
	{
	  pgbuf_unfix_and_init (thread_p, page_p);
	}

      /* Fix next page */
      page_p =
	pgbuf_fix (thread_p, &vpid, OLD_PAGE, PGBUF_LATCH_READ,
		   PGBUF_UNCONDITIONAL_LATCH);
      if (page_p == NULL)
	{
	  return ER_FAILED;
	}

      /* Save the vpid of next page */
      if (heap_vpid_next (&hfid, page_p, &vpid) != NO_ERROR)
	{
	  pgbuf_unfix_and_init (thread_p, page_p);
	  return ER_FAILED;
	}

      /* TODO: check page visibility */

      /* Clean current page */
      if (heap_vacuum_page (thread_p, &hfid, &page_p, lowest_active_mvccid,
			    false, &vacuum_page_data) != NO_ERROR)
	{
	  pgbuf_unfix_and_init (thread_p, page_p);
	  return ER_FAILED;
	}
      n_total_cleaned_records +=
	(vacuum_page_data.n_dead + vacuum_page_data.n_relocations);
      /* The whole implementation should be like this:
       * heap_vacuum_page should collect data on records that should die and
       * records already dead. All these records must be removed from all
       * indexes. Afterwards, they must be marked as unused.
       */
      /* TODO: remove dead OID's from b-tree's */
      /* TODO: mark dead records as unused */
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

      /* Clean vacuum_page_data */
      /* TODO: allocate a big enough vacuum data from the start and avoid
       * memory allocations for each page. But this means a memory of
       * (PAGE_SIZE / SLOT_SIZE) * (PGSLOTID * 2 + VPID_SIZE + OID_SIZE).
       * For a 16k page => 96k memory 
       */
      spage_finalize_vacuum_data (thread_p, &vacuum_page_data);
    }

  /* Unfix the last page */
  if (page_p != NULL)
    {
      pgbuf_unfix_and_init (thread_p, page_p);
    }

  return vacuum_stats_table_update_entry (thread_p, class_oid, &hfid,
					  -n_total_cleaned_records,
					  -n_total_cleaned_records);
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
      (void) vacuum_class (thread_p, &entry->class_oid,
			   logtb_get_lowest_active_mvccid (thread_p));
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
int
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
void
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
