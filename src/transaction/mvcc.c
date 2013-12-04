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
 * mvcc_snapshot.c - mvcc snapshot
 */

#ident "$Id$"

#include "mvcc.h"
#include "dbtype.h"
#include "heap_file.h"
#include "page_buffer.h"
#include "overflow_file.h"

#define MVCC_IS_REC_INSERTER_ACTIVE(thread_p, rec_header_p) \
  (logtb_is_active_mvccid (thread_p, (rec_header_p)->mvcc_ins_id))

#define MVCC_IS_REC_DELETER_ACTIVE(thread_p, rec_header_p) \
  (logtb_is_active_mvccid (thread_p, (rec_header_p)->mvcc_del_id))

#define MVCC_IS_REC_INSERTER_IN_SNAPSHOT(thread_p, rec_header_p, snapshot) \
  (mvcc_is_id_in_snapshot (thread_p, (rec_header_p)->mvcc_ins_id, snapshot))

#define MVCC_IS_REC_DELETER_IN_SNAPSHOT(thread_p, rec_header_p, snapshot) \
  (mvcc_is_id_in_snapshot (thread_p, (rec_header_p)->mvcc_del_id, snapshot))

#define MVCC_IS_REC_INSERTED_SINCE_MVCCID(rec_header_p, mvcc_id) \
  (!mvcc_id_precedes ((rec_header_p)->mvcc_ins_id, mvcc_id))

#define MVCC_IS_REC_DELETED_SINCE_MVCCID(rec_header_p, mvcc_id) \
  (!mvcc_id_precedes ((rec_header_p)->mvcc_del_id, mvcc_id))


/* Used by mvcc_chain_satisfies_vacuum to avoid handling the same OID twice */
enum
{
  /* Any positive value should be an index in relocated slots array */
  NOT_VISITED = -1,
  VISITED_DEAD = -2,
  VISITED_ALIVE = -3
};

/* the lowest active mvcc id computed for last */
/* MVCCID recent_snapshot_lowest_active_mvccid = MVCCID_NULL; */

static bool mvcc_is_id_in_snapshot (THREAD_ENTRY * thread_p,
				    MVCCID mvcc_id, MVCC_SNAPSHOT * snapshot);

/*
 * mvcc_is_id_in_snapshot () - check whether mvcc id is in snapshot -
 *                             is mvcc id active from snapshot point of view?
 *   return: true/false
 *   thread_p(in): thread entry
 *   mvcc_id(in): mvcc id
 *   snapshot(in): mvcc snapshot
 */
static bool
mvcc_is_id_in_snapshot (THREAD_ENTRY * thread_p, MVCCID mvcc_id,
			MVCC_SNAPSHOT * snapshot)
{
  unsigned int i;
  assert (snapshot != NULL);

  if (mvcc_id_precedes (mvcc_id, snapshot->lowest_active_mvccid))
    {
      /* mvcc id is not active */
      return false;
    }

  if (mvcc_id_follow_or_equal (mvcc_id, snapshot->highest_completed_mvccid))
    {
      /* mvcc id is active */
      return true;
    }

  /* TO DO - handle subtransactions */
  for (i = 0; i < snapshot->cnt_active_ids; i++)
    {
      if (MVCCID_IS_EQUAL (mvcc_id, snapshot->active_ids[i]))
	{
	  return true;
	}
    }

  return false;
}

/*
 * mvcc_satisfies_snapshot () - Check whether a record is valid for 
 *				    a snapshot
 *   return: true, if the record is valid for snapshot
 *   thread_p(in): thread entry
 *   rec_header(out): the record header
 *   snapshot(in): the snapshot used for record validation
 *   page_ptr(in): the page where the record reside
 */
bool
mvcc_satisfies_snapshot (THREAD_ENTRY * thread_p,
			 MVCC_REC_HEADER * rec_header,
			 MVCC_SNAPSHOT * snapshot)
{
  assert (rec_header != NULL && snapshot != NULL);

  if (MVCC_IS_DISABLED (rec_header))
    {
      /* MVCC is disabled for this record so it is visible to everyone */
      return true;
    }

  if (!MVCC_IS_FLAG_SET (rec_header, MVCC_FLAG_VALID_DELID))
    {
      /* The record is not deleted */
      if (MVCC_IS_REC_INSERTED_BY_ME (thread_p, rec_header))
	{
	  /* Record was inserted by current transaction and is visible */
	  return true;
	}
      else if (MVCC_IS_REC_INSERTER_IN_SNAPSHOT (thread_p, rec_header,
						 snapshot))
	{
	  /* Record was inserted by an active transaction or by a transaction
	   * that has committed after snapshot was obtained.
	   */
	  return false;
	}
      else
	{
	  /* The inserter transaction has committed and the record is visible
	   * to current transaction.
	   */
	  return true;
	}
    }
  else
    {
      /* The record is deleted */
      if (MVCC_IS_REC_DELETED_BY_ME (thread_p, rec_header))
	{
	  /* The record was deleted by current transaction and it is not
	   * visible anymore.
	   */
	  return false;
	}
      else if (MVCC_IS_REC_DELETER_IN_SNAPSHOT (thread_p, rec_header,
						snapshot))
	{
	  /* The record was deleted by an active transaction or by a
	   * transaction that has committed after snapshot was obtained.
	   */
	  return true;
	}
      else
	{
	  /* The deleter transaction has committed and the record is not
	   * visible to current transaction.
	   */
	  return false;
	}
    }
}

/*
 * mvcc_satisfies_vacuum () - Check whether record satisfies VACUUM
 *
 * return	      : Heap record satisfies vacuum result.
 * thread_p (in)      : Thread entry.
 * rec_header (in)    : MVCC record header.
 * oldest_mvccid (in) : MVCCID for oldest active transaction.
 * page_p (in)	      : Heap page pointer.
 */
MVCC_SATISFIES_VACUUM_RESULT
mvcc_satisfies_vacuum (THREAD_ENTRY * thread_p, MVCC_REC_HEADER * rec_header,
		       MVCCID oldest_mvccid)
{
  if (MVCC_IS_DISABLED (rec_header))
    {
      /* do not vacuum this record ever */
      return VACUUM_RECORD_ALIVE;
    }

  if (!MVCC_IS_FLAG_SET (rec_header, MVCC_FLAG_VALID_DELID))
    {
      /* The record was not deleted */
      if (MVCC_IS_REC_INSERTED_SINCE_MVCCID (rec_header, oldest_mvccid))
	{
	  /* Record was recently inserted and may be still invisible to some
	   * active transactions.
	   */
	  return VACUUM_RECORD_RECENTLY_INSERTED;
	}
      else
	{
	  /* The inserter transaction has committed and the record is visible
	   * to all running transactions.
	   */
	  return VACUUM_RECORD_ALIVE;
	}
    }
  else
    {
      /* The record was deleted */
      if (MVCC_IS_REC_DELETED_SINCE_MVCCID (rec_header, oldest_mvccid))
	{
	  /* Record was recently deleted and may still be visible to some
	   * active transactions.
	   */
	  return VACUUM_RECORD_RECENTLY_DELETED;
	}
      else
	{
	  /* The deleter transaction has committed and the record is not
	   * visible to any running transactions.
	   */
	  return VACUUM_RECORD_DEAD;
	}
    }
}

/*
 * mvcc_chain_satisfies_vacuum () - Checks if an object belongs to a chain
 *				    and decides what to do with the chain
 *				    (kill or relocate).
 *
 * return		     : Error code.
 * thread_p (in)	     : Thread entry.
 * page_p (in/out)	     : Page pointer to the first object page. May
 *			       change if the chain exits the page.
 * vacuum_page_vpid (in/out) : VPID for fist page (that is vacuumed).
 * slotid (in)		     : Slot identifier for the first object.
 * reuse_oid (in)	     : Reusable OIDs.
 * vacuum_page_only (in)     : Stay on page if true (when the chain exists the
 *			       page stop following it).
 * vacuum_data_p (in)	     : Clean data collector.
 * oldest_active (in)	     : MVCCID for oldest active transaction.
 *
 * NOTE: When records are updated in MVCC, the old record is just marked for
 *	 deletion, a new record is created and a link between the two is set.
 *	 Multiple updates will create a chain of records. VACUUM must handle
 *	 all intermediary stages based on what the status of the last chain
 *	 entry is. If OIDs are not reusable and the last chain entry is still
 *	 alive, all previous records must be replaced with relocated slots
 *	 that point to this last record. If last record is dead (or if slots
 *	 are reusable), all records in chain are "killed".
 *
 *	 The fixed page may change if chain exits the first page.
 *
 *	 The algorithm works like this:
 *	  FOR EACH record in chain:
 *	    IF record is dead: -> chain is dead, end chain;
 *	    ELSE IF record is visible -> chain is alive, end chain;
 *					 break loop;
 *	    IF record in page that is vacuumed -> save record to handle later;
 *	    IF reusable -> end chain;
 *	    go to next record;
 *	  IF chain is dead or reusable -> kill all saved records.
 *	  ELSE -> relocate all saved records to the end of chain.
 */
int
mvcc_chain_satisfies_vacuum (THREAD_ENTRY * thread_p, PAGE_PTR * page_p,
			     VPID vacuum_page_vpid, PGSLOTID slotid,
			     bool reuse_oid, bool vacuum_page_only,
			     VACUUM_PAGE_DATA * vacuum_data_p,
			     MVCCID oldest_active)
{
#define DEFAULT_CHAIN_SIZE 32

  int rec_type;
  RECDES recdes;
  SPAGE_SLOT *slotp = NULL;
  PAGE_PTR ovf_page_p = NULL;
  VPID fixed_vpid, ovf_vpid;
  OID forward_oid, current_oid, relocation_dest;
  MVCC_REC_HEADER mvcc_header;
  MVCC_SATISFIES_VACUUM_RESULT satisfies_vacuum;
  bool is_dead = false, is_on_vacuum_page = false;
  int visited_status;

  PGSLOTID static_chain[DEFAULT_CHAIN_SIZE];
  PGSLOTID *chain = NULL, *new_chain = NULL;
  int n_chain_entries, i;
  int chain_size = DEFAULT_CHAIN_SIZE, new_chain_size;

  int error = NO_ERROR;

#if !defined(NDEBUG)
  int nslots;
#endif

  if (vacuum_data_p->visited[slotid] != NOT_VISITED)
    {
      /* Current object was already handled */
      return NO_ERROR;
    }

  /* Initialize current_oid to point to first object */
  current_oid.pageid = vacuum_page_vpid.pageid;
  current_oid.volid = vacuum_page_vpid.volid;
  current_oid.slotid = slotid;

  /* Initialize fixed_vpid */
  assert (page_p != NULL);
  if (*page_p != NULL)
    {
      pgbuf_get_vpid (*page_p, &fixed_vpid);
    }
  else
    {
      VPID_SET_NULL (&fixed_vpid);
    }
  assert (*page_p != NULL);

#ifndef NDEBUG
  if (!VPID_EQ (&fixed_vpid, &vacuum_page_vpid))
    {
      if (*page_p != NULL)
	{
	  pgbuf_unfix_and_init (thread_p, *page_p);
	}
      *page_p =
	pgbuf_fix (thread_p, &vacuum_page_vpid, OLD_PAGE, PGBUF_LATCH_READ,
		   PGBUF_UNCONDITIONAL_LATCH);
    }
  nslots = spage_number_of_slots (*page_p);
#endif /* !NDEBUG */

  /* Initialize chain array */
  n_chain_entries = 0;
  chain = static_chain;

  /* Start looking for an end of the chain (first visible record) */
  /* After this loop, the built update chain will be handled */
  do
    {
      /* Make sure the page with current oid is fixed */
      if (fixed_vpid.volid != current_oid.volid
	  || fixed_vpid.pageid != current_oid.pageid)
	{
	  /* Unfix old page and fix new page */
	  if (*page_p != NULL)
	    {
	      pgbuf_unfix_and_init (thread_p, *page_p);
	    }

	  fixed_vpid.volid = current_oid.volid;
	  fixed_vpid.pageid = current_oid.pageid;
	  *page_p =
	    pgbuf_fix (thread_p, &fixed_vpid, OLD_PAGE, PGBUF_LATCH_READ,
		       PGBUF_UNCONDITIONAL_LATCH);
	  if (*page_p == NULL)
	    {
	      goto error;
	    }
	}

      is_on_vacuum_page = VPID_EQ (&fixed_vpid, &vacuum_page_vpid);

      if (is_on_vacuum_page)
	{
	  /* Check if the current element in chain was already handled */
	  visited_status = vacuum_data_p->visited[current_oid.slotid];

	  if (visited_status == VISITED_DEAD)
	    {
	      /* The entire chain must die */
	      is_dead = true;
	      /* Break loop */
	      break;
	    }
	  else if (visited_status == VISITED_ALIVE)
	    {
	      /* The entire chain will be relocated to current object */
	      COPY_OID (&relocation_dest, &current_oid);
	      /* Break loop */
	      break;
	    }
	  else if (visited_status >= 0)
	    {
	      /* Visited_status is a relocation index for current object */
	      assert (visited_status < vacuum_data_p->n_relocations);
	      /* The entire chain will be relocated to the same destination
	       * as the current object.
	       */
	      COPY_OID (&relocation_dest,
			&vacuum_data_p->relocations[visited_status]);
	      /* Break loop */
	      break;
	    }
	  else
	    {
	      /* Not visited */
	      /* Safety check */
	      assert (visited_status == NOT_VISITED);
	    }
	}

      /* Handle current object */
      /* Get current record type */
      slotp = spage_get_slot (*page_p, current_oid.slotid);
      if (slotp == NULL)
	{
	  assert (0);
	  goto error;
	}
      rec_type = slotp->record_type;

      /* The chain must be stopped if the current object is alive (visible)
       * or if it dead/deleted and it doesn't have any next version.
       * After these checks, current oid is added to chain. To avoid that
       * just break loop before.
       */
      if (rec_type == REC_DEAD || rec_type == REC_DELETED_WILL_REUSE
	  || rec_type == REC_MARKDELETED)
	{
	  /* The chain ends in a dead/removed record */
	  is_dead = true;

	  if (rec_type != REC_DEAD)
	    {
	      /* Force exit to avoid adding this element to chain */
	      break;
	    }
	}
      else if (rec_type == REC_ASSIGN_ADDRESS)
	{
	  /* Stop here */
	  COPY_OID (&relocation_dest, &current_oid);
	  if (is_on_vacuum_page)
	    {
	      /* Mark as visited */
	      vacuum_data_p->visited[current_oid.slotid] = VISITED_ALIVE;
	    }
	  break;
	}
      else if (rec_type == REC_RELOCATION)
	{
	  /* Save current entry in chain array and keep looking */

	  if (reuse_oid)
	    {
	      /* Reusable slots are not replaced with REC_RELOCATION */
	      assert (0);
	      goto error;
	    }

	  /* Get forward OID */
	  recdes.data = (char *) &forward_oid;
	  recdes.area_size = OR_OID_SIZE;
	  recdes.length = OR_OID_SIZE;
	  if (spage_get_record (*page_p, current_oid.slotid, &recdes, COPY)
	      != S_SUCCESS)
	    {
	      assert (0);
	      goto error;
	    }
	}
      else if (rec_type == REC_NEWHOME)
	{
	  /* TODO: Temporarily ignore this case, need more investigation for
	   *       a proper fix.
	   */
	  break;
	}
      else
	{
	  /* Must be a HOME/BIG_ONE record */
	  assert (rec_type == REC_BIGONE || rec_type == REC_HOME);

	  /* Get record data */
	  if (rec_type == REC_BIGONE)
	    {
	      /* First we must go to overflow page */
	      /* Get forward oid */
	      recdes.data = (void *) &forward_oid;
	      recdes.area_size = OR_OID_SIZE;
	      recdes.length = OR_OID_SIZE;
	      if (spage_get_record
		  (*page_p, current_oid.slotid, &recdes, COPY) != S_SUCCESS)
		{
		  assert (0);
		  goto error;
		}
	      ovf_vpid.pageid = forward_oid.pageid;
	      ovf_vpid.volid = forward_oid.volid;
	      ovf_page_p =
		pgbuf_fix (thread_p, &ovf_vpid, OLD_PAGE, PGBUF_LATCH_READ,
			   PGBUF_UNCONDITIONAL_LATCH);
	      if (ovf_page_p == NULL)
		{
		  assert (0);
		  goto error;
		}
	      recdes.data = overflow_get_first_page_data (ovf_page_p);
	    }
	  else if (rec_type == REC_HOME)
	    {
	      if (spage_get_record
		  (*page_p, current_oid.slotid, &recdes, PEEK) != S_SUCCESS)
		{
		  assert (0);
		  goto error;
		}
	    }

	  /* Get MVCC header from records data */
	  or_mvcc_get_header (&recdes, &mvcc_header);

	  /* Overflow page is not needed anymore */
	  if (ovf_page_p != NULL)
	    {
	      /* Overflow page is not needed anymore */
	      pgbuf_unfix_and_init (thread_p, ovf_page_p);
	    }

	  /* Check satisfies vacuum */
	  satisfies_vacuum =
	    mvcc_satisfies_vacuum (thread_p, &mvcc_header, oldest_active);

	  if (satisfies_vacuum == VACUUM_RECORD_DEAD)
	    {
	      /* Record was deleted, the deleting transaction committed and
	       * the old version is not visible to any active transactions.
	       */
	      if (OID_ISNULL (&MVCC_GET_NEXT_VERSION (&mvcc_header)))
		{
		  /* This is last in chain */
		  is_dead = true;
		}
	      else
		{
		  /* Must advance in update chain */
		  COPY_OID (&forward_oid,
			    &MVCC_GET_NEXT_VERSION (&mvcc_header));
		}
	      if (rec_type == REC_BIGONE)
		{
		  /* Overflow pages must be released */
		  VPID_COPY (&vacuum_data_p->
			     ovfl_pages[vacuum_data_p->n_ovfl_pages],
			     &ovf_vpid);
		  vacuum_data_p->n_ovfl_pages++;
		}
	    }
	  else if (satisfies_vacuum == VACUUM_RECORD_ALIVE
		   /* Record is deleted. However it may be still visible to
		    * running transactions.
		    */
		   || satisfies_vacuum == VACUUM_RECORD_RECENTLY_DELETED
		   /* Record was recently inserted. The inserter is still
		    * active or has recently committed. The record is visible
		    * at least to the inserter, yet may be invisible to other
		    * active transactions.
		    */
		   || satisfies_vacuum == VACUUM_RECORD_RECENTLY_INSERTED)
	    {
	      /* The chain is still alive and all entries must be relocated
	       * to this object
	       */
	      COPY_OID (&relocation_dest, &current_oid);

	      if (is_on_vacuum_page)
		{
		  /* Mark this object as visited */
		  vacuum_data_p->visited[current_oid.slotid] = VISITED_ALIVE;

		  if (satisfies_vacuum != VACUUM_RECORD_ALIVE)
		    {
		      /* Recently inserted/deleted which means that it may not
		       * be visible to all active transactions.
		       */
		      vacuum_data_p->all_visible = false;
		    }
		}

	      /* Stop looking */
	      break;
	    }
	  else
	    {
	      /* Unhandled case */
	      assert (0);
	    }
	}

      /* Add entry to chain if it belongs to the first page (which is currently
       * vacuumed).
       */
      if (is_on_vacuum_page)
	{
	  if (n_chain_entries >= chain_size)
	    {
	      /* Must extend chain array */
	      new_chain_size = (chain_size << 1);

	      if (chain == static_chain)
		{
		  new_chain =
		    (PGSLOTID *) db_private_alloc (thread_p,
						   new_chain_size *
						   sizeof (PGSLOTID));
		}
	      else
		{
		  new_chain =
		    (PGSLOTID *) db_private_realloc (thread_p, chain,
						     new_chain_size *
						     sizeof (PGSLOTID));
		}
	      if (new_chain == NULL)
		{
		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			  ER_OUT_OF_VIRTUAL_MEMORY, 1,
			  new_chain_size * sizeof (PGSLOTID));
		  goto error;
		}
	      /* Copy old chain data */
	      memcpy (new_chain, chain, chain_size * sizeof (PGSLOTID));

	      /* Update chain pointer and size */
	      chain = new_chain;
	      chain_size = new_chain_size;
	    }
	  chain[n_chain_entries++] = current_oid.slotid;
	}

      if (reuse_oid || is_dead)
	{
	  /* Stop search */
	  break;
	}

      /* Advance to forward oid */
      COPY_OID (&current_oid, &forward_oid);

      if (vacuum_page_only && (fixed_vpid.pageid != current_oid.pageid
			       || fixed_vpid.volid != current_oid.volid))
	{
	  /* We don't want to follow up the entire chain, stop when it exits
	   * the first page.
	   */
	  /* Relocate the chain to this object */
	  COPY_OID (&relocation_dest, &current_oid);
	  break;
	}
    }
  while (true);

  /* Should be at the end of the chain, handle all intermediate entries */
  if (n_chain_entries == 0)
    {
      /* Nothing to do, fall through */
    }
  else
    {
      /* All chain entries must either all "die" or all be relocated.
       * If the chain is dead or if OID's are reusable, kill the entire chain.
       * Otherwise, all object will point to relocated_dest
       */
      if (is_dead || reuse_oid)
	{
	  /* All chain entries are dead */
	  for (i = 0; i < n_chain_entries; i++)
	    {
	      vacuum_data_p->dead_slots[vacuum_data_p->n_dead++] = chain[i];

	      /* Mark as visited */
	      vacuum_data_p->visited[chain[i]] = VISITED_DEAD;

#ifndef NDEBUG
	      assert (chain[i] >= 0 && chain[i] < nslots);
	      assert (vacuum_data_p->n_dead <= nslots);
#endif /* !NDEBUG */
	    }
	}
      else
	{
	  /* Relocate all chain entries to relocation_dest */
	  assert (!OID_ISNULL (&relocation_dest));
	  for (i = 0; i < n_chain_entries; i++)
	    {
	      vacuum_data_p->relocated_slots[vacuum_data_p->n_relocations] =
		chain[i];
	      COPY_OID (&vacuum_data_p->
			relocations[vacuum_data_p->n_relocations],
			&relocation_dest);
	      /* Mark as visited and save the index in relocations arrays */
	      vacuum_data_p->visited[chain[i]] = vacuum_data_p->n_relocations;
	      /* Advance in relocations arrays */
	      vacuum_data_p->n_relocations++;

#ifndef NDEBUG
	      assert (chain[i] >= 0 && chain[i] < nslots);
	      assert (vacuum_data_p->n_relocations <= nslots);
#endif /* !NDEBUG */
	    }
	}
      vacuum_data_p->vacuum_needed = true;
    }

end:
  /* Cleanup */
  if (chain != NULL && chain != static_chain)
    {
      db_private_free (thread_p, chain);
    }

  return error;

error:
  error = (error == NO_ERROR) ? ER_FAILED : error;
  goto end;
}

/*
 * mvcc_satisfies_delete () - Check whether a record is valid for 
 *			instant snapshot
 *   return: true, if the record is valid for snapshot
 *   thread_p(in): thread entry
 *   rec_header(out): the record header
 *   snapshot(in): the snapshot used for record validation
 *   page_ptr(in): the page where the record reside
 *
 * Note: The function return a complex result since delete/update commands
 *	    needs to know not only if the row is visible or not
 */
MVCC_SATISFIES_DELETE_RESULT
mvcc_satisfies_delete (THREAD_ENTRY * thread_p, MVCC_REC_HEADER * rec_header)
{
  assert (rec_header != NULL);

  if (MVCC_IS_DISABLED (rec_header))
    {
      return DELETE_RECORD_CAN_DELETE;
    }

  if (!MVCC_IS_FLAG_SET (rec_header, MVCC_FLAG_VALID_DELID))
    {
      /* Record was not deleted */
      if (MVCC_IS_REC_INSERTED_BY_ME (thread_p, rec_header))
	{
	  /* Record is only visible to current transaction and can be safely
	   * deleted.
	   */
	  return DELETE_RECORD_CAN_DELETE;
	}
      else if (MVCC_IS_REC_INSERTER_ACTIVE (thread_p, rec_header))
	{
	  /* Record is inserted by an active transaction and is not visible to
	   * current transaction.
	   */
	  return DELETE_RECORD_INVISIBLE;
	}
      else
	{
	  /* The inserter transaction has committed and the record can be
	   * deleted by current transaction.
	   */
	  return DELETE_RECORD_CAN_DELETE;
	}
    }
  else
    {
      /* Record was already deleted */
      if (MVCC_IS_REC_DELETED_BY_ME (thread_p, rec_header))
	{
	  /* Record was already deleted by me... This case should be filtered
	   * by scan phase and it should never get here. Be conservative.
	   */
	  return DELETE_RECORD_DELETED;
	}
      else if (MVCC_IS_REC_DELETER_ACTIVE (thread_p, rec_header))
	{
	  /* Record was deleted by an active transaction. Current transaction
	   * must wait until the deleter completes.
	   */
	  return DELETE_RECORD_IN_PROGRESS;
	}
      else
	{
	  /* Record was already deleted and the deleter has committed. Cannot
	   * be updated by current transaction.
	   */
	  return DELETE_RECORD_DELETED;
	}
    }
}

/*
 * mvcc_satisfies_dirty () - Check whether a record is visible considering
 *			    following effects:
 *			      - committed transactions
 *			      - in progress transactions
 *			      - previous commands of current transaction
 *				    
 *   return: true, if the record is valid for snapshot
 *   thread_p(in): thread entry
 *   rec_header(out): the record header
 *   snapshot(in): the snapshot used for record validation
 *   page_ptr(in): the page where the record reside
 * Note: snapshot->lowest_active_mvccid and snapshot->highest_completed_mvccid 
 *    are set as a side effect. Thus, snapshot->lowest_active_mvccid is set 
 *    to tuple insert id when it is the id of another active transaction, otherwise 
 *    is set to MVCCID_NULL
 */
bool
mvcc_satisfies_dirty (THREAD_ENTRY * thread_p,
		      MVCC_REC_HEADER * rec_header, MVCC_SNAPSHOT * snapshot)
{
  assert (rec_header != NULL && snapshot != NULL);

  snapshot->lowest_active_mvccid = snapshot->highest_completed_mvccid =
    MVCCID_NULL;

  if (MVCC_IS_DISABLED (rec_header))
    {
      return true;
    }

  if (!MVCC_IS_FLAG_SET (rec_header, MVCC_FLAG_VALID_DELID))
    {
      /* Record was not deleted */
      if (MVCC_IS_REC_INSERTED_BY_ME (thread_p, rec_header))
	{
	  /* Record was inserted by current transaction and is visible */
	  return true;
	}
      else if (MVCC_IS_REC_INSERTER_ACTIVE (thread_p, rec_header))
	{
	  /* Record is inserted by an active transaction and is visible */
	  snapshot->lowest_active_mvccid = MVCC_GET_INSID (rec_header);
	  return true;
	}
      else
	{
	  /* Record is inserted by committed transaction. */
	  return true;
	}
    }
  else
    {
      /* Record was already deleted */
      if (MVCC_IS_REC_DELETED_BY_ME (thread_p, rec_header))
	{
	  /* Record was deleted by current transaction and is not visible */
	  return false;
	}
      else if (MVCC_IS_REC_DELETER_ACTIVE (thread_p, rec_header))
	{
	  /* Record was deleted by other active transaction and is still visible
	   */
	  snapshot->highest_completed_mvccid = rec_header->mvcc_del_id;
	  return true;
	}
      else
	{
	  /* Record was already deleted and the deleter has committed. */
	  return false;
	}
    }
}

/*
 * mvcc_id_precedes - compare MVCC ids
 *
 * return: true, if id1 precede id2, false otherwise
 *
 *   id1(in): first MVCC id to compare
 *   id2(in): the second MVCC id to compare
 *
 */
bool
mvcc_id_precedes (MVCCID id1, MVCCID id2)
{
  int difference;

  if (!MVCCID_IS_NORMAL (id1) || !MVCCID_IS_NORMAL (id2))
    {
      return (id1 < id2);
    }

  difference = (int) (id1 - id2);
  return (difference < 0);
}

/*
 * mvcc_id_follow_or_equal - compare MVCC ids
 *
 * return: true, if id1 follow or equal id2, false otherwise
 *
 *   id1(in): first MVCC id to compare
 *   id2(in): the second MVCC id to compare
 *
 */
bool
mvcc_id_follow_or_equal (MVCCID id1, MVCCID id2)
{
  int difference;

  if (!MVCCID_IS_NORMAL (id1) || !MVCCID_IS_NORMAL (id2))
    {
      return (id1 >= id2);
    }

  difference = (int) (id1 - id2);
  return (difference >= 0);
}

/*
 * mvcc_allocate_vacuum_data () - Allocate data in vacuum page data to handle
 *				  up to n_slots entries.
 *
 * return		  : Error code.
 * thread_p (in)	  : Thread entry.
 * vacuum_data_p (in/out) : Pointer to vacuum page data.
 * int n_slots (in)	  : Number of slots.
 */
int
mvcc_allocate_vacuum_data (THREAD_ENTRY * thread_p,
			   VACUUM_PAGE_DATA * vacuum_data_p, int n_slots)
{
  vacuum_data_p->dead_slots =
    (PGSLOTID *) db_private_alloc (thread_p, n_slots * sizeof (PGSLOTID));
  vacuum_data_p->ovfl_pages =
    (VPID *) db_private_alloc (thread_p, n_slots * sizeof (VPID));
  vacuum_data_p->relocated_slots =
    (PGSLOTID *) db_private_alloc (thread_p, n_slots * sizeof (PGSLOTID));
  vacuum_data_p->relocations =
    (OID *) db_private_alloc (thread_p, n_slots * sizeof (OID));
  vacuum_data_p->visited =
    (int *) db_private_alloc (thread_p, n_slots * OR_INT_SIZE);

  if (vacuum_data_p->dead_slots == NULL
      || vacuum_data_p->ovfl_pages == NULL
      || vacuum_data_p->relocated_slots == NULL
      || vacuum_data_p->relocations == NULL || vacuum_data_p->visited == NULL)
    {
      mvcc_finalize_vacuum_data (thread_p, vacuum_data_p);
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1,
	      n_slots * (sizeof (PGSLOTID) * 2 + sizeof (VPID) +
			 sizeof (OID) + OR_INT_SIZE));
      return ER_OUT_OF_VIRTUAL_MEMORY;
    }
  return NO_ERROR;
}

/*
 * mvcc_init_vacuum_data () - Prepare a SPAGE_CLEAN_PAGE structure to handle
 *			      n_slots in a page.
 *
 * return	      : NO_ERROR or ER_OUT_OF_VIRTUAL_MEMORY
 * thread_p (in)      : Thread entry.
 * vacuum_data_p (in) : Pointer to VACUUM_PAGE_DATA.
 * int n_slots (in)   : Number of slots in page.
 */
void
mvcc_init_vacuum_data (THREAD_ENTRY * thread_p,
		       VACUUM_PAGE_DATA * vacuum_data_p, int n_slots)
{
  int i;

  vacuum_data_p->n_dead = 0;
  vacuum_data_p->n_ovfl_pages = 0;
  vacuum_data_p->n_relocations = 0;
  vacuum_data_p->n_vacuumed_records = 0;
  vacuum_data_p->vacuum_needed = false;
  vacuum_data_p->all_visible = true;

  /* Initialize all visited entries to NOT_VISITED */
  for (i = 0; i < n_slots; i++)
    {
      vacuum_data_p->visited[i] = NOT_VISITED;
    }
}

/*
 * mvcc_finalize_vacuum_data () - Frees the content of a VACUUM_PAGE_DATA.
 *
 * return	      : Void.
 * thread_p (in)      : Thread entry.
 * vacuum_data_p (in) : Pointer to VACUUM_PAGE_DATA.
 */
void
mvcc_finalize_vacuum_data (THREAD_ENTRY * thread_p,
			   VACUUM_PAGE_DATA * vacuum_data_p)
{
  if (vacuum_data_p->dead_slots != NULL)
    {
      db_private_free_and_init (thread_p, vacuum_data_p->dead_slots);
    }
  if (vacuum_data_p->ovfl_pages != NULL)
    {
      db_private_free_and_init (thread_p, vacuum_data_p->ovfl_pages);
    }
  if (vacuum_data_p->relocated_slots != NULL)
    {
      db_private_free_and_init (thread_p, vacuum_data_p->relocated_slots);
    }
  if (vacuum_data_p->relocations != NULL)
    {
      db_private_free_and_init (thread_p, vacuum_data_p->relocations);
    }
  if (vacuum_data_p->visited != NULL)
    {
      db_private_free_and_init (thread_p, vacuum_data_p->visited);
    }
}
