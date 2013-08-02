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

#include "heap_file.h"
#include "file_mvcc_status.h"
#include "mvcc.h"
#include "page_buffer.h"
#include "dbtype.h"
#include "overflow_file.h"

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
      if (MVCCID_IS_EQUAL (mvcc_id, snapshot->active_child_ids[i]))
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
			 MVCC_SNAPSHOT * snapshot, PAGE_PTR page)
{
  assert (rec_header != NULL && snapshot != NULL && page != NULL);

  if (rec_header->mvcc_flags & HEAP_MVCC_FLAG_DISABLED)
    {
      return true;
    }

  if (!(rec_header->mvcc_flags & HEAP_MVCC_FLAG_INSID_COMMITTED))
    {
      if (rec_header->mvcc_flags & HEAP_MVCC_FLAG_INSID_INVALID)
	{
	  /* mvcc insert id invalid/aborted */
	  return false;
	}

      if (logtb_is_current_mvccid (thread_p,
				   HEAP_GET_MVCC_INS_ID (rec_header)))
	{
#if defined(MVCC_USE_COMMAND_ID)
	  if (HEAP_GET_MVCC_INS_COMM_ID (rec_header) >=
	      MVCC_SNAPSHOT_GET_COMMAND_ID (snapshot))
	    {
	      /* inserted after scan started */
	      return false;
	    }
#endif /* MVCC_USE_COMMAND_ID */

	  if (rec_header->mvcc_flags & HEAP_MVCC_FLAG_DELID_INVALID)
	    {
	      /* mvcc delete id invald/aborted */
	      return true;
	    }

	  /* TO DO - locked ? */

	  if (!logtb_is_current_mvccid (thread_p,
					HEAP_GET_MVCC_DEL_ID (rec_header)))
	    {
	      heap_set_record_header_flag (thread_p, rec_header, page,
					   HEAP_MVCC_FLAG_DELID_INVALID,
					   MVCCID_NULL);
	      return true;
	    }

#if defined(MVCC_USE_COMMAND_ID)
	  if (HEAP_GET_MVCC_DEL_COMM_ID (rec_header) >=
	      MVCC_SNAPSHOT_GET_COMMAND_ID (snapshot))
	    {
	      /* deleted after scan start */
	      return true;
	    }
	  else
	    {
	      /* deleted before scan start */
	      return false;
	    }
#else /* !MVCC_USE_COMMAND_ID */
	  return false;
#endif /* !MVCC_USE_COMMAND_ID */
	}
      else if (logtb_is_active_mvccid (thread_p,
				       HEAP_GET_MVCC_INS_ID (rec_header)))
	{
	  /* record insertion not committed yet */
	  return false;
	}
      else if (logtb_is_mvccid_committed (thread_p,
					  HEAP_GET_MVCC_INS_ID (rec_header)))
	{
	  /* record insertion committed - set flag */
	  heap_set_record_header_flag (thread_p, rec_header, page,
				       HEAP_MVCC_FLAG_INSID_COMMITTED,
				       HEAP_GET_MVCC_INS_ID (rec_header));
	}
      else
	{
	  /* record insertion aborted - set flag */
	  heap_set_record_header_flag (thread_p, rec_header, page,
				       HEAP_MVCC_FLAG_INSID_INVALID,
				       MVCCID_NULL);
	  return false;
	}

    }

  /* record insertion already committed, need to check when was committed */
  if (mvcc_is_id_in_snapshot (thread_p, HEAP_GET_MVCC_INS_ID (rec_header),
			      snapshot))
    {
      /* insert mvcc id is active from snapshot point of view */
      return false;
    }

  if (rec_header->mvcc_flags & HEAP_MVCC_FLAG_DELID_INVALID)
    {
      /* not deleted or aborted */
      return true;
    }

  /* TO DO - locked ? */

  if (!(rec_header->mvcc_flags & HEAP_MVCC_FLAG_DELID_COMMITTED))
    {
      if (logtb_is_current_mvccid (thread_p,
				   HEAP_GET_MVCC_DEL_ID (rec_header)))
	{
#if defined(MVCC_USE_COMMAND_ID)
	  if (HEAP_GET_MVCC_DEL_COMM_ID (rec_header) >=
	      MVCC_SNAPSHOT_GET_COMMAND_ID (snapshot))
	    {
	      /* delete after scan start */
	      return true;
	    }
	  else
	    {
	      /* already deleted - before scan start */
	      return false;
	    }
#else /* !MVCC_USE_COMMAND_ID */
	  return false;
#endif /* !MVCC_USE_COMMAND_ID */
	}

      if (logtb_is_active_mvccid (thread_p,
				  HEAP_GET_MVCC_DEL_ID (rec_header)))
	{
	  /* record deletion not committed yet */
	  return true;
	}

      if (!logtb_is_mvccid_committed (thread_p,
				      HEAP_GET_MVCC_DEL_ID (rec_header)))
	{
	  /* record deletion aborted */
	  heap_set_record_header_flag (thread_p, rec_header, page,
				       HEAP_MVCC_FLAG_DELID_INVALID,
				       MVCCID_NULL);

	  return true;
	}

      heap_set_record_header_flag (thread_p, rec_header, page,
				   HEAP_MVCC_FLAG_DELID_COMMITTED,
				   HEAP_GET_MVCC_DEL_ID (rec_header));
    }

  /* record deletion already committed, need to check when was committed */
  if (mvcc_is_id_in_snapshot (thread_p, HEAP_GET_MVCC_DEL_ID (rec_header),
			      snapshot))
    {
      /* delete mvcc id is active from snapshot point of view */
      return true;
    }

  return false;
}

/*
 * mvcc_satisfies_vacuum () - Check whether record satisfies VACUUM
 *
 * return	      : Heap record satisfies vacuum result.
 * thread_p (in)      : Thread entry.
 * rec_header (in)    : MVCC record header.
 * oldest_mvccid (in) : MVCCID for oldest active transaction.
 * page_p (in)	      : Heap page pointer.
 *
 * NOTE: The condition for a record to be dead is:
 *	 (!HEAP_MVCC_IS_FLAG_SET (rec_header, HEAP_MVCC_FLAG_INSID_COMMITTED)
 *	      --> inserted record is not committed
 *	  && HEAP_MVCC_IS_FLAG_SET (rec_header, HEAP_MVCC_FLAG_INSID_INVALID))
 *	      -->  and insert was aborted
 *	 || (HEAP_MVCC_IS_FLAG_SET (rec_header, HEAP_MVCC_FLAG_DELID_COMMITTED)
 *	     && !mvcc_id_precedes (HEAP_GET_MVCC_DEL_ID (rec_header), oldest))
 *	      --> records was deleted and committed and is no longer
 *	      visible for any active transaction
 */
MVCC_SATISFIES_VACUUM_RESULT
mvcc_satisfies_vacuum (THREAD_ENTRY * thread_p, MVCC_REC_HEADER * rec_header,
		       MVCCID oldest_mvccid, PAGE_PTR page_p)
{
  if (HEAP_MVCC_IS_FLAG_SET (rec_header, HEAP_MVCC_FLAG_DISABLED))
    {
      /* do not vacuum this record ever */
      return VACUUM_RECORD_ALIVE;
    }

  if (!HEAP_MVCC_IS_FLAG_SET (rec_header, HEAP_MVCC_FLAG_INSID_COMMITTED))
    {
      /* The insert commit flag is not set */
      if (HEAP_MVCC_IS_FLAG_SET (rec_header, HEAP_MVCC_FLAG_INSID_INVALID))
	{
	  /* The inserting transaction was aborted */
	  return VACUUM_RECORD_DEAD;
	}
      else if (!mvcc_id_precedes (HEAP_GET_MVCC_INS_ID (rec_header),
				  oldest_mvccid))
	{
	  /* The record was recently inserted */
	  if (HEAP_MVCC_IS_FLAG_SET
	      (rec_header, HEAP_MVCC_FLAG_DELID_INVALID))
	    {
	      /* The record is not deleted */
	      return VACUUM_RECORD_RECENTLY_INSERTED;
	    }
	  /* The record was also deleted */
	  return VACUUM_RECORD_RECENTLY_DELETED;
	}
      else if (logtb_is_mvccid_committed
	       (thread_p, HEAP_GET_MVCC_INS_ID (rec_header)))
	{
	  /* The inserting transaction did commit and the flag is outdated */
	  heap_set_record_header_flag (thread_p, rec_header, page_p,
				       HEAP_MVCC_FLAG_INSID_COMMITTED,
				       HEAP_GET_MVCC_INS_ID (rec_header));
	}
      else
	{
	  /* The inserting transaction did not commit and it is not active,
	   * it must have been aborted, but the flag is outdated.
	   */
	  heap_set_record_header_flag (thread_p, rec_header, page_p,
				       HEAP_MVCC_FLAG_INSID_INVALID,
				       HEAP_GET_MVCC_INS_ID (rec_header));
	  return VACUUM_RECORD_DEAD;
	}
    }

  /* the inserter committed */

  if (HEAP_MVCC_IS_FLAG_SET (rec_header, HEAP_MVCC_FLAG_DELID_INVALID))
    {
      /* the record was never deleted */
      return VACUUM_RECORD_ALIVE;
    }

  if (!HEAP_MVCC_IS_FLAG_SET (rec_header, HEAP_MVCC_FLAG_DELID_COMMITTED))
    {
      /* the delete commit flag is not set */
      if (!mvcc_id_precedes (HEAP_GET_MVCC_DEL_ID (rec_header),
			     oldest_mvccid))
	{
	  /* the record was recently deleted */
	  return VACUUM_RECORD_RECENTLY_DELETED;
	}
      else if (logtb_is_mvccid_committed
	       (thread_p, HEAP_GET_MVCC_DEL_ID (rec_header)))
	{
	  /* the deleter committed, update flag */
	  heap_set_record_header_flag (thread_p, rec_header, page_p,
				       HEAP_MVCC_FLAG_DELID_COMMITTED,
				       HEAP_GET_MVCC_DEL_ID (rec_header));
	}
      else
	{
	  /* either aborted or crashed */
	  heap_set_record_header_flag (thread_p, rec_header, page_p,
				       HEAP_MVCC_FLAG_DELID_INVALID,
				       HEAP_GET_MVCC_DEL_ID (rec_header));
	  return VACUUM_RECORD_ALIVE;
	}
    }

  /* the deleter committed */

  if (!mvcc_id_precedes (HEAP_GET_MVCC_DEL_ID (rec_header), oldest_mvccid))
    {
      /* recently dead, the record may still be visible to some active
       * transactions.
       */
      return VACUUM_RECORD_RECENTLY_DELETED;
    }

  /* the record was deleted, the deleter committed and no active transactions
   * can see this record.
   * safe to remove.
   */
  return VACUUM_RECORD_DEAD;
}

/*
 * mvcc_chain_satisfies_vacuum () - Checks if an object belongs to a chain
 *				    and decides what to do with the chain
 *				    (kill or relocate).
 *
 * return		 : Error code.
 * thread_p (in)	 : Thread entry.
 * page_p (in/out)	 : Page pointer to the first object page. May change
 *			  if the chain exits the page.
 * page_vpid (in/out)	 : VPID for fist page (that is vacuumed).
 * slotid (in)		 : Slot identifier for the first object.
 * reuse_oid (in)	 : Reusable OIDs.
 * vacuum_page_only (in) : Stay on page if true (when the chain exists the
 *			   page stop following it).
 * vacuum_data_p (in)	 : Clean data collector.
 * oldest_active (in)	 : MVCCID for oldest active transaction.
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
			     VPID page_vpid, PGSLOTID slotid,
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
  OID forward_oid, current_oid;
  MVCC_REC_HEADER *mvcc_header = NULL;
  MVCC_SATISFIES_VACUUM_RESULT satisfies_vacuum;
  bool is_dead = false, end_chain = false;

  PGSLOTID static_chain[DEFAULT_CHAIN_SIZE];
  PGSLOTID *chain = NULL, *new_chain = NULL;
  int n_chain_entries, i;
  int chain_size = DEFAULT_CHAIN_SIZE, new_chain_size;

  int error = NO_ERROR;

  if (vacuum_data_p->visited[slotid])
    {
      /* Current object was already handled */
      return NO_ERROR;
    }

  /* Initialize current_oid to point to first object */
  current_oid.pageid = page_vpid.pageid;
  current_oid.volid = page_vpid.volid;
  current_oid.slotid = slotid;

  /* Make sure the first page is fixed */
  assert (page_p != NULL);
  if (*page_p != NULL)
    {
      pgbuf_get_vpid (*page_p, &fixed_vpid);
      if (!VPID_EQ (&page_vpid, &fixed_vpid))
	{
	  pgbuf_unfix (thread_p, *page_p);
	  *page_p =
	    pgbuf_fix (thread_p, &page_vpid, OLD_PAGE, PGBUF_LATCH_READ,
		       PGBUF_UNCONDITIONAL_LATCH);
	}
    }
  else
    {
      *page_p =
	pgbuf_fix (thread_p, &page_vpid, OLD_PAGE, PGBUF_LATCH_READ,
		   PGBUF_UNCONDITIONAL_LATCH);
    }
  assert (*page_p != NULL);

  /* Initialize chain array */
  n_chain_entries = 0;
  chain = static_chain;

  /* Start looking for an end of the chain (first visible record) */
  do
    {
      /* Get current record type */
      slotp = spage_get_slot (*page_p, current_oid.slotid);
      if (slotp == NULL)
	{
	  assert (0);
	  goto error;
	}
      rec_type = slotp->record_type;

      if (rec_type == REC_DEAD)
	{
	  /* The chain ends in a dead record */
	  is_dead = true;
	  end_chain = true;
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
      else if (rec_type == REC_DELETED_WILL_REUSE
	       || rec_type == REC_MARKDELETED)
	{
	  /* The rest of the chain was already handled, records have been
	   * killed and deleted.
	   */
	  is_dead = true;
	  end_chain = true;

	  /* This can happen only if the chain lipped on another page
	   * The chain on the current page can only end in a REC_DEAD record
	   * if it was already handled.
	   */
	  assert (!VPID_EQ (&fixed_vpid, &page_vpid));
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
	      /* Try to get overflow page without releasing current page */
	      ovf_vpid.pageid = forward_oid.pageid;
	      ovf_vpid.volid = forward_oid.volid;
	      ovf_page_p =
		pgbuf_fix (thread_p, &ovf_vpid, OLD_PAGE, PGBUF_LATCH_READ,
			   PGBUF_CONDITIONAL_LATCH);
	      if (ovf_page_p == NULL)
		{
		  /* Release latch on first page */
		  pgbuf_unfix_and_init (thread_p, *page_p);
		  ovf_page_p =
		    pgbuf_fix (thread_p, &ovf_vpid, OLD_PAGE,
			       PGBUF_LATCH_READ, PGBUF_UNCONDITIONAL_LATCH);
		  if (ovf_page_p == NULL)
		    {
		      assert (0);
		      goto error;
		    }
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
	  mvcc_header = (MVCC_REC_HEADER *) recdes.data;
	  /* Check satisfies vacuum */
	  satisfies_vacuum =
	    mvcc_satisfies_vacuum (thread_p, mvcc_header, oldest_active,
				   *page_p);

	  switch (satisfies_vacuum)
	    {
	    case VACUUM_RECORD_DEAD:
	      /* Record was deleted and the deleting transaction committed */
	      if (OID_ISNULL (&mvcc_header->next_version))
		{
		  /* This is last in chain */
		  is_dead = true;
		  end_chain = true;
		}
	      else
		{
		  /* Must advance in update chain */
		  COPY_OID (&forward_oid, &mvcc_header->next_version);
		}
	      if (rec_type == REC_BIGONE)
		{
		  /* Overflow pages must be released */
		  VPID_COPY (&vacuum_data_p->
			     ovfl_pages[vacuum_data_p->n_ovfl_pages],
			     &ovf_vpid);
		  vacuum_data_p->n_ovfl_pages++;
		}
	      break;

	    case VACUUM_RECORD_RECENTLY_DELETED:
	      /* Record is deleted. However it may be still visible to running
	       * transactions.
	       */
	      /* Fall through */
	    case VACUUM_RECORD_RECENTLY_INSERTED:
	      /* Record was recently inserted. The inserter is still active
	       * or has recently committed. The record is visible at least to
	       * the inserter, yet may be invisible to other active
	       * transactions.
	       */
	      if (VPID_EQ (&page_vpid, &fixed_vpid))
		{
		  /* A record is recently deleted or recently inserted */
		  /* This page will not be all visible after vacuum */
		  vacuum_data_p->all_visible = false;
		}
	    case VACUUM_RECORD_ALIVE:
	      end_chain = true;
	      break;

	    default:
	      /* Unknown or not handled */
	      assert (0);
	      break;
	    }
	}

      if (ovf_page_p != NULL)
	{
	  /* Overflow page is not needed anymore */
	  pgbuf_unfix_and_init (thread_p, ovf_page_p);
	}

      /* Advance to forward_oid */
      if (end_chain && !is_dead)
	{
	  /* Do not add last entry to chain if it is alive */
	  break;
	}

      /* Add entry to chain if it belongs to the first page (which is currently
       * vacuumed).
       */
      if (VPID_EQ (&fixed_vpid, &page_vpid))
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
	      /* Update chain pointer and size */
	      chain = new_chain;
	      chain_size = new_chain_size;
	    }
	  chain[n_chain_entries++] = current_oid.slotid;
	}

      if (end_chain || reuse_oid)
	{
	  /* Stop search */
	  break;
	}

      /* Update current oid */
      COPY_OID (&current_oid, &forward_oid);
      if (fixed_vpid.pageid != current_oid.pageid
	  || fixed_vpid.volid != current_oid.volid)
	{
	  if (vacuum_page_only)
	    {
	      /* We don't want to follow up the entire chain, stop when it exits
	       * the first page.
	       */
	      break;
	    }

	  /* Update the page in use */
	  fixed_vpid.pageid = current_oid.pageid;
	  fixed_vpid.volid = current_oid.volid;

	  /* Unfix old page */
	  pgbuf_unfix (thread_p, *page_p);

	  /* Fix new page */
	  *page_p =
	    pgbuf_fix (thread_p, &fixed_vpid, OLD_PAGE, PGBUF_LATCH_READ,
		       PGBUF_UNCONDITIONAL_LATCH);
	  if (*page_p == NULL)
	    {
	      assert (0);
	      goto error;
	    }
	}
    }
  while (true);

  /* Should be at the end of the chain, handle all intermediate entries */
  if (n_chain_entries == 0)
    {
      /* No intermediate entries, just the root record which is still alive */
      vacuum_data_p->visited[slotid] = true;
    }
  else
    {
      if (is_dead || reuse_oid)
	{
	  /* All chain entries are dead */
	  for (i = 0; i < n_chain_entries; i++)
	    {
	      vacuum_data_p->dead_slots[vacuum_data_p->n_dead++] = chain[i];

	      /* Mark as visited */
	      vacuum_data_p->visited[chain[i]] = true;
	    }
	}
      else
	{
	  /* Relocate all chain entries to the end of the chain */
	  for (i = 0; i < n_chain_entries; i++)
	    {
	      vacuum_data_p->relocated_slots[vacuum_data_p->n_relocations] =
		chain[i];
	      COPY_OID (&vacuum_data_p->
			relocations[vacuum_data_p->n_relocations],
			&current_oid);
	      vacuum_data_p->n_relocations++;

	      /* Mark as visited */
	      vacuum_data_p->visited[chain[i]] = true;
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
mvcc_satisfies_delete (THREAD_ENTRY * thread_p, MVCC_REC_HEADER * rec_header,
#if defined(MVCC_USE_COMMAND_ID)
		       MVCC_COMMAND_ID mvcc_curr_cid,
#endif				/* MVCC_USE_COMMAND_ID */
		       PAGE_PTR page)
{

  assert (rec_header != NULL && page != NULL);

  if (!(rec_header->mvcc_flags & HEAP_MVCC_FLAG_INSID_COMMITTED))
    {
      if (rec_header->mvcc_flags & HEAP_MVCC_FLAG_INSID_INVALID)
	{
	  /* mvcc insert id invalid/aborted */
	  return DELETE_RECORD_INVISIBLE;
	}

      if (logtb_is_current_mvccid (thread_p,
				   HEAP_GET_MVCC_INS_ID (rec_header)))
	{
#if defined(MVCC_USE_COMMAND_ID)
	  if (HEAP_GET_MVCC_INS_COMM_ID (rec_header) >= mvcc_curr_cid)
	    {
	      /* inserted after scan started */
	      return DELETE_RECORD_INVISIBLE;
	    }
#endif /* MVCC_USE_COMMAND_ID */

	  if (rec_header->mvcc_flags & HEAP_MVCC_FLAG_DELID_INVALID)
	    {
	      /* mvcc delete id invald/aborted */
	      return DELETE_RECORD_CAN_DELETE;
	    }

	  if (!logtb_is_current_mvccid (thread_p,
					HEAP_GET_MVCC_DEL_ID (rec_header)))
	    {
	      /* delete subtransaction aborted */
	      heap_set_record_header_flag (thread_p, rec_header, page,
					   HEAP_MVCC_FLAG_DELID_INVALID,
					   MVCCID_NULL);
	      return DELETE_RECORD_CAN_DELETE;
	    }

#if defined(MVCC_USE_COMMAND_ID)
	  if (HEAP_GET_MVCC_DEL_COMM_ID (rec_header) >= mvcc_curr_cid)
	    {
	      /* deleted after scan start */
	      return DELETE_RECORD_SELF_DELETED;
	    }
	  else
	    {
	      /* deleted before scan start */
	      return DELETE_RECORD_INVISIBLE;
	    }
#else /* !MVCC_USE_COMMAND_ID */
	  return DELETE_RECORD_INVISIBLE;
#endif /* !MVCC_USE_COMMAND_ID */
	}
      else if (logtb_is_active_mvccid (thread_p,
				       HEAP_GET_MVCC_INS_ID (rec_header)))
	{
	  /* record insertion not committed yet */
	  return DELETE_RECORD_INVISIBLE;
	}
      else if (logtb_is_mvccid_committed (thread_p,
					  HEAP_GET_MVCC_INS_ID (rec_header)))
	{
	  /* record insertion committed - set flag */
	  heap_set_record_header_flag (thread_p, rec_header, page,
				       HEAP_MVCC_FLAG_INSID_COMMITTED,
				       HEAP_GET_MVCC_INS_ID (rec_header));
	}
      else
	{
	  /* record insertion aborted - set flag */
	  heap_set_record_header_flag (thread_p, rec_header, page,
				       HEAP_MVCC_FLAG_INSID_INVALID,
				       HEAP_GET_MVCC_INS_ID (rec_header));
	  return DELETE_RECORD_INVISIBLE;
	}

    }

  /* record insertion already committed, need to check when was committed */
  if (rec_header->mvcc_flags & HEAP_MVCC_FLAG_DELID_INVALID)
    {
      /* not deleted or aborted */
      return DELETE_RECORD_CAN_DELETE;
    }

  if (rec_header->mvcc_flags & HEAP_MVCC_FLAG_DELID_COMMITTED)
    {
      /* deleted by other committed transaction */
      return DELETE_RECORD_DELETED;
    }

  if (logtb_is_current_mvccid (thread_p, HEAP_GET_MVCC_DEL_ID (rec_header)))
    {
#if defined(MVCC_USE_COMMAND_ID)
      if (HEAP_GET_MVCC_DEL_COMM_ID (rec_header) >= mvcc_curr_cid)
	{
	  /* deleted after scan start */
	  return DELETE_RECORD_SELF_DELETED;
	}
      else
	{
	  /* deleted before scan start */
	  return DELETE_RECORD_INVISIBLE;
	}
#else /* !MVCC_USE_COMMAND_ID */
      return DELETE_RECORD_INVISIBLE;
#endif /* !MVCC_USE_COMMAND_ID */
    }

  if (logtb_is_active_mvccid (thread_p, HEAP_GET_MVCC_DEL_ID (rec_header)))
    {
      /* record deletion not committed yet */
      return DELETE_RECORD_IN_PROGRESS;
    }


  if (!logtb_is_mvccid_committed (thread_p,
				  HEAP_GET_MVCC_DEL_ID (rec_header)))
    {
      /* record deletion aborted */
      heap_set_record_header_flag (thread_p, rec_header, page,
				   HEAP_MVCC_FLAG_DELID_INVALID, MVCCID_NULL);

      return DELETE_RECORD_CAN_DELETE;
    }

  heap_set_record_header_flag (thread_p, rec_header, page,
			       HEAP_MVCC_FLAG_DELID_COMMITTED,
			       HEAP_GET_MVCC_DEL_ID (rec_header));

  /* deleted by other committed transaction */
  return DELETE_RECORD_DELETED;
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
		      MVCC_REC_HEADER * rec_header,
		      MVCC_SNAPSHOT * snapshot, PAGE_PTR page)
{
  assert (rec_header != NULL && snapshot != NULL && page != NULL);

  snapshot->lowest_active_mvccid = snapshot->highest_completed_mvccid =
    MVCCID_NULL;

  if (!(rec_header->mvcc_flags & HEAP_MVCC_FLAG_INSID_COMMITTED))
    {
      if (rec_header->mvcc_flags & HEAP_MVCC_FLAG_INSID_INVALID)
	{
	  /* mvcc insert id invalid/aborted */
	  return false;
	}

      if (logtb_is_current_mvccid (thread_p,
				   HEAP_GET_MVCC_INS_ID (rec_header)))
	{
	  if (rec_header->mvcc_flags & HEAP_MVCC_FLAG_DELID_INVALID)
	    {
	      /* mvcc delete id invald/aborted */
	      return true;
	    }

	  if (!logtb_is_current_mvccid (thread_p,
					HEAP_GET_MVCC_DEL_ID (rec_header)))
	    {
	      heap_set_record_header_flag (thread_p, rec_header, page,
					   HEAP_MVCC_FLAG_DELID_INVALID,
					   MVCCID_NULL);
	      return true;
	    }

	  return false;
	}
      else if (logtb_is_active_mvccid (thread_p,
				       HEAP_GET_MVCC_INS_ID (rec_header)))
	{
	  snapshot->lowest_active_mvccid = HEAP_GET_MVCC_INS_ID (rec_header);
	  /* inserted by other, satisfies dirty */
	  return true;
	}
      else if (logtb_is_mvccid_committed (thread_p,
					  HEAP_GET_MVCC_INS_ID (rec_header)))
	{
	  /* record insertion committed - set flag */
	  heap_set_record_header_flag (thread_p, rec_header, page,
				       HEAP_MVCC_FLAG_INSID_COMMITTED,
				       HEAP_GET_MVCC_INS_ID (rec_header));
	}
      else
	{
	  /* record insertion aborted - set flag */
	  heap_set_record_header_flag (thread_p, rec_header, page,
				       HEAP_MVCC_FLAG_INSID_INVALID,
				       MVCCID_NULL);
	  return false;
	}

    }

  /* record insertion already committed, need to check when was committed */
  if (rec_header->mvcc_flags & HEAP_MVCC_FLAG_DELID_INVALID)
    {
      /* not deleted or aborted */
      return true;
    }

  if (rec_header->mvcc_flags & HEAP_MVCC_FLAG_DELID_COMMITTED)
    {
      /* delete by other - does not satisfies dirty */
      return false;
    }

  if (logtb_is_current_mvccid (thread_p, HEAP_GET_MVCC_DEL_ID (rec_header)))
    {
      return false;
    }

  if (logtb_is_active_mvccid (thread_p, HEAP_GET_MVCC_DEL_ID (rec_header)))
    {
      snapshot->highest_completed_mvccid = HEAP_GET_MVCC_DEL_ID (rec_header);
      /* record deletion not committed yet */
      return true;
    }

  if (!logtb_is_mvccid_committed (thread_p,
				  HEAP_GET_MVCC_DEL_ID (rec_header)))
    {
      /* record deletion aborted */
      heap_set_record_header_flag (thread_p, rec_header, page,
				   HEAP_MVCC_FLAG_DELID_INVALID, MVCCID_NULL);

      return true;
    }

  heap_set_record_header_flag (thread_p, rec_header, page,
			       HEAP_MVCC_FLAG_DELID_COMMITTED,
			       HEAP_GET_MVCC_DEL_ID (rec_header));

  return false;
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
