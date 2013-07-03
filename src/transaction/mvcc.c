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
 *	 || HEAP_MVCC_IS_FLAG_SET (rec_header, HEAP_MVCC_FLAG_DELID_COMMITTED)
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
      else if (logtb_is_active_mvccid
	       (thread_p, HEAP_GET_MVCC_INS_ID (rec_header)))
	{
	  /* The inserting transaction is still active */
	  if (HEAP_MVCC_IS_FLAG_SET
	      (rec_header, HEAP_MVCC_FLAG_DELID_INVALID))
	    {
	      /* The record is not deleted, insert is still in progress */
	      return VACUUM_RECORD_INSERT_IN_PROGRESS;
	    }
	  /* The record was also deleted and delete is in progress */
	  return VACUUM_RECORD_DELETE_IN_PROGRESS;
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
      if (logtb_is_active_mvccid
	  (thread_p, HEAP_GET_MVCC_DEL_ID (rec_header)))
	{
	  /* the deleter is still active */
	  return VACUUM_RECORD_DELETE_IN_PROGRESS;
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
      return VACUUM_RECORD_RECENTLY_DEAD;
    }

  /* the record was deleted, the deleter committed and no active transactions
   * can see this record.
   * safe to remove.
   */
  return VACUUM_RECORD_DEAD;
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


