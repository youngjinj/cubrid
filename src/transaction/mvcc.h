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
 * mvcc_snapshot.h - Multi-Version Concurency Control system (at Server).
 *
 */
#ifndef _MVCC_SNAPSHOT_H_
#define _MVCC_SNAPSHOT_H_

#ident "$Id$"
#include "thread.h"
#include "storage_common.h"

#define MVCC_SNAPSHOT_GET_LOWEST_ACTIVE_ID(snapshot) \
  ((snapshot)->lowest_active_mvccid)

#define MVCC_SNAPSHOT_GET_HIGHEST_COMMITTED_ID(snapshot) \
  ((snapshot)->highest_completed_mvccid)

#if defined(MVCC_USE_COMMAND_ID)
#define MVCC_SNAPSHOT_GET_COMMAND_ID(snapshot) \
  ((snapshot)->current_command_id)
#endif /* MVCC_USE_COMMAND_ID */

typedef struct mvcc_snapshot MVCC_SNAPSHOT;

typedef bool (*MVCC_SNAPSHOT_FUNC) (THREAD_ENTRY * thread_p,
				    MVCC_REC_HEADER * rec_header,
				    MVCC_SNAPSHOT * snapshot,
				    PAGE_PTR page_ptr);

struct mvcc_snapshot
{
  MVCC_SNAPSHOT_FUNC snapshot_fnc;

  MVCCID lowest_active_mvccid;	/* lowest active id */

  MVCCID highest_completed_mvccid;	/* highest committed id */

  MVCCID *active_ids;		/* active ids */

  unsigned int cnt_active_ids;	/* count active ids */

  MVCCID *active_child_ids;	/* active children */

  unsigned int cnt_active_child_ids;	/* count active child ids */

#if defined(MVCC_USE_COMMAND_ID)
  MVCC_COMMAND_ID current_command_id;	/* current command id */
#endif				/* MVCC_USE_COMMAND_ID */
};

typedef enum mvcc_satisfies_delete_result MVCC_SATISFIES_DELETE_RESULT;
enum mvcc_satisfies_delete_result
{
  DELETE_RECORD_INVISIBLE,	/* invisible - created after scan started */
  DELETE_RECORD_CAN_DELETE,	/* is visible and valid - can be deleted */
#if defined(MVCC_USE_COMMAND_ID)
  DELETE_RECORD_SELF_DELETED,	/* deleted by current transaction */
#endif				/* MVCC_USE_COMMAND_ID */
  DELETE_RECORD_DELETED,	/* deleted by committed transaction */
  DELETE_RECORD_IN_PROGRESS	/* deleted by other in progress transaction */
};				/* Heap record satisfies delete result */

typedef enum mvcc_satisfies_vacuum_result MVCC_SATISFIES_VACUUM_RESULT;
enum mvcc_satisfies_vacuum_result
{
  VACUUM_RECORD_DEAD,		/* record is dead and can be removed */
  VACUUM_RECORD_ALIVE,		/* record is alive */
  VACUUM_RECORD_RECENTLY_DEAD,	/* records was deleted and the deleter
				 * committed but it may be visible to some
				 * active transactions
				 */
  VACUUM_RECORD_INSERT_IN_PROGRESS,	/* the inserter is still active */
  VACUUM_RECORD_DELETE_IN_PROGRESS	/* the deleter is still active */
};				/* Heap record satisfies vacuum result */

extern bool mvcc_satisfies_snapshot (THREAD_ENTRY * thread_p,
				     MVCC_REC_HEADER * rec_header,
				     MVCC_SNAPSHOT * snapshot, PAGE_PTR page);
extern MVCC_SATISFIES_VACUUM_RESULT mvcc_satisfies_vacuum (THREAD_ENTRY *
							   thread_p,
							   MVCC_REC_HEADER *
							   rec_header,
							   MVCCID
							   oldest_mvccid,
							   PAGE_PTR page_p);
extern MVCC_SATISFIES_DELETE_RESULT mvcc_satisfies_delete (THREAD_ENTRY *
							   thread_p,
							   MVCC_REC_HEADER *
							   rec_header,
#if defined(MVCC_USE_COMMAND_ID)
							   MVCC_COMMAND_ID
							   mvcc_curr_cid,
#endif				/* MVCC_USE_COMMAND_ID */
							   PAGE_PTR page);

extern bool mvcc_satisfies_dirty (THREAD_ENTRY *
				  thread_p,
				  MVCC_REC_HEADER *
				  rec_header,
				  MVCC_SNAPSHOT * snapshot, PAGE_PTR page);
extern bool mvcc_id_precedes (MVCCID id1, MVCCID id2);
extern bool mvcc_id_follow_or_equal (MVCCID id1, MVCCID id2);
#endif /* _MVCC_SNAPSHOT_H_ */
