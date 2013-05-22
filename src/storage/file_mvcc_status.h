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
 * file_mvcc_status.h - file mvcc status manager
 */
#ifndef _FILE_MVCC_STATUS_H_
#define _FILE_MVCC_STATUS_H_

#ident "$Id$"

#include "storage_common.h"
#include "thread.h"


typedef int MVCC_STATUS;

#define MVCC_STATUS_ACTIVE		0x00
#define MVCC_STATUS_COMMITTED		0x01
#define MVCC_STATUS_ABORTED		0x02

extern void file_mvcc_status_cache_vfid (VFID * vfid);
extern int file_mvcc_get_id_status (THREAD_ENTRY * thread_p, MVCCID mvccid,
				    MVCC_STATUS * mvcc_status);
extern int file_mvcc_set_id_status (THREAD_ENTRY * thread_p, MVCCID mvccid,
				    MVCC_STATUS mvcc_status);
extern VFID *file_mvcc_status_create (THREAD_ENTRY * thread_p, VFID * vfid);
extern int file_mvcc_rv_undo_id_status (THREAD_ENTRY * thread_p,
					LOG_RCV * rcv);
extern int file_mvcc_rv_redo_id_status (THREAD_ENTRY * thread_p,
					LOG_RCV * rcv);
extern int file_mvcc_rv_newpage_reset (THREAD_ENTRY * thread_p,
				       LOG_RCV * rcv);
#endif /* _FILE_MVCC_STATUS_H_ */
