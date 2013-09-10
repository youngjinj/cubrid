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
 * vacuum.h - Vacuuming system (at Server).
 *
 */
#ifndef _VACUUM_
#define _VACUUM_

#include "dbtype.h"
#include "thread.h"
#include "storage_common.h"

/* VACUUM_PAGE_DATA
 * Structures is used by VACUUM to collect data on a heap page. The cleaning
 * process is executed based on this data.
 */
typedef struct vacuum_page_data VACUUM_PAGE_DATA;
struct vacuum_page_data
{
  PGSLOTID *dead_slots;		/* array of ids for dead slots */
  PGSLOTID *relocated_slots;	/* relocated slots */
  OID *relocations;		/* destination for each slot in
				 * relocated_slots.
				 */
  VPID *ovfl_pages;		/* array of ids for overflow pages */

  bool *visited;		/* Is set to true when a slot is handled.
				 * Used to avoid re-verifying slots met in
				 * an update chain.
				 */

  short n_dead;			/* number of dead slots */
  short n_ovfl_pages;		/* number of overflow pages */
  short n_relocations;		/* number of relocated slots */

  bool vacuum_needed;		/* Is set to true if any records are deleted
				 * from current page and compacting would
				 * be useful.
				 */
  bool all_visible;		/* It is true if all records on a page are
				 * visible to all transactions after a
				 * vacuum execution.
				 */
};

extern int vacuum_stats_table_initialize (THREAD_ENTRY * thread_p);
extern void vacuum_stats_table_finalize (THREAD_ENTRY * thread_p);
extern int vacuum_stats_table_update_entry (THREAD_ENTRY * thread_p,
					    const OID * class_oid,
					    HFID * hfid, int n_inserted,
					    int n_deleted);
extern int vacuum_stats_table_remove_entry (OID * class_oid);

extern int xvacuum (THREAD_ENTRY * thread_p, int num_classes,
		    OID * class_oids);

extern void auto_vacuum_start (void);
#endif /* _VACUUM_ */
