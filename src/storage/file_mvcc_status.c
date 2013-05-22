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
 * file_mvcc_status.c - file mvcc status manager
 */

#ident "$Id$"

#include "config.h"
#include "storage_common.h"
#include "page_buffer.h"
#include "file_mvcc_status.h"
#include "file_manager.h"

/* The number of bits per MVCC ID */
#define BITS_PER_MVCCID	  2

/* The number of MVCCIDs per byte */
#define MVCCIDS_PER_BYTE  4

/* The number of MVCCIDs per page */
#define MVCCIDS_PER_PAGE (DB_PAGESIZE * MVCCIDS_PER_BYTE)

/* get page from MVCCID */
#define PAGE_FROM_MVCCID(mvccid) ((mvccid) / (MVCCID) MVCCIDS_PER_PAGE)

/* get page offset from MVCCID */
#define PAGE_OFFSET_FROM_MVCCID(mvccid) ((mvccid) % (MVCCID) MVCCIDS_PER_PAGE)

/* get byte from MVCCID */
#define BYTE_FROM_MVCCID(mvccid) \
  (PAGE_OFFSET_FROM_MVCCID (mvccid) / (MVCCID) MVCCIDS_PER_BYTE)

/* get byte offset from MVCCID */
#define BYTE_OFFSET_FROM_MVCCID(mvccid) \
  (mvccid % (MVCCID) MVCCIDS_PER_BYTE)

/* mvcc bit mask */
#define MVCC_BITMASK ((1 << BITS_PER_MVCCID) - 1)

/* mvcc bits shift */
#define BITS_SHIFT_FROM_MVCCID(mvccid) \
  (BYTE_OFFSET_FROM_MVCCID(mvccid) * BITS_PER_MVCCID)

/* log mvcc get bits */
#define LOG_MVCC_GET_BITS(byte_val, bits_shift) \
  (((byte_val) >> bits_shift) & MVCC_BITMASK)

/* log mvcc clear bits */
#define LOG_MVCC_CLEAR_BITS(byte_val, bits_shift) \
  (byte_val &= ~(MVCC_BITMASK << bits_shift))

/* log mvcc set bits */
#define LOG_MVCC_SET_BITS(byte_val, mvcc_status, bits_shift) \
  (byte_val |= (mvcc_status << bits_shift))

/* number of pages to allocate once */
#define MVCCID_STATUS_FILE_ALLOC_VPIDS 500

#define RESERVED_SIZE_IN_PAGE   sizeof(FILEIO_PAGE_RESERVED)

/* mvcc status heap file identifier */
static VFID mvccid_status_vfid = { NULL_FILEID, NULL_VOLID };

static void file_mvcc_rv_set_id_status (THREAD_ENTRY * thread_p,
					LOG_RCV * rcv,
					MVCC_STATUS mvcc_status);

/*
 * file_mvcc_status_cache_vfid () - Remember mvccid status hfid
 *   vfid(in): Value of mvcc status file identifier
 */
void
file_mvcc_status_cache_vfid (VFID * vfid)
{
  assert (vfid != NULL && !VFID_ISNULL (vfid));
  VFID_COPY (&mvccid_status_vfid, vfid);
}

/*
 * file_mvcc_get_id_status() - Get mvcc id status
 *   return: mvcc id status
 *   mvccid(in): mvcc id
 *
 */
int
file_mvcc_get_id_status (THREAD_ENTRY * thread_p, MVCCID mvccid,
			 MVCC_STATUS * mvcc_status)
{
  PAGE_PTR pgptr = NULL;
  int page_no = PAGE_FROM_MVCCID (mvccid);
  int byte_no = BYTE_FROM_MVCCID (mvccid);
  int bits_shift = BITS_SHIFT_FROM_MVCCID (mvccid);
  VPID vpid;
  int num_found = 0;
  char *byte_ptr;

  assert (mvcc_status != NULL);

  num_found = file_find_nthpages (thread_p, &mvccid_status_vfid, &vpid,
				  page_no, 1);
  if (num_found < 0)
    {
      *mvcc_status = MVCC_STATUS_ACTIVE;
      return NO_ERROR;
    }

  pgptr = pgbuf_fix (thread_p, &vpid, OLD_PAGE, PGBUF_LATCH_READ,
		     PGBUF_UNCONDITIONAL_LATCH);
  if (pgptr == NULL)
    {
      goto exit_on_error;
    }

  byte_ptr = pgptr + byte_no;
  *mvcc_status = LOG_MVCC_GET_BITS (*byte_ptr, bits_shift);

  pgbuf_unfix_and_init (thread_p, pgptr);

  return NO_ERROR;

exit_on_error:
  if (pgptr != NULL)
    {
      pgbuf_unfix_and_init (thread_p, pgptr);
    }
  return (er_errid () == NO_ERROR ? ER_FAILED : er_errid ());
}

/*
 * file_mvcc_set_id_status() - Set mvcc id status
 *   return: error code
 *   mvccid(in): mvcc id
 *   mvcc_status(in): mvcc status
 *
 */
int
file_mvcc_set_id_status (THREAD_ENTRY * thread_p, MVCCID mvccid,
			 MVCC_STATUS mvcc_status)
{
  LOG_DATA_ADDR addr;
  int page_no = PAGE_FROM_MVCCID (mvccid);
  int bits_shift = BITS_SHIFT_FROM_MVCCID (mvccid);
  char *byte_ptr;
  VPID vpid;
  int num_found = 0;
  char recv;

  addr.vfid = &mvccid_status_vfid;
  do
    {
      num_found = file_find_nthpages (thread_p, &mvccid_status_vfid, &vpid,
				      page_no, 1);
      if (num_found < 0)
	{
	  int num_pages = MVCCID_STATUS_FILE_ALLOC_VPIDS;
	  VPID vpids[MVCCID_STATUS_FILE_ALLOC_VPIDS];
	  FILE_ALLOC_VPIDS alloc_vpids;
	  LOG_DATA_ADDR addr;
	  int alloc_nth, val = (char) MVCC_STATUS_ACTIVE, offset_vpid = 0, i;

	  alloc_vpids.vpids = vpids;
	  alloc_vpids.index = 0;
	  if (file_alloc_pages_as_noncontiguous (thread_p,
						 &mvccid_status_vfid,
						 alloc_vpids.vpids,
						 &alloc_nth,
						 num_pages, NULL, NULL,
						 NULL, &alloc_vpids) == NULL)
	    {
	      goto exit_on_error;
	    }

	  addr.offset = 0;
	  for (i = 0; i < num_pages; i++)
	    {
	      addr.pgptr = pgbuf_fix (thread_p, &vpids[i], NEW_PAGE,
				      PGBUF_LATCH_WRITE,
				      PGBUF_UNCONDITIONAL_LATCH);
	      if (addr.pgptr == NULL)
		{
		  goto exit_on_error;
		}

	      memset (addr.pgptr, val, DB_PAGESIZE);

	      log_append_redo_data (thread_p, RVBF_NEWPAGE_RESET, &addr,
				    sizeof (val), &val);
	      pgbuf_set_dirty (thread_p, addr.pgptr, FREE);
	      addr.pgptr = NULL;
	    }
	}
    }
  while (num_found < 0);

  addr.pgptr = pgbuf_fix (thread_p, &vpid, OLD_PAGE, PGBUF_LATCH_WRITE,
			  PGBUF_UNCONDITIONAL_LATCH);
  if (addr.pgptr == NULL)
    {
      goto exit_on_error;
    }

  addr.offset = BYTE_FROM_MVCCID (mvccid);
  byte_ptr = addr.pgptr + addr.offset;
  recv = LOG_MVCC_GET_BITS (*byte_ptr, bits_shift);
  LOG_MVCC_CLEAR_BITS (*byte_ptr, bits_shift);
  LOG_MVCC_SET_BITS (*byte_ptr, mvcc_status, bits_shift);
  recv |= ((char) mvcc_status << 2);

  log_append_undoredo_data (thread_p, RVBF_MVCC_SET_STATUS, &addr,
			    sizeof (recv), sizeof (recv), &recv, &recv);

  pgbuf_set_dirty (thread_p, addr.pgptr, FREE);

  return NO_ERROR;

exit_on_error:

  if (addr.pgptr != NULL)
    {
      pgbuf_unfix_and_init (thread_p, addr.pgptr);
    }

  return (er_errid () == NO_ERROR ? ER_FAILED : er_errid ());
}

/*
 * file_mvcc_rv_set_id_status () - Recovery set mvcc id status
 *   return: int
 *   rcv(in): Recovery structure
 *   mvcc_status(in): mvcc status
 *
 */
static void
file_mvcc_rv_set_id_status (THREAD_ENTRY * thread_p, LOG_RCV * rcv,
			    MVCC_STATUS mvcc_status)
{
  int mvccid = rcv->mvcc_id;
  int page_no = PAGE_FROM_MVCCID (mvccid);
  int bits_shift = BITS_SHIFT_FROM_MVCCID (mvccid);
  char *byte_ptr = rcv->pgptr + rcv->offset;

  LOG_MVCC_CLEAR_BITS (*byte_ptr, bits_shift);
  LOG_MVCC_SET_BITS (*byte_ptr, mvcc_status, bits_shift);
}

/*
 * file_mvcc_status_create () - Create mvccid status file
 *   return: vfid or NULL in case of error
 *   vfid(out): Complete file identifier
 *
 * Note: Create the system mvcc id status file in the volume identified by
 *       vfid->volid. This file is used to keep the status of mvcc ids.
 */

VFID *
file_mvcc_status_create (THREAD_ENTRY * thread_p, VFID * vfid)
{
  int num_pages = MVCCID_STATUS_FILE_ALLOC_VPIDS;
  VPID vpids[MVCCID_STATUS_FILE_ALLOC_VPIDS];
  int alloc_nth, i;
  FILE_ALLOC_VPIDS alloc_vpids;
  LOG_DATA_ADDR addr;
  char val = (char) MVCC_STATUS_ACTIVE;

  VFID_SET_NULL (vfid);
  alloc_vpids.vpids = vpids;
  alloc_vpids.index = 0;

  if (mvcc_Enabled == false)
    {
      /* NULL mvcc status file if mvcc disabled */
      return NULL;
    }

  if (file_create (thread_p, vfid, num_pages, FILE_MVCC_STATUS, NULL, NULL,
		   0) < 0)
    {
      goto exit_on_error;
    }

  if (file_alloc_pages_as_noncontiguous (thread_p, vfid, alloc_vpids.vpids,
					 &alloc_nth, num_pages, NULL, NULL,
					 NULL, &alloc_vpids) == NULL)
    {
      goto exit_on_error;
    }

#if !defined(NDEBUG)
  for (i = 0; i < num_pages; i++)
    {
      assert (!VPID_ISNULL (vpids + i));
    }
#endif

  addr.vfid = vfid;
  addr.offset = 0;
  for (i = 0; i < num_pages; i++)
    {
      addr.pgptr = pgbuf_fix (thread_p, &vpids[i], NEW_PAGE,
			      PGBUF_LATCH_WRITE, PGBUF_UNCONDITIONAL_LATCH);
      if (addr.pgptr == NULL)
	{
	  goto exit_on_error;
	}

      memset (addr.pgptr, val, DB_PAGESIZE);

      log_append_redo_data (thread_p, RVBF_NEWPAGE_RESET, &addr, sizeof (val),
			    &val);
      pgbuf_set_dirty (thread_p, addr.pgptr, FREE);
      addr.pgptr = NULL;
    }

  return vfid;

exit_on_error:

  if (!VFID_ISNULL (vfid))
    {
      (void) file_destroy (thread_p, vfid);
    }

  return NULL;
}

/*
 * file_mvcc_rv_undo_id_status() - Recovery undo mvcc id status
 *   return: int
 *   rcv(in): Recovery structure
 *
 */
int
file_mvcc_rv_undo_id_status (THREAD_ENTRY * thread_p, LOG_RCV * rcv)
{
  char recv = *(char *) rcv->data;
  file_mvcc_rv_set_id_status (thread_p, rcv, ((MVCC_STATUS) (recv & 0x03)));

  return NO_ERROR;
}

/*
 * file_mvcc_rv_redo_id_status () - Recovery redo mvcc id status
 *   return: int
 *   rcv(in): Recovery structure
 *
 */
int
file_mvcc_rv_redo_id_status (THREAD_ENTRY * thread_p, LOG_RCV * rcv)
{
  unsigned char recv = *(unsigned char *) rcv->data;
  file_mvcc_rv_set_id_status (thread_p, rcv,
			      ((MVCC_STATUS) ((recv >> 2) & 0x03)));

  return NO_ERROR;
}

/*
 * file_mvcc_rv_newpage_reset() - Recover an bitmap page reset
 *   return: int
 *   rcv(in): Recovery structure
 *
 * Note: Recover an bitmap page reset
 */
int
file_mvcc_rv_newpage_reset (THREAD_ENTRY * thread_p, LOG_RCV * rcv)
{
  char val = *(char *) rcv->data;
  memset (rcv->pgptr, val, DB_PAGESIZE);

  return NO_ERROR;
}
