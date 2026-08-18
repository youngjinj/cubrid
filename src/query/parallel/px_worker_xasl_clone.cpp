/*
 *
 * Copyright 2016 CUBRID Corporation
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

/*
 * px_worker_xasl_clone.cpp
 */

#include "px_worker_xasl_clone.hpp"

#include "dbtype.h"
#include "memory_alloc.h"
#include "query_executor.h"
#include "query_manager.h"
#include "stream_to_xasl.h"
#include "xasl.h"
#include "xasl_iteration.hpp"
#include "xasl_unpack_info.hpp"

// XXX: SHOULD BE THE LAST INCLUDE HEADER
#include "memory_wrapper.hpp"

namespace parallel_query
{
  int
  worker_xasl_clone::acquire (THREAD_ENTRY *thread_ref, THREAD_ENTRY *main_thread_p, bool uses_xasl_clone,
			      qmgr_query_entry *query_entry, int xasl_id, xasl_node *&found_xasl)
  {
    int err_code = NO_ERROR;

    found_xasl = nullptr;
    m_uses_xasl_clone = uses_xasl_clone;

    if (uses_xasl_clone)
      {
	pthread_mutex_lock (&main_thread_p->m_px_lock_mutex);
	err_code = xcache_find_xasl_id_for_execute (thread_ref, &query_entry->xasl_id, &m_xasl_cache_entry,
		   &m_xasl_clone);
	if (err_code != NO_ERROR)
	  {
	    pthread_mutex_unlock (&main_thread_p->m_px_lock_mutex);
	    return err_code;
	  }
	found_xasl = xasl_find_by_id (m_xasl_clone.xasl, xasl_id);
	if (found_xasl == nullptr)
	  {
	    /* the clone was already acquired; retire it here or it is leaked for good */
	    xcache_retire_clone (thread_ref, m_xasl_cache_entry, &m_xasl_clone);
	    xcache_unfix (thread_ref, m_xasl_cache_entry);
	    m_xasl_cache_entry = nullptr;
	    m_xasl_clone = {nullptr, nullptr};
	    pthread_mutex_unlock (&main_thread_p->m_px_lock_mutex);
	    er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_INVALID_XASLNODE, 0);
	    return ER_FAILED;
	  }
	pthread_mutex_unlock (&main_thread_p->m_px_lock_mutex);
      }
    else
      {
	pthread_mutex_lock (&main_thread_p->m_px_lock_mutex);
	err_code = stx_map_stream_to_xasl (thread_ref, &m_xasl_tree, false,
					   main_thread_p->xasl_unpack_info_ptr->packed_xasl,
					   main_thread_p->xasl_unpack_info_ptr->packed_size, &m_xasl_unpack_info);
	if (err_code != NO_ERROR)
	  {
	    pthread_mutex_unlock (&main_thread_p->m_px_lock_mutex);
	    return err_code;
	  }
	found_xasl = xasl_find_by_id (m_xasl_tree, xasl_id);
	if (found_xasl == nullptr)
	  {
	    /* the unpacked tree was already allocated; free it here or it is leaked for good */
	    free_xasl_unpack_info (thread_ref, m_xasl_unpack_info);
	    m_xasl_tree = nullptr;
	    m_xasl_unpack_info = nullptr;
	    pthread_mutex_unlock (&main_thread_p->m_px_lock_mutex);
	    er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_INVALID_XASLNODE, 0);
	    return ER_FAILED;
	  }
	pthread_mutex_unlock (&main_thread_p->m_px_lock_mutex);
      }

    return NO_ERROR;
  }

  int
  worker_xasl_clone::clone_val_descr (THREAD_ENTRY *thread_ref, const val_descr *orig_vd,
				      xasl_state *&state_out, val_descr *&vd_out)
  {
    xasl_state *state;
    val_descr *vd;
    int i;

    state_out = nullptr;
    vd_out = nullptr;

    state = (xasl_state *) db_private_alloc (thread_ref, sizeof (xasl_state));
    if (state == nullptr)
      {
	er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 0);
	return ER_FAILED;
      }
    state->qp_xasl_line = orig_vd->xasl_state->qp_xasl_line;
    state->query_id = orig_vd->xasl_state->query_id;

    vd = &state->vd;
    memcpy (vd, orig_vd, sizeof (val_descr));
    vd->xasl_state = state;

    if (orig_vd->dbval_cnt > 0)
      {
	vd->dbval_ptr = (DB_VALUE *) db_private_alloc (thread_ref, sizeof (DB_VALUE) * orig_vd->dbval_cnt);
	if (vd->dbval_ptr == nullptr)
	  {
	    er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 0);
	    db_private_free_and_init (thread_ref, state);
	    return ER_FAILED;
	  }
	for (i = 0; i < orig_vd->dbval_cnt; i++)
	  {
	    pr_clone_value (&orig_vd->dbval_ptr[i], &vd->dbval_ptr[i]);
	  }
      }

    state_out = state;
    vd_out = vd;
    return NO_ERROR;
  }

  void
  worker_xasl_clone::release (THREAD_ENTRY *thread_ref, THREAD_ENTRY *main_thread_p,
			      xasl_node *found_xasl, xasl_state *state, val_descr *vd)
  {
    if (vd != nullptr && vd->dbval_ptr != nullptr)
      {
	for (int i = 0; i < vd->dbval_cnt; i++)
	  {
	    pr_clear_value (&vd->dbval_ptr[i]);
	  }
	if (vd->dbval_cnt > 0)
	  {
	    db_private_free (thread_ref, vd->dbval_ptr);
	  }
      }
    if (state != nullptr)
      {
	db_private_free (thread_ref, state);
      }

    if (found_xasl != nullptr)
      {
	qexec_clear_xasl (thread_ref, found_xasl, true, false);
      }

    if (m_xasl_cache_entry != nullptr || m_xasl_unpack_info != nullptr)
      {
	pthread_mutex_lock (&main_thread_p->m_px_lock_mutex);
	if (m_uses_xasl_clone)
	  {
	    if (m_xasl_cache_entry != nullptr)
	      {
		xcache_retire_clone (thread_ref, m_xasl_cache_entry, &m_xasl_clone);
		xcache_unfix (thread_ref, m_xasl_cache_entry);
	      }
	  }
	else
	  {
	    if (m_xasl_unpack_info != nullptr)
	      {
		/* free the XASL tree */
		free_xasl_unpack_info (thread_ref, m_xasl_unpack_info);
	      }
	  }
	pthread_mutex_unlock (&main_thread_p->m_px_lock_mutex);
      }

    m_xasl_cache_entry = nullptr;
    m_xasl_clone = {nullptr, nullptr};
    m_xasl_tree = nullptr;
    m_xasl_unpack_info = nullptr;
  }
}
