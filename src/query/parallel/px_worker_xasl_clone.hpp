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
 * px_worker_xasl_clone.hpp - per-worker private XASL tree acquisition/release
 *
 * Extracted from px_scan_task so that any parallel worker (parallel scan tasks,
 * hash join streaming probe tasks) shares one clone lifecycle:
 *
 *   acquire        : cache clone (xcache) or stream unpack, then locate the target node
 *   clone_val_descr: worker-private xasl_state / val_descr pair with host values copied
 *   release        : partial-initialization safe; releases whatever was acquired, in the
 *                    same order the original px_scan_task::finalize used
 *
 * All xcache/unpack operations are serialized under the main thread's px mutex,
 * exactly as the original code did.
 */

#ifndef _PX_WORKER_XASL_CLONE_HPP_
#define _PX_WORKER_XASL_CLONE_HPP_

#include "thread_entry.hpp"
#include "xasl_cache.h"

// XASL_NODE_ID lives in xasl.h; forward-declare the pieces we need instead of
// dragging the full executor headers into every includer.
struct xasl_node;
struct xasl_state;
struct val_descr;
struct qmgr_query_entry;

namespace parallel_query
{
  class worker_xasl_clone
  {
    public:
      worker_xasl_clone ()
	: m_uses_xasl_clone (false),
	  m_xasl_cache_entry (nullptr),
	  m_xasl_clone ({nullptr, nullptr}),
      m_xasl_tree (nullptr),
      m_xasl_unpack_info (nullptr)
      {
      }

      worker_xasl_clone (const worker_xasl_clone &) = delete;
      worker_xasl_clone &operator= (const worker_xasl_clone &) = delete;

      /* Acquire this worker's private XASL tree and locate the node with header id
       * `xasl_id` in it. On failure nothing is left acquired (release () is still safe).
       * uses_xasl_clone selects the xcache clone path; otherwise the packed stream of
       * main_thread_p->xasl_unpack_info_ptr is unpacked. */
      int acquire (THREAD_ENTRY *thread_ref, THREAD_ENTRY *main_thread_p, bool uses_xasl_clone,
		   qmgr_query_entry *query_entry, int xasl_id, xasl_node *&found_xasl);

      /* Build the worker-private xasl_state/val_descr pair from the main thread's
       * val_descr, cloning host variable values. On failure outputs stay null. */
      int clone_val_descr (THREAD_ENTRY *thread_ref, const val_descr *orig_vd,
			   xasl_state *&state_out, val_descr *&vd_out);

      /* Release everything acquired so far. Safe after any partial acquire:
       * - vd/state from clone_val_descr (host values cleared, buffers freed)
       * - found_xasl runtime values (qexec_clear_xasl)
       * - cache clone retire/unfix or unpack info, under the px mutex
       * Nulls all members; the object can be reused afterwards. */
      void release (THREAD_ENTRY *thread_ref, THREAD_ENTRY *main_thread_p,
		    xasl_node *found_xasl, xasl_state *state, val_descr *vd);

    private:
      bool m_uses_xasl_clone;
      XASL_CACHE_ENTRY *m_xasl_cache_entry;
      XASL_CLONE m_xasl_clone;
      xasl_node *m_xasl_tree;
      XASL_UNPACK_INFO *m_xasl_unpack_info;
  };
}

#endif /* _PX_WORKER_XASL_CLONE_HPP_ */
