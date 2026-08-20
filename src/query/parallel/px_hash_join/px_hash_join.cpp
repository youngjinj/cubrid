/*
 * Copyright 2008 Search Solution Corporation
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
 * px_hash_join.cpp
 */

#include "px_hash_join.hpp"
#include "px_hash_join_task_manager.hpp"

#include "error_manager.h"		/* assert_release_error, er_errid, NO_ERROR, ... */
#include "list_file.h"			/* qfile_open_list, qfile_open_list_scan, qfile_close_scan, ... */
#include "query_manager.h"		/* QMGR_TEMP_FILE (qmgr_temp_file) */
#include "memory_alloc.h"		/* db_private_alloc, db_private_free_and_init */
#include "storage_common.h"		/* OID_INITIALIZER, S_CLOSED, VPID_SET_NULL, ... */

// XXX: SHOULD BE THE LAST INCLUDE HEADER
#include "memory_wrapper.hpp"

namespace parallel_query
{
  namespace hash_join
  {
    /*
     * build_partitions
     */

    int
    build_partitions (cubthread::entry &thread_ref, HASHJOIN_MANAGER *manager, HASHJOIN_SPLIT_INFO *split_info)
    {
      int error;

      assert (manager != nullptr);
      assert (split_info != nullptr);

      error = split_input_partitions (thread_ref, manager, &split_info->outer);
      if (error != NO_ERROR)
	{
	  return error;
	}

      return split_input_partitions (thread_ref, manager, &split_info->inner);
    }

    /*
     * split_input_partitions - one materialized input is split into the manager's
     * partition lists by W parallel split tasks (the single-input round of
     * build_partitions; the streamed grace batching calls it for its build side).
     * The caller owns the committed partition lists on failure.
     */

    int
    split_input_partitions (cubthread::entry &thread_ref, HASHJOIN_MANAGER *manager,
			    HASHJOIN_INPUT_SPLIT_INFO *input)
    {
      HASHJOIN_SHARED_SPLIT_INFO shared_info;
      UINT32 task_cnt, task_index;
      int error = NO_ERROR;

      assert (manager != nullptr);
      assert (input != nullptr);

      HASHJOIN_STATS *stats = manager->single_context.stats;
      HASHJOIN_START_STATS start_stats = HASHJOIN_START_STATS_INITIALIZER;
      assert (!thread_is_on_trace (&thread_ref) || stats != nullptr);

      task_cnt = manager->num_parallel_threads;

      THREAD_ENTRY *main_thread_p = thread_get_main_thread (&thread_ref);
      task_manager task_manager (manager->px_worker_manager, *main_thread_p);
      split_task *task = nullptr;

      error = hjoin_init_shared_split_info (&thread_ref, manager, &shared_info);
      if (error != NO_ERROR)
	{
	  goto error_exit;
	}

      if (thread_is_on_trace (&thread_ref))
	{
	  hjoin_trace_start (&thread_ref, &start_stats);
	}

      /* collect data page sectors for the input relation */
      error = qfile_open_list_sector_scan (&thread_ref, input->fetch_info->list_id, &shared_info.sector_scan);
      if (error != NO_ERROR)
	{
	  goto error_exit;
	}

      for (task_index = 0; task_index < task_cnt; task_index++)
	{
	  task = new split_task (task_manager, manager, input, &shared_info, task_index);
	  task_manager.push_task (task);
	}

      task_manager.join ();

      if (thread_is_on_trace (&thread_ref))
	{
	  hjoin_trace_drain_worker_stats (&thread_ref, manager);
	  hjoin_trace_end (&thread_ref, &stats->split, &start_stats);
	}

      if (task_manager.has_error ())
	{
	  goto error_exit;
	}

      ASSERT_NO_ERROR_OR_INTERRUPTED ();

cleanup:
      hjoin_clear_shared_split_info (&thread_ref, manager, &shared_info);

      return error;

error_exit:
      task_manager.clear_interrupt (thread_ref);

      if (error == NO_ERROR || er_errid () == NO_ERROR)
	{
	  assert_release_error (er_errid () != NO_ERROR);
	  error = er_errid ();
	}

      goto cleanup;
    }

    /*
     * execute_partitions
     */

    int
    execute_partitions (cubthread::entry &thread_ref, HASHJOIN_MANAGER *manager)
    {
      HASHJOIN_CONTEXT *current_context;
      HASHJOIN_SHARED_JOIN_INFO shared_info;
      UINT32 context_index;
      UINT32 task_cnt, task_index;

      int error = NO_ERROR;

      assert (manager != nullptr);

      HASHJOIN_STATS *stats = manager->single_context.stats;
      HASHJOIN_START_STATS start_stats = HASHJOIN_START_STATS_INITIALIZER;
#if HASHJOIN_PROFILE_TIME
      HASHJOIN_START_STATS profile_start_stats = HASHJOIN_START_STATS_INITIALIZER;
#endif /* HASHJOIN_PROFILE_TIME */
      assert (!thread_is_on_trace (&thread_ref) || stats != nullptr);

      task_cnt = manager->num_parallel_threads;

      THREAD_ENTRY *main_thread_p = thread_get_main_thread (&thread_ref);
      task_manager task_manager (manager->px_worker_manager, *main_thread_p);
      join_task *task = nullptr;

      if (thread_is_on_trace (&thread_ref))
	{
	  hjoin_trace_start (&thread_ref, &start_stats);
	}

      for (task_index = 0; task_index < task_cnt; task_index++)
	{
	  task = new join_task (task_manager, manager, manager->contexts, &shared_info, task_index);
	  task_manager.push_task (task);
	}

      task_manager.join ();

      if (thread_is_on_trace (&thread_ref))
	{
	  hjoin_trace_drain_worker_stats (&thread_ref, manager);
	  hjoin_trace_end (&thread_ref, &stats->parallel, &start_stats);

	  stats->build.range_elapsed_time.min = shared_info.build_range_time.min;
	  stats->build.range_elapsed_time.max = shared_info.build_range_time.max;
	  stats->probe.range.elapsed_time.min = shared_info.probe_range_time.min;
	  stats->probe.range.elapsed_time.max = shared_info.probe_range_time.max;
	}

      if (task_manager.has_error ())
	{
	  assert_release_error (er_errid () != NO_ERROR);
	  task_manager.clear_interrupt (thread_ref);
	  return er_errid ();
	}

      for (context_index = 0; context_index < manager->context_cnt; context_index++)
	{
	  current_context = &manager->contexts[context_index];

	  if (thread_is_on_trace (&thread_ref))
	    {
	      hjoin_trace_merge_stats (stats, current_context->stats, manager->single_context.status);
	    }

	  if (current_context->list_id == nullptr)
	    {
	      error = er_errid ();
	      if (error != NO_ERROR)
		{
		  return error;
		}
	      else
		{
		  /* list_id can be NULL when the join result is empty.
		   * In this case, it is NO_ERROR. */
		  continue;
		}
	    }

	  if (current_context->list_id->tuple_cnt == 0)
	    {
	      qfile_destroy_list (&thread_ref, current_context->list_id);
	      QFILE_FREE_AND_INIT_LIST_ID (current_context->list_id);

	      /* empty context */
	      continue;
	    }

	  HJOIN_PROFILE_START (&thread_ref, &profile_start_stats, HASHJOIN_PROFILE_MERGE);
	  error = hjoin_merge_qlist (&thread_ref, manager, current_context);
	  HJOIN_PROFILE_MERGE_END (&thread_ref, &stats->profile, &profile_start_stats, HASHJOIN_PROFILE_MERGE,
				   (manager->single_context.list_id != nullptr) ? manager->single_context.list_id->tuple_cnt : 0);

	  if (error != NO_ERROR)
	    {
	      assert_release_error (er_errid () != NO_ERROR);
	      return er_errid ();
	    }
	}

      ASSERT_NO_ERROR_OR_INTERRUPTED ();
      return NO_ERROR;
    }

    /*
     * parallel_probe
     */

    int
    init_context (cubthread::entry &thread_ref, HASHJOIN_MANAGER *manager, HASHJOIN_CONTEXT *context,
		  HASHJOIN_CONTEXT *source)
    {
      HASHJOIN_CONTEXT *single_context;
      int error = NO_ERROR;

      assert (manager != nullptr);
      assert (context != nullptr);
      assert (source != nullptr);

      single_context = source;

      context->outer.list_id = single_context->outer.list_id;
      context->outer.input = single_context->outer.input;
      context->outer.coerce_domains = single_context->outer.coerce_domains;
      context->outer.need_coerce_domains = single_context->outer.need_coerce_domains;
      context->outer.regu_list_pred = single_context->outer.regu_list_pred;

      context->inner.list_id = single_context->inner.list_id;
      context->inner.input = single_context->inner.input;
      context->inner.coerce_domains = single_context->inner.coerce_domains;
      context->inner.need_coerce_domains = single_context->inner.need_coerce_domains;
      context->inner.regu_list_pred = single_context->inner.regu_list_pred;

      assert (context->list_id == nullptr);

      /* Prevent faults when qfile_close_scan is called */
      context->outer.list_scan_id.status = S_CLOSED;
      context->inner.list_scan_id.status = S_CLOSED;

      switch (manager->join_type)
	{
	case JOIN_INNER:
	  context->outer.fill_record = nullptr;
	  context->inner.fill_record = nullptr;
	  break;

	case JOIN_LEFT:
	  context->outer.fill_record = &context->outer.tuple_record;
	  context->inner.fill_record = nullptr;
	  break;

	case JOIN_RIGHT:
	  context->outer.fill_record = nullptr;
	  context->inner.fill_record = &context->inner.tuple_record;
	  break;

	default:
	  /* impossible case */
	  assert_release_error (false);
	  goto error_exit;
	}

      if (single_context->build == &single_context->outer)
	{
	  /* swap_join_inputs == true */
	  context->build = &context->outer;
	  context->probe = &context->inner;
	}
      else
	{
	  /* swap_join_inputs == false */
	  context->build = &context->inner;
	  context->probe = &context->outer;
	}

      context->list_id = qfile_open_list (&thread_ref, &manager->type_list, nullptr,
					  manager->query_id, manager->qlist_flag, nullptr);
      if (context->list_id == nullptr)
	{
	  goto error_exit;
	}

      context->during_join_pred = single_context->during_join_pred;
      context->after_join_pred = single_context->after_join_pred;
      context->val_descr = single_context->val_descr;

      context->status = HASHJOIN_STATUS_PARALLEL_PROBE;

      ASSERT_NO_ERROR_OR_INTERRUPTED ();
      return NO_ERROR;

error_exit:
      clear_context (thread_ref, context);

      if (error == NO_ERROR || er_errid () == NO_ERROR)
	{
	  assert_release_error (er_errid () != NO_ERROR);
	  error = er_errid ();
	}

      return error;
    }

    void
    clear_context (cubthread::entry &thread_ref, HASHJOIN_CONTEXT *context)
    {
      assert (context != nullptr);

      if (context->list_id != nullptr)
	{
	  qfile_close_list (&thread_ref, context->list_id);
	  qfile_destroy_list (&thread_ref, context->list_id);
	  QFILE_FREE_AND_INIT_LIST_ID (context->list_id);
	}

      assert (context->outer.list_scan_id.curr_pgptr == nullptr);
      assert (context->inner.list_scan_id.curr_pgptr == nullptr);
    }

    int
    probe_prepare (cubthread::entry &thread_ref, HASHJOIN_MANAGER *manager)
    {
      HASHJOIN_CONTEXT *contexts = nullptr;
      HASHJOIN_STATS *context_stats = nullptr;
      UINT32 context_cnt, context_index;
      int error = NO_ERROR;

      assert (manager != nullptr);
      assert (manager->contexts == nullptr);
      assert (manager->context_cnt == 0);

      context_cnt = manager->num_parallel_threads;
      assert (context_cnt > 1);

      contexts = (HASHJOIN_CONTEXT *) db_private_alloc (&thread_ref, context_cnt * sizeof (HASHJOIN_CONTEXT));
      if (contexts == nullptr)
	{
	  goto error_exit;
	}
      memset (contexts, 0, context_cnt * sizeof (HASHJOIN_CONTEXT));

      for (context_index = 0; context_index < context_cnt; context_index++)
	{
	  error = init_context (thread_ref, manager, &contexts[context_index], &manager->single_context);
	  if (error != NO_ERROR)
	    {
	      goto error_exit;
	    }

	  manager->context_cnt++;
	}

      manager->contexts = contexts;

      if (thread_is_on_trace (&thread_ref))
	{
	  context_stats = (HASHJOIN_STATS *) malloc (context_cnt * sizeof (HASHJOIN_STATS));
	  if (context_stats == nullptr)
	    {
	      error = ER_OUT_OF_VIRTUAL_MEMORY;
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1, context_cnt * sizeof (HASHJOIN_STATS));
	      goto error_exit;
	    }
	  memset (context_stats, 0, context_cnt * sizeof (HASHJOIN_STATS));

	  for (context_index = 0; context_index < context_cnt; context_index++)
	    {
	      contexts[context_index].stats = &context_stats[context_index];
	    }

	  assert (manager->stats_group != nullptr);
	  manager->stats_group->context_stats = context_stats;
	  manager->stats_group->context_cnt = context_cnt;
	}
      else
	{
	  assert (manager->stats_group == nullptr);
	}

      ASSERT_NO_ERROR_OR_INTERRUPTED ();
      return NO_ERROR;

error_exit:
      if (contexts != nullptr)
	{
	  for (context_index = 0; context_index < manager->context_cnt; context_index++)
	    {
	      clear_context (thread_ref, &contexts[context_index]);
	    }

	  db_private_free_and_init (&thread_ref, contexts);
	}

      if (thread_is_on_trace (&thread_ref))
	{
	  if (context_stats != nullptr)
	    {
	      free_and_init (context_stats);
	    }

	  assert (manager->stats_group != nullptr);
	  manager->stats_group->context_stats = nullptr;
	  manager->stats_group->context_cnt = 0;
	}
      else
	{
	  assert (context_stats == nullptr);
	  assert (manager->stats_group == nullptr);
	}

      manager->contexts = nullptr;
      manager->context_cnt = 0;

      if (error == NO_ERROR || er_errid () == NO_ERROR)
	{
	  assert_release_error (er_errid () != NO_ERROR);
	  error = er_errid ();
	}

      return error;
    }

    /*
     * probe_execute_target - the parallel probe round over one target context: the
     * target owns the shared hash table and the probe input list; the workers'
     * result lists are merged into single_context->list_id, and the worker stats
     * (with the probe min/max ranges) into target->stats exactly once.
     */

    static int
    probe_execute_target (cubthread::entry &thread_ref, HASHJOIN_MANAGER *manager, HASHJOIN_CONTEXT *target,
			  HASHJOIN_CONTEXT *worker_contexts, UINT32 worker_cnt)
    {
      HASHJOIN_CONTEXT *current_context;
      HASHJOIN_SHARED_PROBE_INFO shared_info;
      UINT32 context_index;
      UINT32 task_index;
      int error = NO_ERROR;

      assert (manager != nullptr);
      assert (target != nullptr);
      assert (worker_contexts != nullptr && worker_cnt > 1);
      assert (manager->px_worker_manager != nullptr);
      assert (worker_cnt == manager->num_parallel_threads);

      HASHJOIN_STATS *stats = target->stats;
      HASHJOIN_START_STATS start_stats = HASHJOIN_START_STATS_INITIALIZER;
#if HASHJOIN_PROFILE_TIME
      HASHJOIN_START_STATS profile_start_stats = HASHJOIN_START_STATS_INITIALIZER;
#endif /* HASHJOIN_PROFILE_TIME */
      assert (!thread_is_on_trace (&thread_ref) || stats != nullptr);

      THREAD_ENTRY *main_thread_p = thread_get_main_thread (&thread_ref);
      task_manager task_manager (manager->px_worker_manager, *main_thread_p);
      probe_task *task = nullptr;

      if (thread_is_on_trace (&thread_ref))
	{
	  hjoin_trace_start (&thread_ref, &start_stats);
	}

      /* collect data page sectors for probe relation */
      error = qfile_open_list_sector_scan (&thread_ref, target->probe->list_id, &shared_info.sector_scan);
      if (error != NO_ERROR)
	{
	  goto error_exit;
	}

      for (task_index = 0; task_index < worker_cnt; task_index++)
	{
	  task = new probe_task (task_manager, manager, &worker_contexts[task_index], target, &shared_info, task_index);
	  task_manager.push_task (task);
	}

      task_manager.join ();

      if (thread_is_on_trace (&thread_ref))
	{
	  hjoin_trace_drain_worker_stats (&thread_ref, manager);
	  hjoin_trace_end (&thread_ref, &stats->probe, &start_stats);

	  stats->probe.range.elapsed_time.min = shared_info.probe_range.elapsed_time.min;
	  stats->probe.range.elapsed_time.max = shared_info.probe_range.elapsed_time.max;
	  stats->probe.range.read_rows.min = shared_info.probe_range.read_rows.min;
	  stats->probe.range.read_rows.max = shared_info.probe_range.read_rows.max;
	  stats->probe.range.read_keys.min = shared_info.probe_range.read_keys.min;
	  stats->probe.range.read_keys.max = shared_info.probe_range.read_keys.max;
	  stats->probe.range.qualified_rows.min = shared_info.probe_range.qualified_rows.min;
	  stats->probe.range.qualified_rows.max = shared_info.probe_range.qualified_rows.max;
	}

      if (task_manager.has_error ())
	{
	  goto error_exit;
	}

      for (context_index = 0; context_index < worker_cnt; context_index++)
	{
	  current_context = &worker_contexts[context_index];

	  if (thread_is_on_trace (&thread_ref))
	    {
	      hjoin_trace_merge_stats (stats, current_context->stats, HASHJOIN_STATUS_PARALLEL_PROBE);
	    }

	  if (current_context->list_id == nullptr)
	    {
	      error = er_errid ();
	      if (error != NO_ERROR)
		{
		  goto error_exit;
		}
	      else
		{
		  /* list_id can be NULL when the join result is empty.
		   * In this case, it is NO_ERROR. */
		  continue;
		}
	    }

	  if (current_context->list_id->tuple_cnt == 0)
	    {
	      qfile_destroy_list (&thread_ref, current_context->list_id);
	      QFILE_FREE_AND_INIT_LIST_ID (current_context->list_id);

	      /* empty context */
	      continue;
	    }

	  HJOIN_PROFILE_START (&thread_ref, &profile_start_stats, HASHJOIN_PROFILE_MERGE);
	  error = hjoin_merge_qlist (&thread_ref, manager, current_context);
	  HJOIN_PROFILE_MERGE_END (&thread_ref, &stats->profile, &profile_start_stats, HASHJOIN_PROFILE_MERGE,
				   (manager->single_context.list_id != nullptr) ? manager->single_context.list_id->tuple_cnt : 0);

	  if (error != NO_ERROR)
	    {
	      goto error_exit;
	    }
	}

      ASSERT_NO_ERROR_OR_INTERRUPTED ();

cleanup:
      qfile_close_list_sector_scan (&thread_ref, &shared_info.sector_scan);

      return error;

error_exit:
      task_manager.clear_interrupt (thread_ref);

      if (error == NO_ERROR || er_errid () == NO_ERROR)
	{
	  assert_release_error (er_errid () != NO_ERROR);
	  error = er_errid ();
	}

      goto cleanup;
    }

    int
    probe_execute (cubthread::entry &thread_ref, HASHJOIN_MANAGER *manager)
    {
      assert (manager != nullptr);
      assert (manager->single_context.status == HASHJOIN_STATUS_PARALLEL_PROBE);

      return probe_execute_target (thread_ref, manager, &manager->single_context,
				   manager->contexts, manager->num_parallel_threads);
    }

    /*
     * partition_probe_prepare - allocate the session-owned worker arrays once,
     * before the partition loop. The contexts are (re)armed per partition by
     * partition_probe_execute; manager->contexts is never touched.
     */

    int
    partition_probe_prepare (cubthread::entry &thread_ref, HASHJOIN_MANAGER *manager,
			     partition_probe_session *session)
    {
      UINT32 worker_cnt;

      assert (manager != nullptr);
      assert (session != nullptr);
      assert (session->worker_contexts == nullptr && session->worker_stats == nullptr);

      worker_cnt = manager->num_parallel_threads;
      assert (worker_cnt > 1);

      session->worker_contexts =
	      (HASHJOIN_CONTEXT *) db_private_alloc (&thread_ref, worker_cnt * sizeof (HASHJOIN_CONTEXT));
      if (session->worker_contexts == nullptr)
	{
	  assert_release_error (er_errid () != NO_ERROR);
	  return er_errid ();
	}
      memset (session->worker_contexts, 0, worker_cnt * sizeof (HASHJOIN_CONTEXT));

      if (thread_is_on_trace (&thread_ref))
	{
	  session->worker_stats = (HASHJOIN_STATS *) db_private_alloc (&thread_ref,
				  worker_cnt * sizeof (HASHJOIN_STATS));
	  if (session->worker_stats == nullptr)
	    {
	      db_private_free_and_init (&thread_ref, session->worker_contexts);

	      assert_release_error (er_errid () != NO_ERROR);
	      return er_errid ();
	    }
	  memset (session->worker_stats, 0, worker_cnt * sizeof (HASHJOIN_STATS));
	}

      session->worker_cnt = worker_cnt;

      return NO_ERROR;
    }

    /*
     * partition_probe_execute - rearm the session workers for one target partition
     * and run the parallel probe round over it. The rearm is transactional: a
     * failure while opening the W result lists clears exactly the initialized
     * contexts, and any worker list left unmerged by a failed round is destroyed
     * here, so the session arrays always come back empty.
     */

    int
    partition_probe_execute (cubthread::entry &thread_ref, HASHJOIN_MANAGER *manager,
			     HASHJOIN_CONTEXT *target, partition_probe_session *session)
    {
      HASHJOIN_CONTEXT *context;
      UINT32 armed_cnt, worker_index;
      int error = NO_ERROR;

      assert (manager != nullptr);
      assert (target != nullptr && target != &manager->single_context);
      assert (target->status == HASHJOIN_STATUS_PARALLEL_PROBE);
      assert (session != nullptr && session->worker_contexts != nullptr && session->worker_cnt > 1);

      /* rearm: a fresh probe_prepare-equivalent state per partition (stats zeroed so
       * this round merges only its own numbers into the target) */
      for (armed_cnt = 0; armed_cnt < session->worker_cnt; armed_cnt++)
	{
	  context = &session->worker_contexts[armed_cnt];

	  memset (context, 0, sizeof (HASHJOIN_CONTEXT));
	  if (session->worker_stats != nullptr)
	    {
	      memset (&session->worker_stats[armed_cnt], 0, sizeof (HASHJOIN_STATS));
	      context->stats = &session->worker_stats[armed_cnt];
	    }

	  error = init_context (thread_ref, manager, context, target);
	  if (error != NO_ERROR)
	    {
	      break;
	    }
	}

      if (error == NO_ERROR)
	{
	  error = probe_execute_target (thread_ref, manager, target, session->worker_contexts, session->worker_cnt);

	  if (error == NO_ERROR && target->stats != nullptr)
	    {
	      /* the partition loop's merge aggregates this into the join's stats */
	      target->stats->num_parallel_threads = session->worker_cnt;
	    }
	}

      /* disarm: destroy any result list a failed round left behind (a successful
       * round consumed them all through the merge) */
      for (worker_index = 0; worker_index < armed_cnt; worker_index++)
	{
	  clear_context (thread_ref, &session->worker_contexts[worker_index]);
	}

      return error;
    }

    /*
     * partition_build_execute (P8-4) - build one partition's table with W workers
     * inserting CONCURRENTLY into ONE pre-sized shared table (mht_put_hls_concurrent):
     * no worker tables, no merge, no growth (the partition list's exact tuple_cnt
     * sizes the table upfront). Worker arenas are attached to the table BEFORE the
     * tasks launch, so on every path -- success, error, interrupt -- the wholesale
     * mht_destroy_hls frees them. A platform without lock-free 16-byte CAS or any
     * pre-launch failure returns with *done == false for the untouched serial build.
     */

    int
    partition_build_execute (cubthread::entry &thread_ref, HASHJOIN_MANAGER *manager, HASHJOIN_CONTEXT *target,
			     HASH_METHOD method, partition_probe_session *session, bool *done)
    {
      MHT_HLS_TABLE *table = nullptr;
      HASHJOIN_SHARED_PROBE_INFO shared_info;
      QFILE_LIST_ID *build_list;
      UINT64 worker_rows[PRM_MAX_PARALLELISM] = { 0, };
      UINT64 worker_slots[PRM_MAX_PARALLELISM] = { 0, };
      UINT64 total_rows = 0, total_slots = 0;
      UINT32 w, worker_cnt;
      bool sector_open = false;
      int error = NO_ERROR;

      assert (manager != nullptr && target != nullptr && session != nullptr && done != nullptr);
      assert (target != &manager->single_context);
      assert (session->worker_cnt > 1 && session->worker_cnt <= PRM_MAX_PARALLELISM);
      assert (target->hash_scan.hash_list_scan_type == HASH_METH_NOT_USE);
      assert (method == HASH_METH_IN_MEM || method == HASH_METH_HYBRID);

      *done = false;

      HASHJOIN_STATS *stats = target->stats;
      HASHJOIN_START_STATS start_stats = HASHJOIN_START_STATS_INITIALIZER;
      assert (!thread_is_on_trace (&thread_ref) || stats != nullptr);

      build_list = target->build->list_id;
      assert (build_list != nullptr && build_list->tuple_cnt > 0 && build_list->tuple_cnt <= INT_MAX);

      worker_cnt = session->worker_cnt;

      THREAD_ENTRY *main_thread_p = thread_get_main_thread (&thread_ref);
      task_manager task_manager (manager->px_worker_manager, *main_thread_p);
      build_task *task = nullptr;

      /* the same pre-sizing the serial hjoin_scan_init_table would use */
      table = mht_create_hls ("Hash Join", (int) build_list->tuple_cnt, nullptr, nullptr);
      if (table == nullptr)
	{
	  ASSERT_ERROR_AND_SET (error);
	  return error;
	}

      /* worker arenas: created and ATTACHED before any task launches, so the table
       * owns them on every subsequent path */
      error = mht_prepare_attached_arenas_hls (table, (int) worker_cnt);
      if (error != NO_ERROR)
	{
	  goto cleanup;
	}

      {
	HL_HEAPID arenas[PRM_MAX_PARALLELISM] = { 0, };

	for (w = 0; w < worker_cnt; w++)
	  {
	    arenas[w] = db_create_ostk_heap (HASH_LIST_SCAN_DATA_CHUNK_SIZE);
	    if (arenas[w] == 0 || mht_attach_arena_hls (table, arenas[w]) != NO_ERROR)
	      {
		if (arenas[w] != 0)
		  {
		    db_destroy_ostk_heap (arenas[w]);
		  }
		ASSERT_ERROR_AND_SET (error);
		goto cleanup;
	      }
	  }

	if (thread_is_on_trace (&thread_ref))
	  {
	    hjoin_trace_start (&thread_ref, &start_stats);
	  }

	error = qfile_open_list_sector_scan (&thread_ref, build_list, &shared_info.sector_scan);
	if (error != NO_ERROR)
	  {
	    goto cleanup;
	  }
	sector_open = true;

	for (w = 0; w < worker_cnt; w++)
	  {
	    task = new build_task (task_manager, manager, table, method, arenas[w],
				   &worker_rows[w], &worker_slots[w], &shared_info, (int) w);
	    task_manager.push_task (task);
	  }
      }

      task_manager.join ();

      if (thread_is_on_trace (&thread_ref))
	{
	  hjoin_trace_drain_worker_stats (&thread_ref, manager);
	}

      if (task_manager.has_error ())
	{
	  task_manager.clear_interrupt (thread_ref);
	  assert_release_error (er_errid () != NO_ERROR);
	  error = er_errid ();
	  goto cleanup;
	}

      for (w = 0; w < worker_cnt; w++)
	{
	  total_rows += worker_rows[w];
	  total_slots += worker_slots[w];
	}
      assert (total_rows == (UINT64) build_list->tuple_cnt);
      table->nslots_used = (unsigned int) total_slots;

      /* publish, mirroring hjoin_scan_init_table's setup for the method */
      target->hash_scan.hash_list_scan_type = method;
      target->hash_scan.memory.hash_table = table;
      target->hash_scan.memory.curr_hash_entry = nullptr;
      table = nullptr;

      if (thread_is_on_trace (&thread_ref))
	{
	  hjoin_trace_end (&thread_ref, &stats->build, &start_stats);
	  stats->build.read_rows = build_list->tuple_cnt;
	  stats->build.qualified_rows = build_list->tuple_cnt;
	}

      *done = true;

cleanup:
      if (sector_open)
	{
	  qfile_close_list_sector_scan (&thread_ref, &shared_info.sector_scan);
	}

      if (table != nullptr)
	{
	  mht_destroy_hls (table);	/* frees the attached worker arenas too */
	}

      return error;
    }

    /*
     * partition_probe_round - see the header comment. Owns nothing the session or
     * the target own; the workers reference its task manager and shared info until
     * partition_probe_finish joins them.
     */

    struct partition_probe_round
    {
      task_manager tman;
      HASHJOIN_SHARED_PROBE_INFO shared;
      HASHJOIN_CONTEXT *target;
      HASHJOIN_START_STATS start_stats;
      UINT32 armed;
      bool sector_open;

      partition_probe_round (worker_manager *worker_mgr, cubthread::entry &main_thread_ref)
	: tman (worker_mgr, main_thread_ref)
	, shared ()
	, target (nullptr)
	, start_stats HASHJOIN_START_STATS_INITIALIZER
	, armed (0)
	, sector_open (false)
      {
      }
    };

    /*
     * partition_probe_start - rearm the session workers for one target partition and
     * push its probe tasks WITHOUT joining them: every fallible step (rearm, sector
     * scan open, round allocation) happens before the first push, so a failure here
     * means no task was launched and the session arrays are already clean. The main
     * thread may run other work (the next partition's build) until
     * partition_probe_finish.
     */

    int
    partition_probe_start (cubthread::entry &thread_ref, HASHJOIN_MANAGER *manager, HASHJOIN_CONTEXT *target,
			   partition_probe_session *session, partition_probe_round **round_out)
    {
      partition_probe_round *round = nullptr;
      HASHJOIN_CONTEXT *context;
      UINT32 armed_cnt = 0, worker_index;
      int error = NO_ERROR;

      assert (manager != nullptr);
      assert (target != nullptr && target != &manager->single_context);
      assert (target->status == HASHJOIN_STATUS_PARALLEL_PROBE);
      assert (session != nullptr && session->worker_contexts != nullptr && session->worker_cnt > 1);
      assert (round_out != nullptr);

      *round_out = nullptr;

      THREAD_ENTRY *main_thread_p = thread_get_main_thread (&thread_ref);

      round = new partition_probe_round (manager->px_worker_manager, *main_thread_p);
      round->target = target;

      /* rearm: a fresh probe_prepare-equivalent state per partition (stats zeroed so
       * this round merges only its own numbers into the target) */
      for (armed_cnt = 0; armed_cnt < session->worker_cnt; armed_cnt++)
	{
	  context = &session->worker_contexts[armed_cnt];

	  memset (context, 0, sizeof (HASHJOIN_CONTEXT));
	  if (session->worker_stats != nullptr)
	    {
	      memset (&session->worker_stats[armed_cnt], 0, sizeof (HASHJOIN_STATS));
	      context->stats = &session->worker_stats[armed_cnt];
	    }

	  error = init_context (thread_ref, manager, context, target);
	  if (error != NO_ERROR)
	    {
	      break;
	    }
	}

      if (error == NO_ERROR)
	{
	  error = qfile_open_list_sector_scan (&thread_ref, target->probe->list_id, &round->shared.sector_scan);
	  if (error == NO_ERROR)
	    {
	      round->sector_open = true;
	    }
	}

      if (error != NO_ERROR)
	{
	  for (worker_index = 0; worker_index < armed_cnt; worker_index++)
	    {
	      clear_context (thread_ref, &session->worker_contexts[worker_index]);
	    }
	  delete round;
	  return error;
	}

      round->armed = armed_cnt;

      if (thread_is_on_trace (&thread_ref))
	{
	  hjoin_trace_start (&thread_ref, &round->start_stats);
	}

      for (worker_index = 0; worker_index < session->worker_cnt; worker_index++)
	{
	  probe_task *task = new probe_task (round->tman, manager, &session->worker_contexts[worker_index],
					     target, &round->shared, (int) worker_index);
	  round->tman.push_task (task);
	}

      *round_out = round;
      return NO_ERROR;
    }

    /*
     * partition_probe_finish - the mandatory second half of a round: joins the tasks
     * FIRST on every path (success, error, interrupt), merges the worker results and
     * stats into the target exactly once, disarms the session workers and releases
     * the round. Safe to call exactly once per started round.
     */

    int
    partition_probe_finish (cubthread::entry &thread_ref, HASHJOIN_MANAGER *manager,
			    partition_probe_session *session, partition_probe_round *round)
    {
      HASHJOIN_CONTEXT *current_context;
      HASHJOIN_CONTEXT *target;
      UINT32 worker_index;
      int error = NO_ERROR;

      assert (manager != nullptr && session != nullptr && round != nullptr);

      target = round->target;
      HASHJOIN_STATS *stats = target->stats;
      assert (!thread_is_on_trace (&thread_ref) || stats != nullptr);
#if HASHJOIN_PROFILE_TIME
      HASHJOIN_START_STATS profile_start_stats = HASHJOIN_START_STATS_INITIALIZER;
#endif /* HASHJOIN_PROFILE_TIME */

      round->tman.join ();

      if (thread_is_on_trace (&thread_ref))
	{
	  hjoin_trace_drain_worker_stats (&thread_ref, manager);
	  hjoin_trace_end (&thread_ref, &stats->probe, &round->start_stats);

	  stats->probe.range.elapsed_time.min = round->shared.probe_range.elapsed_time.min;
	  stats->probe.range.elapsed_time.max = round->shared.probe_range.elapsed_time.max;
	  stats->probe.range.read_rows.min = round->shared.probe_range.read_rows.min;
	  stats->probe.range.read_rows.max = round->shared.probe_range.read_rows.max;
	  stats->probe.range.read_keys.min = round->shared.probe_range.read_keys.min;
	  stats->probe.range.read_keys.max = round->shared.probe_range.read_keys.max;
	  stats->probe.range.qualified_rows.min = round->shared.probe_range.qualified_rows.min;
	  stats->probe.range.qualified_rows.max = round->shared.probe_range.qualified_rows.max;
	}

      if (round->tman.has_error ())
	{
	  round->tman.clear_interrupt (thread_ref);
	  assert_release_error (er_errid () != NO_ERROR);
	  error = er_errid ();
	  goto disarm;
	}

      for (worker_index = 0; worker_index < round->armed; worker_index++)
	{
	  current_context = &session->worker_contexts[worker_index];

	  if (thread_is_on_trace (&thread_ref))
	    {
	      hjoin_trace_merge_stats (stats, current_context->stats, HASHJOIN_STATUS_PARALLEL_PROBE);
	    }

	  if (current_context->list_id == nullptr)
	    {
	      error = er_errid ();
	      if (error != NO_ERROR)
		{
		  goto disarm;
		}
	      continue;		/* empty result */
	    }

	  if (current_context->list_id->tuple_cnt == 0)
	    {
	      qfile_destroy_list (&thread_ref, current_context->list_id);
	      QFILE_FREE_AND_INIT_LIST_ID (current_context->list_id);
	      continue;
	    }

	  HJOIN_PROFILE_START (&thread_ref, &profile_start_stats, HASHJOIN_PROFILE_MERGE);
	  error = hjoin_merge_qlist (&thread_ref, manager, current_context);
	  HJOIN_PROFILE_MERGE_END (&thread_ref, &stats->profile, &profile_start_stats, HASHJOIN_PROFILE_MERGE,
				   (manager->single_context.list_id != nullptr)
				   ? manager->single_context.list_id->tuple_cnt : 0);
	  if (error != NO_ERROR)
	    {
	      goto disarm;
	    }
	}

      if (stats != nullptr && thread_is_on_trace (&thread_ref))
	{
	  stats->num_parallel_threads = session->worker_cnt;
	}

disarm:
      for (worker_index = 0; worker_index < round->armed; worker_index++)
	{
	  clear_context (thread_ref, &session->worker_contexts[worker_index]);
	}

      if (round->sector_open)
	{
	  qfile_close_list_sector_scan (&thread_ref, &round->shared.sector_scan);
	}

      delete round;
      return error;
    }

    /*
     * partition_probe_clear - free the session-owned arrays once, after the
     * partition loop.
     */

    void
    partition_probe_clear (cubthread::entry &thread_ref, HASHJOIN_MANAGER *manager,
			   partition_probe_session *session)
    {
      assert (manager != nullptr);
      assert (session != nullptr);

      if (session->worker_contexts != nullptr)
	{
	  db_private_free_and_init (&thread_ref, session->worker_contexts);
	}

      if (session->worker_stats != nullptr)
	{
	  db_private_free_and_init (&thread_ref, session->worker_stats);
	}

      session->worker_cnt = 0;
    }

  } /* namespace hash_join */
} /* namespace parallel_query */
