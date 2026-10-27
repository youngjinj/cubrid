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
 * query_eval_hash.hpp - the hash over a list subquery's values that answers the equality lookups of a predicate
 *			 (IN, NOT IN, = SOME, <> ALL) in place of reading the whole list
 */

#ifndef _QUERY_EVAL_HASH_HPP_
#define _QUERY_EVAL_HASH_HPP_

#if !defined (SERVER_MODE) && !defined (SA_MODE)
#error Belongs to server module
#endif /* !defined (SERVER_MODE) && !defined (SA_MODE) */

#include "dbtype_def.h"
#include "storage_common.h"
#include "thread_compat.hpp"

struct DOMAIN_COMPARE;
struct qfile_list_id;
struct xasl_node;

typedef struct eval_list_hash EVAL_LIST_HASH;

extern EVAL_LIST_HASH *eval_hash_create (THREAD_ENTRY *thread_p);
extern void eval_hash_destroy (THREAD_ENTRY *thread_p, EVAL_LIST_HASH *hash);
extern void eval_hash_destroy_tree (THREAD_ENTRY *thread_p, xasl_node *xasl);

extern bool eval_hash_comparable (const DOMAIN_COMPARE *compare);
extern int eval_hash_for_lookup (THREAD_ENTRY *thread_p, xasl_node *subquery, qfile_list_id *list_id,
				 const DOMAIN_COMPARE *compare, EVAL_LIST_HASH **hash);
extern void eval_hash_count_read (xasl_node *subquery, INT64 rows);
extern bool eval_hash_has_null (const EVAL_LIST_HASH *hash);
extern bool eval_hash_start (EVAL_LIST_HASH *hash, const DB_VALUE *item);
extern SCAN_CODE eval_hash_next (THREAD_ENTRY *thread_p, EVAL_LIST_HASH *hash, DB_VALUE **value);
extern void eval_hash_end (THREAD_ENTRY *thread_p, EVAL_LIST_HASH *hash);

#endif /* _QUERY_EVAL_HASH_HPP_ */
