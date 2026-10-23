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
 * query_eval_hash.cpp - the hash over a list subquery's values for the equality lookups of a predicate
 *
 * A list subquery's R_EQ lookups read the whole list for every row that finds no match in it. From the second lookup
 * of one execution of the subquery on (a hash file, which costs more to build, the later the more rows a page of the
 * list holds), a hash over the list answers them: a value is hashed in the type its comparison compares it in, and only
 * the list values of the same hash are given back, for the caller to compare by that same comparison. A value the hash
 * cannot take as its comparison compares it (a conversion that fails, a NaN, a type or a collation left out) sends that
 * lookup, or the whole execution, back to reading the list.
 * A correlated subquery executes again for each row, so no execution of it reaches a second lookup.
 */

#include "config.h"

#include <string.h>

#include "query_eval_hash.hpp"

#include "dbtype.h"
#include "domain_rules.h"
#include "error_manager.h"
#include "language_support.h"
#include "list_file.h"
#include "memory_alloc.h"
#include "memory_hash.h"
#include "numeric_opfunc.h"
#include "object_primitive.h"
#include "qfile_tuple_layout.h"
#include "query_hash_scan.h"
#include "query_manager.h"
#include "system_parameter.h"
#include "xasl.h"
#include "xasl_iteration.hpp"
// XXX: SHOULD BE THE LAST INCLUDE HEADER
#include "memory_wrapper.hpp"

struct eval_list_hash
{
  unsigned int execution_no;	/* the subquery execution the fields below hold for */
  int lookups;			/* lookups of that execution before the table was built */
  bool unusable;		/* that execution reads its list: a value the table cannot take, or hashing turned off */
  bool has_null;		/* the list holds a NULL */
  const DOMAIN_COMPARE *compare;	/* the comparison the table was built for */
  HASH_LIST_SCAN hash_scan;	/* the table; HASH_METH_NOT_USE: none */
  QFILE_LIST_SCAN_ID list_scan;	/* open while the table is: the table is built over it and reads tuples through it */
  QFILE_TUPLE_RECORD tuple_record;	/* the tuple the last eval_hash_next found */
  DB_VALUE value;		/* that tuple's list value */
};

static unsigned int eval_hash_bytes (const void *bytes, size_t size);
static unsigned int eval_hash_weights (const unsigned int *weights, const unsigned char *str, int size);
static bool eval_hash_type (DB_TYPE type);
static bool eval_hash_value (const DOMAIN_COMPARE *compare, const DB_VALUE *value, unsigned int *hash_key);
static bool eval_hash_key (const DOMAIN_COMPARE *compare, int side, const DB_VALUE *value, unsigned int *hash_key);
static int eval_hash_build (THREAD_ENTRY *thread_p, EVAL_LIST_HASH *hash, QFILE_LIST_ID *list_id,
			    const DOMAIN_COMPARE *compare, HASH_METHOD method);
static void eval_hash_clear (THREAD_ENTRY *thread_p, EVAL_LIST_HASH *hash);

/* building a hash file measured about 21 reads of its list with 2,000 INT rows a page of the list, and about 3 with 73
 * VARCHAR of 200 bytes: a wider row costs a read more. Measure again if the cost of a hash file changes */
static const int EVAL_HASH_FILE_ROWS_PER_READ = 64;

/*
 * Hash values
 */

/* FNV-1a */
static const unsigned int EVAL_HASH_FNV_OFFSET = 2166136261u;
static const unsigned int EVAL_HASH_FNV_PRIME = 16777619u;

static unsigned int
eval_hash_bytes (const void *bytes, size_t size)
{
  const unsigned char *p = (const unsigned char *) bytes;
  unsigned int hash_key = EVAL_HASH_FNV_OFFSET;
  for (size_t i = 0; i < size; i++)
    {
      hash_key = (hash_key ^ p[i]) * EVAL_HASH_FNV_PRIME;
    }
  return hash_key;
}

/* the bytes' weights, as lang_fastcmp_byte compares them: a space and a NUL weigh 0, and trailing ones never tell two
 * equal strings apart */
static unsigned int
eval_hash_weights (const unsigned int *weights, const unsigned char *str, int size)
{
  unsigned int hash_key = EVAL_HASH_FNV_OFFSET;
  while (size > 0 && weights[str[size - 1]] == 0)
    {
      size--;
    }
  for (int i = 0; i < size; i++)
    {
      hash_key = (hash_key ^ weights[str[i]]) * EVAL_HASH_FNV_PRIME;
    }
  return hash_key;
}

/* The types whose values eval_hash_value hashes alike when their comparison finds them equal. */
static bool
eval_hash_type (DB_TYPE type)
{
  switch (type)
    {
    case DB_TYPE_SMALLINT:
    case DB_TYPE_INTEGER:
    case DB_TYPE_BIGINT:
    case DB_TYPE_NUMERIC:
    case DB_TYPE_FLOAT:
    case DB_TYPE_DOUBLE:
    case DB_TYPE_CHAR:
    case DB_TYPE_VARCHAR:
    case DB_TYPE_DATE:
    case DB_TYPE_TIME:
    case DB_TYPE_TIMESTAMP:
    case DB_TYPE_DATETIME:
      return true;
    default:
      return false;
    }
}

/*
 * eval_hash_comparable () - a comparison the hash can answer for: its two sides are the row's values, and it either
 *			     compares them as they are or converts them once each, without a failure resolved in
 *			     advance or a codeset brought to an ENUM's
 */
bool
eval_hash_comparable (const DOMAIN_COMPARE *compare)
{
  return (compare->method == DOMAIN_COMPARE_DIRECT || compare->method == DOMAIN_COMPARE_CONVERT)
	 && compare->value[0] < 0 && compare->value[1] < 0 && compare->failed == 0 && compare->codeset_side < 0
	 && compare->cmp != NULL && eval_hash_type (compare->cmp->id);
}

/*
 * eval_hash_value () - the hash of a value in the type its comparison compares it in: values the comparison finds
 *			equal hash alike
 *   return: false if the value has no such hash
 */
static bool
eval_hash_value (const DOMAIN_COMPARE *compare, const DB_VALUE *value, unsigned int *hash_key)
{
  const DB_TYPE type = DB_VALUE_DOMAIN_TYPE (value);
  if (type != compare->cmp->id && ! (TP_IS_CHAR_TYPE (type) && TP_IS_CHAR_TYPE (compare->cmp->id)))
    {
      return false;
    }
  switch (type)
    {
    case DB_TYPE_CHAR:
    case DB_TYPE_VARCHAR:
    {
      /* the comparison's collation, not the value's. The other collations' hash functions (lang_mht2str_*) tell apart
       * strings their comparison finds equal */
      const LANG_COLLATION *lang_coll = lang_get_collation (compare->collation);
      if (!lang_is_coll_fastcmp_byte (lang_coll) || db_get_string_codeset (value) != lang_coll->codeset)
	{
	  return false;
	}
      *hash_key = eval_hash_weights (lang_coll->coll.weights_ti, (const unsigned char *) db_get_string (value),
				     db_get_string_size (value));
      return true;
    }
    case DB_TYPE_NUMERIC:
    {
      /* 1.00 and 1.000 compare equal: the digits without trailing zeros. A float NUMERIC keeps its scale in the
       * value, not in its domain */
      uint8_t digits[DB_NUMERIC_BUF_SIZE];
      int precision, scale;
      db_get_numeric_precision_and_scale (value, &precision, &scale, NULL);
      if (scale < 0)
	{
	  return false;
	}
      if (scale == 0)
	{
	  *hash_key = eval_hash_bytes (db_locate_numeric (value), DB_NUMERIC_BUF_SIZE);
	  return true;
	}
      float_numeric_normalize_for_hash ((DB_C_NUMERIC) db_locate_numeric (value), digits, precision, scale);
      *hash_key = eval_hash_bytes (digits, DB_NUMERIC_BUF_SIZE);
      return true;
    }
    case DB_TYPE_DOUBLE:
    case DB_TYPE_FLOAT:
    {
      double d = type == DB_TYPE_DOUBLE ? db_get_double (value) : db_get_float (value);
      if (d != d)
	{
	  /* a NaN compares equal to every value (MR_CMP) */
	  return false;
	}
      /* -0.0 compares equal to 0.0 */
      d = d == 0 ? 0 : d;
      *hash_key = eval_hash_bytes (&d, sizeof (d));
      return true;
    }
    default:
      *hash_key = mht_get_hash_number (UINT_MAX, value);
      return true;
    }
}

/*
 * eval_hash_key () - the hash of a comparison side's value, converted as the comparison converts that side
 *   return: false if the conversion fails or the value has no hash
 *   side(in): 0 the row's item, 1 a list value
 */
static bool
eval_hash_key (const DOMAIN_COMPARE *compare, int side, const DB_VALUE *value, unsigned int *hash_key)
{
  if (compare->conv[side] == NULL)
    {
      return eval_hash_value (compare, value, hash_key);
    }
  DB_VALUE converted;
  bool hashed = false;
  if (tp_value_convert (compare->conv[side], compare->target[side], value, &converted) == DOMAIN_COMPATIBLE)
    {
      hashed = eval_hash_value (compare, &converted, hash_key);
    }
  pr_clear_value (&converted);
  return hashed;
}

/*
 * The hash of a subquery's list, owned by the subquery's XASL node (xasl_node::list_hash)
 */

/*
 * eval_hash_create () - a hash with no table yet
 *   return: the hash, or NULL with an error set
 */
EVAL_LIST_HASH *
eval_hash_create (THREAD_ENTRY *thread_p)
{
  EVAL_LIST_HASH *hash = (EVAL_LIST_HASH *) db_private_alloc (thread_p, sizeof (EVAL_LIST_HASH));
  if (hash == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1, sizeof (EVAL_LIST_HASH));
      return NULL;
    }
  /* no table (HASH_METH_NOT_USE is 0), so no list scan open */
  memset (hash, 0, sizeof (EVAL_LIST_HASH));
  return hash;
}

/*
 * eval_hash_destroy () - frees a hash eval_hash_create made, with its table (qexec_clear_xasl)
 */
void
eval_hash_destroy (THREAD_ENTRY *thread_p, EVAL_LIST_HASH *hash)
{
  eval_hash_clear (thread_p, hash);
  db_private_free (thread_p, hash);
}

/*
 * eval_hash_destroy_tree () - frees the hashes in an XASL tree: a parallel aptr worker frees those it made in its own
 *			       private heap before another thread clears the tree (qexec_clear_xasl)
 */
void
eval_hash_destroy_tree (THREAD_ENTRY *thread_p, xasl_node *xasl)
{
  cubxasl::iterate_xasl_tree<bool> (xasl, [thread_p] (XASL_NODE *node)
  {
    if (node->list_hash != NULL)
      {
	eval_hash_destroy (thread_p, node->list_hash);
	node->list_hash = NULL;
      }
    return true;
  }, true);
}

/*
 * eval_hash_build () - the table over the list's values for the comparison; hash->unusable instead when a value has no
 *			hash
 *   return: error code
 *   method(in): the table's method (qdata_hscan_method)
 */
static int
eval_hash_build (THREAD_ENTRY *thread_p, EVAL_LIST_HASH *hash, QFILE_LIST_ID *list_id,
		 const DOMAIN_COMPARE *compare, HASH_METHOD method)
{
  QFILE_TUPLE_RECORD tuple_record = QFILE_TUPLE_RECORD_INITIALIZER;
  SCAN_CODE qp_scan;
  DB_VALUE value;
  unsigned int hash_key;
  bool is_null, hashed;
  int error;

  error = qfile_open_list_scan (list_id, &hash->list_scan);
  if (error != NO_ERROR)
    {
      return error;
    }
  error =
	  qdata_hscan_create (thread_p, &hash->hash_scan, method, list_id->tuple_cnt, "Eval Hash");
  if (error != NO_ERROR)
    {
      qfile_close_scan (thread_p, &hash->list_scan);
      return error;
    }
  hash->compare = compare;
  qfile_slot_set_layout (&hash->tuple_record, &hash->list_scan.list_id.type_list);

  while ((qp_scan = qfile_scan_list_next (thread_p, &hash->list_scan, &tuple_record, PEEK)) == S_SUCCESS)
    {
      error = qfile_slot_read_column_value (&tuple_record, 0, list_id->type_list.domp[0], &value, false, &is_null);
      if (error != NO_ERROR)
	{
	  break;
	}
      if (is_null)
	{
	  hash->has_null = true;
	  continue;
	}
      hashed = eval_hash_key (compare, 1, &value, &hash_key);
      /* read in place, but a compressed string is decompressed into its own buffer */
      pr_clear_value (&value);
      if (!hashed)
	{
	  hash->unusable = true;
	  break;
	}
      qdata_hscan_set_key (&hash->hash_scan, hash_key);
      error = qdata_hscan_insert (thread_p, &hash->hash_scan, &hash->list_scan, &tuple_record);
      if (error != NO_ERROR)
	{
	  break;
	}
    }
  if (error == NO_ERROR && !hash->unusable && qp_scan != S_END)
    {
      assert_release_error (er_errid () != NO_ERROR);
      error = er_errid ();
    }

  if (error != NO_ERROR || hash->unusable)
    {
      eval_hash_clear (thread_p, hash);
    }
  return error;
}

/*
 * eval_hash_clear () - frees the table and closes the list scan it reads through: the table's positions first, then
 *			the scan; the state of the execution stays
 */
static void
eval_hash_clear (THREAD_ENTRY *thread_p, EVAL_LIST_HASH *hash)
{
  if (hash->hash_scan.hash_list_scan_type != HASH_METH_NOT_USE)
    {
      qdata_hscan_destroy (thread_p, &hash->hash_scan);
      qfile_close_scan (thread_p, &hash->list_scan);
    }
  pr_clear_value (&hash->value);
  hash->has_null = false;
}

/*
 * eval_hash_for_lookup () - the hash that answers a lookup of the subquery's list in its current execution
 *   return: error code
 *   subquery(in): the subquery whose list list_id is; it owns the hash
 *   compare(in): the lookup's comparison (eval_hash_comparable)
 *   hash(out): the hash with its table, or NULL: the caller reads the list (the execution's first lookup, a value the
 *		table cannot take, hashing turned off, or another comparison than the table's)
 */
int
eval_hash_for_lookup (THREAD_ENTRY *thread_p, xasl_node *subquery, QFILE_LIST_ID *list_id,
		      const DOMAIN_COMPARE *compare, EVAL_LIST_HASH **hash)
{
  EVAL_LIST_HASH *list_hash = subquery->list_hash;
  int error;

  *hash = NULL;
  if (list_hash == NULL)
    {
      list_hash = eval_hash_create (thread_p);
      if (list_hash == NULL)
	{
	  return er_errid ();
	}
      list_hash->execution_no = subquery->execution_no;
      subquery->list_hash = list_hash;
    }
  else if (list_hash->execution_no != subquery->execution_no)
    {
      /* the subquery executed again: its list is another */
      eval_hash_clear (thread_p, list_hash);
      list_hash->execution_no = subquery->execution_no;
      list_hash->lookups = 0;
      list_hash->unusable = false;
    }

  if (list_hash->unusable)
    {
      return NO_ERROR;
    }
  if (list_hash->hash_scan.hash_list_scan_type == HASH_METH_NOT_USE)
    {
      const UINT64 mem_limit = (UINT64) prm_get_bigint_value (PRM_ID_MAX_HASH_LIST_SCAN_SIZE);
      if (mem_limit == 0)
	{
	  /* 0 turns hashing a list off, as it does for a hash list scan */
	  list_hash->unusable = true;
	  return NO_ERROR;
	}
      /* a table in memory costs about one read of the list to build, a hash file more the more rows a page of the list
       * holds: the lookups before the build read the list */
      const HASH_METHOD method = qdata_hscan_method (list_id, mem_limit);
      int build_lookup = 2;
      if (method == HASH_METH_HASH_FILE)
	{
	  const INT64 rows_per_page = list_id->tuple_cnt / MAX (list_id->page_cnt, 1);
	  build_lookup = MAX (2, (int) (1 + rows_per_page / EVAL_HASH_FILE_ROWS_PER_READ));
	}
      if (++list_hash->lookups < build_lookup)
	{
	  return NO_ERROR;
	}
      error = eval_hash_build (thread_p, list_hash, list_id, compare, method);
      if (error != NO_ERROR || list_hash->unusable)
	{
	  return error;
	}
      subquery->xasl_stats.hash_lookup_builds++;
      subquery->xasl_stats.hash_lookup_method = list_hash->hash_scan.hash_list_scan_type;
    }
  if (list_hash->compare != compare)
    {
      /* a term other than the one the table was built for */
      return NO_ERROR;
    }
  assert (list_hash->list_scan.list_id.tuple_cnt == list_id->tuple_cnt);

  *hash = list_hash;
  return NO_ERROR;
}

/*
 * eval_hash_has_null () - the list holds a NULL: a lookup that finds no equal value is UNKNOWN, not FALSE
 */
bool
eval_hash_has_null (const EVAL_LIST_HASH *hash)
{
  return hash->has_null;
}

/*
 * eval_hash_start () - starts the list values of the item's hash (eval_hash_next)
 *   return: false if the item has no hash in the type its comparison compares it in: the caller reads the list
 */
bool
eval_hash_start (EVAL_LIST_HASH *hash, const DB_VALUE *item)
{
  unsigned int hash_key;

  if (!eval_hash_key (hash->compare, 0, item, &hash_key))
    {
      return false;
    }
  qdata_hscan_set_key (&hash->hash_scan, hash_key);
  qfile_slot_reset (&hash->tuple_record);
  return true;
}

/*
 * eval_hash_next () - the next list value of the item's hash
 *   return: S_SUCCESS, S_END or S_ERROR
 *   value(out): the list value, read in place; it holds until the next call
 */
SCAN_CODE
eval_hash_next (THREAD_ENTRY *thread_p, EVAL_LIST_HASH *hash, DB_VALUE **value)
{
  bool is_null;

  if (qdata_hscan_probe (thread_p, &hash->hash_scan, &hash->list_scan, &hash->tuple_record) != NO_ERROR)
    {
      return S_ERROR;
    }
  if (hash->tuple_record.tpl == NULL)
    {
      return S_END;
    }
  pr_clear_value (&hash->value);
  if (qfile_slot_read_column_value (&hash->tuple_record, 0, hash->list_scan.list_id.type_list.domp[0], &hash->value,
				    false, &is_null) != NO_ERROR)
    {
      return S_ERROR;
    }
  /* the list's NULLs are not in the table */
  assert (!is_null);
  *value = &hash->value;
  return S_SUCCESS;
}

/*
 * eval_hash_end () - ends the list values of an item: a tuple read by its position keeps its list page fixed, so it is
 *		      unfixed between rows, as a hash list scan does when a probe ends
 */
void
eval_hash_end (THREAD_ENTRY *thread_p, EVAL_LIST_HASH *hash)
{
  if (hash->list_scan.position == S_ON && hash->list_scan.curr_pgptr != NULL)
    {
      qmgr_free_old_page_and_init (thread_p, hash->list_scan.curr_pgptr, hash->list_scan.list_id.tfile_vfid);
      hash->list_scan.position = S_AFTER;
    }
}
