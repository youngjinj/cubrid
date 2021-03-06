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
 * uca_support.c : Unicode Collation Algorithm support
 *
 */

#ifndef _UCA_SUPPORT_H_
#define _UCA_SUPPORT_H_

#ident "$Id$"

#include "locale_support.h"

#ifdef __cplusplus
extern "C"
{
#endif

  int uca_process_collation (LOCALE_COLLATION * lc, bool is_verbose);
  void uca_free_data (void);

#ifdef __cplusplus
}
#endif

#endif				/* _UCA_SUPPORT_H_ */
