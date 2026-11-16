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
 * query_normalize.h - the normalized form of a SQL text: one spelling of every text the lexer reads the same
 */

#ifndef _QUERY_NORMALIZE_H_
#define _QUERY_NORMALIZE_H_

/* both are <= 0, so qr_lookup's "norm_len <= 0" guard covers them unchanged;
 * qr_parse_file tells them apart only to word its rejection message. */
#define QR_NORM_ERR_OVERFLOW	(-1)
#define QR_NORM_ERR_LITERAL	(-2)

extern int qr_normalize_query (char *dst, int dst_size, const char *src, unsigned int *dst_hash, bool * unterminated);

#endif /* _QUERY_NORMALIZE_H_ */
