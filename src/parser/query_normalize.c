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
 * query_normalize.c - the normalized form of a SQL text, which the query replace rules of the broker and the XASL
 *                     cache key of a statement are made of
 */

#ident "$Id$"

#include "config.h"

#include <string.h>

#include "porting.h"
#include "porting_inline.hpp"

#include "query_normalize.h"
// XXX: SHOULD BE THE LAST INCLUDE HEADER
#include "memory_wrapper.hpp"

/* character-class flags used by the query normalizer (qr_normalize_query) */
#define QR_CC_WS      0x01	/* whitespace, as csql_lexer.l defines it */
#define QR_CC_QUOTE   0x02	/* ' or "  : string-literal delimiter */
#define QR_CC_CMT     0x04	/* / or -  : possible comment or hint opener */
#define QR_CC_WORD    0x08	/* token body: gluing two of these merges two tokens */
#define QR_CC_IDQ     0x10	/* [ or `  : delimited-identifier opener */
#define QR_CC_DIGIT   0x20	/* 0-9     : can build a numeric literal across a '.' */
#define QR_CC_IDENT   0x40	/* a byte csql_lexer.l lets continue an identifier or number;
				 * unlike QR_CC_WORD it excludes '$' '?' and the quotes, which
				 * always end the lexer token before them */

/* highest operator id qr_norm_glue can address; id 0 means "not an operator" */
#define QR_NORM_NOPS  15

/* what the last '.' emitted was attached to, which decides whether it can be part of a
 * number: "1." continues one, "qr_t1." does not, and a '.' with whitespace before it may
 * still start one (".5"). */
#define QR_DOT_SEP    0
#define QR_DOT_IDENT  1
#define QR_DOT_NUM    2

/* qr_norm_char_class[b] = OR of the QR_CC_* flags for byte b, built once at
 * process startup so both the CAS lookup path and the broker rule-build path
 * index it directly with no per-call setup or per-character multi-compare. */
static unsigned char qr_norm_char_class[256];

/* toupper() must not be used in its place: it is locale dependent and folds bytes
 * >= 0x80, which would corrupt UTF-8 / EUC-KR identifiers.  no supported charset puts
 * a trail byte in 'a'..'z', so folding that range alone is safe byte-wise. */
static unsigned char qr_norm_fold[256];

static unsigned char qr_norm_id_close[256];

/* qr_norm_glue[qr_norm_opid[p]] bit qr_norm_opid[c] = "p immediately followed by c
 * lexes as one token", so the whitespace between them may not be dropped. */
static unsigned char qr_norm_opid[256];
static unsigned short qr_norm_glue[QR_NORM_NOPS + 1];

static_assert (QR_NORM_NOPS < (int) (sizeof (qr_norm_glue[0]) * 8), "operator id must fit qr_norm_glue");

static bool
qr_build_norm_char_class (void)
{
  /* the multi-character tokens of csql_lexer.l, plus the comment delimiters.  the
   * three-byte forms ("->>", "<=>") are caught by the tail check in
   * qr_norm_need_space (); their leading two bytes are already listed here. */
  static const char *const glue_pairs[] = {
    "--", "//", "/*", "*/", "->", ">>", "<<", "<>", ">=", "<=", "!=", "||", "&&", ":=", "?:", NULL
  };
  int i, j, nops = 0;

  qr_norm_char_class[(unsigned char) ' '] |= QR_CC_WS;
  qr_norm_char_class[(unsigned char) '\t'] |= QR_CC_WS;
  qr_norm_char_class[(unsigned char) '\n'] |= QR_CC_WS;
  qr_norm_char_class[(unsigned char) '\r'] |= QR_CC_WS;
  /* '\f' and '\v' are not listed: csql_lexer.l returns them as one-byte tokens, a syntax
   * error that would otherwise land on the key of the statement with a space there. */
  qr_norm_char_class[(unsigned char) '\''] |= QR_CC_QUOTE;
  qr_norm_char_class[(unsigned char) '"'] |= QR_CC_QUOTE;
  qr_norm_char_class[(unsigned char) '/'] |= QR_CC_CMT;
  qr_norm_char_class[(unsigned char) '-'] |= QR_CC_CMT;

  for (i = '0'; i <= '9'; i++)
    {
      qr_norm_char_class[i] |= QR_CC_WORD | QR_CC_DIGIT | QR_CC_IDENT;
    }
  for (i = 'a'; i <= 'z'; i++)
    {
      qr_norm_char_class[i] |= QR_CC_WORD | QR_CC_IDENT;
    }
  for (i = 'A'; i <= 'Z'; i++)
    {
      qr_norm_char_class[i] |= QR_CC_WORD | QR_CC_IDENT;
    }
  for (i = 0x80; i <= 0xff; i++)
    {
      qr_norm_char_class[i] |= QR_CC_WORD | QR_CC_IDENT;
    }
  qr_norm_char_class[(unsigned char) '_'] |= QR_CC_WORD | QR_CC_IDENT;
  qr_norm_char_class[(unsigned char) '$'] |= QR_CC_WORD;
  qr_norm_char_class[(unsigned char) '#'] |= QR_CC_WORD | QR_CC_IDENT;
  qr_norm_char_class[(unsigned char) '?'] |= QR_CC_WORD;
  /* a string delimiter opens a whole token, so the space in "'ZZ1' AND" survives
   * while the one in "code = 'ZZ1'" does not */
  qr_norm_char_class[(unsigned char) '\''] |= QR_CC_WORD;
  qr_norm_char_class[(unsigned char) '"'] |= QR_CC_WORD;

  qr_norm_char_class[(unsigned char) '['] |= QR_CC_IDQ;
  qr_norm_char_class[(unsigned char) '`'] |= QR_CC_IDQ;
  qr_norm_id_close[(unsigned char) '['] = (unsigned char) ']';
  qr_norm_id_close[(unsigned char) '`'] = (unsigned char) '`';

  for (i = 0; i < 256; i++)
    {
      qr_norm_fold[i] = (unsigned char) i;
    }
  for (i = 'a'; i <= 'z'; i++)
    {
      qr_norm_fold[i] = (unsigned char) (i - 'a' + 'A');
    }

  /* every byte of every pair gets an id, so a pair can never reference id 0 -- which
   * qr_norm_need_space reads for any byte that is not an operator at all. */
  for (i = 0; glue_pairs[i] != NULL; i++)
    {
      for (j = 0; j < 2; j++)
	{
	  unsigned char b = (unsigned char) glue_pairs[i][j];

	  if (qr_norm_opid[b] == 0)
	    {
	      assert (nops < QR_NORM_NOPS);
	      qr_norm_opid[b] = (unsigned char) ++nops;
	    }
	}
    }
  for (i = 0; glue_pairs[i] != NULL; i++)
    {
      qr_norm_glue[qr_norm_opid[(unsigned char) glue_pairs[i][0]]] |=
	(unsigned short) (1u << qr_norm_opid[(unsigned char) glue_pairs[i][1]]);
    }

  return true;
}

static bool qr_norm_char_class_ready = qr_build_norm_char_class ();

/*
 * qr_norm_need_space () - must the run of whitespace before c survive?  every other
 *   run is dropped, so "code = 'ZZ1'" and "code='ZZ1'" become one key.  the token
 *   stream may not change: the normalized ORIG is parsed by qr_check_replace_policy.
 *   return: true if one separator space has to be emitted
 *   dst(in), len(in): output so far (len > 0)
 *   c(in): the byte about to be emitted, already folded
 *   num_tok(in): the token body last emitted began with a digit, so it is a number and not an
 *     identifier -- "1 . 2" has to stay apart while "qr_t1 . code" may merge
 *   dot_kind(in): QR_DOT_* for the last '.' emitted
 */
STATIC_INLINE bool qr_norm_need_space (const char *dst, int len, char c, int num_tok, int dot_kind)
  __attribute__ ((ALWAYS_INLINE));

STATIC_INLINE bool
qr_norm_need_space (const char *dst, int len, char c, int num_tok, int dot_kind)
{
  unsigned char prev = (unsigned char) dst[len - 1];
  unsigned char pid;

  if ((qr_norm_char_class[prev] & QR_CC_WORD) && (qr_norm_char_class[(unsigned char) c] & QR_CC_WORD))
    {
      return true;
    }

  /* two backtick identifiers side by side ("`a` `b`": column a aliased b) would otherwise glue
   * into the doubled-backtick escape of ONE identifier ("`a``b`": the column named a`b) -- two
   * valid statements on one key.  '[' after ']' needs no space: csql_lexer.l has no "]]" escape,
   * so "[a][b]" already reads as two names; '"' is a QR_CC_WORD and is covered above. */
  if (prev == '`' && c == '`')
    {
      return true;
    }

  /* "\N" is NULL and "\USD" a currency sign, but "\ N" and "\ USD" are the won sign
   * followed by an identifier -- a syntax error that would otherwise land on the key of
   * the valid rule. */
  if (prev == '\\' && (qr_norm_char_class[(unsigned char) c] & QR_CC_IDENT))
    {
      return true;
    }

  /* besides keeping "a - -1" from becoming "a--1", a line comment that would swallow
   * the rest of the statement, this closes a spoofing hole: without it the invalid
   * "a < = 1" would land on the key of the valid rule "a <= 1" and be replaced,
   * turning the client's syntax error into a successful wrong query. */
  pid = qr_norm_opid[prev];
  if (pid != 0 && ((qr_norm_glue[pid] >> qr_norm_opid[(unsigned char) c]) & 1u) != 0)
    {
      return true;
    }

  if (c == '>' && len >= 2 && (memcmp (dst + len - 2, "->", 2) == 0 || memcmp (dst + len - 2, "<=", 2) == 0))
    {
      return true;
    }

  /* a '.' beside a digit builds a numeric literal out of what were three tokens: "1 . 2" is a
   * syntax error and "1.2" is one number.  the token before the dot tells a number from an
   * identifier, so "qr_t1 . code" keeps merging with "qr_t1.code" -- csql_lexer.l reads a
   * dot followed by whitespace and a name as the same DOT and name. */
  if (c == '.' && num_tok && (qr_norm_char_class[prev] & QR_CC_DIGIT))
    {
      return true;
    }
  if (prev == '.')
    {
      /* a dot directly followed by a digit always starts or continues a number (".5", "1.5"),
       * while with whitespace between it is a DOT token -- even after a name: "qr_t1.2" is
       * qr_t1 and the number .2, "qr_t1. 2" is qr_t1, DOT and 2. */
      if (qr_norm_char_class[(unsigned char) c] & QR_CC_DIGIT)
	{
	  return true;
	}
      /* an exponent or float suffix right after the dot keeps building the number: "1.E2" is
       * 100.0 while "1. e2" is 1.0 aliased e2 -- both parse, so a collision here does not even
       * announce itself as a broken query. */
      if ((c == 'E' || c == 'e' || c == 'F' || c == 'f') && dot_kind == QR_DOT_NUM)
	{
	  return true;
	}
    }

  /* the sign of an exponent belongs to the number the same way: "1e+5" is one token while
   * "1e +5" and "1e+ 5" are syntax errors.  an identifier that merely ends in a digit and an
   * 'e' ("foo1e + 2") is not a number, so it keeps merging.  "1.e+5" counts too: the 'e'
   * there opens a new word after the dot, so num_tok is clear and dot_kind tells instead. */
  if ((c == '+' || c == '-') && (prev == 'e' || prev == 'E') && len >= 2
      && ((num_tok && (qr_norm_char_class[(unsigned char) dst[len - 2]] & QR_CC_DIGIT))
	  || (dst[len - 2] == '.' && dot_kind == QR_DOT_NUM)))
    {
      return true;
    }
  if ((qr_norm_char_class[(unsigned char) c] & QR_CC_DIGIT) && (prev == '+' || prev == '-')
      && len >= 3 && (dst[len - 2] == 'e' || dst[len - 2] == 'E')
      && ((num_tok && (qr_norm_char_class[(unsigned char) dst[len - 3]] & QR_CC_DIGIT))
	  || (dst[len - 3] == '.' && dot_kind == QR_DOT_NUM)))
    {
      return true;
    }

  return false;
}

/*
 * qr_norm_is_backslash_token () - is run (the letters glued to a '\') one of the fixed tokens
 *   csql_lexer.l cuts right after those letters?  a digit glued to such a token starts a number
 *   ("\USD1.5" is the money literal 1.5, "\USD1 .5" a syntax error), while after any other run
 *   it only continues an identifier ("\n1" is '\' and the name n1).
 *   return: true for "\N" (NULL) and the currency signs
 *   run(in), n(in): the run, without the backslash
 */
static bool
qr_norm_is_backslash_token (const char *run, int n)
{
  static const char *const toks[] = {
    "N", "TL", "JPY", "USD", "KRW", "TRY", "KHR", "CNY", "INR", "RUB", "AUD", "CAD", "BRL",
    "RON", "EUR", "CHF", "DKK", "NOK", "BGN", "GBP", "VND", "CZK", "PLN", "SEK", "HRK", "RSD", NULL
  };
  int i;

  for (i = 0; toks[i] != NULL; i++)
    {
      if ((int) strlen (toks[i]) == n && memcmp (run, toks[i], n) == 0)
	{
	  return true;
	}
    }
  return false;
}

int
qr_normalize_query (char *dst, int dst_size, const char *src, unsigned int *dst_hash, bool * unterminated)
{
  int len = 0;
  int prev_space = 1;		/* drop leading whitespace */
  int num_tok = 0;		/* the token body last emitted began with a digit */
  int dot_kind = QR_DOT_SEP;	/* what the last '.' emitted was attached to */
  int raw_ident = 0;		/* the identifier being emitted is glued to a '\' */
  int raw_start = 0;		/* dst offset of the first byte after that '\' */
  char quote = 0;		/* current open quote char, 0 = outside literal */
  const char *s;
  unsigned int h = 5381;

  /* set when a hint, block comment, delimited name or string literal is not closed:
   * csql_lexer.l fails such text with UNEXPECTED_EOF, while the normalizer truncates it
   * (or fabricates a closer).  rule loading rejects such text, and lookup declines to
   * replace it. */
  if (unterminated != NULL)
    {
      *unterminated = false;
    }

  /* emit one char to dst with bounds check + rolling hash */
#define QR_PUT(ch) \
  do { \
    if (len >= dst_size - 1) \
      { \
        return QR_NORM_ERR_OVERFLOW; \
      } \
    dst[len++] = (char) (ch); \
    h = ((h << 5) + h) ^ (unsigned char) (ch); \
  } while (0)

/* keywords and identifiers are case-insensitive in CUBRID (the lexer matches keywords
 * as [aA][bB].., schema names go through sm_downcase_name / intl_identifier_casecmp),
 * so folding makes one key out of every spelling.  string literals are excluded: they
 * compare under the column's collation, which is case sensitive by default.  so are the
 * few spots where csql_lexer.l itself is case sensitive: see the 0x/0b and backslash
 * cases in the ordinary-character path below. */
#define QR_FOLD(ch)	((char) qr_norm_fold[(unsigned char) (ch)])

#define QR_PEND(ch) \
  do { \
    if (prev_space && len > 0 && qr_norm_need_space (dst, len, (ch), num_tok, dot_kind)) \
      { \
        QR_PUT (' '); \
      } \
  } while (0)

  for (s = src; *s != '\0'; s++)
    {
      char c = *s;

      if (quote != 0)
	{
	  /* inside a string literal: copy verbatim AND hash, so the returned
	   * hash equals qr_hash_str(dst) even when the query contains a literal. */
	  if (c == '\\')
	    {
	      /* csql_lexer.l pairs a backslash with the byte after it under either setting of
	       * no_backslash_escapes ([\\].), so "C:\\dir", "a\\nb", the LIKE escapes and an
	       * escaped backslash "\\\\" all close the literal at the same byte; only the
	       * literal's value differs, and the replacement is prepared in the same session.  the
	       * one exception is a backslash before the literal's own quote: with
	       * no_backslash_escapes=yes the quote closes the literal, with =no it is an escaped
	       * quote and the literal goes on.  that is a session parameter, and guessing wrong
	       * mis-tracks the quote state -- the rest of the statement would then be folded as if
	       * it were code -- so only that case costs the query its replacement.  copying the pair
	       * together is what tells "\\\\'" (an escaped backslash, then the closing quote) from
	       * "\\'". */
	      if (*(s + 1) == quote)
		{
		  return QR_NORM_ERR_LITERAL;
		}
	      QR_PUT (c);
	      if (*(s + 1) != '\0')
		{
		  s++;
		  QR_PUT (*s);
		}
	      prev_space = 0;
	      continue;
	    }
	  QR_PUT (c);
	  if (c == quote)
	    {
	      if (*(s + 1) == quote)
		{
		  /* doubled quote = escaped quote, stay inside the literal */
		  QR_PUT (quote);
		  s++;
		}
	      else
		{
		  quote = 0;
		}
	    }
	  prev_space = 0;
	  continue;
	}

      unsigned char cls = qr_norm_char_class[(unsigned char) c];

      if (cls & QR_CC_QUOTE)
	{
	  QR_PEND (c);
	  QR_PUT (c);
	  quote = c;
	  prev_space = 0;
	  continue;
	}

      if (cls & QR_CC_IDQ)
	{
	  /* a delimited identifier is one token, so its inner whitespace may not be
	   * collapsed -- "[my  col]" and "[my col]" name different columns.  its case IS
	   * folded: CUBRID identifiers are case-insensitive however they are delimited.
	   * '"' stays with the string handling above because whether it delimits an
	   * identifier or a string depends on ansi_quotes. */
	  char close = (char) qr_norm_id_close[(unsigned char) c];

	  QR_PEND (c);
	  QR_PUT (c);
	  for (s++; *s != '\0'; s++)
	    {
	      if (*s != close)
		{
		  QR_PUT (QR_FOLD (*s));
		  continue;
		}
	      QR_PUT (close);
	      /* csql_lexer.l gives only the backtick form a doubled escape; "]]" is a
	       * closed identifier followed by a stray ']'. */
	      if (close != '`' || *(s + 1) != close)
		{
		  break;
		}
	      QR_PUT (close);
	      s++;
	    }
	  if (*s == '\0')
	    {
	      if (unterminated != NULL)
		{
		  *unterminated = true;
		}
	      s--;		/* unterminated; loop s++ re-reads the terminator */
	    }
	  prev_space = 0;
	  continue;
	}

      if (cls & QR_CC_CMT)
	{
	  /* c is '/' or '-'.  a comment opens with slash-star, slash-slash or
	   * dash-dash; a lone '/' or '-' is an operator (falls through to the
	   * ordinary handling below).  a regular comment is stripped (acts as a
	   * separator); a HINT comment (opener followed by '+') is preserved with
	   * its inner whitespace collapsed, because it can change the plan. */
	  int block = (c == '/' && *(s + 1) == '*');
	  int line = ((c == '/' && *(s + 1) == '/') || (c == '-' && *(s + 1) == '-'));

	  if (block || line)
	    {
	      if (*(s + 2) == '+')
		{
		  /* HINT comment: keep opener and '+', collapse inner whitespace */
		  int inner_prev_space = 1;

		  QR_PEND (c);
		  QR_PUT (c);
		  QR_PUT (*(s + 1));
		  QR_PUT ('+');
		  s += 3;	/* skip opener and the '+' */

#define QR_HINT_END(p)	(block ? ((p)[0] == '*' && (p)[1] == '/') : (p)[0] == '\n')
		  for (; *s != '\0' && !QR_HINT_END (s); s++)
		    {
		      if (*s == '"' || *s == '[' || *s == '`')
			{
			  /* read_hint_args reads a delimited name as one token, so its inner
			   * whitespace is kept: "[i  1]" and "[i 1]" name different indexes.  the
			   * lexer still ends the hint at the first terminator, quoted or not. */
			  char close = (*s == '[') ? ']' : *s;

			  if (inner_prev_space && len > 0 && qr_norm_need_space (dst, len, *s, num_tok, dot_kind))
			    {
			      QR_PUT (' ');
			    }
			  QR_PUT (*s);
			  for (s++; *s != '\0' && !QR_HINT_END (s); s++)
			    {
			      QR_PUT (QR_FOLD (*s));
			      if (*s == '\\' && *(s + 1) == close)
				{
				  s++;	/* the hint scanner skips an escaped delimiter */
				  QR_PUT (*s);
				}
			      else if (*s == close)
				{
				  break;
				}
			    }
			  if (*s == '\0' || QR_HINT_END (s))
			    {
			      s--;	/* loop s++ re-reads the terminator */
			    }
			  inner_prev_space = 0;
			  continue;
			}
		      if (qr_norm_char_class[(unsigned char) *s] & QR_CC_WS)
			{
			  inner_prev_space = 1;
			}
		      else
			{
			  char fc = QR_FOLD (*s);

			  if (inner_prev_space && len > 0 && qr_norm_need_space (dst, len, fc, num_tok, dot_kind))
			    {
			      QR_PUT (' ');
			    }
			  QR_PUT (fc);
			  inner_prev_space = 0;
			}
		    }
#undef QR_HINT_END

		  if (block)
		    {
		      QR_PUT ('*');
		      QR_PUT ('/');
		      if (*s != '\0')
			{
			  s++;	/* at the closing slash; loop s++ steps past it */
			}
		      else
			{
			  if (unterminated != NULL)
			    {
			      *unterminated = true;
			    }
			  s--;	/* unterminated; loop s++ re-reads the terminator */
			}
		    }
		  else
		    {
		      /* the newline terminates a line hint, so it must survive: folded into a
		       * space it would let the hint swallow the rest of the statement, and
		       * qr_check_replace_policy parses this text.  the key stays consistent
		       * because the broker and the CAS both normalize with this function. */
		      QR_PUT ('\n');
		      s--;	/* loop s++ re-reads the newline or terminator */
		    }
		  prev_space = 1;	/* separate the hint from the next token */
		  continue;
		}
	      else
		{
		  /* regular comment: skip it, leaving a separator */
		  if (block)
		    {
		      s += 2;	/* skip the block-comment opener */
		      while (*s != '\0' && !(*s == '*' && *(s + 1) == '/'))
			{
			  s++;
			}
		      if (*s != '\0')
			{
			  s++;	/* at the closing slash; loop s++ steps past it */
			}
		      else
			{
			  if (unterminated != NULL)
			    {
			      *unterminated = true;
			    }
			  s--;	/* unterminated; loop s++ re-reads the terminator */
			}
		    }
		  else
		    {
		      s += 2;	/* skip the line-comment opener */
		      while (*s != '\0' && *s != '\n')
			{
			  s++;
			}
		      s--;	/* loop s++ re-reads the newline or terminator */
		    }
		  prev_space = 1;
		  continue;
		}
	    }
	  /* lone '/' or '-' operator: fall through to ordinary handling */
	}

      if (cls & QR_CC_WS)
	{
	  prev_space = 1;
	  continue;
	}

      {
	char fc = QR_FOLD (c);

	QR_PEND (fc);
	if (qr_norm_char_class[(unsigned char) c] & QR_CC_IDENT)
	  {
	    /* csql_lexer.l matches these case-sensitively, so folding would give one key
	     * to statements it reads differently:
	     *   "\N" (NULL), "\USD" .. : upper case only; "\n" is '\' plus identifier n
	     *   "0x1F", "0b101"        : lower-case prefix only; "0X1F" is 0 aliased X1F */
	    if (len == 0 || !(qr_norm_char_class[(unsigned char) dst[len - 1]] & QR_CC_IDENT))
	      {
		raw_ident = (len > 0 && dst[len - 1] == '\\');
		raw_start = len;
	      }
	    if (raw_ident)
	      {
		fc = c;
	      }
	    else if ((c == 'x' || c == 'X' || c == 'b' || c == 'B') && len > 0 && dst[len - 1] == '0'
		     && (len == 1 || !(qr_norm_char_class[(unsigned char) dst[len - 2]] & QR_CC_IDENT)))
	      {
		fc = c;
	      }
	  }
	if (qr_norm_char_class[(unsigned char) fc] & QR_CC_WORD)
	  {
	    /* a new lexer token starts here unless fc continues an identifier or number, which
	     * QR_CC_IDENT tells: '$' and '?' are tokens of their own, so the 1 of "$1" opens a
	     * number -- "$1 .5" (a syntax error) must not merge into the money literal "$1.5".
	     * a "\USD"-like sign is cut after its letters the same way. */
	    int tok_start = (len == 0
			     || !(qr_norm_char_class[(unsigned char) fc] & QR_CC_IDENT)
			     || !(qr_norm_char_class[(unsigned char) dst[len - 1]] & QR_CC_IDENT));

	    if (!tok_start && raw_ident && (qr_norm_char_class[(unsigned char) fc] & QR_CC_DIGIT))
	      {
		tok_start = qr_norm_is_backslash_token (dst + raw_start, len - raw_start);
	      }
	    if (tok_start)
	      {
		num_tok = (qr_norm_char_class[(unsigned char) fc] & QR_CC_DIGIT) ? 1 : 0;
	      }
	  }
	else if (fc == '.')
	  {
	    if (prev_space || len == 0 || !(qr_norm_char_class[(unsigned char) dst[len - 1]] & QR_CC_WORD))
	      {
		dot_kind = QR_DOT_SEP;
	      }
	    else
	      {
		dot_kind = num_tok ? QR_DOT_NUM : QR_DOT_IDENT;
	      }
	  }
	prev_space = 0;
	QR_PUT (fc);
      }
    }
  if (quote != 0 && unterminated != NULL)
    {
      *unterminated = true;	/* the string literal never closed */
    }
#undef QR_PEND
#undef QR_FOLD
#undef QR_PUT

  dst[len] = '\0';
  *dst_hash = h;

  return len;
}
