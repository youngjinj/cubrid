/*
* Copyright (C) 2008 Search Solution Corporation. All rights reserved by Search Solution. 
*
* Redistribution and use in source and binary forms, with or without modification, 
* are permitted provided that the following conditions are met: 
*
* - Redistributions of source code must retain the above copyright notice, 
*   this list of conditions and the following disclaimer. 
*
* - Redistributions in binary form must reproduce the above copyright notice, 
*   this list of conditions and the following disclaimer in the documentation 
*   and/or other materials provided with the distribution. 
*
* - Neither the name of the <ORGANIZATION> nor the names of its contributors 
*   may be used to endorse or promote products derived from this software without 
*   specific prior written permission. 
*
* THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND 
* ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED 
* WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. 
* IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, 
* INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, 
* BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, 
* OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, 
* WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) 
* ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY 
* OF SUCH DAMAGE. 
*
*/  
  
#ifndef	__ODBC_DESC_HEADER	/* to avoid multiple inclusion */
#define	__ODBC_DESC_HEADER
  
#include		"odbc_connection.h"
#include		"odbc_portable.h"
#include		"odbc_diag_record.h"
#include		"odbc_statement.h"
  
/*------------------------------------------------------------------------
STRUCT : st_odbc_record (ODBC_RECORD)
*-----------------------------------------------------------------------*/ 
typedef struct st_odbc_record 
{
  
    /* Driver-specific members */ 
  struct st_odbc_desc *desc;
   
   
  
    /* supported descriptor record field */ 
  char *base_column_name;
   
   
   
   
   
   
   
   
   
   
   
   
   
   
   
   
   
   
   
   
   
   
   
   
   
   
  
    /* not supported */ 
  long auto_unique_value;
   
   
   
   
   


{
  
   
  
   
  struct st_odbc_desc *next;	// for explicitly allocated descriptor
   
    /* header fields */ 
  short alloc_type;
   
  
    // the previous array_size of fetch call (SQLFetch, SQLExtendedFetch, SQLFetchScroll)
  unsigned long fetched_size;
  
   
   
   
   
  







				     

				   
				   
				   

				     

					  
					  
					  SQLLEN * indicator_ptr);


			   




#endif	/* ! __ODBC_DESC_HEADER */