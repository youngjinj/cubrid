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

#include "dbgw3/sql/nbase_t/NBaseTCommon.h"
#include "dbgw3/sql/nbase_t/NBaseTException.h"

namespace dbgw
{

  namespace sql
  {

    NBaseTException::NBaseTException(const _ExceptionContext &context) :
      Exception(context)
    {
      switch (getInterfaceErrorCode())
        {
        case nbase_t::NE_CONN:
        case nbase_t::NE_RPC:
          setConnectionError(true);
          break;
        default:
          setConnectionError(false);
          break;
        }
    }

    NBaseTException NBaseTExceptionFactory::create(
        const std::string &errorMessage)
    {
      return create(-1, errorMessage);
    }

    NBaseTException NBaseTExceptionFactory::create(
        int nInterfaceErrorCode, const std::string &errorMessage)
    {
      _ExceptionContext context;
      context.nErrorCode = DBGW_ER_INTERFACE_ERROR;
      context.nInterfaceErrorCode = nInterfaceErrorCode;

      const char *szErrMsg = nbase_get_result_errmsg(nInterfaceErrorCode);
      if (nInterfaceErrorCode != -1 && szErrMsg != NULL)
        {
          context.errorMessage = szErrMsg;
          context.what = szErrMsg;
        }
      else
        {
          context.errorMessage = errorMessage;
          context.what = errorMessage;
        }
      context.bConnectionError = false;

      std::stringstream buffer;
      buffer << "[" << context.nErrorCode << "]";
      buffer << "[" << context.nInterfaceErrorCode << "]";
      buffer << " " << context.errorMessage;
      context.what = buffer.str();

      return NBaseTException(context);
    }

  }

}
