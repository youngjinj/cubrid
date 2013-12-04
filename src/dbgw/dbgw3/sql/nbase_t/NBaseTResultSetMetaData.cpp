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
#include "dbgw3/sql/nbase_t/NBaseTExecutor.h"
#include "dbgw3/sql/nbase_t/NBaseTResultSetMetaData.h"

namespace dbgw
{

  namespace sql
  {

    void NBaseTResultSetMetaData::addColumnName(const char *szName)
    {
      _NBaseTResultSetMetaDataRaw raw;
      raw.name = szName;
      raw.type = DBGW_VAL_TYPE_STRING;

      m_metaList.push_back(raw);
    }

    void NBaseTResultSetMetaData::setColumnType(size_t nIndex, ValueType type)
    {
      m_metaList[nIndex].type = type;
    }

    size_t NBaseTResultSetMetaData::getColumnCount() const
    {
      return m_metaList.size();
    }

    std::string NBaseTResultSetMetaData::getColumnName(size_t nIndex) const
    {
      if (m_metaList.size() < nIndex + 1)
        {
          ArrayIndexOutOfBoundsException e(nIndex,
              "NBaseTResultSetMetaData");
          DBGW_LOG_ERROR(e.what());
          throw e;
        }

      return m_metaList[nIndex].name;
    }

    ValueType NBaseTResultSetMetaData::getColumnType(size_t nIndex) const
    {
      if (m_metaList.size() < nIndex + 1)
        {
          ArrayIndexOutOfBoundsException e(nIndex,
              "NBaseTResultSetMetaData");
          DBGW_LOG_ERROR(e.what());
          throw e;
        }

      return m_metaList[nIndex].type;
    }

  }

}
