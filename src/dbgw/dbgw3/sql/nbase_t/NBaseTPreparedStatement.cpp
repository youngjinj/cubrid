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
#include "dbgw3/sql/nbase_t/NBaseTPreparedStatement.h"
#include "dbgw3/sql/nbase_t/NBaseTResultSet.h"
#include "dbgw3/sql/nbase_t/NBaseTConnection.h"

namespace dbgw
{

  namespace sql
  {

    NBaseTPreparedStatement::NBaseTPreparedStatement(
        trait<Connection>::sp pConnection, const char *szSql) :
      PreparedStatement(pConnection),
      m_pExecutor((NBaseTExecutor *) pConnection->getNativeHandle()),
      m_sql(szSql)
    {
      pConnection->registerResource(this);
    }

    NBaseTPreparedStatement::~ NBaseTPreparedStatement()
    {
      try
        {
          close();
        }
      catch (...)
        {
        }
    }

    void NBaseTPreparedStatement::addBatch()
    {
      UnsupportedOperationException e;
      DBGW_LOG_ERROR(e.what());
      throw e;
    }

    void NBaseTPreparedStatement::clearBatch()
    {
      UnsupportedOperationException e;
      DBGW_LOG_ERROR(e.what());
      throw e;
    }

    void NBaseTPreparedStatement::clearParameters()
    {
      UnsupportedOperationException e;
      DBGW_LOG_ERROR(e.what());
      throw e;
    }

    trait<ResultSet>::sp NBaseTPreparedStatement::executeQuery()
    {
      trait<NBaseTConnection>::sp pConnection =
          boost::dynamic_pointer_cast<NBaseTConnection>(getConnection());

      pConnection->beginTransaction();

      m_pExecutor->executeQuery(m_sql.c_str());

      trait<ResultSet>::sp pResultSet(
          new NBaseTResultSet(shared_from_this(),
              m_pExecutor->getResultStream().c_str()));
      return pResultSet;
    }

    int NBaseTPreparedStatement::executeUpdate()
    {
      trait<NBaseTConnection>::sp pConnection =
          boost::dynamic_pointer_cast<NBaseTConnection>(getConnection());

      pConnection->beginTransaction();

      return m_pExecutor->executeUpdate(m_sql.c_str());
    }

    std::vector<int> NBaseTPreparedStatement::executeBatch()
    {
      UnsupportedOperationException e;
      DBGW_LOG_ERROR(e.what());
      throw e;
    }

    void NBaseTPreparedStatement::setInt(int nIndex, int nValue)
    {
      UnsupportedOperationException e;
      DBGW_LOG_ERROR(e.what());
      throw e;
    }

    void NBaseTPreparedStatement::setLong(int nIndex, int64 lValue)
    {
      UnsupportedOperationException e;
      DBGW_LOG_ERROR(e.what());
      throw e;
    }

    void NBaseTPreparedStatement::setChar(int nIndex, char cValue)
    {
      UnsupportedOperationException e;
      DBGW_LOG_ERROR(e.what());
      throw e;
    }

    void NBaseTPreparedStatement::setCString(int nIndex, const char *szValue)
    {
      UnsupportedOperationException e;
      DBGW_LOG_ERROR(e.what());
      throw e;
    }

    void NBaseTPreparedStatement::setFloat(int nIndex, float fValue)
    {
      UnsupportedOperationException e;
      DBGW_LOG_ERROR(e.what());
      throw e;
    }

    void NBaseTPreparedStatement::setDouble(int nIndex, double dValue)
    {
      UnsupportedOperationException e;
      DBGW_LOG_ERROR(e.what());
      throw e;
    }

    void NBaseTPreparedStatement::setDate(int nIndex, const char *szValue)
    {
      UnsupportedOperationException e;
      DBGW_LOG_ERROR(e.what());
      throw e;
    }

    void NBaseTPreparedStatement::setDate(int nIndex, const struct tm &tmValue)
    {
      UnsupportedOperationException e;
      DBGW_LOG_ERROR(e.what());
      throw e;
    }

    void NBaseTPreparedStatement::setTime(int nIndex, const char *szValue)
    {
      UnsupportedOperationException e;
      DBGW_LOG_ERROR(e.what());
      throw e;
    }

    void NBaseTPreparedStatement::setTime(int nIndex, const struct tm &tmValue)
    {
      UnsupportedOperationException e;
      DBGW_LOG_ERROR(e.what());
      throw e;
    }

    void NBaseTPreparedStatement::setDateTime(int nIndex, const char *szValue)
    {
      UnsupportedOperationException e;
      DBGW_LOG_ERROR(e.what());
      throw e;
    }

    void NBaseTPreparedStatement::setDateTime(int nIndex, const struct tm &tmValue)
    {
      UnsupportedOperationException e;
      DBGW_LOG_ERROR(e.what());
      throw e;
    }

    void NBaseTPreparedStatement::setBytes(int nIndex, size_t nSize, const void *pValue)
    {
      UnsupportedOperationException e;
      DBGW_LOG_ERROR(e.what());
      throw e;
    }

    void NBaseTPreparedStatement::setNull(int nIndex, ValueType type)
    {
      UnsupportedOperationException e;
      DBGW_LOG_ERROR(e.what());
      throw e;
    }

    void NBaseTPreparedStatement::setClob(int nIndex, trait<Lob>::sp pLob)
    {
      UnsupportedOperationException e;
      DBGW_LOG_ERROR(e.what());
      throw e;
    }

    void NBaseTPreparedStatement::setBlob(int nIndex, trait<Lob>::sp pLob)
    {
      UnsupportedOperationException e;
      DBGW_LOG_ERROR(e.what());
      throw e;
    }

    void *NBaseTPreparedStatement::getNativeHandle() const
    {
      return NULL;
    }

    void NBaseTPreparedStatement::doClose()
    {
    }

  }

}
