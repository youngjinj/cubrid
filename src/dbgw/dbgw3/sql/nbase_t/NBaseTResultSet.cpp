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

#include <jansson.h>
#include "dbgw3/sql/nbase_t/NBaseTCommon.h"
#include "dbgw3/sql/nbase_t/NBaseTException.h"
#include "dbgw3/sql/nbase_t/NBaseTExecutor.h"
#include "dbgw3/sql/nbase_t/NBaseTPreparedStatement.h"
#include "dbgw3/sql/nbase_t/NBaseTResultSetMetaData.h"
#include "dbgw3/sql/nbase_t/NBaseTResultSet.h"

namespace dbgw
{

  namespace sql
  {
    json_t *getJsonObject(json_t *root_p, const char *szKey)
    {
      json_t *object_p = json_object_get(root_p, szKey);
      if (object_p == NULL)
        {
          std::string errMsg = "fail to decode query result. there is no ";
          errMsg += szKey;

          NBaseTException e = NBaseTExceptionFactory::create(errMsg);
          DBGW_LOG_ERROR(e.what());
          throw e;
        }

      return object_p;
    }

    json_t *getJsonList(json_t *root_p, const char *szKey)
    {
      json_t *object_p = getJsonObject(root_p, szKey);
      if (json_is_array(object_p) == false)
        {
          std::string errMsg = "fail to decode query result. invalid value type (";
          errMsg += szKey;
          errMsg += ")";

          NBaseTException e = NBaseTExceptionFactory::create(errMsg);
          DBGW_LOG_ERROR(e.what());
          throw e;
        }

      return object_p;
    }

    int getJsonInteger(json_t *root_p, const char *szKey)
    {
      json_t *object_p = getJsonObject(root_p, szKey);
      if (json_is_integer(object_p) == false)
        {
          std::string errMsg = "fail to decode query result. invalid value type (";
          errMsg += szKey;
          errMsg += ")";

          NBaseTException e = NBaseTExceptionFactory::create(errMsg);
          DBGW_LOG_ERROR(e.what());
          throw e;
        }

      return json_integer_value(object_p);
    }

    class NBaseTResultSet::Impl
    {
    public:
      Impl(NBaseTResultSet *pSelf, trait<Statement>::sp pStatement,
          const char *szResultStream) :
        m_pSelf(pSelf), m_pJsRoot(NULL), m_nRowCount(0), m_nCursor(0),
        m_pMetaData(new NBaseTResultSetMetaData())
      {
        if (strcmp(szResultStream, "") != 0)
          {
            json_error_t jsonError;
            m_pJsRoot = json_loads(szResultStream, 0, &jsonError);
            if (m_pJsRoot == NULL)
              {
                std::string errMsg = "fail to decode query result. (";
                errMsg += szResultStream;
                errMsg += ")";

                NBaseTException e = NBaseTExceptionFactory::create(errMsg);
                DBGW_LOG_ERROR(e.what());
                throw e;
              }

            m_nRowCount = getJsonInteger(m_pJsRoot, "retcode");

            makeMetaData();
          }
      }

      ~Impl()
      {
        close();
      }

      void close()
      {
        if (m_pJsRoot)
          {
            json_decref(m_pJsRoot);
          }

        m_pJsRoot = NULL;
      }

      bool isFirst()
      {
        return m_nCursor == 0;
      }

      bool first()
      {
        m_nCursor = 0;
        return true;
      }

      bool next()
      {
        if (m_nCursor >= m_nRowCount)
          {
            return false;
          }

        makeResultSet();

        m_nCursor++;

        return true;
      }

      int getRowCount() const
      {
        return m_nRowCount;
      }

      ValueType getType(int nIndex) const
      {
        if (m_nCursor == 0)
          {
            InvalidCursorPositionException e;
            DBGW_LOG_ERROR(e.what());
            throw e;
          }

        return m_pMetaData->getColumnType(nIndex);
      }

      int getInt(int nIndex) const
      {
        if (m_nCursor == 0)
          {
            InvalidCursorPositionException e;
            DBGW_LOG_ERROR(e.what());
            throw e;
          }

        int nValue;
        if (m_resultSet.getInt(nIndex, &nValue) == false)
          {
            throw getLastException();
          }

        return nValue;
      }

      const char *getCString(int nIndex) const
      {
        if (m_nCursor == 0)
          {
            InvalidCursorPositionException e;
            DBGW_LOG_ERROR(e.what());
            throw e;
          }

        const char *szalue;
        if (m_resultSet.getCString(nIndex, &szalue) == false)
          {
            throw getLastException();
          }

        return szalue;
      }

      int64 getLong(int nIndex) const
      {
        if (m_nCursor == 0)
          {
            InvalidCursorPositionException e;
            DBGW_LOG_ERROR(e.what());
            throw e;
          }

        int64 lValue;
        if (m_resultSet.getLong(nIndex, &lValue) == false)
          {
            throw getLastException();
          }

        return lValue;
      }

      char getChar(int nIndex) const
      {
        if (m_nCursor == 0)
          {
            InvalidCursorPositionException e;
            DBGW_LOG_ERROR(e.what());
            throw e;
          }

        char cValue;
        if (m_resultSet.getChar(nIndex, &cValue) == false)
          {
            throw getLastException();
          }

        return cValue;
      }

      float getFloat(int nIndex) const
      {
        if (m_nCursor == 0)
          {
            InvalidCursorPositionException e;
            DBGW_LOG_ERROR(e.what());
            throw e;
          }

        float fValue;
        if (m_resultSet.getFloat(nIndex, &fValue) == false)
          {
            throw getLastException();
          }

        return fValue;
      }

      double getDouble(int nIndex) const
      {
        if (m_nCursor == 0)
          {
            InvalidCursorPositionException e;
            DBGW_LOG_ERROR(e.what());
            throw e;
          }

        double dValue;
        if (m_resultSet.getDouble(nIndex, &dValue) == false)
          {
            throw getLastException();
          }

        return dValue;
      }

      bool getBool(int nIndex) const
      {
        if (m_nCursor == 0)
          {
            InvalidCursorPositionException e;
            DBGW_LOG_ERROR(e.what());
            throw e;
          }

        bool bValue;
        if (m_resultSet.getBool(nIndex, &bValue) == false)
          {
            throw getLastException();
          }

        return bValue;
      }

      struct tm getDateTime(int nIndex) const
      {
        if (m_nCursor == 0)
          {
            InvalidCursorPositionException e;
            DBGW_LOG_ERROR(e.what());
            throw e;
          }

        struct tm tmValue;
        if (m_resultSet.getDateTime(nIndex, &tmValue) == false)
          {
            throw getLastException();
          }

        return tmValue;
      }

      void getBytes(int nIndex, size_t *pSize, const char **pValue) const
      {
        if (m_nCursor == 0)
          {
            InvalidCursorPositionException e;
            DBGW_LOG_ERROR(e.what());
            throw e;
          }

        if (m_resultSet.getBytes(nIndex, pSize, pValue) == false)
          {
            throw getLastException();
          }
      }

      const Value *getValue(int nIndex) const
      {
        if (m_nCursor == 0)
          {
            InvalidCursorPositionException e;
            DBGW_LOG_ERROR(e.what());
            throw e;
          }

        const Value *pValue = m_resultSet.getValue(nIndex);
        if (pValue == NULL)
          {
            throw getLastException();
          }

        return pValue;
      }

      trait<ResultSetMetaData>::sp getMetaData() const
      {
        return m_pMetaData;
      }

      _ValueSet &getInternalValuSet()
      {
        return m_resultSet;
      }

    private:
      void makeMetaData()
      {
        json_t *pJsRetData = getJsonObject(m_pJsRoot, "retdata");
        json_t *pJsColInfo = getJsonList(pJsRetData, "colinfo");
        for (size_t i = 0; i < json_array_size(pJsColInfo); i++)
          {
            m_pMetaData->addColumnName(
                json_string_value(json_array_get(pJsColInfo, i)));
          }

        json_t *pJsRows = getJsonObject(pJsRetData, "rows");
        json_t *pJsRow = NULL;
        json_t *pJsCol = NULL;
        for (size_t i = 0; i < m_pMetaData->getColumnCount(); i++)
          {
            for (size_t j = 0; j < json_array_size(pJsRows); j++)
              {
                pJsRow = json_array_get(pJsRows, j);
                pJsCol = json_array_get(pJsRow, i);

                if (json_is_null(pJsCol))
                  {
                    continue;
                  }

                if (json_is_integer(pJsCol))
                  {
                    m_pMetaData->setColumnType(i, DBGW_VAL_TYPE_LONG);
                    break;
                  }
                else if (json_is_real(pJsCol))
                  {
                    m_pMetaData->setColumnType(i, DBGW_VAL_TYPE_DOUBLE);
                    break;
                  }
                else if (json_is_boolean(pJsCol))
                  {
                    m_pMetaData->setColumnType(i, DBGW_VAL_TYPE_BOOL);
                    break;
                  }
                else
                  {
                    const char *szValue = json_string_value(pJsCol);
                    const char *szFmt = "%d:%d:%d.%d %s %d/%d/%d";

                    int hh, mi, ss, ms, yy, mm, dd;
                    char ampm[3];
                    int res = sscanf(szValue, szFmt, &hh, &mi, &ss, &ms, ampm,
                        &mm, &dd, &yy);
                    if (res == 8 && (strcasecmp(ampm, "am") == 0
                        || strcasecmp(ampm, "pm") == 0))
                      {
                        m_pMetaData->setColumnType(i, DBGW_VAL_TYPE_DATETIME);
                      }

                    break;
                  }
              }
          }
      }

      void makeResultSet()
      {
        json_t *pJsRetData = getJsonObject(m_pJsRoot, "retdata");
        json_t *pJsRows = getJsonList(pJsRetData, "rows");
        json_t *pJsRow = json_array_get(pJsRows, m_nCursor);
        json_t *pJsCol = NULL;

        ValueType type;
        const char *szKey;
        for (size_t i = 0; i < json_array_size(pJsRow); i++)
          {
            pJsCol = json_array_get(pJsRow, i);
            type = m_pMetaData->getColumnType(i);
            szKey = m_pMetaData->getColumnName(i).c_str();

            if (json_is_null(pJsCol))
              {
                doMakeResultSet(i, szKey, type, NULL, true, 0);
                continue;
              }

            if (type == DBGW_VAL_TYPE_LONG)
              {
                int64 lValue = json_integer_value(pJsCol);
                doMakeResultSet(i, szKey, DBGW_VAL_TYPE_LONG, &lValue, false,
                    sizeof(int64));
              }
            else if (type == DBGW_VAL_TYPE_DOUBLE)
              {
                double dValue = json_real_value(pJsCol);
                doMakeResultSet(i, szKey, DBGW_VAL_TYPE_DOUBLE, &dValue, false,
                    sizeof(double));
              }
            else if (type == DBGW_VAL_TYPE_BOOL)
              {
                bool bValue = true;
                if (json_is_false(pJsCol))
                  {
                    bValue = false;
                  }

                doMakeResultSet(i, szKey, DBGW_VAL_TYPE_BOOL, &bValue, false,
                    sizeof(int));
              }
            else if (type == DBGW_VAL_TYPE_DATETIME)
              {
                const char *szValue = json_string_value(pJsCol);
                const char *szFmt = "%d:%d:%d.%d %s %d/%d/%d";

                int hh, mi, ss, ms, yy, mm, dd;
                char ampm[3];
                int res = sscanf(szValue, szFmt, &hh, &mi, &ss, &ms, ampm, &mm,
                    &dd, &yy);

                if (strcasecmp(ampm, "pm") == 0 && hh < 12)
                  {
                    hh += 12;
                  }

                char szDateTime[20];
                snprintf(szDateTime, 20, "%04d-%02d-%02d %02d:%02d:%02d",
                    yy, mm, dd, hh, mi, ss);

                doMakeResultSet(i, szKey, DBGW_VAL_TYPE_DATETIME,
                    (void *) szDateTime, false, 19);
              }
            else
              {
                const char *szValue = json_string_value(pJsCol);
                doMakeResultSet(i, szKey, DBGW_VAL_TYPE_STRING,
                    (void *) szValue, false, strlen(szValue));
              }
          }
      }

      void doMakeResultSet(size_t nIndex, const char *szKey, ValueType type,
          void *pValue, bool bNull, int nLength)
      {
        if (m_nCursor == 0)
          {
            m_resultSet.put(szKey, type, pValue, bNull, nLength);
          }
        else
          {
            m_resultSet.replace(nIndex, type, pValue, bNull, nLength);
          }
      }

    private:
      NBaseTResultSet *m_pSelf;
      json_t *m_pJsRoot;
      int m_nRowCount;
      int m_nCursor;
      _ValueSet m_resultSet;
      trait<NBaseTResultSetMetaData>::sp m_pMetaData;
    };

    NBaseTResultSet::NBaseTResultSet(trait<Statement>::sp pStatement,
        const char *szResultStream) :
      ResultSet(pStatement), m_pImpl(new Impl(this, pStatement, szResultStream))
    {
      pStatement->registerResource(this);
    }

    NBaseTResultSet::~NBaseTResultSet()
    {
      try
        {
          close();
        }
      catch (...)
        {
        }

      if (m_pImpl)
        {
          delete m_pImpl;
        }
    }

    bool NBaseTResultSet::isFirst()
    {
      return m_pImpl->isFirst();
    }

    bool NBaseTResultSet::first()
    {
      return m_pImpl->first();
    }

    bool NBaseTResultSet::next()
    {
      return m_pImpl->next();
    }

    int NBaseTResultSet::getRowCount() const
    {
      return m_pImpl->getRowCount();
    }

    ValueType NBaseTResultSet::getType(int nIndex) const
    {
      return m_pImpl->getType(nIndex);
    }

    int NBaseTResultSet::getInt(int nIndex) const
    {
      return m_pImpl->getInt(nIndex);
    }

    const char *NBaseTResultSet::getCString(int nIndex) const
    {
      return m_pImpl->getCString(nIndex);
    }

    int64 NBaseTResultSet::getLong(int nIndex) const
    {
      return m_pImpl->getLong(nIndex);
    }

    char NBaseTResultSet::getChar(int nIndex) const
    {
      return m_pImpl->getChar(nIndex);
    }

    float NBaseTResultSet::getFloat(int nIndex) const
    {
      return m_pImpl->getFloat(nIndex);
    }

    double NBaseTResultSet::getDouble(int nIndex) const
    {
      return m_pImpl->getDouble(nIndex);
    }

    bool NBaseTResultSet::getBool(int nIndex) const
    {
      return m_pImpl->getBool(nIndex);
    }

    struct tm NBaseTResultSet::getDateTime(int nIndex) const
    {
      return m_pImpl->getDateTime(nIndex);
    }

    void NBaseTResultSet::getBytes(int nIndex, size_t *pSize,
        const char **pValue) const
    {
      m_pImpl->getBytes(nIndex, pSize, pValue);
    }

    const Value *NBaseTResultSet::getValue(int nIndex) const
    {
      return m_pImpl->getValue(nIndex);
    }

    trait<Lob>::sp NBaseTResultSet::getClob(int nIndex) const
    {
      UnsupportedOperationException e;
      DBGW_LOG_ERROR(e.what());
      throw e;
    }

    trait<Lob>::sp NBaseTResultSet::getBlob(int nIndex) const
    {
      UnsupportedOperationException e;
      DBGW_LOG_ERROR(e.what());
      throw e;
    }

    trait<ResultSet>::sp NBaseTResultSet::getResultSet(int nIndex) const
    {
      UnsupportedOperationException e;
      DBGW_LOG_ERROR(e.what());
      throw e;
    }

    trait<ResultSetMetaData>::sp NBaseTResultSet::getMetaData() const
    {
      return m_pImpl->getMetaData();
    }

    _ValueSet &NBaseTResultSet::getInternalValuSet()
    {
      return m_pImpl->getInternalValuSet();
    }

    void NBaseTResultSet::doClose()
    {
      m_pImpl->close();
    }

  }

}
