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

#include <sstream>
#include "dbgw3/system/Mutex.h"
#include "dbgw3/system/ConditionVariable.h"
#include "dbgw3/sql/nbase_t/NBaseTCommon.h"
#include "dbgw3/sql/nbase_t/NBaseTException.h"
#include "dbgw3/sql/nbase_t/NBaseTExecutor.h"

namespace dbgw
{

  namespace sql
  {

    typedef boost::unordered_map<std::string, trait<nbase_mgmt>::sp,
            boost::hash<std::string>, func::compareString> _NBaseTMgmtMap;

    typedef enum
    {
      NBASE_SQL_TYPE_SELECT,
      NBASE_SQL_TYPE_OTHERS
    } NBASE_SQL_TYPE;

    class _NBaseTGlobal::Impl
    {
    public:
      Impl()
      {
        nbase_param_t param;

        nbase_get_param(&param);
        nbase_init(&param);
      }

      ~Impl()
      {
        system::_MutexAutoLock lock(&m_mutex);

        for (_NBaseTMgmtMap::iterator it = m_mgmtMap.begin();
            it != m_mgmtMap.end(); it++)
          {
            int nResult = nbase_mgmt_unregister_cs_list(it->second.get());
            if (NE_ERROR(nResult))
              {
                NBaseTException e = NBaseTExceptionFactory::create(
                    nResult, "fail to unregister cs list");
                DBGW_LOG_ERROR(e.what());
              }
          }

        m_mgmtMap.clear();
        nbase_finalize();
      }

      nbase_mgmt *getMgmtHandle(const char *szHost, int nPort)
      {
        system::_MutexAutoLock lock(&m_mutex);

        std::string key = szHost;
        key += ":";
        key += nPort;

        _NBaseTMgmtMap::iterator it = m_mgmtMap.find(key);
        if (it == m_mgmtMap.end())
          {
            trait<nbase_mgmt>::sp pMgmt(new nbase_mgmt());

            int nResult = nbase_mgmt_register_cs_list(szHost, nPort,
                pMgmt.get());
            if (NE_ERROR(nResult) && nResult != nbase_t::NE_EXIST)
              {
                NBaseTException e = NBaseTExceptionFactory::create(
                    nResult, "fail to register cs list");
                DBGW_LOG_ERROR(e.what());
                throw e;
              }

            m_mgmtMap[key] = pMgmt;
            return pMgmt.get();
          }
        else
          {
            return it->second.get();
          }
      }

    private:
      system::_Mutex m_mutex;
      _NBaseTMgmtMap m_mgmtMap;
    };

    _NBaseTGlobal::_NBaseTGlobal() :
      m_pImpl(new Impl())
    {
    }

    _NBaseTGlobal::~_NBaseTGlobal()
    {
      if (m_pImpl)
        {
          delete m_pImpl;
        }
    }

    trait<_NBaseTGlobal>::sp _NBaseTGlobal::getInstance()
    {
      static trait<_NBaseTGlobal>::sp pInstance;
      if (pInstance == NULL)
        {
          pInstance = trait<_NBaseTGlobal>::sp(new _NBaseTGlobal());
        }

      return pInstance;
    }

    nbase_mgmt *_NBaseTGlobal::getMgmtHandle(const char *szHost, int nPort)
    {
      return m_pImpl->getMgmtHandle(szHost, nPort);
    }

    class NBaseTExecutor::Impl
    {
    public:
      Impl(const std::string &mgmtHost, int mgmtPort,
          const std::string &keyspace) :
        m_keyspace(keyspace), m_csPort(0), m_pMgmt(NULL),
        m_nTimeoutMilSec(10000), m_bAutocommit(true),
        m_type(NBASE_SQL_TYPE_OTHERS)
      {
        m_pGlobal = _NBaseTGlobal::getInstance();

        m_pMgmt = m_pGlobal->getMgmtHandle(mgmtHost.c_str(), mgmtPort);
      }

      virtual ~Impl()
      {
      }

      void setContainerKey(const char *szKey)
      {
        m_ckey = szKey;
      }

      void beginTransaction()
      {
        if (m_ckey == "")
          {
            NBaseTException e = NBaseTExceptionFactory::create(
                "there is no container key");
            DBGW_LOG_ERROR(e.what());
            throw e;
          }

        m_exIpList.clear();

        doBeginTransaction();

        m_bAutocommit = false;
      }

      void commit()
      {
        int nResult = nbase_end_tx(&m_tx, true);
        if (NE_ERROR(nResult))
          {
            NBaseTException e = NBaseTExceptionFactory::create(nResult,
                "fail to rollback transaction");
            DBGW_LOG_ERROR(e.what());
            throw e;
          }

        m_bAutocommit = true;

        DBGW_LOGF_DEBUG("commit transaction (key:%s) (%s:%d)",
            m_ckey.c_str(), m_csAddr.c_str(), m_csPort);

        clearContainerKey();
      }

      void rollback()
      {
        int nResult = nbase_end_tx(&m_tx, false);
        if (NE_ERROR(nResult))
          {
            NBaseTException e = NBaseTExceptionFactory::create(nResult,
                "fail to rollback transaction");
            DBGW_LOG_ERROR(e.what());
            throw e;
          }

        m_bAutocommit = true;

        DBGW_LOGF_DEBUG("rollback transaction (key:%s) (%s:%d)",
            m_ckey.c_str(), m_csAddr.c_str(), m_csPort);

        clearContainerKey();
      }

      int executeUpdate(const char *szSql)
      {
        m_exIpList.clear();

        freeResult();

        if (m_ckey == "")
          {
            NBaseTException e = NBaseTExceptionFactory::create(
                "there is no container key");
            DBGW_LOG_ERROR(e.what());
            throw e;
          }

        return doExecuteUpdate(szSql);
      }

      int executeQuery(const char *szSql)
      {
        m_exIpList.clear();

        freeResult();

        if (m_ckey == "")
          {
            NBaseTException e = NBaseTExceptionFactory::create(
                "there is no container key");
            DBGW_LOG_ERROR(e.what());
            throw e;
          }

        return doExecuteQuery(szSql);
      }

      std::string getResultStream() const
      {
        if (m_type == NBASE_SQL_TYPE_SELECT)
          {
            return m_resultStream.str();
          }
        else
          {
            return "";
          }
      }

    private:
      void freeResult()
      {
        m_type = NBASE_SQL_TYPE_OTHERS;
        m_resultStream.str("");
      }

      void doBeginTransaction()
      {
        int nResult = NE_SUCCESS;

        do
          {
            getRandomContainerServer();

            nResult = nbase_begin_tx(m_csAddr.c_str(), m_csPort, m_ckey.c_str(),
                m_nTimeoutMilSec, &m_tx);

            if (nResult == nbase_t::NE_RPC || nResult == nbase_t::NE_CONN
                || nResult == nbase_t::NE_UPGRADE)
              {
                m_exIpList += m_csAddr;
                m_exIpList += ";";

                continue;
              }

            break;
          }
        while (true);

        if (NE_ERROR(nResult))
          {
            NBaseTException e = NBaseTExceptionFactory::create(nResult,
                "fail to begin transaction");
            DBGW_LOG_ERROR(e.what());
            throw e;
          }

        DBGW_LOGF_DEBUG("begin transaction (key:%s) (%s:%d)",
            m_ckey.c_str(), m_csAddr.c_str(), m_csPort);
      }

      int doExecuteUpdate(const char *szSql)
      {
        int nResult = NE_SUCCESS;
        nquery_res result;

        do
          {
            if (m_bAutocommit)
              {
                getRandomContainerServer();

                nResult = nbase_query_opts(m_csAddr.c_str(), m_csPort,
                    m_keyspace.c_str(), m_ckey.c_str(), szSql, m_nTimeoutMilSec,
                    NBASE_RESULT_FORMAT_JSON, &result);

                if (nResult == nbase_t::NE_RPC || nResult == nbase_t::NE_CONN
                    || nResult == nbase_t::NE_UPGRADE)
                  {
                    m_exIpList += m_csAddr;
                    m_exIpList += ";";

                    continue;
                  }
              }
            else
              {
                nResult = nbase_query_opts_with_tx(&m_tx, m_keyspace.c_str(),
                    m_ckey.c_str(), szSql, m_nTimeoutMilSec,
                    NBASE_RESULT_FORMAT_JSON, &result);
              }

            break;
          }
        while (true);

        DBGW_LOGF_DEBUG("execute query (sql:\"%s\",key:%s) (%s:%d)",
            szSql, m_ckey.c_str(), m_csAddr.c_str(), m_csPort);

        if (m_bAutocommit)
          {
            clearContainerKey();
          }

        if (NE_ERROR(nResult))
          {
            NBaseTException e = NBaseTExceptionFactory::create(nResult,
                "fail to execute query");
            DBGW_LOG_ERROR(e.what());
            throw e;
          }
        else
          {
            nbase_free_result(result);

            m_type = NBASE_SQL_TYPE_OTHERS;

            return nResult;
          }
      }

      int doExecuteQuery(const char *szSql)
      {
        int nResult = NE_SUCCESS;

        do
          {
            if (m_bAutocommit)
              {
                getRandomContainerServer();

                nResult = nbase_query_callback_opts(m_csAddr.c_str(), m_csPort,
                    m_keyspace.c_str(), m_ckey.c_str(), szSql, m_nTimeoutMilSec,
                    NBASE_RESULT_FORMAT_JSON, queryCallback, this);
                if (nResult == nbase_t::NE_RPC || nResult == nbase_t::NE_CONN
                    || nResult == nbase_t::NE_UPGRADE)
                  {
                    m_exIpList += m_csAddr;
                    m_exIpList += ";";

                    continue;
                  }
              }
            else
              {
                nResult = nbase_query_callback_opts_with_tx(&m_tx,
                    m_keyspace.c_str(), m_ckey.c_str(), szSql, m_nTimeoutMilSec,
                    NBASE_RESULT_FORMAT_JSON, queryCallback, this);
              }

            break;
          }
        while (true);

        DBGW_LOGF_DEBUG("execute query (sql:\"%s\",key:%s) (%s:%d)",
            szSql, m_ckey.c_str(), m_csAddr.c_str(), m_csPort);

        if (m_bAutocommit)
          {
            clearContainerKey();
          }

        if (NE_ERROR(nResult))
          {
            NBaseTException e = NBaseTExceptionFactory::create(nResult,
                "fail to execute query");
            DBGW_LOG_ERROR(e.what());
            throw e;
          }
        else
          {
            m_type = NBASE_SQL_TYPE_SELECT;

            return nResult;
          }
      }

      static void queryCallback(nquery_res result, void *arg)
      {
        Impl *pImpl = (Impl *) arg;

        pImpl->appendResultStream(result);
      }

    private:
      void clearContainerKey()
      {
        m_ckey = "";
      }

      void getRandomContainerServer()
      {
        nbase_ipstr csAddr;

        int nResult = nbase_mgmt_get_rand_cs(m_pMgmt, m_ckey.c_str(), csAddr,
            &m_csPort, m_exIpList.c_str());
        if (NE_ERROR(nResult))
          {
            NBaseTException e = NBaseTExceptionFactory::create(nResult,
                "fail to get container server");
            DBGW_LOG_ERROR(e.what());
            throw e;
          }

        m_csAddr = csAddr;

        DBGW_LOGF_DEBUG("connect container server (%s:%d)",
            m_csAddr.c_str(), m_csPort);
      }

      void appendResultStream(nquery_res result)
      {
        int nResult = nbase_get_result_code(result);
        if (nResult == 0)
          {
            return;
          }

        const char *szStream = nbase_get_result_str(result);
        if (szStream == NULL)
          {
            return;
          }

        m_resultStream << szStream;
      }

    private:
      trait<_NBaseTGlobal>::sp m_pGlobal;
      std::string m_ckey;
      std::string m_csAddr;
      std::string m_keyspace;
      std::string m_exIpList;
      unsigned short m_csPort;
      nbase_mgmt *m_pMgmt;
      nbase_tx m_tx;
      int m_nTimeoutMilSec;
      bool m_bAutocommit;
      NBASE_SQL_TYPE m_type;
      std::stringstream m_resultStream;
    };

    NBaseTExecutor::NBaseTExecutor(const std::string &mgmtHost, int mgmtPort,
        const std::string &keyspace) :
      m_pImpl(new Impl(mgmtHost, mgmtPort, keyspace))
    {
    }

    NBaseTExecutor::~NBaseTExecutor()
    {
      if (m_pImpl)
        {
          delete m_pImpl;
        }
    }

    void NBaseTExecutor::setContainerKey(const char *szKey)
    {
      m_pImpl->setContainerKey(szKey);
    }

    void NBaseTExecutor::beginTransaction()
    {
      m_pImpl->beginTransaction();
    }

    void NBaseTExecutor::commit()
    {
      m_pImpl->commit();
    }

    void NBaseTExecutor::rollback()
    {
      m_pImpl->rollback();
    }

    int NBaseTExecutor::executeUpdate(const char *szSql)
    {
      return m_pImpl->executeUpdate(szSql);
    }

    int NBaseTExecutor::executeQuery(const char *szSql)
    {
      return m_pImpl->executeQuery(szSql);
    }

    std::string NBaseTExecutor::getResultStream() const
    {
      return m_pImpl->getResultStream();
    }

  }

}
