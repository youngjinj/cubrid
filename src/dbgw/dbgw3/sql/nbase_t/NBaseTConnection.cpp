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

#include <boost/lexical_cast.hpp>
#include <boost/algorithm/string.hpp>
#include "dbgw3/sql/nbase_t/NBaseTCommon.h"
#include "dbgw3/sql/nbase_t/NBaseTException.h"
#include "dbgw3/sql/nbase_t/NBaseTConnection.h"
#include "dbgw3/sql/nbase_t/NBaseTExecutor.h"
#include "dbgw3/sql/nbase_t/NBaseTPreparedStatement.h"

namespace dbgw
{

  namespace sql
  {

    class NBaseTConnection::Impl
    {
    public:
      Impl(NBaseTConnection *pSelf, const char *szUrl) :
        m_pExecutor(NULL), m_pSelf(pSelf), m_bTransactionStared(false)
      {
        std::string url = szUrl;

        /**
         * NBASE URL
         * nbase://<hostname>:<port>
         */

        if (url.substr(0, 8) != "nbase://")
          {
            NBaseTException e =
                NBaseTExceptionFactory::create("invalid nbase cdbc url");
            std::string replace(e.what());
            replace += "(";
            replace += url;
            replace += ")";
            DBGW_LOG_ERROR(replace.c_str());
            throw e;
          }

        std::string tmpUrl = url.substr(8);
        std::vector<std::string> tokenList;
        boost::algorithm::split(tokenList, tmpUrl, boost::is_any_of("/"));
        if (tokenList.size() != 2)
          {
            NBaseTException e =
                NBaseTExceptionFactory::create("invalid nbase cdbc url");
            std::string replace(e.what());
            replace += "(";
            replace += url;
            replace += ")";
            DBGW_LOG_ERROR(replace.c_str());
            throw e;
          }

        tmpUrl = tokenList[0];
        std::string keyspace = tokenList[1];

        tokenList.clear();
        boost::algorithm::split(tokenList, tmpUrl, boost::is_any_of(":"));
        if (tokenList.size() != 2)
          {
            NBaseTException e =
                NBaseTExceptionFactory::create("invalid nbase cdbc url");
            std::string replace(e.what());
            replace += "(";
            replace += url;
            replace += ")";
            DBGW_LOG_ERROR(replace.c_str());
            throw e;
          }

        std::string mgmtHost = tokenList[0];
        int mgmtPort = boost::lexical_cast<int>(tokenList[1]);

        m_pExecutor = new NBaseTExecutor(mgmtHost, mgmtPort, keyspace);
      }

      ~Impl()
      {
        if (m_pExecutor)
          {
            delete m_pExecutor;
          }
      }

      trait<CallableStatement>::sp prepareCall(const char *szSql)
      {
        UnsupportedOperationException e;
        DBGW_LOG_ERROR(e.what());
        throw e;
      }

      trait<PreparedStatement>::sp prepareStatement(const char *szSql)
      {
        trait<PreparedStatement>::sp pStmt(new NBaseTPreparedStatement(
            m_pSelf->shared_from_this(), szSql));
        return pStmt;
      }

      void setContainerKey(const char *szKey)
      {
        if (m_bTransactionStared)
          {
            NBaseTException e = NBaseTExceptionFactory::create(
                "cannot change container key while transaction is started.");
            DBGW_LOG_ERROR(e.what());
            throw e;
          }

        m_pExecutor->setContainerKey(szKey);
      }

      void beginTransaction()
      {
        if (m_pSelf->getAutoCommit() || m_bTransactionStared)
          {
            return;
          }

        m_pExecutor->beginTransaction();

        m_bTransactionStared = true;
      }

      void cancel()
      {
      }

      trait<Lob>::sp createClob()
      {
        UnsupportedOperationException e;
        DBGW_LOG_ERROR(e.what());
        throw e;
      }

      trait<Lob>::sp createBlob()
      {
        UnsupportedOperationException e;
        DBGW_LOG_ERROR(e.what());
        throw e;
      }

      void *getNativeHandle()
      {
        return m_pExecutor;
      }

      void doConnect()
      {
      }

      void doClose()
      {
        if (m_pSelf->getAutoCommit() == false)
          {
            m_pSelf->rollback();
          }
      }

      void doSetTransactionIsolation(TransactionIsolation isolation)
      {
      }

      void doSetAutoCommit(bool bAutoCommit)
      {
      }

      void doCommit()
      {
        if (m_bTransactionStared)
          {
            m_bTransactionStared = false;

            m_pExecutor->commit();
          }
      }

      void doRollback()
      {
        if (m_bTransactionStared)
          {
            m_bTransactionStared = false;

            m_pExecutor->rollback();
          }
      }

    private:
      NBaseTExecutor *m_pExecutor;
      NBaseTConnection *m_pSelf;
      bool m_bTransactionStared;
    };

    NBaseTConnection::NBaseTConnection(const char *szUrl) :
      Connection(), m_pImpl(new Impl(this, szUrl))
    {
      connect();
    }

    NBaseTConnection::~NBaseTConnection()
    {
      try
        {
          close();
        }
      catch (...)
        {
        }

      if (m_pImpl != NULL)
        {
          delete m_pImpl;
        }
    }

    trait<CallableStatement>::sp NBaseTConnection::prepareCall(
        const char *szSql)
    {
      return m_pImpl->prepareCall(szSql);
    }

    trait<PreparedStatement>::sp NBaseTConnection::prepareStatement(
        const char *szSql)
    {
      return m_pImpl->prepareStatement(szSql);
    }

    void NBaseTConnection::setContainerKey(const char *szKey)
    {
      m_pImpl->setContainerKey(szKey);
    }

    void NBaseTConnection::beginTransaction()
    {
      m_pImpl->beginTransaction();
    }

    void NBaseTConnection::cancel()
    {
      m_pImpl->cancel();
    }

    trait<Lob>::sp NBaseTConnection::createClob()
    {
      return m_pImpl->createClob();
    }

    trait<Lob>::sp NBaseTConnection::createBlob()
    {
      return m_pImpl->createBlob();
    }

    void *NBaseTConnection::getNativeHandle() const
    {
      return m_pImpl->getNativeHandle();
    }

    void NBaseTConnection::doConnect()
    {
      m_pImpl->doConnect();
    }

    void NBaseTConnection::doClose()
    {
      m_pImpl->doClose();
    }

    void NBaseTConnection::doSetTransactionIsolation(
        TransactionIsolation isolation)
    {
      m_pImpl->doSetTransactionIsolation(isolation);
    }

    void NBaseTConnection::doSetAutoCommit(bool bAutoCommit)
    {
      m_pImpl->doSetAutoCommit(bAutoCommit);
    }

    void NBaseTConnection::doCommit()
    {
      m_pImpl->doCommit();
    }

    void NBaseTConnection::doRollback()
    {
      m_pImpl->doRollback();
    }

  }

}
