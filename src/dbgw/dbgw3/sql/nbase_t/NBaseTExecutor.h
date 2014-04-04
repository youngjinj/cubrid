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

#ifndef NBASETEXECUTOR_H_
#define NBASETEXECUTOR_H_

namespace dbgw
{

  namespace sql
  {

    class _NBaseTGlobal
    {
    public:
      virtual ~_NBaseTGlobal();

      void registerCsList(const char *szHost, int nPort);
      static trait<_NBaseTGlobal>::sp getInstance();

    private:
      _NBaseTGlobal();
    };

    class NBaseTExecutor
    {
    public:
      NBaseTExecutor(const std::string &mgmtHost, int mgmtPort,
          const std::string &keyspace);
      virtual ~NBaseTExecutor();

      void setContainerKey(const char *szKey);
      void beginTransaction();
      void commit();
      void rollback();
      int executeUpdate(const char *szSql);
      int executeQuery(const char *szSql);

    public:
      std::string getResultStream() const;

    private:
      class Impl;
      Impl *m_pImpl;
    };

  }

}

#endif
