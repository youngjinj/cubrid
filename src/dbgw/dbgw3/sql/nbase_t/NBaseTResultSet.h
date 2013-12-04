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

#ifndef NBASETRESULTSET_H_
#define NBASETRESULTSET_H_

namespace dbgw
{

  namespace sql
  {

    class NBaseTResultSet : public ResultSet
    {
    public:
      NBaseTResultSet(trait<Statement>::sp pStatement,
          const char *szResultStream);
      virtual ~NBaseTResultSet();

      virtual bool isFirst();
      virtual bool first();
      virtual bool next();

    public:
      virtual int getRowCount() const;
      virtual ValueType getType(int nIndex) const;
      virtual int getInt(int nIndex) const;
      virtual const char *getCString(int nIndex) const;
      virtual int64 getLong(int nIndex) const;
      virtual char getChar(int nIndex) const;
      virtual float getFloat(int nIndex) const;
      virtual double getDouble(int nIndex) const;
      virtual bool getBool(int nIndex) const;
      virtual struct tm getDateTime(int nIndex) const;
      virtual void getBytes(int nIndex, size_t *pSize,
          const char **pValue) const;
      virtual const Value *getValue(int nIndex) const;
      virtual trait<Lob>::sp getClob(int nIndex) const;
      virtual trait<Lob>::sp getBlob(int nIndex) const;
      virtual trait<ResultSet>::sp getResultSet(int nIndex) const;
      virtual trait<ResultSetMetaData>::sp getMetaData() const;
      virtual _ValueSet &getInternalValuSet();

    protected:
      virtual void doClose();

    private:
      class Impl;
      Impl *m_pImpl;
    };

  }

}

#endif
