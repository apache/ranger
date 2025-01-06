/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.ranger.db;

import org.apache.ranger.common.db.BaseDao;
import org.apache.ranger.entity.XXAccessAudit;
import org.apache.ranger.entity.XXAccessAuditV4;
import org.apache.ranger.entity.XXAccessAuditV5;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import javax.persistence.NoResultException;

import java.util.ArrayList;
import java.util.List;

@Service
public class XXAccessAuditDao extends BaseDao<XXAccessAudit> {
    private static final Logger logger = LoggerFactory.getLogger(XXAccessAuditDao.class);

    public XXAccessAuditDao(RangerDaoManagerBase daoManager) {
        super(daoManager);
    }

    public Long getMaxIdOfXXAccessAudit() {
        Long maxXXAccessAuditID = null;

        try {
            maxXXAccessAuditID = getEntityManager().createNamedQuery("XXAccessAudit.getMaxIdOfXXAccessAudit", Long.class).getSingleResult();
        } catch (NoResultException e) {
            logger.debug(e.getMessage());
        } finally {
            if (maxXXAccessAuditID == null) {
                maxXXAccessAuditID = 0L;
            }
        }

        return maxXXAccessAuditID;
    }

    @SuppressWarnings("unchecked")
    public List<String> getColumnNames(String dbFlavor) {
        List<String> columnList = null;
        String       sqlStr     = null;

        if ("MYSQL".equalsIgnoreCase(dbFlavor)) {
            sqlStr = "SELECT lower(column_name) FROM information_schema.columns WHERE table_schema=database() AND table_name = 'xa_access_audit'";
        } else if ("ORACLE".equalsIgnoreCase(dbFlavor)) {
            sqlStr = "SELECT lower(column_name) FROM user_tab_cols WHERE table_name = upper('XA_ACCESS_AUDIT')";
        } else if ("POSTGRES".equalsIgnoreCase(dbFlavor)) {
            sqlStr = "SELECT lower(attname) FROM pg_attribute WHERE attrelid IN(SELECT oid FROM pg_class WHERE relname='xa_access_audit')";
        } else if ("MSSQL".equalsIgnoreCase(dbFlavor)) {
            sqlStr = "SELECT lower(column_name) FROM INFORMATION_SCHEMA.columns WHERE table_name = 'xa_access_audit'";
        } else if ("SQLA".equalsIgnoreCase(dbFlavor)) {
            sqlStr = "SELECT lower(cname) FROM SYS.SYSCOLUMNS WHERE tname = 'xa_access_audit'";
        }

        try {
            if (sqlStr != null) {
                columnList = getEntityManager().createNativeQuery(sqlStr).getResultList();
            }
        } catch (NoResultException e) {
            logger.debug(e.getMessage());
        } finally {
            if (columnList == null) {
                columnList = new ArrayList<>();
            }
        }

        return columnList;
    }

    public List<XXAccessAuditV4> getByIdRangeV4(long idFrom, long idTo) {
        List<XXAccessAuditV4> xXAccessAuditList = null;

        try {
            //idFrom and idTo both exclusive
            xXAccessAuditList = getEntityManager().createNamedQuery("XXAccessAuditV4.getByIdRangeV4", XXAccessAuditV4.class)
                    .setParameter("idFrom", idFrom).setParameter("idTo", idTo).getResultList();
        } catch (NoResultException e) {
            logger.debug(e.getMessage());
        } finally {
            if (xXAccessAuditList == null) {
                xXAccessAuditList = new ArrayList<>();
            }
        }

        return xXAccessAuditList;
    }

    public List<XXAccessAuditV5> getByIdRangeV5(long idFrom, long idTo) {
        List<XXAccessAuditV5> xXAccessAuditList = null;

        try {
            //idFrom and idTo both exclusive
            xXAccessAuditList = getEntityManager().createNamedQuery("XXAccessAuditV5.getByIdRangeV5", XXAccessAuditV5.class)
                    .setParameter("idFrom", idFrom).setParameter("idTo", idTo).getResultList();
        } catch (NoResultException e) {
            logger.debug(e.getMessage());
        } finally {
            if (xXAccessAuditList == null) {
                xXAccessAuditList = new ArrayList<>();
            }
        }

        return xXAccessAuditList;
    }

    public List<XXAccessAudit> getByIdRangeV6(long idFrom, long idTo) {
        List<XXAccessAudit> xXAccessAuditList = null;

        try {
            //idFrom and idTo both exclusive
            xXAccessAuditList = getEntityManager().createNamedQuery("XXAccessAudit.getByIdRangeV6", XXAccessAudit.class)
                    .setParameter("idFrom", idFrom).setParameter("idTo", idTo).getResultList();
        } catch (NoResultException e) {
            logger.debug(e.getMessage());
        } finally {
            if (xXAccessAuditList == null) {
                xXAccessAuditList = new ArrayList<>();
            }
        }

        return xXAccessAuditList;
    }
}
