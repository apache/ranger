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
import org.apache.ranger.entity.XXTrxLogV2;
import org.apache.ranger.util.TimezoneAdjustedDateUtil;
import org.apache.ranger.view.RangerAuditAdminMetricsByDays;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import javax.persistence.NoResultException;
import javax.persistence.Query;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Service
public class XXTrxLogV2Dao extends BaseDao<XXTrxLogV2> {
    private static final Logger logger = LoggerFactory.getLogger(XXTrxLogV2Dao.class);

    public XXTrxLogV2Dao(RangerDaoManagerBase daoManager) {
        super(daoManager);
    }

    public List<XXTrxLogV2> findByTransactionId(String transactionId) {
        List<XXTrxLogV2> ret = null;

        if (transactionId != null) {
            try {
                ret = getEntityManager().createNamedQuery("XXTrxLogV2.findByTrxId", XXTrxLogV2.class).setParameter("transactionId", transactionId).getResultList();
            } catch (NoResultException e) {
                logger.debug(e.getMessage());
            }
        }

        return ret;
    }

    public long deleteOlderThan(int olderThanInDays) {
        Date since = new Date(System.currentTimeMillis() - TimeUnit.DAYS.toMillis(olderThanInDays));

        logger.info("Deleting x_trx_log_v2 records that are older than {} days, that is, older than {}", olderThanInDays, since);

        long ret = getEntityManager().createNamedQuery("XXTrxLogV2.deleteOlderThan").setParameter("olderThan", since).executeUpdate();

        logger.info("Deleted {} x_trx_log_v2 records", ret);

        return ret;
    }

    @SuppressWarnings("unchecked")
    public List<RangerAuditAdminMetricsByDays> getRangerAuditAdminMetricsByDays(int days, Set<Integer> objectClassTypes, List<String> actions, ZoneId zoneId) {
        Date since = new Date(System.currentTimeMillis() - TimeUnit.DAYS.toMillis(days));
        List<RangerAuditAdminMetricsByDays> ret = new ArrayList<>();

        String classTypeInClause = objectClassTypes.stream().map(ct -> "?").collect(Collectors.joining(","));
        String actionInClause    = actions.stream().map(a -> "?").collect(Collectors.joining(","));
        String auditDateExpr     = TimezoneAdjustedDateUtil.getTimezoneAdjustedDateExpression("create_time", zoneId);
        String groupByAuditDate    = auditDateExpr.contains("?") ? "auditDate" : auditDateExpr;

        String sql =
                "SELECT class_type, " +
                "SUM(CASE WHEN action = 'create' THEN 1 ELSE 0 END), " +
                "SUM(CASE WHEN action = 'update' THEN 1 ELSE 0 END), " +
                "SUM(CASE WHEN action = 'delete' THEN 1 ELSE 0 END), " +
                auditDateExpr + " AS auditDate " +
                "FROM x_trx_log_v2 " +
                "WHERE create_time >= ? " +
                "AND class_type IN (" + classTypeInClause + ") " +
                "AND action IN (" + actionInClause + ") " +
                "GROUP BY class_type, " + groupByAuditDate + " " +
                "ORDER BY auditDate";

        try {
            Query query = getEntityManager().createNativeQuery(sql);

            int index = 1;
            Object timezoneParameter = TimezoneAdjustedDateUtil.getTimezoneParameter(zoneId);
            if (timezoneParameter != null) {
                query.setParameter(index++, timezoneParameter);
            }
            query.setParameter(index++, since);

            for (Integer ct : objectClassTypes) {
                query.setParameter(index++, ct);
            }

            for (String action : actions) {
                query.setParameter(index++, action);
            }

            List<Object[]> rows = query.getResultList();

            if (rows != null) {
                for (Object[] row : rows) {
                    ret.add(new RangerAuditAdminMetricsByDays(
                            ((Number) row[0]).intValue(),
                            ((Number) row[1]).longValue(),
                            ((Number) row[2]).longValue(),
                            ((Number) row[3]).longValue(),
                            ((Date) row[4]).getTime()));
                }
            }
        } catch (NoResultException e) {
            logger.debug(e.getMessage());
        }

        return ret;
    }
}
