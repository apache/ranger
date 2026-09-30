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
import org.apache.ranger.entity.XXPolicyExportAudit;
import org.apache.ranger.util.TimezoneAdjustedDateUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import javax.persistence.NoResultException;
import javax.persistence.Query;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@Service
public class XXPolicyExportAuditDao extends BaseDao<XXPolicyExportAudit> {
    private static final Logger logger = LoggerFactory.getLogger(XXPolicyExportAuditDao.class);

    public XXPolicyExportAuditDao(RangerDaoManagerBase daoManager) {
        super(daoManager);
    }

    public long deleteOlderThan(int olderThanInDays) {
        Date since = new Date(System.currentTimeMillis() - TimeUnit.DAYS.toMillis(olderThanInDays));

        logger.info("Deleting x_policy_export_audit records that are older than {} days, that is, older than {}", olderThanInDays, since);

        long ret = getEntityManager().createNamedQuery("XXPolicyExportAudit.deleteOlderThan").setParameter("olderThan", since).executeUpdate();

        logger.info("Deleted x_policy_export_audit {} records", ret);

        return ret;
    }

    public List<Map<String, Object>> getRangerPluginPolicySyncMetricsByDays(int days, ZoneId zoneId) {
        Date since = new Date(System.currentTimeMillis() - TimeUnit.DAYS.toMillis(days));
        String auditDateExpr    = TimezoneAdjustedDateUtil.getTimezoneAdjustedDateExpression("create_time", zoneId);
        String groupByAuditDate = auditDateExpr.contains("?") ? "auditDate" : auditDateExpr;

        List<Map<String, Object>> ret = new ArrayList<>();

        try {
            String sql =
                    "SELECT auditDate, COUNT(repository_name) AS audit_count FROM (" +
                    "SELECT repository_name, " + auditDateExpr + " AS auditDate " +
                    "FROM x_policy_export_audit " +
                    "WHERE create_time >= ? " +
                    "GROUP BY repository_name, agent_id, client_ip, http_ret_code, " + groupByAuditDate +
                    ") t " +
                    "GROUP BY auditDate " +
                    "ORDER BY auditDate";

            Query query = getEntityManager().createNativeQuery(sql);

            int index = 1;
            Object timezoneParameter = TimezoneAdjustedDateUtil.getTimezoneParameter(zoneId);
            if (timezoneParameter != null) {
                query.setParameter(index++, timezoneParameter);
            }
            query.setParameter(index++, since);

            List<Object[]> rows = query.getResultList();

            if (rows != null) {
                for (Object[] row : rows) {
                    Date auditDate = (Date) row[0];
                    Long numberOfAudits = ((Number) row[1]).longValue();

                    Map<String, Object> metric = new HashMap<>();
                    metric.put("auditDate", auditDate.getTime());
                    metric.put("numberOfPolicySyncCount", numberOfAudits);

                    ret.add(metric);
                }
            }
        } catch (NoResultException e) {
            logger.debug(e.getMessage());
        }

        return ret;
    }
}
