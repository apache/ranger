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

import org.apache.commons.collections.ListUtils;
import org.apache.ranger.common.db.BaseDao;
import org.apache.ranger.entity.XXAuditMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import javax.persistence.NoResultException;

import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;

@Service
public class XXAuditMetricsDao extends BaseDao<XXAuditMetrics> {
    private static final Logger logger = LoggerFactory.getLogger(XXAuditMetricsDao.class);

    public XXAuditMetricsDao(RangerDaoManagerBase daoManager) {
        super(daoManager);
    }

    public XXAuditMetrics findLatestAuditMetricsByServiceTypeAndName(Long serviceType, String serviceName) {
        XXAuditMetrics ret = null;
        if (serviceType != null) {
            try {
                ret = getEntityManager()
                        .createNamedQuery("XXAuditMetrics.findLatestAuditMetricsByServiceTypeAndName", XXAuditMetrics.class)
                        .setParameter("serviceType", serviceType)
                        .setParameter("serviceName", serviceName)
                        .getSingleResult();
            } catch (NoResultException e) {
                logger.debug(e.getMessage());
            }
        } else {
            logger.debug("Query XXAuditMetricsDao.findLatestAuditMetricsByServiceTypeAndName() failed...ServiceType or ServiceName not provided.");
        }

        return ret;
    }

    public XXAuditMetrics findById(Long id) {
        XXAuditMetrics ret = null;
        if (id != null) {
            try {
                ret = getEntityManager()
                        .createNamedQuery("XXAuditMetrics.findById", XXAuditMetrics.class)
                        .setParameter("id", id)
                        .getSingleResult();
            } catch (NoResultException e) {
                logger.debug(e.getMessage());
            }
        } else {
            logger.debug("Query XXAuditMetricsDao.findById() failed...ServiceType not provided.");
        }

        return ret;
    }

    public List<XXAuditMetrics> findAllLatestAuditMetrics() {
        List<XXAuditMetrics> ret;
        try {
            ret = getEntityManager()
                    .createNamedQuery("XXAuditMetrics.findAllLatestAuditMetrics", tClass)
                    .getResultList();
        } catch (NoResultException e) {
            ret = ListUtils.EMPTY_LIST;
        }
        return ret;
    }

    @SuppressWarnings("unchecked")
    public List<Object[]> getRangerAuditMetricsByHours() {
        List<Object[]> ret;
        try {
            String sqlStr;
            sqlStr = "select service_type, service_name, app_id, cluster_name, client_ip, hours, numberOfAudits from vx_audit_metrics_by_hours ";
            ret    = getEntityManager().createNativeQuery(sqlStr).getResultList();
        } catch (NoResultException e) {
            ret = ListUtils.EMPTY_LIST;
        }
        return ret;
    }

    @SuppressWarnings("unchecked")
    public List<Object[]> getRangerAuditMetricsByDays(Integer olderThanInDays) {
        List<Object[]> ret;
        Date           since = new Date(System.currentTimeMillis() - TimeUnit.DAYS.toMillis(olderThanInDays));

        try {
            StringBuilder sqlStr = new StringBuilder();
            sqlStr.append("select service_type, service_name, app_id, cluster_name, client_ip, days, numberOfAudits, auditDate from vx_audit_metrics_by_days ");
            if (olderThanInDays != null) {
                sqlStr.append(" where auditDate >= ?1");
            }
            ret = getEntityManager().createNativeQuery(sqlStr.toString()).setParameter(1, since).getResultList();
        } catch (NoResultException e) {
            ret = ListUtils.EMPTY_LIST;
        }
        return ret;
    }

    public void deleteRangerAuditMetrics(Integer olderThanInDays, String serviceName, Long serviceType) {
        Date since = new Date(System.currentTimeMillis() - TimeUnit.DAYS.toMillis(olderThanInDays));

        logger.debug("Deleting records from x_audit_metrics that are older than {} days, that is,  older than {}", olderThanInDays, since);

        getEntityManager().createNamedQuery("XXAuditMetrics.deleteOlderThan")
                .setParameter("olderThanInDays", since)
                .setParameter("serviceName", serviceName)
                .setParameter("serviceType", serviceType)
                .executeUpdate();
    }

    public long getCountOfAuditMetrics(Integer olderThanInDays, String serviceName, Long serviceType) {
        Date since = new Date(System.currentTimeMillis() - TimeUnit.DAYS.toMillis(olderThanInDays));

        logger.debug("No of records from x_audit_metrics that are older than {} days, that is,  older than {}", olderThanInDays, since);

        return getEntityManager().createNamedQuery("XXAuditMetrics.getCountOfAuditMetrics", Long.class)
                .setParameter("olderThanInDays", since)
                .setParameter("serviceName", serviceName)
                .setParameter("serviceType", serviceType)
                .getSingleResult();
    }
}
