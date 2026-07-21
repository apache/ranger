/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ranger.biz;

import org.apache.commons.collections.CollectionUtils;
import org.apache.ranger.authorization.hadoop.config.RangerAdminConfig;
import org.apache.ranger.common.MessageEnums;
import org.apache.ranger.common.RESTErrorUtil;
import org.apache.ranger.db.RangerDaoManager;
import org.apache.ranger.entity.XXAuditMetrics;
import org.apache.ranger.entity.XXServiceDef;
import org.apache.ranger.plugin.model.RangerAuditMetrics;
import org.apache.ranger.plugin.model.RangerAuditMetricsByDays;
import org.apache.ranger.plugin.model.RangerAuditMetricsByHours;
import org.apache.ranger.plugin.store.AbstractPredicateUtil;
import org.apache.ranger.plugin.store.AuditMetricsPredicateUtil;
import org.apache.ranger.plugin.store.AuditMetricsStore;
import org.apache.ranger.plugin.util.SearchFilter;
import org.apache.ranger.service.RangerAuditMetricsService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

@Component
public class AuditMetricsDBStore implements AuditMetricsStore {
    private static final Logger  LOG                                                 = LoggerFactory.getLogger(AuditMetricsDBStore.class);

    private static final String  PROP_AUDIT_METRICS_RETENTION_PERIOD_IN_DAYS         = "ranger.audit.metrics.retention.period";
    private static final Integer PROP_AUDIT_METRICS_RETENTION_PERIOD_IN_DAYS_DEFAULT = 7;
    private static final String  PROP_AUDIT_METRICS_MAX_SUPPORTED_DAYS               = "ranger.audit.metrics.max.supported.days";
    private static final Integer PROP_AUDIT_METRICS_MAX_SUPPORTED_DAYS_DEFAULT       = 90;

    @Autowired
    RangerAuditMetricsService rangerAuditMetricsService;

    @Autowired
    RangerDaoManager daoMgr;

    @Autowired
    RESTErrorUtil restErrorUtil;

    RangerAdminConfig     config;
    AbstractPredicateUtil predicateUtil;
    private final Boolean populateExistingBaseFields = true;

    public void init() throws Exception {}

    @Override
    public RangerAuditMetrics createRangerAuditMetrics(RangerAuditMetrics rangerAuditMetrics) throws RuntimeException {
        LOG.debug("==> AuditMetricsDBStore.createRangerAuditMetrics()");
        //Delete older Ranger Audit metrics if any present.
        String serviceName = rangerAuditMetrics.getServiceName();
        String serviceType = rangerAuditMetrics.getServiceType();

        Integer retentionPeriod = config.getInt(PROP_AUDIT_METRICS_RETENTION_PERIOD_IN_DAYS, PROP_AUDIT_METRICS_RETENTION_PERIOD_IN_DAYS_DEFAULT);
        if (auditMetricsExistsForDeletion(retentionPeriod, serviceName, serviceType)) {
            deleteRangerAuditMetrics(retentionPeriod, serviceName, serviceType);
        }

        //Create new audit metrics
        RangerAuditMetrics ret = rangerAuditMetricsService.create(rangerAuditMetrics);

        LOG.debug("<== AuditMetricsDBStore.createRangerAuditMetrics() {}", ret);

        return ret;
    }

    @Override
    public RangerAuditMetrics getRangerAuditMetrics(Long id) throws RuntimeException {
        XXAuditMetrics xxAuditMetrics = daoMgr.getXXAuditMetricsDao().findById(id);
        if (xxAuditMetrics == null) {
            throw restErrorUtil.createRESTException("AuditMetrics with Id: " + id + " does not exist");
        }
        return rangerAuditMetricsService.read(xxAuditMetrics.getId());
    }

    @Override
    public RangerAuditMetrics getLatestRangerAuditMetrics(String serviceType, String serviceName) throws RuntimeException {
        XXServiceDef xxServiceDef = daoMgr.getXXServiceDef().findByName(serviceType);
        if (xxServiceDef == null) {
            throw restErrorUtil.createRESTException("No ServiceDefinition found with Service Type :" + serviceType, MessageEnums.INVALID_INPUT_DATA);
        }
        Long           svcType        = xxServiceDef.getId();
        XXAuditMetrics xxAuditMetrics = daoMgr.getXXAuditMetricsDao().findLatestAuditMetricsByServiceTypeAndName(svcType, serviceName);
        if (xxAuditMetrics == null) {
            throw restErrorUtil.createRESTException("Error fetching audit metrics for given service type: " + serviceType + " service name: " + serviceName);
        }
        return rangerAuditMetricsService.read(xxAuditMetrics.getId());
    }

    @Override
    public List<RangerAuditMetrics> getAllLatestRangerAuditMetrics(SearchFilter filter) throws RuntimeException {
        List<RangerAuditMetrics> ret = new ArrayList<>();

        List<XXAuditMetrics> xxAuditMetricsList = daoMgr.getXXAuditMetricsDao().findAllLatestAuditMetrics();
        if (xxAuditMetricsList == null) {
            throw restErrorUtil.createRESTException("Error fetching audit metrics....");
        }

        if (CollectionUtils.isNotEmpty(xxAuditMetricsList)) {
            for (XXAuditMetrics xxAuditMetric : xxAuditMetricsList) {
                ret.add(rangerAuditMetricsService.read(xxAuditMetric.getId()));
            }
            if (predicateUtil != null && filter != null && !filter.isEmpty()) {
                List<RangerAuditMetrics> copy = new ArrayList<>(ret);
                predicateUtil.applyFilter(copy, filter);
                ret = copy;
            }
        }

        return ret;
    }

    @Override
    public List<RangerAuditMetricsByHours> getRangerAuditMetricsByHours(SearchFilter filter) throws RuntimeException {
        List<RangerAuditMetricsByHours> ret           = new ArrayList<>();
        String                          xxSvcTypeName = null;
        List<Object[]>                  objList       = daoMgr.getXXAuditMetricsDao().getRangerAuditMetricsByHours();
        if (objList == null) {
            throw restErrorUtil.createRESTException("Error fetching audit metrics by hours...");
        }

        if (CollectionUtils.isNotEmpty(objList)) {
            for (Object[] obj : objList) {
                Long    svcType    = getLongValueFromObj(obj[0]);
                String  svcName    = (String) obj[1];
                String  appId      = (String) obj[2];
                String  clientName = (String) obj[3];
                String  clientIP   = (String) obj[4];
                Integer hours      = getIntegerValueFromObj(obj[5]);
                Long    nOfA       = getLongValueFromObj(obj[6]);

                if (svcType != null) {
                    XXServiceDef xxServiceDef = daoMgr.getXXServiceDef().getById(svcType);
                    if (xxServiceDef == null) {
                        LOG.error("No Service Definition for serviceType: {}, Skipping the service from metrics", svcType);
                    } else {
                        xxSvcTypeName = xxServiceDef.getName();
                    }
                }
                RangerAuditMetricsByHours rangerAuditMetricsByHours = new RangerAuditMetricsByHours(xxSvcTypeName, svcName, appId, clientName, clientIP, hours, nOfA);
                ret.add(rangerAuditMetricsByHours);
            }
            if (predicateUtil != null && filter != null && !filter.isEmpty()) {
                List<RangerAuditMetricsByHours> copy = new ArrayList<>(ret);
                predicateUtil.applyFilter(copy, filter);
                ret = copy;
            }
        }
        LOG.debug("<== AuditMetricsDBStore.getRangerAuditMetricsByHours() ret:{}", ret);
        return ret;
    }

    @Override
    public List<RangerAuditMetricsByDays> getRangerAuditMetricsByDays(Integer olderThanInDays, SearchFilter filter) throws RuntimeException {
        List<RangerAuditMetricsByDays> ret           = new ArrayList<>();
        String                         xxSvcTypeName = null;
        List<Object[]>                 objList       = daoMgr.getXXAuditMetricsDao().getRangerAuditMetricsByDays(olderThanInDays);
        if (objList == null) {
            throw restErrorUtil.createRESTException("Error fetching audit metrics by days....");
        }

        if (CollectionUtils.isNotEmpty(objList)) {
            for (Object[] obj : objList) {
                Long    svcType    = getLongValueFromObj(obj[0]);
                String  svcName    = (String) obj[1];
                String  appId      = (String) obj[2];
                String  clientName = (String) obj[3];
                String  clientIP   = (String) obj[4];
                Integer days       = getIntegerValueFromObj(obj[5]);
                Long    nOfA       = getLongValueFromObj(obj[6]);
                Date    auditDate  = (Date) obj[7];

                if (svcType != null) {
                    XXServiceDef xxServiceDef = daoMgr.getXXServiceDef().getById(svcType);
                    if (xxServiceDef == null) {
                        LOG.error("No Service Definition for service: {}, Skipping the service from metrics", svcType);
                    } else {
                        xxSvcTypeName = xxServiceDef.getName();
                    }
                }
                RangerAuditMetricsByDays rangerAuditMetricsByDays = new RangerAuditMetricsByDays(xxSvcTypeName, svcName, appId, clientName, clientIP, days, auditDate.getTime(), nOfA);
                ret.add(rangerAuditMetricsByDays);
            }
            if (predicateUtil != null && filter != null && !filter.isEmpty()) {
                List<RangerAuditMetricsByDays> copy = new ArrayList<>(ret);
                predicateUtil.applyFilter(copy, filter);
                ret = copy;
            }
        }
        LOG.debug("<== AuditMetricsDBStore.getRangerAuditMetricsByDays() ret:{}", ret);
        return ret;
    }

    @Override
    public void deleteRangerAuditMetrics(Integer olderThanInDays, String serviceName, String serviceType) throws RuntimeException {
        XXServiceDef xxServiceDef = daoMgr.getXXServiceDef().findByName(serviceType);
        if (xxServiceDef == null) {
            throw restErrorUtil.createRESTException("No ServiceDefinition found with Service Type :" + serviceType, MessageEnums.INVALID_INPUT_DATA);
        }
        Long svcType = xxServiceDef.getId();
        daoMgr.getXXAuditMetricsDao().deleteRangerAuditMetrics(olderThanInDays, serviceName, svcType);
    }

    @PostConstruct
    public void initStore() {
        LOG.debug("==> AuditMetricsDBStore.initStore()");

        config = RangerAdminConfig.getInstance();
        rangerAuditMetricsService.setPopulateExistingBaseFields(populateExistingBaseFields);
        predicateUtil = new AuditMetricsPredicateUtil();

        LOG.debug("<== AuditMetricsDBStore.initStore()");
    }

    private boolean auditMetricsExistsForDeletion(Integer olderThanInDays, String serviceName, String serviceType) {
        if (olderThanInDays == null) {
            throw restErrorUtil.createRESTException("Retention Period cannot be null, hence older audit metrics not deleted...");
        }
        XXServiceDef xxServiceDef = daoMgr.getXXServiceDef().findByName(serviceType);
        if (xxServiceDef == null) {
            throw restErrorUtil.createRESTException("No ServiceDefinition found with Service Type :" + serviceType, MessageEnums.INVALID_INPUT_DATA);
        }
        Long svcType = xxServiceDef.getId();
        return daoMgr.getXXAuditMetricsDao().getCountOfAuditMetrics(olderThanInDays, serviceName, svcType) > 0;
    }

    private Integer getIntegerValueFromObj(Object obj) {
        Integer intVal;
        if (obj instanceof Double) {
            intVal = ((Double) obj).intValue();
        } else if (obj instanceof BigDecimal) {
            intVal = ((BigDecimal) obj).intValue();
        } else {
            intVal = (Integer) obj;
        }
        return intVal;
    }

    private Long getLongValueFromObj(Object obj) {
        Long longVal;
        if (obj instanceof Double) {
            longVal = ((Double) obj).longValue();
        } else if (obj instanceof BigDecimal) {
            longVal = ((BigDecimal) obj).longValue();
        } else {
            longVal = (Long) obj;
        }
        return longVal;
    }

    public void validateRetentionDays(Integer days) {
        Integer maxAllowedDays = Math.max(config.getInt(PROP_AUDIT_METRICS_RETENTION_PERIOD_IN_DAYS, PROP_AUDIT_METRICS_RETENTION_PERIOD_IN_DAYS_DEFAULT),
                config.getInt(PROP_AUDIT_METRICS_MAX_SUPPORTED_DAYS, PROP_AUDIT_METRICS_MAX_SUPPORTED_DAYS_DEFAULT));

        if (days == null || days <= 0 || days > maxAllowedDays) {
            throw restErrorUtil.createRESTException(String.format("Supported days must be between 1 and %d (provided: %s)", maxAllowedDays, days), MessageEnums.INVALID_INPUT_DATA);
        }
    }
}
