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

package org.apache.ranger.audit.metrics;

import org.apache.commons.lang3.StringUtils;
import org.apache.ranger.db.RangerDaoManager;
import org.apache.ranger.entity.XXService;
import org.apache.ranger.entity.XXServiceDef;
import org.apache.ranger.plugin.model.RangerAuditMetrics;
import org.apache.ranger.plugin.model.RangerAuditMetricsByDays;
import org.apache.ranger.plugin.model.RangerAuditMetricsByHours;
import org.apache.ranger.plugin.util.SearchFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.time.DateTimeException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeParseException;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class AuditMetricsHelper {
    private static final Logger LOGGER = LoggerFactory.getLogger(AuditMetricsHelper.class);

    public static final long MISSING_REPO_TYPE_SENTINEL = -1L;

    private final Map<String, Long> repoTypeByServiceType = new ConcurrentHashMap<>();

    @Autowired
    RangerDaoManager daoManager;

    public SearchFilter buildSearchFilter(String serviceType, String serviceName, String appId, String clusterName, String clientIP) {
        SearchFilter filter = new SearchFilter();

        filter.setParam(SearchFilter.SERVICE_TYPE, serviceType);
        filter.setParam(SearchFilter.SERVICE_NAME, serviceName);
        filter.setParam(SearchFilter.PLUGIN_APP_ID, appId);
        filter.setParam(SearchFilter.CLUSTER_NAME, clusterName);
        filter.setParam(SearchFilter.CLIENT_IP, clientIP);

        return filter;
    }

    public FilterParams getFilterParams(SearchFilter filter) {
        FilterParams params = new FilterParams();

        if (filter != null) {
            params.serviceType = filter.getParam(SearchFilter.SERVICE_TYPE);
            params.serviceName = filter.getParam(SearchFilter.SERVICE_NAME);
            params.appId = filter.getParam(SearchFilter.PLUGIN_APP_ID);
            params.clusterName = filter.getParam(SearchFilter.CLUSTER_NAME);
            params.clientIP = filter.getParam(SearchFilter.CLIENT_IP);
        }

        return params;
    }

    public RangerAuditMetrics buildAuditMetrics(String serviceType, String serviceName, String appId, String clusterName, String clientIP, long count) {
        RangerAuditMetrics metric = new RangerAuditMetrics();

        metric.setServiceName(serviceName);
        metric.setServiceType(resolveServiceType(serviceType, serviceName));
        metric.setAppId(appId);
        metric.setClusterName(clusterName);
        metric.setClientIP(clientIP);
        metric.setNumberOfAudits(count);

        Long serviceId = resolveServiceId(serviceName);

        if (serviceId != null) {
            metric.setId(serviceId);
        }

        return metric;
    }

    public RangerAuditMetricsByDays buildAuditMetricsByDays(FilterParams params, Long auditDate, long count) {
        return new RangerAuditMetricsByDays(params.serviceType, params.serviceName, params.appId, params.clusterName, params.clientIP, auditDate, count);
    }

    public RangerAuditMetricsByHours buildAuditMetricsByHours(FilterParams params, int hour, long count) {
        return new RangerAuditMetricsByHours(params.serviceType, params.serviceName, params.appId, params.clusterName, params.clientIP, hour, count);
    }

    public Long resolveServiceId(String serviceName) {
        Long ret = null;

        if (StringUtils.isNotBlank(serviceName) && daoManager != null && daoManager.getXXService() != null) {
            XXService service = daoManager.getXXService().findByName(serviceName);

            if (service != null) {
                ret = service.getId();
            }
        }

        return ret;
    }

    public String resolveServiceType(XXService service) {
        String ret = null;

        if (service != null && daoManager != null && daoManager.getXXServiceDef() != null) {
            XXServiceDef serviceDef = daoManager.getXXServiceDef().getById(service.getType());

            if (serviceDef != null) {
                ret = serviceDef.getName();
            }
        }

        return ret;
    }

    public String resolveServiceType(String serviceType, String serviceName) {
        String ret = serviceType;

        if (StringUtils.isBlank(ret)) {
            if (StringUtils.isNotBlank(serviceName) && daoManager != null && daoManager.getXXService() != null) {
                XXService service = daoManager.getXXService().findByName(serviceName);
                ret = resolveServiceType(service);
            } else {
                ret = null;
            }
        }

        return ret;
    }

    public long resolveRepoType(String serviceType) {
        if (StringUtils.isBlank(serviceType)) {
            return MISSING_REPO_TYPE_SENTINEL;
        }

        String cacheKey = serviceType.trim().toLowerCase();
        Long cached = repoTypeByServiceType.get(cacheKey);

        if (cached != null) {
            return cached;
        }

        long resolved = MISSING_REPO_TYPE_SENTINEL;

        if (daoManager != null && daoManager.getXXServiceDef() != null) {
            XXServiceDef serviceDef = daoManager.getXXServiceDef().findByName(serviceType);

            if (serviceDef != null && serviceDef.getId() != null) {
                resolved = serviceDef.getId();
            }
        }

        repoTypeByServiceType.put(cacheKey, resolved);

        return resolved;
    }

    public ZoneId parseZoneId(String timezone) {
        ZoneId ret = null;

        if (StringUtils.isNotBlank(timezone)) {
            try {
                ret = ZoneId.of(timezone.trim());
            } catch (DateTimeException e) {
                LOGGER.warn("Invalid timezone value: {}", timezone, e);
            }
        }

        return ret;
    }

    public ZoneId resolveZoneId(String timezone) {
        ZoneId ret = parseZoneId(timezone);

        if (ret == null) {
            ret = ZoneOffset.UTC;
        }

        return ret;
    }

    public Long parseAuditDate(Object value) {
        Long ret = null;

        if (value instanceof Date) {
            ret = ((Date) value).getTime();
        } else if (value instanceof Number) {
            ret = ((Number) value).longValue();
        } else if (value instanceof String) {
            try {
                ret = Instant.parse((String) value).toEpochMilli();
            } catch (DateTimeParseException e) {
                LOGGER.warn("Unable to parse audit metric date value: {}", value, e);
            }
        }

        return ret;
    }

    public static class FilterParams {
        private String serviceType;
        private String serviceName;
        private String appId;
        private String clusterName;
        private String clientIP;

        public String getServiceType() {
            return serviceType;
        }

        public String getServiceName() {
            return serviceName;
        }

        public String getAppId() {
            return appId;
        }

        public String getClusterName() {
            return clusterName;
        }

        public String getClientIP() {
            return clientIP;
        }
    }
}
