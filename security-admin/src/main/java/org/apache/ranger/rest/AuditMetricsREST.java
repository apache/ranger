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

package org.apache.ranger.rest;

import org.apache.commons.collections.CollectionUtils;
import org.apache.ranger.audit.metrics.AccessAuditsMetricsService;
import org.apache.ranger.audit.metrics.AccessAuditsMetricsServiceFactory;
import org.apache.ranger.biz.AuditMetricsDBStore;
import org.apache.ranger.common.RESTErrorUtil;
import org.apache.ranger.common.RangerSearchUtil;
import org.apache.ranger.plugin.model.RangerAuditMetrics;
import org.apache.ranger.plugin.model.RangerAuditMetricsByDays;
import org.apache.ranger.plugin.model.RangerAuditMetricsByHours;
import org.apache.ranger.plugin.util.SearchFilter;
import org.apache.ranger.security.context.RangerAPIList;
import org.apache.ranger.view.RangerAuditAdminMetricsByDays;
import org.apache.ranger.view.RangerAuditMetricsList;
import org.apache.ranger.view.RangerAuditMetricsListByDays;
import org.apache.ranger.view.RangerAuditMetricsListByHours;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Path("audit")
@Component
@Scope("request")
@Transactional(propagation = Propagation.REQUIRES_NEW)
public class AuditMetricsREST {
    private static final Logger LOG = LoggerFactory.getLogger(AuditMetricsREST.class);

    @Autowired
    RESTErrorUtil restErrorUtil;

    @Autowired
    RangerSearchUtil searchUtil;

    @Autowired
    AccessAuditsMetricsServiceFactory accessAuditsMetricsServiceFactory;

    @Autowired
    AuditMetricsDBStore auditMetricsDBStore;

    @GET
    @Path("/metrics/servicetype/{servicetype}/servicename/{servicename}")
    @Produces("application/json")
    @PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.GET_LATEST_AUDIT_METRICS + "\")")
    public RangerAuditMetrics getLatestAuditMetrics(@PathParam("servicetype") String serviceType, @PathParam("servicename") String serviceName,
            @QueryParam("timezone") String timezone) {
        LOG.debug("==> AuditMetricsREST.getLatestAuditMetrics(serviceType={} serviceName={})", serviceType, serviceName);
        RangerAuditMetrics ret;
        try {
            AccessAuditsMetricsService accessAuditsMetricsService = accessAuditsMetricsServiceFactory.getAccessAuditsMetricsService();

            ret = accessAuditsMetricsService.getLatestAuditMetrics(serviceType, serviceName, timezone);
        } catch (WebApplicationException excp) {
            throw excp;
        } catch (Throwable excp) {
            LOG.error("getLatestAuditMetrics for serviceType={}, serviceName={} failed", serviceType, serviceName, excp);

            throw restErrorUtil.createRESTException(excp.getMessage());
        }
        LOG.debug("<== AuditMetricsREST.getLatestAuditMetrics(serviceType={} serviceName={}): {}", serviceType, serviceName, ret);
        return ret;
    }

    @GET
    @Path("/metrics/{id}")
    @Produces("application/json")
    @PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.GET_AUDIT_METRICS + "\")")
    public RangerAuditMetrics getAuditMetrics(@PathParam("id") Long id, @QueryParam("timezone") String timezone) {
        LOG.debug("==> AuditMetricsREST.getAuditMetrics(id={})", id);
        RangerAuditMetrics ret;
        try {
            AccessAuditsMetricsService accessAuditsMetricsService = accessAuditsMetricsServiceFactory.getAccessAuditsMetricsService();

            ret = accessAuditsMetricsService.getAuditMetrics(id, timezone);
        } catch (WebApplicationException excp) {
            throw excp;
        } catch (Throwable excp) {
            LOG.error("getAuditMetrics({}) failed", id, excp);

            throw restErrorUtil.createRESTException(excp.getMessage());
        }
        LOG.debug("<== AuditMetricsREST.getAuditMetrics(id={}):{}", id, ret);
        return ret;
    }

    @GET
    @Path("/metrics")
    @Produces("application/json")
    @PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.GET_ALL_LATEST_AUDIT_METRICS + "\")")
    public RangerAuditMetricsList getAllLatestRangerAuditMetrics(@Context HttpServletRequest request, @QueryParam("timezone") String timezone) {
        LOG.debug("==> AuditMetricsREST.getAllLatestAuditMetrics()");
        RangerAuditMetricsList   ret = new RangerAuditMetricsList();
        List<RangerAuditMetrics> rangerAuditMetrics;

        SearchFilter filter = searchUtil.getSearchFilter(request, Collections.emptyList());

        try {
            AccessAuditsMetricsService accessAuditsMetricsService = accessAuditsMetricsServiceFactory.getAccessAuditsMetricsService();

            rangerAuditMetrics = accessAuditsMetricsService.getLatestAuditMetricsList(filter, timezone);
        } catch (WebApplicationException excp) {
            throw excp;
        } catch (Throwable excp) {
            LOG.error("getAllLatestRangerAuditMetrics failed", excp);
            throw restErrorUtil.createRESTException(excp.getMessage());
        }

        if (CollectionUtils.isNotEmpty(rangerAuditMetrics)) {
            ret = new RangerAuditMetricsList(rangerAuditMetrics);
        }

        LOG.debug("<== AuditMetricsREST.getAllLatestRangerAuditMetrics()");

        return ret;
    }

    @GET
    @Path("/dailymetrics")
    @Produces("application/json")
    @PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.GET_DAILY_AUDIT_METRICS + "\")")
    public RangerAuditMetricsListByHours getDailyAuditMetrics(@Context HttpServletRequest request, @QueryParam("timezone") String timezone) {
        LOG.debug("==> AuditMetricsREST.getDailyAuditMetrics()");
        RangerAuditMetricsListByHours   ret = new RangerAuditMetricsListByHours();
        List<RangerAuditMetricsByHours> rangerAuditMetricsByHours;

        SearchFilter filter = searchUtil.getSearchFilter(request, Collections.emptyList());
        try {
            AccessAuditsMetricsService accessAuditsMetricsService = accessAuditsMetricsServiceFactory.getAccessAuditsMetricsService();

            rangerAuditMetricsByHours = accessAuditsMetricsService.getAuditMetricsByHours(filter, timezone);
        } catch (WebApplicationException excp) {
            throw excp;
        } catch (Throwable excp) {
            LOG.error("getDailyAuditMetrics failed", excp);
            throw restErrorUtil.createRESTException(excp.getMessage());
        }

        if (CollectionUtils.isNotEmpty(rangerAuditMetricsByHours)) {
            ret = new RangerAuditMetricsListByHours(rangerAuditMetricsByHours);
        }

        LOG.debug("<== AuditMetricsREST.getDailyAuditMetrics()");

        return ret;
    }

    @GET
    @Path("/daysmetrics")
    @Produces("application/json")
    @PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.GET_DAYS_AUDIT_METRICS + "\")")
    public RangerAuditMetricsListByDays getDaysAuditMetrics(@Context HttpServletRequest request, @DefaultValue("7") @QueryParam("olderThanInDays") Integer olderThanInDays,
            @QueryParam("timezone") String timezone) {
        LOG.debug("==> AuditMetricsREST.getDaysAuditMetrics()");

        auditMetricsDBStore.validateMaxAllowedDays(olderThanInDays);

        RangerAuditMetricsListByDays   ret    = new RangerAuditMetricsListByDays();
        List<RangerAuditMetricsByDays> rangerAuditMetricsByDays;
        SearchFilter                   filter = searchUtil.getSearchFilter(request, Collections.emptyList());
        try {
            AccessAuditsMetricsService accessAuditsMetricsService = accessAuditsMetricsServiceFactory.getAccessAuditsMetricsService();

            rangerAuditMetricsByDays = accessAuditsMetricsService.getAuditMetricsByDays(olderThanInDays, filter, timezone);
        } catch (WebApplicationException excp) {
            throw excp;
        } catch (Throwable excp) {
            LOG.error("getDaysAuditMetrics failed", excp);
            throw restErrorUtil.createRESTException(excp.getMessage());
        }

        if (CollectionUtils.isNotEmpty(rangerAuditMetricsByDays)) {
            ret = new RangerAuditMetricsListByDays(rangerAuditMetricsByDays);
        }

        LOG.debug("<== AuditMetricsREST.getDaysAuditMetrics()");

        return ret;
    }

    @GET
    @Path("/days-audit-admin-metrics")
    @Produces("application/json")
    @PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.GET_DAYS_AUDIT_ADMIN_METRICS + "\")")
    public Map<String, List<RangerAuditAdminMetricsByDays>> getDaysAuditAdminMetrics(@QueryParam("objectClassTypes") List<String> objectClassTypes,
            @QueryParam("actions") List<String> actions, @DefaultValue("7") @QueryParam("olderThanInDays") Integer olderThanInDays,
            @DefaultValue("UTC") @QueryParam("timezone") String timezone) {
        LOG.debug("==> AuditMetricsREST.getDaysAuditAdminMetrics(objectClassTypes={}, actions={}, olderThanInDays={}, timezone={})", objectClassTypes, actions, olderThanInDays, timezone);

        Map<String, List<RangerAuditAdminMetricsByDays>> ret = new LinkedHashMap<>();
        List<RangerAuditAdminMetricsByDays> rangerAuditAdminMetrics;

        try {
            rangerAuditAdminMetrics = auditMetricsDBStore.getRangerAuditAdminMetricsByDays(olderThanInDays, objectClassTypes, actions, timezone);
        } catch (WebApplicationException excp) {
            throw excp;
        } catch (Throwable excp) {
            LOG.error("getDaysAuditAdminMetrics failed", excp);
            throw restErrorUtil.createRESTException(excp.getMessage());
        }

        if (CollectionUtils.isNotEmpty(rangerAuditAdminMetrics)) {
            RangerAuditAdminMetricsByDays.TYPE_TO_KEY.values().forEach(k -> ret.put(k, new ArrayList<>()));

            for (RangerAuditAdminMetricsByDays auditAdminMetric : rangerAuditAdminMetrics) {
                String key = RangerAuditAdminMetricsByDays.TYPE_TO_KEY.get(auditAdminMetric.getObjectClassType());
                if (key != null) {
                    ret.get(key).add(auditAdminMetric);
                }
            }
        }

        LOG.debug("<== AuditMetricsREST.getDaysAuditAdminMetrics(): {}", ret);

        return ret;
    }

    @GET
    @Path("/days-audit-access-metrics")
    @Produces("application/json")
    @PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.GET_DAYS_AUDIT_ACCESS_METRICS + "\")")
    public Map<String, List<Map<String, Object>>> getDaysAuditAccessMetrics(@DefaultValue("7") @QueryParam("olderThanInDays") Integer olderThanInDays,
            @DefaultValue("UTC") @QueryParam("timezone") String timezone) {
        LOG.debug("==> AuditMetricsREST.getDaysAuditAccessMetrics(olderThanInDays={}, timezone={})", olderThanInDays, timezone);

        auditMetricsDBStore.validateMaxAllowedDays(olderThanInDays);

        Map<String, List<Map<String, Object>>> ret = new LinkedHashMap<>();
        List<Map<String, Object>> rangerAuditAccessMetrics;

        try {
            AccessAuditsMetricsService accessAuditsMetricsService = accessAuditsMetricsServiceFactory.getAccessAuditsMetricsService();

            rangerAuditAccessMetrics = accessAuditsMetricsService.getAuditAccessMetricsByDays(olderThanInDays, timezone);
        } catch (WebApplicationException excp) {
            throw excp;
        } catch (Throwable excp) {
            LOG.error("getDaysAuditAccessMetrics failed", excp);
            throw restErrorUtil.createRESTException(excp.getMessage());
        }

        ret.put("AuditAccessMetricsByDays", rangerAuditAccessMetrics);

        LOG.debug("<== AuditMetricsREST.getDaysAuditAccessMetrics(): {}", ret);

        return ret;
    }
}
