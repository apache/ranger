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
import org.apache.ranger.biz.AuditMetricsDBStore;
import org.apache.ranger.common.RESTErrorUtil;
import org.apache.ranger.common.RangerSearchUtil;
import org.apache.ranger.plugin.model.RangerAuditMetrics;
import org.apache.ranger.plugin.model.RangerAuditMetricsByDays;
import org.apache.ranger.plugin.model.RangerAuditMetricsByHours;
import org.apache.ranger.plugin.util.SearchFilter;
import org.apache.ranger.security.context.RangerAPIList;
import org.apache.ranger.service.RangerAuditMetricsService;
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
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;

import java.util.List;

@Path("audit")
@Component
@Scope("request")
@Transactional(propagation = Propagation.REQUIRES_NEW)
public class AuditMetricsREST {
    private static final Logger LOG = LoggerFactory.getLogger(AuditMetricsREST.class);

    @Autowired
    RESTErrorUtil restErrorUtil;

    @Autowired
    AuditMetricsDBStore auditMetricsDBStore;

    @Autowired
    RangerSearchUtil searchUtil;

    @Autowired
    RangerAuditMetricsService rangerAuditMetricsService;

    @POST
    @Path("/metrics")
    @Produces("application/json")
    public RangerAuditMetrics createAuditMetrics(RangerAuditMetrics rangerAuditMetrics) {
        LOG.debug("==> AuditMetricsREST.createAuditMetrics({})", rangerAuditMetrics);

        if (rangerAuditMetrics == null) {
            // send error message with response code 400
            throw restErrorUtil.createRESTException("Can not create RangerAuditMetrics for null.");
        }

        RangerAuditMetrics ret;
        try {
            ret = auditMetricsDBStore.createRangerAuditMetrics(rangerAuditMetrics);
        } catch (WebApplicationException excp) {
            throw excp;
        } catch (Throwable excp) {
            LOG.error("createRangerAuditMetrics({}) failed", rangerAuditMetrics, excp);
            throw restErrorUtil.createRESTException(excp.getMessage());
        }
        LOG.debug("<== AuditMetricsREST.createRangerAuditMetrics({}):{}", rangerAuditMetrics, ret);
        return ret;
    }

    @GET
    @Path("/metrics/servicetype/{servicetype}/servicename/{servicename}")
    @Produces("application/json")
    @PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.GET_LATEST_AUDIT_METRICS + "\")")
    public RangerAuditMetrics getLatestAuditMetrics(@PathParam("servicetype") String serviceType, @PathParam("servicename") String serviceName) {
        LOG.debug("==> AuditMetricsREST.getAuditMetrics(serviceType={} serviceName={})", serviceType, serviceName);
        RangerAuditMetrics ret;
        try {
            ret = auditMetricsDBStore.getLatestRangerAuditMetrics(serviceType, serviceName);
        } catch (WebApplicationException excp) {
            throw excp;
        } catch (Throwable excp) {
            LOG.error("getAuditMetrics for serviceType={}, ServiceName={} failed", serviceType, serviceName, excp);

            throw restErrorUtil.createRESTException(excp.getMessage());
        }
        LOG.debug("<== AuditMetricsREST.getAuditMetrics(serviceType= \" + serviceType + \"serviceName=\" + serviceName + \")\"):{}", ret);
        return ret;
    }

    @GET
    @Path("/metrics/{id}")
    @Produces("application/json")
    @PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.GET_AUDIT_METRICS + "\")")
    public RangerAuditMetrics getAuditMetrics(@PathParam("id") Long id) {
        LOG.debug("==> AuditMetricsREST.getAuditMetrics(id={})", id);
        RangerAuditMetrics ret;
        try {
            ret = auditMetricsDBStore.getRangerAuditMetrics(id);
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
    public RangerAuditMetricsList getAllLatestRangerAuditMetrics(@Context HttpServletRequest request) {
        LOG.debug("==> AuditMetricsREST.getAllLatestAuditMetrics()");
        RangerAuditMetricsList   ret = new RangerAuditMetricsList();
        List<RangerAuditMetrics> rangerAuditMetrics;

        SearchFilter filter = searchUtil.getSearchFilter(request, rangerAuditMetricsService.sortFields);

        try {
            rangerAuditMetrics = auditMetricsDBStore.getAllLatestRangerAuditMetrics(filter);
        } catch (WebApplicationException excp) {
            throw excp;
        } catch (Throwable excp) {
            LOG.error("Error getting all latest audit metrics..", excp);
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
    public RangerAuditMetricsListByHours getDailyAuditMetrics(@Context HttpServletRequest request) {
        LOG.debug("==> AuditMetricsREST.getRangerAuditMetricsByHours()");
        RangerAuditMetricsListByHours   ret = new RangerAuditMetricsListByHours();
        List<RangerAuditMetricsByHours> rangerAuditMetricsByHours;

        SearchFilter filter = searchUtil.getSearchFilter(request, rangerAuditMetricsService.sortFields);
        try {
            rangerAuditMetricsByHours = auditMetricsDBStore.getRangerAuditMetricsByHours(filter);
        } catch (WebApplicationException excp) {
            throw excp;
        } catch (Throwable excp) {
            LOG.error("Error getting all latest audit metrics..", excp);
            throw restErrorUtil.createRESTException(excp.getMessage());
        }

        if (CollectionUtils.isNotEmpty(rangerAuditMetricsByHours)) {
            ret = new RangerAuditMetricsListByHours(rangerAuditMetricsByHours);
        }

        LOG.debug("<== AuditMetricsREST.getRangerAuditMetricsByHours()");

        return ret;
    }

    @GET
    @Path("/daysmetrics")
    @Produces("application/json")
    @PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.GET_DAYS_AUDIT_METRICS + "\")")
    public RangerAuditMetricsListByDays getDaysAuditMetrics(@Context HttpServletRequest request, @DefaultValue("7") @QueryParam("olderThanInDays") Integer olderThanInDays) {
        LOG.debug("==> AuditMetricsREST.getDaysAuditMetrics()");
        auditMetricsDBStore.validateRetentionDays(olderThanInDays);

        RangerAuditMetricsListByDays   ret    = new RangerAuditMetricsListByDays();
        List<RangerAuditMetricsByDays> rangerAuditMetricsByDays;
        SearchFilter                   filter = searchUtil.getSearchFilter(request, rangerAuditMetricsService.sortFields);
        try {
            rangerAuditMetricsByDays = auditMetricsDBStore.getRangerAuditMetricsByDays(olderThanInDays, filter);
        } catch (WebApplicationException excp) {
            throw excp;
        } catch (Throwable excp) {
            LOG.error("Error getting all latest audit metrics..", excp);
            throw restErrorUtil.createRESTException(excp.getMessage());
        }

        if (CollectionUtils.isNotEmpty(rangerAuditMetricsByDays)) {
            ret = new RangerAuditMetricsListByDays(rangerAuditMetricsByDays);
        }

        LOG.debug("<== AuditMetricsREST.getDaysAuditMetrics()");

        return ret;
    }

    @DELETE
    @Path("/metrics")
    @Produces("application/json")
    public void deleteRangerAuditMetrics(@DefaultValue("7") @QueryParam("retentionPeriod") Integer olderThanInDays, @DefaultValue("") @QueryParam("serviceType") String serviceType, @DefaultValue("") @QueryParam("serviceName") String serviceName, @Context HttpServletRequest request) {
        LOG.debug("==> AuditMetricsREST.deleteRangerAuditMetrics({})", olderThanInDays);
        auditMetricsDBStore.validateRetentionDays(olderThanInDays);

        try {
            auditMetricsDBStore.deleteRangerAuditMetrics(olderThanInDays, serviceName, serviceType);
        } catch (WebApplicationException excp) {
            throw excp;
        } catch (Exception e) {
            LOG.error("Error deleting Ranger Audit Metrics older than: {}", olderThanInDays, e);
            throw restErrorUtil.createRESTException(e.getMessage());
        }
        LOG.debug("<== AuditMetricsREST.deleteRangerAuditMetrics({})", olderThanInDays);
    }
}
