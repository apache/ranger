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

import org.apache.ranger.metrics.RangerAdminMetricsWrapper;
import org.apache.ranger.plugin.model.RangerMetrics;
import org.apache.ranger.util.RangerMetricsUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.util.LinkedHashMap;
import java.util.Map;

@Path("metrics")
@Component
@Scope("request")
@Transactional(propagation = Propagation.REQUIRES_NEW)
public class MetricsREST {
    private static final Logger LOG = LoggerFactory.getLogger(MetricsREST.class);

    private static final RuntimeMXBean RUNTIME                         = ManagementFactory.getRuntimeMXBean();
    private static final String        JVM_MACHINE_ACTUAL_NAME         = RUNTIME.getVmName();
    private static final String        VERSION                         = RUNTIME.getVmVersion();
    private static final String        JVM_MACHINE_REPRESENTATION_NAME = RUNTIME.getName();
    private static final String        JVM_VENDOR_NAME                 = RUNTIME.getVmVendor();

    @Autowired
    RangerMetricsUtil jvmMetricUtil;

    @Autowired
    private RangerAdminMetricsWrapper rangerAdminMetricsWrapper;

    @GET
    @Path("/status")
    @Produces("application/json")
    public RangerMetrics getStatus() {
        LOG.debug("==> MetricsREST.getStatus()");

        Map<String, Object> jvm       = new LinkedHashMap<>();
        Map<String, Object> vmDetails = new LinkedHashMap<>();

        vmDetails.put("JVM Machine Actual Name", JVM_MACHINE_ACTUAL_NAME);
        vmDetails.put("version", VERSION);
        vmDetails.put("JVM Machine Representation Name", JVM_MACHINE_REPRESENTATION_NAME);
        vmDetails.put("Up time of JVM", RUNTIME.getUptime());
        vmDetails.put("JVM Vendor Name", JVM_VENDOR_NAME);
        vmDetails.putAll(jvmMetricUtil.getValues());

        jvm.put("jvm", vmDetails);

        LOG.debug("<== MetricsREST.getStatus(): jvm={}", jvm);

        return new RangerMetrics(jvm);
    }

    @GET
    @Path("/prometheus")
    @Produces(MediaType.TEXT_PLAIN)
    public String getMetricsPrometheus() {
        LOG.debug("==> MetricsREST.getMetricsPrometheus()");

        String ret = "";

        try {
            ret = rangerAdminMetricsWrapper.getRangerMetricsInPrometheusFormat();
        } catch (Exception e) {
            LOG.error("MetricsREST.getMetricsPrometheus(): Exception occurred while getting metric.", e);
        }

        LOG.debug("<== MetricsREST.getMetricsPrometheus(): ret={}", ret);

        return ret;
    }

    @GET
    @Path("/json")
    @Produces(MediaType.APPLICATION_JSON)
    public Map<String, Map<String, Object>> getMetricsJson() {
        LOG.debug("==> MetricsREST.getMetricsJson()");

        Map<String, Map<String, Object>> ret = null;

        try {
            ret = rangerAdminMetricsWrapper.getRangerMetrics();
        } catch (Exception e) {
            LOG.error("MetricsREST.getMetricsJson(): Exception occurred while getting metric.", e);
        }

        LOG.debug("<== MetricsREST.getMetricsJson(): ret={}", ret);

        return ret;
    }
}
