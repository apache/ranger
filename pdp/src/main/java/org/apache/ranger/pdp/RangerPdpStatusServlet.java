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

package org.apache.ranger.pdp;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import org.apache.ranger.pdp.config.RangerPdpConfig;
import org.apache.ranger.pdp.config.RangerPdpConstants;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.io.File;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

public class RangerPdpStatusServlet extends HttpServlet {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final RangerPdpStats  runtimeState;
    private final RangerPdpConfig config;
    private final Mode            mode;

    public enum Mode { LIVE, READY, METRICS }

    public RangerPdpStatusServlet(RangerPdpStats runtimeState, RangerPdpConfig config, Mode mode) {
        this.runtimeState = runtimeState;
        this.config       = config;
        this.mode         = mode;
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        switch (mode) {
            case LIVE:
                writeLive(resp);
                break;
            case READY:
                writeReady(resp);
                break;
            case METRICS:
                writeMetrics(resp);
                break;
            default:
                resp.setStatus(HttpServletResponse.SC_NOT_FOUND);
        }
    }

    private void writeLive(HttpServletResponse resp) throws IOException {
        Map<String, Object> payload = new LinkedHashMap<>();

        payload.put("status", runtimeState.isServerStarted() ? "UP" : "DOWN");
        payload.put("service", "ranger-pdp");
        payload.put("live", runtimeState.isServerStarted());

        resp.setStatus(runtimeState.isServerStarted() ? HttpServletResponse.SC_OK : HttpServletResponse.SC_SERVICE_UNAVAILABLE);
        resp.setContentType("application/json");

        MAPPER.writeValue(resp.getOutputStream(), payload);
    }

    private void writeReady(HttpServletResponse resp) throws IOException {
        boolean ready = runtimeState.isServerStarted() && runtimeState.isAuthorizerInitialized() && runtimeState.isAcceptingRequests();

        Map<String, Object> payload = new LinkedHashMap<>();

        payload.put("status", ready ? "READY" : "NOT_READY");
        payload.put("service", "ranger-pdp");
        payload.put("ready", ready);
        payload.put("authorizerInitialized", runtimeState.isAuthorizerInitialized());
        payload.put("acceptingRequests", runtimeState.isAcceptingRequests());
        payload.put("policyCacheAgeMs", getPolicyCacheAgeMs());

        resp.setStatus(ready ? HttpServletResponse.SC_OK : HttpServletResponse.SC_SERVICE_UNAVAILABLE);
        resp.setContentType("application/json");

        MAPPER.writeValue(resp.getOutputStream(), payload);
    }

    private void writeMetrics(HttpServletResponse resp) throws IOException {
        StringBuilder sb = new StringBuilder(512);

        sb.append("# TYPE ranger_pdp_requests_total counter\n");
        sb.append("ranger_pdp_requests_total ").append(runtimeState.getTotalRequests()).append('\n');
        sb.append("# TYPE ranger_pdp_requests_success_total counter\n");
        sb.append("ranger_pdp_requests_success_total ").append(runtimeState.getTotalAuthzSuccess()).append('\n');
        sb.append("# TYPE ranger_pdp_requests_bad_request_total counter\n");
        sb.append("ranger_pdp_requests_bad_request_total ").append(runtimeState.getTotalAuthzBadRequest()).append('\n');
        sb.append("# TYPE ranger_pdp_requests_error_total counter\n");
        sb.append("ranger_pdp_requests_error_total ").append(runtimeState.getTotalAuthzErrors()).append('\n');
        sb.append("# TYPE ranger_pdp_auth_failures_total counter\n");
        sb.append("ranger_pdp_auth_failures_total ").append(runtimeState.getTotalAuthFailures()).append('\n');
        sb.append("# TYPE ranger_pdp_request_latency_avg_ms gauge\n");
        sb.append("ranger_pdp_request_latency_avg_ms ").append(runtimeState.getAverageLatencyMs()).append('\n');
        sb.append("# TYPE ranger_pdp_policy_cache_age_ms gauge\n");
        sb.append("ranger_pdp_policy_cache_age_ms ").append(getPolicyCacheAgeMs()).append('\n');

        resp.setStatus(HttpServletResponse.SC_OK);
        resp.setContentType("text/plain; version=0.0.4");

        resp.getWriter().write(sb.toString());
    }

    private long getPolicyCacheAgeMs() {
        String cacheDir = config.get(RangerPdpConstants.PROP_AUTHZ_POLICY_CACHE_DIR, null);

        if (StringUtils.isNotBlank(cacheDir)) {
            File dir = new File(cacheDir);

            if (dir.exists() && dir.isDirectory()) {
                File[] files = dir.listFiles((d, name) -> name.endsWith(".json"));

                if (files != null && files.length > 0) {
                    long latestMtime = 0L;

                    for (File f : files) {
                        latestMtime = Math.max(latestMtime, f.lastModified());
                    }

                    if (latestMtime > 0L) {
                        return Math.max(0L, System.currentTimeMillis() - latestMtime);
                    }
                }
            }
        }

        return -1;
    }
}
