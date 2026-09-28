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

package org.apache.ranger.audit.rest;

import org.apache.ranger.audit.dispatcher.kafka.AuditDispatcherTracker;
import org.apache.ranger.audit.provider.MiscUtil;
import org.apache.ranger.audit.server.AuditServerConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Response;

import java.util.HashMap;
import java.util.Map;

/**
 * Health check REST endpoint for Audit Dispatcher Service
 */
@Path("/health")
@Component
@Scope("request")
public class AuditDispatcherHealthREST {
    private static final Logger LOG = LoggerFactory.getLogger(AuditDispatcherHealthREST.class);

    /**
     * Simple ping endpoint to verify the service is running
     */
    @GET
    @Path("/ping")
    @Produces("application/json")
    public Response ping() {
        Map<String, Object> resp = new HashMap<>();
        resp.put("status", "UP");
        resp.put("service", "audit-dispatcher");

        try {
            Response ret = Response.status(Response.Status.OK)
                    .entity(buildResponse(resp))
                    .build();
            return ret;
        } catch (Exception e) {
            LOG.error("Error creating ping response", e);
            return Response.serverError().build();
        }
    }

    /**
     * Detailed health check endpoint that verifies internal components
     */
    @GET
    @Path("/status")
    @Produces("application/json")
    public Response status() {
        Map<String, Object> resp = new HashMap<>();
        try {
            String dispatcherType = System.getProperty(AuditServerConstants.PROP_DISPATCHER_TYPE);
            resp.put("service", "audit-dispatcher-" + (dispatcherType != null ? dispatcherType : "unknown"));

            Response.Status status = null;
            if (dispatcherType != null && !dispatcherType.trim().isEmpty()) {
                boolean isActive = AuditDispatcherTracker.getInstance().getActiveDispatcherTypes().contains(dispatcherType.toLowerCase());
                if (isActive) {
                    status = Response.Status.OK;
                    resp.put("status", "UP");
                } else {
                    status = Response.Status.SERVICE_UNAVAILABLE;
                    resp.put("status", "DOWN");
                    resp.put("reason", dispatcherType + " Dispatcher is not active");
                }
            } else {
                status = Response.Status.SERVICE_UNAVAILABLE;
                resp.put("status", "DOWN");
                resp.put("reason", "Dispatcher type not provided: ");
            }

            Response ret = Response.status(status)
                    .entity(buildResponse(resp))
                    .build();
            return ret;
        } catch (Exception e) {
            LOG.error("Error checking status", e);
            resp.put("status", "ERROR");
            resp.put("error", e.getMessage());
            return Response.serverError().entity(resp).build();
        }
    }

    private String buildResponse(Map<String, Object> respMap) {
        try {
            return MiscUtil.getMapper().writeValueAsString(respMap);
        } catch (Exception e) {
            return "Error: " + e.getMessage();
        }
    }
}
