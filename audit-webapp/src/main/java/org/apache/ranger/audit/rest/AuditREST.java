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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonSyntaxException;
import com.google.gson.reflect.TypeToken;
import org.apache.ranger.audit.destination.AuditDestinationMgr;
import org.apache.ranger.audit.model.AuthzAuditEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Response;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Path("/audit")
@Component
@Scope("request")
public class AuditREST {
    private static final Logger LOG  = LoggerFactory.getLogger(AuditREST.class);
    private              Gson   gson = new GsonBuilder().setDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ").create();
    @Autowired
    AuditDestinationMgr auditDestinationMgr;

    /**
     * Health check endpoint
     */
    @GET
    @Path("/health")
    @Produces("application/json")
    public Response healthCheck() {
        LOG.debug("==> AuditREST.healthCheck()");
        Response ret;
        String   jsonString;

        try {
            // Check if audit destination manager is available and healthy
            if (auditDestinationMgr != null) {
                Map<String, Object> resp = new HashMap<>();
                resp.put("status", "UP");
                resp.put("service", "audit-server");
                jsonString = buildResponse(resp);
                ret = Response.ok()
                        .entity(jsonString)
                        .build();
            } else {
                Map<String, Object> resp = new HashMap<>();
                resp.put("status", "DOWN");
                resp.put("service", "audit-server");
                resp.put("reason", "AuditDestinationMgr not available");
                jsonString = buildResponse(resp);
                ret = Response.status(Response.Status.SERVICE_UNAVAILABLE)
                        .entity(jsonString)
                        .build();
            }
        } catch (Exception e) {
            LOG.error("Health check failed", e);
            Map<String, Object> resp = new HashMap<>();
            resp.put("status", "DOWN");
            resp.put("service", "audit-server");
            resp.put("reason",  e.getMessage());
            jsonString = buildResponse(resp);
            ret = Response.status(Response.Status.SERVICE_UNAVAILABLE)
                    .entity(jsonString)
                    .build();
        }

        LOG.debug("<== AuditREST.healthCheck(): {}", ret);

        return ret;
    }

    /**
     * Status endpoint for monitoring
     */
    @GET
    @Path("/status")
    @Produces("application/json")
    public Response getStatus() {
        LOG.debug("==> AuditREST.getStatus()");

        Response ret;
        try {
            long timestamp = System.currentTimeMillis();
            String status = (auditDestinationMgr != null) ? "READY" : "NOT_READY";

            String jsonResponse = String.format("{\"status\":\"%s\",\"timestamp\":%d,\"service\":\"audit-server\"}", status, timestamp);
            ret = Response.ok()
                    .entity(jsonResponse)
                    .build();
        } catch (Exception e) {
            LOG.error("Status check failed", e);
            ret = Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity("{\"status\":\"ERROR\",\"error\":\"" + e.getMessage() + "\"}")
                    .build();
        }

        LOG.debug("<== AuditREST.getStatus(): {}", ret);

        return ret;
    }

    /**
     *  Access Audits consumer endpoint.
     */
    @POST
    @Path("/post")
    @Consumes("application/json")
    @Produces("application/json")
    public Response postAudit(String accessAudits) {
        LOG.debug("==> AuditREST.postAudit(): {}", accessAudits);

        Response ret;
        if (accessAudits == null || accessAudits.trim().isEmpty()) {
            LOG.warn("Empty or null audit events batch received");
            ret = Response.status(Response.Status.BAD_REQUEST)
                    .entity(buildErrorResponse("Audit events cannot be empty"))
                    .build();
        } else if (auditDestinationMgr == null) {
            LOG.error("AuditDestinationMgr not initialized");
            ret = Response.status(Response.Status.SERVICE_UNAVAILABLE)
                    .entity(buildErrorResponse("Audit service not available"))
                    .build();
        } else {
            try {
                String jsonString;
                List<AuthzAuditEvent> events = gson.fromJson(accessAudits, new TypeToken<List<AuthzAuditEvent>>() {}.getType());

                for (AuthzAuditEvent event : events) {
                    auditDestinationMgr.log(event);
                }

                Map<String, Object> response = new HashMap<>();
                response.put("total", events.size());
                response.put("timestamp", System.currentTimeMillis());
                jsonString = buildResponse(response);
                ret = Response.status(Response.Status.OK)
                        .entity(jsonString)
                        .build();
            } catch (JsonSyntaxException e) {
                LOG.error("Invalid Access audit JSON string...", e);
                ret = Response.status(Response.Status.BAD_REQUEST)
                        .entity(buildErrorResponse("Invalid Access audit JSON string..." +  e.getMessage()))
                        .build();
            } catch (Exception e) {
                LOG.error("Error processing access audits batch...", e);
                ret = Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                        .entity(buildErrorResponse("Invalid Access audit JSON string..." +  e.getMessage()))
                        .build();
            }
        }

        LOG.debug("<== AuditREST.postAudit(): HttpStatus {}", ret.getStatus());

        return ret;
    }

    private String buildResponse(Map<String, Object> respMap) {
        String ret;

        try {
            ObjectMapper objectMapper = new ObjectMapper();
            ret = objectMapper.writeValueAsString(respMap);
        } catch (Exception e) {
            ret = "Error: " + e.getMessage();
        }

        return ret;
    }

    private String buildErrorResponse(String message) {
        try {
            Map<String, Object> error = new HashMap<>();
            error.put("status", "error");
            error.put("message", message);
            error.put("timestamp", System.currentTimeMillis());
            ObjectMapper mapper = new ObjectMapper();
            return mapper.writeValueAsString(error);
        } catch (Exception e) {
            LOG.error("Error building error response", e);
            return "{\"status\":\"error\",\"message\":\"" + message.replace("\"", "'") + "\"}";
        }
    }
}
