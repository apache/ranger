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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.security.authentication.util.KerberosName;
import org.apache.ranger.audit.model.AuthzAuditEvent;
import org.apache.ranger.audit.producer.AuditDestinationMgr;
import org.apache.ranger.audit.provider.MiscUtil;
import org.apache.ranger.audit.server.AuditServerConfig;
import org.apache.ranger.audit.server.AuditServerConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Path("/audit")
@Component
@Scope("request")
public class AuditREST {
    private static final Logger LOG = LoggerFactory.getLogger(AuditREST.class);
    private static final TypeReference<List<AuthzAuditEvent>> AUDIT_EVENT_LIST_TYPE = new TypeReference<List<AuthzAuditEvent>>() {};
    private static final Set<String> allowedServiceUsers;

    static {
        allowedServiceUsers = initializeAllowedUsers();
        initializeAuthToLocal();
    }

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
                resp.put("service", "ranger-audit-server");
                jsonString = buildResponse(resp);
                ret = Response.ok()
                        .entity(jsonString)
                        .build();
            } else {
                Map<String, Object> resp = new HashMap<>();
                resp.put("status", "DOWN");
                resp.put("service", "ranger-audit-server");
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
            resp.put("service", "ranger-audit-server");
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
        String   jsonString;

        try {
            String status = (auditDestinationMgr != null) ? "READY" : "NOT_READY";

            Map<String, Object> resp = new HashMap<>();
            resp.put("status", status);
            resp.put("timestamp", System.currentTimeMillis());
            resp.put("service", "ranger-audit-server");
            jsonString = buildResponse(resp);
            ret = Response.status(Response.Status.OK)
                    .entity(jsonString)
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
     *  Access Audits producer endpoint.
     *  @param serviceName Required query parameter to identify the source service (hdfs, hive, kafka, solr, etc.)
     *  @param accessAudits JSON payload containing audit events
     *  @param request HTTP request to extract authenticated user
     */
    @POST
    @Path("/access")
    @Consumes("application/json")
    @Produces("application/json")
    public Response accessAudit(@QueryParam("serviceName") String serviceName, String accessAudits, @Context HttpServletRequest request) {
        String authenticatedUser = getAuthenticatedUser(request);

        LOG.debug("==> AuditREST.accessAudit(): received JSON payload from service: {}, authenticatedUser: {}", StringUtils.isNotEmpty(serviceName) ? serviceName : "unknown", authenticatedUser);

        Response ret;

        if (StringUtils.isEmpty(serviceName)) {
            LOG.error("serviceName query parameter is required. Rejecting audit request.");
            ret = Response.status(Response.Status.BAD_REQUEST)
                    .entity(buildErrorResponse("serviceName query parameter is required"))
                    .build();
            return ret;
        }

        if (StringUtils.isEmpty(authenticatedUser)) {
            LOG.error("No authenticated user found in request for service: {}. Rejecting audit request.", serviceName);
            ret = Response.status(Response.Status.UNAUTHORIZED)
                    .entity(buildErrorResponse("Authentication required to send audit events"))
                    .build();
            return ret;
        }

        if (!serviceName.equals(authenticatedUser)) {
            LOG.error("Authentication mismatch: serviceName={} but authenticatedUser={}. Rejecting audit request.", serviceName, authenticatedUser);
            ret = Response.status(Response.Status.FORBIDDEN)
                    .entity(buildErrorResponse("Service name does not match authenticated user"))
                    .build();
            return ret;
        }

        if (!isAllowedServiceUser(authenticatedUser)) {
            LOG.error("Unauthorized user: authenticatedUser={} is not in the allowed service users list. Rejecting audit request.", authenticatedUser);
            ret = Response.status(Response.Status.FORBIDDEN)
                    .entity(buildErrorResponse("User is not authorized to send audit events"))
                    .build();
            return ret;
        }

        if (accessAudits == null || accessAudits.trim().isEmpty()) {
            LOG.warn("Empty or null audit events batch received from service: {}, user: {}", serviceName, authenticatedUser);
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
                ObjectMapper mapper = MiscUtil.getMapper();
                List<AuthzAuditEvent> events = mapper.readValue(accessAudits, AUDIT_EVENT_LIST_TYPE);

                LOG.debug("Successfully deserialized {} audit events from service: {}", events.size(), serviceName);

                for (AuthzAuditEvent event : events) {
                    auditDestinationMgr.log(event);
                }

                Map<String, Object> response = new HashMap<>();
                response.put("total", events.size());
                response.put("timestamp", System.currentTimeMillis());
                if (StringUtils.isNotEmpty(serviceName)) {
                    response.put("serviceName", serviceName);
                }
                if (StringUtils.isNotEmpty(authenticatedUser)) {
                    response.put("authenticatedUser", authenticatedUser);
                }
                String jsonString = buildResponse(response);
                ret = Response.status(Response.Status.OK)
                        .entity(jsonString)
                        .build();
            } catch (Exception e) {
                LOG.error("Error processing access audits batch from service: {}", serviceName, e);
                ret = Response.status(Response.Status.BAD_REQUEST)
                        .entity(buildErrorResponse("Failed to process audit events: " + e.getMessage()))
                        .build();
            }
        }

        LOG.debug("<== AuditREST.accessAudit(): HttpStatus {} for service: {}, user: {}", ret.getStatus(), serviceName, authenticatedUser);

        return ret;
    }

    private String buildResponse(Map<String, Object> respMap) {
        try {
            return MiscUtil.getMapper().writeValueAsString(respMap);
        } catch (Exception e) {
            return "Error: " + e.getMessage();
        }
    }

    private String buildErrorResponse(String message) {
        try {
            Map<String, Object> error = new HashMap<>();
            error.put("status", "error");
            error.put("message", message);
            error.put("timestamp", System.currentTimeMillis());
            return MiscUtil.getMapper().writeValueAsString(error);
        } catch (Exception e) {
            LOG.error("Error building error response", e);
            return "{\"status\":\"error\",\"message\":\"" + message.replace("\"", "'") + "\"}";
        }
    }

    private String getAuthenticatedUser(HttpServletRequest request) {
        if (request != null && request.getUserPrincipal() != null) {
            String principalName = request.getUserPrincipal().getName();
            LOG.debug("Authenticated user from Principal: {}", principalName);
            String shortName = applyAuthToLocal(principalName);
            if (!shortName.equals(principalName)) {
                LOG.debug("Applied auth_to_local: '{}' -> '{}'", principalName, shortName);
            }

            return shortName;
        }

        LOG.debug("No authenticated user found in request");
        return null;
    }

    /**
     * Apply Hadoop's auth_to_local rules to convert Kerberos principal to short username.
     * For JWT or basic auth, the username is already in short form and returned as-is.
     */
    private String applyAuthToLocal(String principal) {
        if (StringUtils.isEmpty(principal)) {
            return principal;
        }

        // Check if this looks like a Kerberos principal (has @ or /)
        if (!principal.contains("@") && !principal.contains("/")) {
            LOG.debug("Username '{}' is already a short name (JWT/basic auth), no auth_to_local mapping needed", principal);
            return principal;
        }

        try {
            KerberosName kerberosName = new KerberosName(principal);
            String shortName = kerberosName.getShortName();
            return shortName;
        } catch (Exception e) {
            LOG.warn("Failed to apply auth_to_local rules to principal '{}': {}. Using original principal.", principal, e.getMessage());
            return principal;
        }
    }

    /**
     * Rules are loaded from ranger.audit.service.auth.to.local property in ranger-audit-server-site.xml.
     */
    private static void initializeAuthToLocal() {
        AuditServerConfig config = AuditServerConfig.getInstance();
        String authToLocalRules = config.get(AuditServerConstants.PROP_AUTH_TO_LOCAL);
        if (StringUtils.isNotEmpty(authToLocalRules)) {
            try {
                KerberosName.setRules(authToLocalRules);
                LOG.debug("Auth_to_local rules: {}", authToLocalRules);
            } catch (Exception e) {
                LOG.error("Failed to set auth_to_local rules from configuration: {}", e.getMessage(), e);
            }
        } else {
            LOG.warn("No auth_to_local rules configured. Kerberos principal mapping may not work correctly.");
            LOG.warn("Set property '{}' in ranger-audit-server-site.xml", AuditServerConstants.PROP_AUTH_TO_LOCAL);
        }
    }

    /**
     * Check if the user login into audit server for posting audits is in the allowed service users list
     * @param userName The username to check
     * @return true if user is allowed, false otherwise
     */
    private boolean isAllowedServiceUser(String userName) {
        boolean ret;

        if (StringUtils.isEmpty(userName)) {
            ret = false;
        } else {
            ret = allowedServiceUsers.contains(userName);
            LOG.debug("User '{}' allowed: {}", userName, ret);
        }

        return ret;
    }

    /**
     * Initialize the set of allowed service users from configuration
     * @return Set of allowed usernames
     */
    private static Set<String> initializeAllowedUsers() {
        Set<String>       ret    = new HashSet<>();
        AuditServerConfig config = AuditServerConfig.getInstance();

        String allowedUsersStr = config.get(AuditServerConstants.PROP_ALLOWED_USERS, AuditServerConstants.DEFAULT_ALLOWED_USERS);
        if (StringUtils.isNotEmpty(allowedUsersStr)) {
            String[] users = allowedUsersStr.split(",");
            for (String user : users) {
                String trimmedUser = user.trim();
                if (StringUtils.isNotEmpty(trimmedUser)) {
                    ret.add(trimmedUser);
                }
            }

            LOG.debug("Allowed service users: {}", ret);
        }

        return ret;
    }
}
