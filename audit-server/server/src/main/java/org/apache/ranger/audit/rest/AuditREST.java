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

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.ranger.audit.server.AuditServerConstants.PROP_PREFIX_AUDIT_SERVER_SERVICE;
import static org.apache.ranger.audit.server.AuditServerConstants.PROP_SUFFIX_ALLOWED_USERS;

@Path("/audit")
@Component
@Scope("request")
public class AuditREST {
    private static final Logger LOG = LoggerFactory.getLogger(AuditREST.class);

    private static final Map<String, Set<String>> allowedServiceUsers;

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

        Response.Status     status;
        Map<String, Object> resp = new HashMap<>();

        resp.put("service", "ranger-audit-server");

        try {
            // Check if audit destination manager is available and healthy
            if (auditDestinationMgr != null) {
                status = Response.Status.OK;

                resp.put("status", "UP");
            } else {
                status = Response.Status.SERVICE_UNAVAILABLE;

                resp.put("status", "DOWN");
                resp.put("reason", "AuditDestinationMgr not available");
            }
        } catch (Exception e) {
            LOG.error("Health check failed", e);

            status = Response.Status.SERVICE_UNAVAILABLE;

            resp.put("status", "DOWN");
            resp.put("reason",  e.getMessage());
        }

        Response ret = Response.status(status)
                .entity(buildResponse(resp))
                .build();

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
     *  @param appId Optional query parameter for batch processing - identifies the application instance
     *  @param accessAudits List of audit events to process
     *  @param request HTTP request to extract authenticated user
     */
    @POST
    @Path("/access")
    @Consumes("application/json")
    @Produces("application/json")
    public Response logAccessAudit(@QueryParam("serviceName") String serviceName, @QueryParam("appId") String appId, List<AuthzAuditEvent> accessAudits, @Context HttpServletRequest request) {
        LOG.debug("==> AuditREST.logAccessAudit(serviceName={}, appId={}, auditsCount={}}", serviceName, appId, accessAudits != null ? accessAudits.size() : 0);

        Response ret;
        String authenticatedUser = getAuthenticatedUser(request);

        if (auditDestinationMgr == null) {
            LOG.error("AuditDestinationMgr not initialized");

            ret = Response.status(Response.Status.SERVICE_UNAVAILABLE)
                    .entity(buildErrorResponse("Audit service not available"))
                    .build();
        } else if (accessAudits == null || accessAudits.isEmpty()) {
            LOG.warn("Empty or null audit events batch received from serviceName: {}, user: {}", serviceName, authenticatedUser);

            ret = Response.status(Response.Status.BAD_REQUEST)
                    .entity(buildErrorResponse("Audit events cannot be empty"))
                    .build();
        } else if (StringUtils.isBlank(serviceName)) {
            LOG.error("serviceName query parameter is required. Rejecting audit request.");

            ret = Response.status(Response.Status.BAD_REQUEST)
                    .entity(buildErrorResponse("serviceName query parameter is required"))
                    .build();
        } else if (StringUtils.isEmpty(authenticatedUser)) {
            LOG.error("No authenticated user found. Rejecting audit request.");

            ret = Response.status(Response.Status.UNAUTHORIZED)
                    .entity(buildErrorResponse("Authentication required to send audit events"))
                    .build();
        } else if (!isAllowedServiceUser(serviceName, authenticatedUser)) {
            LOG.error("Unauthorized user: user={} is authorized report audit logs for service={}. Rejecting audit request.", authenticatedUser, serviceName);

            ret = Response.status(Response.Status.FORBIDDEN)
                    .entity(buildErrorResponse("User is not authorized to send audit events"))
                    .build();
        } else {
            try {
                LOG.debug("Processing {} audit events from service: {}, appId: {}", accessAudits.size(), serviceName, appId);

                boolean success = auditDestinationMgr.logBatch(accessAudits, appId);

                if (success) {
                    Map<String, Object> response = new HashMap<>();

                    response.put("total", accessAudits.size());
                    response.put("timestamp", System.currentTimeMillis());
                    response.put("serviceName", serviceName);

                    if (StringUtils.isNotEmpty(appId)) {
                        response.put("appId", appId);
                    }

                    if (StringUtils.isNotEmpty(authenticatedUser)) {
                        response.put("authenticatedUser", authenticatedUser);
                    }

                    String jsonString = buildResponse(response);

                    ret = Response.status(Response.Status.OK)
                            .entity(jsonString)
                            .build();
                } else {
                    LOG.warn("Batch processing failed for {} events from serviceName: {}, appId: {}. Events spooled to recovery.", accessAudits.size(), serviceName, appId);

                    ret = Response.status(Response.Status.ACCEPTED)
                            .entity(buildErrorResponse("Batch processing failed. Events have been queued for retry."))
                            .build();
                }
            } catch (Exception e) {
                LOG.error("Error processing access audits batch from serviceName: {}, appId: {}", serviceName, appId, e);

                ret = Response.status(Response.Status.BAD_REQUEST)
                        .entity(buildErrorResponse("Failed to process audit events: " + e.getMessage()))
                        .build();
            }
        }

        LOG.debug("<== AuditREST.accessAudit(): HttpStatus {} for serviceName: {}, user: {}", ret.getStatus(), serviceName, authenticatedUser);

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
            String shortName     = applyAuthToLocal(principalName);

            LOG.debug("Authenticated user: Principal: {}, shortName: {}", principalName, shortName);

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

            return kerberosName.getShortName();
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
     * @param serviceName The name of service from which audit is being sent
     * @param userName The username to check
     * @return true if user is allowed, false otherwise
     */
    private boolean isAllowedServiceUser(String serviceName, String userName) {
        boolean ret;

        if (StringUtils.isNotBlank(serviceName) && StringUtils.isNotBlank(userName)) {
            Set<String> allowedUsers = allowedServiceUsers.get(serviceName);

            ret = allowedUsers != null && allowedUsers.contains(userName);
        } else {
            ret = false;
        }

        LOG.debug("isAllowedServiceUser(serviceName={}, userName={}): ret={}", serviceName, userName, ret);

        return ret;
    }

    /**
     * Initialize the set of allowed service users from configuration
     * @return Map of allowed usernames per service
     */
    private static Map<String, Set<String>> initializeAllowedUsers() {
        Map<String, Set<String>> ret    = new HashMap<>();
        AuditServerConfig        config = AuditServerConfig.getInstance();

        for (Map.Entry<String, String> entry : config) {
            String key = entry.getKey();

            if (key.startsWith(PROP_PREFIX_AUDIT_SERVER_SERVICE) && key.endsWith(PROP_SUFFIX_ALLOWED_USERS)) {
                String serviceName = key.substring(PROP_PREFIX_AUDIT_SERVER_SERVICE.length(), key.length() - PROP_SUFFIX_ALLOWED_USERS.length());

                if (StringUtils.isNotBlank(serviceName)) {
                    Set<String> allowedUsers = Arrays.stream(entry.getValue().split(",")).map(String::trim).filter(StringUtils::isNotBlank).collect(Collectors.toSet());

                    if (!allowedUsers.isEmpty()) {
                        ret.put(serviceName, allowedUsers);
                    }

                    LOG.debug("Allowed users for service {}: {}", serviceName, allowedUsers);
                }
            }
        }

        return ret;
    }
}
