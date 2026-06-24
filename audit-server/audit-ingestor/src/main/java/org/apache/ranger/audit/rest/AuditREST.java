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
import org.apache.ranger.audit.producer.kafka.partition.AuthToLocalRuleComposer;
import org.apache.ranger.audit.producer.kafka.partition.PartitionPlanHolder;
import org.apache.ranger.audit.producer.kafka.partition.PartitionPlanKafkaConfig;
import org.apache.ranger.audit.producer.kafka.partition.PartitionPlanService;
import org.apache.ranger.audit.producer.kafka.partition.ServiceAllowlistResolver;
import org.apache.ranger.audit.producer.kafka.partition.exception.PartitionPlanConflictException;
import org.apache.ranger.audit.producer.kafka.partition.exception.PartitionPlanException;
import org.apache.ranger.audit.producer.kafka.partition.model.OnboardPlugin;
import org.apache.ranger.audit.producer.kafka.partition.model.PartitionPlan;
import org.apache.ranger.audit.producer.kafka.partition.model.UpdatePlugin;
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
import javax.ws.rs.PATCH;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
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

    @Autowired
    PartitionPlanService partitionPlanService;

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
     * Access Audits producer endpoint.
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

    /** Returns the in-memory partition plan when dynamic mode is enabled. */
    @GET
    @Path("/partition-plan")
    @Produces("application/json")
    public Response getPartitionPlan(@Context HttpServletRequest httpRequest) {
        LOG.debug("==> AuditREST.getPartitionPlan()");
        Response ret;
        if (!partitionPlanService.isDynamicPartitionPlanEnabled()) {
            ret = partitionPlanDisabled("GET /partition-plan");
        } else {
            Response authFailure = authorizePartitionPlanAdmin(httpRequest, "GET /partition-plan");
            if (authFailure != null) {
                ret = authFailure;
            } else {
                try {
                    ret = Response.ok(partitionPlanService.getPartitionPlan().toJson()).build();
                } catch (PartitionPlanException e) {
                    LOG.error("Partition plan GET failed", e);
                    ret = Response.status(Response.Status.SERVICE_UNAVAILABLE).entity(buildErrorResponse(e.getMessage())).build();
                }
            }
        }
        LOG.debug("<== AuditREST.getPartitionPlan(): status={}", ret.getStatus());
        return ret;
    }

    /** Onboards a plugin from the buffer and registers service allowlists. */
    @POST
    @Path("/partition-plan/plugins")
    @Consumes("application/json")
    @Produces("application/json")
    public Response onboardPlugin(OnboardPlugin request, @Context HttpServletRequest httpRequest) {
        LOG.debug("==> AuditREST.onboardPlugin(pluginId={})", request != null ? request.getPluginId() : null);
        Response ret;
        if (!partitionPlanService.isDynamicPartitionPlanEnabled()) {
            ret = partitionPlanDisabled("POST /partition-plan/plugins");
        } else {
            Response authFailure = authorizePartitionPlanAdmin(httpRequest, "POST /partition-plan/plugins");
            if (authFailure != null) {
                ret = authFailure;
            } else {
                try {
                    ret = toSuccessfulPartitionPlanResponse(partitionPlanService.onboardPlugin(request, resolveUpdatedBy(httpRequest)));
                } catch (PartitionPlanConflictException e) {
                    ret = toPartitionPlanConflictResponse("POST /partition-plan/plugins", e);
                } catch (PartitionPlanException e) {
                    ret = toPartitionPlanErrorResponse("POST /partition-plan/plugins", e);
                } catch (Exception e) {
                    LOG.error("Unexpected error onboarding plugin in partition plan", e);
                    ret = Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(buildErrorResponse("Failed to onboard plugin in partition plan")).build();
                }
            }
        }
        LOG.debug("<== AuditREST.onboardPlugin(): status={}", ret.getStatus());
        return ret;
    }

    /** Updates an onboarded plugin: scale partitions and/or mutate service allowlists. */
    @PATCH
    @Path("/partition-plan/plugins/{pluginId}")
    @Consumes("application/json")
    @Produces("application/json")
    public Response updatePlugin(@PathParam("pluginId") String pluginId, UpdatePlugin updateRequest, @Context HttpServletRequest httpRequest) {
        LOG.debug("==> AuditREST.updatePlugin(pluginId={})", pluginId);
        Response ret;
        if (!partitionPlanService.isDynamicPartitionPlanEnabled()) {
            ret = partitionPlanDisabled("PATCH /partition-plan/plugins/{pluginId}");
        } else {
            Response authFailure = authorizePartitionPlanAdmin(httpRequest, "PATCH /partition-plan/plugins/{pluginId}");
            if (authFailure != null) {
                ret = authFailure;
            } else {
                try {
                    ret = toSuccessfulPartitionPlanResponse(partitionPlanService.updatePlugin(pluginId, updateRequest, resolveUpdatedBy(httpRequest)));
                } catch (PartitionPlanConflictException e) {
                    ret = toPartitionPlanConflictResponse("PATCH /partition-plan/plugins/" + pluginId, e);
                } catch (PartitionPlanException e) {
                    ret = toPartitionPlanErrorResponse("PATCH /partition-plan/plugins/" + pluginId, e);
                } catch (Exception e) {
                    LOG.error("Unexpected error updating plugin in partition plan", e);
                    ret = Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(buildErrorResponse("Failed to update plugin in partition plan")).build();
                }
            }
        }
        LOG.debug("<== AuditREST.updatePlugin(): status={}", ret.getStatus());
        return ret;
    }

    /** Returns HTTP 200 with the updated plan JSON body. */
    private Response toSuccessfulPartitionPlanResponse(PartitionPlan updatedPlan) {
        return Response.ok(updatedPlan.toJson()).build();
    }

    /** Returns HTTP 409 with the current plan when optimistic locking fails. */
    private Response toPartitionPlanConflictResponse(String operation, PartitionPlanConflictException conflict) {
        PartitionPlan currentPlan = conflict.getCurrentPlan();
        if (currentPlan == null) {
            LOG.error("{} rejected: partition plan version conflict (current plan unavailable)", operation, conflict);
            return Response.status(Response.Status.CONFLICT).entity(buildErrorResponse("Partition plan version conflict")).build();
        }
        LOG.error("{} rejected: partition plan version conflict; current version is {}", operation, currentPlan.getVersion(), conflict);
        return Response.status(Response.Status.CONFLICT).entity(currentPlan.toJson()).build();
    }

    /** Returns HTTP 400 or 503 for validation and Kafka admin failures. */
    private Response toPartitionPlanErrorResponse(String operation, PartitionPlanException error) {
        Response.Status status = resolvePartitionPlanErrorStatus(error);
        LOG.error("{} failed: {}", operation, error.getMessage(), error);
        return Response.status(status).entity(buildErrorResponse(error.getMessage())).build();
    }

    /** Returns HTTP 503 when dynamic partition-plan mode is disabled. */
    private Response partitionPlanDisabled(String operation) {
        LOG.error("{} rejected: dynamic partition plan is not enabled", operation);
        return Response.status(Response.Status.SERVICE_UNAVAILABLE).entity(buildErrorResponse("Dynamic partition plan is not enabled")).build();
    }

    /**
     * When {@code kafka.partition.plan.allowed.users} is configured, restrict partition-plan REST to those short names.
     * When unset, any authenticated principal may call partition-plan (backward compatible).
     */
    private Response authorizePartitionPlanAdmin(HttpServletRequest request, String operation) {
        Response ret  = null;
        String   user = getAuthenticatedUser(request);

        if (StringUtils.isBlank(user)) {
            LOG.error("{} rejected: authentication required", operation);
            ret = Response.status(Response.Status.UNAUTHORIZED).entity(buildErrorResponse("Authentication required")).build();
        } else {
            Set<String> adminUsers = partitionPlanService.getPartitionPlanAdminUsers();

            if (!adminUsers.isEmpty() && !adminUsers.contains(user)) {
                LOG.error("{} rejected: user '{}' is not in partition plan admin allowlist", operation, user);
                ret = Response.status(Response.Status.FORBIDDEN).entity(buildErrorResponse("User is not authorized to manage partition plan")).build();
            }
        }

        return ret;
    }

    /** Maps service/infrastructure failures to 503; client validation mistakes to 400. */
    private static Response.Status resolvePartitionPlanErrorStatus(PartitionPlanException error) {
        Response.Status ret = Response.Status.BAD_REQUEST;

        if (error.getCause() != null) {
            ret = Response.Status.SERVICE_UNAVAILABLE;
        } else {
            String message = error.getMessage();

            if (message != null && (message.contains("Partition plan is not loaded in memory")
                    || message.contains("Partition plan disappeared during update")
                    || message.contains("No partition plan found in Kafka")
                    || message.contains("Mandatory read-back failed"))) {
                ret = Response.Status.SERVICE_UNAVAILABLE;
            }
        }

        return ret;
    }

    /** Records the authenticated admin user on plan mutations. */
    private String resolveUpdatedBy(HttpServletRequest request) {
        String user = getAuthenticatedUser(request);
        return StringUtils.isNotBlank(user) ? user : "rest-api";
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
        String ret = principal;

        if (StringUtils.isNotEmpty(principal)) {
            // Check if this looks like a Kerberos principal (has @ or /)
            if (!principal.contains("@") && !principal.contains("/")) {
                LOG.debug("Username '{}' is already a short name (JWT/basic auth), no auth_to_local mapping needed", principal);
            } else {
                try {
                    KerberosName kerberosName = new KerberosName(principal);

                    ret = kerberosName.getShortName();
                } catch (Exception e) {
                    LOG.warn("Failed to apply auth_to_local rules to principal '{}': {}. Using original principal.", principal, e.getMessage());
                }
            }
        }

        return ret;
    }

    /**
     * Loads the auth_to_local catalog from ranger-audit-ingestor-site.xml. When dynamic partition-plan
     * mode is disabled, applies the full catalog immediately. When enabled, applies static XML rules
     * only if the partition-plan Kafka topic does not exist yet; otherwise composed rules are installed
     * on {@link PartitionPlanHolder#install(PartitionPlan, Integer)} via {@link AuthToLocalRuleComposer#applyForPlan}.
     */
    private static void initializeAuthToLocal() {
        AuditServerConfig config = AuditServerConfig.getInstance();
        String authToLocalRules = config.get(AuditServerConstants.PROP_AUTH_TO_LOCAL);
        if (StringUtils.isEmpty(authToLocalRules)) {
            LOG.warn("No auth_to_local rules configured. Kerberos principal mapping may not work correctly.");
            LOG.warn("Set property '{}' in ranger-audit-ingestor-site.xml", AuditServerConstants.PROP_AUTH_TO_LOCAL);
            return;
        }

        AuthToLocalRuleComposer composer = AuthToLocalRuleComposer.getInstance();
        composer.initializeFromConfig();
        if (PartitionPlanKafkaConfig.isDynamicPartitionPlanEnabled(config.getProperties(), PartitionPlanService.INGESTOR_PROP_PREFIX)) {
            composer.applyStartupRulesForDynamicMode(config.getProperties(), PartitionPlanService.INGESTOR_PROP_PREFIX);
        } else {
            composer.applyStaticRules();
            LOG.debug("Applied static auth_to_local catalog from site XML");
        }
    }

    /**
     * Check if the user login into audit server for posting audits is in the allowed service users list
     * @param serviceName The name of service from which audit is being sent
     * @param userName The username to check
     * @return true if user is allowed, false otherwise
     */
    private boolean isAllowedServiceUser(String serviceName, String userName) {
        boolean dynamicEnabled = partitionPlanService != null
                && partitionPlanService.isDynamicPartitionPlanEnabled();
        boolean ret = ServiceAllowlistResolver.isAllowedServiceUser(
                serviceName,
                userName,
                dynamicEnabled,
                PartitionPlanHolder.getInstance(),
                allowedServiceUsers);
        LOG.debug(
                "isAllowedServiceUser(serviceName={}, userName={}, dynamic={}): ret={}",
                serviceName, userName, dynamicEnabled, ret);
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
