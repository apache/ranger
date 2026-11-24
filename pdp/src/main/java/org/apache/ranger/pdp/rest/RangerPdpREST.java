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

package org.apache.ranger.pdp.rest;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.ranger.admin.client.RangerAdminRESTClient;
import org.apache.ranger.authz.api.RangerAuthorizer;
import org.apache.ranger.authz.api.RangerAuthzException;
import org.apache.ranger.authz.embedded.RangerAuthzConfig;
import org.apache.ranger.authz.model.RangerAccessInfo;
import org.apache.ranger.authz.model.RangerAuthzRequest;
import org.apache.ranger.authz.model.RangerAuthzResult;
import org.apache.ranger.authz.model.RangerFilterResourcesRequest;
import org.apache.ranger.authz.model.RangerFilterResourcesResult;
import org.apache.ranger.authz.model.RangerMultiAuthzRequest;
import org.apache.ranger.authz.model.RangerMultiAuthzResult;
import org.apache.ranger.authz.model.RangerResourceInfo;
import org.apache.ranger.authz.model.RangerResourcePermissions;
import org.apache.ranger.authz.model.RangerResourcePermissionsRequest;
import org.apache.ranger.authz.model.RangerUserInfo;
import org.apache.ranger.pdp.RangerPdpStats;
import org.apache.ranger.pdp.config.RangerPdpConfig;
import org.apache.ranger.pdp.config.RangerPdpConstants;
import org.apache.ranger.pdp.model.ErrorResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.inject.Singleton;
import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.stream.Collectors;

import static javax.ws.rs.core.Response.Status.BAD_REQUEST;
import static javax.ws.rs.core.Response.Status.FORBIDDEN;
import static javax.ws.rs.core.Response.Status.INTERNAL_SERVER_ERROR;
import static javax.ws.rs.core.Response.Status.OK;
import static javax.ws.rs.core.Response.Status.UNAUTHORIZED;
import static org.apache.ranger.authz.api.RangerAuthzApiErrorCode.INVALID_REQUEST_USER_INFO_MISSING;
import static org.apache.ranger.pdp.config.RangerPdpConstants.PROP_PDP_SERVICE_PREFIX;
import static org.apache.ranger.pdp.config.RangerPdpConstants.PROP_SERVICE_ACCOUNT_REFRESHER_ENABLED;
import static org.apache.ranger.pdp.config.RangerPdpConstants.PROP_SUFFIX_DELEGATION_USERS;
import static org.apache.ranger.pdp.config.RangerPdpConstants.WILDCARD_SERVICE_NAME;

/**
 * REST resource that exposes the three core {@link RangerAuthorizer} methods over HTTP.
 *
 * <p>All endpoints are under {@code /authz/v1} and produce/consume {@code application/json}.
 * Authentication is enforced upstream by {@link org.apache.ranger.pdp.security.RangerPdpAuthNFilter}; the authenticated
 * caller's identity is read from the {@link RangerPdpConstants#ATTR_AUTHENTICATED_USER}
 * request attribute.
 *
 * <table border="1">
 *   <tr><th>Method</th><th>Path</th><th>Request body</th><th>Response body</th></tr>
 *   <tr><td>POST</td><td>/authz/v1/authorize</td>
 *       <td>{@link RangerAuthzRequest}</td><td>{@link RangerAuthzResult}</td></tr>
 *   <tr><td>POST</td><td>/authz/v1/authorizeMulti</td>
 *       <td>{@link RangerMultiAuthzRequest}</td><td>{@link RangerMultiAuthzResult}</td></tr>
 *   <tr><td>POST</td><td>/authz/v1/permissions</td>
 *       <td>{@link RangerResourcePermissionsRequest}</td>
 *       <td>{@link RangerResourcePermissions}</td></tr>
 * </table>
 */
@Path("/v1")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
@Singleton
public class RangerPdpREST {
    private static final Logger LOG = LoggerFactory.getLogger(RangerPdpREST.class);

    private static final Response RESPONSE_OK = Response.ok().build();

    private static final String PROP_POLICY_REST_URL         = RangerAuthzConfig.PROP_PREFIX_DEFAULT + "policy.rest.url";
    private static final String PROP_POLICY_POLL_INTERVAL_MS = RangerAuthzConfig.PROP_PREFIX_DEFAULT + "policy.pollIntervalMs";
    private static final long   DEFAULT_POLL_INTERVAL_MS     = 30 * 1000L;

    private final Map<String, Set<String>> configuredDelegationUsers = new HashMap<>();

    private volatile Set<String>              wildcardServiceUsers     = Collections.emptySet();
    private volatile Map<String, Set<String>> delegationUsersByService = Collections.emptyMap();
    private          RangerAdminRESTClient    rangerAdminClient;
    private          Timer                    serviceAccountUsersRefreshTimer;

    @Inject
    private RangerAuthorizer authorizer;

    @Inject
    private RangerPdpConfig config;

    @Context
    private ServletContext servletContext;

    @PostConstruct
    public void initialize() {
        initializeDelegationUsersOnly();
        startServiceAccountUsersRefresh();
    }

    @PreDestroy
    public void destroy() {
        stopServiceAccountUsersRefresh();
    }

    public synchronized void startServiceAccountUsersRefresh() {
        Properties properties = config != null ? config.getAuthzProperties() : new Properties();
        startServiceAccountUsersRefresh(properties);
    }

    public synchronized void stopServiceAccountUsersRefresh() {
        Timer timer = serviceAccountUsersRefreshTimer;
        serviceAccountUsersRefreshTimer = null;
        if (timer != null) {
            timer.cancel();
        }
    }

    /**
     * Evaluates a single access request.
     *
     * @param request the authorization request
     * @return {@code 200 OK} with {@link RangerAuthzResult}, or {@code 400} / {@code 500} on error
     */
    @POST
    @Path("/authorize")
    public Response authorize(RangerAuthzRequest request, @Context HttpServletRequest httpRequest) {
        long     startNanos = System.nanoTime();
        Response ret        = null;

        try {
            String           requestId   = request != null ? request.getRequestId() : null;
            String           caller      = getAuthenticatedUser(httpRequest);
            String           serviceName = getServiceName(request);
            RangerUserInfo   user        = request != null ? request.getUser() : null;
            RangerAccessInfo access      = request != null ? request.getAccess() : null;

            LOG.debug("==> authorize(requestId={}, caller={}, serviceName={})", requestId, caller, serviceName);

            ret = validateCaller(caller, user, access, serviceName);

            if (isStatusOk(ret)) {
                try {
                    RangerAuthzResult result = authorizer.authorize(request);

                    ret = Response.ok(result).build();
                } catch (RangerAuthzException e) {
                    LOG.warn("authorize(requestId={}): authorization error; caller={}", requestId, caller, e);

                    ret = badRequest(e);
                } catch (Exception e) {
                    LOG.error("authorize(requestId={}): internal error; caller={}", requestId, caller, e);

                    ret = serverError();
                }
            }

            LOG.debug("<== authorize(requestId={}, caller={}, serviceName={}): ret={}", requestId, caller, serviceName, ret != null ? ret.getStatus() : null);
        } finally {
            recordRequestMetrics(ret, startNanos, httpRequest);
        }

        return ret;
    }

    /**
     * Evaluates multiple access requests in a single call.
     *
     * @param request the multi-access authorization request
     * @return {@code 200 OK} with {@link RangerMultiAuthzResult}, or {@code 400} / {@code 500} on error
     */
    @POST
    @Path("/authorizeMulti")
    public Response authorizeMulti(RangerMultiAuthzRequest request, @Context HttpServletRequest httpRequest) {
        long     startNanos = System.nanoTime();
        Response ret        = null;

        try {
            String                 requestId   = request != null ? request.getRequestId() : null;
            String                 caller      = getAuthenticatedUser(httpRequest);
            String                 serviceName = getServiceName(request);
            RangerUserInfo         user        = request != null ? request.getUser() : null;
            List<RangerAccessInfo> accesses    = request != null ? request.getAccesses() : null;

            LOG.debug("==> authorizeMulti(requestId={}, caller={}, serviceName={})", requestId, caller, serviceName);

            ret = validateCaller(caller, user, accesses, serviceName);

            if (isStatusOk(ret)) {
                try {
                    RangerMultiAuthzResult result = authorizer.authorize(request);

                    ret = Response.ok(result).build();
                } catch (RangerAuthzException e) {
                    LOG.warn("authorizeMulti(requestId={}): authorization error; caller={}", requestId, caller, e);

                    ret = badRequest(e);
                } catch (Exception e) {
                    LOG.error("authorizeMulti(requestId={}): internal error; caller={}", requestId, caller, e);

                    ret = serverError();
                }
            }

            LOG.debug("<== authorizeMulti(requestId={}, caller={}, serviceName={}): ret={}", requestId, caller, serviceName, ret != null ? ret.getStatus() : null);
        } finally {
            recordRequestMetrics(ret, startNanos, httpRequest);
        }

        return ret;
    }

    /**
     * Returns the effective permissions for a resource, broken down by user/group/role.
     *
     * @param request wrapper containing the resource info and access context
     * @return {@code 200 OK} with {@link RangerResourcePermissions}, or {@code 400} / {@code 500} on error
     */
    @POST
    @Path("/permissions")
    public Response getResourcePermissions(RangerResourcePermissionsRequest request, @Context HttpServletRequest httpRequest) {
        long     startNanos = System.nanoTime();
        Response ret        = null;

        try {
            String caller      = getAuthenticatedUser(httpRequest);
            String serviceName = getServiceName(request);

            LOG.debug("==> getResourcePermissions(caller={}, serviceName={})", caller, serviceName);

            ret = validateCaller(caller, serviceName);

            if (isStatusOk(ret)) {
                try {
                    RangerResourcePermissions result = authorizer.getResourcePermissions(request);

                    ret = Response.ok(result).build();
                } catch (RangerAuthzException e) {
                    LOG.warn("getResourcePermissions(): validation error; caller={}", caller, e);

                    ret = badRequest(e);
                } catch (Exception e) {
                    LOG.error("getResourcePermissions(): unexpected error; caller={}", caller, e);

                    ret = serverError();
                }
            }

            LOG.debug("<== getResourcePermissions(caller={}, serviceName={}): ret={}", caller, serviceName, ret != null ? ret.getStatus() : null);
        } finally {
            recordRequestMetrics(ret, startNanos, httpRequest);
        }

        return ret;
    }

    /**
     * Returns the effective permissions for a resource, broken down by user/group/role.
     *
     * @param request wrapper containing the resource info and access context
     * @return {@code 200 OK} with {@link RangerResourcePermissions}, or {@code 400} / {@code 500} on error
     */
    @POST
    @Path("/filterResources")
    public Response filterResources(RangerFilterResourcesRequest request, @Context HttpServletRequest httpRequest) {
        long     startNanos = System.nanoTime();
        Response ret        = null;

        try {
            String caller      = getAuthenticatedUser(httpRequest);
            String serviceName = getServiceName(request);

            LOG.debug("==> filterResources(caller={}, serviceName={})", caller, serviceName);

            ret = validateCaller(caller, serviceName);

            if (isStatusOk(ret)) {
                try {
                    RangerFilterResourcesResult result = authorizer.filterResources(request);

                    ret = Response.ok(result).build();
                } catch (RangerAuthzException e) {
                    LOG.warn("filterResources(): validation error; caller={}", caller, e);

                    ret = badRequest(e);
                } catch (Exception e) {
                    LOG.error("filterResources(): unexpected error; caller={}", caller, e);

                    ret = serverError();
                }
            }

            LOG.debug("<== filterResources(caller={}, serviceName={}): ret={}", caller, serviceName, ret != null ? ret.getStatus() : null);
        } finally {
            recordRequestMetrics(ret, startNanos, httpRequest);
        }

        return ret;
    }

    private String getAuthenticatedUser(HttpServletRequest httpRequest) {
        Object user = httpRequest.getAttribute(RangerPdpConstants.ATTR_AUTHENTICATED_USER);

        return user != null ? user.toString() : null;
    }

    private static String getServiceName(RangerAuthzRequest request) {
        return request != null && request.getContext() != null ? request.getContext().getServiceName() : null;
    }

    private static String getServiceName(RangerMultiAuthzRequest request) {
        return request != null && request.getContext() != null ? request.getContext().getServiceName() : null;
    }

    private static String getServiceName(RangerResourcePermissionsRequest request) {
        return request != null && request.getContext() != null ? request.getContext().getServiceName() : null;
    }

    private static String getServiceName(RangerFilterResourcesRequest request) {
        return request != null && request.getContext() != null ? request.getContext().getServiceName() : null;
    }

    private Response validateCaller(String caller, RangerUserInfo user, RangerAccessInfo access, String serviceName) {
        final Response ret;

        if (StringUtils.isBlank(caller)) {
            ret = Response.status(UNAUTHORIZED)
                    .entity(new ErrorResponse(UNAUTHORIZED, "Authentication required"))
                    .build();
        } else if (user == null || StringUtils.isBlank(user.getName())) {
            ret = Response.status(BAD_REQUEST)
                    .entity(new ErrorResponse(BAD_REQUEST, INVALID_REQUEST_USER_INFO_MISSING.getMessage()))
                    .build();
        } else {
            boolean needsDelegation = isDelegationNeeded(caller, user) || isDelegationNeeded(access);

            if (needsDelegation) {
                if (!isDelegationUserForService(serviceName, caller)) {
                    LOG.info("{} is not a delegation user in service {}", caller, serviceName);

                    ret = Response.status(FORBIDDEN)
                            .entity(new ErrorResponse(FORBIDDEN, caller + " is not authorized"))
                            .build();
                } else {
                    ret = RESPONSE_OK;
                }
            } else {
                ret = RESPONSE_OK;
            }
        }

        return ret;
    }

    private Response validateCaller(String caller, RangerUserInfo user, List<RangerAccessInfo> accesses, String serviceName) {
        final Response ret;

        if (StringUtils.isBlank(caller)) {
            ret = Response.status(UNAUTHORIZED)
                    .entity(new ErrorResponse(UNAUTHORIZED, "Authentication required"))
                    .build();
        } else if (user == null || StringUtils.isBlank(user.getName())) {
            ret = Response.status(BAD_REQUEST)
                    .entity(new ErrorResponse(BAD_REQUEST, INVALID_REQUEST_USER_INFO_MISSING.getMessage()))
                    .build();
        } else {
            boolean needsDelegation = isDelegationNeeded(caller, user) || isDelegationNeeded(accesses);

            if (needsDelegation) {
                if (!isDelegationUserForService(serviceName, caller)) {
                    LOG.info("{} is not a delegation user in service {}", caller, serviceName);

                    ret = Response.status(FORBIDDEN)
                            .entity(new ErrorResponse(FORBIDDEN, caller + " is not authorized"))
                            .build();
                } else {
                    ret = RESPONSE_OK;
                }
            } else {
                ret = RESPONSE_OK;
            }
        }

        return ret;
    }

    private Response validateCaller(String caller, String serviceName) {
        final Response ret;

        if (StringUtils.isBlank(caller)) {
            ret = Response.status(UNAUTHORIZED)
                    .entity(new ErrorResponse(UNAUTHORIZED, "Authentication required"))
                    .build();
        } else if (!isDelegationUserForService(serviceName, caller)) {
            LOG.info("{} is not a delegation user in service {}", caller, serviceName);

            ret = Response.status(FORBIDDEN)
                    .entity(new ErrorResponse(FORBIDDEN, caller + " is not authorized"))
                    .build();
        } else {
            ret = RESPONSE_OK;
        }

        return ret;
    }

    private boolean isDelegationNeeded(String caller, RangerUserInfo user) {
        String  userName        = user.getName();
        boolean needsDelegation = !caller.equals(userName);

        if (!needsDelegation) {
            // don't trust user-attributes/groups/roles if caller doesn't have delegation permission
            needsDelegation = MapUtils.isNotEmpty(user.getAttributes()) || CollectionUtils.isNotEmpty(user.getGroups()) || CollectionUtils.isNotEmpty(user.getRoles());
        }

        return needsDelegation;
    }

    private boolean isDelegationNeeded(RangerAccessInfo access) {
        RangerResourceInfo resource = access != null ? access.getResource() : null;

        // delegation permission is needed when resource attributes are specified
        return (resource != null && MapUtils.isNotEmpty(resource.getAttributes()));
    }

    private boolean isDelegationNeeded(List<RangerAccessInfo> accesses) {
        if (accesses != null) {
            for (RangerAccessInfo access : accesses) {
                if (isDelegationNeeded(access)) {
                    return true;
                }
            }
        }

        return false;
    }

    private RangerPdpStats getRuntimeState(HttpServletRequest httpRequest) {
        Object state = httpRequest.getServletContext().getAttribute(RangerPdpConstants.SERVLET_CTX_ATTR_RUNTIME_STATE);

        if (state instanceof RangerPdpStats) {
            return (RangerPdpStats) state;
        } else {
            Object fallback = servletContext != null ? servletContext.getAttribute(RangerPdpConstants.SERVLET_CTX_ATTR_RUNTIME_STATE) : null;

            return (fallback instanceof RangerPdpStats) ? (RangerPdpStats) fallback : new RangerPdpStats();
        }
    }

    private void recordRequestMetrics(Response ret, long startNanos, HttpServletRequest httpRequest) {
        RangerPdpStats state   = getRuntimeState(httpRequest);
        int            status  = ret != null ? ret.getStatus() : 500;
        long           elapsed = System.nanoTime() - startNanos;

        if (status >= 200 && status < 300) {
            state.recordRequestSuccess(elapsed);
        } else if (status == 401 || status == 403) { // UNAUTHORIZED or FORBIDDEN
            state.recordAuthFailure(elapsed);
        } else if (status == 400) {
            state.recordRequestBadRequest(elapsed);
        } else {
            state.recordRequestError(elapsed);
        }
    }

    private boolean isDelegationUserForService(String serviceName, String userName) {
        final boolean ret;

        if (StringUtils.isBlank(serviceName) || StringUtils.isBlank(userName)) {
            ret = false;
        } else if (wildcardServiceUsers.contains(userName)) {
            ret = true;
        } else {
            Set<String> delegationUsers = delegationUsersByService.get(serviceName);

            ret = delegationUsers != null && delegationUsers.contains(userName);
        }

        LOG.debug("isDelegationUserForService(serviceName={}, userName={}): ret={}", serviceName, userName, ret);

        return ret;
    }

    private void initializeDelegationUsersOnly() {
        Properties properties = config != null ? config.getAuthzProperties() : new Properties();

        for (String key : properties.stringPropertyNames()) {
            if (key.startsWith(PROP_PDP_SERVICE_PREFIX) && key.endsWith(PROP_SUFFIX_DELEGATION_USERS)) {
                String serviceName = key.substring(PROP_PDP_SERVICE_PREFIX.length(), key.length() - PROP_SUFFIX_DELEGATION_USERS.length());

                if (StringUtils.isBlank(serviceName)) {
                    continue;
                }

                Set<String> delegationUsers = Arrays.stream(StringUtils.defaultString(properties.getProperty(key)).split(","))
                                                 .map(String::trim)
                                                 .filter(StringUtils::isNotBlank)
                                                 .collect(Collectors.toSet());

                if (!delegationUsers.isEmpty()) {
                    configuredDelegationUsers.put(serviceName, delegationUsers);

                    LOG.info("Delegation users for service '{}': {}", serviceName, delegationUsers);
                }
            }
        }

        delegationUsersByService = mergeDelegationUsers(configuredDelegationUsers, Collections.emptyMap());

        if (configuredDelegationUsers.isEmpty()) {
            LOG.warn("No delegation users configured");
        }
    }

    void refreshDelegationUsers() {
        RangerAdminRESTClient currentRangerAdminClient = rangerAdminClient;

        if (currentRangerAdminClient != null) {
            try {
                delegationUsersByService = mergeDelegationUsers(configuredDelegationUsers, currentRangerAdminClient.getServiceAccountUsers());
            } catch (Exception e) {
                LOG.warn("Failed to refresh service account users from Ranger Admin", e);
            }
        } else {
            LOG.warn("Failed to refresh service account users from Ranger admin: admin client not initialized");
        }
    }

    Map<String, Set<String>> mergeDelegationUsers(Map<String, Set<String>> configuredDelegationUsers, Map<String, Set<String>> serviceAccountUsersByServiceName) {
        Map<String, Set<String>> ret = new HashMap<>();

        for (Map.Entry<String, Set<String>> entry : configuredDelegationUsers.entrySet()) {
            ret.put(entry.getKey(), new HashSet<>(entry.getValue()));
        }

        for (Map.Entry<String, Set<String>> entry : serviceAccountUsersByServiceName.entrySet()) {
            String      serviceName         = entry.getKey();
            Set<String> serviceAccountUsers = entry.getValue();

            if (StringUtils.isNotBlank(serviceName) && serviceAccountUsers != null && !serviceAccountUsers.isEmpty()) {
                Set<String> delegationUsers = ret.computeIfAbsent(serviceName, k -> new HashSet<>());

                delegationUsers.addAll(serviceAccountUsers);
            }
        }

        Set<String> wildcardServiceUsers = ret.get(WILDCARD_SERVICE_NAME);

        this.wildcardServiceUsers = (wildcardServiceUsers != null) ? wildcardServiceUsers : Collections.emptySet();

        return ret;
    }

    private void startServiceAccountUsersRefresh(Properties properties) {
        if (!Boolean.parseBoolean(StringUtils.defaultString(properties.getProperty(PROP_SERVICE_ACCOUNT_REFRESHER_ENABLED), "false"))) {
            LOG.info("service account user refresh is not enabled. Property: {}", PROP_SERVICE_ACCOUNT_REFRESHER_ENABLED);
            return;
        }

        LOG.info("Starting service account user refresher");

        String rangerAdminUrl = properties.getProperty(PROP_POLICY_REST_URL);

        if (serviceAccountUsersRefreshTimer == null && StringUtils.isNotBlank(rangerAdminUrl)) {
            try {
                Configuration         rangerAdminConfig = new Configuration(false);
                RangerAdminRESTClient client            = new RangerAdminRESTClient();

                for (String key : properties.stringPropertyNames()) {
                    String value = properties.getProperty(key);

                    if (value != null) {
                        rangerAdminConfig.set(key, value);
                    }
                }

                client.init("ranger-pdp", "ranger-pdp", StringUtils.removeEnd(RangerAuthzConfig.PROP_PREFIX_DEFAULT, "."), rangerAdminConfig);

                rangerAdminClient = client;

                long  pollIntervalMs = rangerAdminConfig.getLong(PROP_POLICY_POLL_INTERVAL_MS, DEFAULT_POLL_INTERVAL_MS);
                Timer timer          = new Timer("pdpServiceAccountUsersRefresh", true);

                refreshDelegationUsers();

                timer.schedule(new TimerTask() {
                    @Override
                    public void run() {
                        refreshDelegationUsers();
                    }
                }, pollIntervalMs, pollIntervalMs);

                serviceAccountUsersRefreshTimer = timer;

                LOG.info("Scheduled service account user refresh every {} ms from {}", pollIntervalMs, rangerAdminUrl);
            } catch (Exception e) {
                LOG.warn("Failed to start service account user refresh", e);
            }
        }
    }

    private Response badRequest(RangerAuthzException e) {
        return Response.status(BAD_REQUEST)
                       .entity(new ErrorResponse(BAD_REQUEST, e.getMessage()))
                       .build();
    }

    private Response serverError() {
        return Response.serverError()
                       .entity(new ErrorResponse(INTERNAL_SERVER_ERROR, "Internal Server Error"))
                       .build();
    }

    private boolean isStatusOk(Response resp) {
        return resp != null && resp.getStatus() == OK.getStatusCode();
    }
}
