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

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.ranger.authz.api.RangerAuthorizer;
import org.apache.ranger.authz.api.RangerAuthzException;
import org.apache.ranger.authz.model.RangerAccessInfo;
import org.apache.ranger.authz.model.RangerAuthzRequest;
import org.apache.ranger.authz.model.RangerAuthzResult;
import org.apache.ranger.authz.model.RangerMultiAuthzRequest;
import org.apache.ranger.authz.model.RangerMultiAuthzResult;
import org.apache.ranger.authz.model.RangerResourceInfo;
import org.apache.ranger.authz.model.RangerResourcePermissions;
import org.apache.ranger.authz.model.RangerResourcePermissionsRequest;
import org.apache.ranger.authz.model.RangerUserInfo;
import org.apache.ranger.pdp.RangerPdpStats;
import org.apache.ranger.pdp.config.RangerPdpConfig;
import org.apache.ranger.pdp.config.RangerPdpConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PostConstruct;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.ranger.pdp.config.RangerPdpConstants.PROP_PDP_SERVICE_PREFIX;
import static org.apache.ranger.pdp.config.RangerPdpConstants.PROP_SUFFIX_DELEGATION_USERS;
import static org.apache.ranger.pdp.config.RangerPdpConstants.WILDCARD_SERVICE_NAME;

/**
 * REST resource that exposes the three core {@link RangerAuthorizer} methods over HTTP.
 *
 * <p>All endpoints are under {@code /authz/v1} and produce/consume {@code application/json}.
 * Authentication is enforced upstream by {@link RangerPdpAuthFilter}; the authenticated
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

    private final Map<String, Set<String>> delegationUsersByService = new HashMap<>();

    @Inject
    private RangerAuthorizer authorizer;

    @Inject
    private RangerPdpConfig config;

    @Context
    private ServletContext servletContext;

    @PostConstruct
    public void initialize() {
        initializeDelegationUsers();
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

            if (RESPONSE_OK.equals(ret)) {
                try {
                    RangerAuthzResult result = authorizer.authorize(request);

                    ret = Response.ok(result).build();
                } catch (RangerAuthzException e) {
                    LOG.warn("authorize(requestId={}): authorization error; caller={}", requestId, caller, e);

                    ret = badRequest(e);
                } catch (Exception e) {
                    LOG.error("authorize(requestId={}): internal error; caller={}", requestId, caller, e);

                    ret = serverError(e);
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

            if (RESPONSE_OK.equals(ret)) {
                try {
                    RangerMultiAuthzResult result = authorizer.authorize(request);

                    ret = Response.ok(result).build();
                } catch (RangerAuthzException e) {
                    LOG.warn("authorizeMulti(requestId={}): authorization error; caller={}", requestId, caller, e);

                    ret = badRequest(e);
                } catch (Exception e) {
                    LOG.error("authorizeMulti(requestId={}): internal error; caller={}", requestId, caller, e);

                    ret = serverError(e);
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

            if (RESPONSE_OK.equals(ret)) {
                try {
                    RangerResourcePermissions result = authorizer.getResourcePermissions(request);

                    ret = Response.ok(result).build();
                } catch (RangerAuthzException e) {
                    LOG.warn("getResourcePermissions(): validation error; caller={}", caller, e);

                    ret = badRequest(e);
                } catch (Exception e) {
                    LOG.error("getResourcePermissions(): unexpected error; caller={}", caller, e);

                    ret = serverError(e);
                }
            }

            LOG.debug("==> getResourcePermissions(caller={}, serviceName={}): ret={}", caller, serviceName, ret != null ? ret.getStatus() : null);
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

    private Response validateCaller(String caller, RangerUserInfo user, RangerAccessInfo access, String serviceName) {
        final Response ret;

        if (StringUtils.isBlank(caller)) {
            ret = Response.status(Response.Status.UNAUTHORIZED)
                    .entity(new ErrorResponse(Response.Status.UNAUTHORIZED, "Authentication required"))
                    .build();
        } else {
            boolean needsDelegation = isDelegationNeeded(caller, user) || isDelegationNeeded(access);

            if (needsDelegation) {
                if (!isDelegationUserForService(serviceName, caller)) {
                    LOG.info("{} is not a delegation user in service {}", caller, serviceName);

                    ret = Response.status(Response.Status.FORBIDDEN)
                            .entity(new ErrorResponse(Response.Status.FORBIDDEN, caller + " is not authorized"))
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
            ret = Response.status(Response.Status.UNAUTHORIZED)
                    .entity(new ErrorResponse(Response.Status.UNAUTHORIZED, "Authentication required"))
                    .build();
        } else {
            boolean needsDelegation = isDelegationNeeded(caller, user) || isDelegationNeeded(accesses);

            if (needsDelegation) {
                if (!isDelegationUserForService(serviceName, caller)) {
                    LOG.info("{} is not a delegation user in service {}", caller, serviceName);

                    ret = Response.status(Response.Status.FORBIDDEN)
                            .entity(new ErrorResponse(Response.Status.FORBIDDEN, caller + " is not authorized"))
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
            ret = Response.status(Response.Status.UNAUTHORIZED)
                    .entity(new ErrorResponse(Response.Status.UNAUTHORIZED, "Authentication required"))
                    .build();
        } else if (!isDelegationUserForService(serviceName, caller)) {
            LOG.info("{} is not a delegation user in service {}", caller, serviceName);

            ret = Response.status(Response.Status.FORBIDDEN)
                    .entity(new ErrorResponse(Response.Status.FORBIDDEN, caller + " is not authorized"))
                    .build();
        } else {
            ret = RESPONSE_OK;
        }

        return ret;
    }

    private boolean isDelegationNeeded(String caller, RangerUserInfo user) {
        String  userName        = user != null ? user.getName() : null;
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
        } if (status == 401 || status == 403) { // UNAUTHORIZED or FORBIDDEN
            state.recordAuthFailure();
        } else if (status == 400) {
            state.recordRequestBadRequest(elapsed);
        } else {
            state.recordRequestError(elapsed);
        }
    }

    /**
     * Allowed-users config format:
     *  ranger.pdp.service.<serviceName>.allowed.users=user1,user2
     *  ranger.pdp.service.*.allowed.users=user3,user4
     *
     * If no allowed-users entries are configured, this check is disabled
     * (backward-compatible behavior).
     */
    private boolean isDelegationUserForService(String serviceName, String userName) {
        boolean ret;
        Map<String, Set<String>> delegationUsersByService = this.delegationUsersByService;

        if (delegationUsersByService.isEmpty()) {
            ret = false;
        } else if (StringUtils.isBlank(serviceName) || StringUtils.isBlank(userName)) {
            ret = false;
        } else {
            Set<String> delegationUsers = delegationUsersByService.get(serviceName);

            if (delegationUsers == null) {
                delegationUsers = delegationUsersByService.get(RangerPdpConstants.WILDCARD_SERVICE_NAME);
            }

            ret = delegationUsers != null && delegationUsers.contains(userName);
        }

        LOG.debug("isDelegationUserForService(serviceName={}, userName={}): ret={}", serviceName, userName, ret);

        return ret;
    }

    private void initializeDelegationUsers() {
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
                    delegationUsersByService.put(serviceName, delegationUsers);

                    LOG.info("Delegation users for service '{}': {}", serviceName, delegationUsers);
                }
            }
        }

        if (delegationUsersByService.isEmpty()) {
            LOG.warn("No delegation users configured");
        } else {
            Set<String> wildcardDelegationUsers = delegationUsersByService.get(WILDCARD_SERVICE_NAME);

            // delegation users for WILDCARD_SERVICE_NAME are delegates for all services
            if (wildcardDelegationUsers != null && !wildcardDelegationUsers.isEmpty()) {
                delegationUsersByService.forEach((k, v) -> v.addAll(wildcardDelegationUsers));
            }
        }
    }

    private Response badRequest(RangerAuthzException e) {
        return Response.status(Response.Status.BAD_REQUEST)
                       .entity(new ErrorResponse(Response.Status.BAD_REQUEST, e.getMessage()))
                       .build();
    }

    private Response serverError(Exception e) {
        return Response.serverError()
                       .entity(new ErrorResponse(Response.Status.INTERNAL_SERVER_ERROR, e.getMessage()))
                       .build();
    }

    @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class ErrorResponse {
        private final String code;
        private final String message;

        public ErrorResponse(Response.Status status, String message) {
            this.code    = status.name();
            this.message = message;
        }

        public String getCode() {
            return code;
        }

        public String getMessage() {
            return message;
        }
    }

}
