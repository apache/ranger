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

package org.apache.ranger.admin.client;

import com.fasterxml.jackson.core.type.TypeReference;
import com.sun.jersey.api.client.ClientResponse;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.http.HttpStatus;
import org.apache.ranger.admin.client.datatype.RESTResponse;
import org.apache.ranger.audit.provider.MiscUtil;
import org.apache.ranger.authorization.hadoop.config.RangerPluginConfig;
import org.apache.ranger.authorization.utils.StringUtil;
import org.apache.ranger.plugin.model.RangerRole;
import org.apache.ranger.plugin.util.GrantRevokeRequest;
import org.apache.ranger.plugin.util.GrantRevokeRoleRequest;
import org.apache.ranger.plugin.util.JsonUtilsV2;
import org.apache.ranger.plugin.util.RangerCommonConstants;
import org.apache.ranger.plugin.util.RangerPluginCapability;
import org.apache.ranger.plugin.util.RangerRESTClient;
import org.apache.ranger.plugin.util.RangerRESTUtils;
import org.apache.ranger.plugin.util.RangerRoles;
import org.apache.ranger.plugin.util.RangerServiceNotFoundException;
import org.apache.ranger.plugin.util.RangerUserStore;
import org.apache.ranger.plugin.util.ServiceGdsInfo;
import org.apache.ranger.plugin.util.ServicePolicies;
import org.apache.ranger.plugin.util.ServiceTags;
import org.apache.ranger.plugin.util.URLEncoderUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.Cookie;
import javax.ws.rs.core.NewCookie;

import java.io.UnsupportedEncodingException;
import java.security.PrivilegedExceptionAction;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RangerAdminRESTClient extends AbstractRangerAdminClient {
    private static final Logger LOG = LoggerFactory.getLogger(RangerAdminRESTClient.class);

    private static final TypeReference<List<String>> TYPE_LIST_STRING = new TypeReference<List<String>>() {};

    private final String           pluginCapabilities = Long.toHexString(new RangerPluginCapability().getPluginCapabilities());
    private final RangerRESTUtils  restUtils          = new RangerRESTUtils();
    private       String           serviceName;
    private       String           serviceNameUrlParam;
    private       String           pluginId;
    private       String           clusterName;
    private       RangerRESTClient restClient;
    private       boolean          supportsPolicyDeltas;
    private       boolean          supportsTagDeltas;
    private       boolean          isRangerCookieEnabled;
    private       String           rangerAdminCookieName;
    private       Cookie           sessionId;

    @Override
    public void init(String serviceName, String appId, String propertyPrefix, Configuration config) {
        super.init(serviceName, appId, propertyPrefix, config);

        this.serviceName = serviceName;
        this.pluginId    = restUtils.getPluginId(serviceName, appId);

        String url               = "";
        String tmpUrl            = config.get(propertyPrefix + ".policy.rest.url");
        String sslConfigFileName = config.get(propertyPrefix + ".policy.rest.ssl.config.file");

        clusterName = config.get(propertyPrefix + ".access.cluster.name", "");

        if (StringUtil.isEmpty(clusterName)) {
            clusterName = config.get(propertyPrefix + ".ambari.cluster.name", "");

            if (StringUtil.isEmpty(clusterName)) {
                if (config instanceof RangerPluginConfig) {
                    clusterName = ((RangerPluginConfig) config).getClusterName();
                }
            }
        }

        int restClientConnTimeOutMs    = config.getInt(propertyPrefix + ".policy.rest.client.connection.timeoutMs", 120 * 1000);
        int restClientReadTimeOutMs    = config.getInt(propertyPrefix + ".policy.rest.client.read.timeoutMs", 30 * 1000);
        int restClientMaxRetryAttempts = config.getInt(propertyPrefix + ".policy.rest.client.max.retry.attempts", 3);
        int restClientRetryIntervalMs  = config.getInt(propertyPrefix + ".policy.rest.client.retry.interval.ms", 1 * 1000);

        supportsPolicyDeltas  = config.getBoolean(propertyPrefix + RangerCommonConstants.PLUGIN_CONFIG_SUFFIX_POLICY_DELTA, RangerCommonConstants.PLUGIN_CONFIG_SUFFIX_POLICY_DELTA_DEFAULT);
        supportsTagDeltas     = config.getBoolean(propertyPrefix + RangerCommonConstants.PLUGIN_CONFIG_SUFFIX_TAG_DELTA, RangerCommonConstants.PLUGIN_CONFIG_SUFFIX_TAG_DELTA_DEFAULT);
        isRangerCookieEnabled = config.getBoolean(propertyPrefix + ".policy.rest.client.cookie.enabled", RangerCommonConstants.POLICY_REST_CLIENT_SESSION_COOKIE_ENABLED);
        rangerAdminCookieName = config.get(propertyPrefix + ".policy.rest.client.session.cookie.name", RangerCommonConstants.DEFAULT_COOKIE_NAME);

        if (!StringUtil.isEmpty(tmpUrl)) {
            url = tmpUrl.trim();
        }

        if (url.endsWith("/")) {
            url = url.substring(0, url.length() - 1);
        }

        init(url, sslConfigFileName, restClientConnTimeOutMs, restClientReadTimeOutMs, restClientMaxRetryAttempts, restClientRetryIntervalMs, config);

        try {
            this.serviceNameUrlParam = URLEncoderUtil.encodeURIParam(serviceName);
        } catch (UnsupportedEncodingException e) {
            LOG.warn("Unsupported encoding, serviceName={}", serviceName);
            this.serviceNameUrlParam = serviceName;
        }
    }

    @Override
    public ServicePolicies getServicePoliciesIfUpdated(final long lastKnownVersion, final long lastActivationTimeInMillis) throws Exception {
        LOG.debug("==> RangerAdminRESTClient.getServicePoliciesIfUpdated({}, {})", lastKnownVersion, lastActivationTimeInMillis);

        final ServicePolicies      ret;
        final UserGroupInformation user         = MiscUtil.getUGILoginUser();
        final boolean              isSecureMode = isKerberosEnabled(user);
        final Cookie               sessionId    = this.sessionId;
        final Map<String, String>  queryParams  = new HashMap<>();

        queryParams.put(RangerRESTUtils.REST_PARAM_LAST_KNOWN_POLICY_VERSION, Long.toString(lastKnownVersion));
        queryParams.put(RangerRESTUtils.REST_PARAM_LAST_ACTIVATION_TIME, Long.toString(lastActivationTimeInMillis));
        queryParams.put(RangerRESTUtils.REST_PARAM_PLUGIN_ID, pluginId);
        queryParams.put(RangerRESTUtils.REST_PARAM_CLUSTER_NAME, clusterName);
        queryParams.put(RangerRESTUtils.REST_PARAM_SUPPORTS_POLICY_DELTAS, Boolean.toString(supportsPolicyDeltas));
        queryParams.put(RangerRESTUtils.REST_PARAM_CAPABILITIES, pluginCapabilities);

        final ClientResponse response;

        if (isSecureMode) {
            LOG.debug("Checking Service policy if updated as user : {}", user);

            response = MiscUtil.executePrivilegedAction((PrivilegedExceptionAction<ClientResponse>) () -> {
                try {
                    String relativeURL = RangerRESTUtils.REST_URL_POLICY_GET_FOR_SECURE_SERVICE_IF_UPDATED + serviceNameUrlParam;

                    return restClient.get(relativeURL, queryParams, sessionId);
                } catch (Exception e) {
                    LOG.error("Failed to get response, Error is : {}", e.getMessage());
                }

                return null;
            });
        } else {
            LOG.debug("Checking Service policy if updated with old api call");

            String relativeURL = RangerRESTUtils.REST_URL_POLICY_GET_FOR_SERVICE_IF_UPDATED + serviceNameUrlParam;

            response = restClient.get(relativeURL, queryParams, sessionId);
        }

        checkAndResetSessionCookie(response);

        if (response == null || response.getStatus() == HttpServletResponse.SC_NOT_MODIFIED || response.getStatus() == HttpServletResponse.SC_NO_CONTENT) {
            if (response == null) {
                LOG.error("Error getting policies; Received NULL response!!. secureMode={}, user={}, serviceName={}", isSecureMode, user, serviceName);
            } else {
                RESTResponse resp = RESTResponse.fromClientResponse(response);

                LOG.debug("No change in policies. secureMode={}, user={}, response={}, serviceName={}, lastKnownVersion={}, lastActivationTimeInMillis={}",
                        isSecureMode, user, resp, serviceName, lastKnownVersion, lastActivationTimeInMillis);
            }

            ret = null;
        } else if (response.getStatus() == HttpServletResponse.SC_OK) {
            ret = JsonUtilsV2.readResponse(response, ServicePolicies.class);
        } else if (response.getStatus() == HttpServletResponse.SC_NOT_FOUND) {
            ret = null;

            LOG.error("Error getting policies; service not found. secureMode={}, user={}, response={}, serviceName={}, lastKnownVersion={}, lastActivationTimeInMillis={}",
                    isSecureMode, user, response.getStatus(), serviceName, lastKnownVersion, lastActivationTimeInMillis);

            String exceptionMsg = response.hasEntity() ? response.getEntity(String.class) : null;

            RangerServiceNotFoundException.throwExceptionIfServiceNotFound(serviceName, exceptionMsg);

            LOG.warn("Received 404 error code with body:[{}], Ignoring", exceptionMsg);
        } else {
            RESTResponse resp = RESTResponse.fromClientResponse(response);

            LOG.warn("Error getting policies. secureMode={}, user={}, response={}, serviceName={}", isSecureMode, user, resp, serviceName);

            ret = null;
        }

        LOG.debug("<== RangerAdminRESTClient.getServicePoliciesIfUpdated({}, {}): {}", lastKnownVersion, lastActivationTimeInMillis, ret);

        return ret;
    }

    @Override
    public RangerRoles getRolesIfUpdated(final long lastKnownRoleVersion, final long lastActivationTimeInMillis) throws Exception {
        LOG.debug("==> RangerAdminRESTClient.getRolesIfUpdated({}, {})", lastKnownRoleVersion, lastActivationTimeInMillis);

        final RangerRoles          ret;
        final UserGroupInformation user         = MiscUtil.getUGILoginUser();
        final boolean              isSecureMode = isKerberosEnabled(user);
        final Cookie               sessionId    = this.sessionId;
        final Map<String, String>  queryParams  = new HashMap<>();

        queryParams.put(RangerRESTUtils.REST_PARAM_LAST_KNOWN_ROLE_VERSION, Long.toString(lastKnownRoleVersion));
        queryParams.put(RangerRESTUtils.REST_PARAM_LAST_ACTIVATION_TIME, Long.toString(lastActivationTimeInMillis));
        queryParams.put(RangerRESTUtils.REST_PARAM_PLUGIN_ID, pluginId);
        queryParams.put(RangerRESTUtils.REST_PARAM_CLUSTER_NAME, clusterName);
        queryParams.put(RangerRESTUtils.REST_PARAM_CAPABILITIES, pluginCapabilities);

        final ClientResponse response;

        if (isSecureMode) {
            LOG.debug("Checking Roles updated as user : {}", user);

            response = MiscUtil.executePrivilegedAction((PrivilegedExceptionAction<ClientResponse>) () -> {
                try {
                    String relativeURL = RangerRESTUtils.REST_URL_SERVICE_SERCURE_GET_USER_GROUP_ROLES + serviceNameUrlParam;

                    return restClient.get(relativeURL, queryParams, sessionId);
                } catch (Exception e) {
                    LOG.error("Failed to get response, Error is : {}", e.getMessage());
                }

                return null;
            });
        } else {
            LOG.debug("Checking Roles updated as user : {}", user);

            String relativeURL = RangerRESTUtils.REST_URL_SERVICE_GET_USER_GROUP_ROLES + serviceNameUrlParam;

            response = restClient.get(relativeURL, queryParams, sessionId);
        }

        checkAndResetSessionCookie(response);

        if (response == null || response.getStatus() == HttpServletResponse.SC_NOT_MODIFIED || response.getStatus() == HttpServletResponse.SC_NO_CONTENT) {
            if (response == null) {
                LOG.error("Error getting Roles; Received NULL response!!. secureMode={}, user={}, serviceName={}", isSecureMode, user, serviceName);
            } else {
                RESTResponse resp = RESTResponse.fromClientResponse(response);

                LOG.debug("No change in Roles. secureMode={}, user={}, response={}, serviceName={}, lastKnownRoleVersion={}, lastActivationTimeInMillis={}",
                        isSecureMode, user, resp, serviceName, lastKnownRoleVersion, lastActivationTimeInMillis);
            }

            ret = null;
        } else if (response.getStatus() == HttpServletResponse.SC_OK) {
            ret = JsonUtilsV2.readResponse(response, RangerRoles.class);
        } else if (response.getStatus() == HttpServletResponse.SC_NOT_FOUND) {
            ret = null;

            LOG.error("Error getting Roles; service not found. secureMode={}, user={}, response={}, serviceName={}, lastKnownRoleVersion={}, lastActivationTimeInMillis={}",
                    isSecureMode, user, response.getStatus(), serviceName, lastKnownRoleVersion, lastActivationTimeInMillis);

            String exceptionMsg = response.hasEntity() ? response.getEntity(String.class) : null;

            RangerServiceNotFoundException.throwExceptionIfServiceNotFound(serviceName, exceptionMsg);

            LOG.warn("Received 404 error code with body:[{}], Ignoring", exceptionMsg);
        } else {
            RESTResponse resp = RESTResponse.fromClientResponse(response);

            LOG.warn("Error getting Roles. secureMode={}, user={}, response={}, serviceName={}", isSecureMode, user, resp, serviceName);

            ret = null;
        }

        LOG.debug("<== RangerAdminRESTClient.getRolesIfUpdated({}, {}): ", lastKnownRoleVersion, lastActivationTimeInMillis);

        return ret;
    }

    @Override
    public RangerRole createRole(final RangerRole request) throws Exception {
        LOG.debug("==> RangerAdminRESTClient.createRole({})", request);

        final RangerRole           ret;
        final UserGroupInformation user         = MiscUtil.getUGILoginUser();
        final boolean              isSecureMode = isKerberosEnabled(user);
        final String               relativeURL  = RangerRESTUtils.REST_URL_SERVICE_CREATE_ROLE;
        final Cookie               sessionId    = this.sessionId;
        final Map<String, String>  queryParams  = new HashMap<>();

        queryParams.put(RangerRESTUtils.SERVICE_NAME_PARAM, serviceNameUrlParam);

        final ClientResponse response;

        if (isSecureMode) {
            LOG.debug("create role as user {}", user);

            response = MiscUtil.executePrivilegedAction((PrivilegedExceptionAction<ClientResponse>) () -> {
                try {
                    return restClient.post(relativeURL, queryParams, request, sessionId);
                } catch (Exception e) {
                    LOG.error("Failed to get response, Error is : {}", e.getMessage());
                }

                return null;
            });
        } else {
            response = restClient.post(relativeURL, queryParams, request, sessionId);
        }

        checkAndResetSessionCookie(response);

        if (response != null && response.getStatus() != HttpServletResponse.SC_OK) {
            RESTResponse resp = RESTResponse.fromClientResponse(response);

            LOG.error("createRole() failed: HTTP status={}, message={}, isSecure={}{}", response.getStatus(), resp.getMessage(), isSecureMode, (isSecureMode ? (", user=" + user) : ""));

            if (response.getStatus() == HttpServletResponse.SC_UNAUTHORIZED) {
                throw new AccessControlException();
            }

            throw new Exception("HTTP " + response.getStatus() + " Error: " + resp.getMessage());
        } else if (response == null) {
            throw new Exception("unknown error during createRole. roleName=" + request.getName());
        } else {
            ret = JsonUtilsV2.readResponse(response, RangerRole.class);
        }

        LOG.debug("<== RangerAdminRESTClient.createRole({})", request);

        return ret;
    }

    @Override
    public void dropRole(final String execUser, final String roleName) throws Exception {
        LOG.debug("==> RangerAdminRESTClient.dropRole({})", roleName);

        final UserGroupInformation user         = MiscUtil.getUGILoginUser();
        final boolean              isSecureMode = isKerberosEnabled(user);
        final Cookie               sessionId    = this.sessionId;
        final Map<String, String> queryParams = new HashMap<>();

        queryParams.put(RangerRESTUtils.SERVICE_NAME_PARAM, serviceNameUrlParam);
        queryParams.put(RangerRESTUtils.REST_PARAM_EXEC_USER, execUser);

        final String         relativeURL = RangerRESTUtils.REST_URL_SERVICE_DROP_ROLE + roleName;
        final ClientResponse response;

        if (isSecureMode) {
            LOG.debug("drop role as user {}", user);

            response = MiscUtil.executePrivilegedAction((PrivilegedExceptionAction<ClientResponse>) () -> {
                try {
                    return restClient.delete(relativeURL, queryParams, sessionId);
                } catch (Exception e) {
                    LOG.error("Failed to get response, Error is : {}", e.getMessage());
                }

                return null;
            });
        } else {
            response = restClient.delete(relativeURL, queryParams, sessionId);
        }

        checkAndResetSessionCookie(response);

        if (response == null) {
            throw new Exception("unknown error during deleteRole. roleName=" + roleName);
        } else if (response.getStatus() != HttpServletResponse.SC_OK && response.getStatus() != HttpServletResponse.SC_NO_CONTENT) {
            RESTResponse resp = RESTResponse.fromClientResponse(response);

            LOG.error("createRole() failed: HTTP status={}, message={}, isSecure={}{}", response.getStatus(), resp.getMessage(), isSecureMode, (isSecureMode ? (", user=" + user) : ""));

            if (response.getStatus() == HttpServletResponse.SC_UNAUTHORIZED) {
                throw new AccessControlException();
            }

            throw new Exception("HTTP " + response.getStatus() + " Error: " + resp.getMessage());
        }

        LOG.debug("<== RangerAdminRESTClient.deleteRole({})", roleName);
    }

    @Override
    public List<String> getAllRoles(final String execUser) throws Exception {
        LOG.debug("==> RangerAdminRESTClient.getAllRoles()");

        final UserGroupInformation user         = MiscUtil.getUGILoginUser();
        final boolean              isSecureMode = isKerberosEnabled(user);
        final String               relativeURL  = RangerRESTUtils.REST_URL_SERVICE_GET_ALL_ROLES;
        final Cookie               sessionId    = this.sessionId;
        final Map<String, String>  queryParams  = new HashMap<>();

        queryParams.put(RangerRESTUtils.SERVICE_NAME_PARAM, serviceNameUrlParam);
        queryParams.put(RangerRESTUtils.REST_PARAM_EXEC_USER, execUser);

        final ClientResponse response;

        if (isSecureMode) {
            LOG.debug("get roles as user {}", user);

            response = MiscUtil.executePrivilegedAction((PrivilegedExceptionAction<ClientResponse>) () -> {
                try {
                    return restClient.get(relativeURL, queryParams, sessionId);
                } catch (Exception e) {
                    LOG.error("Failed to get response, Error is : {}", e.getMessage());
                }

                return null;
            });
        } else {
            response = restClient.get(relativeURL, queryParams, sessionId);
        }

        checkAndResetSessionCookie(response);

        List<String> ret;

        if (response != null) {
            if (response.getStatus() != HttpServletResponse.SC_OK) {
                RESTResponse resp = RESTResponse.fromClientResponse(response);

                LOG.error("getAllRoles() failed: HTTP status={}, message={}, isSecure={}{}", response.getStatus(), resp.getMessage(),  isSecureMode, (isSecureMode ? (", user=" + user) : ""));

                if (response.getStatus() == HttpServletResponse.SC_UNAUTHORIZED) {
                    throw new AccessControlException();
                }

                throw new Exception("HTTP " + response.getStatus() + " Error: " + resp.getMessage());
            } else {
                ret = JsonUtilsV2.readResponse(response, TYPE_LIST_STRING);
            }
        } else {
            throw new Exception("unknown error during getAllRoles.");
        }

        LOG.debug("<== RangerAdminRESTClient.getAllRoles()");

        return ret;
    }

    @Override
    public List<String> getUserRoles(final String execUser) throws Exception {
        LOG.debug("==> RangerAdminRESTClient.getUserRoles({})", execUser);

        final UserGroupInformation user         = MiscUtil.getUGILoginUser();
        final boolean              isSecureMode = isKerberosEnabled(user);
        final String               relativeURL  = RangerRESTUtils.REST_URL_SERVICE_GET_USER_ROLES + execUser;
        final Cookie               sessionId    = this.sessionId;
        final ClientResponse       response;

        if (isSecureMode) {
            LOG.debug("get roles as user {}", user);

            response = MiscUtil.executePrivilegedAction((PrivilegedExceptionAction<ClientResponse>) () -> {
                try {
                    return restClient.get(relativeURL, null, sessionId);
                } catch (Exception e) {
                    LOG.error("Failed to get response, Error is : {}", e.getMessage());
                }

                return null;
            });
        } else {
            response = restClient.get(relativeURL, null, sessionId);
        }

        checkAndResetSessionCookie(response);

        List<String> ret;

        if (response != null) {
            if (response.getStatus() != HttpServletResponse.SC_OK) {
                RESTResponse resp = RESTResponse.fromClientResponse(response);

                LOG.error("getUserRoles() failed: HTTP status={}, message={}, isSecure={}{}", response.getStatus(), resp.getMessage(), isSecureMode, (isSecureMode ? (", user=" + user) : ""));

                if (response.getStatus() == HttpServletResponse.SC_UNAUTHORIZED) {
                    throw new AccessControlException();
                }

                throw new Exception("HTTP " + response.getStatus() + " Error: " + resp.getMessage());
            } else {
                ret = JsonUtilsV2.readResponse(response, TYPE_LIST_STRING);
            }
        } else {
            throw new Exception("unknown error during getUserRoles. execUser=" + execUser);
        }

        LOG.debug("<== RangerAdminRESTClient.getUserRoles({})", execUser);

        return ret;
    }

    @Override
    public RangerRole getRole(final String execUser, final String roleName) throws Exception {
        LOG.debug("==> RangerAdminRESTClient.getRole({}, {})", execUser, roleName);

        final UserGroupInformation user         = MiscUtil.getUGILoginUser();
        final boolean              isSecureMode = isKerberosEnabled(user);
        final String               relativeURL  = RangerRESTUtils.REST_URL_SERVICE_GET_ROLE_INFO + roleName;
        final Cookie               sessionId    = this.sessionId;
        final Map<String, String>  queryParams  = new HashMap<>();
        final ClientResponse       response;

        queryParams.put(RangerRESTUtils.SERVICE_NAME_PARAM, serviceNameUrlParam);
        queryParams.put(RangerRESTUtils.REST_PARAM_EXEC_USER, execUser);

        if (isSecureMode) {
            LOG.debug("get role info as user {}", user);

            response = MiscUtil.executePrivilegedAction((PrivilegedExceptionAction<ClientResponse>) () -> {
                try {
                    return restClient.get(relativeURL, queryParams, sessionId);
                } catch (Exception e) {
                    LOG.error("Failed to get response, Error is : {}", e.getMessage());
                }

                return null;
            });
        } else {
            response = restClient.get(relativeURL, queryParams, sessionId);
        }

        checkAndResetSessionCookie(response);

        RangerRole ret;

        if (response != null) {
            if (response.getStatus() != HttpServletResponse.SC_OK) {
                RESTResponse resp = RESTResponse.fromClientResponse(response);

                LOG.error("getRole() failed: HTTP status={}, message={}, isSecure={}{}", response.getStatus(), resp.getMessage(), isSecureMode, (isSecureMode ? (", user=" + user) : ""));

                if (response.getStatus() == HttpServletResponse.SC_UNAUTHORIZED) {
                    throw new AccessControlException();
                }

                throw new Exception("HTTP " + response.getStatus() + " Error: " + resp.getMessage());
            } else {
                ret = JsonUtilsV2.readResponse(response, RangerRole.class);
            }
        } else {
            throw new Exception("unknown error during getPrincipalsForRole. roleName=" + roleName);
        }

        LOG.debug("<== RangerAdminRESTClient.getRole({}, {})", execUser, roleName);

        return ret;
    }

    @Override
    public void grantRole(final GrantRevokeRoleRequest request) throws Exception {
        LOG.debug("==> RangerAdminRESTClient.grantRole({})", request);

        final UserGroupInformation user         = MiscUtil.getUGILoginUser();
        final boolean              isSecureMode = isKerberosEnabled(user);
        final String               relativeURL  = RangerRESTUtils.REST_URL_SERVICE_GRANT_ROLE + serviceNameUrlParam;
        final Cookie               sessionId    = this.sessionId;
        final ClientResponse       response;

        if (isSecureMode) {
            LOG.debug("grant role as user {}", user);

            response = MiscUtil.executePrivilegedAction((PrivilegedExceptionAction<ClientResponse>) () -> {
                try {
                    return restClient.put(relativeURL, request, sessionId);
                } catch (Exception e) {
                    LOG.error("Failed to get response, Error is : {}", e.getMessage());
                }

                return null;
            });
        } else {
            response = restClient.put(relativeURL, request, sessionId);
        }

        checkAndResetSessionCookie(response);

        if (response != null && response.getStatus() != HttpServletResponse.SC_OK) {
            RESTResponse resp = RESTResponse.fromClientResponse(response);

            LOG.error("grantRole() failed: HTTP status={}, message={}, isSecure={}{}", response.getStatus(), resp.getMessage(), isSecureMode, (isSecureMode ? (", user=" + user) : ""));

            if (response.getStatus() == HttpServletResponse.SC_UNAUTHORIZED) {
                throw new AccessControlException();
            }

            throw new Exception("HTTP " + response.getStatus() + " Error: " + resp.getMessage());
        } else if (response == null) {
            throw new Exception("unknown error during grantRole. serviceName=" + serviceName);
        }

        LOG.debug("<== RangerAdminRESTClient.grantRole({})", request);
    }

    @Override
    public void revokeRole(final GrantRevokeRoleRequest request) throws Exception {
        LOG.debug("==> RangerAdminRESTClient.revokeRole({})", request);

        final UserGroupInformation user         = MiscUtil.getUGILoginUser();
        final boolean              isSecureMode = isKerberosEnabled(user);
        final String               relativeURL  = RangerRESTUtils.REST_URL_SERVICE_REVOKE_ROLE + serviceNameUrlParam;
        final Cookie               sessionId    = this.sessionId;
        final ClientResponse       response;

        if (isSecureMode) {
            LOG.debug("revoke role as user {}", user);

            response = MiscUtil.executePrivilegedAction((PrivilegedExceptionAction<ClientResponse>) () -> {
                try {
                    return restClient.put(relativeURL, request, sessionId);
                } catch (Exception e) {
                    LOG.error("Failed to get response, Error is : {}", e.getMessage());
                }

                return null;
            });
        } else {
            response = restClient.put(relativeURL, request, sessionId);
        }

        checkAndResetSessionCookie(response);

        if (response != null && response.getStatus() != HttpServletResponse.SC_OK) {
            RESTResponse resp = RESTResponse.fromClientResponse(response);

            LOG.error("revokeRole() failed: HTTP status={}, message={}, isSecure={}{}", response.getStatus(), resp.getMessage(), isSecureMode, (isSecureMode ? (", user=" + user) : ""));

            if (response.getStatus() == HttpServletResponse.SC_UNAUTHORIZED) {
                throw new AccessControlException();
            }

            throw new Exception("HTTP " + response.getStatus() + " Error: " + resp.getMessage());
        } else if (response == null) {
            throw new Exception("unknown error during revokeRole. serviceName=" + serviceName);
        }

        LOG.debug("<== RangerAdminRESTClient.revokeRole({})", request);
    }

    @Override
    public void grantAccess(final GrantRevokeRequest request) throws Exception {
        LOG.debug("==> RangerAdminRESTClient.grantAccess({})", request);

        final UserGroupInformation user         = MiscUtil.getUGILoginUser();
        final boolean              isSecureMode = isKerberosEnabled(user);
        final Cookie               sessionId    = this.sessionId;
        final Map<String, String>  queryParams  = new HashMap<>();

        queryParams.put(RangerRESTUtils.REST_PARAM_PLUGIN_ID, pluginId);

        final ClientResponse response;

        if (isSecureMode) {
            LOG.debug("grantAccess as user {}", user);

            response = MiscUtil.executePrivilegedAction((PrivilegedExceptionAction<ClientResponse>) () -> {
                try {
                    String relativeURL = RangerRESTUtils.REST_URL_SECURE_SERVICE_GRANT_ACCESS + serviceNameUrlParam;

                    return restClient.post(relativeURL, queryParams, request, sessionId);
                } catch (Exception e) {
                    LOG.error("Failed to get response, Error is : {}", e.getMessage());
                }

                return null;
            });
        } else {
            String relativeURL = RangerRESTUtils.REST_URL_SERVICE_GRANT_ACCESS + serviceNameUrlParam;

            response = restClient.post(relativeURL, queryParams, request, sessionId);
        }

        checkAndResetSessionCookie(response);

        if (response != null && response.getStatus() != HttpServletResponse.SC_OK) {
            RESTResponse resp = RESTResponse.fromClientResponse(response);

            LOG.error("grantAccess() failed: HTTP status={}, message={}, isSecure={}{}", response.getStatus(), resp.getMessage(), isSecureMode, (isSecureMode ? (", user=" + user) : ""));

            if (response.getStatus() == HttpServletResponse.SC_UNAUTHORIZED) {
                throw new AccessControlException();
            }

            throw new Exception("HTTP " + response.getStatus() + " Error: " + resp.getMessage());
        } else if (response == null) {
            throw new Exception("unknown error during grantAccess. serviceName=" + serviceName);
        }

        LOG.debug("<== RangerAdminRESTClient.grantAccess({})", request);
    }

    @Override
    public void revokeAccess(final GrantRevokeRequest request) throws Exception {
        LOG.debug("==> RangerAdminRESTClient.revokeAccess({})", request);

        final UserGroupInformation user         = MiscUtil.getUGILoginUser();
        final boolean              isSecureMode = isKerberosEnabled(user);
        final Cookie               sessionId    = this.sessionId;
        final Map<String, String>  queryParams  = new HashMap<>();

        queryParams.put(RangerRESTUtils.REST_PARAM_PLUGIN_ID, pluginId);

        final ClientResponse response;

        if (isSecureMode) {
            LOG.debug("revokeAccess as user {}", user);

            response = MiscUtil.executePrivilegedAction((PrivilegedExceptionAction<ClientResponse>) () -> {
                try {
                    String relativeURL = RangerRESTUtils.REST_URL_SECURE_SERVICE_REVOKE_ACCESS + serviceNameUrlParam;

                    return restClient.post(relativeURL, queryParams, request, sessionId);
                } catch (Exception e) {
                    LOG.error("Failed to get response, Error is : {}", e.getMessage());
                }

                return null;
            });
        } else {
            String relativeURL = RangerRESTUtils.REST_URL_SERVICE_REVOKE_ACCESS + serviceNameUrlParam;

            response = restClient.post(relativeURL, queryParams, request, sessionId);
        }

        checkAndResetSessionCookie(response);

        if (response != null && response.getStatus() != HttpServletResponse.SC_OK) {
            RESTResponse resp = RESTResponse.fromClientResponse(response);

            LOG.error("revokeAccess() failed: HTTP status={}, message={}, isSecure={}{}", response.getStatus(), resp.getMessage(), isSecureMode, (isSecureMode ? (", user=" + user) : ""));

            if (response.getStatus() == HttpServletResponse.SC_UNAUTHORIZED) {
                throw new AccessControlException();
            }

            throw new Exception("HTTP " + response.getStatus() + " Error: " + resp.getMessage());
        } else if (response == null) {
            throw new Exception("unknown error. revokeAccess(). serviceName=" + serviceName);
        }

        LOG.debug("<== RangerAdminRESTClient.revokeAccess({})", request);
    }

    @Override
    public ServiceTags getServiceTagsIfUpdated(final long lastKnownVersion, final long lastActivationTimeInMillis) throws Exception {
        LOG.debug("==> RangerAdminRESTClient.getServiceTagsIfUpdated({}, {}): ", lastKnownVersion, lastActivationTimeInMillis);

        final UserGroupInformation user         = MiscUtil.getUGILoginUser();
        final boolean              isSecureMode = isKerberosEnabled(user);
        final Cookie               sessionId    = this.sessionId;
        final Map<String, String>  queryParams  = new HashMap<>();

        queryParams.put(RangerRESTUtils.LAST_KNOWN_TAG_VERSION_PARAM, Long.toString(lastKnownVersion));
        queryParams.put(RangerRESTUtils.REST_PARAM_LAST_ACTIVATION_TIME, Long.toString(lastActivationTimeInMillis));
        queryParams.put(RangerRESTUtils.REST_PARAM_PLUGIN_ID, pluginId);
        queryParams.put(RangerRESTUtils.REST_PARAM_SUPPORTS_TAG_DELTAS, Boolean.toString(supportsTagDeltas));
        queryParams.put(RangerRESTUtils.REST_PARAM_CAPABILITIES, pluginCapabilities);

        final ClientResponse response;

        if (isSecureMode) {
            LOG.debug("getServiceTagsIfUpdated as user {}", user);

            response = MiscUtil.executePrivilegedAction((PrivilegedExceptionAction<ClientResponse>) () -> {
                try {
                    String relativeURL = RangerRESTUtils.REST_URL_GET_SECURE_SERVICE_TAGS_IF_UPDATED + serviceNameUrlParam;

                    return restClient.get(relativeURL, queryParams, sessionId);
                } catch (Exception e) {
                    LOG.error("Failed to get response, Error is : {}", e.getMessage());
                }

                return null;
            });
        } else {
            String relativeURL = RangerRESTUtils.REST_URL_GET_SERVICE_TAGS_IF_UPDATED + serviceNameUrlParam;

            response = restClient.get(relativeURL, queryParams, sessionId);
        }

        checkAndResetSessionCookie(response);

        final ServiceTags ret;

        if (response == null || response.getStatus() == HttpServletResponse.SC_NOT_MODIFIED) {
            if (response == null) {
                LOG.error("Error getting tags; Received NULL response!!. secureMode={}, user={}, serviceName={}", isSecureMode, user, serviceName);
            } else {
                RESTResponse resp = RESTResponse.fromClientResponse(response);

                LOG.debug("No change in tags. secureMode={}, user={}, response={}, serviceName={}, lastKnownVersion={}, lastActivationTimeInMillis={}",
                        isSecureMode, user, resp, serviceName, lastKnownVersion, lastActivationTimeInMillis);
            }

            ret = null;
        } else if (response.getStatus() == HttpServletResponse.SC_OK) {
            ret = JsonUtilsV2.readResponse(response, ServiceTags.class);
        } else if (response.getStatus() == HttpServletResponse.SC_NOT_FOUND) {
            ret = null;

            LOG.error("Error getting tags; service not found. secureMode={}, user={}, response={}, serviceName={}, lastKnownVersion={}, lastActivationTimeInMillis={}",
                    isSecureMode, user, response.getStatus(), serviceName, lastKnownVersion, lastActivationTimeInMillis);

            String exceptionMsg = response.hasEntity() ? response.getEntity(String.class) : null;

            RangerServiceNotFoundException.throwExceptionIfServiceNotFound(serviceName, exceptionMsg);

            LOG.warn("Received 404 error code with body:[{}], Ignoring", exceptionMsg);
        } else {
            RESTResponse resp = RESTResponse.fromClientResponse(response);

            LOG.warn("Error getting tags. secureMode={}, user={}, response={}, serviceName={}", isSecureMode, user, resp, serviceName);

            ret = null;
        }

        LOG.debug("<== RangerAdminRESTClient.getServiceTagsIfUpdated({}, {})", lastKnownVersion, lastActivationTimeInMillis);

        return ret;
    }

    @Override
    public List<String> getTagTypes(String pattern) throws Exception {
        LOG.debug("==> RangerAdminRESTClient.getTagTypes({}): ", pattern);

        final String               relativeURL  = RangerRESTUtils.REST_URL_LOOKUP_TAG_NAMES;
        final UserGroupInformation user         = MiscUtil.getUGILoginUser();
        final boolean              isSecureMode = isKerberosEnabled(user);
        final Cookie               sessionId    = this.sessionId;
        final Map<String, String>  queryParams  = new HashMap<>();

        queryParams.put(RangerRESTUtils.SERVICE_NAME_PARAM, serviceNameUrlParam);
        queryParams.put(RangerRESTUtils.PATTERN_PARAM, pattern);

        final ClientResponse response;

        if (isSecureMode) {
            LOG.debug("getTagTypes as user {}", user);

            response = MiscUtil.executePrivilegedAction((PrivilegedExceptionAction<ClientResponse>) () -> {
                try {
                    return restClient.get(relativeURL, queryParams, sessionId);
                } catch (Exception e) {
                    LOG.error("Failed to get response, Error is : {}", e.getMessage());
                }

                return null;
            });
        } else {
            response = restClient.get(relativeURL, queryParams, sessionId);
        }

        checkAndResetSessionCookie(response);

        List<String> ret;

        if (response != null && response.getStatus() == HttpServletResponse.SC_OK) {
            ret = JsonUtilsV2.readResponse(response, TYPE_LIST_STRING);
        } else {
            RESTResponse resp = RESTResponse.fromClientResponse(response);

            LOG.error("Error getting tags. response={}, serviceName={}, pattern={}", resp, serviceName, pattern);

            throw new Exception(resp.getMessage());
        }

        LOG.debug("<== RangerAdminRESTClient.getTagTypes({}): {}", pattern, ret);

        return ret;
    }

    @Override
    public RangerUserStore getUserStoreIfUpdated(long lastKnownUserStoreVersion, long lastActivationTimeInMillis) throws Exception {
        LOG.debug("==> RangerAdminRESTClient.getUserStoreIfUpdated({}, {})", lastKnownUserStoreVersion, lastActivationTimeInMillis);

        final UserGroupInformation user         = MiscUtil.getUGILoginUser();
        final boolean              isSecureMode = isKerberosEnabled(user);
        final Cookie               sessionId    = this.sessionId;
        final Map<String, String>  queryParams  = new HashMap<>();

        queryParams.put(RangerRESTUtils.REST_PARAM_LAST_KNOWN_USERSTORE_VERSION, Long.toString(lastKnownUserStoreVersion));
        queryParams.put(RangerRESTUtils.REST_PARAM_LAST_ACTIVATION_TIME, Long.toString(lastActivationTimeInMillis));
        queryParams.put(RangerRESTUtils.REST_PARAM_PLUGIN_ID, pluginId);
        queryParams.put(RangerRESTUtils.REST_PARAM_CLUSTER_NAME, clusterName);
        queryParams.put(RangerRESTUtils.REST_PARAM_CAPABILITIES, pluginCapabilities);

        final ClientResponse response;

        if (isSecureMode) {
            LOG.debug("Checking UserStore updated as user : {}", user);

            response = MiscUtil.executePrivilegedAction((PrivilegedExceptionAction<ClientResponse>) () -> {
                try {
                    String relativeURL = RangerRESTUtils.REST_URL_SERVICE_SERCURE_GET_USERSTORE + serviceNameUrlParam;

                    return restClient.get(relativeURL, queryParams, sessionId);
                } catch (Exception e) {
                    LOG.error("Failed to get response, Error is : {}", e.getMessage());
                }

                return null;
            });
        } else {
            LOG.debug("Checking UserStore updated as user : {}", user);

            String relativeURL = RangerRESTUtils.REST_URL_SERVICE_GET_USERSTORE + serviceNameUrlParam;

            response = restClient.get(relativeURL, queryParams, sessionId);
        }

        checkAndResetSessionCookie(response);

        final RangerUserStore ret;

        if (response == null || response.getStatus() == HttpServletResponse.SC_NOT_MODIFIED) {
            if (response == null) {
                LOG.error("Error getting UserStore; Received NULL response!!. secureMode={}, user={}, serviceName={}", isSecureMode, user, serviceName);
            } else {
                RESTResponse resp = RESTResponse.fromClientResponse(response);

                LOG.debug("No change in UserStore. secureMode={}, user={}, response={}, serviceName={}, lastKnownUserStoreVersion={}, lastActivationTimeInMillis={}",
                        isSecureMode, user, resp, serviceName, lastKnownUserStoreVersion, lastActivationTimeInMillis);
            }

            ret = null;
        } else if (response.getStatus() == HttpServletResponse.SC_OK) {
            ret = JsonUtilsV2.readResponse(response, RangerUserStore.class);
        } else if (response.getStatus() == HttpServletResponse.SC_NOT_FOUND) {
            ret = null;

            LOG.error("Error getting UserStore; service not found. secureMode={}, user={}, response={}, serviceName={}, lastKnownUserStoreVersion={}, lastActivationTimeInMillis={}",
                    isSecureMode, user, response.getStatus(), serviceName, lastKnownUserStoreVersion, lastActivationTimeInMillis);

            String exceptionMsg = response.hasEntity() ? response.getEntity(String.class) : null;

            RangerServiceNotFoundException.throwExceptionIfServiceNotFound(serviceName, exceptionMsg);

            LOG.warn("Received 404 error code with body:[{}], Ignoring", exceptionMsg);
        } else {
            RESTResponse resp = RESTResponse.fromClientResponse(response);

            LOG.warn("Error getting UserStore. secureMode={}, user={}, response={}, serviceName={}", isSecureMode, user, resp, serviceName);

            ret = null;
        }

        LOG.debug("<== RangerAdminRESTClient.getUserStoreIfUpdated({}, {}): ", lastKnownUserStoreVersion, lastActivationTimeInMillis);

        return ret;
    }

    @Override
    public ServiceGdsInfo getGdsInfoIfUpdated(long lastKnownVersion, long lastActivationTimeInMillis) throws Exception {
        LOG.debug("==> RangerAdminRESTClient.getGdsInfoIfUpdated({}, {})", lastKnownVersion, lastActivationTimeInMillis);

        final UserGroupInformation user         = MiscUtil.getUGILoginUser();
        final boolean              isSecureMode = isKerberosEnabled(user);
        final Cookie               sessionId    = this.sessionId;
        final Map<String, String>  queryParams  = new HashMap<>();

        queryParams.put(RangerRESTUtils.REST_PARAM_LAST_KNOWN_GDS_VERSION, Long.toString(lastKnownVersion));
        queryParams.put(RangerRESTUtils.REST_PARAM_LAST_ACTIVATION_TIME, Long.toString(lastActivationTimeInMillis));
        queryParams.put(RangerRESTUtils.REST_PARAM_PLUGIN_ID, pluginId);
        queryParams.put(RangerRESTUtils.REST_PARAM_CLUSTER_NAME, clusterName);
        queryParams.put(RangerRESTUtils.REST_PARAM_CAPABILITIES, pluginCapabilities);

        LOG.debug("Checking for updated GdsInfo: secureMode={}, user={}, serviceName={}", isSecureMode, user, serviceName);

        final ClientResponse response;

        if (isSecureMode) {
            response = MiscUtil.executePrivilegedAction((PrivilegedExceptionAction<ClientResponse>) () -> {
                try {
                    String relativeURL = RangerRESTUtils.REST_URL_SERVICE_SECURE_GET_GDSINFO + serviceNameUrlParam;

                    return restClient.get(relativeURL, queryParams, sessionId);
                } catch (Exception e) {
                    LOG.error("Failed to get response", e);
                }

                return null;
            });
        } else {
            String relativeURL = RangerRESTUtils.REST_URL_SERVICE_GET_GDSINFO + serviceNameUrlParam;

            response = restClient.get(relativeURL, queryParams, sessionId);
        }

        checkAndResetSessionCookie(response);

        final ServiceGdsInfo ret;

        if (response == null) {
            ret = null;

            LOG.error("Error getting GdsInfo - received NULL response: secureMode={}, user={}, serviceName={}", isSecureMode, user, serviceName);
        } else if (response.getStatus() == HttpServletResponse.SC_NOT_MODIFIED) {
            ret = null;

            RESTResponse resp = RESTResponse.fromClientResponse(response);

            LOG.debug("No change in GdsInfo: secureMode={}, user={}, response={}, serviceName={}, lastKnownGdsVersion={}, lastActivationTimeInMillis={}",
                    isSecureMode, user, resp, serviceName, lastKnownVersion, lastActivationTimeInMillis);
        } else if (response.getStatus() == HttpServletResponse.SC_OK) {
            ret = JsonUtilsV2.readResponse(response, ServiceGdsInfo.class);
        } else if (response.getStatus() == HttpServletResponse.SC_NOT_FOUND) {
            ret = null;

            LOG.error("Error getting GdsInfo - service not found: secureMode={}, user={}, response={}, serviceName={}, lastKnownGdsVersion={},lastActivationTimeInMillis={}",
                    isSecureMode, user, response.getStatus(), serviceName, lastKnownVersion, lastActivationTimeInMillis);

            String exceptionMsg = response.hasEntity() ? response.getEntity(String.class) : null;

            RangerServiceNotFoundException.throwExceptionIfServiceNotFound(serviceName, exceptionMsg);

            LOG.warn("Received 404 error code with body:[{}], Ignoring", exceptionMsg);
        } else {
            ret = null;

            RESTResponse resp = RESTResponse.fromClientResponse(response);

            LOG.warn("Error getting GdsInfo: unexpected status code {}: secureMode={}, user={}, response={}, serviceName={}",
                    response.getStatus(), isSecureMode, user, resp, serviceName);
        }

        LOG.debug("<== RangerAdminRESTClient.getGdsInfoIfUpdated({}, {}): ret={}", lastKnownVersion, lastActivationTimeInMillis, ret);

        return ret;
    }

    private void init(String url, String sslConfigFileName, int restClientConnTimeOutMs, int restClientReadTimeOutMs, int restClientMaxRetryAttempts, int restClientRetryIntervalMs, Configuration config) {
        LOG.debug("==> RangerAdminRESTClient.init({}, {})", url, sslConfigFileName);

        restClient = new RangerRESTClient(url, sslConfigFileName, config);

        restClient.setRestClientConnTimeOutMs(restClientConnTimeOutMs);
        restClient.setRestClientReadTimeOutMs(restClientReadTimeOutMs);
        restClient.setMaxRetryAttempts(restClientMaxRetryAttempts);
        restClient.setRetryIntervalMs(restClientRetryIntervalMs);

        LOG.debug("<== RangerAdminRESTClient.init({}, {})", url, sslConfigFileName);
    }

    private void checkAndResetSessionCookie(ClientResponse response) {
        if (isRangerCookieEnabled) {
            if (response == null) {
                LOG.debug("checkAndResetSessionCookie(): RESETTING sessionId - response is null");

                sessionId = null;
            } else {
                int status = response.getStatus();

                if (status == HttpStatus.SC_OK || status == HttpStatus.SC_NO_CONTENT || status == HttpStatus.SC_NOT_MODIFIED) {
                    Cookie newCookie = null;

                    for (NewCookie cookie : response.getCookies()) {
                        if (cookie.getName().equalsIgnoreCase(rangerAdminCookieName)) {
                            newCookie = cookie;

                            break;
                        }
                    }

                    if (sessionId == null || newCookie != null) {
                        LOG.debug("checkAndResetSessionCookie(): status={}, sessionIdCookie={}, newCookie={}", status, sessionId, newCookie);

                        sessionId = newCookie;
                    }
                } else {
                    LOG.debug("checkAndResetSessionCookie(): RESETTING sessionId - status={}", status);

                    sessionId = null;
                }
            }
        }
    }
}
