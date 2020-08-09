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


import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.GenericType;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.ranger.admin.client.datatype.RESTResponse;
import org.apache.ranger.audit.provider.MiscUtil;
import org.apache.ranger.authorization.hadoop.config.RangerPluginConfig;
import org.apache.ranger.authorization.utils.StringUtil;
import org.apache.ranger.plugin.model.RangerRole;
import org.apache.ranger.plugin.util.*;

import javax.servlet.http.HttpServletResponse;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.security.PrivilegedAction;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RangerAdminRESTClient extends AbstractRangerAdminClient {
	private static final Log LOG = LogFactory.getLog(RangerAdminRESTClient.class);

	private String           serviceName;
    private String           serviceNameUrlParam;
	private String           pluginId;
	private String           clusterName;
	private RangerRESTClient restClient;
	private RangerRESTUtils  restUtils   = new RangerRESTUtils();
	private String 		     supportsPolicyDeltas;
	private String 		     supportsTagDeltas;
	private final String     pluginCapabilities = Long.toHexString(new RangerPluginCapability().getPluginCapabilities());

	public static <T> GenericType<List<T>> getGenericType(final T clazz) {

		ParameterizedType parameterizedGenericType = new ParameterizedType() {
			public Type[] getActualTypeArguments() {
				return new Type[] { clazz.getClass() };
			}

			public Type getRawType() {
				return List.class;
			}

			public Type getOwnerType() {
				return List.class;
			}
		};

		return new GenericType<List<T>>(parameterizedGenericType) {};
	}

	@Override
	public void init(String serviceName, String appId, String propertyPrefix, Configuration config) {
	    super.init(serviceName, appId, propertyPrefix, config);

		this.serviceName = serviceName;
		this.pluginId    = restUtils.getPluginId(serviceName, appId);

		String url                      = "";
		String tmpUrl                   = config.get(propertyPrefix + ".policy.rest.url");
		String sslConfigFileName 		= config.get(propertyPrefix + ".policy.rest.ssl.config.file");
		clusterName       				= config.get(propertyPrefix + ".access.cluster.name", "");
		if(StringUtil.isEmpty(clusterName)){
			clusterName =config.get(propertyPrefix + ".ambari.cluster.name", "");
			if (StringUtil.isEmpty(clusterName)) {
				if (config instanceof RangerPluginConfig) {
					clusterName = ((RangerPluginConfig)config).getClusterName();
				}
			}
		}
		int	 restClientConnTimeOutMs	= config.getInt(propertyPrefix + ".policy.rest.client.connection.timeoutMs", 120 * 1000);
		int	 restClientReadTimeOutMs	= config.getInt(propertyPrefix + ".policy.rest.client.read.timeoutMs", 30 * 1000);
		supportsPolicyDeltas            = config.get(propertyPrefix + ".policy.rest.supports.policy.deltas", "false");
		supportsTagDeltas               = config.get(propertyPrefix + ".tag.rest.supports.tag.deltas", "false");

        if (!StringUtil.isEmpty(tmpUrl)) {
            url = tmpUrl.trim();
        }
        if (url.endsWith("/")) {
            url = url.substring(0, url.length() - 1);
        }
		if (!"true".equalsIgnoreCase(supportsPolicyDeltas)) {
			supportsPolicyDeltas = "false";
		}
		if (!"true".equalsIgnoreCase(supportsTagDeltas)) {
			supportsTagDeltas = "false";
		}

		init(url, sslConfigFileName, restClientConnTimeOutMs , restClientReadTimeOutMs, config);

        try {
            this.serviceNameUrlParam = URLEncoderUtil.encodeURIParam(serviceName);
        } catch (UnsupportedEncodingException e) {
            LOG.warn("Unsupported encoding, serviceName=" + serviceName);
            this.serviceNameUrlParam = serviceName;
        }
	}

	@Override
	public ServicePolicies getServicePoliciesIfUpdated(final long lastKnownVersion, final long lastActivationTimeInMillis) throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAdminRESTClient.getServicePoliciesIfUpdated(" + lastKnownVersion + ", " + lastActivationTimeInMillis + ")");
		}

		final ServicePolicies ret;

		final UserGroupInformation user = MiscUtil.getUGILoginUser();
		final boolean isSecureMode = user != null && UserGroupInformation.isSecurityEnabled();
		final ClientResponse response;

		Map<String, String> queryParams = new HashMap<String, String>();
		queryParams.put(RangerRESTUtils.REST_PARAM_LAST_KNOWN_POLICY_VERSION, Long.toString(lastKnownVersion));
		queryParams.put(RangerRESTUtils.REST_PARAM_LAST_ACTIVATION_TIME, Long.toString(lastActivationTimeInMillis));
		queryParams.put(RangerRESTUtils.REST_PARAM_PLUGIN_ID, pluginId);
		queryParams.put(RangerRESTUtils.REST_PARAM_CLUSTER_NAME, clusterName);
		queryParams.put(RangerRESTUtils.REST_PARAM_SUPPORTS_POLICY_DELTAS, supportsPolicyDeltas);
		queryParams.put(RangerRESTUtils.REST_PARAM_CAPABILITIES, pluginCapabilities);

		if (isSecureMode) {
			if (LOG.isDebugEnabled()) {
				LOG.debug("Checking Service policy if updated as user : " + user);
			}
			PrivilegedAction<ClientResponse> action = new PrivilegedAction<ClientResponse>() {
				public ClientResponse run() {
					ClientResponse clientRes = null;
					String relativeURL = RangerRESTUtils.REST_URL_POLICY_GET_FOR_SECURE_SERVICE_IF_UPDATED + serviceNameUrlParam;
					try {
						clientRes =  restClient.get(relativeURL, queryParams);
					} catch (Exception e) {
						LOG.error("Failed to get response, Error is : "+e.getMessage());
					}
					return clientRes;
				}
			};
			response = user.doAs(action);
		} else {
			if (LOG.isDebugEnabled()) {
				LOG.debug("Checking Service policy if updated with old api call");
			}
			String relativeURL = RangerRESTUtils.REST_URL_POLICY_GET_FOR_SERVICE_IF_UPDATED + serviceNameUrlParam;
			response = restClient.get(relativeURL, queryParams);
		}

		if (response == null || response.getStatus() == HttpServletResponse.SC_NOT_MODIFIED) {
			if (response == null) {
				LOG.error("Error getting policies; Received NULL response!!. secureMode=" + isSecureMode + ", user=" + user + ", serviceName=" + serviceName);
			} else {
				RESTResponse resp = RESTResponse.fromClientResponse(response);
				if (LOG.isDebugEnabled()) {
					LOG.debug("No change in policies. secureMode=" + isSecureMode + ", user=" + user + ", response=" + resp + ", serviceName=" + serviceName);
				}
			}
			ret = null;
		} else if (response.getStatus() == HttpServletResponse.SC_OK) {
			ret = response.getEntity(ServicePolicies.class);
		} else if (response.getStatus() == HttpServletResponse.SC_NOT_FOUND) {
			LOG.error("Error getting policies; service not found. secureMode=" + isSecureMode + ", user=" + user
					+ ", response=" + response.getStatus() + ", serviceName=" + serviceName
					+ ", " + "lastKnownVersion=" + lastKnownVersion
					+ ", " + "lastActivationTimeInMillis=" + lastActivationTimeInMillis);
			ret = null;
			String exceptionMsg = response.hasEntity() ? response.getEntity(String.class) : null;

			RangerServiceNotFoundException.throwExceptionIfServiceNotFound(serviceName, exceptionMsg);

			LOG.warn("Received 404 error code with body:[" + exceptionMsg + "], Ignoring");
		} else {
			RESTResponse resp = RESTResponse.fromClientResponse(response);
			LOG.warn("Error getting policies. secureMode=" + isSecureMode + ", user=" + user + ", response=" + resp + ", serviceName=" + serviceName);
			ret = null;
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAdminRESTClient.getServicePoliciesIfUpdated(" + lastKnownVersion + ", " + lastActivationTimeInMillis + "): " + ret);
		}

		return ret;
	}

	@Override
	public RangerRoles getRolesIfUpdated(final long lastKnownRoleVersion, final long lastActivationTimeInMillis) throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAdminRESTClient.getRolesIfUpdated(" + lastKnownRoleVersion + ", " + lastActivationTimeInMillis + ")");
		}

		final RangerRoles ret;
		final UserGroupInformation user = MiscUtil.getUGILoginUser();
		final boolean isSecureMode = user != null && UserGroupInformation.isSecurityEnabled();
		final ClientResponse response;

		Map<String, String> queryParams = new HashMap<String, String>();
		queryParams.put(RangerRESTUtils.REST_PARAM_LAST_KNOWN_ROLE_VERSION, Long.toString(lastKnownRoleVersion));
		queryParams.put(RangerRESTUtils.REST_PARAM_LAST_ACTIVATION_TIME, Long.toString(lastActivationTimeInMillis));
		queryParams.put(RangerRESTUtils.REST_PARAM_PLUGIN_ID, pluginId);
		queryParams.put(RangerRESTUtils.REST_PARAM_CLUSTER_NAME, clusterName);
		queryParams.put(RangerRESTUtils.REST_PARAM_CAPABILITIES, pluginCapabilities);

		if (isSecureMode) {
			if (LOG.isDebugEnabled()) {
				LOG.debug("Checking Roles updated as user : " + user);
			}
			PrivilegedAction<ClientResponse> action = new PrivilegedAction<ClientResponse>() {
				public ClientResponse run() {
					ClientResponse clientRes = null;
					String relativeURL = RangerRESTUtils.REST_URL_SERVICE_SERCURE_GET_USER_GROUP_ROLES + serviceNameUrlParam;
					try {
						clientRes =  restClient.get(relativeURL, queryParams);
					} catch (Exception e) {
						LOG.error("Failed to get response, Error is : "+e.getMessage());
					}
					return clientRes;
				}
			};
			response = user.doAs(action);
		} else {
			if (LOG.isDebugEnabled()) {
				LOG.debug("Checking Roles updated as user : " + user);
			}
			String relativeURL = RangerRESTUtils.REST_URL_SERVICE_GET_USER_GROUP_ROLES + serviceNameUrlParam;
			response = restClient.get(relativeURL, queryParams);
		}

		if (response == null || response.getStatus() == HttpServletResponse.SC_NOT_MODIFIED) {
			if (response == null) {
				LOG.error("Error getting Roles; Received NULL response!!. secureMode=" + isSecureMode + ", user=" + user + ", serviceName=" + serviceName);
			} else {
				RESTResponse resp = RESTResponse.fromClientResponse(response);
				if (LOG.isDebugEnabled()) {
					LOG.debug("No change in Roles. secureMode=" + isSecureMode + ", user=" + user
							+ ", response=" + resp + ", serviceName=" + serviceName
							+ ", " + "lastKnownRoleVersion=" + lastKnownRoleVersion
							+ ", " + "lastActivationTimeInMillis=" + lastActivationTimeInMillis);
				}
			}
			ret = null;
		} else if (response.getStatus() == HttpServletResponse.SC_OK) {
			ret = response.getEntity(RangerRoles.class);
		} else if (response.getStatus() == HttpServletResponse.SC_NOT_FOUND) {
			ret = null;
			LOG.error("Error getting Roles; service not found. secureMode=" + isSecureMode + ", user=" + user
					+ ", response=" + response.getStatus() + ", serviceName=" + serviceName
					+ ", " + "lastKnownRoleVersion=" + lastKnownRoleVersion
					+ ", " + "lastActivationTimeInMillis=" + lastActivationTimeInMillis);
			String exceptionMsg = response.hasEntity() ? response.getEntity(String.class) : null;

			RangerServiceNotFoundException.throwExceptionIfServiceNotFound(serviceName, exceptionMsg);

			LOG.warn("Received 404 error code with body:[" + exceptionMsg + "], Ignoring");
		} else {
			RESTResponse resp = RESTResponse.fromClientResponse(response);
			LOG.warn("Error getting Roles. secureMode=" + isSecureMode + ", user=" + user + ", response=" + resp + ", serviceName=" + serviceName);
			ret = null;
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAdminRESTClient.getRolesIfUpdated(" + lastKnownRoleVersion + ", " + lastActivationTimeInMillis + "): ");
		}

		return ret;
	}

	@Override
	public RangerRole createRole(final RangerRole request) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAdminRESTClient.createRole(" + request + ")");
		}

		RangerRole ret = null;

		ClientResponse response = null;
		UserGroupInformation user = MiscUtil.getUGILoginUser();
		boolean isSecureMode = user != null && UserGroupInformation.isSecurityEnabled();
		String relativeURL = RangerRESTUtils.REST_URL_SERVICE_CREATE_ROLE;

		Map <String, String> queryParams = new HashMap<String, String> ();
		queryParams.put(RangerRESTUtils.SERVICE_NAME_PARAM, serviceNameUrlParam);

		if (isSecureMode) {
			PrivilegedAction<ClientResponse> action = new PrivilegedAction<ClientResponse>() {
				public ClientResponse run() {
					ClientResponse clientRes = null;
					try {
						clientRes = restClient.post(relativeURL, queryParams, request);
					} catch (Exception e) {
						LOG.error("Failed to get response, Error is : "+e.getMessage());
					}
					return clientRes;
				}
			};
			if (LOG.isDebugEnabled()) {
				LOG.debug("create role as user " + user);
			}
			response = user.doAs(action);
		} else {
			response = restClient.post(relativeURL, queryParams, request);
		}

		if(response != null && response.getStatus() != HttpServletResponse.SC_OK) {
			RESTResponse resp = RESTResponse.fromClientResponse(response);
			LOG.error("createRole() failed: HTTP status=" + response.getStatus() + ", message=" + resp.getMessage() + ", isSecure=" + isSecureMode + (isSecureMode ? (", user=" + user) : ""));

			if(response.getStatus()==HttpServletResponse.SC_UNAUTHORIZED) {
				throw new AccessControlException();
			}

			throw new Exception("HTTP " + response.getStatus() + " Error: " + resp.getMessage());
		} else if(response == null) {
			throw new Exception("unknown error during createRole. roleName="  + request.getName());
		} else {
			ret = response.getEntity(RangerRole.class);
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAdminRESTClient.createRole(" + request + ")");
		}
		return ret;
	}

	@Override
	public void dropRole(final String execUser, final String roleName) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAdminRESTClient.dropRole(" + roleName + ")");
		}

		ClientResponse response = null;
		UserGroupInformation user = MiscUtil.getUGILoginUser();
		boolean isSecureMode = user != null && UserGroupInformation.isSecurityEnabled();

		Map<String, String> queryParams = new HashMap<String, String>();
		queryParams.put(RangerRESTUtils.SERVICE_NAME_PARAM, serviceNameUrlParam);
		queryParams.put(RangerRESTUtils.REST_PARAM_EXEC_USER, execUser);

		String relativeURL = RangerRESTUtils.REST_URL_SERVICE_DROP_ROLE + roleName;

		if (isSecureMode) {
			PrivilegedAction<ClientResponse> action = new PrivilegedAction<ClientResponse>() {
				public ClientResponse run() {
					ClientResponse clientRes = null;
					try {
						clientRes = restClient.delete(relativeURL, queryParams);
					} catch (Exception e) {
						LOG.error("Failed to get response, Error is : "+e.getMessage());
					}
					return clientRes;
				}
			};
			if (LOG.isDebugEnabled()) {
				LOG.debug("drop role as user " + user);
			}
			response = user.doAs(action);
		} else {
			response = restClient.delete(relativeURL, queryParams);
		}
		if(response == null) {
			throw new Exception("unknown error during deleteRole. roleName="  + roleName);
		} else if(response.getStatus() != HttpServletResponse.SC_OK && response.getStatus() != HttpServletResponse.SC_NO_CONTENT) {
			RESTResponse resp = RESTResponse.fromClientResponse(response);
			LOG.error("createRole() failed: HTTP status=" + response.getStatus() + ", message=" + resp.getMessage() + ", isSecure=" + isSecureMode + (isSecureMode ? (", user=" + user) : ""));

			if(response.getStatus()==HttpServletResponse.SC_UNAUTHORIZED) {
				throw new AccessControlException();
			}

			throw new Exception("HTTP " + response.getStatus() + " Error: " + resp.getMessage());
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAdminRESTClient.deleteRole(" + roleName + ")");
		}
	}

	@Override
	public List<String> getUserRoles(final String execUser) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAdminRESTClient.getUserRoles(" + execUser + ")");
		}

		List<String> ret = null;
		String emptyString = "";
		ClientResponse response = null;
		UserGroupInformation user = MiscUtil.getUGILoginUser();
		boolean isSecureMode = user != null && UserGroupInformation.isSecurityEnabled();
		String relativeURL = RangerRESTUtils.REST_URL_SERVICE_GET_USER_ROLES + execUser;

		if (isSecureMode) {
			PrivilegedAction<ClientResponse> action = new PrivilegedAction<ClientResponse>() {
				public ClientResponse run() {
					ClientResponse clientRes = null;
					try {
						clientRes = restClient.get(relativeURL, null);
					} catch (Exception e) {
						LOG.error("Failed to get response, Error is : "+e.getMessage());
					}
					return clientRes;
				}
			};
			if (LOG.isDebugEnabled()) {
				LOG.debug("get roles as user " + user);
			}
			response = user.doAs(action);
		} else {
			response = restClient.get(relativeURL, null);
		}
		if(response != null) {
			if (response.getStatus() != HttpServletResponse.SC_OK) {
				RESTResponse resp = RESTResponse.fromClientResponse(response);
				LOG.error("getUserRoles() failed: HTTP status=" + response.getStatus() + ", message=" + resp.getMessage() + ", isSecure=" + isSecureMode + (isSecureMode ? (", user=" + user) : ""));

				if (response.getStatus() == HttpServletResponse.SC_UNAUTHORIZED) {
					throw new AccessControlException();
				}

				throw new Exception("HTTP " + response.getStatus() + " Error: " + resp.getMessage());
			} else {
				ret = response.getEntity(getGenericType(emptyString));
			}
		} else {
			throw new Exception("unknown error during getUserRoles. execUser="  + execUser);
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAdminRESTClient.getUserRoles(" + execUser + ")");
		}
		return ret;
	}

	@Override
	public List<String> getAllRoles(final String execUser) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAdminRESTClient.getAllRoles()");
		}

		List<String> ret = null;
		String emptyString = "";
		ClientResponse response = null;
		UserGroupInformation user = MiscUtil.getUGILoginUser();
		boolean isSecureMode = user != null && UserGroupInformation.isSecurityEnabled();
		String relativeURL = RangerRESTUtils.REST_URL_SERVICE_GET_ALL_ROLES;

		Map<String, String> queryParams = new HashMap<String, String>();
		queryParams.put(RangerRESTUtils.SERVICE_NAME_PARAM, serviceNameUrlParam);
		queryParams.put(RangerRESTUtils.REST_PARAM_EXEC_USER, execUser);

		if (isSecureMode) {
			PrivilegedAction<ClientResponse> action = new PrivilegedAction<ClientResponse>() {
				public ClientResponse run() {
					ClientResponse clientRes = null;
					try {
						clientRes = restClient.get(relativeURL, queryParams);
					} catch (Exception e) {
						LOG.error("Failed to get response, Error is : "+e.getMessage());
					}
				return clientRes;
				}
			};
			if (LOG.isDebugEnabled()) {
				LOG.debug("get roles as user " + user);
			}
			response = user.doAs(action);
		} else {
			response = restClient.get(relativeURL, queryParams);
		}
		if(response != null) {
			if (response.getStatus() != HttpServletResponse.SC_OK) {
				RESTResponse resp = RESTResponse.fromClientResponse(response);
				LOG.error("getAllRoles() failed: HTTP status=" + response.getStatus() + ", message=" + resp.getMessage() + ", isSecure=" + isSecureMode + (isSecureMode ? (", user=" + user) : ""));

				if (response.getStatus() == HttpServletResponse.SC_UNAUTHORIZED) {
					throw new AccessControlException();
				}

				throw new Exception("HTTP " + response.getStatus() + " Error: " + resp.getMessage());
			} else {
				ret = response.getEntity(getGenericType(emptyString));
			}
		} else {
			throw new Exception("unknown error during getAllRoles.");
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAdminRESTClient.getAllRoles()");
		}
		return ret;
	}

	@Override
	public RangerRole getRole(final String execUser, final String roleName) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAdminRESTClient.getPrincipalsForRole(" + roleName + ")");
		}

		RangerRole ret = null;
		ClientResponse response = null;
		UserGroupInformation user = MiscUtil.getUGILoginUser();
		boolean isSecureMode = user != null && UserGroupInformation.isSecurityEnabled();
		String relativeURL = RangerRESTUtils.REST_URL_SERVICE_GET_ROLE_INFO + roleName;

		Map<String, String> queryParams = new HashMap<String, String>();
		queryParams.put(RangerRESTUtils.SERVICE_NAME_PARAM, serviceNameUrlParam);
		queryParams.put(RangerRESTUtils.REST_PARAM_EXEC_USER, execUser);

		if (isSecureMode) {
			PrivilegedAction<ClientResponse> action = new PrivilegedAction<ClientResponse>() {
				public ClientResponse run() {
					ClientResponse clientResp = null;
					try {
						clientResp = restClient.get(relativeURL, queryParams);
					} catch (Exception e) {
						LOG.error("Failed to get response, Error is : "+e.getMessage());
					}
				return clientResp;
				}
			};
			if (LOG.isDebugEnabled()) {
				LOG.debug("get role info as user " + user);
			}
			response = user.doAs(action);
		} else {
			response = restClient.get(relativeURL, queryParams);
		}
		if(response != null) {
			if (response.getStatus() != HttpServletResponse.SC_OK) {
				RESTResponse resp = RESTResponse.fromClientResponse(response);
				LOG.error("getPrincipalsForRole() failed: HTTP status=" + response.getStatus() + ", message=" + resp.getMessage() + ", isSecure=" + isSecureMode + (isSecureMode ? (", user=" + user) : ""));

				if (response.getStatus() == HttpServletResponse.SC_UNAUTHORIZED) {
					throw new AccessControlException();
				}

				throw new Exception("HTTP " + response.getStatus() + " Error: " + resp.getMessage());
			} else {
				ret = response.getEntity(RangerRole.class);
			}
		} else {
			throw new Exception("unknown error during getPrincipalsForRole. roleName="  + roleName);
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAdminRESTClient.getPrincipalsForRole(" + roleName + ")");
		}
		return ret;
	}


	@Override
	public void grantRole(final GrantRevokeRoleRequest request) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAdminRESTClient.grantRole(" + request + ")");
		}

		ClientResponse response = null;
		UserGroupInformation user = MiscUtil.getUGILoginUser();
		boolean isSecureMode = user != null && UserGroupInformation.isSecurityEnabled();
		String relativeURL = RangerRESTUtils.REST_URL_SERVICE_GRANT_ROLE + serviceNameUrlParam;

		if (isSecureMode) {
			PrivilegedAction<ClientResponse> action = new PrivilegedAction<ClientResponse>() {
				public ClientResponse run() {
					ClientResponse clientResp = null;
					try {
						clientResp = restClient.put(relativeURL, null, request);
					} catch (Exception e) {
						LOG.error("Failed to get response, Error is : "+e.getMessage());
					}
				return clientResp;
				}
			};
			if (LOG.isDebugEnabled()) {
				LOG.debug("grant role as user " + user);
			}
			response = user.doAs(action);
		} else {
			response = restClient.put(relativeURL, null, request);
		}
		if(response != null && response.getStatus() != HttpServletResponse.SC_OK) {
			RESTResponse resp = RESTResponse.fromClientResponse(response);
			LOG.error("grantRole() failed: HTTP status=" + response.getStatus() + ", message=" + resp.getMessage() + ", isSecure=" + isSecureMode + (isSecureMode ? (", user=" + user) : ""));

			if(response.getStatus()==HttpServletResponse.SC_UNAUTHORIZED) {
				throw new AccessControlException();
			}

			throw new Exception("HTTP " + response.getStatus() + " Error: " + resp.getMessage());
		} else if(response == null) {
			throw new Exception("unknown error during grantRole. serviceName="  + serviceName);
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAdminRESTClient.grantRole(" + request + ")");
		}
	}

	@Override
	public void revokeRole(final GrantRevokeRoleRequest request) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAdminRESTClient.revokeRole(" + request + ")");
		}

		ClientResponse response = null;
		UserGroupInformation user = MiscUtil.getUGILoginUser();
		boolean isSecureMode = user != null && UserGroupInformation.isSecurityEnabled();
		String relativeURL = RangerRESTUtils.REST_URL_SERVICE_REVOKE_ROLE + serviceNameUrlParam;

		if (isSecureMode) {
			PrivilegedAction<ClientResponse> action = new PrivilegedAction<ClientResponse>() {
				public ClientResponse run() {
					ClientResponse clientResp = null;
					try {
						clientResp = restClient.put(relativeURL, null, request);
					} catch (Exception e) {
						LOG.error("Failed to get response, Error is : "+e.getMessage());
					}
				return clientResp;
				}
			};
			if (LOG.isDebugEnabled()) {
				LOG.debug("revoke role as user " + user);
			}
			response = user.doAs(action);
		} else {
			response = restClient.put(relativeURL, null, request);
		}
		if(response != null && response.getStatus() != HttpServletResponse.SC_OK) {
			RESTResponse resp = RESTResponse.fromClientResponse(response);
			LOG.error("revokeRole() failed: HTTP status=" + response.getStatus() + ", message=" + resp.getMessage() + ", isSecure=" + isSecureMode + (isSecureMode ? (", user=" + user) : ""));

			if(response.getStatus()==HttpServletResponse.SC_UNAUTHORIZED) {
				throw new AccessControlException();
			}

			throw new Exception("HTTP " + response.getStatus() + " Error: " + resp.getMessage());
		} else if(response == null) {
			throw new Exception("unknown error during revokeRole. serviceName="  + serviceName);
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAdminRESTClient.revokeRole(" + request + ")");
		}
	}

	@Override
	public void grantAccess(final GrantRevokeRequest request) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAdminRESTClient.grantAccess(" + request + ")");
		}

		ClientResponse response = null;
		UserGroupInformation user = MiscUtil.getUGILoginUser();
		boolean isSecureMode = user != null && UserGroupInformation.isSecurityEnabled();

		Map<String, String> queryParams = new HashMap<String, String>();
		queryParams.put(RangerRESTUtils.REST_PARAM_PLUGIN_ID, pluginId);

		if (isSecureMode) {
			PrivilegedAction<ClientResponse> action = new PrivilegedAction<ClientResponse>() {
				public ClientResponse run() {
					String relativeURL = RangerRESTUtils.REST_URL_SECURE_SERVICE_GRANT_ACCESS + serviceNameUrlParam;
					ClientResponse clientResp = null;
					try {
						clientResp = restClient.post(relativeURL, queryParams, request);
					} catch (Exception e) {
						LOG.error("Failed to get response, Error is : "+e.getMessage());
					}
				return clientResp;
				}
			};
			if (LOG.isDebugEnabled()) {
				LOG.debug("grantAccess as user " + user);
			}
			response = user.doAs(action);
		} else {
			String relativeURL = RangerRESTUtils.REST_URL_SERVICE_GRANT_ACCESS + serviceNameUrlParam;
			response = restClient.post(relativeURL, queryParams, request);
		}
		if(response != null && response.getStatus() != HttpServletResponse.SC_OK) {
			RESTResponse resp = RESTResponse.fromClientResponse(response);
			LOG.error("grantAccess() failed: HTTP status=" + response.getStatus() + ", message=" + resp.getMessage() + ", isSecure=" + isSecureMode + (isSecureMode ? (", user=" + user) : ""));

			if(response.getStatus()==HttpServletResponse.SC_UNAUTHORIZED) {
				throw new AccessControlException();
			}

			throw new Exception("HTTP " + response.getStatus() + " Error: " + resp.getMessage());
		} else if(response == null) {
			throw new Exception("unknown error during grantAccess. serviceName="  + serviceName);
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAdminRESTClient.grantAccess(" + request + ")");
		}
	}

	@Override
	public void revokeAccess(final GrantRevokeRequest request) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAdminRESTClient.revokeAccess(" + request + ")");
		}

		ClientResponse response = null;
		UserGroupInformation user = MiscUtil.getUGILoginUser();
		boolean isSecureMode = user != null && UserGroupInformation.isSecurityEnabled();

		Map<String, String> queryParams = new HashMap<String, String>();
		queryParams.put(RangerRESTUtils.REST_PARAM_PLUGIN_ID, pluginId);

		if (isSecureMode) {
			PrivilegedAction<ClientResponse> action = new PrivilegedAction<ClientResponse>() {
				public ClientResponse run() {
					String relativeURL = RangerRESTUtils.REST_URL_SECURE_SERVICE_REVOKE_ACCESS + serviceNameUrlParam;
					ClientResponse clientResp = null;
					try {
						clientResp = restClient.post(relativeURL, queryParams, request);
					} catch (Exception e) {
						LOG.error("Failed to get response, Error is : "+e.getMessage());
					}
				return clientResp;
				}
			};
			if (LOG.isDebugEnabled()) {
				LOG.debug("revokeAccess as user " + user);
			}
			response = user.doAs(action);
		} else {
			String relativeURL = RangerRESTUtils.REST_URL_SERVICE_REVOKE_ACCESS + serviceNameUrlParam;
			response = restClient.post(relativeURL, queryParams, request);
		}

		if(response != null && response.getStatus() != HttpServletResponse.SC_OK) {
			RESTResponse resp = RESTResponse.fromClientResponse(response);
			LOG.error("revokeAccess() failed: HTTP status=" + response.getStatus() + ", message=" + resp.getMessage() + ", isSecure=" + isSecureMode + (isSecureMode ? (", user=" + user) : ""));

			if(response.getStatus() == HttpServletResponse.SC_UNAUTHORIZED) {
				throw new AccessControlException();
			}

			throw new Exception("HTTP " + response.getStatus() + " Error: " + resp.getMessage());
		} else if(response == null) {
			throw new Exception("unknown error. revokeAccess(). serviceName=" + serviceName);
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAdminRESTClient.revokeAccess(" + request + ")");
		}
	}

	private void init(String url, String sslConfigFileName, int restClientConnTimeOutMs , int restClientReadTimeOutMs, Configuration config) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAdminRESTClient.init(" + url + ", " + sslConfigFileName + ")");
		}

		restClient = new RangerRESTClient(url, sslConfigFileName, config);
		restClient.setRestClientConnTimeOutMs(restClientConnTimeOutMs);
		restClient.setRestClientReadTimeOutMs(restClientReadTimeOutMs);

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAdminRESTClient.init(" + url + ", " + sslConfigFileName + ")");
		}
	}

	@Override
	public ServiceTags getServiceTagsIfUpdated(final long lastKnownVersion, final long lastActivationTimeInMillis) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAdminRESTClient.getServiceTagsIfUpdated(" + lastKnownVersion + ", " + lastActivationTimeInMillis + "): ");
		}

		ServiceTags ret = null;
		ClientResponse response = null;
		UserGroupInformation user = MiscUtil.getUGILoginUser();
		boolean isSecureMode = user != null && UserGroupInformation.isSecurityEnabled();

		Map<String, String> queryParams = new HashMap<String, String>();
		queryParams.put(RangerRESTUtils.LAST_KNOWN_TAG_VERSION_PARAM, Long.toString(lastKnownVersion));
		queryParams.put(RangerRESTUtils.REST_PARAM_LAST_ACTIVATION_TIME, Long.toString(lastActivationTimeInMillis));
		queryParams.put(RangerRESTUtils.REST_PARAM_PLUGIN_ID, pluginId);
		queryParams.put(RangerRESTUtils.REST_PARAM_SUPPORTS_TAG_DELTAS, supportsTagDeltas);
		queryParams.put(RangerRESTUtils.REST_PARAM_CAPABILITIES, pluginCapabilities);

		if (isSecureMode) {
			PrivilegedAction<ClientResponse> action = new PrivilegedAction<ClientResponse>() {
				public ClientResponse run() {
					String relativeURL = RangerRESTUtils.REST_URL_GET_SECURE_SERVICE_TAGS_IF_UPDATED + serviceNameUrlParam;
					ClientResponse clientResp = null;
					try {
						clientResp = restClient.get(relativeURL, queryParams);
					} catch (Exception e) {
						LOG.error("Failed to get response, Error is : "+e.getMessage());
					}
					return clientResp;
				}
			};
			if (LOG.isDebugEnabled()) {
				LOG.debug("getServiceTagsIfUpdated as user " + user);
			}
			response = user.doAs(action);
		} else {
			String relativeURL = RangerRESTUtils.REST_URL_GET_SERVICE_TAGS_IF_UPDATED + serviceNameUrlParam;
			response = restClient.get(relativeURL, queryParams);
		}

		if (response == null || response.getStatus() == HttpServletResponse.SC_NOT_MODIFIED) {
			if (response == null) {
				LOG.error("Error getting tags; Received NULL response!!. secureMode=" + isSecureMode + ", user=" + user + ", serviceName=" + serviceName);
			} else {
				RESTResponse resp = RESTResponse.fromClientResponse(response);
				if (LOG.isDebugEnabled()) {
					LOG.debug("No change in tags. secureMode=" + isSecureMode + ", user=" + user
							+ ", response=" + resp + ", serviceName=" + serviceName
							+ ", " + "lastKnownVersion=" + lastKnownVersion
							+ ", " + "lastActivationTimeInMillis=" + lastActivationTimeInMillis);
				}
			}
			ret = null;
		} else if (response.getStatus() == HttpServletResponse.SC_OK) {
			ret = response.getEntity(ServiceTags.class);
		} else if (response.getStatus() == HttpServletResponse.SC_NOT_FOUND) {
			LOG.error("Error getting tags; service not found. secureMode=" + isSecureMode + ", user=" + user
					+ ", response=" + response.getStatus() + ", serviceName=" + serviceName
					+ ", " + "lastKnownVersion=" + lastKnownVersion
					+ ", " + "lastActivationTimeInMillis=" + lastActivationTimeInMillis);
			String exceptionMsg = response.hasEntity() ? response.getEntity(String.class) : null;

			RangerServiceNotFoundException.throwExceptionIfServiceNotFound(serviceName, exceptionMsg);

			LOG.warn("Received 404 error code with body:[" + exceptionMsg + "], Ignoring");
		} else {
			RESTResponse resp = RESTResponse.fromClientResponse(response);
			LOG.warn("Error getting tags. secureMode=" + isSecureMode + ", user=" + user + ", response=" + resp + ", serviceName=" + serviceName);
			ret = null;
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAdminRESTClient.getServiceTagsIfUpdated(" + lastKnownVersion + ", " + lastActivationTimeInMillis + "): ");
		}

		return ret;
	}

	@Override
	public List<String> getTagTypes(String pattern) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAdminRESTClient.getTagTypes(" + pattern + "): ");
		}

		List<String> ret = null;
		String emptyString = "";
		UserGroupInformation user = MiscUtil.getUGILoginUser();
		boolean isSecureMode = user != null && UserGroupInformation.isSecurityEnabled();

		Map<String, String> queryParams = new HashMap<String, String>();
		queryParams.put(RangerRESTUtils.SERVICE_NAME_PARAM, serviceNameUrlParam);
		queryParams.put(RangerRESTUtils.PATTERN_PARAM, pattern);
		String relativeURL = RangerRESTUtils.REST_URL_LOOKUP_TAG_NAMES;

		ClientResponse response = null;
		if (isSecureMode) {
			PrivilegedAction<ClientResponse> action = new PrivilegedAction<ClientResponse>() {
				public ClientResponse run() {
					ClientResponse clientResp = null;
					try {
						clientResp = restClient.get(relativeURL, queryParams);
					} catch (Exception e) {
						LOG.error("Failed to get response, Error is : "+e.getMessage());
					}
				return clientResp;
				}
			};
			if (LOG.isDebugEnabled()) {
				LOG.debug("getTagTypes as user " + user);
			}
			response = user.doAs(action);
		} else {
			response = restClient.get(relativeURL, queryParams);
		}

		if(response != null && response.getStatus() == HttpServletResponse.SC_OK) {
			ret = response.getEntity(getGenericType(emptyString));
		} else {
			RESTResponse resp = RESTResponse.fromClientResponse(response);
			LOG.error("Error getting tags. response=" + resp + ", serviceName=" + serviceName + ", " + "pattern=" + pattern);
			throw new Exception(resp.getMessage());
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAdminRESTClient.getTagTypes(" + pattern + "): " + ret);
		}

		return ret;
	}

	@Override
	public RangerUserStore getUserStoreIfUpdated(long lastKnownUserStoreVersion, long lastActivationTimeInMillis) throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAdminRESTClient.getUserStoreIfUpdated(" + lastKnownUserStoreVersion + ", " + lastActivationTimeInMillis + ")");
		}

		final RangerUserStore ret;
		final UserGroupInformation user = MiscUtil.getUGILoginUser();
		final boolean isSecureMode = user != null && UserGroupInformation.isSecurityEnabled();
		final ClientResponse response;

		Map<String, String> queryParams = new HashMap<String, String>();
		queryParams.put(RangerRESTUtils.REST_PARAM_LAST_KNOWN_USERSTORE_VERSION, Long.toString(lastKnownUserStoreVersion));
		queryParams.put(RangerRESTUtils.REST_PARAM_LAST_ACTIVATION_TIME, Long.toString(lastActivationTimeInMillis));
		queryParams.put(RangerRESTUtils.REST_PARAM_PLUGIN_ID, pluginId);
		queryParams.put(RangerRESTUtils.REST_PARAM_CLUSTER_NAME, clusterName);
		queryParams.put(RangerRESTUtils.REST_PARAM_CAPABILITIES, pluginCapabilities);

		if (isSecureMode) {
			if (LOG.isDebugEnabled()) {
				LOG.debug("Checking UserStore updated as user : " + user);
			}
			PrivilegedAction<ClientResponse> action = new PrivilegedAction<ClientResponse>() {
				public ClientResponse run() {
					ClientResponse clientRes = null;
					String relativeURL = RangerRESTUtils.REST_URL_SERVICE_SERCURE_GET_USERSTORE + serviceNameUrlParam;
					try {
						clientRes =  restClient.get(relativeURL, queryParams);
					} catch (Exception e) {
						LOG.error("Failed to get response, Error is : "+e.getMessage());
					}
					return clientRes;
				}
			};
			response = user.doAs(action);
		} else {
			if (LOG.isDebugEnabled()) {
				LOG.debug("Checking UserStore updated as user : " + user);
			}
			String relativeURL = RangerRESTUtils.REST_URL_SERVICE_SERCURE_GET_USERSTORE + serviceNameUrlParam;
			response = restClient.get(relativeURL, queryParams);
		}

		if (response == null || response.getStatus() == HttpServletResponse.SC_NOT_MODIFIED) {
			if (response == null) {
				LOG.error("Error getting UserStore; Received NULL response!!. secureMode=" + isSecureMode + ", user=" + user + ", serviceName=" + serviceName);
			} else {
				RESTResponse resp = RESTResponse.fromClientResponse(response);
				if (LOG.isDebugEnabled()) {
					LOG.debug("No change in UserStore. secureMode=" + isSecureMode + ", user=" + user
							+ ", response=" + resp + ", serviceName=" + serviceName
							+ ", " + "lastKnownUserStoreVersion=" + lastKnownUserStoreVersion
							+ ", " + "lastActivationTimeInMillis=" + lastActivationTimeInMillis);
				}
			}
			ret = null;
		} else if (response.getStatus() == HttpServletResponse.SC_OK) {
			ret = response.getEntity(RangerUserStore.class);
		} else if (response.getStatus() == HttpServletResponse.SC_NOT_FOUND) {
			ret = null;
			LOG.error("Error getting UserStore; service not found. secureMode=" + isSecureMode + ", user=" + user
					+ ", response=" + response.getStatus() + ", serviceName=" + serviceName
					+ ", " + "lastKnownUserStoreVersion=" + lastKnownUserStoreVersion
					+ ", " + "lastActivationTimeInMillis=" + lastActivationTimeInMillis);
			String exceptionMsg = response.hasEntity() ? response.getEntity(String.class) : null;

			RangerServiceNotFoundException.throwExceptionIfServiceNotFound(serviceName, exceptionMsg);

			LOG.warn("Received 404 error code with body:[" + exceptionMsg + "], Ignoring");
		} else {
			RESTResponse resp = RESTResponse.fromClientResponse(response);
			LOG.warn("Error getting UserStore. secureMode=" + isSecureMode + ", user=" + user + ", response=" + resp + ", serviceName=" + serviceName);
			ret = null;
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAdminRESTClient.getUserStoreIfUpdated(" + lastKnownUserStoreVersion + ", " + lastActivationTimeInMillis + "): ");
		}

		return ret;
	}

}
