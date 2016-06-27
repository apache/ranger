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
import com.sun.jersey.api.client.WebResource;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.ranger.admin.client.datatype.RESTResponse;
import org.apache.ranger.audit.provider.MiscUtil;
import org.apache.ranger.authorization.hadoop.config.RangerConfiguration;
import org.apache.ranger.plugin.util.*;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.security.PrivilegedAction;
import java.util.List;

public class RangerAdminRESTClient implements RangerAdminClient {
	private static final Log LOG = LogFactory.getLog(RangerAdminRESTClient.class);
 
	private String           serviceName = null;
	private String           pluginId    = null;
	private RangerRESTClient restClient  = null;
	private RangerRESTUtils  restUtils   = new RangerRESTUtils();

	public RangerAdminRESTClient() {
	}

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
	public void init(String serviceName, String appId, String propertyPrefix) {
		this.serviceName = serviceName;
		this.pluginId    = restUtils.getPluginId(serviceName, appId);

		String url               		= RangerConfiguration.getInstance().get(propertyPrefix + ".policy.rest.url");
		String sslConfigFileName 		= RangerConfiguration.getInstance().get(propertyPrefix + ".policy.rest.ssl.config.file");
		int	 restClientConnTimeOutMs	= RangerConfiguration.getInstance().getInt(propertyPrefix + ".policy.rest.client.connection.timeoutMs", 120 * 1000);
		int	 restClientReadTimeOutMs	= RangerConfiguration.getInstance().getInt(propertyPrefix + ".policy.rest.client.read.timeoutMs", 30 * 1000);
		
		init(url, sslConfigFileName, restClientConnTimeOutMs , restClientReadTimeOutMs);
	}

	@Override
	public ServicePolicies getServicePoliciesIfUpdated(final long lastKnownVersion) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAdminRESTClient.getServicePoliciesIfUpdated(" + lastKnownVersion + ")");
		}

		ServicePolicies ret = null;
		UserGroupInformation user = MiscUtil.getUGILoginUser();
		boolean isSecureMode = user != null && UserGroupInformation.isSecurityEnabled();

		ClientResponse response = null;
		if (isSecureMode) {
			if(LOG.isDebugEnabled()) {
				LOG.debug("Checking Service policy if updated as user : " + user);
			}
			PrivilegedAction<ClientResponse> action = new PrivilegedAction<ClientResponse>() {
				public ClientResponse run() {
					WebResource secureWebResource = createWebResource(RangerRESTUtils.REST_URL_POLICY_GET_FOR_SECURE_SERVICE_IF_UPDATED + serviceName)
												.queryParam(RangerRESTUtils.REST_PARAM_LAST_KNOWN_POLICY_VERSION, Long.toString(lastKnownVersion))
												.queryParam(RangerRESTUtils.REST_PARAM_PLUGIN_ID, pluginId);
					return secureWebResource.accept(RangerRESTUtils.REST_MIME_TYPE_JSON).get(ClientResponse.class);
				};
			};				
			response = user.doAs(action);
		}else{
			if(LOG.isDebugEnabled()) {
				LOG.debug("Checking Service policy if updated with old api call");
			}
			WebResource webResource = createWebResource(RangerRESTUtils.REST_URL_POLICY_GET_FOR_SERVICE_IF_UPDATED + serviceName)
                                                                                .queryParam(RangerRESTUtils.REST_PARAM_LAST_KNOWN_POLICY_VERSION, Long.toString(lastKnownVersion))
                                                                                .queryParam(RangerRESTUtils.REST_PARAM_PLUGIN_ID, pluginId);
			response = webResource.accept(RangerRESTUtils.REST_MIME_TYPE_JSON).get(ClientResponse.class);
		}
		
		if(response != null && response.getStatus() == 200) {
			ret = response.getEntity(ServicePolicies.class);
		} else if(response != null && response.getStatus() == 304) {
			// no change
		} else {
			RESTResponse resp = RESTResponse.fromClientResponse(response);
			LOG.error("Error getting policies. secureMode=" + isSecureMode + ", user=" + user + ", response=" + resp.toString() + ", serviceName=" + serviceName);

			throw new Exception(resp.getMessage());
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAdminRESTClient.getServicePoliciesIfUpdated(" + lastKnownVersion + "): " + ret);
		}

		return ret;
	}

	@Override
	public void grantAccess(final GrantRevokeRequest request) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAdminRESTClient.grantAccess(" + request + ")");
		}

		ClientResponse response = null;
		UserGroupInformation user = MiscUtil.getUGILoginUser();
		boolean isSecureMode = user != null && UserGroupInformation.isSecurityEnabled();

		if (isSecureMode) {
			PrivilegedAction<ClientResponse> action = new PrivilegedAction<ClientResponse>() {
				public ClientResponse run() {
					WebResource secureWebResource = createWebResource(RangerRESTUtils.REST_URL_SECURE_SERVICE_GRANT_ACCESS + serviceName)
							.queryParam(RangerRESTUtils.REST_PARAM_PLUGIN_ID, pluginId);
					return secureWebResource.accept(RangerRESTUtils.REST_EXPECTED_MIME_TYPE).type(RangerRESTUtils.REST_EXPECTED_MIME_TYPE).post(ClientResponse.class, restClient.toJson(request));
				};
			};
			if (LOG.isDebugEnabled()) {
				LOG.debug("grantAccess as user " + user);
			}
			response = user.doAs(action);
		} else {
			WebResource webResource = createWebResource(RangerRESTUtils.REST_URL_SERVICE_GRANT_ACCESS + serviceName)
                                                                                .queryParam(RangerRESTUtils.REST_PARAM_PLUGIN_ID, pluginId);
			response = webResource.accept(RangerRESTUtils.REST_EXPECTED_MIME_TYPE).type(RangerRESTUtils.REST_EXPECTED_MIME_TYPE).post(ClientResponse.class, restClient.toJson(request));
		}
		if(response != null && response.getStatus() != 200) {
			RESTResponse resp = RESTResponse.fromClientResponse(response);
			LOG.error("grantAccess() failed: HTTP status=" + response.getStatus() + ", message=" + resp.getMessage() + ", isSecure=" + isSecureMode + (isSecureMode ? (", user=" + user) : ""));

			if(response.getStatus() == 401) {
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

		if (isSecureMode) {
			PrivilegedAction<ClientResponse> action = new PrivilegedAction<ClientResponse>() {
				public ClientResponse run() {
					WebResource secureWebResource = createWebResource(RangerRESTUtils.REST_URL_SECURE_SERVICE_REVOKE_ACCESS + serviceName)
							.queryParam(RangerRESTUtils.REST_PARAM_PLUGIN_ID, pluginId);
					return secureWebResource.accept(RangerRESTUtils.REST_EXPECTED_MIME_TYPE).type(RangerRESTUtils.REST_EXPECTED_MIME_TYPE).post(ClientResponse.class, restClient.toJson(request));
				};
			};
			if (LOG.isDebugEnabled()) {
				LOG.debug("revokeAccess as user " + user);
			}
			response = user.doAs(action);
		} else {
			WebResource webResource = createWebResource(RangerRESTUtils.REST_URL_SERVICE_REVOKE_ACCESS + serviceName)
                                                                                .queryParam(RangerRESTUtils.REST_PARAM_PLUGIN_ID, pluginId);
			response = webResource.accept(RangerRESTUtils.REST_EXPECTED_MIME_TYPE).type(RangerRESTUtils.REST_EXPECTED_MIME_TYPE).post(ClientResponse.class, restClient.toJson(request));
		}

		if(response != null && response.getStatus() != 200) {
			RESTResponse resp = RESTResponse.fromClientResponse(response);
			LOG.error("revokeAccess() failed: HTTP status=" + response.getStatus() + ", message=" + resp.getMessage() + ", isSecure=" + isSecureMode + (isSecureMode ? (", user=" + user) : ""));

			if(response.getStatus() == 401) {
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

	private void init(String url, String sslConfigFileName, int restClientConnTimeOutMs , int restClientReadTimeOutMs ) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAdminRESTClient.init(" + url + ", " + sslConfigFileName + ")");
		}

		restClient = new RangerRESTClient(url, sslConfigFileName);
		restClient.setRestClientConnTimeOutMs(restClientConnTimeOutMs);
		restClient.setRestClientReadTimeOutMs(restClientReadTimeOutMs);

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAdminRESTClient.init(" + url + ", " + sslConfigFileName + ")");
		}
	}

	private WebResource createWebResource(String url) {
		WebResource ret = restClient.getResource(url);
		
		return ret;
	}

	@Override
	public ServiceTags getServiceTagsIfUpdated(final long lastKnownVersion) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAdminRESTClient.getServiceTagsIfUpdated(" + lastKnownVersion + "): ");
		}

		ServiceTags ret = null;
		ClientResponse response = null;
		WebResource webResource = null;
		UserGroupInformation user = MiscUtil.getUGILoginUser();
		boolean isSecureMode = user != null && UserGroupInformation.isSecurityEnabled();

		if (isSecureMode) {
			PrivilegedAction<ClientResponse> action = new PrivilegedAction<ClientResponse>() {
				public ClientResponse run() {
					WebResource secureWebResource = createWebResource(RangerRESTUtils.REST_URL_GET_SECURE_SERVICE_TAGS_IF_UPDATED + serviceName)
							.queryParam(RangerRESTUtils.LAST_KNOWN_TAG_VERSION_PARAM, Long.toString(lastKnownVersion))
							.queryParam(RangerRESTUtils.REST_PARAM_PLUGIN_ID, pluginId);
					return secureWebResource.accept(RangerRESTUtils.REST_MIME_TYPE_JSON).get(ClientResponse.class);
				};
			};
			if (LOG.isDebugEnabled()) {
				LOG.debug("getServiceTagsIfUpdated as user " + user);
			}
			response = user.doAs(action);
		} else {
			webResource = createWebResource(RangerRESTUtils.REST_URL_GET_SERVICE_TAGS_IF_UPDATED + serviceName)
					.queryParam(RangerRESTUtils.LAST_KNOWN_TAG_VERSION_PARAM, Long.toString(lastKnownVersion))
					.queryParam(RangerRESTUtils.REST_PARAM_PLUGIN_ID, pluginId);
			response = webResource.accept(RangerRESTUtils.REST_MIME_TYPE_JSON).get(ClientResponse.class);
		}

		if(response != null && response.getStatus() == 200) {
			ret = response.getEntity(ServiceTags.class);
		} else if(response != null && response.getStatus() == 304) {
			// no change
		} else {
			RESTResponse resp = RESTResponse.fromClientResponse(response);
			LOG.error("Error getting taggedResources. secureMode=" + isSecureMode + ", user=" + user
					+ ", response=" + resp.toString() + ", serviceName=" + serviceName
					+ ", " + "lastKnownVersion=" + lastKnownVersion);
			throw new Exception(resp.getMessage());
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAdminRESTClient.getServiceTagsIfUpdated(" + lastKnownVersion + "): ");
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

		final WebResource webResource = createWebResource(RangerRESTUtils.REST_URL_LOOKUP_TAG_NAMES)
				.queryParam(RangerRESTUtils.SERVICE_NAME_PARAM, serviceName)
				.queryParam(RangerRESTUtils.PATTERN_PARAM, pattern);

		ClientResponse response = null;
		if (isSecureMode) {
			PrivilegedAction<ClientResponse> action = new PrivilegedAction<ClientResponse>() {
				public ClientResponse run() {
					return webResource.accept(RangerRESTUtils.REST_MIME_TYPE_JSON).get(ClientResponse.class);
				};
			};
			if (LOG.isDebugEnabled()) {
				LOG.debug("getTagTypes as user " + user);
			}
			response = user.doAs(action);
		} else {
			response = webResource.accept(RangerRESTUtils.REST_MIME_TYPE_JSON).get(ClientResponse.class);
		}

		if(response != null && response.getStatus() == 200) {
			ret = response.getEntity(getGenericType(emptyString));
		} else {
			RESTResponse resp = RESTResponse.fromClientResponse(response);
			LOG.error("Error getting taggedResources. request=" + webResource.toString()
					+ ", response=" + resp.toString() + ", serviceName=" + serviceName
					+ ", " + "pattern=" + pattern);
			throw new Exception(resp.getMessage());
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAdminRESTClient.getTagTypes(" + pattern + "): " + ret);
		}

		return ret;
	}

}
