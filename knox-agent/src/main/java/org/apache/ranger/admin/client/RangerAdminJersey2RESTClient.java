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

import java.lang.reflect.Type;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.security.PrivilegedAction;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSession;
import javax.ws.rs.ProcessingException;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.ranger.plugin.util.*;
import org.apache.ranger.audit.provider.MiscUtil;
import org.apache.ranger.authorization.utils.StringUtil;
import org.glassfish.jersey.client.ClientProperties;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonParseException;
import com.sun.jersey.api.client.ClientHandlerException;

public class RangerAdminJersey2RESTClient extends AbstractRangerAdminClient {

	// none of the members are public -- this is only for testability.  None of these is meant to be accessible
	private static final Log LOG = LogFactory.getLog(RangerAdminJersey2RESTClient.class);

	boolean _isSSL = false;
	volatile Client _client = null;
	SSLContext _sslContext = null;
	HostnameVerifier _hv;
	String _sslConfigFileName = null;
	String _serviceName = null;
	String _clusterName = null;
	String _supportsPolicyDeltas = null;
	String _supportsTagDeltas = null;
	String _pluginId = null;
	int	   _restClientConnTimeOutMs;
	int	   _restClientReadTimeOutMs;
	private int lastKnownActiveUrlIndex;
	private List<String> configURLs;
	private final String   pluginCapabilities = Long.toHexString(new RangerPluginCapability().getPluginCapabilities());
	private static final int MAX_PLUGIN_ID_LEN = 255;

	@Override
	public void init(String serviceName, String appId, String configPropertyPrefix, Configuration config) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAdminJersey2RESTClient.init(" + configPropertyPrefix + ")");
		}

		super.init(serviceName, appId, configPropertyPrefix, config);

		_serviceName             = serviceName;
		_pluginId 		         = getPluginId(serviceName, appId);
		String tmpUrl 		     = config.get(configPropertyPrefix + ".policy.rest.url");
		_sslConfigFileName 		 = config.get(configPropertyPrefix + ".policy.rest.ssl.config.file");
		_restClientConnTimeOutMs = config.getInt(configPropertyPrefix + ".policy.rest.client.connection.timeoutMs", 120 * 1000);
		_restClientReadTimeOutMs = config.getInt(configPropertyPrefix + ".policy.rest.client.read.timeoutMs", 30 * 1000);
		_clusterName             = config.get(configPropertyPrefix + ".access.cluster.name", "");
		if(StringUtil.isEmpty(_clusterName)){
			_clusterName =config.get(configPropertyPrefix + ".ambari.cluster.name", "");
		}
		_supportsPolicyDeltas = config.get(configPropertyPrefix + ".policy.rest.supports.policy.deltas", "false");
		if (!"true".equalsIgnoreCase(_supportsPolicyDeltas)) {
			_supportsPolicyDeltas = "false";
		}
		_supportsTagDeltas = config.get(configPropertyPrefix + ".tag.rest.supports.tag.deltas", "false");
		if (!"true".equalsIgnoreCase(_supportsTagDeltas)) {
			_supportsTagDeltas = "false";
		}

		configURLs = StringUtil.getURLs(tmpUrl);
		this.lastKnownActiveUrlIndex = new Random().nextInt(configURLs.size());
		String url = configURLs.get(this.lastKnownActiveUrlIndex);
		_isSSL = isSsl(url);
		LOG.info("Init params: " + String.format("Base URL[%s], SSL Config filename[%s], ServiceName=[%s], SupportsPolicyDeltas=[%s], ConfigURLs=[%s]", url, _sslConfigFileName, _serviceName, _supportsPolicyDeltas, _supportsTagDeltas, configURLs));
		
		_client = getClient();
		_client.property(ClientProperties.CONNECT_TIMEOUT, _restClientConnTimeOutMs);
		_client.property(ClientProperties.READ_TIMEOUT, _restClientReadTimeOutMs);

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAdminJersey2RESTClient.init(" + configPropertyPrefix + "): " + _client.toString());
		}
	}

	@Override
	public ServicePolicies getServicePoliciesIfUpdated(final long lastKnownVersion, final long lastActivationTimeInMillis) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAdminJersey2RESTClient.getServicePoliciesIfUpdated(" + lastKnownVersion + ", " + lastActivationTimeInMillis + ")");
		}

		UserGroupInformation user = MiscUtil.getUGILoginUser();
		boolean isSecureMode = user != null && UserGroupInformation.isSecurityEnabled();

		String relativeURL = null;
		ServicePolicies servicePolicies = null;
		Response response = null;

		Map<String, String> queryParams = new HashMap<String, String>();
		queryParams.put(RangerRESTUtils.REST_PARAM_LAST_KNOWN_POLICY_VERSION, Long.toString(lastKnownVersion));
		queryParams.put(RangerRESTUtils.REST_PARAM_LAST_ACTIVATION_TIME, Long.toString(lastActivationTimeInMillis));
		queryParams.put(RangerRESTUtils.REST_PARAM_PLUGIN_ID, _pluginId);
		queryParams.put(RangerRESTUtils.REST_PARAM_CLUSTER_NAME, _clusterName);
		queryParams.put(RangerRESTUtils.REST_PARAM_SUPPORTS_POLICY_DELTAS, _supportsPolicyDeltas);
		queryParams.put(RangerRESTUtils.REST_PARAM_CAPABILITIES, pluginCapabilities);

		if (isSecureMode) {
			if (LOG.isDebugEnabled()) {
				LOG.debug("Checking Service policy if updated as user : " + user);
			}
			relativeURL = RangerRESTUtils.REST_URL_POLICY_GET_FOR_SECURE_SERVICE_IF_UPDATED + _serviceName;
			final String secureRelativeUrl = relativeURL;
			PrivilegedAction<Response> action = new PrivilegedAction<Response>() {
				public Response run() {
					return get(queryParams, secureRelativeUrl);
				}
			};
			response = user.doAs(action);
		} else {
			if (LOG.isDebugEnabled()) {
				LOG.debug("Checking Service policy if updated with old api call");
			}
			relativeURL = RangerRESTUtils.REST_URL_POLICY_GET_FOR_SERVICE_IF_UPDATED + _serviceName;
			response = get(queryParams, relativeURL);
		}

		int httpResponseCode = response == null ? -1 : response.getStatus();
		String body = null;

		switch (httpResponseCode) {
			case 200:
				body = response.readEntity(String.class);

				if (LOG.isDebugEnabled()) {
					LOG.debug("Response from 200 server: " + body);
				}

				Gson gson = getGson();
				servicePolicies = gson.fromJson(body, ServicePolicies.class);

				if (LOG.isDebugEnabled()) {
					LOG.debug("Deserialized response to: " + servicePolicies);
				}
				break;
			case 304:
				LOG.debug("Got response: 304. Ok. Returning null");
				break;
			case -1:
				LOG.warn("Unexpected: Null response from policy server while trying to get policies! Returning null!");
				break;
			case 404: {
				if (response.hasEntity()) {
					body = response.readEntity(String.class);
					if (StringUtils.isNotBlank(body)) {
						RangerServiceNotFoundException.throwExceptionIfServiceNotFound(_serviceName, body);
					}
				}
				LOG.warn("Received 404 error code with body:[" + body + "], Ignoring");
				break;
			}
			default:
				body = response.readEntity(String.class);
				LOG.warn(String.format("Unexpected: Received status[%d] with body[%s] form url[%s]", httpResponseCode, body, relativeURL));
				break;
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAdminJersey2RESTClient.getServicePoliciesIfUpdated(" + lastKnownVersion + ", " + lastActivationTimeInMillis + "): " + servicePolicies);
		}
		return servicePolicies;
	}

	@Override
	public RangerRoles getRolesIfUpdated(final long lastKnowRoleVersion, final long lastActivationTimeInMillis) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAdminJersey2RESTClient.getRolesIfUpdated(" + lastKnowRoleVersion + ", " + lastActivationTimeInMillis + ")");
		}

		UserGroupInformation user = MiscUtil.getUGILoginUser();
		boolean isSecureMode = user != null && UserGroupInformation.isSecurityEnabled();

		String      relativeURL = null;
		RangerRoles ret         = null;
		Response    response    = null;

		Map<String, String> queryParams = new HashMap<String, String>();
		queryParams.put(RangerRESTUtils.REST_PARAM_LAST_KNOWN_ROLE_VERSION, Long.toString(lastKnowRoleVersion));
		queryParams.put(RangerRESTUtils.REST_PARAM_LAST_ACTIVATION_TIME, Long.toString(lastActivationTimeInMillis));
		queryParams.put(RangerRESTUtils.REST_PARAM_PLUGIN_ID, _pluginId);
		queryParams.put(RangerRESTUtils.REST_PARAM_CLUSTER_NAME, _clusterName);
		queryParams.put(RangerRESTUtils.REST_PARAM_CAPABILITIES, pluginCapabilities);

		if (isSecureMode) {
			if (LOG.isDebugEnabled()) {
				LOG.debug("Checking Roles if updated as user : " + user);
			}

			relativeURL = RangerRESTUtils.REST_URL_SERVICE_SERCURE_GET_USER_GROUP_ROLES + _serviceName;
			final String secureRelativeUrl = relativeURL;
			PrivilegedAction<Response> action = new PrivilegedAction<Response>() {
				public Response run() {
					return get(queryParams, secureRelativeUrl);
				}
			};
			response = user.doAs(action);
		} else {
			if (LOG.isDebugEnabled()) {
				LOG.debug("Checking Roles if updated with old api call");
			}

			relativeURL = RangerRESTUtils.REST_URL_SERVICE_GET_USER_GROUP_ROLES + _serviceName;
			response = get(queryParams, relativeURL);
		}

		int httpResponseCode = response == null ? -1 : response.getStatus();
		String body = null;

		switch (httpResponseCode) {
			case 200:
				body = response.readEntity(String.class);

				if (LOG.isDebugEnabled()) {
					LOG.debug("Response from 200 server: " + body);
				}

				Gson gson = getGson();
				ret = gson.fromJson(body, RangerRoles.class);

				if (LOG.isDebugEnabled()) {
					LOG.debug("Deserialized response to: " + ret);
				}
				break;
			case 304:
				LOG.debug("Got response: 304. Ok. Returning null");
				break;
			case -1:
				LOG.warn("Unexpected: Null response from policy server while trying to get policies! Returning null!");
				break;
			case 404: {
				if (response.hasEntity()) {
					body = response.readEntity(String.class);
					if (StringUtils.isNotBlank(body)) {
						RangerServiceNotFoundException.throwExceptionIfServiceNotFound(_serviceName, body);
					}
				}
				LOG.warn("Received 404 error code with body:[" + body + "], Ignoring");
				break;
			}
			default:
				body = response.readEntity(String.class);
				LOG.warn(String.format("Unexpected: Received status[%d] with body[%s] form url[%s]", httpResponseCode, body, relativeURL));
				break;
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAdminJersey2RESTClient.getRolesIfUpdated(" + lastKnowRoleVersion + ", " + lastActivationTimeInMillis + "): " + ret);
		}
		return ret;
	}

	@Override
	public void grantAccess(GrantRevokeRequest request) throws Exception {

		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAdminRESTClient.grantAccess(" + request + ")");
		}

		Map<String, String> queryParams = new HashMap<String, String>();
		queryParams.put(RangerRESTUtils.REST_PARAM_PLUGIN_ID, _pluginId);

		String relativeURL = RangerRESTUtils.REST_URL_SERVICE_GRANT_ACCESS + _serviceName;
		Response response = get(queryParams, relativeURL);

		int httpResponseCode = response == null ? -1 : response.getStatus();
		
		switch(httpResponseCode) {
		case -1:
			LOG.warn("Unexpected: Null response from policy server while granting access! Returning null!");
			throw new Exception("unknown error!");
		case 200:
			LOG.debug("grantAccess() suceeded: HTTP status=" + httpResponseCode);
			break;
		case 401:
			throw new AccessControlException();
		default:
			String body = response.readEntity(String.class);
			String message = String.format("Unexpected: Received status[%d] with body[%s] form url[%s]", httpResponseCode, body, relativeURL);
			LOG.warn(message);
			throw new Exception("HTTP status: " + httpResponseCode);
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAdminRESTClient.grantAccess(" + request + ")");
		}
	}

	@Override
	public void revokeAccess(GrantRevokeRequest request) throws Exception {

		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAdminRESTClient.grantAccess(" + request + ")");
		}

		Map<String, String> queryParams = new HashMap<String, String>();
		queryParams.put(RangerRESTUtils.REST_PARAM_PLUGIN_ID, _pluginId);

		String relativeURL = RangerRESTUtils.REST_URL_SERVICE_REVOKE_ACCESS + _serviceName;
		Response response = get(queryParams, relativeURL);

		int httpResponseCode = response == null ? -1 : response.getStatus();
		
		switch(httpResponseCode) {
		case -1:
			LOG.warn("Unexpected: Null response from policy server while granting access! Returning null!");
			throw new Exception("unknown error!");
		case 200:
			LOG.debug("grantAccess() suceeded: HTTP status=" + httpResponseCode);
			break;
		case 401:
			throw new AccessControlException();
		default:
			String body = response.readEntity(String.class);
			String message = String.format("Unexpected: Received status[%d] with body[%s] form url[%s]", httpResponseCode, body, relativeURL);
			LOG.warn(message);
			throw new Exception("HTTP status: " + httpResponseCode);
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAdminRESTClient.grantAccess(" + request + ")");
		}
	}

	@Override
	public ServiceTags getServiceTagsIfUpdated(final long lastKnownVersion, final long lastActivationTimeInMillis) throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAdminJersey2RESTClient.getServiceTagsIfUpdated(" + lastKnownVersion + ", " + lastActivationTimeInMillis + ")");
		}

		UserGroupInformation user = MiscUtil.getUGILoginUser();
		boolean isSecureMode = user != null && UserGroupInformation.isSecurityEnabled();

		Map<String, String> queryParams = new HashMap<String, String>();
		queryParams.put(RangerRESTUtils.REST_PARAM_LAST_KNOWN_POLICY_VERSION, Long.toString(lastKnownVersion));
		queryParams.put(RangerRESTUtils.REST_PARAM_LAST_ACTIVATION_TIME, Long.toString(lastActivationTimeInMillis));
		queryParams.put(RangerRESTUtils.REST_PARAM_PLUGIN_ID, _pluginId);
		queryParams.put(RangerRESTUtils.REST_PARAM_SUPPORTS_TAG_DELTAS, _supportsTagDeltas);
		queryParams.put(RangerRESTUtils.REST_PARAM_CAPABILITIES, pluginCapabilities);

		String relativeURL = null;
		ServiceTags serviceTags = null;
		Response response = null;
		if (isSecureMode) {
			if (LOG.isDebugEnabled()) {
				LOG.debug("Checking Service tags if updated as user : " + user);
			}
			relativeURL = RangerRESTUtils.REST_URL_GET_SECURE_SERVICE_TAGS_IF_UPDATED + _serviceName;
			final String secureRelativeURLUrl = relativeURL;
			PrivilegedAction<Response> action = new PrivilegedAction<Response>() {
				public Response run() {
					return get(queryParams, secureRelativeURLUrl);
				}
			};
			response = user.doAs(action);
		} else {
			if (LOG.isDebugEnabled()) {
				LOG.debug("Checking Service tags if updated with old api call");
			}
			relativeURL = RangerRESTUtils.REST_URL_GET_SERVICE_TAGS_IF_UPDATED + _serviceName;
			response = get(queryParams, relativeURL);
		}

		int httpResponseCode = response == null ? -1 : response.getStatus();
		String body = null;

		switch (httpResponseCode) {
			case 200:
				body = response.readEntity(String.class);

				if (LOG.isDebugEnabled()) {
					LOG.debug("Response from 200 server: " + body);
				}

				Gson gson = getGson();
				serviceTags = gson.fromJson(body, ServiceTags.class);

				if (LOG.isDebugEnabled()) {
					LOG.debug("Deserialized response to: " + serviceTags);
				}
				break;
			case 304:
				LOG.debug("Got response: 304. Ok. Returning null");
				break;
			case -1:
				LOG.warn("Unexpected: Null response from tag server while trying to get tags! Returning null!");
				break;
			case 404:
				if (response.hasEntity()) {
					body = response.readEntity(String.class);
					if (StringUtils.isNotBlank(body)) {
						RangerServiceNotFoundException.throwExceptionIfServiceNotFound(_serviceName, body);
					}
				}
				LOG.warn("Received 404 error code with body:[" + body + "], Ignoring");
				break;
			default:
				body = response.readEntity(String.class);
				LOG.warn(String.format("Unexpected: Received status[%d] with body[%s] form url[%s]", httpResponseCode, body, relativeURL));
				break;
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAdminJersey2RESTClient.getServiceTagsIfUpdated(" + lastKnownVersion + ", " + lastActivationTimeInMillis + "): " + serviceTags);
		}
		return serviceTags;
	}

	@Override
	public List<String> getTagTypes(String pattern) throws Exception {
		throw new Exception("RangerAdminjersey2RESTClient.getTagTypes() -- *** NOT IMPLEMENTED *** ");
	}

	// We get date from the policy manager as unix long!  This deserializer exists to deal with it.  Remove this class once we start send date/time per RFC 3339
	public static class GsonUnixDateDeserializer implements JsonDeserializer<Date> {

		@Override
		public Date deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context) throws JsonParseException {
			return new Date(json.getAsJsonPrimitive().getAsLong());
		}

	}

	// package level methods left so (and not private only for testability!)  Not intended for use outside this class!!
	Gson getGson() {
		return new GsonBuilder()
			.setPrettyPrinting()
			// We get date from the policy manager as unix long!  This deserializer exists to deal with it.  Remove this class once we start send date/time per RFC 3339
			.registerTypeAdapter(Date.class, new GsonUnixDateDeserializer())
			.create();
	}
	
	Client getClient() {
		Client result = _client;
		if(result == null) {
			synchronized(this) {
				result = _client;
				if(result == null) {
					_client = result = buildClient();
				}
			}
		}

		return result;
	}

	Client buildClient() {
		
		if (_isSSL) {
			if (_sslContext == null) {
				RangerSslHelper sslHelper = new RangerSslHelper(_sslConfigFileName);
				_sslContext = sslHelper.createContext();
			}
			if (_hv == null) {
				_hv = new HostnameVerifier() {
					public boolean verify(String urlHostName, SSLSession session) {
						return session.getPeerHost().equals(urlHostName);
					}
				};
			}				
			_client = ClientBuilder.newBuilder()
					.sslContext(_sslContext)
					.hostnameVerifier(_hv)
					.build();
		}

		if(_client == null) {
			_client = ClientBuilder.newClient();
		}
		
		return _client;
	}

	private Response get(Map<String, String> queyParams, String relativeURL) {
		Response response = null;
		int startIndex = this.lastKnownActiveUrlIndex;
        int currentIndex = 0;

		for (int index = 0; index < configURLs.size(); index++) {
			try {
				currentIndex = (startIndex + index) % configURLs.size();

				WebTarget target = _client.target(configURLs.get(currentIndex) + relativeURL);
				response = setQueryParams(target, queyParams).request(MediaType.APPLICATION_JSON_TYPE).get();
				if (response != null) {
					setLastKnownActiveUrlIndex(currentIndex);
					break;
				}
			} catch (ProcessingException e) {
				LOG.warn("Failed to communicate with Ranger Admin, URL : " + configURLs.get(currentIndex));
				if (index == configURLs.size() - 1) {
					throw new ClientHandlerException(
							"Failed to communicate with all Ranger Admin's URL's : [ " + configURLs + " ]", e);
				}
			}
		}
		return response;
	}

	private static WebTarget setQueryParams(WebTarget target, Map<String, String> params) {
		WebTarget ret = target;
		if (target != null && params != null) {
			Set<Map.Entry<String, String>> entrySet = params.entrySet();
			for (Map.Entry<String, String> entry : entrySet) {
				ret = ret.queryParam(entry.getKey(), entry.getValue());
			}
		}
		return ret;
	}

	private void setLastKnownActiveUrlIndex(int lastKnownActiveUrlIndex) {
		this.lastKnownActiveUrlIndex = lastKnownActiveUrlIndex;
	}

	private boolean isSsl(String url) {
		return !StringUtils.isEmpty(url) && url.toLowerCase().startsWith("https");
	}

	private String getPluginId(String serviceName, String appId) {
		String hostName = null;

		try {
			hostName = InetAddress.getLocalHost().getHostName();
		} catch (UnknownHostException e) {
			LOG.error("ERROR: Unable to find hostname for the agent ", e);
			hostName = "unknownHost";
		}

		String ret  = hostName + "-" + serviceName;

		if(! StringUtils.isEmpty(appId)) {
			ret = appId + "@" + ret;
		}

		if (ret.length() > MAX_PLUGIN_ID_LEN ) {
			ret = ret.substring(0,MAX_PLUGIN_ID_LEN);
		}

		return ret ;
	}
}
