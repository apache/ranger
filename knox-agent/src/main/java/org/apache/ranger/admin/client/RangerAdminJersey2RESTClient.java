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
import java.security.PrivilegedAction;
import java.util.Date;
import java.util.List;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSession;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.ranger.plugin.util.*;
import org.apache.ranger.audit.provider.MiscUtil;
import org.apache.ranger.authorization.hadoop.config.RangerConfiguration;
import org.glassfish.jersey.client.ClientProperties;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonParseException;

public class RangerAdminJersey2RESTClient implements RangerAdminClient {

	// none of the members are public -- this is only for testability.  None of these is meant to be accessible
	private static final Log LOG = LogFactory.getLog(RangerAdminJersey2RESTClient.class);
	RangerRESTUtils _utils = new RangerRESTUtils();
	
	boolean _isSSL = false;
	volatile Client _client = null;
	SSLContext _sslContext = null;
	HostnameVerifier _hv;
	String _baseUrl = null;
	String _sslConfigFileName = null;
	String _serviceName = null;
	String _clusterName = null;
	String _pluginId = null;
	int	   _restClientConnTimeOutMs;
	int	   _restClientReadTimeOutMs;

	@Override
	public void init(String serviceName, String appId, String configPropertyPrefix) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAdminJersey2RESTClient.init(" + configPropertyPrefix + ")");
		}

		_serviceName = serviceName;
		_pluginId = _utils.getPluginId(serviceName, appId);
		_baseUrl = _utils.getPolicyRestUrl(configPropertyPrefix);
		_sslConfigFileName = _utils.getSsslConfigFileName(configPropertyPrefix);
		_isSSL = _utils.isSsl(_baseUrl);
		_restClientConnTimeOutMs = RangerConfiguration.getInstance().getInt(configPropertyPrefix + ".policy.rest.client.connection.timeoutMs", 120 * 1000);
		_restClientReadTimeOutMs = RangerConfiguration.getInstance().getInt(configPropertyPrefix + ".policy.rest.client.read.timeoutMs", 30 * 1000);
		_clusterName = RangerConfiguration.getInstance().get(configPropertyPrefix + ".ambari.cluster.name", "");

		LOG.info("Init params: " + String.format("Base URL[%s], SSL Congig filename[%s], ServiceName=[%s]", _baseUrl, _sslConfigFileName, _serviceName));
		
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

		String url = null;
		ServicePolicies servicePolicies = null;
		Response response = null;

		if (isSecureMode) {
			if (LOG.isDebugEnabled()) {
				LOG.debug("Checking Service policy if updated as user : " + user);
			}
			url = _utils.getSecureUrlForPolicyUpdate(_baseUrl, _serviceName);
			final String secureUrl = url;
			PrivilegedAction<Response> action = new PrivilegedAction<Response>() {
				public Response run() {
					return _client.target(secureUrl)
							.queryParam(RangerRESTUtils.REST_PARAM_LAST_KNOWN_POLICY_VERSION, Long.toString(lastKnownVersion))
							.queryParam(RangerRESTUtils.REST_PARAM_LAST_ACTIVATION_TIME, Long.toString(lastActivationTimeInMillis))
							.queryParam(RangerRESTUtils.REST_PARAM_PLUGIN_ID, _pluginId)
							.queryParam(RangerRESTUtils.REST_PARAM_CLUSTER_NAME, _clusterName)
							.request(MediaType.APPLICATION_JSON_TYPE)
							.get();
				}
			};
			response = user.doAs(action);
		} else {
			if (LOG.isDebugEnabled()) {
				LOG.debug("Checking Service policy if updated with old api call");
			}
			url = _utils.getUrlForPolicyUpdate(_baseUrl, _serviceName);
			response = _client.target(url)
					.queryParam(RangerRESTUtils.REST_PARAM_LAST_KNOWN_POLICY_VERSION, Long.toString(lastKnownVersion))
					.queryParam(RangerRESTUtils.REST_PARAM_LAST_ACTIVATION_TIME, Long.toString(lastActivationTimeInMillis))
					.queryParam(RangerRESTUtils.REST_PARAM_PLUGIN_ID, _pluginId)
					.queryParam(RangerRESTUtils.REST_PARAM_CLUSTER_NAME, _clusterName)
					.request(MediaType.APPLICATION_JSON_TYPE)
					.get();
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
				LOG.warn(String.format("Unexpected: Received status[%d] with body[%s] form url[%s]", httpResponseCode, body, url));
				break;
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAdminJersey2RESTClient.getServicePoliciesIfUpdated(" + lastKnownVersion + ", " + lastActivationTimeInMillis + "): " + servicePolicies);
		}
		return servicePolicies;
	}

	@Override
	public void grantAccess(GrantRevokeRequest request) throws Exception {

		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAdminRESTClient.grantAccess(" + request + ")");
		}

		String url = _utils.getUrlForGrantAccess(_baseUrl, _serviceName);
		Response response = _client.target(url)
				.queryParam(RangerRESTUtils.REST_PARAM_PLUGIN_ID, _pluginId)
				.request(MediaType.APPLICATION_JSON_TYPE)
				.get();
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
			String message = String.format("Unexpected: Received status[%d] with body[%s] form url[%s]", httpResponseCode, body, url);
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

		String url = _utils.getUrlForRevokeAccess(_baseUrl, _serviceName);
		Response response = _client.target(url)
				.queryParam(RangerRESTUtils.REST_PARAM_PLUGIN_ID, _pluginId)
				.request(MediaType.APPLICATION_JSON_TYPE)
				.get();
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
			String message = String.format("Unexpected: Received status[%d] with body[%s] form url[%s]", httpResponseCode, body, url);
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

		String url = null;
		ServiceTags serviceTags = null;
		Response response = null;
		if (isSecureMode) {
			if (LOG.isDebugEnabled()) {
				LOG.debug("Checking Service tags if updated as user : " + user);
			}
			url = _utils.getSecureUrlForTagUpdate(_baseUrl, _serviceName);
			final String secureUrl = url;
			PrivilegedAction<Response> action = new PrivilegedAction<Response>() {
				public Response run() {
					return _client.target(secureUrl)
							.queryParam(RangerRESTUtils.LAST_KNOWN_TAG_VERSION_PARAM, Long.toString(lastKnownVersion))
							.queryParam(RangerRESTUtils.REST_PARAM_LAST_ACTIVATION_TIME, Long.toString(lastActivationTimeInMillis))
							.queryParam(RangerRESTUtils.REST_PARAM_PLUGIN_ID, _pluginId)
							.request(MediaType.APPLICATION_JSON_TYPE)
							.get();
				}
			};
			response = user.doAs(action);
		} else {
			if (LOG.isDebugEnabled()) {
				LOG.debug("Checking Service tags if updated with old api call");
			}
			url = _utils.getUrlForTagUpdate(_baseUrl, _serviceName);
			response = _client.target(url)
					.queryParam(RangerRESTUtils.LAST_KNOWN_TAG_VERSION_PARAM, Long.toString(lastKnownVersion))
					.queryParam(RangerRESTUtils.REST_PARAM_LAST_ACTIVATION_TIME, Long.toString(lastActivationTimeInMillis))
					.queryParam(RangerRESTUtils.REST_PARAM_PLUGIN_ID, _pluginId)
					.request(MediaType.APPLICATION_JSON_TYPE)
					.get();
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
				LOG.warn(String.format("Unexpected: Received status[%d] with body[%s] form url[%s]", httpResponseCode, body, url));
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
}
