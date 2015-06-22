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

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.security.AccessControlException;
import org.apache.ranger.admin.client.datatype.RESTResponse;
import org.apache.ranger.authorization.hadoop.config.RangerConfiguration;

import org.apache.ranger.plugin.model.RangerTaggedResource;
import org.apache.ranger.plugin.util.GrantRevokeRequest;
import org.apache.ranger.plugin.util.RangerRESTClient;
import org.apache.ranger.plugin.util.RangerRESTUtils;
import org.apache.ranger.plugin.util.ServicePolicies;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;
import java.util.Set;


public class RangerAdminRESTClient implements RangerAdminClient {
	private static final Log LOG = LogFactory.getLog(RangerAdminRESTClient.class);
 
	private String           serviceName = null;
	private String           pluginId    = null;
	private RangerRESTClient restClient  = null;
	private RangerRESTUtils  restUtils   = new RangerRESTUtils();


	public RangerAdminRESTClient() {
	}

	@Override
	public void init(String serviceName, String appId, String propertyPrefix) {
		this.serviceName = serviceName;
		this.pluginId    = restUtils.getPluginId(serviceName, appId);

		String url               = RangerConfiguration.getInstance().get(propertyPrefix + ".policy.rest.url");
		String sslConfigFileName = RangerConfiguration.getInstance().get(propertyPrefix + ".policy.rest.ssl.config.file");

		init(url, sslConfigFileName);
	}

	@Override
	public ServicePolicies getServicePoliciesIfUpdated(long lastKnownVersion) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAdminRESTClient.getServicePoliciesIfUpdated(" + lastKnownVersion + ")");
		}

		ServicePolicies ret = null;

		WebResource webResource = createWebResource(RangerRESTUtils.REST_URL_POLICY_GET_FOR_SERVICE_IF_UPDATED + serviceName)
										.queryParam(RangerRESTUtils.REST_PARAM_LAST_KNOWN_POLICY_VERSION, Long.toString(lastKnownVersion))
										.queryParam(RangerRESTUtils.REST_PARAM_PLUGIN_ID, pluginId);
		ClientResponse response = webResource.accept(RangerRESTUtils.REST_MIME_TYPE_JSON).get(ClientResponse.class);

		if(response != null && response.getStatus() == 200) {
			ret = response.getEntity(ServicePolicies.class);
		} else if(response != null && response.getStatus() == 304) {
			// no change
		} else {
			RESTResponse resp = RESTResponse.fromClientResponse(response);
			LOG.error("Error getting policies. request=" + webResource.toString() 
					+ ", response=" + resp.toString() + ", serviceName=" + serviceName);
			throw new Exception(resp.getMessage());
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAdminRESTClient.getServicePoliciesIfUpdated(" + lastKnownVersion + "): " + ret);
		}

		return ret;
	}

	@Override
	public void grantAccess(GrantRevokeRequest request) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAdminRESTClient.grantAccess(" + request + ")");
		}

		WebResource webResource = createWebResource(RangerRESTUtils.REST_URL_SERVICE_GRANT_ACCESS + serviceName)
										.queryParam(RangerRESTUtils.REST_PARAM_PLUGIN_ID, pluginId);
		ClientResponse response = webResource.accept(RangerRESTUtils.REST_EXPECTED_MIME_TYPE).type(RangerRESTUtils.REST_EXPECTED_MIME_TYPE).post(ClientResponse.class, restClient.toJson(request));

		if(response != null && response.getStatus() != 200) {
			LOG.error("grantAccess() failed: HTTP status=" + response.getStatus());

			if(response.getStatus() == 401) {
				throw new AccessControlException();
			}

			throw new Exception("HTTP " + response.getStatus());
		} else if(response == null) {
			throw new Exception("unknown error during grantAccess. serviceName="  + serviceName);
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAdminRESTClient.grantAccess(" + request + ")");
		}
	}

	@Override
	public void revokeAccess(GrantRevokeRequest request) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAdminRESTClient.revokeAccess(" + request + ")");
		}

		WebResource webResource = createWebResource(RangerRESTUtils.REST_URL_SERVICE_REVOKE_ACCESS + serviceName)
										.queryParam(RangerRESTUtils.REST_PARAM_PLUGIN_ID, pluginId);
		ClientResponse response = webResource.accept(RangerRESTUtils.REST_EXPECTED_MIME_TYPE).type(RangerRESTUtils.REST_EXPECTED_MIME_TYPE).post(ClientResponse.class, restClient.toJson(request));

		if(response != null && response.getStatus() != 200) {
			LOG.error("revokeAccess() failed: HTTP status=" + response.getStatus());

			if(response.getStatus() == 401) {
				throw new AccessControlException();
			}

			throw new Exception("HTTP " + response.getStatus());
		} else if(response == null) {
			throw new Exception("unknown error. revokeAccess(). serviceName=" + serviceName);
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAdminRESTClient.revokeAccess(" + request + ")");
		}
	}

	private void init(String url, String sslConfigFileName) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAdminRESTClient.init(" + url + ", " + sslConfigFileName + ")");
		}

		restClient = new RangerRESTClient(url, sslConfigFileName);

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAdminRESTClient.init(" + url + ", " + sslConfigFileName + ")");
		}
	}

	private WebResource createWebResource(String url) {
		WebResource ret = restClient.getResource(url);
		
		return ret;
	}

	public void init(String serviceName, Map<String, String> configs) {
		this.serviceName = serviceName;
		// Get all configuration parameter to connect to DGI from configs
		String url               = configs.get("URL");
		String sslConfigFileName = configs.get("SSL_CONFIG_FILE_NAME");
		String userName = configs.get("username");
		String password = configs.get("password");

		init(url, sslConfigFileName);
		if (restClient != null) {
			restClient.setBasicAuthInfo(userName, password);
		}
	}


	public  List<RangerTaggedResource> getTaggedResources(String componentType) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAdminRESTClient.getTaggedResources(" +  serviceName + ", " + componentType + "): ");
		}

		ParameterizedType parameterizedGenericType = new ParameterizedType() {
			public Type[] getActualTypeArguments() {
				return new Type[] { new RangerTaggedResource().getClass() };
			}

			public Type getRawType() {
				return List.class;
			}

			public Type getOwnerType() {
				return List.class;
			}
		};

		GenericType<List<RangerTaggedResource>> genericType = new GenericType<List<RangerTaggedResource>>(
				parameterizedGenericType) {
		};

		List<RangerTaggedResource> ret;

		WebResource webResource = createWebResource(RangerRESTUtils.REST_URL_GET_TAGGED_RESOURCES)
				.queryParam(RangerRESTUtils.TAG_SERVICE_NAME_PARAM, serviceName)
				.queryParam(RangerRESTUtils.COMPONENT_TYPE_PARAM, componentType);
		ClientResponse response = webResource.accept(RangerRESTUtils.REST_MIME_TYPE_JSON).get(ClientResponse.class);

		if(response != null && response.getStatus() == 200) {
			ret = response.getEntity(genericType);
		} else {
			RESTResponse resp = RESTResponse.fromClientResponse(response);
			LOG.error("Error getting taggedResources. request=" + webResource.toString()
					+ ", response=" + resp.toString() + ", serviceName=" + serviceName + ", componentType=" + componentType);
			throw new Exception(resp.getMessage());
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAdminRESTClient.getTaggedResources(" +  serviceName + ", " + componentType + "): " + ret);
		}

		return ret;
	}

	public Set<String> getTagNames(String componentType, String tagNamePattern) throws Exception {
		// TODO
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAdminRESTClient.getTagNames(" +  serviceName + ", " + componentType
					+ ", " + tagNamePattern + "): ");
		}

		Set<String> ret = null;

		WebResource webResource = createWebResource(RangerRESTUtils.REST_URL_LOOKUP_TAG_NAMES)
				.queryParam(RangerRESTUtils.TAG_SERVICE_NAME_PARAM, serviceName)
				.queryParam(RangerRESTUtils.TAG_PATTERN_PARAM, tagNamePattern);

		if (StringUtils.isNotBlank(componentType)) {
			webResource.queryParam(RangerRESTUtils.COMPONENT_TYPE_PARAM, componentType);
		}

		ClientResponse response = webResource.accept(RangerRESTUtils.REST_MIME_TYPE_JSON).get(ClientResponse.class);

		if(response != null && response.getStatus() == 200) {
			ret = (Set<String>)response.getEntity(Set.class);
		} else {
			RESTResponse resp = RESTResponse.fromClientResponse(response);
			LOG.error("Error getting taggedResources. request=" + webResource.toString()
					+ ", response=" + resp.toString() + ", serviceName=" + serviceName + ", componentType=" + componentType);
			throw new Exception(resp.getMessage());
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAdminRESTClient.getTagNames(" +  serviceName + ", " + componentType + ", " + tagNamePattern + "): " + ret);
		}

		return ret;
	}

}
