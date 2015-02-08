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
import com.sun.jersey.api.client.WebResource;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ranger.admin.client.datatype.RESTResponse;
import org.apache.ranger.authorization.hadoop.config.RangerConfiguration;
import org.apache.ranger.plugin.util.GrantRevokeRequest;
import org.apache.ranger.plugin.util.RangerRESTClient;
import org.apache.ranger.plugin.util.ServicePolicies;


public class RangerAdminRESTClient implements RangerAdminClient {
	private static final Log LOG = LogFactory.getLog(RangerAdminRESTClient.class);

	public final String REST_URL_POLICY_GET_FOR_SERVICE_IF_UPDATED = "/service/plugins/policies/download/";
	public final String REST_URL_SERVICE_GRANT_ACCESS              = "/service/plugins/services/grant/";
	public final String REST_URL_SERVICE_REVOKE_ACCESS             = "/service/plugins/services/revoke/";

	public static final String REST_EXPECTED_MIME_TYPE = "application/json" ;
	public static final String REST_MIME_TYPE_JSON = "application/json" ;

	private RangerRESTClient restClient = null;


	public RangerAdminRESTClient() {
		String url               = RangerConfiguration.getInstance().get("ranger.service.store.rest.url");
		String sslConfigFileName = RangerConfiguration.getInstance().get("ranger.service.store.rest.ssl.config.file");

		init(url, sslConfigFileName);
	}

	public RangerAdminRESTClient(String url, String sslConfigFileName) {
		init(url, sslConfigFileName);
	}

	@Override
	public ServicePolicies getServicePoliciesIfUpdated(String serviceName, long lastKnownVersion) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAdminRESTClient.getServicePoliciesIfUpdated(" + serviceName + ", " + lastKnownVersion + ")");
		}

		ServicePolicies ret = null;

		WebResource    webResource = createWebResource(REST_URL_POLICY_GET_FOR_SERVICE_IF_UPDATED + serviceName + "/" + lastKnownVersion);
		ClientResponse response    = webResource.accept(REST_MIME_TYPE_JSON).get(ClientResponse.class);

		if(response != null && response.getStatus() == 200) {
			ret = response.getEntity(ServicePolicies.class);
		} else if(response != null && response.getStatus() == 304) {
			// no change
		} else {
			RESTResponse resp = RESTResponse.fromClientResponse(response);

			throw new Exception(resp.getMessage());
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAdminRESTClient.getServicePoliciesIfUpdated(" + serviceName + ", " + lastKnownVersion + "): " + ret);
		}

		return ret;
	}

	@Override
	public void grantAccess(String serviceName, GrantRevokeRequest request) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAdminRESTClient.grantAccess(" + serviceName + ", " + request + ")");
		}

		WebResource    webResource = createWebResource(REST_URL_SERVICE_GRANT_ACCESS + serviceName);
		ClientResponse response    = webResource.accept(REST_EXPECTED_MIME_TYPE).type(REST_EXPECTED_MIME_TYPE).post(ClientResponse.class, restClient.toJson(request));

		if(response == null || response.getStatus() != 200) {
			RESTResponse resp = RESTResponse.fromClientResponse(response);

			throw new Exception(resp.getMessage());
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAdminRESTClient.grantAccess(" + serviceName + ", " + request + ")");
		}
	}

	@Override
	public void revokeAccess(String serviceName, GrantRevokeRequest request) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAdminRESTClient.revokeAccess(" + serviceName + ", " + request + ")");
		}

		WebResource    webResource = createWebResource(REST_URL_SERVICE_REVOKE_ACCESS + serviceName);
		ClientResponse response    = webResource.accept(REST_EXPECTED_MIME_TYPE).type(REST_EXPECTED_MIME_TYPE).post(ClientResponse.class, restClient.toJson(request));

		if(response == null || response.getStatus() != 200) {
			RESTResponse resp = RESTResponse.fromClientResponse(response);

			throw new Exception(resp.getMessage());
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAdminRESTClient.revokeAccess(" + serviceName + ", " + request + ")");
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
}
