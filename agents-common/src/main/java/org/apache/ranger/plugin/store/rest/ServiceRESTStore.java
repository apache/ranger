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

package org.apache.ranger.plugin.store.rest;

import java.security.PrivilegedAction;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.MapUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.ranger.admin.client.datatype.RESTResponse;
import org.apache.ranger.audit.provider.MiscUtil;
import org.apache.ranger.authorization.hadoop.config.RangerConfiguration;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerService;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.store.AbstractServiceStore;
import org.apache.ranger.plugin.util.RangerRESTClient;
import org.apache.ranger.plugin.util.SearchFilter;
import org.apache.ranger.plugin.util.ServicePolicies;

import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.GenericType;
import com.sun.jersey.api.client.WebResource;


public class ServiceRESTStore extends AbstractServiceStore {
	private static final Log LOG = LogFactory.getLog(ServiceRESTStore.class);


	public final String REST_URL_SERVICEDEF_CREATE      = "/service/plugins/definitions";
	public final String REST_URL_SERVICEDEF_UPDATE      = "/service/plugins/definitions/";
	public final String REST_URL_SERVICEDEF_DELETE      = "/service/plugins/definitions/";
	public final String REST_URL_SERVICEDEF_GET         = "/service/plugins/definitions/";
	public final String REST_URL_SERVICEDEF_GET_BY_NAME = "/service/plugins/definitions/name/";
	public final String REST_URL_SERVICEDEF_GET_ALL     = "/service/plugins/definitions";

	public final String REST_URL_SERVICE_CREATE      = "/service/plugins/services";
	public final String REST_URL_SERVICE_UPDATE      = "/service/plugins/services/";
	public final String REST_URL_SERVICE_DELETE      = "/service/plugins/services/";
	public final String REST_URL_SERVICE_GET         = "/service/plugins/services/";
	public final String REST_URL_SERVICE_GET_BY_NAME = "/service/plugins/services/name/";
	public final String REST_URL_SERVICE_GET_ALL     = "/service/plugins/services";

	public final String REST_URL_POLICY_CREATE      = "/service/plugins/policies";
	public final String REST_URL_POLICY_UPDATE      = "/service/plugins/policies/";
	public final String REST_URL_POLICY_DELETE      = "/service/plugins/policies/";
	public final String REST_URL_POLICY_GET         = "/service/plugins/policies/";
	public final String REST_URL_POLICY_GET_BY_NAME = "/service/plugins/policies/name/";
	public final String REST_URL_POLICY_GET_ALL     = "/service/plugins/policies";
	public final String REST_URL_POLICY_GET_FOR_SERVICE         = "/service/plugins/policies/service/";
	public final String REST_URL_POLICY_GET_FOR_SERVICE_BY_NAME = "/service/plugins/policies/service/name/";
	public final String REST_URL_POLICY_GET_FOR_SERVICE_IF_UPDATED = "/service/plugins/policies/download/";
	public final String REST_URL_POLICY_GET_FOR_SECURE_SERVICE_IF_UPDATED = "/service/plugins/secure/policies/download/";

	public static final String REST_MIME_TYPE_JSON = "application/json" ;
	
	private Boolean populateExistingBaseFields = false;

	private RangerRESTClient restClient;

	public ServiceRESTStore() {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceRESTStore.ServiceRESTStore()");
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceRESTStore.ServiceRESTStore()");
		}
	}

	@Override
	public void init() throws Exception {
		String restUrl       = RangerConfiguration.getInstance().get("ranger.service.store.rest.url");
		String sslConfigFile = RangerConfiguration.getInstance().get("ranger.service.store.rest.ssl.config.file");
		String userName = RangerConfiguration.getInstance().get("ranger.service.store.rest.basicauth.username");
		String password = RangerConfiguration.getInstance().get("ranger.service.store.rest.basicauth.password");

		restClient = new RangerRESTClient(restUrl, sslConfigFile);
		restClient.setBasicAuthInfo(userName, password);
	}

	@Override
	public RangerServiceDef createServiceDef(RangerServiceDef serviceDef) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceRESTStore.createServiceDef(" + serviceDef + ")");
		}

		RangerServiceDef ret = null;

		WebResource    webResource = createWebResource(REST_URL_SERVICEDEF_CREATE);
		ClientResponse response    = webResource.accept(REST_MIME_TYPE_JSON).type(REST_MIME_TYPE_JSON).post(ClientResponse.class, restClient.toJson(serviceDef));

		if(response != null && response.getStatus() == 200) {
			ret = response.getEntity(RangerServiceDef.class);
		} else {
			RESTResponse resp = RESTResponse.fromClientResponse(response);

			throw new Exception(resp.getMessage());
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceRESTStore.createServiceDef(" + serviceDef + "): " + ret);
		}

		return ret;
	}

	@Override
	public RangerServiceDef updateServiceDef(RangerServiceDef serviceDef) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceRESTStore.updateServiceDef(" + serviceDef + ")");
		}

		RangerServiceDef ret = null;

		WebResource    webResource = createWebResource(REST_URL_SERVICEDEF_UPDATE + serviceDef.getId());
		ClientResponse response    = webResource.accept(REST_MIME_TYPE_JSON).type(REST_MIME_TYPE_JSON).put(ClientResponse.class, restClient.toJson(serviceDef));

		if(response != null && response.getStatus() == 200) {
			ret = response.getEntity(RangerServiceDef.class);
		} else {
			RESTResponse resp = RESTResponse.fromClientResponse(response);

			throw new Exception(resp.getMessage());
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceRESTStore.updateServiceDef(" + serviceDef + "): " + ret);
		}

		return ret;
	}

	@Override
	public void deleteServiceDef(Long id) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceRESTStore.deleteServiceDef(" + id + ")");
		}

		WebResource    webResource = createWebResource(REST_URL_SERVICEDEF_DELETE + id);
		ClientResponse response    = webResource.accept(REST_MIME_TYPE_JSON).delete(ClientResponse.class);

		if(response == null || (response.getStatus() != 200 && response.getStatus() != 204)) {
			RESTResponse resp = RESTResponse.fromClientResponse(response);

			throw new Exception(resp.getMessage());
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceRESTStore.deleteServiceDef(" + id + ")");
		}
	}

	@Override
	public RangerServiceDef getServiceDef(Long id) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceRESTStore.getServiceDef(" + id + ")");
		}

		RangerServiceDef ret = null;

		WebResource    webResource = createWebResource(REST_URL_SERVICEDEF_GET + id);
		ClientResponse response    = webResource.accept(REST_MIME_TYPE_JSON).get(ClientResponse.class);

		if(response != null && response.getStatus() == 200) {
			ret = response.getEntity(RangerServiceDef.class);
		} else {
			RESTResponse resp = RESTResponse.fromClientResponse(response);

			throw new Exception(resp.getMessage());
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceRESTStore.getServiceDef(" + id + "): " + ret);
		}

		return ret;
	}

	@Override
	public RangerServiceDef getServiceDefByName(String name) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceRESTStore.getServiceDefByName(" + name + ")");
		}

		RangerServiceDef ret = null;

		WebResource    webResource = createWebResource(REST_URL_SERVICEDEF_GET_BY_NAME + name);
		ClientResponse response    = webResource.accept(REST_MIME_TYPE_JSON).get(ClientResponse.class);

		if(response != null && response.getStatus() == 200) {
			ret = response.getEntity(RangerServiceDef.class);
		} else {
			RESTResponse resp = RESTResponse.fromClientResponse(response);

			throw new Exception(resp.getMessage());
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceRESTStore.getServiceDefByName(" + name + "): " + ret);
		}

		return ret;
	}

	@Override
	public List<RangerServiceDef> getServiceDefs(SearchFilter filter) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceRESTStore.getServiceDefs()");
		}

		List<RangerServiceDef> ret = null;

		WebResource    webResource = createWebResource(REST_URL_SERVICEDEF_GET_ALL, filter);
		ClientResponse response    = webResource.accept(REST_MIME_TYPE_JSON).get(ClientResponse.class);

		if(response != null && response.getStatus() == 200) {
			ret = response.getEntity(new GenericType<List<RangerServiceDef>>() { });
		} else {
			RESTResponse resp = RESTResponse.fromClientResponse(response);

			throw new Exception(resp.getMessage());
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceRESTStore.getAllServiceDefs(): " + ret);
		}

		return ret;
	}

	@Override
	public RangerService createService(RangerService service) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceRESTStore.createService(" + service + ")");
		}

		RangerService ret = null;

		WebResource    webResource = createWebResource(REST_URL_SERVICE_CREATE);
		ClientResponse response    = webResource.accept(REST_MIME_TYPE_JSON).type(REST_MIME_TYPE_JSON).post(ClientResponse.class, restClient.toJson(service));

		if(response != null && response.getStatus() == 200) {
			ret = response.getEntity(RangerService.class);
		} else {
			RESTResponse resp = RESTResponse.fromClientResponse(response);

			throw new Exception(resp.getMessage());
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceRESTStore.createService(" + service + "): " + ret);
		}

		return ret;
	}

	@Override
	public RangerService updateService(RangerService service) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceRESTStore.updateService(" + service + ")");
		}

		RangerService ret = null;

		WebResource    webResource = createWebResource(REST_URL_SERVICE_UPDATE + service.getId());
		ClientResponse response    = webResource.accept(REST_MIME_TYPE_JSON).type(REST_MIME_TYPE_JSON).put(ClientResponse.class, restClient.toJson(service));

		if(response != null && response.getStatus() == 200) {
			ret = response.getEntity(RangerService.class);
		} else {
			RESTResponse resp = RESTResponse.fromClientResponse(response);

			throw new Exception(resp.getMessage());
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceRESTStore.updateService(" + service + "): " + ret);
		}

		return ret;
	}

	@Override
	public void deleteService(Long id) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceRESTStore.deleteService(" + id + ")");
		}

		WebResource    webResource = createWebResource(REST_URL_SERVICE_DELETE + id);
		ClientResponse response    = webResource.accept(REST_MIME_TYPE_JSON).delete(ClientResponse.class);

		if(response == null || (response.getStatus() != 200 && response.getStatus() != 204)) {
			RESTResponse resp = RESTResponse.fromClientResponse(response);

			throw new Exception(resp.getMessage());
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceRESTStore.deleteService(" + id + ")");
		}
	}

	@Override
	public RangerService getService(Long id) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceRESTStore.getService(" + id + ")");
		}

		RangerService ret = null;

		WebResource    webResource = createWebResource(REST_URL_SERVICE_GET + id);
		ClientResponse response    = webResource.accept(REST_MIME_TYPE_JSON).get(ClientResponse.class);

		if(response != null && response.getStatus() == 200) {
			ret = response.getEntity(RangerService.class);
		} else {
			RESTResponse resp = RESTResponse.fromClientResponse(response);

			throw new Exception(resp.getMessage());
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceRESTStore.getService(" + id + "): " + ret);
		}

		return ret;
	}

	@Override
	public RangerService getServiceByName(String name) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceRESTStore.getServiceByName(" + name + ")");
		}

		RangerService ret = null;

		WebResource    webResource = createWebResource(REST_URL_SERVICE_GET_BY_NAME + name);
		ClientResponse response    = webResource.accept(REST_MIME_TYPE_JSON).get(ClientResponse.class);

		if(response != null && response.getStatus() == 200) {
			ret = response.getEntity(RangerService.class);
		} else {
			RESTResponse resp = RESTResponse.fromClientResponse(response);

			throw new Exception(resp.getMessage());
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceRESTStore.getServiceByName(" + name + "): " + ret);
		}

		return ret;
	}

	@Override
	public List<RangerService> getServices(SearchFilter filter) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceRESTStore.getServices()");
		}

		List<RangerService> ret = null;

		WebResource    webResource = createWebResource(REST_URL_SERVICE_GET_ALL, filter);
		ClientResponse response    = webResource.accept(REST_MIME_TYPE_JSON).get(ClientResponse.class);

		if(response != null && response.getStatus() == 200) {
			ret = response.getEntity(new GenericType<List<RangerService>>() { });
		} else {
			RESTResponse resp = RESTResponse.fromClientResponse(response);

			throw new Exception(resp.getMessage());
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceRESTStore.getServices(): " + ret);
		}

		return ret;
	}

	@Override
	public RangerPolicy createPolicy(RangerPolicy policy) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceRESTStore.createPolicy(" + policy + ")");
		}

		RangerPolicy ret = null;

		WebResource    webResource = createWebResource(REST_URL_POLICY_CREATE);
		ClientResponse response    = webResource.accept(REST_MIME_TYPE_JSON).type(REST_MIME_TYPE_JSON).post(ClientResponse.class, restClient.toJson(policy));

		if(response != null && response.getStatus() == 200) {
			ret = response.getEntity(RangerPolicy.class);
		} else {
			RESTResponse resp = RESTResponse.fromClientResponse(response);

			throw new Exception(resp.getMessage());
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceRESTStore.createPolicy(" + policy + "): " + ret);
		}

		return ret;
	}

	@Override
	public RangerPolicy updatePolicy(RangerPolicy policy) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceRESTStore.updatePolicy(" + policy + ")");
		}

		RangerPolicy ret = null;

		WebResource    webResource = createWebResource(REST_URL_POLICY_UPDATE + policy.getId());
		ClientResponse response    = webResource.accept(REST_MIME_TYPE_JSON).type(REST_MIME_TYPE_JSON).put(ClientResponse.class, restClient.toJson(policy));

		if(response != null && response.getStatus() == 200) {
			ret = response.getEntity(RangerPolicy.class);
		} else {
			RESTResponse resp = RESTResponse.fromClientResponse(response);

			throw new Exception(resp.getMessage());
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceRESTStore.updatePolicy(" + policy + "): " + ret);
		}

		return ret;
	}

	@Override
	public void deletePolicy(Long id) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceRESTStore.deletePolicy(" + id + ")");
		}

		WebResource    webResource = createWebResource(REST_URL_POLICY_DELETE + id);
		ClientResponse response    = webResource.accept(REST_MIME_TYPE_JSON).delete(ClientResponse.class);

		if(response == null || (response.getStatus() != 200 && response.getStatus() != 204)) {
			RESTResponse resp = RESTResponse.fromClientResponse(response);

			throw new Exception(resp.getMessage());
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceRESTStore.deletePolicy(" + id + ")");
		}
	}

	@Override
	public RangerPolicy getPolicy(Long id) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceRESTStore.getPolicy(" + id + ")");
		}

		RangerPolicy ret = null;

		WebResource    webResource = createWebResource(REST_URL_POLICY_GET + id);
		ClientResponse response    = webResource.accept(REST_MIME_TYPE_JSON).get(ClientResponse.class);

		if(response != null && response.getStatus() == 200) {
			ret = response.getEntity(RangerPolicy.class);
		} else {
			RESTResponse resp = RESTResponse.fromClientResponse(response);

			throw new Exception(resp.getMessage());
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceRESTStore.getPolicy(" + id + "): " + ret);
		}

		return ret;
	}

	@Override
	public List<RangerPolicy> getPolicies(SearchFilter filter) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceRESTStore.getPolicies()");
		}

		List<RangerPolicy> ret = null;

		WebResource    webResource = createWebResource(REST_URL_POLICY_GET_ALL, filter);
		ClientResponse response    = webResource.accept(REST_MIME_TYPE_JSON).get(ClientResponse.class);

		if(response != null && response.getStatus() == 200) {
			ret = response.getEntity(new GenericType<List<RangerPolicy>>() { });
		} else {
			RESTResponse resp = RESTResponse.fromClientResponse(response);

			throw new Exception(resp.getMessage());
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceRESTStore.getPolicies(): " + ret);
		}

		return ret;
	}

	@Override
	public List<RangerPolicy> getServicePolicies(Long serviceId, SearchFilter filter) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceRESTStore.getServicePolicies(" + serviceId + ")");
		}

		List<RangerPolicy> ret = null;

		WebResource    webResource = createWebResource(REST_URL_POLICY_GET_FOR_SERVICE + serviceId, filter);
		ClientResponse response    = webResource.accept(REST_MIME_TYPE_JSON).get(ClientResponse.class);

		if(response != null && response.getStatus() == 200) {
			ret = response.getEntity(new GenericType<List<RangerPolicy>>() { });
		} else {
			RESTResponse resp = RESTResponse.fromClientResponse(response);

			throw new Exception(resp.getMessage());
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceRESTStore.getServicePolicies(" + serviceId + "): " + ret);
		}

		return ret;
	}

	@Override
	public List<RangerPolicy> getServicePolicies(String serviceName, SearchFilter filter) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceRESTStore.getServicePolicies(" + serviceName + ")");
		}

		List<RangerPolicy> ret = null;

		WebResource    webResource = createWebResource(REST_URL_POLICY_GET_FOR_SERVICE_BY_NAME + serviceName, filter);
		ClientResponse response    = webResource.accept(REST_MIME_TYPE_JSON).get(ClientResponse.class);

		if(response != null && response.getStatus() == 200) {
			ret = response.getEntity(new GenericType<List<RangerPolicy>>() { });
		} else {
			RESTResponse resp = RESTResponse.fromClientResponse(response);

			throw new Exception(resp.getMessage());
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceRESTStore.getServicePolicies(" + serviceName + "): " + ret);
		}

		return ret;
	}

	@Override
	public ServicePolicies getServicePoliciesIfUpdated(final String serviceName, final Long lastKnownVersion) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceRESTStore.getServicePoliciesIfUpdated(" + serviceName + ", " + lastKnownVersion + ")");
		}

		ServicePolicies ret = null;
		ClientResponse response = null;
		if (MiscUtil.getUGILoginUser() != null && UserGroupInformation.isSecurityEnabled()) {
			LOG.info("Checking Service policy if updated as user : "+ MiscUtil.getUGILoginUser());
			PrivilegedAction<ClientResponse> action = new PrivilegedAction<ClientResponse>() {
				public ClientResponse run() {
					WebResource secureWebResource = createWebResource(REST_URL_POLICY_GET_FOR_SECURE_SERVICE_IF_UPDATED + serviceName + "/" + lastKnownVersion);
					return secureWebResource.accept(REST_MIME_TYPE_JSON).get(ClientResponse.class);
				};
			};
			response = MiscUtil.getUGILoginUser().doAs(action);
		} else {
			WebResource  webResource = createWebResource(REST_URL_POLICY_GET_FOR_SERVICE_IF_UPDATED + serviceName + "/" + lastKnownVersion);
			response = webResource.accept(REST_MIME_TYPE_JSON).get(ClientResponse.class);
		}

		if(response != null && response.getStatus() == 200) {
			ret = response.getEntity(ServicePolicies.class);
		} else if(response != null && response.getStatus() == 304) {
			// no change
		} else {
			RESTResponse resp = RESTResponse.fromClientResponse(response);

			throw new Exception(resp.getMessage());
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceRESTStore.getServicePoliciesIfUpdated(" + serviceName + ", " + lastKnownVersion + "): " + ret);
		}

		return ret;
	}

	@Override
	public ServicePolicies getServicePolicies(String serviceName) throws Exception {
		return getServicePoliciesIfUpdated(serviceName, -1L);
	}

	private WebResource createWebResource(String url) {
		return createWebResource(url, null);
	}

	private WebResource createWebResource(String url, SearchFilter filter) {
		WebResource ret = restClient.getResource(url);

		if(filter != null && !MapUtils.isEmpty(filter.getParams())) {
			for(Map.Entry<String, String> e : filter.getParams().entrySet()) {
				String name  = e.getKey();
				String value = e.getValue();

				ret.queryParam(name, value);
			}
		}

		return ret;
	}

	@Override
	public List<RangerPolicy> getPoliciesByResourceSignature(String serviceName, String policySignature, Boolean isPolicyEnabled) throws Exception {
		throw new UnsupportedOperationException("Querying policies by resource signature is not supported!");
	}

	@Override
	public void setPopulateExistingBaseFields(Boolean populateExistingBaseFields) {
		this.populateExistingBaseFields = populateExistingBaseFields;
	}

	@Override
	public Boolean getPopulateExistingBaseFields() {
		return populateExistingBaseFields;
	}
}
