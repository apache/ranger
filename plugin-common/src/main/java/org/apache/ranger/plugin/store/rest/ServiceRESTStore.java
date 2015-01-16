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

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ranger.admin.client.datatype.RESTResponse;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerService;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.store.ServiceStore;
import org.apache.ranger.plugin.util.RangerRESTClient;
import org.apache.ranger.plugin.util.ServicePolicies;

import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.GenericType;
import com.sun.jersey.api.client.WebResource;


public class ServiceRESTStore implements ServiceStore {
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

	public static final String REST_MIME_TYPE_JSON = "application/json" ;

	private RangerRESTClient restClient;

	public ServiceRESTStore(RangerRESTClient restClient) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceRESTStore.ServiceRESTStore(" + restClient + ")");
		}

		this.restClient = restClient;

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceRESTStore.ServiceRESTStore(" + restClient + ")");
		}
	}


	@Override
	public RangerServiceDef createServiceDef(RangerServiceDef serviceDef) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceRESTStore.createServiceDef(" + serviceDef + ")");
		}

		RangerServiceDef ret = null;

		WebResource    webResource = restClient.getResource(REST_URL_SERVICEDEF_CREATE);
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

		WebResource    webResource = restClient.getResource(REST_URL_SERVICEDEF_UPDATE + serviceDef.getId());
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

		WebResource    webResource = restClient.getResource(REST_URL_SERVICEDEF_DELETE + id);
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

		WebResource    webResource = restClient.getResource(REST_URL_SERVICEDEF_GET + id);
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

		WebResource    webResource = restClient.getResource(REST_URL_SERVICEDEF_GET_BY_NAME + name);
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
	public List<RangerServiceDef> getAllServiceDefs() throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceRESTStore.getAllServiceDefs()");
		}

		List<RangerServiceDef> ret = null;

		WebResource    webResource = restClient.getResource(REST_URL_SERVICEDEF_GET_ALL);
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

		WebResource    webResource = restClient.getResource(REST_URL_SERVICE_CREATE);
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

		WebResource    webResource = restClient.getResource(REST_URL_SERVICE_UPDATE + service.getId());
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

		WebResource    webResource = restClient.getResource(REST_URL_SERVICE_DELETE + id);
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

		WebResource    webResource = restClient.getResource(REST_URL_SERVICE_GET + id);
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

		WebResource    webResource = restClient.getResource(REST_URL_SERVICE_GET_BY_NAME + name);
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
	public List<RangerService> getAllServices() throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceRESTStore.getAllServices()");
		}

		List<RangerService> ret = null;

		WebResource    webResource = restClient.getResource(REST_URL_SERVICE_GET_ALL);
		ClientResponse response    = webResource.accept(REST_MIME_TYPE_JSON).get(ClientResponse.class);

		if(response != null && response.getStatus() == 200) {
			ret = response.getEntity(new GenericType<List<RangerService>>() { });
		} else {
			RESTResponse resp = RESTResponse.fromClientResponse(response);

			throw new Exception(resp.getMessage());
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceRESTStore.getAllServices(): " + ret);
		}

		return ret;
	}

	@Override
	public RangerPolicy createPolicy(RangerPolicy policy) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceRESTStore.createPolicy(" + policy + ")");
		}

		RangerPolicy ret = null;

		WebResource    webResource = restClient.getResource(REST_URL_POLICY_CREATE);
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

		WebResource    webResource = restClient.getResource(REST_URL_POLICY_UPDATE + policy.getId());
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

		WebResource    webResource = restClient.getResource(REST_URL_POLICY_DELETE + id);
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

		WebResource    webResource = restClient.getResource(REST_URL_POLICY_GET + id);
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
	public List<RangerPolicy> getAllPolicies() throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceRESTStore.getAllPolicies()");
		}

		List<RangerPolicy> ret = null;

		WebResource    webResource = restClient.getResource(REST_URL_POLICY_GET_ALL);
		ClientResponse response    = webResource.accept(REST_MIME_TYPE_JSON).get(ClientResponse.class);

		if(response != null && response.getStatus() == 200) {
			ret = response.getEntity(new GenericType<List<RangerPolicy>>() { });
		} else {
			RESTResponse resp = RESTResponse.fromClientResponse(response);

			throw new Exception(resp.getMessage());
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceRESTStore.getAllPolicies(): " + ret);
		}

		return ret;
	}

	@Override
	public List<RangerPolicy> getServicePolicies(Long serviceId) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceRESTStore.getServicePolicies(" + serviceId + ")");
		}

		List<RangerPolicy> ret = null;

		WebResource    webResource = restClient.getResource(REST_URL_POLICY_GET_FOR_SERVICE + serviceId);
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
	public List<RangerPolicy> getServicePolicies(String serviceName) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceRESTStore.getServicePolicies(" + serviceName + ")");
		}

		List<RangerPolicy> ret = null;

		WebResource    webResource = restClient.getResource(REST_URL_POLICY_GET_FOR_SERVICE_BY_NAME + serviceName);
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
	public ServicePolicies getServicePoliciesIfUpdated(String serviceName,
			Long lastKnownVersion) throws Exception {
		// TODO Auto-generated method stub
		return null;
	}
}
