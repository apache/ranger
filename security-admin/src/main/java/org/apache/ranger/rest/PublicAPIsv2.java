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

package org.apache.ranger.rest;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.apache.ranger.common.RESTErrorUtil;
import org.apache.ranger.common.annotation.RangerAnnotationJSMgrName;
import org.apache.ranger.plugin.model.RangerPluginInfo;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerService;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.util.SearchFilter;
import org.apache.ranger.view.RangerPluginInfoList;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;

import java.util.ArrayList;
import java.util.List;

@Path("public/v2")
@Component
@Scope("request")
@RangerAnnotationJSMgrName("PublicMgr")
@Transactional(propagation = Propagation.REQUIRES_NEW)
public class PublicAPIsv2 {
	private static final Logger logger = Logger.getLogger(PublicAPIsv2.class);

	@Autowired
	ServiceREST serviceREST;

	@Autowired
	RESTErrorUtil restErrorUtil;

	/*
	* ServiceDef Manipulation APIs
	 */

	@GET
	@Path("/api/servicedef/{id}")
	@PreAuthorize("hasRole('ROLE_SYS_ADMIN')")
	@Produces({ "application/json", "application/xml" })
	public RangerServiceDef getServiceDef(@PathParam("id") Long id) {
		return serviceREST.getServiceDef(id);
	}

	@GET
	@Path("/api/servicedef/name/{name}")
	@PreAuthorize("hasRole('ROLE_SYS_ADMIN')")
	@Produces({ "application/json", "application/xml" })
	public RangerServiceDef getServiceDefByName(@PathParam("name") String name) {
		return serviceREST.getServiceDefByName(name);
	}

	@GET
	@Path("/api/servicedef/")
	@PreAuthorize("hasRole('ROLE_SYS_ADMIN')")
	@Produces({ "application/json", "application/xml" })
	public List<RangerServiceDef> searchServiceDefs(@Context HttpServletRequest request) {
		return serviceREST.getServiceDefs(request).getServiceDefs();
	}

	@POST
	@Path("/api/servicedef/")
	@PreAuthorize("hasRole('ROLE_SYS_ADMIN')")
	@Produces({ "application/json", "application/xml" })
	public RangerServiceDef createServiceDef(RangerServiceDef serviceDef) {
		return serviceREST.createServiceDef(serviceDef);
	}

	@PUT
	@Path("/api/servicedef/{id}")
	@PreAuthorize("hasRole('ROLE_SYS_ADMIN')")
	@Produces({ "application/json", "application/xml" })
	public RangerServiceDef updateServiceDef(RangerServiceDef serviceDef, @PathParam("id") Long id) {
		// if serviceDef.id is specified, it should be same as param 'id'
		if(serviceDef.getId() == null) {
			serviceDef.setId(id);
		} else if(!serviceDef.getId().equals(id)) {
			throw restErrorUtil.createRESTException(HttpServletResponse.SC_BAD_REQUEST , "serviceDef id mismatch", true);
		}

		return serviceREST.updateServiceDef(serviceDef);
	}


	@PUT
	@Path("/api/servicedef/name/{name}")
	@PreAuthorize("hasRole('ROLE_SYS_ADMIN')")
	@Produces({ "application/json", "application/xml" })
	public RangerServiceDef updateServiceDefByName(RangerServiceDef serviceDef,
	                                     @PathParam("name") String name) {
		// serviceDef.name is immutable
		// if serviceDef.name is specified, it should be same as the param 'name'
		if(serviceDef.getName() == null) {
			serviceDef.setName(name);
		} else if(!serviceDef.getName().equals(name)) {
			throw restErrorUtil.createRESTException(HttpServletResponse.SC_BAD_REQUEST , "serviceDef name mismatch", true);
		}

		// ignore serviceDef.id - if specified. Retrieve using the given name and use id from the retrieved object
		RangerServiceDef existingServiceDef = getServiceDefByName(name);
		serviceDef.setId(existingServiceDef.getId());
		if(StringUtils.isEmpty(serviceDef.getGuid())) {
			serviceDef.setGuid(existingServiceDef.getGuid());
		}

		return serviceREST.updateServiceDef(serviceDef);
	}

	/*
	* Should add this back when guid is used for search and delete operations as well
	@PUT
	@Path("/api/servicedef/guid/{guid}")
	@PreAuthorize("hasRole('ROLE_SYS_ADMIN')")
	@Produces({ "application/json", "application/xml" })
	public RangerServiceDef updateServiceDefByGuid(RangerServiceDef serviceDef,
	                                               @PathParam("guid") String guid) {
		// ignore serviceDef.id - if specified. Retrieve using the given guid and use id from the retrieved object
		RangerServiceDef existingServiceDef = getServiceDefByGuid(guid);
		serviceDef.setId(existingServiceDef.getId());
		if(StringUtils.isEmpty(serviceDef.getGuid())) {
			serviceDef.setGuid(existingServiceDef.getGuid());
		}

		return serviceREST.updateServiceDef(serviceDef);
	}
	*/


	@DELETE
	@Path("/api/servicedef/{id}")
	@PreAuthorize("hasRole('ROLE_SYS_ADMIN')")
	public void deleteServiceDef(@PathParam("id") Long id, @Context HttpServletRequest request) {
		serviceREST.deleteServiceDef(id, request);
	}

	@DELETE
	@Path("/api/servicedef/name/{name}")
	@PreAuthorize("hasRole('ROLE_SYS_ADMIN')")
	public void deleteServiceDefByName(@PathParam("name") String name, @Context HttpServletRequest request) {
		RangerServiceDef serviceDef = serviceREST.getServiceDefByName(name);
		serviceREST.deleteServiceDef(serviceDef.getId(), request);
	}

	/*
	* Service Manipulation APIs
	 */

	@GET
	@Path("/api/service/{id}")
	@Produces({ "application/json", "application/xml" })
	@PreAuthorize("@rangerPreAuthSecurityHandler.isAPISpnegoAccessible()")
	public RangerService getService(@PathParam("id") Long id) {
		return serviceREST.getService(id);
	}

	@GET
	@Path("/api/service/name/{name}")
	@Produces({ "application/json", "application/xml" })
	@PreAuthorize("@rangerPreAuthSecurityHandler.isAPISpnegoAccessible()")
	public RangerService getServiceByName(@PathParam("name") String name) {
		return serviceREST.getServiceByName(name);
	}

	@GET
	@Path("/api/service/")
	@Produces({ "application/json", "application/xml" })
	@PreAuthorize("@rangerPreAuthSecurityHandler.isAPISpnegoAccessible()")
	public List<RangerService> searchServices(@Context HttpServletRequest request) {
		return serviceREST.getServices(request).getServices();
	}

	@POST
	@Path("/api/service/")
	@PreAuthorize("@rangerPreAuthSecurityHandler.isAPISpnegoAccessible()")
	@Produces({ "application/json", "application/xml" })
	public RangerService createService(RangerService service) {
		return serviceREST.createService(service);
	}

	@PUT
	@Path("/api/service/{id}")
	@PreAuthorize("@rangerPreAuthSecurityHandler.isAPISpnegoAccessible()")
	@Produces({ "application/json", "application/xml" })
	public RangerService updateService(RangerService service, @PathParam("id") Long id,
                                       @Context HttpServletRequest request) {
		// if service.id is specified, it should be same as the param 'id'
		if(service.getId() == null) {
			service.setId(id);
		} else if(!service.getId().equals(id)) {
			throw restErrorUtil.createRESTException(HttpServletResponse.SC_BAD_REQUEST , "service id mismatch", true);
		}

		return serviceREST.updateService(service, request);
	}


	@PUT
	@Path("/api/service/name/{name}")
	@PreAuthorize("@rangerPreAuthSecurityHandler.isAPISpnegoAccessible()")
	@Produces({ "application/json", "application/xml" })
	public RangerService updateServiceByName(RangerService service,
                                             @PathParam("name") String name,
                                             @Context HttpServletRequest request) {
		// ignore service.id - if specified. Retrieve using the given name and use id from the retrieved object
		RangerService existingService = getServiceByName(name);
		service.setId(existingService.getId());
		if(StringUtils.isEmpty(service.getGuid())) {
			service.setGuid(existingService.getGuid());
		}
		if (StringUtils.isEmpty(service.getName())) {
			service.setName(existingService.getName());
		}

		return serviceREST.updateService(service, request);
	}

	/*
	 * Should add this back when guid is used for search and delete operations as well
	@PUT
	@Path("/api/service/guid/{guid}")
	@PreAuthorize("hasRole('ROLE_SYS_ADMIN')")
	@Produces({ "application/json", "application/xml" })
	public RangerService updateServiceByGuid(RangerService service,
	                                               @PathParam("guid") String guid) {
		// ignore service.id - if specified. Retrieve using the given guid and use id from the retrieved object
		RangerService existingService = getServiceByGuid(guid);
		service.setId(existingService.getId());
		if(StringUtils.isEmpty(service.getGuid())) {
			service.setGuid(existingService.getGuid());
		}

		return serviceREST.updateService(service);
	}
	*/

	@DELETE
	@Path("/api/service/{id}")
	@PreAuthorize("@rangerPreAuthSecurityHandler.isAPISpnegoAccessible()")
	public void deleteService(@PathParam("id") Long id) {
		serviceREST.deleteService(id);
	}

	@DELETE
	@Path("/api/service/name/{name}")
	@PreAuthorize("@rangerPreAuthSecurityHandler.isAPISpnegoAccessible()")
	public void deleteServiceByName(@PathParam("name") String name) {
		RangerService service = serviceREST.getServiceByName(name);
		serviceREST.deleteService(service.getId());
	}

	/*
	* Policy Manipulation APIs
	 */

	@GET
	@Path("/api/policy/{id}")
	@Produces({ "application/json", "application/xml" })
	public RangerPolicy getPolicy(@PathParam("id") Long id) {
		return serviceREST.getPolicy(id);
	}

	@GET
	@Path("/api/policy/")
	@Produces({ "application/json", "application/xml" })
	public List<RangerPolicy> getPolicies(@Context HttpServletRequest request) {

		List<RangerPolicy> ret  = new ArrayList<RangerPolicy>();

		if(logger.isDebugEnabled()) {
			logger.debug("==> PublicAPIsv2.getPolicies()");
		}

		ret = serviceREST.getPolicies(request).getPolicies();

		if(logger.isDebugEnabled()) {
			logger.debug("<== PublicAPIsv2.getPolicies(Request: " + request.getQueryString() + " Result Size: "  + ret.size() );
		}

		return ret;
	}

	@GET
	@Path("/api/service/{servicename}/policy/{policyname}")
	@Produces({ "application/json", "application/xml" })
	public RangerPolicy getPolicyByName(@PathParam("servicename") String serviceName,
	                                    @PathParam("policyname") String policyName,
	                                    @Context HttpServletRequest request) {
		if(logger.isDebugEnabled()) {
			logger.debug("==> PublicAPIsv2.getPolicyByName(" + serviceName + "," + policyName + ")");
		}

		SearchFilter filter = new SearchFilter();
		filter.setParam(SearchFilter.SERVICE_NAME, serviceName);
		filter.setParam(SearchFilter.POLICY_NAME, policyName);
		List<RangerPolicy> policies = serviceREST.getPolicies(filter);

		if (policies.size() != 1) {
			throw restErrorUtil.createRESTException(HttpServletResponse.SC_NOT_FOUND, "Not found", true);
		}
		RangerPolicy policy = policies.get(0);

		if(logger.isDebugEnabled()) {
			logger.debug("<== PublicAPIsv2.getPolicyByName(" + serviceName + "," + policyName + ")" + policy);
		}
		return policy;
	}

	@GET
	@Path("/api/service/{servicename}/policy/")
	@Produces({ "application/json", "application/xml" })
	public List<RangerPolicy> searchPolicies(@PathParam("servicename") String serviceName,
	                                         @Context HttpServletRequest request) {
		return serviceREST.getServicePoliciesByName(serviceName, request).getPolicies();
	}

	@GET
	@Path("/api/policies/{serviceDefName}/for-resource/")
	@Produces({ "application/json", "application/xml" })
	public List<RangerPolicy> getPoliciesForResource(@PathParam("serviceDefName") String serviceDefName,
													 @DefaultValue("") @QueryParam("serviceName") String serviceName,
													 @Context HttpServletRequest request) {
		return serviceREST.getPoliciesForResource(serviceDefName, serviceName, request);
	}

	@POST
	@Path("/api/policy/")
	@Produces({ "application/json", "application/xml" })
	public RangerPolicy createPolicy(RangerPolicy policy , @Context HttpServletRequest request) {
		return serviceREST.createPolicy(policy, request);
	}

	@POST
	@Path("/api/policy/apply/")
	@Produces({ "application/json", "application/xml" })
	public RangerPolicy applyPolicy(RangerPolicy policy, @Context HttpServletRequest request) { // new API
		return serviceREST.applyPolicy(policy, request);
	}

	@PUT
	@Path("/api/policy/{id}")
	@Produces({ "application/json", "application/xml" })
	public RangerPolicy updatePolicy(RangerPolicy policy, @PathParam("id") Long id) {
		// if policy.id is specified, it should be same as the param 'id'
		if(policy.getId() == null) {
			policy.setId(id);
		} else if(!policy.getId().equals(id)) {
			throw restErrorUtil.createRESTException(HttpServletResponse.SC_BAD_REQUEST , "policyID mismatch", true);
		}

		return serviceREST.updatePolicy(policy);
	}

	@PUT
	@Path("/api/service/{servicename}/policy/{policyname}")
	@Produces({ "application/json", "application/xml" })
	public RangerPolicy updatePolicyByName(RangerPolicy policy,
	                                               @PathParam("servicename") String serviceName,
	                                               @PathParam("policyname") String policyName,
	                                               @Context HttpServletRequest request) {
		if (policy.getService() == null || !policy.getService().equals(serviceName)) {
			throw restErrorUtil.createRESTException(HttpServletResponse.SC_BAD_REQUEST , "service name mismatch", true);
		}
		RangerPolicy oldPolicy = getPolicyByName(serviceName, policyName, request);

		// ignore policy.id - if specified. Retrieve using the given serviceName+policyName and use id from the retrieved object
		policy.setId(oldPolicy.getId());
		if(StringUtils.isEmpty(policy.getGuid())) {
			policy.setGuid(oldPolicy.getGuid());
		}
		if(StringUtils.isEmpty(policy.getName())) {
			policy.setName(StringUtils.trim(oldPolicy.getName()));
		}

		return serviceREST.updatePolicy(policy);
	}


	/* Should add this back when guid is used for search and delete operations as well
	@PUT
	@Path("/api/policy/guid/{guid}")
	@Produces({ "application/json", "application/xml" })
	public RangerPolicy updatePolicyByGuid(RangerPolicy policy,
	                                               @PathParam("guid") String guid) {
		// ignore policy.guid - if specified. Retrieve using the given guid and use id from the retrieved object
		RangerPolicy existingPolicy = getPolicyByGuid(name);
		policy.setId(existingPolicy.getId());
		if(StringUtils.isEmpty(policy.getGuid())) {
			policy.setGuid(existingPolicy.getGuid());
		}

		return serviceREST.updatePolicy(policy);
	}
	*/


	@DELETE
	@Path("/api/policy/{id}")
	public void deletePolicy(@PathParam("id") Long id) {
		serviceREST.deletePolicy(id);
	}

	@DELETE
	@Path("/api/policy")
	public void deletePolicyByName(@QueryParam("servicename") String serviceName,
	                               @QueryParam("policyname") String policyName,
	                               @Context HttpServletRequest request) {
		if(logger.isDebugEnabled()) {
			logger.debug("==> PublicAPIsv2.deletePolicyByName(" + serviceName + "," + policyName + ")");
		}

		if (serviceName == null || policyName == null) {
			throw restErrorUtil.createRESTException(HttpServletResponse.SC_BAD_REQUEST , "Invalid service name or policy name", true);
		}
		RangerPolicy policy = getPolicyByName(serviceName, policyName, request);
		serviceREST.deletePolicy(policy.getId());
		if(logger.isDebugEnabled()) {
			logger.debug("<== PublicAPIsv2.deletePolicyByName(" + serviceName + "," + policyName + ")");
		}
	}

	@GET
	@Path("/api/plugins/info")
	public List<RangerPluginInfo> getPluginsInfo(@Context HttpServletRequest request) {
		if (logger.isDebugEnabled()) {
			logger.debug("==> PublicAPIsv2.getPluginsInfo()");
		}

		RangerPluginInfoList pluginInfoList = serviceREST.getPluginsInfo(request);

		if (logger.isDebugEnabled()) {
			logger.debug("<== PublicAPIsv2.getPluginsInfo()");
		}
		return pluginInfoList.getPluginInfoList();
	}
}
