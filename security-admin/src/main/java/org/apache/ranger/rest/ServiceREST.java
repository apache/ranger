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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;

import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerService;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.service.ResourceLookupContext;
import org.apache.ranger.plugin.store.ServiceStore;
import org.apache.ranger.plugin.store.ServiceStoreFactory;
import org.apache.ranger.plugin.util.SearchFilter;
import org.apache.ranger.plugin.util.ServicePolicies;
import org.apache.ranger.view.VXResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.stereotype.Component;
import org.apache.ranger.biz.AssetMgr;
import org.apache.ranger.biz.ServiceMgr;
import org.apache.ranger.common.RESTErrorUtil;
import org.apache.ranger.entity.XXPolicyExportAudit;


@Path("plugins")
@Component
@Scope("request")
public class ServiceREST {
	private static final Log LOG = LogFactory.getLog(ServiceREST.class);

	@Autowired
	RESTErrorUtil restErrorUtil;

	@Autowired
	ServiceMgr serviceMgr;

	@Autowired
	AssetMgr assetMgr;

	private ServiceStore svcStore = null;

	public ServiceREST() {
		svcStore = ServiceStoreFactory.instance().getServiceStore();
	}


	@POST
	@Path("/definitions")
	@Produces({ "application/json", "application/xml" })
	@PreAuthorize("hasRole('ROLE_SYS_ADMIN')")
	public RangerServiceDef createServiceDef(RangerServiceDef serviceDef) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceREST.createServiceDef(" + serviceDef + ")");
		}

		RangerServiceDef ret = null;

		try {
			ret = svcStore.createServiceDef(serviceDef);
		} catch(Exception excp) {
			throw restErrorUtil.createRESTException(HttpServletResponse.SC_BAD_REQUEST, excp.getMessage(), true);
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceREST.createServiceDef(" + serviceDef + "): " + ret);
		}

		return ret;
	}

	@PUT
	@Path("/definitions/{id}")
	@Produces({ "application/json", "application/xml" })
	@PreAuthorize("hasRole('ROLE_SYS_ADMIN')")
	public RangerServiceDef updateServiceDef(RangerServiceDef serviceDef) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceREST.updateServiceDef(" + serviceDef + ")");
		}

		RangerServiceDef ret = null;

		try {
			ret = svcStore.updateServiceDef(serviceDef);
		} catch(Exception excp) {
			throw restErrorUtil.createRESTException(HttpServletResponse.SC_BAD_REQUEST, excp.getMessage(), true);
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceREST.updateServiceDef(" + serviceDef + "): " + ret);
		}

		return ret;
	}

	@DELETE
	@Path("/definitions/{id}")
	@Produces({ "application/json", "application/xml" })
	@PreAuthorize("hasRole('ROLE_SYS_ADMIN')")
	public void deleteServiceDef(@PathParam("id") Long id) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceREST.deleteServiceDef(" + id + ")");
		}

		try {
			svcStore.deleteServiceDef(id);
		} catch(Exception excp) {
			throw restErrorUtil.createRESTException(HttpServletResponse.SC_BAD_REQUEST, excp.getMessage(), true);
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceREST.deleteServiceDef(" + id + ")");
		}
	}

	@GET
	@Path("/definitions/{id}")
	@Produces({ "application/json", "application/xml" })
	public RangerServiceDef getServiceDef(@PathParam("id") Long id) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceREST.getServiceDef(" + id + ")");
		}

		RangerServiceDef ret = null;

		try {
			ret = svcStore.getServiceDef(id);
		} catch(Exception excp) {
			throw restErrorUtil.createRESTException(HttpServletResponse.SC_BAD_REQUEST, excp.getMessage(), true);
		}

		if(ret == null) {
			throw restErrorUtil.createRESTException(HttpServletResponse.SC_NOT_FOUND, "Not found", true);
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceREST.getServiceDef(" + id + "): " + ret);
		}

		return ret;
	}

	@GET
	@Path("/definitions/name/{name}")
	@Produces({ "application/json", "application/xml" })
	public RangerServiceDef getServiceDefByName(@PathParam("name") String name) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceREST.getServiceDefByName(" + name + ")");
		}

		RangerServiceDef ret = null;

		try {
			ret = svcStore.getServiceDefByName(name);
		} catch(Exception excp) {
			throw restErrorUtil.createRESTException(HttpServletResponse.SC_BAD_REQUEST, excp.getMessage(), true);
		}

		if(ret == null) {
			throw restErrorUtil.createRESTException(HttpServletResponse.SC_NOT_FOUND, "Not found", true);
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceREST.getServiceDefByName(" + name + "): " + ret);
		}

		return ret;
	}

	@GET
	@Path("/definitions")
	@Produces({ "application/json", "application/xml" })
	public List<RangerServiceDef> getServiceDefs(@Context HttpServletRequest request) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceREST.getServiceDefs()");
		}

		List<RangerServiceDef> ret = null;

		SearchFilter filter = getSearchFilter(request);

		try {
			ret = svcStore.getServiceDefs(filter);
		} catch(Exception excp) {
			throw restErrorUtil.createRESTException(HttpServletResponse.SC_BAD_REQUEST, excp.getMessage(), true);
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceREST.getServiceDefs(): count=" + (ret == null ? 0 : ret.size()));
		}

		return ret;
	}


	@POST
	@Path("/services")
	@Produces({ "application/json", "application/xml" })
	@PreAuthorize("hasRole('ROLE_SYS_ADMIN')")
	public RangerService createService(RangerService service) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceREST.createService(" + service + ")");
		}

		RangerService ret = null;

		try {
			ret = svcStore.createService(service);
		} catch(Exception excp) {
			throw restErrorUtil.createRESTException(HttpServletResponse.SC_BAD_REQUEST, excp.getMessage(), true);
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceREST.createService(" + service + "): " + ret);
		}

		return ret;
	}

	@PUT
	@Path("/services/{id}")
	@Produces({ "application/json", "application/xml" })
	@PreAuthorize("hasRole('ROLE_SYS_ADMIN')")
	public RangerService updateService(RangerService service) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceREST.updateService(): " + service);
		}

		RangerService ret = null;

		try {
			ret = svcStore.updateService(service);
		} catch(Exception excp) {
			throw restErrorUtil.createRESTException(HttpServletResponse.SC_BAD_REQUEST, excp.getMessage(), true);
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceREST.updateService(" + service + "): " + ret);
		}

		return ret;
	}

	@DELETE
	@Path("/services/{id}")
	@Produces({ "application/json", "application/xml" })
	@PreAuthorize("hasRole('ROLE_SYS_ADMIN')")
	public void deleteService(@PathParam("id") Long id) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceREST.deleteService(" + id + ")");
		}

		try {
			svcStore.deleteService(id);
		} catch(Exception excp) {
			throw restErrorUtil.createRESTException(HttpServletResponse.SC_BAD_REQUEST, excp.getMessage(), true);
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceREST.deleteService(" + id + ")");
		}
	}

	@GET
	@Path("/services/{id}")
	@Produces({ "application/json", "application/xml" })
	public RangerService getService(@PathParam("id") Long id) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceREST.getService(" + id + ")");
		}

		RangerService ret = null;

		try {
			ret = svcStore.getService(id);
		} catch(Exception excp) {
			throw restErrorUtil.createRESTException(HttpServletResponse.SC_BAD_REQUEST, excp.getMessage(), true);
		}

		if(ret == null) {
			throw restErrorUtil.createRESTException(HttpServletResponse.SC_NOT_FOUND, "Not found", true);
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceREST.getService(" + id + "): " + ret);
		}

		return ret;
	}

	@GET
	@Path("/services/name/{name}")
	@Produces({ "application/json", "application/xml" })
	public RangerService getServiceByName(@PathParam("name") String name) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceREST.getServiceByName(" + name + ")");
		}

		RangerService ret = null;

		try {
			ret = svcStore.getServiceByName(name);
		} catch(Exception excp) {
			throw restErrorUtil.createRESTException(HttpServletResponse.SC_BAD_REQUEST, excp.getMessage(), true);
		}

		if(ret == null) {
			throw restErrorUtil.createRESTException(HttpServletResponse.SC_NOT_FOUND, "Not found", true);
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceREST.getServiceByName(" + name + "): " + ret);
		}

		return ret;
	}

	@GET
	@Path("/services")
	@Produces({ "application/json", "application/xml" })
	public List<RangerService> getServices(@Context HttpServletRequest request) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceREST.getServices():");
		}

		List<RangerService> ret = null;

		SearchFilter filter = getSearchFilter(request);

		try {
			ret = svcStore.getServices(filter);
		} catch(Exception excp) {
			throw restErrorUtil.createRESTException(HttpServletResponse.SC_BAD_REQUEST, excp.getMessage(), true);
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceREST.getServices(): count=" + (ret == null ? 0 : ret.size()));
		}

		return ret;
	}

	@GET
	@Path("/services/count")
	@Produces({ "application/json", "application/xml" })
	public Long countServices(@Context HttpServletRequest request) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceREST.countServices():");
		}

		Long ret = null;

		try {
			List<RangerService> services = getServices(request);
			
			ret = new Long(services == null ? 0 : services.size());
		} catch(Exception excp) {
			throw restErrorUtil.createRESTException(HttpServletResponse.SC_BAD_REQUEST, excp.getMessage(), true);
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceREST.countServices(): " + ret);
		}

		return ret;
	}

	@POST
	@Path("/services/validateConfig")
	@Produces({ "application/json", "application/xml" })
	public VXResponse validateConfig(RangerService service) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceREST.validateConfig(" + service + ")");
		}

		VXResponse ret = new VXResponse();

		try {
			ret = serviceMgr.validateConfig(service);
		} catch(Exception excp) {
			throw restErrorUtil.createRESTException(HttpServletResponse.SC_BAD_REQUEST, excp.getMessage(), true);
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceREST.validateConfig(" + service + "): " + ret);
		}

		return ret;
	}
	
	@POST
	@Path("/services/lookupResource/{serviceName}")
	@Produces({ "application/json", "application/xml" })
	public List<String> lookupResource(@PathParam("serviceName") String serviceName, ResourceLookupContext context) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceREST.lookupResource(" + serviceName + ")");
		}

		List<String> ret = new ArrayList<String>();

		try {
			ret = serviceMgr.lookupResource(serviceName,context);
		} catch(Exception excp) {
			LOG.error("lookupResource() failed", excp);

			throw restErrorUtil.createRESTException(HttpServletResponse.SC_BAD_REQUEST, excp.getMessage(), true);
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceREST.validateConfig(" + serviceName + "): " + ret);
		}

		return ret;
	}


	@POST
	@Path("/policies")
	@Produces({ "application/json", "application/xml" })
	public RangerPolicy createPolicy(RangerPolicy policy) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceREST.createPolicy(" + policy + ")");
		}

		RangerPolicy ret = null;

		try {
			ret = svcStore.createPolicy(policy);
		} catch(Exception excp) {
			throw restErrorUtil.createRESTException(HttpServletResponse.SC_BAD_REQUEST, excp.getMessage(), true);
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceREST.createPolicy(" + policy + "): " + ret);
		}

		return ret;
	}

	@PUT
	@Path("/policies/{id}")
	@Produces({ "application/json", "application/xml" })
	public RangerPolicy updatePolicy(RangerPolicy policy) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceREST.updatePolicy(" + policy + ")");
		}

		RangerPolicy ret = null;

		try {
			ret = svcStore.updatePolicy(policy);
		} catch(Exception excp) {
			throw restErrorUtil.createRESTException(HttpServletResponse.SC_BAD_REQUEST, excp.getMessage(), true);
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceREST.updatePolicy(" + policy + "): " + ret);
		}

		return ret;
	}

	@DELETE
	@Path("/policies/{id}")
	@Produces({ "application/json", "application/xml" })
	public void deletePolicy(@PathParam("id") Long id) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceREST.deletePolicy(" + id + ")");
		}

		try {
			svcStore.deletePolicy(id);
		} catch(Exception excp) {
			throw restErrorUtil.createRESTException(HttpServletResponse.SC_BAD_REQUEST, excp.getMessage(), true);
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceREST.deletePolicy(" + id + ")");
		}
	}

	@GET
	@Path("/policies/{id}")
	@Produces({ "application/json", "application/xml" })
	public RangerPolicy getPolicy(@PathParam("id") Long id) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceREST.getPolicy(" + id + ")");
		}

		RangerPolicy ret = null;

		try {
			ret = svcStore.getPolicy(id);
		} catch(Exception excp) {
			throw restErrorUtil.createRESTException(HttpServletResponse.SC_BAD_REQUEST, excp.getMessage(), true);
		}

		if(ret == null) {
			throw restErrorUtil.createRESTException(HttpServletResponse.SC_NOT_FOUND, "Not found", true);
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceREST.getPolicy(" + id + "): " + ret);
		}

		return ret;
	}

	@GET
	@Path("/policies")
	@Produces({ "application/json", "application/xml" })
	public List<RangerPolicy> getPolicies(@Context HttpServletRequest request) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceREST.getPolicies()");
		}

		List<RangerPolicy> ret = null;

		SearchFilter filter = getSearchFilter(request);

		try {
			ret = svcStore.getPolicies(filter);
		} catch(Exception excp) {
			throw restErrorUtil.createRESTException(HttpServletResponse.SC_BAD_REQUEST, excp.getMessage(), true);
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceREST.getPolicies(): count=" + (ret == null ? 0 : ret.size()));
		}

		return ret;
	}

	@GET
	@Path("/policies/count")
	@Produces({ "application/json", "application/xml" })
	public Long countPolicies( @Context HttpServletRequest request) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceREST.countPolicies():");
		}

		Long ret = null;

		try {
			List<RangerPolicy> services = getPolicies(request);
			
			ret = new Long(services == null ? 0 : services.size());
		} catch(Exception excp) {
			throw restErrorUtil.createRESTException(HttpServletResponse.SC_BAD_REQUEST, excp.getMessage(), true);
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceREST.countPolicies(): " + ret);
		}

		return ret;
	}

	@GET
	@Path("/policies/service/{id}")
	@Produces({ "application/json", "application/xml" })
	public List<RangerPolicy> getServicePolicies(@PathParam("id") Long serviceId, @Context HttpServletRequest request) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceREST.getServicePolicies(" + serviceId + ")");
		}

		List<RangerPolicy> ret = null;

		SearchFilter filter = getSearchFilter(request);

		try {
			ret = svcStore.getServicePolicies(serviceId, filter);
		} catch(Exception excp) {
			throw restErrorUtil.createRESTException(HttpServletResponse.SC_BAD_REQUEST, excp.getMessage(), true);
		}

		if(ret == null) {
			throw restErrorUtil.createRESTException(HttpServletResponse.SC_NOT_FOUND, "Not found", true);
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceREST.getServicePolicies(" + serviceId + "): count=" + (ret == null ? 0 : ret.size()));
		}

		return ret;
	}

	@GET
	@Path("/policies/service/name/{name}")
	@Produces({ "application/json", "application/xml" })
	public List<RangerPolicy> getServicePolicies(@PathParam("name") String serviceName, @Context HttpServletRequest request) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceREST.getServicePolicies(" + serviceName + ")");
		}

		List<RangerPolicy> ret = null;

		SearchFilter filter = getSearchFilter(request);

		try {
			ret = svcStore.getServicePolicies(serviceName, filter);
		} catch(Exception excp) {
			throw restErrorUtil.createRESTException(HttpServletResponse.SC_BAD_REQUEST, excp.getMessage(), true);
		}

		if(ret == null) {
			throw restErrorUtil.createRESTException(HttpServletResponse.SC_NOT_FOUND, "Not found", true);
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceREST.getServicePolicies(" + serviceName + "): count=" + (ret == null ? 0 : ret.size()));
		}

		return ret;
	}

	@GET
	@Path("/policies/download/{serviceName}/{lastKnownVersion}")
	@Produces({ "application/json", "application/xml" })
	public ServicePolicies getServicePoliciesIfUpdated(@PathParam("serviceName") String serviceName, @PathParam("lastKnownVersion") Long lastKnownVersion, @Context HttpServletRequest request) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceREST.getServicePoliciesIfUpdated(" + serviceName + ", " + lastKnownVersion + ")");
		}

		ServicePolicies ret      = null;
		int             httpCode = HttpServletResponse.SC_OK;
		String          logMsg   = null;

		try {
			ret = svcStore.getServicePoliciesIfUpdated(serviceName, lastKnownVersion);

			if(ret == null) {
				httpCode = HttpServletResponse.SC_NOT_MODIFIED ;
				logMsg   = "No change since last update";
			} else {
				httpCode = HttpServletResponse.SC_OK;
				logMsg   = "Returning " + (ret.getPolicies() != null ? ret.getPolicies().size() : 0) + " policies. Policy version=" + ret.getPolicyVersion();
			}
		} catch(Exception excp) {
			httpCode = HttpServletResponse.SC_BAD_REQUEST;
			logMsg   = excp.getMessage();
		} finally {
			createPolicyDownloadAudit(serviceName, lastKnownVersion, ret, httpCode, request);
		}

		if(httpCode != HttpServletResponse.SC_OK) {
			throw restErrorUtil.createRESTException(httpCode, logMsg, true);
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceREST.getServicePoliciesIfUpdated(" + serviceName + ", " + lastKnownVersion + "): count=" + ((ret == null || ret.getPolicies() == null) ? 0 : ret.getPolicies().size()));
		}

		return ret;
	}

	private SearchFilter getSearchFilter(HttpServletRequest request) {
		if(request == null || MapUtils.isEmpty(request.getParameterMap())) {
			return null;
		}

		SearchFilter ret = new SearchFilter();

		ret.setParam(SearchFilter.LOGIN_USER, request.getParameter(SearchFilter.LOGIN_USER));
		ret.setParam(SearchFilter.SERVICE_TYPE, request.getParameter(SearchFilter.SERVICE_TYPE));
		ret.setParam(SearchFilter.SERVICE_TYPE_ID, request.getParameter(SearchFilter.SERVICE_TYPE_ID));
		ret.setParam(SearchFilter.SERVICE_NAME, request.getParameter(SearchFilter.SERVICE_NAME));
		ret.setParam(SearchFilter.SERVICE_ID, request.getParameter(SearchFilter.SERVICE_ID));
		ret.setParam(SearchFilter.POLICY_NAME, request.getParameter(SearchFilter.POLICY_NAME));
		ret.setParam(SearchFilter.POLICY_ID, request.getParameter(SearchFilter.POLICY_ID));
		ret.setParam(SearchFilter.STATUS, request.getParameter(SearchFilter.STATUS));
		ret.setParam(SearchFilter.USER, request.getParameter(SearchFilter.USER));
		ret.setParam(SearchFilter.GROUP, request.getParameter(SearchFilter.GROUP));
		ret.setParam(SearchFilter.SORT_BY, request.getParameter(SearchFilter.SORT_BY));
		
		for(Map.Entry<String, String[]> e : request.getParameterMap().entrySet()) {
			String   name   = e.getKey();
			String[] values = e.getValue();
			
			if(!StringUtils.isEmpty(name) && !ArrayUtils.isEmpty(values) && name.startsWith(SearchFilter.RESOURCE_PREFIX)) {
				ret.setParam(name, values[0]);
			}
		}

		return ret;
	}

	private void createPolicyDownloadAudit(String serviceName, Long lastKnownVersion, ServicePolicies policies, int httpRespCode, HttpServletRequest request) {
		try {
			String  agentId   = request.getParameter("agentId");
			String  ipAddress = request.getHeader("X-FORWARDED-FOR");  

			if (ipAddress == null) {  
				ipAddress = request.getRemoteAddr();
			}

			XXPolicyExportAudit policyExportAudit = new XXPolicyExportAudit();

			policyExportAudit.setRepositoryName(serviceName);
			policyExportAudit.setAgentId(agentId);
			policyExportAudit.setClientIP(ipAddress);
			policyExportAudit.setRequestedEpoch(lastKnownVersion);
			policyExportAudit.setHttpRetCode(httpRespCode);

			assetMgr.createPolicyAudit(policyExportAudit);
		} catch(Exception excp) {
			LOG.error("error while creating policy download audit", excp);
		}
	}
}
