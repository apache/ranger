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

import java.util.List;

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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ranger.plugin.manager.ServiceDefManager;
import org.apache.ranger.plugin.manager.ServiceManager;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerService;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.view.VXResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.stereotype.Component;
import org.apache.ranger.common.RESTErrorUtil;


@Path("plugins")
@Component
@Scope("request")
public class ServiceREST {
	private static final Log LOG = LogFactory.getLog(ServiceREST.class);

	@Autowired
	RESTErrorUtil restErrorUtil;

	private ServiceDefManager sdMgr  = null;
	private ServiceManager    svcMgr  = null;

	public ServiceREST() {
		sdMgr  = new ServiceDefManager();
		svcMgr = new ServiceManager();
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
			ret = sdMgr.get(id);
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
			ret = sdMgr.getByName(name);
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

		try {
			ret = sdMgr.getAll();
		} catch(Exception excp) {
			throw restErrorUtil.createRESTException(HttpServletResponse.SC_BAD_REQUEST, excp.getMessage(), true);
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceREST.getServiceDefs(): count=" + (ret == null ? 0 : ret.size()));
		}

		return ret;
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
			ret = sdMgr.create(serviceDef);
		} catch(Exception excp) {
			throw restErrorUtil.createRESTException(HttpServletResponse.SC_BAD_REQUEST, excp.getMessage(), true);
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceREST.createServiceDef(" + serviceDef + "): " + ret);
		}

		return ret;
	}

	@PUT
	@Path("/definitions")
	@Produces({ "application/json", "application/xml" })
	@PreAuthorize("hasRole('ROLE_SYS_ADMIN')")
	public RangerServiceDef updateServiceDef(RangerServiceDef serviceDef) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceREST.updateServiceDef(" + serviceDef + ")");
		}

		RangerServiceDef ret = null;

		try {
			ret = sdMgr.update(serviceDef);
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
			sdMgr.delete(id);
		} catch(Exception excp) {
			throw restErrorUtil.createRESTException(HttpServletResponse.SC_BAD_REQUEST, excp.getMessage(), true);
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceREST.deleteServiceDef(" + id + ")");
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
			ret = svcMgr.get(id);
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
			ret = svcMgr.getByName(name);
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

		try {
			ret = svcMgr.getAll();
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
	@Path("/services")
	@Produces({ "application/json", "application/xml" })
	public RangerService createService(RangerService service) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceREST.createService(" + service + ")");
		}

		RangerService ret = null;

		try {
			ret = svcMgr.create(service);
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
	public RangerService updateService(RangerService service) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceREST.updateService(): " + service);
		}

		RangerService ret = null;

		try {
			ret = svcMgr.update(service);
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
			svcMgr.delete(id);
		} catch(Exception excp) {
			throw restErrorUtil.createRESTException(HttpServletResponse.SC_BAD_REQUEST, excp.getMessage(), true);
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceREST.deleteService(" + id + ")");
		}
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
			svcMgr.validateConfig(service);
		} catch(Exception excp) {
			ret.setStatusCode(VXResponse.STATUS_ERROR);
			// TODO: message
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceREST.validateConfig(" + service + "): " + ret);
		}

		return ret;
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
			ret = svcMgr.getPolicy(id);
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

		try {
			Long serviceId = Long.parseLong(request.getParameter("serviceId"));
			ret = svcMgr.getPolicies(serviceId);
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
	public Long countPolicies(@Context HttpServletRequest request) {
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
	@Path("/services/{id}/policies")
	@Produces({ "application/json", "application/xml" })
	public List<RangerPolicy> getServicePolicies(@PathParam("id") Long serviceId, @Context HttpServletRequest request) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceREST.getServicePolicies(" + serviceId + ")");
		}

		List<RangerPolicy> ret = null;

		try {
			ret = svcMgr.getPolicies(serviceId);
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

	@POST
	@Path("/policies")
	@Produces({ "application/json", "application/xml" })
	public RangerPolicy createPolicy(RangerPolicy policy) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceREST.createPolicy(" + policy + ")");
		}

		RangerPolicy ret = null;

		try {
			ret = svcMgr.createPolicy(policy);
		} catch(Exception excp) {
			throw restErrorUtil.createRESTException(HttpServletResponse.SC_BAD_REQUEST, excp.getMessage(), true);
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceREST.createPolicy(" + policy + "): " + ret);
		}

		return ret;
	}

	@PUT
	@Path("/policies")
	@Produces({ "application/json", "application/xml" })
	public RangerPolicy updatePolicy(RangerPolicy policy) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceREST.updatePolicy(" + policy + ")");
		}

		RangerPolicy ret = null;

		try {
			ret = svcMgr.updatePolicy(policy);
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
	@PreAuthorize("hasRole('ROLE_SYS_ADMIN')")
	public void deletePolicy(@PathParam("id") Long id) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceREST.deletePolicy(" + id + ")");
		}

		try {
			svcMgr.deletePolicy(id);
		} catch(Exception excp) {
			throw restErrorUtil.createRESTException(HttpServletResponse.SC_BAD_REQUEST, excp.getMessage(), true);
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceREST.deletePolicy(" + id + ")");
		}
	}
}
