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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ranger.admin.client.datatype.RESTResponse;
import org.apache.ranger.authorization.hadoop.config.RangerConfiguration;
import org.apache.ranger.biz.AssetMgr;
import org.apache.ranger.biz.RangerBizUtil;
import org.apache.ranger.biz.ServiceDBStore;
import org.apache.ranger.biz.ServiceMgr;
import org.apache.ranger.biz.XUserMgr;
import org.apache.ranger.common.GUIDUtil;
import org.apache.ranger.common.MessageEnums;
import org.apache.ranger.common.RESTErrorUtil;
import org.apache.ranger.common.RangerConfigUtil;
import org.apache.ranger.common.RangerSearchUtil;
import org.apache.ranger.common.RangerValidatorFactory;
import org.apache.ranger.common.ServiceUtil;
import org.apache.ranger.db.RangerDaoManager;
import org.apache.ranger.entity.XXPolicyExportAudit;
import org.apache.ranger.entity.XXService;
import org.apache.ranger.entity.XXServiceDef;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItem;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItemAccess;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyResource;
import org.apache.ranger.plugin.model.RangerService;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.model.validation.RangerPolicyValidator;
import org.apache.ranger.plugin.model.validation.RangerServiceDefValidator;
import org.apache.ranger.plugin.model.validation.RangerServiceValidator;
import org.apache.ranger.plugin.model.validation.RangerValidator.Action;
import org.apache.ranger.plugin.policyengine.RangerAccessResource;
import org.apache.ranger.plugin.policyengine.RangerAccessResourceImpl;
import org.apache.ranger.plugin.policyengine.RangerPolicyEngine;
import org.apache.ranger.plugin.policyengine.RangerPolicyEngineCache;
import org.apache.ranger.plugin.policyengine.RangerPolicyEngineOptions;
import org.apache.ranger.plugin.policyevaluator.RangerPolicyEvaluator;
import org.apache.ranger.plugin.service.ResourceLookupContext;
import org.apache.ranger.plugin.store.PList;
import org.apache.ranger.plugin.store.EmbeddedServiceDefsUtil;
import org.apache.ranger.plugin.util.GrantRevokeRequest;
import org.apache.ranger.plugin.util.RangerPerfTracer;
import org.apache.ranger.plugin.util.SearchFilter;
import org.apache.ranger.plugin.util.ServicePolicies;
import org.apache.ranger.security.context.RangerAPIList;
import org.apache.ranger.service.RangerPolicyService;
import org.apache.ranger.service.RangerServiceDefService;
import org.apache.ranger.service.RangerServiceService;
import org.apache.ranger.view.RangerPolicyList;
import org.apache.ranger.view.RangerServiceDefList;
import org.apache.ranger.view.RangerServiceList;
import org.apache.ranger.view.VXResponse;
import org.apache.ranger.view.VXString;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

@Path("plugins")
@Component
@Scope("request")
@Transactional(propagation = Propagation.REQUIRES_NEW)
public class ServiceREST {
	private static final Log LOG = LogFactory.getLog(ServiceREST.class);
	private static final Log PERF_LOG = RangerPerfTracer.getPerfLogger("rest.ServiceREST");

	@Autowired
	RESTErrorUtil restErrorUtil;

	@Autowired
	ServiceMgr serviceMgr;

	@Autowired
	AssetMgr assetMgr;

	@Autowired
	XUserMgr userMgr;

	@Autowired
	ServiceDBStore svcStore;
	
	@Autowired
	ServiceUtil serviceUtil;
	
	@Autowired
	RangerConfigUtil configUtil;
	
	@Autowired
	RangerPolicyService policyService;
	
	@Autowired
	RangerServiceService svcService;
	
	@Autowired
	RangerServiceDefService serviceDefService;
	
	@Autowired
	RangerSearchUtil searchUtil;
	
    @Autowired
    RangerBizUtil bizUtil;

	@Autowired
	GUIDUtil guidUtil;
	
	@Autowired
	RangerValidatorFactory validatorFactory; 

	@Autowired
	RangerDaoManager daoManager;

	public ServiceREST() {
	}

	@POST
	@Path("/definitions")
	@Produces({ "application/json", "application/xml" })
	@PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.CREATE_SERVICE_DEF + "\")")
	public RangerServiceDef createServiceDef(RangerServiceDef serviceDef) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceREST.createServiceDef(" + serviceDef + ")");
		}

		RangerServiceDef ret  = null;
		RangerPerfTracer perf = null;

		try {
			if(RangerPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
				perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "ServiceREST.createServiceDef(serviceDefName=" + serviceDef.getName() + ")");
			}
			RangerServiceDefValidator validator = validatorFactory.getServiceDefValidator(svcStore);
			validator.validate(serviceDef, Action.CREATE);

			bizUtil.hasAdminPermissions("Service-Def");
			bizUtil.hasKMSPermissions("Service-Def", serviceDef.getImplClass());

			ret = svcStore.createServiceDef(serviceDef);
		} catch(WebApplicationException excp) {
			throw excp;
		} catch(Throwable excp) {
			LOG.error("createServiceDef(" + serviceDef + ") failed", excp);

			throw restErrorUtil.createRESTException(excp.getMessage());
		} finally {
			RangerPerfTracer.log(perf);
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceREST.createServiceDef(" + serviceDef + "): " + ret);
		}

		return ret;
	}

	@PUT
	@Path("/definitions/{id}")
	@Produces({ "application/json", "application/xml" })
	@PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.UPDATE_SERVICE_DEF + "\")")
	public RangerServiceDef updateServiceDef(RangerServiceDef serviceDef) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceREST.updateServiceDef(serviceDefName=" + serviceDef.getName() + ")");
		}

		RangerServiceDef ret  = null;
		RangerPerfTracer perf = null;

		try {
			if(RangerPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
				perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "ServiceREST.updateServiceDef(" + serviceDef.getName() + ")");
			}
			RangerServiceDefValidator validator = validatorFactory.getServiceDefValidator(svcStore);
			validator.validate(serviceDef, Action.UPDATE);

			bizUtil.hasAdminPermissions("Service-Def");
			bizUtil.hasKMSPermissions("Service-Def", serviceDef.getImplClass());

			ret = svcStore.updateServiceDef(serviceDef);
		} catch(WebApplicationException excp) {
			throw excp;
		} catch(Throwable excp) {
			LOG.error("updateServiceDef(" + serviceDef + ") failed", excp);

			throw restErrorUtil.createRESTException(excp.getMessage());
		} finally {
			RangerPerfTracer.log(perf);
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceREST.updateServiceDef(" + serviceDef + "): " + ret);
		}

		return ret;
	}

	@DELETE
	@Path("/definitions/{id}")
	@Produces({ "application/json", "application/xml" })
	@PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.DELETE_SERVICE_DEF + "\")")
	public void deleteServiceDef(@PathParam("id") Long id, @Context HttpServletRequest request) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceREST.deleteServiceDef(" + id + ")");
		}

		RangerPerfTracer perf = null;

		try {
			if(RangerPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
				perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "ServiceREST.deleteServiceDef(serviceDefId=" + id + ")");
			}
			RangerServiceDefValidator validator = validatorFactory.getServiceDefValidator(svcStore);
			validator.validate(id, Action.DELETE);

			bizUtil.hasAdminPermissions("Service-Def");
			XXServiceDef xServiceDef = daoManager.getXXServiceDef().getById(id);
			bizUtil.hasKMSPermissions("Service-Def", xServiceDef.getImplclassname());

			String forceDeleteStr = request.getParameter("forceDelete");
			boolean forceDelete = false;
			if(!StringUtils.isEmpty(forceDeleteStr) && forceDeleteStr.equalsIgnoreCase("true")) {
				forceDelete = true;
			}
			
			svcStore.deleteServiceDef(id, forceDelete);
		} catch(WebApplicationException excp) {
			throw excp;
		} catch(Throwable excp) {
			LOG.error("deleteServiceDef(" + id + ") failed", excp);

			throw restErrorUtil.createRESTException(excp.getMessage());
		} finally {
			RangerPerfTracer.log(perf);
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceREST.deleteServiceDef(" + id + ")");
		}
	}

	@GET
	@Path("/definitions/{id}")
	@Produces({ "application/json", "application/xml" })
	@PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.GET_SERVICE_DEF + "\")")
	public RangerServiceDef getServiceDef(@PathParam("id") Long id) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceREST.getServiceDef(" + id + ")");
		}

		RangerServiceDef ret  = null;
		RangerPerfTracer perf = null;

		try {
			if(RangerPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
				perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "ServiceREST.getServiceDef(serviceDefId=" + id + ")");
			}
			XXServiceDef xServiceDef = daoManager.getXXServiceDef().getById(id);
			if (!bizUtil.hasAccess(xServiceDef, null)) {
				throw restErrorUtil.createRESTException(
						"User is not allowed to access service-def, id: " + xServiceDef.getId(),
						MessageEnums.OPER_NO_PERMISSION);
			}

			ret = svcStore.getServiceDef(id);
		} catch(WebApplicationException excp) {
			throw excp;
		} catch(Throwable excp) {
			LOG.error("getServiceDef(" + id + ") failed", excp);

			throw restErrorUtil.createRESTException(excp.getMessage());
		} finally {
			RangerPerfTracer.log(perf);
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
	@PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.GET_SERVICE_DEF_BY_NAME + "\")")
	public RangerServiceDef getServiceDefByName(@PathParam("name") String name) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceREST.getServiceDefByName(serviceDefName=" + name + ")");
		}

		RangerServiceDef ret  = null;
		RangerPerfTracer perf = null;

		try {
			if(RangerPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
				perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "ServiceREST.getServiceDefByName(" + name + ")");
			}
			XXServiceDef xServiceDef = daoManager.getXXServiceDef().findByName(name);
			if (xServiceDef != null) {
				if (!bizUtil.hasAccess(xServiceDef, null)) {
					throw restErrorUtil.createRESTException(
							"User is not allowed to access service-def: " + xServiceDef.getName(),
							MessageEnums.OPER_NO_PERMISSION);
				}
			}

			ret = svcStore.getServiceDefByName(name);
		} catch(WebApplicationException excp) {
			throw excp;
		} catch(Throwable excp) {
			LOG.error("getServiceDefByName(" + name + ") failed", excp);

			throw restErrorUtil.createRESTException(excp.getMessage());
		} finally {
			RangerPerfTracer.log(perf);
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
	@PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.GET_SERVICE_DEFS + "\")")
	public RangerServiceDefList getServiceDefs(@Context HttpServletRequest request) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceREST.getServiceDefs()");
		}

		RangerServiceDefList ret  = null;
		RangerPerfTracer     perf = null;

		PList<RangerServiceDef> paginatedSvcDefs = null;

		SearchFilter filter = searchUtil.getSearchFilter(request, serviceDefService.sortFields);

		try {
			if(RangerPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
				perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "ServiceREST.getServiceDefs()");
			}
			paginatedSvcDefs = svcStore.getPaginatedServiceDefs(filter);

			if(paginatedSvcDefs != null) {
				ret = new RangerServiceDefList();

				ret.setServiceDefs(paginatedSvcDefs.getList());
				ret.setPageSize(paginatedSvcDefs.getPageSize());
				ret.setResultSize(paginatedSvcDefs.getResultSize());
				ret.setStartIndex(paginatedSvcDefs.getStartIndex());
				ret.setTotalCount(paginatedSvcDefs.getTotalCount());
				ret.setSortBy(paginatedSvcDefs.getSortBy());
				ret.setSortType(paginatedSvcDefs.getSortType());
			}
		} catch(WebApplicationException excp) {
			throw excp;
		} catch (Throwable excp) {
			LOG.error("getServiceDefs() failed", excp);

			throw restErrorUtil.createRESTException(excp.getMessage());
		} finally {
			RangerPerfTracer.log(perf);
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceREST.getServiceDefs(): count=" + (ret == null ? 0 : ret.getListSize()));
		}
		return ret;
	}

	@POST
	@Path("/services")
	@Produces({ "application/json", "application/xml" })
	@PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.CREATE_SERVICE + "\")")
	public RangerService createService(RangerService service) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceREST.createService(" + service + ")");
		}

		RangerService    ret  = null;
		RangerPerfTracer perf = null;

		try {
			if(RangerPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
				perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "ServiceREST.createService(serviceName=" + service.getName() + ")");
			}
			RangerServiceValidator validator = validatorFactory.getServiceValidator(svcStore);
			validator.validate(service, Action.CREATE);

			bizUtil.hasAdminPermissions("Services");

			// TODO: As of now we are allowing SYS_ADMIN to create all the
			// services including KMS

			XXServiceDef xxServiceDef = daoManager.getXXServiceDef().findByName(service.getType());
			bizUtil.hasKMSPermissions("Service", xxServiceDef.getImplclassname());

			ret = svcStore.createService(service);
		} catch(WebApplicationException excp) {
			throw excp;
		} catch(Throwable excp) {
			LOG.error("createService(" + service + ") failed", excp);

			throw restErrorUtil.createRESTException(excp.getMessage());
		} finally {
			RangerPerfTracer.log(perf);
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceREST.createService(" + service + "): " + ret);
		}

		return ret;
	}

	@PUT
	@Path("/services/{id}")
	@Produces({ "application/json", "application/xml" })
	@PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.UPDATE_SERVICE + "\")")
	public RangerService updateService(RangerService service) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceREST.updateService(): " + service);
		}

		RangerService    ret  = null;
		RangerPerfTracer perf = null;

		try {
			if(RangerPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
				perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "ServiceREST.updateService(serviceName=" + service.getName() + ")");
			}
			RangerServiceValidator validator = validatorFactory.getServiceValidator(svcStore);
			validator.validate(service, Action.UPDATE);

			bizUtil.hasAdminPermissions("Services");

			// TODO: As of now we are allowing SYS_ADMIN to create all the
			// services including KMS

			XXServiceDef xxServiceDef = daoManager.getXXServiceDef().findByName(service.getType());
			bizUtil.hasKMSPermissions("Service", xxServiceDef.getImplclassname());

			ret = svcStore.updateService(service);
		} catch(WebApplicationException excp) {
			throw excp;
		} catch(Throwable excp) {
			LOG.error("updateService(" + service + ") failed", excp);

			throw restErrorUtil.createRESTException(excp.getMessage());
		} finally {
			RangerPerfTracer.log(perf);
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceREST.updateService(" + service + "): " + ret);
		}

		return ret;
	}

	@DELETE
	@Path("/services/{id}")
	@Produces({ "application/json", "application/xml" })
	@PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.DELETE_SERVICE + "\")")
	public void deleteService(@PathParam("id") Long id) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceREST.deleteService(" + id + ")");
		}

		RangerPerfTracer perf = null;

		try {
			if(RangerPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
				perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "ServiceREST.deleteService(serviceId=" + id + ")");
			}
			RangerServiceValidator validator = validatorFactory.getServiceValidator(svcStore);
			validator.validate(id, Action.DELETE);

			bizUtil.hasAdminPermissions("Services");

			// TODO: As of now we are allowing SYS_ADMIN to create all the
			// services including KMS

			XXService service = daoManager.getXXService().getById(id);
			XXServiceDef xxServiceDef = daoManager.getXXServiceDef().getById(service.getType());
			bizUtil.hasKMSPermissions("Service", xxServiceDef.getImplclassname());

			svcStore.deleteService(id);
		} catch(WebApplicationException excp) {
			throw excp;
		} catch(Throwable excp) {
			LOG.error("deleteService(" + id + ") failed", excp);

			throw restErrorUtil.createRESTException(excp.getMessage());
		} finally {
			RangerPerfTracer.log(perf);
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceREST.deleteService(" + id + ")");
		}
	}

	@GET
	@Path("/services/{id}")
	@Produces({ "application/json", "application/xml" })
	@PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.GET_SERVICE + "\")")
	public RangerService getService(@PathParam("id") Long id) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceREST.getService(" + id + ")");
		}

		RangerService    ret  = null;
		RangerPerfTracer perf = null;

		try {
			if(RangerPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
				perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "ServiceREST.getService(serviceId=" + id + ")");
			}
			ret = svcStore.getService(id);
		} catch(WebApplicationException excp) {
			throw excp;
		} catch(Throwable excp) {
			LOG.error("getService(" + id + ") failed", excp);

			throw restErrorUtil.createRESTException(excp.getMessage());
		} finally {
			RangerPerfTracer.log(perf);
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
	@PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.GET_SERVICE_BY_NAME + "\")")
	public RangerService getServiceByName(@PathParam("name") String name) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceREST.getServiceByName(" + name + ")");
		}

		RangerService    ret  = null;
		RangerPerfTracer perf = null;

		try {
			if(RangerPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
				perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "ServiceREST.getService(serviceName=" + name + ")");
			}
			ret = svcStore.getServiceByName(name);
		} catch(WebApplicationException excp) {
			throw excp;
		} catch(Throwable excp) {
			LOG.error("getServiceByName(" + name + ") failed", excp);

			throw restErrorUtil.createRESTException(excp.getMessage());
		} finally {
			RangerPerfTracer.log(perf);
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
	@PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.GET_SERVICES + "\")")
	public RangerServiceList getServices(@Context HttpServletRequest request) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceREST.getServices()");
		}

		RangerServiceList ret  = null;
		RangerPerfTracer  perf = null;

		PList<RangerService> paginatedSvcs = null;

		SearchFilter filter = searchUtil.getSearchFilter(request, svcService.sortFields);

		try {
			if(RangerPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
				perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "ServiceREST.getServices()");
			}
			paginatedSvcs = svcStore.getPaginatedServices(filter);

			if(paginatedSvcs != null) {
				ret = new RangerServiceList();

				ret.setServices(paginatedSvcs.getList());
				ret.setPageSize(paginatedSvcs.getPageSize());
				ret.setResultSize(paginatedSvcs.getResultSize());
				ret.setStartIndex(paginatedSvcs.getStartIndex());
				ret.setTotalCount(paginatedSvcs.getTotalCount());
				ret.setSortBy(paginatedSvcs.getSortBy());
				ret.setSortType(paginatedSvcs.getSortType());
			}
		} catch(WebApplicationException excp) {
			throw excp;
		} catch (Throwable excp) {
			LOG.error("getServices() failed", excp);

			throw restErrorUtil.createRESTException(excp.getMessage());
		} finally {
			RangerPerfTracer.log(perf);
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceREST.getServices(): count=" + (ret == null ? 0 : ret.getListSize()));
		}
		return ret;
	}

	public List<RangerService> getServices(SearchFilter filter) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceREST.getServices():");
		}

		List<RangerService> ret  = null;
		RangerPerfTracer    perf = null;

		try {
			if(RangerPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
				perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "ServiceREST.getServices()");
			}
			ret = svcStore.getServices(filter);
		} catch(WebApplicationException excp) {
			throw excp;
		} catch(Throwable excp) {
			LOG.error("getServices() failed", excp);

			throw restErrorUtil.createRESTException(excp.getMessage());
		} finally {
			RangerPerfTracer.log(perf);
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceREST.getServices(): count=" + (ret == null ? 0 : ret.size()));
		}

		return ret;
	}


	@GET
	@Path("/services/count")
	@Produces({ "application/json", "application/xml" })
	@PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.COUNT_SERVICES + "\")")
	public Long countServices(@Context HttpServletRequest request) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceREST.countServices():");
		}

		Long             ret  = null;
		RangerPerfTracer perf = null;

		try {
			if(RangerPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
				perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "ServiceREST.countService()");
			}
			List<RangerService> services = getServices(request).getServices();
			
			ret = new Long(services == null ? 0 : services.size());
		} catch(WebApplicationException excp) {
			throw excp;
		} catch(Throwable excp) {
			LOG.error("countServices() failed", excp);

			throw restErrorUtil.createRESTException(excp.getMessage());
		} finally {
			RangerPerfTracer.log(perf);
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceREST.countServices(): " + ret);
		}

		return ret;
	}

	@POST
	@Path("/services/validateConfig")
	@Produces({ "application/json", "application/xml" })
	@PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.VALIDATE_CONFIG + "\")")
	public VXResponse validateConfig(RangerService service) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceREST.validateConfig(" + service + ")");
		}

		VXResponse       ret  = new VXResponse();
		RangerPerfTracer perf = null;

		try {
			if(RangerPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
				perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "ServiceREST.validateConfig(serviceName=" + service.getName() + ")");
			}
			ret = serviceMgr.validateConfig(service, svcStore);
		} catch(WebApplicationException excp) {
			throw excp;
		} catch(Throwable excp) {
			LOG.error("validateConfig(" + service + ") failed", excp);

			throw restErrorUtil.createRESTException(excp.getMessage());
		} finally {
			RangerPerfTracer.log(perf);
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceREST.validateConfig(" + service + "): " + ret);
		}

		return ret;
	}
	
	@POST
	@Path("/services/lookupResource/{serviceName}")
	@Produces({ "application/json", "application/xml" })
	@PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.LOOKUP_RESOURCE + "\")")
	public List<String> lookupResource(@PathParam("serviceName") String serviceName, ResourceLookupContext context) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceREST.lookupResource(" + serviceName + ")");
		}

		List<String>     ret  = new ArrayList<String>();
		RangerPerfTracer perf = null;

		try {
			if(RangerPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
				perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "ServiceREST.lookupResource(serviceName=" + serviceName + ")");
			}
			ret = serviceMgr.lookupResource(serviceName, context, svcStore);
		} catch(WebApplicationException excp) {
			throw excp;
		} catch(Throwable excp) {
			LOG.error("lookupResource(" + serviceName + ", " + context + ") failed", excp);

			throw restErrorUtil.createRESTException(excp.getMessage());
		} finally {
			RangerPerfTracer.log(perf);
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceREST.lookupResource(" + serviceName + "): " + ret);
		}

		return ret;
	}

	@POST
	@Path("/services/grant/{serviceName}")
	@Produces({ "application/json", "application/xml" })
	public RESTResponse grantAccess(@PathParam("serviceName") String serviceName, GrantRevokeRequest grantRequest, @Context HttpServletRequest request) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceREST.grantAccess(" + serviceName + ", " + grantRequest + ")");
		}

		RESTResponse     ret  = new RESTResponse();
		RangerPerfTracer perf = null;

		if (serviceUtil.isValidateHttpsAuthentication(serviceName, request)) {

			try {
				if(RangerPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
					perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "ServiceREST.grantAccess(serviceName=" + serviceName + ")");
				}

				String               userName   = grantRequest.getGrantor();
				Set<String>          userGroups = userMgr.getGroupsForUser(userName);
				RangerAccessResource resource   = new RangerAccessResourceImpl(grantRequest.getResource());
				RangerPolicyEngine   policyEngine = getPolicyEngine(serviceName);
	
				boolean isAdmin = hasAdminAccess(policyEngine, userName, userGroups, resource);
	
				if(!isAdmin) {
					throw restErrorUtil.createRESTException(HttpServletResponse.SC_UNAUTHORIZED, "", true);
				}
	
				RangerPolicy policy = getExactMatchPolicyForResource(policyEngine, resource);
		
				if(policy != null) {
					boolean policyUpdated = false;
	
					// replace all existing privileges for users and groups
					if(grantRequest.getReplaceExistingPermissions()) {
						policyUpdated = removeUsersAndGroupsFromPolicy(policy, grantRequest.getUsers(), grantRequest.getGroups());
					}
	
					for(String user : grantRequest.getUsers()) {
						RangerPolicyItem policyItem = getPolicyItemForUser(policy, user);
						
						if(policyItem != null) {
							if(addAccesses(policyItem, grantRequest.getAccessTypes())) {
								policyUpdated = true;
							}
						} else {
							policyItem = new RangerPolicyItem();
							
							policyItem.getUsers().add(user);
							addAccesses(policyItem, grantRequest.getAccessTypes());
							policy.getPolicyItems().add(policyItem);
	
							policyUpdated = true;
						}
	
						if(grantRequest.getDelegateAdmin()) {
							if(!policyItem.getDelegateAdmin()) {
								policyItem.setDelegateAdmin(Boolean.TRUE);
		
								policyUpdated = true;
							}
						}
					}
	
					for(String group : grantRequest.getGroups()) {
						RangerPolicyItem policyItem = getPolicyItemForGroup(policy, group);
						
						if(policyItem != null) {
							if(addAccesses(policyItem, grantRequest.getAccessTypes())) {
								policyUpdated = true;
							}
						} else {
							policyItem = new RangerPolicyItem();
							
							policyItem.getGroups().add(group);
							addAccesses(policyItem, grantRequest.getAccessTypes());
							policy.getPolicyItems().add(policyItem);
	
							policyUpdated = true;
						}
	
						if(grantRequest.getDelegateAdmin()) {
							if(!policyItem.getDelegateAdmin()) {
								policyItem.setDelegateAdmin(Boolean.TRUE);
		
								policyUpdated = true;
							}
						}
					}
	
					if(policyUpdated) {
						svcStore.updatePolicy(policy);
					}
				} else {
					policy = new RangerPolicy();
					policy.setService(serviceName);
					policy.setName("grant-" + System.currentTimeMillis()); // TODO: better policy name
					policy.setDescription("created by grant");
					policy.setIsAuditEnabled(grantRequest.getEnableAudit());
					policy.setCreatedBy(userName);
		
					Map<String, RangerPolicyResource> policyResources = new HashMap<String, RangerPolicyResource>();
					Set<String>                       resourceNames   = resource.getKeys();
		
					if(! CollectionUtils.isEmpty(resourceNames)) {
						for(String resourceName : resourceNames) {
							RangerPolicyResource policyResource = new RangerPolicyResource(resource.getValue(resourceName));
							policyResource.setIsRecursive(grantRequest.getIsRecursive());
	
							policyResources.put(resourceName, policyResource);
						}
					}
					policy.setResources(policyResources);
		
					for(String user : grantRequest.getUsers()) {
						RangerPolicyItem policyItem = new RangerPolicyItem();
			
						policyItem.getUsers().add(user);
						for(String accessType : grantRequest.getAccessTypes()) {
							policyItem.getAccesses().add(new RangerPolicyItemAccess(accessType, Boolean.TRUE));
						}
						policyItem.setDelegateAdmin(grantRequest.getDelegateAdmin());
						policy.getPolicyItems().add(policyItem);
					}
					
					for(String group : grantRequest.getGroups()) {
						RangerPolicyItem policyItem = new RangerPolicyItem();
			
						policyItem.getGroups().add(group);
						for(String accessType : grantRequest.getAccessTypes()) {
							policyItem.getAccesses().add(new RangerPolicyItemAccess(accessType, Boolean.TRUE));
						}
						policyItem.setDelegateAdmin(grantRequest.getDelegateAdmin());
						policy.getPolicyItems().add(policyItem);
					}
		
					svcStore.createPolicy(policy);
				}
			} catch(WebApplicationException excp) {
				throw excp;
			} catch(Throwable excp) {
				LOG.error("grantAccess(" + serviceName + ", " + grantRequest + ") failed", excp);
	
				throw restErrorUtil.createRESTException(excp.getMessage());
			} finally {
				RangerPerfTracer.log(perf);
			}
	
			ret.setStatusCode(RESTResponse.STATUS_SUCCESS);
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceREST.grantAccess(" + serviceName + ", " + grantRequest + "): " + ret);
		}

		return ret;
	}

	@POST
	@Path("/services/revoke/{serviceName}")
	@Produces({ "application/json", "application/xml" })
	public RESTResponse revokeAccess(@PathParam("serviceName") String serviceName, GrantRevokeRequest revokeRequest, @Context HttpServletRequest request) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceREST.revokeAccess(" + serviceName + ", " + revokeRequest + ")");
		}

		RESTResponse     ret  = new RESTResponse();
		RangerPerfTracer perf = null;

		if (serviceUtil.isValidateHttpsAuthentication(serviceName,request)) {

			try {
				if(RangerPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
					perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "ServiceREST.revokeAccess(serviceName=" + serviceName + ")");
				}

				String               userName     = revokeRequest.getGrantor();
				Set<String>          userGroups   =  userMgr.getGroupsForUser(userName);
				RangerAccessResource resource     = new RangerAccessResourceImpl(revokeRequest.getResource());
				RangerPolicyEngine   policyEngine = getPolicyEngine(serviceName);

				boolean isAdmin = hasAdminAccess(policyEngine, userName, userGroups, resource);
				
				if(!isAdmin) {
					throw restErrorUtil.createRESTException(HttpServletResponse.SC_UNAUTHORIZED, "", true);
				}
	
				RangerPolicy policy = getExactMatchPolicyForResource(policyEngine, resource);
				
				if(policy != null) {
					boolean policyUpdated = false;

					// remove all existing privileges for users and groups
					if(revokeRequest.getReplaceExistingPermissions()) {
						policyUpdated = removeUsersAndGroupsFromPolicy(policy, revokeRequest.getUsers(), revokeRequest.getGroups());
					} else {
						for(String user : revokeRequest.getUsers()) {
							RangerPolicyItem policyItem = getPolicyItemForUser(policy, user);

							if (policyItem != null) {
								if (removeAccesses(policyItem, revokeRequest.getAccessTypes())) {
									policyUpdated = true;
								}

								if (revokeRequest.getDelegateAdmin()) { // remove delegate?
									if (policyItem.getDelegateAdmin()) {
										policyItem.setDelegateAdmin(Boolean.FALSE);
										policyUpdated = true;
									}

								}
							}
						}
	
						for(String group : revokeRequest.getGroups()) {
							RangerPolicyItem policyItem = getPolicyItemForGroup(policy, group);
						
							if(policyItem != null) {
								if(removeAccesses(policyItem, revokeRequest.getAccessTypes())) {
									policyUpdated = true;
								}

								if(revokeRequest.getDelegateAdmin()) { // remove delegate?
									if(policyItem.getDelegateAdmin()) {
										policyItem.setDelegateAdmin(Boolean.FALSE);
										policyUpdated = true;
									}
								}
							}
						}
	
						if(compactPolicy(policy)) {
							policyUpdated = true;
						}
					}
	
					if(policyUpdated) {
						svcStore.updatePolicy(policy);
					}
				} else {
					// nothing to revoke!
				}
			} catch(WebApplicationException excp) {
				throw excp;
			} catch(Throwable excp) {
				LOG.error("revokeAccess(" + serviceName + ", " + revokeRequest + ") failed", excp);
	
				throw restErrorUtil.createRESTException(excp.getMessage());
			} finally {
				RangerPerfTracer.log(perf);
			}
	
			ret.setStatusCode(RESTResponse.STATUS_SUCCESS);
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceREST.revokeAccess(" + serviceName + ", " + revokeRequest + "): " + ret);
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

		RangerPolicy     ret  = null;
		RangerPerfTracer perf = null;

		try {
			if(RangerPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
				perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "ServiceREST.createPolicy(policyName=" + policy.getName() + ")");
			}
			// this needs to happen before validator is called
			// set name of policy if unspecified
			if (StringUtils.isBlank(policy.getName())) { // use of isBlank over isEmpty is deliberate as a blank string does not strike us as a particularly useful policy name!
				String guid = policy.getGuid();
				if (StringUtils.isBlank(guid)) { // use of isBlank is deliberate. External parties could send the guid in, perhaps to sync between dev/test/prod instances?
					guid = guidUtil.genGUID();
					policy.setGuid(guid);
					if (LOG.isDebugEnabled()) {
						LOG.debug("No GUID supplied on the policy!  Ok, setting GUID to [" + guid + "].");
					}
				}
				String name = policy.getService() + "-" + guid;
				policy.setName(name);
				if (LOG.isDebugEnabled()) {
					LOG.debug("Policy did not have its name set!  Ok, setting name to [" + name + "]");
				}
			}
			RangerPolicyValidator validator = validatorFactory.getPolicyValidator(svcStore);
			validator.validate(policy, Action.CREATE, bizUtil.isAdmin());

			ensureAdminAccess(policy.getService(), policy.getResources());

			ret = svcStore.createPolicy(policy);
		} catch(WebApplicationException excp) {
			throw excp;
		} catch(Throwable excp) {
			LOG.error("createPolicy(" + policy + ") failed", excp);

			throw restErrorUtil.createRESTException(excp.getMessage());
		} finally {
			RangerPerfTracer.log(perf);
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

		RangerPolicy     ret  = null;
		RangerPerfTracer perf = null;

		try {
			if(RangerPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
				perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "ServiceREST.updatePolicy(policyId=" + policy.getId() + ")");
			}
			RangerPolicyValidator validator = validatorFactory.getPolicyValidator(svcStore);
			validator.validate(policy, Action.UPDATE, bizUtil.isAdmin());

			ensureAdminAccess(policy.getService(), policy.getResources());

			ret = svcStore.updatePolicy(policy);
		} catch(WebApplicationException excp) {
			throw excp;
		} catch(Throwable excp) {
			LOG.error("updatePolicy(" + policy + ") failed", excp);

			throw restErrorUtil.createRESTException(excp.getMessage());
		} finally {
			RangerPerfTracer.log(perf);
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

		RangerPerfTracer perf = null;

		try {
			if(RangerPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
				perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "ServiceREST.deletePolicy(policyId=" + id + ")");
			}
			RangerPolicyValidator validator = validatorFactory.getPolicyValidator(svcStore);
			validator.validate(id, Action.DELETE);

			RangerPolicy policy = svcStore.getPolicy(id);

			ensureAdminAccess(policy.getService(), policy.getResources());

			svcStore.deletePolicy(id);
		} catch(WebApplicationException excp) {
			throw excp;
		} catch(Throwable excp) {
			LOG.error("deletePolicy(" + id + ") failed", excp);

			throw restErrorUtil.createRESTException(excp.getMessage());
		} finally {
			RangerPerfTracer.log(perf);
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

		RangerPolicy     ret  = null;
		RangerPerfTracer perf = null;

		try {
			if(RangerPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
				perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "ServiceREST.getPolicy(policyId=" + id + ")");
			}
			ret = svcStore.getPolicy(id);

			if(ret != null) {
				ensureAdminAccess(ret.getService(), ret.getResources());
			}
		} catch(WebApplicationException excp) {
			throw excp;
		} catch(Throwable excp) {
			LOG.error("getPolicy(" + id + ") failed", excp);

			throw restErrorUtil.createRESTException(excp.getMessage());
		} finally {
			RangerPerfTracer.log(perf);
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
	public RangerPolicyList getPolicies(@Context HttpServletRequest request) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceREST.getPolicies()");
		}

		RangerPolicyList ret  = new RangerPolicyList();
		RangerPerfTracer perf = null;

		SearchFilter filter = searchUtil.getSearchFilter(request, policyService.sortFields);

		try {
			if(RangerPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
				perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "ServiceREST.getPolicies()");
			}

			if(isAdminUserWithNoFilterParams(filter)) {
				PList<RangerPolicy> policies = svcStore.getPaginatedPolicies(filter);

				ret = toRangerPolicyList(policies);
			} else {
				// get all policies from the store; pick the page to return after applying filter
				int savedStartIndex = filter == null ? 0 : filter.getStartIndex();
				int savedMaxRows    = filter == null ? Integer.MAX_VALUE : filter.getMaxRows();

				if(filter != null) {
					filter.setStartIndex(0);
					filter.setMaxRows(Integer.MAX_VALUE);
				}

				List<RangerPolicy> policies = svcStore.getPolicies(filter);

				if(filter != null) {
					filter.setStartIndex(savedStartIndex);
					filter.setMaxRows(savedMaxRows);
				}

				policies = applyAdminAccessFilter(policies);

				ret = toRangerPolicyList(policies, filter);
			}

		} catch(WebApplicationException excp) {
			throw excp;
		} catch (Throwable excp) {
			LOG.error("getPolicies() failed", excp);

			throw restErrorUtil.createRESTException(excp.getMessage());
		} finally {
			RangerPerfTracer.log(perf);
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceREST.getPolicies(): count=" + (ret == null ? 0 : ret.getListSize()));
		}
		return ret;
	}

	public List<RangerPolicy> getPolicies(SearchFilter filter) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceREST.getPolicies(filter)");
		}

		List<RangerPolicy> ret  = null;
		RangerPerfTracer   perf = null;

		try {
			if(RangerPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
				perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "ServiceREST.getPolicies()");
			}
			ret = svcStore.getPolicies(filter);

			ret = applyAdminAccessFilter(ret);
		} catch(WebApplicationException excp) {
			throw excp;
		} catch(Throwable excp) {
			LOG.error("getPolicies() failed", excp);

			throw restErrorUtil.createRESTException(excp.getMessage());
		} finally {
			RangerPerfTracer.log(perf);
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceREST.getPolicies(filter): count=" + (ret == null ? 0 : ret.size()));
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

		Long             ret  = null;
		RangerPerfTracer perf = null;

		try {
			if(RangerPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
				perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "ServiceREST.countPolicies()");
			}
			List<RangerPolicy> policies = getPolicies(request).getPolicies();

			policies = applyAdminAccessFilter(policies);
			
			ret = new Long(policies == null ? 0 : policies.size());
		} catch(WebApplicationException excp) {
			throw excp;
		} catch(Throwable excp) {
			LOG.error("countPolicies() failed", excp);

			throw restErrorUtil.createRESTException(excp.getMessage());
		} finally {
			RangerPerfTracer.log(perf);
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceREST.countPolicies(): " + ret);
		}

		return ret;
	}

	@GET
	@Path("/policies/service/{id}")
	@Produces({ "application/json", "application/xml" })
	public RangerPolicyList getServicePolicies(@PathParam("id") Long serviceId,
			@Context HttpServletRequest request) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceREST.getServicePolicies(" + serviceId + ")");
		}

		RangerPolicyList ret  = new RangerPolicyList();
		RangerPerfTracer perf = null;

		SearchFilter filter = searchUtil.getSearchFilter(request, policyService.sortFields);

		try {
			if(RangerPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
				perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "ServiceREST.getServicePolicies(serviceId=" + serviceId + ")");
			}

			if(isAdminUserWithNoFilterParams(filter)) {
				PList<RangerPolicy> policies = svcStore.getPaginatedServicePolicies(serviceId, filter);

				ret = toRangerPolicyList(policies);
			} else {
				// get all policies from the store; pick the page to return after applying filter
				int savedStartIndex = filter == null ? 0 : filter.getStartIndex();
				int savedMaxRows    = filter == null ? Integer.MAX_VALUE : filter.getMaxRows();

				if(filter != null) {
					filter.setStartIndex(0);
					filter.setMaxRows(Integer.MAX_VALUE);
				}

				List<RangerPolicy> servicePolicies = svcStore.getServicePolicies(serviceId, filter);

				if(filter != null) {
					filter.setStartIndex(savedStartIndex);
					filter.setMaxRows(savedMaxRows);
				}

				servicePolicies = applyAdminAccessFilter(servicePolicies);

				ret = toRangerPolicyList(servicePolicies, filter);
			}
		} catch(WebApplicationException excp) {
			throw excp;
		} catch (Throwable excp) {
			LOG.error("getServicePolicies(" + serviceId + ") failed", excp);

			throw restErrorUtil.createRESTException(excp.getMessage());
		} finally {
			RangerPerfTracer.log(perf);
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceREST.getServicePolicies(" + serviceId + "): count="
					+ (ret == null ? 0 : ret.getListSize()));
		}
		return ret;
	}

	@GET
	@Path("/policies/service/name/{name}")
	@Produces({ "application/json", "application/xml" })
	public RangerPolicyList getServicePoliciesByName(@PathParam("name") String serviceName,
			@Context HttpServletRequest request) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceREST.getServicePolicies(" + serviceName + ")");
		}

		RangerPolicyList ret  = new RangerPolicyList();
		RangerPerfTracer perf = null;

		SearchFilter filter = searchUtil.getSearchFilter(request, policyService.sortFields);

		try {
			if(RangerPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
				perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "ServiceREST.getServicePolicies(serviceName=" + serviceName + ")");
			}

			if(isAdminUserWithNoFilterParams(filter)) {
				PList<RangerPolicy> policies = svcStore.getPaginatedServicePolicies(serviceName, filter);

				ret = toRangerPolicyList(policies);
			} else {
				// get all policies from the store; pick the page to return after applying filter
				int savedStartIndex = filter == null ? 0 : filter.getStartIndex();
				int savedMaxRows    = filter == null ? Integer.MAX_VALUE : filter.getMaxRows();

				if(filter != null) {
					filter.setStartIndex(0);
					filter.setMaxRows(Integer.MAX_VALUE);
				}

				List<RangerPolicy> servicePolicies = svcStore.getServicePolicies(serviceName, filter);

				if(filter != null) {
					filter.setStartIndex(savedStartIndex);
					filter.setMaxRows(savedMaxRows);
				}

				servicePolicies = applyAdminAccessFilter(servicePolicies);

				ret = toRangerPolicyList(servicePolicies, filter);
			}
		} catch(WebApplicationException excp) {
			throw excp;
		} catch (Throwable excp) {
			LOG.error("getServicePolicies(" + serviceName + ") failed", excp);

			throw restErrorUtil.createRESTException(excp.getMessage());
		} finally {
			RangerPerfTracer.log(perf);
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceREST.getServicePolicies(" + serviceName + "): count="
					+ (ret == null ? 0 : ret.getListSize()));
		}

		return ret;
	}

	@GET
	@Path("/policies/download/{serviceName}")
	@Produces({ "application/json", "application/xml" })
	public ServicePolicies getServicePoliciesIfUpdated(@PathParam("serviceName") String serviceName, @QueryParam("lastKnownVersion") Long lastKnownVersion, @QueryParam("pluginId") String pluginId, @Context HttpServletRequest request) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceREST.getServicePoliciesIfUpdated(" + serviceName + ", " + lastKnownVersion + ")");
		}

		ServicePolicies ret      = null;
		int             httpCode = HttpServletResponse.SC_OK;
		String          logMsg   = null;
		RangerPerfTracer perf    = null;

		if (serviceUtil.isValidateHttpsAuthentication(serviceName, request)) {
			if(lastKnownVersion == null) {
				lastKnownVersion = new Long(-1);
			}
			
			try {
				if(RangerPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
					perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "ServiceREST.getServicePoliciesIfUpdated(serviceName=" + serviceName + ",lastKnownVersion=" + lastKnownVersion + ")");
				}
				ret = svcStore.getServicePoliciesIfUpdated(serviceName, lastKnownVersion);
	
				if(ret == null) {
					httpCode = HttpServletResponse.SC_NOT_MODIFIED;
					logMsg   = "No change since last update";
				} else {
					httpCode = HttpServletResponse.SC_OK;
					logMsg   = "Returning " + (ret.getPolicies() != null ? ret.getPolicies().size() : 0) + " policies. Policy version=" + ret.getPolicyVersion();
				}
			} catch(Throwable excp) {
				LOG.error("getServicePoliciesIfUpdated(" + serviceName + ", " + lastKnownVersion + ") failed", excp);
	
				httpCode = HttpServletResponse.SC_BAD_REQUEST;
				logMsg   = excp.getMessage();
			} finally {
				createPolicyDownloadAudit(serviceName, lastKnownVersion, pluginId, ret, httpCode, request);

				RangerPerfTracer.log(perf);
			}
	
			if(httpCode != HttpServletResponse.SC_OK) {
				boolean logError = httpCode != HttpServletResponse.SC_NOT_MODIFIED;
				throw restErrorUtil.createRESTException(httpCode, logMsg, logError);
			}
		 }
 
		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceREST.getServicePoliciesIfUpdated(" + serviceName + ", " + lastKnownVersion + "): count=" + ((ret == null || ret.getPolicies() == null) ? 0 : ret.getPolicies().size()));
		}
   
		return ret;
	}

	private void createPolicyDownloadAudit(String serviceName, Long lastKnownVersion, String pluginId, ServicePolicies policies, int httpRespCode, HttpServletRequest request) {
		try {
			String ipAddress = request.getHeader("X-FORWARDED-FOR");

			if (ipAddress == null) {  
				ipAddress = request.getRemoteAddr();
			}

			XXPolicyExportAudit policyExportAudit = new XXPolicyExportAudit();

			policyExportAudit.setRepositoryName(serviceName);
			policyExportAudit.setAgentId(pluginId);
			policyExportAudit.setClientIP(ipAddress);
			policyExportAudit.setRequestedEpoch(lastKnownVersion);
			policyExportAudit.setHttpRetCode(httpRespCode);

			assetMgr.createPolicyAudit(policyExportAudit);
		} catch(Exception excp) {
			LOG.error("error while creating policy download audit", excp);
		}
	}

	private RangerPolicy getExactMatchPolicyForResource(RangerPolicyEngine policyEngine, RangerAccessResource resource) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceREST.getExactMatchPolicyForResource(" + resource + ")");
		}

		RangerPolicy ret = policyEngine != null ? policyEngine.getExactMatchPolicy(resource) : null;

		if(ret != null) {
			// at this point, ret is a policy in policy-engine; the caller might update the policy (for grant/revoke); so get a copy from the store
			ret = svcStore.getPolicy(ret.getId());
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceREST.getExactMatchPolicyForResource(" + resource + "): " + ret);
		}

		return ret;
	}

	private boolean compactPolicy(RangerPolicy policy) {
		boolean ret = false;

		List<?>[] policyItemsList = new List<?>[] { policy.getPolicyItems(),
													policy.getDenyPolicyItems(),
													policy.getAllowExceptions(),
													policy.getDenyExceptions()
												  };

		for(List<?> policyItemsObj : policyItemsList) {
			@SuppressWarnings("unchecked")
			List<RangerPolicyItem> policyItems = (List<RangerPolicyItem>)policyItemsObj;

			int numOfItems = policyItems.size();
		
			for(int i = 0; i < numOfItems; i++) {
				RangerPolicyItem policyItem = policyItems.get(i);
			
				// remove the policy item if 1) there are no users and groups OR 2) if there are no accessTypes and not a delegate-admin
				if((CollectionUtils.isEmpty(policyItem.getUsers()) && CollectionUtils.isEmpty(policyItem.getGroups())) ||
				   (CollectionUtils.isEmpty(policyItem.getAccesses()) && !policyItem.getDelegateAdmin())) {
					policyItems.remove(i);
					numOfItems--;
					i--;

					ret = true;
				}
			}
		}

		return ret;
	}

	private RangerPolicyItem getPolicyItemForUser(RangerPolicy policy, String userName) {
		RangerPolicyItem ret = null;

		for(RangerPolicyItem policyItem : policy.getPolicyItems()) {
			if(policyItem.getUsers().size() != 1) {
				continue;
			}

			if(policyItem.getUsers().contains(userName)) {
				ret = policyItem;
				break;
			}
		}

		return ret;
	}

	private RangerPolicyItem getPolicyItemForGroup(RangerPolicy policy, String groupName) {
		RangerPolicyItem ret = null;

		for(RangerPolicyItem policyItem : policy.getPolicyItems()) {
			if(policyItem.getGroups().size() != 1) {
				continue;
			}

			if(policyItem.getGroups().contains(groupName)) {
				ret = policyItem;
				break;
			}
		}

		return ret;
	}

	private boolean addAccesses(RangerPolicyItem policyItem, Set<String> accessTypes) {
		boolean ret = false;

		for(String accessType : accessTypes) {
			RangerPolicyItemAccess policyItemAccess = null;

			for(RangerPolicyItemAccess itemAccess : policyItem.getAccesses()) {
				if(StringUtils.equals(itemAccess.getType(), accessType)) {
					policyItemAccess = itemAccess;
					break;
				}
			}

			if(policyItemAccess != null) {
				if(!policyItemAccess.getIsAllowed()) {
					policyItemAccess.setIsAllowed(Boolean.TRUE);
					ret = true;
				}
			} else {
				policyItem.getAccesses().add(new RangerPolicyItemAccess(accessType, Boolean.TRUE));
				ret = true;
			}
		}

		return ret;
	}

	private boolean removeAccesses(RangerPolicyItem policyItem, Set<String> accessTypes) {
		boolean ret = false;

		for(String accessType : accessTypes) {
			int numOfItems = policyItem.getAccesses().size();

			for(int i = 0; i < numOfItems; i++) {
				RangerPolicyItemAccess itemAccess = policyItem.getAccesses().get(i);
				
				if(StringUtils.equals(itemAccess.getType(), accessType)) {
					policyItem.getAccesses().remove(i);
					numOfItems--;
					i--;

					ret = true;
				}
			}
		}

		return ret;
	}

	private boolean removeUsersAndGroupsFromPolicy(RangerPolicy policy, Set<String> users, Set<String> groups) {
		boolean policyUpdated = false;

		List<RangerPolicyItem> policyItems = policy.getPolicyItems();

		int numOfItems = policyItems.size();

		for(int i = 0; i < numOfItems; i++) {
			RangerPolicyItem policyItem = policyItems.get(i);

			if(CollectionUtils.containsAny(policyItem.getUsers(), users)) {
				policyItem.getUsers().removeAll(users);

				policyUpdated = true;
			}

			if(CollectionUtils.containsAny(policyItem.getGroups(), groups)) {
				policyItem.getGroups().removeAll(groups);

				policyUpdated = true;
			}

			if(CollectionUtils.isEmpty(policyItem.getUsers()) && CollectionUtils.isEmpty(policyItem.getGroups())) {
				policyItems.remove(i);
				numOfItems--;
				i--;

				policyUpdated = true;
			}
		}

		return policyUpdated;
	}

	@GET
	@Path("/policies/eventTime")
	@Produces({ "application/json", "application/xml" })
	@PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.GET_POLICY_FROM_EVENT_TIME + "\")")
	public RangerPolicy getPolicyFromEventTime(@Context HttpServletRequest request) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceREST.getPolicyFromEventTime()");
		}

		String eventTimeStr = request.getParameter("eventTime");
		String policyIdStr = request.getParameter("policyId");

		if (StringUtils.isEmpty(eventTimeStr) || StringUtils.isEmpty(policyIdStr)) {
			throw restErrorUtil.createRESTException("EventTime or policyId cannot be null or empty string.",
					MessageEnums.INVALID_INPUT_DATA);
		}

		Long policyId = Long.parseLong(policyIdStr);

		RangerPolicy policy=null;
		try {
			policy = svcStore.getPolicyFromEventTime(eventTimeStr, policyId);
			if(policy != null) {
				ensureAdminAccess(policy.getService(), policy.getResources());
			}
		} catch(WebApplicationException excp) {
			throw excp;
		} catch(Throwable excp) {
			LOG.error("getPolicy(" + policyId + ") failed", excp);

			throw restErrorUtil.createRESTException(excp.getMessage());
		}

		if(policy == null) {
			throw restErrorUtil.createRESTException(HttpServletResponse.SC_NOT_FOUND, "Not found", true);
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceREST.getPolicy(" + policyId + "): " + policy);
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceREST.getPolicyFromEventTime()");
		}

		return policy;
	}

	@GET
	@Path("/policy/{policyId}/versionList")
	@PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.GET_POLICY_VERSION_LIST + "\")")
	public VXString getPolicyVersionList(@PathParam("policyId") Long policyId) {

		VXString policyVersionListStr = svcStore.getPolicyVersionList(policyId);

		return policyVersionListStr;
	}

	@GET
	@Path("/policy/{policyId}/version/{versionNo}")
	@Produces({ "application/json", "application/xml" })
	@PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.GET_POLICY_FOR_VERSION_NO + "\")")
	public RangerPolicy getPolicyForVersionNumber(@PathParam("policyId") Long policyId,
			@PathParam("versionNo") int versionNo) {
		return svcStore.getPolicyForVersionNumber(policyId, versionNo);
	}

	private List<RangerPolicy> applyAdminAccessFilter(List<RangerPolicy> policies) {
		List<RangerPolicy> ret = new ArrayList<RangerPolicy>();
		RangerPerfTracer  perf = null;

		if(RangerPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
			perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "ServiceREST.applyAdminAccessFilter(policyCount=" + (policies == null ? 0 : policies.size()) + ")");
		}

		if (CollectionUtils.isNotEmpty(policies)) {
			boolean     isAdmin    = bizUtil.isAdmin();
			boolean     isKeyAdmin = bizUtil.isKeyAdmin();
			String      userName   = bizUtil.getCurrentUserLoginId();
			Set<String> userGroups = null;

			Map<String, List<RangerPolicy>> servicePoliciesMap = new HashMap<String, List<RangerPolicy>>();

			for (int i = 0; i < policies.size(); i++) {
				RangerPolicy       policy      = policies.get(i);
				String             serviceName = policy.getService();
				List<RangerPolicy> policyList  = servicePoliciesMap.get(serviceName);

				if (policyList == null) {
					policyList = new ArrayList<RangerPolicy>();

					servicePoliciesMap.put(serviceName, policyList);
				}

				policyList.add(policy);
			}

			for (Map.Entry<String, List<RangerPolicy>> entry : servicePoliciesMap.entrySet()) {
				String             serviceName  = entry.getKey();
				List<RangerPolicy> listToFilter = entry.getValue();

				if (CollectionUtils.isNotEmpty(listToFilter)) {
					if (isAdmin || isKeyAdmin) {
						XXService xService     = daoManager.getXXService().findByName(serviceName);
						Long      serviceDefId = xService.getType();
						boolean   isKmsService = serviceDefId.equals(EmbeddedServiceDefsUtil.instance().getKmsServiceDefId());

						if (isAdmin) {
							if (!isKmsService) {
								ret.addAll(listToFilter);
							}
						} else { // isKeyAdmin
							if (isKmsService) {
								ret.addAll(listToFilter);
							}
						}

						continue;
					}

					RangerPolicyEngine policyEngine = getPolicyEngine(serviceName);

					if (policyEngine != null) {
						if(userGroups == null) {
							userGroups = daoManager.getXXGroupUser().findGroupNamesByUserName(userName);
						}

						for (RangerPolicy policy : listToFilter) {
							if (policyEngine.isAccessAllowed(policy.getResources(), userName, userGroups, RangerPolicyEngine.ADMIN_ACCESS)) {
								ret.add(policy);
							}
						}
					}

				}
			}
		}

		RangerPerfTracer.log(perf);

		return ret;
	}

	void ensureAdminAccess(String serviceName, Map<String, RangerPolicyResource> resources) {
		boolean isAdmin = bizUtil.isAdmin();
		boolean isKeyAdmin = bizUtil.isKeyAdmin();

		XXService xService = daoManager.getXXService().findByName(serviceName);
		XXServiceDef xServiceDef = daoManager.getXXServiceDef().getById(xService.getType());

		if(!isAdmin && !isKeyAdmin) {
			RangerPolicyEngine policyEngine = getPolicyEngine(serviceName);
			String             userName     = bizUtil.getCurrentUserLoginId();
			Set<String>        userGroups   = userMgr.getGroupsForUser(userName);

			boolean isAllowed = hasAdminAccess(policyEngine, userName, userGroups, resources);

			if(!isAllowed) {
				throw restErrorUtil.createRESTException(HttpServletResponse.SC_UNAUTHORIZED,
						"User '" + userName + "' does not have delegated-admin privilege on given resources", true);
			}
		} else if (isAdmin) {
			if (xServiceDef.getImplclassname().equals(EmbeddedServiceDefsUtil.KMS_IMPL_CLASS_NAME)) {
				throw restErrorUtil.createRESTException(
						"KMS Policies/Services/Service-Defs are not accessible for logged in user.",
						MessageEnums.OPER_NO_PERMISSION);
			}
		} else if (isKeyAdmin) {
			if (!xServiceDef.getImplclassname().equals(EmbeddedServiceDefsUtil.KMS_IMPL_CLASS_NAME)) {
				throw restErrorUtil.createRESTException(
						"Only KMS Policies/Services/Service-Defs are accessible for logged in user.",
						MessageEnums.OPER_NO_PERMISSION);
			}
		}
	}

	private boolean hasAdminAccess(RangerPolicyEngine policyEngine, String userName, Set<String> userGroups, Map<String, RangerPolicyResource> resources) {
		boolean isAllowed = false;

		if(policyEngine != null) {
			isAllowed = policyEngine.isAccessAllowed(resources, userName, userGroups, RangerPolicyEngine.ADMIN_ACCESS);
		}

		return isAllowed;
	}

	private boolean hasAdminAccess(RangerPolicyEngine policyEngine, String userName, Set<String> userGroups, RangerAccessResource resource) {
		boolean isAllowed = false;

		if(policyEngine != null) {
			isAllowed = policyEngine.isAccessAllowed(resource, userName, userGroups, RangerPolicyEngine.ADMIN_ACCESS);
		}

		return isAllowed;
	}

	private RangerPolicyEngine getPolicyEngine(String serviceName) {
		if(RangerPolicyEngineCache.getInstance().getPolicyEngineOptions() == null) {
			RangerPolicyEngineOptions options = new RangerPolicyEngineOptions();

			String propertyPrefix = "ranger.admin";

			options.evaluatorType           = RangerPolicyEvaluator.EVALUATOR_TYPE_OPTIMIZED;
			options.cacheAuditResults       = RangerConfiguration.getInstance().getBoolean(propertyPrefix + ".policyengine.option.cache.audit.results", false);
			options.disableContextEnrichers = RangerConfiguration.getInstance().getBoolean(propertyPrefix + ".policyengine.option.disable.context.enrichers", true);
			options.disableCustomConditions = RangerConfiguration.getInstance().getBoolean(propertyPrefix + ".policyengine.option.disable.custom.conditions", true);
			options.evaluateDelegateAdminOnly = RangerConfiguration.getInstance().getBoolean(propertyPrefix + ".policyengine.option.evaluate.delegateadmin.only", true);

			RangerPolicyEngineCache.getInstance().setPolicyEngineOptions(options);;
		}

		RangerPolicyEngine ret = RangerPolicyEngineCache.getInstance().getPolicyEngine(serviceName, svcStore);

		return ret;
	}

	@GET
	@Path("/checksso")
	@Produces(MediaType.TEXT_PLAIN)
	public String checkSSO() {
		return String.valueOf(bizUtil.isSSOEnabled());
	}

	boolean isAdminUserWithNoFilterParams(SearchFilter filter) {
		return (filter == null || MapUtils.isEmpty(filter.getParams())) &&
			   (bizUtil.isAdmin() || bizUtil.isKeyAdmin());
	}

	private RangerPolicyList toRangerPolicyList(PList<RangerPolicy> policyList) {
		RangerPolicyList ret = new RangerPolicyList();

		if(policyList != null) {
			ret.setPolicies(policyList.getList());
			ret.setPageSize(policyList.getPageSize());
			ret.setResultSize(policyList.getResultSize());
			ret.setStartIndex(policyList.getStartIndex());
			ret.setTotalCount(policyList.getTotalCount());
			ret.setSortBy(policyList.getSortBy());
			ret.setSortType(policyList.getSortType());
		}

		return ret;
	}

	private RangerPolicyList toRangerPolicyList(List<RangerPolicy> policyList, SearchFilter filter) {
		RangerPolicyList ret = new RangerPolicyList();

		if(CollectionUtils.isNotEmpty(policyList)) {
			int    totalCount = policyList.size();
			int    startIndex = filter == null ? 0 : filter.getStartIndex();
			int    pageSize   = filter == null ? totalCount : filter.getMaxRows();
			int    toIndex    = Math.min(startIndex + pageSize, totalCount);
			String sortType   = filter == null ? null : filter.getSortType();
			String sortBy     = filter == null ? null : filter.getSortBy();

			List<RangerPolicy> retList = new ArrayList<RangerPolicy>();
			for(int i = startIndex; i < toIndex; i++) {
				retList.add(policyList.get(i));
			}

			ret.setPolicies(retList);
			ret.setPageSize(pageSize);
			ret.setResultSize(retList.size());
			ret.setStartIndex(startIndex);
			ret.setTotalCount(totalCount);
			ret.setSortBy(sortBy);
			ret.setSortType(sortType);
		}

		return ret;
	}

}
