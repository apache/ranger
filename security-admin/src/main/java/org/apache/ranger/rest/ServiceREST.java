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

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import javax.annotation.PostConstruct;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
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
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ranger.admin.client.datatype.RESTResponse;
import org.apache.ranger.authorization.hadoop.config.RangerConfiguration;
import org.apache.ranger.authorization.utils.StringUtil;
import org.apache.ranger.biz.AssetMgr;
import org.apache.ranger.biz.RangerBizUtil;
import org.apache.ranger.biz.ServiceDBStore;
import org.apache.ranger.biz.ServiceMgr;
import org.apache.ranger.biz.TagDBStore;
import org.apache.ranger.biz.XUserMgr;
import org.apache.ranger.common.AppConstants;
import org.apache.ranger.common.ContextUtil;
import org.apache.ranger.common.GUIDUtil;
import org.apache.ranger.common.JSONUtil;
import org.apache.ranger.common.MessageEnums;
import org.apache.ranger.common.PropertiesUtil;
import org.apache.ranger.common.RESTErrorUtil;
import org.apache.ranger.common.RangerConstants;
import org.apache.ranger.common.RangerSearchUtil;
import org.apache.ranger.common.RangerValidatorFactory;
import org.apache.ranger.common.ServiceUtil;
import org.apache.ranger.common.UserSessionBase;
import org.apache.ranger.db.RangerDaoManager;
import org.apache.ranger.entity.XXPolicyExportAudit;
import org.apache.ranger.entity.XXService;
import org.apache.ranger.entity.XXServiceDef;
import org.apache.ranger.entity.XXTrxLog;
import org.apache.ranger.plugin.model.RangerPluginInfo;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItem;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItemAccess;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyResource;
import org.apache.ranger.plugin.model.RangerService;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.model.validation.RangerPolicyValidator;
import org.apache.ranger.plugin.model.validation.RangerServiceDefHelper;
import org.apache.ranger.plugin.model.validation.RangerServiceDefValidator;
import org.apache.ranger.plugin.model.validation.RangerServiceValidator;
import org.apache.ranger.plugin.model.validation.RangerValidator.Action;
import org.apache.ranger.plugin.policyengine.RangerAccessResource;
import org.apache.ranger.plugin.policyengine.RangerAccessResourceImpl;
import org.apache.ranger.plugin.policyengine.RangerPolicyEngine;
import org.apache.ranger.plugin.policyengine.RangerPolicyEngineCacheForEngineOptions;
import org.apache.ranger.plugin.policyengine.RangerPolicyEngineImpl;
import org.apache.ranger.plugin.policyengine.RangerPolicyEngineOptions;
import org.apache.ranger.plugin.service.ResourceLookupContext;
import org.apache.ranger.plugin.store.EmbeddedServiceDefsUtil;
import org.apache.ranger.plugin.store.PList;
import org.apache.ranger.plugin.store.ServiceStore;
import org.apache.ranger.plugin.util.GrantRevokeRequest;
import org.apache.ranger.plugin.util.RangerAccessRequestUtil;
import org.apache.ranger.plugin.util.RangerPerfTracer;
import org.apache.ranger.plugin.util.SearchFilter;
import org.apache.ranger.plugin.util.ServicePolicies;
import org.apache.ranger.security.context.RangerAPIList;
import org.apache.ranger.security.web.filter.RangerCSRFPreventionFilter;
import org.apache.ranger.service.RangerPluginInfoService;
import org.apache.ranger.service.RangerPolicyLabelsService;
import org.apache.ranger.service.RangerPolicyService;
import org.apache.ranger.service.RangerServiceDefService;
import org.apache.ranger.service.RangerServiceService;
import org.apache.ranger.service.XUserService;
import org.apache.ranger.view.RangerExportPolicyList;
import org.apache.ranger.view.RangerPluginInfoList;
import org.apache.ranger.view.RangerPolicyList;
import org.apache.ranger.view.RangerServiceDefList;
import org.apache.ranger.view.RangerServiceList;
import org.apache.ranger.view.VXResponse;
import org.apache.ranger.view.VXString;
import org.apache.ranger.view.VXUser;
import org.codehaus.jackson.map.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import com.sun.jersey.core.header.FormDataContentDisposition;
import com.sun.jersey.multipart.FormDataParam;

@Path("plugins")
@Component
@Scope("request")
@Transactional(propagation = Propagation.REQUIRES_NEW)
public class ServiceREST {
	private static final Log LOG = LogFactory.getLog(ServiceREST.class);
	private static final Log PERF_LOG = RangerPerfTracer.getPerfLogger("rest.ServiceREST");

	final static public String PARAM_SERVICE_NAME     = "serviceName";
	final static public String PARAM_SERVICE_TYPE     = "serviceType";
	final static public String PARAM_POLICY_NAME      = "policyName";
	final static public String PARAM_UPDATE_IF_EXISTS = "updateIfExists";
	public static final String Allowed_User_List_For_Download = "policy.download.auth.users";
	public static final String Allowed_User_List_For_Grant_Revoke = "policy.grantrevoke.auth.users";

	public static final String isCSRF_ENABLED = "ranger.rest-csrf.enabled";
	public static final String BROWSER_USER_AGENT_PARAM = "ranger.rest-csrf.browser-useragents-regex";
	public static final String CUSTOM_METHODS_TO_IGNORE_PARAM = "ranger.rest-csrf.methods-to-ignore";
	public static final String CUSTOM_HEADER_PARAM = "ranger.rest-csrf.custom-header";
	
	@Autowired
	RESTErrorUtil restErrorUtil;

	@Autowired
	ServiceMgr serviceMgr;

        @Autowired
        XUserService xUserService;
	@Autowired
	AssetMgr assetMgr;

	@Autowired
	XUserMgr userMgr;

	@Autowired
	ServiceDBStore svcStore;
	
	@Autowired
	ServiceUtil serviceUtil;

	@Autowired
	RangerPolicyService policyService;
	
	@Autowired
        RangerPolicyLabelsService policyLabelsService;

        @Autowired
	RangerServiceService svcService;
	
	@Autowired
	RangerServiceDefService serviceDefService;

	@Autowired
    RangerPluginInfoService pluginInfoService;

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

	@Autowired
	TagDBStore tagStore;
	
	@Autowired
    JSONUtil jsonUtil;

	private RangerPolicyEngineOptions delegateAdminOptions;
	private RangerPolicyEngineOptions policySearchAdminOptions;
	private RangerPolicyEngineOptions defaultAdminOptions;

	public ServiceREST() {
	}

	@PostConstruct
	public void initStore() {
		tagStore.setServiceStore(svcStore);
		delegateAdminOptions = getDelegatedAdminPolicyEngineOptions();
		policySearchAdminOptions = getPolicySearchRangerAdminPolicyEngineOptions();
		defaultAdminOptions = getDefaultRangerAdminPolicyEngineOptions();
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
                        bizUtil.blockAuditorRoleUser();
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
                        bizUtil.blockAuditorRoleUser();
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
			if(!StringUtils.isEmpty(forceDeleteStr) && "true".equalsIgnoreCase(forceDeleteStr)) {
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
			if(EmbeddedServiceDefsUtil.EMBEDDED_SERVICEDEF_TAG_NAME.equals(xServiceDef.getName())) {
				if (!bizUtil.hasModuleAccess(RangerConstants.MODULE_TAG_BASED_POLICIES)) {
					throw restErrorUtil.createRESTException(HttpServletResponse.SC_FORBIDDEN, "User is not having permissions on the tag module.", true);
				}
			}
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
				if(EmbeddedServiceDefsUtil.EMBEDDED_SERVICEDEF_TAG_NAME.equals(xServiceDef.getName())) {
					if (!bizUtil.hasModuleAccess(RangerConstants.MODULE_TAG_BASED_POLICIES)) {
						throw restErrorUtil.createRESTException(HttpServletResponse.SC_FORBIDDEN, "User is not having permissions on the tag module", true);
					}
				}
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

		if (!bizUtil.hasModuleAccess(RangerConstants.MODULE_RESOURCE_BASED_POLICIES)) {
			throw restErrorUtil.createRESTException(HttpServletResponse.SC_FORBIDDEN, "User is not having permissions on the "+RangerConstants.MODULE_RESOURCE_BASED_POLICIES+" module.", true);
		}

		RangerServiceDefList ret  = null;
		RangerPerfTracer     perf = null;

		PList<RangerServiceDef> paginatedSvcDefs = null;

		SearchFilter filter = searchUtil.getSearchFilter(request, serviceDefService.sortFields);
		String pageSource= null;
		pageSource=request.getParameter("pageSource");
		if(pageSource!=null)
			filter.setParam("pageSource",pageSource);

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

	@GET
	@Path("/policies/{serviceDefName}/for-resource")
	@Produces({ "application/json", "application/xml" })
	public List<RangerPolicy> getPoliciesForResource(@PathParam("serviceDefName") String serviceDefName,
												  @DefaultValue("") @QueryParam("serviceName") String serviceName,
												  @Context HttpServletRequest request) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceREST.getPoliciesForResource(service-type=" + serviceDefName + ", service-name=" + serviceName + ")");
		}

		List<RangerPolicy> ret = new ArrayList<>();

		List<RangerService> services = new ArrayList<>();
		Map<String, Object> resource = new HashMap<>();

		String validationMessage = validateResourcePoliciesRequest(serviceDefName, serviceName, request, services, resource);

		if (StringUtils.isNotEmpty(validationMessage)) {
			LOG.error("Invalid request: [" + validationMessage + "]");
			throw restErrorUtil.createRESTException(validationMessage,
					MessageEnums.INVALID_INPUT_DATA);
		} else {
			RangerService service = services.get(0);
			if (LOG.isDebugEnabled()) {
				LOG.debug("getServicePolicies with service-name=" + service.getName());
			}

			RangerPolicyEngine engine = null;

			try {
				engine = getPolicySearchPolicyEngine(service.getName());
			} catch (Exception e) {
				LOG.error("Cannot initialize Policy-Engine", e);
				throw restErrorUtil.createRESTException("Cannot initialize Policy Engine",
						MessageEnums.ERROR_SYSTEM);
			}

			if (engine != null) {
				ret = engine.getMatchingPolicies(new RangerAccessResourceImpl(resource));
			}

		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceREST.getPoliciesForResource(service-type=" + serviceDefName + ", service-name=" + serviceName + ") : " + ret.toString());
		}
		return ret;
	}

	private String validateResourcePoliciesRequest(String serviceDefName, String serviceName, HttpServletRequest request, List<RangerService> services, Map<String, Object> resource) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceREST.validatePoliciesForResourceRequest(service-type=" + serviceDefName + ", service-name=" + serviceName + ")");
		}
		final String ret;

		if (MapUtils.isNotEmpty(request.getParameterMap())) {
			for (Map.Entry<String, String[]> e : request.getParameterMap().entrySet()) {
				String name = e.getKey();
				String[] values = e.getValue();

				if (!StringUtils.isEmpty(name) && !ArrayUtils.isEmpty(values)
						&& name.startsWith(SearchFilter.RESOURCE_PREFIX)) {
					resource.put(name.substring(SearchFilter.RESOURCE_PREFIX.length()), values[0]);
				}
			}
		}
		if (MapUtils.isEmpty(resource)) {
			ret = "No resource specified";
		} else {
			RangerServiceDef serviceDef = null;
			try {
				serviceDef = svcStore.getServiceDefByName(serviceDefName);
			} catch (Exception e) {
				LOG.error("Invalid service-type:[" + serviceDefName + "]", e);
			}
			if (serviceDef == null) {
				ret = "Invalid service-type:[" + serviceDefName + "]";
			} else {
				Set<String> resourceDefNames = resource.keySet();
				RangerServiceDefHelper serviceDefHelper = new RangerServiceDefHelper(serviceDef);
				Set<List<RangerServiceDef.RangerResourceDef>> resourceHierarchies = serviceDefHelper.getResourceHierarchies(RangerPolicy.POLICY_TYPE_ACCESS, resourceDefNames);
				if (CollectionUtils.isEmpty(resourceHierarchies)) {
					ret = "Invalid resource specified: resource-names:" + resourceDefNames +" are not part of any valid resource hierarchy for service-type:[" + serviceDefName + "]";
				} else {
					if (StringUtils.isNotBlank(serviceName)) {
						RangerService service = null;
						try {
							service = svcStore.getServiceByName(serviceName);
						} catch (Exception e) {
							LOG.error("Invalid service-name:[" + serviceName + "]");
						}
						if (service == null || !StringUtils.equals(service.getType(), serviceDefName)) {
							ret = "Invalid service-name:[" + serviceName + "] or service-name is not of service-type:[" + serviceDefName + "]";
						} else {
							services.add(service);
							ret = StringUtils.EMPTY;
						}
					} else {
						SearchFilter filter = new SearchFilter();
						filter.setParam(SearchFilter.SERVICE_TYPE, serviceDefName);
						List<RangerService> serviceList = null;
						try {
							serviceList = svcStore.getServices(filter);
						} catch (Exception e) {
							LOG.error("Cannot find service of service-type:[" + serviceDefName + "]");
						}
						if (CollectionUtils.isEmpty(serviceList) || serviceList.size() != 1) {
							ret = "Either 0 or more than 1 services found for service-type :[" + serviceDefName + "]";
						} else {
							services.add(serviceList.get(0));
							ret = StringUtils.EMPTY;
						}
					}
				}
			}
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceREST.validatePoliciesForResourceRequest(service-type=" + serviceDefName + ", service-name=" + serviceName + ") : " + ret);
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
			
			if(!StringUtils.isEmpty(service.getName().trim())){
				service.setName(service.getName().trim());
			}

			UserSessionBase session = ContextUtil.getCurrentUserSession();
			XXServiceDef xxServiceDef = daoManager.getXXServiceDef().findByName(service.getType());
			if(session != null && !session.isSpnegoEnabled()){
				bizUtil.hasAdminPermissions("Services");

				// TODO: As of now we are allowing SYS_ADMIN to create all the
				// services including KMS
				bizUtil.hasKMSPermissions("Service", xxServiceDef.getImplclassname());
			}
			if(session != null && session.isSpnegoEnabled()){
				if (session.isKeyAdmin() && !EmbeddedServiceDefsUtil.KMS_IMPL_CLASS_NAME.equals(xxServiceDef.getImplclassname())) {
					throw restErrorUtil.createRESTException("KeyAdmin can create/update/delete only KMS ",
							MessageEnums.OPER_NO_PERMISSION);
				}
				if ((!session.isKeyAdmin() && !session.isUserAdmin()) && EmbeddedServiceDefsUtil.KMS_IMPL_CLASS_NAME.equals(xxServiceDef.getImplclassname())) {
					throw restErrorUtil.createRESTException("User cannot create/update/delete KMS Service",
							MessageEnums.OPER_NO_PERMISSION);
				}
			}
                         bizUtil.blockAuditorRoleUser();
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
	public RangerService updateService(RangerService service,
                                       @Context HttpServletRequest request) {
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
			
			if(!StringUtils.isEmpty(service.getName().trim())){
				service.setName(service.getName().trim());
			}

			bizUtil.hasAdminPermissions("Services");

			// TODO: As of now we are allowing SYS_ADMIN to create all the
			// services including KMS

			XXServiceDef xxServiceDef = daoManager.getXXServiceDef().findByName(service.getType());
			bizUtil.hasKMSPermissions("Service", xxServiceDef.getImplclassname());
                        bizUtil.blockAuditorRoleUser();
			Map<String, Object> options = getOptions(request);

            ret = svcStore.updateService(service, options);
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
			EmbeddedServiceDefsUtil embeddedServiceDefsUtil = EmbeddedServiceDefsUtil.instance();
			if (service.getType().equals(embeddedServiceDefsUtil.getTagServiceDefId())){
				List<XXService> referringServices=daoManager.getXXService().findByTagServiceId(id);
				if(!CollectionUtils.isEmpty(referringServices)){
					Set<String> referringServiceNames=new HashSet<String>();
					for(XXService xXService:referringServices){
						referringServiceNames.add(xXService.getName());
						if(referringServiceNames.size()>=10){
							break;
						}
					}
					if(referringServices.size()<=10){
						throw restErrorUtil.createRESTException("Tag service '" + service.getName() + "' is being referenced by " + referringServices.size() + " services: "+referringServiceNames,MessageEnums.OPER_NOT_ALLOWED_FOR_STATE);
					}else{
						throw restErrorUtil.createRESTException("Tag service '" + service.getName() + "' is being referenced by " + referringServices.size() + " services: "+referringServiceNames+" and more..",MessageEnums.OPER_NOT_ALLOWED_FOR_STATE);
					}
				}
			}
			XXServiceDef xxServiceDef = daoManager.getXXServiceDef().getById(service.getType());
			bizUtil.hasKMSPermissions("Service", xxServiceDef.getImplclassname());
                        bizUtil.blockAuditorRoleUser();
			tagStore.deleteAllTagObjectsForService(service.getName());

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
			
			if (ret != null) {
				UserSessionBase userSession = ContextUtil
						.getCurrentUserSession();
				if (userSession != null && userSession.getLoginId() != null) {
					VXUser loggedInVXUser = xUserService
							.getXUserByUserName(userSession.getLoginId());
					if (loggedInVXUser != null) {
						if (loggedInVXUser.getUserRoleList().size() == 1
								&& loggedInVXUser.getUserRoleList().contains(
										RangerConstants.ROLE_USER)) {

							ret = hideCriticalServiceDetailsForRoleUser(ret);
						}
					}
				}
			}
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
			
			if (ret != null) {
				UserSessionBase userSession = ContextUtil
						.getCurrentUserSession();
				if (userSession != null && userSession.getLoginId() != null) {
					VXUser loggedInVXUser = xUserService
							.getXUserByUserName(userSession.getLoginId());
					if (loggedInVXUser != null) {
						if (loggedInVXUser.getUserRoleList().size() == 1
								&& loggedInVXUser.getUserRoleList().contains(
										RangerConstants.ROLE_USER)) {

							ret = hideCriticalServiceDetailsForRoleUser(ret);
						}
					}
				}
			}
			
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
			
			if(paginatedSvcs!= null && !paginatedSvcs.getList().isEmpty()){
				UserSessionBase userSession = ContextUtil
						.getCurrentUserSession();
				if (userSession != null && userSession.getLoginId() != null) {
					VXUser loggedInVXUser = xUserService
							.getXUserByUserName(userSession.getLoginId());
					if (loggedInVXUser != null) {
						if (loggedInVXUser.getUserRoleList().size() == 1
								&& loggedInVXUser.getUserRoleList().contains(
										RangerConstants.ROLE_USER)) {
							
							List<RangerService> updateServiceList = new ArrayList<RangerService>();
							for(RangerService rangerService : paginatedSvcs.getList()){
								
								if(rangerService != null){
									updateServiceList.add(hideCriticalServiceDetailsForRoleUser(rangerService));
								}
							}
							
							if(updateServiceList != null && !updateServiceList.isEmpty()){
								paginatedSvcs.setList(updateServiceList);
							}
						}
					}
				}
			}

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
			
			ret = Long.valueOf(services == null ? 0 : services.size());
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
			if (RangerPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
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
			if (RangerPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
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

		if(grantRequest!=null){
			if (serviceUtil.isValidateHttpsAuthentication(serviceName, request)) {

				try {
					if(RangerPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
						perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "ServiceREST.grantAccess(serviceName=" + serviceName + ")");
					}

					validateGrantRevokeRequest(grantRequest);
					String               userName   = grantRequest.getGrantor();
					Set<String>          userGroups = CollectionUtils.isNotEmpty(grantRequest.getGrantorGroups()) ? grantRequest.getGrantorGroups() : userMgr.getGroupsForUser(userName);
					RangerAccessResource resource   = new RangerAccessResourceImpl(StringUtil.toStringObjectMap(grantRequest.getResource()));
                                        VXUser vxUser = xUserService.getXUserByUserName(userName);
                                        if(vxUser.getUserRoleList().contains(RangerConstants.ROLE_ADMIN_AUDITOR) || vxUser.getUserRoleList().contains(RangerConstants.ROLE_KEY_ADMIN_AUDITOR)){
                                                 VXResponse vXResponse = new VXResponse();
                         vXResponse.setStatusCode(HttpServletResponse.SC_UNAUTHORIZED);
                         vXResponse.setMsgDesc("Operation"
                                         + " denied. LoggedInUser="
                                         +  vxUser.getId()
                                         + " ,isn't permitted to perform the action.");
                         throw restErrorUtil.generateRESTException(vXResponse);
                                        }
					boolean isAdmin = hasAdminAccess(serviceName, userName, userGroups, resource);

					if(!isAdmin) {
						throw restErrorUtil.createGrantRevokeRESTException( "User doesn't have necessary permission to grant access");
					}

					RangerPolicy policy = getExactMatchPolicyForResource(serviceName, resource, userName);
	
					if(policy != null) {
						boolean policyUpdated = false;
						policyUpdated = ServiceRESTUtil.processGrantRequest(policy, grantRequest);
	
						if(policyUpdated) {
							svcStore.updatePolicy(policy);
						} else {
							LOG.error("processGrantRequest processing failed");
							throw new Exception("processGrantRequest processing failed");
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
								RangerPolicyResource policyResource = new RangerPolicyResource((String) resource.getValue(resourceName));
								policyResource.setIsRecursive(grantRequest.getIsRecursive());
		
								policyResources.put(resourceName, policyResource);
							}
						}
						policy.setResources(policyResources);
	
						RangerPolicyItem policyItem = new RangerPolicyItem();
	
						policyItem.setDelegateAdmin(grantRequest.getDelegateAdmin());
						policyItem.getUsers().addAll(grantRequest.getUsers());
						policyItem.getGroups().addAll(grantRequest.getGroups());
	
						for(String accessType : grantRequest.getAccessTypes()) {
							policyItem.getAccesses().add(new RangerPolicyItemAccess(accessType, Boolean.TRUE));
						}
	
						policy.getPolicyItems().add(policyItem);
	
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
		}
		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceREST.grantAccess(" + serviceName + ", " + grantRequest + "): " + ret);
		}

		return ret;
	}
	
	@POST
	@Path("/secure/services/grant/{serviceName}")
	@Produces({ "application/json", "application/xml" })
	public RESTResponse secureGrantAccess(@PathParam("serviceName") String serviceName, GrantRevokeRequest grantRequest, @Context HttpServletRequest request) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceREST.secureGrantAccess(" + serviceName + ", " + grantRequest + ")");
		}
		RESTResponse     ret  = new RESTResponse();
		RangerPerfTracer perf = null;
		boolean isAllowed = false;
		boolean isKeyAdmin = bizUtil.isKeyAdmin();
                bizUtil.blockAuditorRoleUser();
		if(grantRequest!=null){
			if (serviceUtil.isValidService(serviceName, request)) {
				try {
					if(RangerPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
						perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "ServiceREST.scureGrantAccess(serviceName=" + serviceName + ")");
					}

					validateGrantRevokeRequest(grantRequest);

					String               userName   = grantRequest.getGrantor();
					Set<String>          userGroups = CollectionUtils.isNotEmpty(grantRequest.getGrantorGroups()) ? grantRequest.getGrantorGroups() : userMgr.getGroupsForUser(userName);
					RangerAccessResource resource   = new RangerAccessResourceImpl(StringUtil.toStringObjectMap(grantRequest.getResource()));
					boolean isAdmin = hasAdminAccess(serviceName, userName, userGroups, resource);

					XXService xService = daoManager.getXXService().findByName(serviceName);
					XXServiceDef xServiceDef = daoManager.getXXServiceDef().getById(xService.getType());
					RangerService rangerService = svcStore.getServiceByName(serviceName);

					if (StringUtils.equals(xServiceDef.getImplclassname(), EmbeddedServiceDefsUtil.KMS_IMPL_CLASS_NAME)) {
						if (isKeyAdmin) {
							isAllowed = true;
						}else {
							isAllowed = bizUtil.isUserAllowedForGrantRevoke(rangerService, Allowed_User_List_For_Grant_Revoke, userName);
						}
					}else{
						if (isAdmin) {
							isAllowed = true;
						}
						else{
							isAllowed = bizUtil.isUserAllowedForGrantRevoke(rangerService, Allowed_User_List_For_Grant_Revoke, userName);
						}
					}
					if (isAllowed) {
						RangerPolicy policy = getExactMatchPolicyForResource(serviceName, resource, userName);

						if(policy != null) {
							boolean policyUpdated = false;
							policyUpdated = ServiceRESTUtil.processGrantRequest(policy, grantRequest);

							if(policyUpdated) {
								svcStore.updatePolicy(policy);
							} else {
								LOG.error("processSecureGrantRequest processing failed");
								throw new Exception("processSecureGrantRequest processing failed");
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
									RangerPolicyResource policyResource = new RangerPolicyResource((String) resource.getValue(resourceName));
									policyResource.setIsRecursive(grantRequest.getIsRecursive());

									policyResources.put(resourceName, policyResource);
								}
							}
							policy.setResources(policyResources);

							RangerPolicyItem policyItem = new RangerPolicyItem();

							policyItem.setDelegateAdmin(grantRequest.getDelegateAdmin());
							policyItem.getUsers().addAll(grantRequest.getUsers());
							policyItem.getGroups().addAll(grantRequest.getGroups());

							for(String accessType : grantRequest.getAccessTypes()) {
								policyItem.getAccesses().add(new RangerPolicyItemAccess(accessType, Boolean.TRUE));
							}

							policy.getPolicyItems().add(policyItem);

							svcStore.createPolicy(policy);
						}
					}else{
						LOG.error("secureGrantAccess(" + serviceName + ", " + grantRequest + ") failed as User doesn't have permission to grant Policy");
						throw restErrorUtil.createGrantRevokeRESTException( "User doesn't have necessary permission to grant access");
					}
				} catch(WebApplicationException excp) {
					throw excp;
				} catch(Throwable excp) {
					LOG.error("secureGrantAccess(" + serviceName + ", " + grantRequest + ") failed", excp);

					throw restErrorUtil.createRESTException(excp.getMessage());
				} finally {
					RangerPerfTracer.log(perf);
				}

				ret.setStatusCode(RESTResponse.STATUS_SUCCESS);
			}
		}
		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceREST.secureGrantAccess(" + serviceName + ", " + grantRequest + "): " + ret);
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
		if(revokeRequest!=null){
			if (serviceUtil.isValidateHttpsAuthentication(serviceName,request)) {

				try {
					if(RangerPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
						perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "ServiceREST.revokeAccess(serviceName=" + serviceName + ")");
					}

					validateGrantRevokeRequest(revokeRequest);

					String               userName   = revokeRequest.getGrantor();
					Set<String>          userGroups = CollectionUtils.isNotEmpty(revokeRequest.getGrantorGroups()) ? revokeRequest.getGrantorGroups() : userMgr.getGroupsForUser(userName);
					RangerAccessResource resource   = new RangerAccessResourceImpl(StringUtil.toStringObjectMap(revokeRequest.getResource()));
                                        VXUser vxUser = xUserService.getXUserByUserName(userName);
                                        if(vxUser.getUserRoleList().contains(RangerConstants.ROLE_ADMIN_AUDITOR) || vxUser.getUserRoleList().contains(RangerConstants.ROLE_KEY_ADMIN_AUDITOR)){
                                                 VXResponse vXResponse = new VXResponse();
                         vXResponse.setStatusCode(HttpServletResponse.SC_UNAUTHORIZED);
                         vXResponse.setMsgDesc("Operation"
                                         + " denied. LoggedInUser="
                                         +  vxUser.getId()
                                         + " ,isn't permitted to perform the action.");
                         throw restErrorUtil.generateRESTException(vXResponse);
                                        }
					boolean isAdmin = hasAdminAccess(serviceName, userName, userGroups, resource);

					if(!isAdmin) {
						throw restErrorUtil.createGrantRevokeRESTException("User doesn't have necessary permission to revoke access");
					}

					RangerPolicy policy = getExactMatchPolicyForResource(serviceName, resource, userName);

					if(policy != null) {
						boolean policyUpdated = false;
						policyUpdated = ServiceRESTUtil.processRevokeRequest(policy, revokeRequest);

						if(policyUpdated) {
							svcStore.updatePolicy(policy);
						} else {
							LOG.error("processRevokeRequest processing failed");
							throw new Exception("processRevokeRequest processing failed");
						}
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
		}
		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceREST.revokeAccess(" + serviceName + ", " + revokeRequest + "): " + ret);
		}

		return ret;
	}

	@POST
	@Path("/secure/services/revoke/{serviceName}")
	@Produces({ "application/json", "application/xml" })
	public RESTResponse secureRevokeAccess(@PathParam("serviceName") String serviceName, GrantRevokeRequest revokeRequest, @Context HttpServletRequest request) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceREST.secureRevokeAccess(" + serviceName + ", " + revokeRequest + ")");
		}
		RESTResponse     ret  = new RESTResponse();
		RangerPerfTracer perf = null;
		if(revokeRequest!=null){
			if (serviceUtil.isValidService(serviceName,request)) {
				try {
					if(RangerPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
						perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "ServiceREST.secureRevokeAccess(serviceName=" + serviceName + ")");
					}

					validateGrantRevokeRequest(revokeRequest);

					String               userName   = revokeRequest.getGrantor();
					Set<String>          userGroups = CollectionUtils.isNotEmpty(revokeRequest.getGrantorGroups()) ? revokeRequest.getGrantorGroups() : userMgr.getGroupsForUser(userName);
					RangerAccessResource resource   = new RangerAccessResourceImpl(StringUtil.toStringObjectMap(revokeRequest.getResource()));
					boolean isAdmin = hasAdminAccess(serviceName, userName, userGroups, resource);
					boolean isAllowed = false;
					boolean isKeyAdmin = bizUtil.isKeyAdmin();
                                        bizUtil.blockAuditorRoleUser();
					XXService xService = daoManager.getXXService().findByName(serviceName);
					XXServiceDef xServiceDef = daoManager.getXXServiceDef().getById(xService.getType());
					RangerService rangerService = svcStore.getServiceByName(serviceName);

					if (StringUtils.equals(xServiceDef.getImplclassname(), EmbeddedServiceDefsUtil.KMS_IMPL_CLASS_NAME)) {
						if (isKeyAdmin) {
							isAllowed = true;
						}else {
							isAllowed = bizUtil.isUserAllowedForGrantRevoke(rangerService, Allowed_User_List_For_Grant_Revoke, userName);
						}
					}else{
						if (isAdmin) {
							isAllowed = true;
						}
						else{
							isAllowed = bizUtil.isUserAllowedForGrantRevoke(rangerService, Allowed_User_List_For_Grant_Revoke, userName);
						}
					}

					if (isAllowed) {
						RangerPolicy policy = getExactMatchPolicyForResource(serviceName, resource, userName);

						if(policy != null) {
							boolean policyUpdated = false;
							policyUpdated = ServiceRESTUtil.processRevokeRequest(policy, revokeRequest);

							if(policyUpdated) {
								svcStore.updatePolicy(policy);
							} else {
								LOG.error("processSecureRevokeRequest processing failed");
								throw new Exception("processSecureRevokeRequest processing failed");
							}
						}
					}else{
						LOG.error("secureRevokeAccess(" + serviceName + ", " + revokeRequest + ") failed as User doesn't have permission to revoke Policy");
						throw restErrorUtil.createGrantRevokeRESTException("User doesn't have necessary permission to revoke access");
					}
				} catch(WebApplicationException excp) {
					throw excp;
				} catch(Throwable excp) {
					LOG.error("secureRevokeAccess(" + serviceName + ", " + revokeRequest + ") failed", excp);

					throw restErrorUtil.createRESTException(excp.getMessage());
				} finally {
					RangerPerfTracer.log(perf);
				}

				ret.setStatusCode(RESTResponse.STATUS_SUCCESS);
			}
		}
		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceREST.secureRevokeAccess(" + serviceName + ", " + revokeRequest + "): " + ret);
		}
		return ret;
	}	
	
	@POST
	@Path("/policies")
	@Produces({ "application/json", "application/xml" })
	public RangerPolicy createPolicy(RangerPolicy policy, @Context HttpServletRequest request) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceREST.createPolicy(" + policy + ")");
		}

		RangerPolicy     ret  = null;
		RangerPerfTracer perf = null;

		try {
			if(RangerPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
				perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "ServiceREST.createPolicy(policyName=" + policy.getName() + ")");
			}

			if(request != null) {
				String serviceName    = request.getParameter(PARAM_SERVICE_NAME);
				String policyName     = request.getParameter(PARAM_POLICY_NAME);
				String updateIfExists = request.getParameter(PARAM_UPDATE_IF_EXISTS);

				if (serviceName == null && policyName == null && updateIfExists != null
						&& updateIfExists.equalsIgnoreCase("true")) {
					serviceName = (String) request.getAttribute(PARAM_SERVICE_NAME);
					policyName = (String) request.getAttribute(PARAM_POLICY_NAME);
				}

				if(StringUtils.isNotEmpty(serviceName)) {
					policy.setService(serviceName);
				}

				if(StringUtils.isNotEmpty(policyName)) {
					policy.setName(StringUtils.trim(policyName));
				}

				if (updateIfExists != null && Boolean.valueOf(updateIfExists)) {
					RangerPolicy existingPolicy = null;
					try {
						if(StringUtils.isNotEmpty(policy.getGuid())) {
							existingPolicy = getPolicyByGuid(policy.getGuid());
						}

						if(existingPolicy == null && StringUtils.isNotEmpty(serviceName) && StringUtils.isNotEmpty(policyName)) {
							existingPolicy = getPolicyByName(policy.getService(), policy.getName());
						}

						if (existingPolicy != null) {
							policy.setId(existingPolicy.getId());
							ret = updatePolicy(policy);
						}
					} catch(Exception excp) {
						LOG.info("ServiceREST.createPolicy(): Failed to find/update exising policy, will attempt to create the policy", excp);
					}
				}
			}

			if(ret == null) {
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

				ensureAdminAccess(policy);
                                bizUtil.blockAuditorRoleUser();
				ret = svcStore.createPolicy(policy);
			}
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

	/*
	The verb for applyPolicy is POST as it could be partial update or a create
	*/

	@POST
	@Path("/policies/apply")
	@Produces({ "application/json", "application/xml" })
	public RangerPolicy applyPolicy(RangerPolicy policy, @Context HttpServletRequest request) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceREST.applyPolicy(" + policy + ")");
		}

		RangerPolicy ret = null;

		if (policy != null && StringUtils.isNotBlank(policy.getService())) {
			try {
				// Check if applied policy contains any conditions
				if (ServiceRESTUtil.containsRangerCondition(policy)) {
					LOG.error("Applied policy contains condition(s); not supported:" + policy);
					throw new Exception("Applied policy contains condition(s); not supported:" + policy);
				}

				String user = request.getRemoteUser();
				RangerPolicy existingPolicy = getExactMatchPolicyForResource(policy.getService(), policy.getResources(), StringUtils.isNotBlank(user) ? user :"admin");

				if (existingPolicy == null) {
					ret = createPolicy(policy, null);
				} else {
					ServiceRESTUtil.processApplyPolicy(existingPolicy, policy);

					ret = updatePolicy(existingPolicy);
				}
			} catch(WebApplicationException excp) {
				throw excp;
			} catch (Exception exception) {
				LOG.error("Failed to apply policy:", exception);
				throw restErrorUtil.createRESTException(exception.getMessage());
			}
		} else {
			throw restErrorUtil.createRESTException("Non-existing service specified:");
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceREST.applyPolicy(" + policy + ") : " + ret);
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

		RangerPolicy ret  = null;
		RangerPerfTracer perf = null;

		try {
			if(RangerPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
				perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "ServiceREST.updatePolicy(policyId=" + policy.getId() + ")");
			}
			RangerPolicyValidator validator = validatorFactory.getPolicyValidator(svcStore);
			validator.validate(policy, Action.UPDATE, bizUtil.isAdmin());

			ensureAdminAccess(policy);
                        bizUtil.blockAuditorRoleUser();
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

			ensureAdminAccess(policy);
                        bizUtil.blockAuditorRoleUser();
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
                                ensureAdminAndAuditAccess(ret);
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
	@Path("/policyLabels")
	@Produces({ "application/json", "application/xml" })
	public List<String> getPolicyLabels(@Context HttpServletRequest request) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceREST.getPolicyLabels()");
		}

		List<String> ret = new ArrayList<String>();
		RangerPerfTracer perf = null;

		try {
			if (RangerPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
				perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "ServiceREST.getPolicyLabels()");
			}

			SearchFilter filter = searchUtil.getSearchFilter(request, policyLabelsService.sortFields);
			ret = svcStore.getPolicyLabels(filter);
		} catch (WebApplicationException excp) {
			throw excp;
		} catch (Throwable excp) {
			LOG.error("getPolicyLabels() failed", excp);

			throw restErrorUtil.createRESTException(excp.getMessage());
		} finally {
			RangerPerfTracer.log(perf);
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceREST.getPolicyLabels()");
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
				final int savedStartIndex = filter.getStartIndex();
				final int savedMaxRows    = filter.getMaxRows();

				filter.setStartIndex(0);
				filter.setMaxRows(Integer.MAX_VALUE);

				List<RangerPolicy> policies = svcStore.getPolicies(filter);

				filter.setStartIndex(savedStartIndex);
				filter.setMaxRows(savedMaxRows);

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

	@GET
	@Path("/policies/downloadExcel")
	@Produces("application/ms-excel")
	public void getPoliciesInExcel(@Context HttpServletRequest request,
			@Context HttpServletResponse response) {

		if (LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceREST.getPoliciesInExcel()");
		}
		RangerPerfTracer perf = null;
		SearchFilter filter = searchUtil.getSearchFilter(request, policyService.sortFields);

		try {
			if (RangerPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
				perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "ServiceREST.getPoliciesInExcel()");
			}
			List<RangerPolicy> policyLists = new ArrayList<RangerPolicy>();
			
			policyLists = getAllFilteredPolicyList(filter, request, policyLists);
			if (CollectionUtils.isNotEmpty(policyLists)){
                                for (RangerPolicy rangerPolicy : policyLists) {
                                        if (rangerPolicy != null) {
                                                ensureAdminAndAuditAccess(rangerPolicy);
                                        }
                                }
				svcStore.getPoliciesInExcel(policyLists, response);
			}else{
				response.setStatus(HttpServletResponse.SC_NO_CONTENT);
				LOG.error("No policies found to download!");
			}
			
			RangerExportPolicyList rangerExportPolicyList = new RangerExportPolicyList();
			svcStore.putMetaDataInfo(rangerExportPolicyList);
			String metaDataInfo = new ObjectMapper().writeValueAsString(rangerExportPolicyList.getMetaDataInfo());
			
			List<XXTrxLog> trxLogList = new ArrayList<XXTrxLog>();
			XXTrxLog xxTrxLog = new XXTrxLog();
			xxTrxLog.setAction("EXPORT EXCEL");
			xxTrxLog.setObjectClassType(AppConstants.CLASS_TYPE_RANGER_POLICY);
			xxTrxLog.setPreviousValue(metaDataInfo);
			trxLogList.add(xxTrxLog);
			bizUtil.createTrxLog(trxLogList);
		} catch (WebApplicationException excp) {
			throw excp;
		} catch (Throwable excp) {
			LOG.error("Error while downloading policy report", excp);
			throw restErrorUtil.createRESTException(excp.getMessage());
		} finally {
			RangerPerfTracer.log(perf);
		}
	}

	@GET
	@Path("/policies/csv")
	@Produces("text/csv")
	public void getPoliciesInCsv(@Context HttpServletRequest request, @Context HttpServletResponse response) throws IOException {

		if (LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceREST.getPoliciesInCsv()");
		}
		RangerPerfTracer perf = null;
		
		SearchFilter filter = searchUtil.getSearchFilter(request, policyService.sortFields);

		try {
			if (RangerPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
				perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "ServiceREST.getPoliciesInCsv()");
			}
			List<RangerPolicy> policyLists = new ArrayList<RangerPolicy>();
			
			policyLists = getAllFilteredPolicyList(filter, request, policyLists);
			if (CollectionUtils.isNotEmpty(policyLists)){
                                for (RangerPolicy rangerPolicy : policyLists) {
                                        if (rangerPolicy != null) {
                                                ensureAdminAndAuditAccess(rangerPolicy);
                                        }
                                }

				svcStore.getPoliciesInCSV(policyLists, response);
			}else{
				response.setStatus(HttpServletResponse.SC_NO_CONTENT);
				LOG.error("No policies found to download!");
			}
			
			RangerExportPolicyList rangerExportPolicyList = new RangerExportPolicyList();
			svcStore.putMetaDataInfo(rangerExportPolicyList);
			String metaDataInfo = new ObjectMapper().writeValueAsString(rangerExportPolicyList.getMetaDataInfo());
			
			List<XXTrxLog> trxLogList = new ArrayList<XXTrxLog>();
			XXTrxLog xxTrxLog = new XXTrxLog();
			xxTrxLog.setAction("EXPORT CSV");
			xxTrxLog.setObjectClassType(AppConstants.CLASS_TYPE_RANGER_POLICY);
			xxTrxLog.setPreviousValue(metaDataInfo);
			trxLogList.add(xxTrxLog);
			bizUtil.createTrxLog(trxLogList);
		} catch (WebApplicationException excp) {
			throw excp;
		} catch (Throwable excp) {
			LOG.error("Error while downloading policy report", excp);
			throw restErrorUtil.createRESTException(excp.getMessage());
		} finally {
			RangerPerfTracer.log(perf);
		}
	}

	@GET
	@Path("/policies/exportJson")
	@Produces("text/json")
	public void getPoliciesInJson(@Context HttpServletRequest request,
			@Context HttpServletResponse response,
			@QueryParam("checkPoliciesExists") Boolean checkPoliciesExists) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceREST.getPoliciesInJson()");
		}
		
		RangerPerfTracer perf = null;
		SearchFilter filter = searchUtil.getSearchFilter(request,policyService.sortFields);

		try {
			if (RangerPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
				perf = RangerPerfTracer.getPerfTracer(PERF_LOG,"ServiceREST.getPoliciesInJson()");
			}
			if (checkPoliciesExists == null){
				checkPoliciesExists = false;
			}

			List<RangerPolicy> policyLists = new ArrayList<RangerPolicy>();
			
			policyLists = getAllFilteredPolicyList(filter, request, policyLists);
			if (CollectionUtils.isNotEmpty(policyLists)) {
				for (RangerPolicy rangerPolicy : policyLists) {
					if (rangerPolicy != null) {
						ensureAdminAndAuditAccess(rangerPolicy);
					}
				}
				bizUtil.blockAuditorRoleUser();
				svcStore.getPoliciesInJson(policyLists, response);
			} else {
				checkPoliciesExists = true;
				response.setStatus(HttpServletResponse.SC_NO_CONTENT);
				LOG.error("There is no Policy to Export!!");
			}
                        
			if(!checkPoliciesExists){
				RangerExportPolicyList rangerExportPolicyList = new RangerExportPolicyList();
				svcStore.putMetaDataInfo(rangerExportPolicyList);
				String metaDataInfo = new ObjectMapper().writeValueAsString(rangerExportPolicyList.getMetaDataInfo());
							
				List<XXTrxLog> trxLogList = new ArrayList<XXTrxLog>();
				XXTrxLog xxTrxLog = new XXTrxLog();
				xxTrxLog.setAction("EXPORT JSON");
				xxTrxLog.setObjectClassType(AppConstants.CLASS_TYPE_RANGER_POLICY);
				xxTrxLog.setPreviousValue(metaDataInfo);
				trxLogList.add(xxTrxLog);
				bizUtil.createTrxLog(trxLogList);
			}
		} catch (WebApplicationException excp) {
			throw excp;
		} catch (Throwable excp) {
			LOG.error("Error while exporting policy file!!", excp);
			throw restErrorUtil.createRESTException(excp.getMessage());
		} finally {
			RangerPerfTracer.log(perf);
		}
	}
	
	@POST
	@Path("/policies/importPoliciesFromFile")
	@Consumes({MediaType.MULTIPART_FORM_DATA, MediaType.APPLICATION_JSON})
	@Produces({ "application/json", "application/xml" })
	@PreAuthorize("@rangerPreAuthSecurityHandler.isAdminOrKeyAdminRole()")
	public void importPoliciesFromFile(
			@Context HttpServletRequest request,
			@FormDataParam("servicesMapJson") InputStream serviceMapStream,
			@FormDataParam("file") InputStream uploadedInputStream,
			@FormDataParam("file") FormDataContentDisposition fileDetail,
			@QueryParam("isOverride") Boolean isOverride) {
		
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceREST.importPoliciesFromFile()");
		}
		RangerPerfTracer perf = null;
		String metaDataInfo = null;
		List<XXTrxLog> trxLogListError = new ArrayList<XXTrxLog>();
		XXTrxLog xxTrxLogError = new XXTrxLog();
		
		try {
			if (RangerPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
				perf = RangerPerfTracer.getPerfTracer(PERF_LOG,"ServiceREST.importPoliciesFromFile()");
			}
			
			List<XXTrxLog> trxLogList = new ArrayList<XXTrxLog>();
			XXTrxLog xxTrxLog = new XXTrxLog();
			xxTrxLog.setAction("IMPORT START");
			xxTrxLog.setObjectClassType(AppConstants.CLASS_TYPE_RANGER_POLICY);
                        xxTrxLog.setPreviousValue("IMPORT START");
			trxLogList.add(xxTrxLog);
			bizUtil.createTrxLog(trxLogList);
			
			if (isOverride == null){
				isOverride = false;
			}
			List<String> serviceNameList = new ArrayList<String>();
			String serviceType = null;
			List<String> serviceTypeList = null;
			SearchFilter filter = searchUtil.getSearchFilter(request,policyService.sortFields);
			if (StringUtils.isNotEmpty(request.getParameter(PARAM_SERVICE_TYPE))){
				serviceType = request.getParameter(PARAM_SERVICE_TYPE);
			}
			if(StringUtils.isNotEmpty(serviceType)){
				serviceTypeList = new ArrayList<String>(Arrays.asList(serviceType.split(",")));
			}
			List<RangerService> rangerServiceList = null;
			List<RangerService> rangerServiceLists = new ArrayList<RangerService>();
			if (CollectionUtils.isNotEmpty(serviceTypeList)){
				for (String s : serviceTypeList) {
					filter.removeParam(PARAM_SERVICE_TYPE);
					filter.setParam(PARAM_SERVICE_TYPE, s.trim());
					rangerServiceList = getServices(filter);
					rangerServiceLists.addAll(rangerServiceList);
				}
			}
			if(!CollectionUtils.sizeIsEmpty(rangerServiceLists)){
				for(RangerService rService : rangerServiceLists){
					if (StringUtils.isNotEmpty(rService.getName())){
						serviceNameList.add(rService.getName());
					}
				}
			}

			Map<String, String> servicesMappingMap = new LinkedHashMap<String, String>();
			List<String> sourceServices = new ArrayList<String>();
			List<String> destinationServices = new ArrayList<String>();
			if (serviceMapStream != null){
				servicesMappingMap = svcStore.getServiceMap(serviceMapStream);
			}
			if(!CollectionUtils.sizeIsEmpty(servicesMappingMap)){
				for (Entry<String, String> map : servicesMappingMap.entrySet()) {
					String sourceServiceName = null;
					String destinationServiceName = null;
					if (StringUtils.isNotEmpty(map.getKey().trim()) && StringUtils.isNotEmpty(map.getValue().trim())){
						sourceServiceName = map.getKey().trim();
						destinationServiceName = map.getValue().trim();
					}else{
						LOG.error("Source service or destonation service name is not provided!!");
						throw restErrorUtil.createRESTException("Source service or destonation service name is not provided!!");
					}
					if (StringUtils.isNotEmpty(sourceServiceName)
							&& StringUtils.isNotEmpty(destinationServiceName)) {
						sourceServices.add(sourceServiceName);
						destinationServices.add(destinationServiceName);
					}
				}
			}	
			
			String fileName = fileDetail.getFileName();
			int totalPolicyCreate = 0;
			Map<String, RangerPolicy> policiesMap = new LinkedHashMap<String, RangerPolicy>();
			List<String> dataFileSourceServices = new ArrayList<String>();
			if (fileName.endsWith("json")) {
				try {
					RangerExportPolicyList rangerExportPolicyList = null;
					List<RangerPolicy> policies = null;
					Gson gson = new Gson();
					
					String policiesString = IOUtils.toString(uploadedInputStream);
					policiesString = policiesString.trim();
					if (StringUtils.isNotEmpty(policiesString)){
						gson.fromJson(policiesString, RangerExportPolicyList.class);
						rangerExportPolicyList = new ObjectMapper().readValue(policiesString, RangerExportPolicyList.class);
					} else {
						LOG.error("Provided json file is empty!!");
						throw restErrorUtil.createRESTException("Provided json file is empty!!");
					}
					if (rangerExportPolicyList != null && !CollectionUtils.sizeIsEmpty(rangerExportPolicyList.getMetaDataInfo())){
						metaDataInfo = new ObjectMapper().writeValueAsString(rangerExportPolicyList.getMetaDataInfo());
					} else {
						LOG.info("metadata info is not provided!!");
					}
					if (rangerExportPolicyList != null && !CollectionUtils.sizeIsEmpty(rangerExportPolicyList.getPolicies())){
						policies = rangerExportPolicyList.getPolicies();
					} else {
						LOG.error("Provided json file does not contain any policy!!");
						throw restErrorUtil.createRESTException("Provided json file does not contain any policy!!");
					}
					if (CollectionUtils.sizeIsEmpty(servicesMappingMap) && isOverride){
						if(policies != null && !CollectionUtils.sizeIsEmpty(policies)){
							for (RangerPolicy policyInJson: policies){
								if (policyInJson != null) {
									if (StringUtils.isNotEmpty(policyInJson.getService().trim())) {
										String serviceName = policyInJson.getService().trim();
										if (CollectionUtils.isNotEmpty(serviceNameList) && serviceNameList.contains(serviceName)) {
											sourceServices.add(serviceName);
											destinationServices.add(serviceName);
										} else if (CollectionUtils.isEmpty(serviceNameList)) {
											sourceServices.add(serviceName);
											destinationServices.add(serviceName);
										}
									}else{
										LOG.error("Service Name or Policy Name is not provided!!");
										throw restErrorUtil.createRESTException("Service Name or Policy Name is not provided!!");
									}
								}
							}
						}
					}else if (!CollectionUtils.sizeIsEmpty(servicesMappingMap)) {
						if (policies != null && !CollectionUtils.sizeIsEmpty(policies)){
							for (RangerPolicy policyInJson: policies){
								if (policyInJson != null){
									if (StringUtils.isNotEmpty(policyInJson.getService().trim())) {
										dataFileSourceServices.add(policyInJson.getService().trim());
									}else{
										LOG.error("Service Name or Policy Name is not provided!!");
										throw restErrorUtil.createRESTException("Service Name or Policy Name is not provided!!");
									}
								}
							}
							if(!dataFileSourceServices.containsAll(sourceServices)){
								LOG.error("Json File does not contain sepcified source service name.");
								throw restErrorUtil.createRESTException("Json File does not contain sepcified source service name.");
							}
						}
					}
					String updateIfExists = request.getParameter(PARAM_UPDATE_IF_EXISTS);
					String polResource = request.getParameter(SearchFilter.POL_RESOURCE);
					if (updateIfExists == null || updateIfExists.isEmpty()) {
						updateIfExists = "false";
					} else if (updateIfExists.equalsIgnoreCase("true")) {
						isOverride = false;
					}
					if (isOverride && "false".equalsIgnoreCase(updateIfExists) && StringUtils.isEmpty(polResource)) {
						if (LOG.isDebugEnabled()) {
							LOG.debug("Deleting Policy from provided services in servicesMapJson file...");
						}
						if (CollectionUtils.isNotEmpty(sourceServices) && CollectionUtils.isNotEmpty(destinationServices)) {
							deletePoliciesProvidedInServiceMap(sourceServices, destinationServices);
						}
					}

					if ("true".equalsIgnoreCase(updateIfExists) && StringUtils.isNotEmpty(polResource)) {
						if (LOG.isDebugEnabled()) {
							LOG.debug("Deleting Policy from provided services in servicesMapJson file for specific resource...");
						}
						if (CollectionUtils.isNotEmpty(sourceServices) && CollectionUtils.isNotEmpty(destinationServices)){
							deletePoliciesForResource(sourceServices, destinationServices, request, policies);
						}
					}
					if (policies != null && !CollectionUtils.sizeIsEmpty(policies)){
						for (RangerPolicy policyInJson: policies){
							if (policyInJson != null){
								policiesMap = svcStore.createPolicyMap(servicesMappingMap, sourceServices, destinationServices, policyInJson, policiesMap);
							}
						}
					}
					if (!CollectionUtils.sizeIsEmpty(policiesMap.entrySet())) {
						for (Entry<String, RangerPolicy> entry : policiesMap.entrySet()) {
							RangerPolicy policy = entry.getValue();
							if (policy != null){
								if (!CollectionUtils.isEmpty(serviceNameList)) {
									for (String service : serviceNameList) {
										if (StringUtils.isNotEmpty(service.trim()) && StringUtils.isNotEmpty(policy.getService().trim())){
											if (policy.getService().trim().equalsIgnoreCase(service.trim())) {
												if (updateIfExists != null && !updateIfExists.isEmpty()){
													request.setAttribute(PARAM_SERVICE_NAME, policy.getService());
													request.setAttribute(PARAM_POLICY_NAME, policy.getName());
												}
												createPolicy(policy, request);
												totalPolicyCreate = totalPolicyCreate + 1;
												if (LOG.isDebugEnabled()) {
													LOG.debug("Policy " + policy.getName() + " created successfully.");
												}
												break;
											}
										} else {
											LOG.error("Service Name or Policy Name is not provided!!");
											throw restErrorUtil.createRESTException("Service Name or Policy Name is not provided!!");
										}
									}
								} else {
									if (updateIfExists != null && !updateIfExists.isEmpty()){
										request.setAttribute(PARAM_SERVICE_NAME, policy.getService());
										request.setAttribute(PARAM_POLICY_NAME, policy.getName());
									}
									createPolicy(policy, request);
									totalPolicyCreate = totalPolicyCreate + 1;
									if (LOG.isDebugEnabled()) {
										LOG.debug("Policy " + policy.getName() + " created successfully.");
									}
								}
							}
						}
						if (LOG.isDebugEnabled()) {
							LOG.debug("Total Policy Created From Json file : " + totalPolicyCreate);
						}
						if(!(totalPolicyCreate > 0)){
							LOG.error("zero policy is created from provided data file!!");
							throw restErrorUtil.createRESTException("zero policy is created from provided data file!!");
						}
					}
				} catch (IOException e) {
					LOG.error(e.getMessage());
					throw restErrorUtil.createRESTException(e.getMessage());
				}
			}else{
				LOG.error("Provided file format is not supported!!");
				throw restErrorUtil.createRESTException("Provided file format is not supported!!");
			}
		} catch(JsonSyntaxException ex) { 
			LOG.error("Provided json file is not valid!!", ex);
			xxTrxLogError.setAction("IMPORT ERROR");
			xxTrxLogError.setObjectClassType(AppConstants.CLASS_TYPE_RANGER_POLICY);
			if(StringUtils.isNotEmpty(metaDataInfo)){
				xxTrxLogError.setPreviousValue(metaDataInfo);
			}
			trxLogListError.add(xxTrxLogError);
			bizUtil.createTrxLog(trxLogListError);
			throw restErrorUtil.createRESTException(ex.getMessage());
	      }catch (WebApplicationException excp) {
			LOG.error("Error while importing policy from file!!", excp);
			xxTrxLogError.setAction("IMPORT ERROR");
			xxTrxLogError.setObjectClassType(AppConstants.CLASS_TYPE_RANGER_POLICY);
			if(StringUtils.isNotEmpty(metaDataInfo)){
				xxTrxLogError.setPreviousValue(metaDataInfo);
			}
			trxLogListError.add(xxTrxLogError);
			bizUtil.createTrxLog(trxLogListError);
			throw excp;
		} catch (Throwable excp) {
			LOG.error("Error while importing policy from file!!", excp);
			xxTrxLogError.setAction("IMPORT ERROR");
			xxTrxLogError.setObjectClassType(AppConstants.CLASS_TYPE_RANGER_POLICY);
			if(StringUtils.isNotEmpty(metaDataInfo)){
				xxTrxLogError.setPreviousValue(metaDataInfo);
			}
			trxLogListError.add(xxTrxLogError);
			bizUtil.createTrxLog(trxLogListError);
			throw restErrorUtil.createRESTException(excp.getMessage());
		} finally {
			RangerPerfTracer.log(perf);
			List<XXTrxLog> trxLogListEnd = new ArrayList<XXTrxLog>();
			XXTrxLog xxTrxLogEnd = new XXTrxLog();
			xxTrxLogEnd.setAction("IMPORT END");
			xxTrxLogEnd.setObjectClassType(AppConstants.CLASS_TYPE_RANGER_POLICY);
			if(StringUtils.isNotEmpty(metaDataInfo)){
				xxTrxLogEnd.setPreviousValue(metaDataInfo);
			}
			trxLogListEnd.add(xxTrxLogEnd);
			bizUtil.createTrxLog(trxLogListEnd);
			if (LOG.isDebugEnabled()) {
				LOG.debug("<== ServiceREST.importPoliciesFromFile()");
			}
		}
	}
	
	private List<RangerPolicy> getAllFilteredPolicyList(SearchFilter filter,
			HttpServletRequest request, List<RangerPolicy> policyLists) {
		String serviceNames = null;
		String serviceType = null;
		List<String> serviceNameList = null;
		List<String> serviceTypeList = null;
		List<String> serviceNameInServiceTypeList = new ArrayList<String>();
		boolean isServiceExists = false;
		
		if (request.getParameter(PARAM_SERVICE_NAME) != null){
			serviceNames = request.getParameter(PARAM_SERVICE_NAME);
		}
		if (StringUtils.isNotEmpty(serviceNames)) {
			serviceNameList = new ArrayList<String>(Arrays.asList(serviceNames.split(",")));
		}
		
		if (request.getParameter(PARAM_SERVICE_TYPE) != null){
			serviceType = request.getParameter(PARAM_SERVICE_TYPE);
		}
		if(StringUtils.isNotEmpty(serviceType)){
			serviceTypeList = new ArrayList<String>(Arrays.asList(serviceType.split(",")));
		}
		
		List<RangerPolicy> policyList = new ArrayList<RangerPolicy>();
		List<RangerPolicy> policyListByServiceName = new ArrayList<RangerPolicy>();
		
		if (filter != null) {
			filter.setStartIndex(0);
			filter.setMaxRows(Integer.MAX_VALUE);
			
			if (!CollectionUtils.isEmpty(serviceTypeList)) {
				for (String s : serviceTypeList) {
					filter.removeParam(PARAM_SERVICE_TYPE);
					if (request.getParameter(PARAM_SERVICE_NAME) != null){
						filter.removeParam(PARAM_SERVICE_NAME);
					}
					filter.setParam(PARAM_SERVICE_TYPE, s.trim());
					policyList = getPolicies(filter);
					policyLists.addAll(policyList);
				}
				if(!CollectionUtils.sizeIsEmpty(policyLists)){
					for (RangerPolicy rangerPolicy:policyLists){
						if (StringUtils.isNotEmpty(rangerPolicy.getService())){
							serviceNameInServiceTypeList.add(rangerPolicy.getService());
						}
					}
				}
			}
			if (!CollectionUtils.isEmpty(serviceNameList) && !CollectionUtils.isEmpty(serviceTypeList)){
				isServiceExists = serviceNameInServiceTypeList.containsAll(serviceNameList);
				if(isServiceExists){
					for (String s : serviceNameList) {
						filter.removeParam(PARAM_SERVICE_NAME);
						filter.removeParam(PARAM_SERVICE_TYPE);
						filter.setParam(PARAM_SERVICE_NAME, s.trim());
						policyList = getPolicies(filter);
						policyListByServiceName.addAll(policyList);
					}
					policyLists = policyListByServiceName;
				}else{
					policyLists = new ArrayList<RangerPolicy>();
				}
			}else if (CollectionUtils.isEmpty(serviceNameList) && CollectionUtils.isEmpty(serviceTypeList)){
				policyLists = getPolicies(filter);
			}
			if (!CollectionUtils.isEmpty(serviceNameList) && CollectionUtils.isEmpty(serviceTypeList)) {
				for (String s : serviceNameList) {
					filter.removeParam(PARAM_SERVICE_NAME);
					filter.setParam(PARAM_SERVICE_NAME, s.trim());
					policyList = getPolicies(filter);
					policyLists.addAll(policyList);
				}
			}
		}
		if (StringUtils.isNotEmpty(request.getParameter("resourceMatch"))
				&& "full".equalsIgnoreCase(request.getParameter("resourceMatch"))) {
			policyLists = serviceUtil.getMatchingPoliciesForResource(request, policyLists);
		}
		Map<Long, RangerPolicy> orderedPolicies = new TreeMap<Long, RangerPolicy>();
		
		if (!CollectionUtils.isEmpty(policyLists)) {
			for (RangerPolicy policy : policyLists) {
				if (policy != null) {
					orderedPolicies.put(policy.getId(), policy);
				}
			}
			if (!orderedPolicies.isEmpty()) {
				policyLists.clear();
				policyLists.addAll(orderedPolicies.values());
			}
		}
		return policyLists;
	}
	
	private void deletePoliciesProvidedInServiceMap(
			List<String> sourceServices, List<String> destinationServices) {
		int totalDeletedPilicies = 0;
		if (CollectionUtils.isNotEmpty(sourceServices)
				&& CollectionUtils.isNotEmpty(destinationServices)) {
			RangerPolicyValidator validator = validatorFactory.getPolicyValidator(svcStore);
			for (int i = 0; i < sourceServices.size(); i++) {
				if (!destinationServices.get(i).isEmpty()) {
					final RangerPolicyList servicePolicies = getServicePolicies(destinationServices.get(i), new SearchFilter());
					if (servicePolicies != null) {
						List<RangerPolicy> rangerPolicyList = servicePolicies.getPolicies();
						if (CollectionUtils.isNotEmpty(rangerPolicyList)) {
							for (RangerPolicy rangerPolicy : rangerPolicyList) {
								if (rangerPolicy != null) {
									try {
										validator.validate(rangerPolicy.getId(), Action.DELETE);
										ensureAdminAccess(rangerPolicy);
                                                                                bizUtil.blockAuditorRoleUser();
										svcStore.deletePolicy(rangerPolicy.getId());
										totalDeletedPilicies = totalDeletedPilicies + 1;
										if (LOG.isDebugEnabled()) {
											LOG.debug("Policy " + rangerPolicy.getName() + " deleted successfully." );
											LOG.debug("TotalDeletedPilicies: " +totalDeletedPilicies);
										}
									} catch(Throwable excp) {
										LOG.error("deletePolicy(" + rangerPolicy.getId() + ") failed", excp);
									}
								}
							}
						}
					}
				}
			}
		}
		if (LOG.isDebugEnabled()) {
			LOG.debug("Total Deleted Policy : " + totalDeletedPilicies);
		}
	}

	private void deletePoliciesForResource(List<String> sourceServices, List<String> destinationServices, HttpServletRequest request, List<RangerPolicy> exportPolicies) {
		int totalDeletedPilicies = 0;
		if (CollectionUtils.isNotEmpty(sourceServices)
				&& CollectionUtils.isNotEmpty(destinationServices)) {
			Set<String> exportedPolicyNames=new HashSet<String>();
			if (CollectionUtils.isNotEmpty(exportPolicies)) {
				for (RangerPolicy rangerPolicy : exportPolicies) {
					if (rangerPolicy!=null) {
						exportedPolicyNames.add(rangerPolicy.getName());
					}
				}
			}
			for (int i = 0; i < sourceServices.size(); i++) {
				if (!destinationServices.get(i).isEmpty()) {
					RangerPolicyList servicePolicies = null;
					servicePolicies = getServicePoliciesByName(destinationServices.get(i), request);
					if (servicePolicies != null) {
						List<RangerPolicy> rangerPolicyList = servicePolicies.getPolicies();
						if (CollectionUtils.isNotEmpty(rangerPolicyList)) {
							for (RangerPolicy rangerPolicy : rangerPolicyList) {
								if (rangerPolicy != null) {
									Map<String, RangerPolicy.RangerPolicyResource> rangerPolicyResourceMap=rangerPolicy.getResources();
									if (rangerPolicyResourceMap!=null) {
										RangerPolicy.RangerPolicyResource rangerPolicyResource=null;
										if (rangerPolicyResourceMap.containsKey("path")) {
					                        rangerPolicyResource=rangerPolicyResourceMap.get("path");
					                    } else if (rangerPolicyResourceMap.containsKey("database")) {
					                        rangerPolicyResource=rangerPolicyResourceMap.get("database");
					                    }
										if (rangerPolicyResource!=null) {
					                        if (CollectionUtils.isNotEmpty(rangerPolicyResource.getValues()) && rangerPolicyResource.getValues().size()>1) {
					                            continue;
					                        }
					                    }
									}
									if (rangerPolicy.getId() != null) {
										if (!exportedPolicyNames.contains(rangerPolicy.getName())) {
											deletePolicy(rangerPolicy.getId());
											if (LOG.isDebugEnabled()) {
												LOG.debug("Policy " + rangerPolicy.getName() + " deleted successfully.");
											}
											totalDeletedPilicies = totalDeletedPilicies + 1;
										}
									}
								}
							}
						}
					}
				}
			}
		}
	}

	public List<RangerPolicy> getPolicies(SearchFilter filter) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceREST.getPolicies(filter)");
		}

		List<RangerPolicy> ret  = null;
		RangerPerfTracer   perf = null;

		try {
			if (RangerPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
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
			
			ret = Long.valueOf(policies == null ? 0 : policies.size());
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

		SearchFilter filter = searchUtil.getSearchFilter(request, policyService.sortFields);

		RangerPolicyList ret = getServicePolicies(serviceName, filter);

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceREST.getServicePolicies(" + serviceName + "): count="
					+ (ret == null ? 0 : ret.getListSize()));
		}

		return ret;
	}

	private RangerPolicyList getServicePolicies(String serviceName, SearchFilter filter) {
		RangerPerfTracer perf = null;
		try {
			if(RangerPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
				perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "ServiceREST.getServicePolicies(serviceName=" + serviceName + ")");
			}

			if(isAdminUserWithNoFilterParams(filter)) {
				PList<RangerPolicy> policies = svcStore.getPaginatedServicePolicies(serviceName, filter);

				return toRangerPolicyList(policies);
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

				return toRangerPolicyList(servicePolicies, filter);
			}
		} catch(WebApplicationException excp) {
			throw excp;
		} catch (Throwable excp) {
			LOG.error("getServicePolicies(" + serviceName + ") failed", excp);

			throw restErrorUtil.createRESTException(excp.getMessage());
		} finally {
			RangerPerfTracer.log(perf);
		}
	}

	@GET
	@Path("/policies/download/{serviceName}")
	@Produces({ "application/json", "application/xml" })
	public ServicePolicies getServicePoliciesIfUpdated(
			@PathParam("serviceName") String serviceName,
			@QueryParam("lastKnownVersion") Long lastKnownVersion,
			@DefaultValue("0") @QueryParam("lastActivationTime") Long lastActivationTime,
			@QueryParam("pluginId") String pluginId,
			@DefaultValue("") @QueryParam("clusterName") String clusterName,
			@Context HttpServletRequest request) throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceREST.getServicePoliciesIfUpdated("
					+ serviceName + ", " + lastKnownVersion + ", "
					+ lastActivationTime + ")");
		}

		ServicePolicies ret      = null;
		int             httpCode = HttpServletResponse.SC_OK;
		String          logMsg   = null;
		RangerPerfTracer perf    = null;
		Long downloadedVersion   = null;
		boolean isValid          = false;

		try {
			isValid = serviceUtil.isValidateHttpsAuthentication(serviceName, request);
		} catch (WebApplicationException webException) {
			httpCode = webException.getResponse().getStatus();
			logMsg = webException.getResponse().getEntity().toString();
		} catch (Exception e) {
			httpCode = HttpServletResponse.SC_BAD_REQUEST;
			logMsg = e.getMessage();
		}
		if (isValid) {
			if (lastKnownVersion == null) {
				lastKnownVersion = Long.valueOf(-1);
			}

			try {
				if(RangerPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
					perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "ServiceREST.getServicePoliciesIfUpdated(serviceName=" + serviceName + ",lastKnownVersion=" + lastKnownVersion + ",lastActivationTime=" + lastActivationTime + ")");
				}
				ServicePolicies servicePolicies = svcStore.getServicePoliciesIfUpdated(serviceName, lastKnownVersion);

				if (servicePolicies == null) {
					downloadedVersion = lastKnownVersion;
					httpCode = HttpServletResponse.SC_NOT_MODIFIED;
					logMsg = "No change since last update";
				} else {
					downloadedVersion = servicePolicies.getPolicyVersion();
					ret = filterServicePolicies(servicePolicies);
					httpCode = HttpServletResponse.SC_OK;
					logMsg = "Returning " + (ret.getPolicies() != null ? ret.getPolicies().size() : 0) + " policies. Policy version=" + ret.getPolicyVersion();
				}
			} catch (Throwable excp) {
				LOG.error("getServicePoliciesIfUpdated(" + serviceName + ", " + lastKnownVersion + ", " + lastActivationTime + ") failed");

				httpCode = HttpServletResponse.SC_BAD_REQUEST;
				logMsg = excp.getMessage();
			} finally {
				createPolicyDownloadAudit(serviceName, lastKnownVersion, pluginId, httpCode, clusterName, request);
				RangerPerfTracer.log(perf);
			}
		}
		assetMgr.createPluginInfo(serviceName, pluginId, request, RangerPluginInfo.ENTITY_TYPE_POLICIES, downloadedVersion, lastKnownVersion, lastActivationTime, httpCode);

		if(httpCode != HttpServletResponse.SC_OK) {
			boolean logError = httpCode != HttpServletResponse.SC_NOT_MODIFIED;
			throw restErrorUtil.createRESTException(httpCode, logMsg, logError);
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceREST.getServicePoliciesIfUpdated(" + serviceName + ", " + lastKnownVersion + ", " + lastActivationTime + "): count=" + ((ret == null || ret.getPolicies() == null) ? 0 : ret.getPolicies().size()));
		}

		return ret;
	}

	@GET
	@Path("/secure/policies/download/{serviceName}")
	@Produces({ "application/json", "application/xml" })
	public ServicePolicies getSecureServicePoliciesIfUpdated(
			@PathParam("serviceName") String serviceName,
			@QueryParam("lastKnownVersion") Long lastKnownVersion,
			@DefaultValue("0") @QueryParam("lastActivationTime") Long lastActivationTime,
			@QueryParam("pluginId") String pluginId,
			@DefaultValue("") @QueryParam("clusterName") String clusterName,
			@Context HttpServletRequest request) throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceREST.getSecureServicePoliciesIfUpdated("
					+ serviceName + ", " + lastKnownVersion + ")");
		}
		ServicePolicies ret = null;
		int httpCode = HttpServletResponse.SC_OK;
		String logMsg = null;
		RangerPerfTracer perf = null;
		boolean isAllowed = false;
		boolean isAdmin = bizUtil.isAdmin();
		boolean isKeyAdmin = bizUtil.isKeyAdmin();
		request.setAttribute("downloadPolicy", "secure");
		Long downloadedVersion = null;
		boolean isValid = false;
		try {
			isValid = serviceUtil.isValidService(serviceName, request);
		} catch (WebApplicationException webException) {
			httpCode = webException.getResponse().getStatus();
			logMsg = webException.getResponse().getEntity().toString();
		} catch (Exception e) {
			httpCode = HttpServletResponse.SC_BAD_REQUEST;
			logMsg = e.getMessage();
		}
		if (isValid) {
			if (lastKnownVersion == null) {
				lastKnownVersion = Long.valueOf(-1);
			}
			try {
				if (RangerPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
					perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "ServiceREST.getSecureServicePoliciesIfUpdated(serviceName=" + serviceName + ",lastKnownVersion=" + lastKnownVersion + ",lastActivationTime=" + lastActivationTime + ")");
				}
				XXService xService = daoManager.getXXService().findByName(serviceName);
				XXServiceDef xServiceDef = daoManager.getXXServiceDef().getById(xService.getType());
				RangerService rangerService = null;

				if (StringUtils.equals(xServiceDef.getImplclassname(), EmbeddedServiceDefsUtil.KMS_IMPL_CLASS_NAME)) {
					rangerService = svcStore.getServiceByNameForDP(serviceName);
					if (isKeyAdmin) {
						isAllowed = true;
					} else {
						if (rangerService != null) {
							isAllowed = bizUtil.isUserAllowed(rangerService, Allowed_User_List_For_Download);
							if (!isAllowed) {
								isAllowed = bizUtil.isUserAllowed(rangerService, Allowed_User_List_For_Grant_Revoke);
							}
						}
					}
				} else {
					rangerService = svcStore.getServiceByName(serviceName);
					if (isAdmin) {
						isAllowed = true;
					} else {
						if (rangerService != null) {
							isAllowed = bizUtil.isUserAllowed(rangerService, Allowed_User_List_For_Download);
							if (!isAllowed) {
								isAllowed = bizUtil.isUserAllowed(rangerService, Allowed_User_List_For_Grant_Revoke);
							}
						}
					}
				}
				if (isAllowed) {
					ServicePolicies servicePolicies = svcStore.getServicePoliciesIfUpdated(serviceName, lastKnownVersion);
					if (servicePolicies == null) {
						downloadedVersion = lastKnownVersion;
						httpCode = HttpServletResponse.SC_NOT_MODIFIED;
						logMsg = "No change since last update";
					} else {
						downloadedVersion = servicePolicies.getPolicyVersion();
						ret = filterServicePolicies(servicePolicies);
						httpCode = HttpServletResponse.SC_OK;
						logMsg = "Returning " + (ret.getPolicies() != null ? ret.getPolicies().size() : 0) + " policies. Policy version=" + ret.getPolicyVersion();
					}

				} else {
					LOG.error("getSecureServicePoliciesIfUpdated(" + serviceName + ", " + lastKnownVersion + ") failed as User doesn't have permission to download Policy");
					httpCode = HttpServletResponse.SC_UNAUTHORIZED;
					logMsg = "User doesn't have permission to download policy";
				}
			} catch (Throwable excp) {
				LOG.error("getSecureServicePoliciesIfUpdated(" + serviceName + ", " + lastKnownVersion + ", " + lastActivationTime + ") failed");
				httpCode = HttpServletResponse.SC_BAD_REQUEST;
				logMsg = excp.getMessage();
			} finally {
				createPolicyDownloadAudit(serviceName, lastKnownVersion, pluginId, httpCode, clusterName, request);
				RangerPerfTracer.log(perf);
			}
		}
		assetMgr.createPluginInfo(serviceName, pluginId, request, RangerPluginInfo.ENTITY_TYPE_POLICIES, downloadedVersion, lastKnownVersion, lastActivationTime, httpCode);

		if (httpCode != HttpServletResponse.SC_OK) {
			boolean logError = httpCode != HttpServletResponse.SC_NOT_MODIFIED;
			throw restErrorUtil.createRESTException(httpCode, logMsg, logError);
		}
		if (LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceREST.getSecureServicePoliciesIfUpdated(" + serviceName + ", " + lastKnownVersion + ", " + lastActivationTime + "): count=" + ((ret == null || ret.getPolicies() == null) ? 0 : ret.getPolicies().size()));
		}
		return ret;
	}		

	private void createPolicyDownloadAudit(String serviceName, Long lastKnownVersion, String pluginId, int httpRespCode, String clusterName, HttpServletRequest request) {
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
			policyExportAudit.setClusterName(clusterName);
			
			assetMgr.createPolicyAudit(policyExportAudit);
		} catch(Exception excp) {
			LOG.error("error while creating policy download audit", excp);
		}
	}

	private RangerPolicy getExactMatchPolicyForResource(String serviceName, RangerAccessResource resource, String user) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceREST.getExactMatchPolicyForResource(" + resource + ", " + user + ")");
		}

		RangerPolicy       ret          = null;
		RangerPolicyEngine policyEngine = getPolicyEngine(serviceName);

		Map<String, Object> evalContext = new HashMap<String, Object>();
		RangerAccessRequestUtil.setCurrentUserInContext(evalContext, user);

		List<RangerPolicy> policies     = policyEngine != null ? policyEngine.getExactMatchPolicies(resource, evalContext) : null;

		if(CollectionUtils.isNotEmpty(policies)) {
			// at this point, ret is a policy in policy-engine; the caller might update the policy (for grant/revoke); so get a copy from the store
			ret = svcStore.getPolicy(policies.get(0).getId());
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceREST.getExactMatchPolicyForResource(" + resource + ", " + user + "): " + ret);
		}

		return ret;
	}

	private RangerPolicy getExactMatchPolicyForResource(String serviceName, Map<String, RangerPolicyResource> resources, String user) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceREST.getExactMatchPolicyForResource(" + resources + ", " + user + ")");
		}

		RangerPolicy       ret          = null;
		RangerPolicyEngine policyEngine = getPolicyEngine(serviceName);

		Map<String, Object> evalContext = new HashMap<String, Object>();
		RangerAccessRequestUtil.setCurrentUserInContext(evalContext, user);

		List<RangerPolicy> policies     = policyEngine != null ? policyEngine.getExactMatchPolicies(resources, evalContext) : null;

		if(CollectionUtils.isNotEmpty(policies)) {
			// at this point, ret is a policy in policy-engine; the caller might update the policy (for grant/revoke); so get a copy from the store
			ret = svcStore.getPolicy(policies.get(0).getId());
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceREST.getExactMatchPolicyForResource(" + resources + ", " + user + "): " + ret);
		}

		return ret;
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
                                ensureAdminAndAuditAccess(policy);
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

	@GET
	@Path("/plugins/info")
	@Produces({ "application/json", "application/xml" })
	@PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\"" + RangerAPIList.GET_PLUGINS_INFO + "\")")
	public RangerPluginInfoList getPluginsInfo(@Context HttpServletRequest request) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceREST.getPluginsInfo()");
		}

		RangerPluginInfoList ret = null;

		SearchFilter filter = searchUtil.getSearchFilter(request, pluginInfoService.getSortFields());

		try {
			PList<RangerPluginInfo> paginatedPluginsInfo = pluginInfoService.searchRangerPluginInfo(filter);
			if (paginatedPluginsInfo != null) {
				ret = new RangerPluginInfoList();

				ret.setPluginInfoList(paginatedPluginsInfo.getList());
				ret.setPageSize(paginatedPluginsInfo.getPageSize());
				ret.setResultSize(paginatedPluginsInfo.getResultSize());
				ret.setStartIndex(paginatedPluginsInfo.getStartIndex());
				ret.setTotalCount(paginatedPluginsInfo.getTotalCount());
				ret.setSortBy(paginatedPluginsInfo.getSortBy());
				ret.setSortType(paginatedPluginsInfo.getSortType());
			}
		} catch (WebApplicationException excp) {
			throw excp;
		} catch (Throwable excp) {
			LOG.error("getPluginsInfo() failed", excp);

			throw restErrorUtil.createRESTException(excp.getMessage());
		}
		if (LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceREST.getPluginsInfo()");
		}

		return ret;
	}

	private RangerPolicy getPolicyByGuid(String guid) {
		RangerPolicy ret = null;

		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceREST.getPolicyByGuid(" + guid +")");
		}

		SearchFilter filter = new SearchFilter();
		filter.setParam(SearchFilter.GUID, guid);
		List<RangerPolicy> policies = getPolicies(filter);

		if (CollectionUtils.isNotEmpty(policies)) {
			ret = policies.get(0);
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceREST.getPolicyByGuid(" + guid + ")" + ret);
		}
		return ret;
	}

	private RangerPolicy getPolicyByName(String serviceName,String policyName) {
		RangerPolicy ret = null;
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceREST.getPolicyByName(" + serviceName + "," + policyName + ")");
		}

		SearchFilter filter = new SearchFilter();
		filter.setParam(SearchFilter.SERVICE_NAME, serviceName);
		filter.setParam(SearchFilter.POLICY_NAME, policyName);
		List<RangerPolicy> policies = getPolicies(filter);

		if (CollectionUtils.isNotEmpty(policies)) {
			ret = policies.get(0);
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceREST.getPolicyByName(" + serviceName + "," + policyName + ")" + ret);
		}
		return ret;
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
                        boolean	    isAuditAdmin = bizUtil.isAuditAdmin();
                        boolean     isAuditKeyAdmin = bizUtil.isAuditKeyAdmin();
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
					boolean isServiceAdminUser=isAdmin || svcStore.isServiceAdminUser(serviceName, userName);
					if (isAdmin || isKeyAdmin || isAuditAdmin || isAuditKeyAdmin || isServiceAdminUser) {
						XXService xService     = daoManager.getXXService().findByName(serviceName);
						Long      serviceDefId = xService.getType();
						boolean   isKmsService = serviceDefId.equals(EmbeddedServiceDefsUtil.instance().getKmsServiceDefId());

						if (isAdmin) {
							if (!isKmsService) {
								ret.addAll(listToFilter);
							}
                                                } else if (isAuditAdmin) {
                                                        if (!isKmsService) {
                                                                ret.addAll(listToFilter);
                                                        }
                                                } else if (isAuditKeyAdmin) {
                                                        if (isKmsService) {
                                                                ret.addAll(listToFilter);
                                                        }
						} else if (isKeyAdmin) {
							if (isKmsService) {
								ret.addAll(listToFilter);
							}
						} else if (isServiceAdminUser) {
							ret.addAll(listToFilter);
						}

						continue;
					}

					RangerPolicyEngine policyEngine = getDelegatedAdminPolicyEngine(serviceName);

					if (policyEngine != null) {
						if(userGroups == null) {
							userGroups = daoManager.getXXGroupUser().findGroupNamesByUserName(userName);
						}

						for (RangerPolicy policy : listToFilter) {
							if (policyEngine.isAccessAllowed(policy, userName, userGroups, RangerPolicyEngine.ADMIN_ACCESS)) {
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

	void ensureAdminAccess(RangerPolicy policy) {
		boolean isAdmin = bizUtil.isAdmin();
		boolean isKeyAdmin = bizUtil.isKeyAdmin();
		String userName = bizUtil.getCurrentUserLoginId();
		boolean isSvcAdmin = isAdmin || svcStore.isServiceAdminUser(policy.getService(), userName);

		if(!isAdmin && !isKeyAdmin && !isSvcAdmin) {
			boolean isAllowed = false;

			Set<String> userGroups = userMgr.getGroupsForUser(userName);
			isAllowed = hasAdminAccess(policy, userName, userGroups);

			if (!isAllowed) {
				throw restErrorUtil.createRESTException(HttpServletResponse.SC_UNAUTHORIZED,
						"User '" + userName + "' does not have delegated-admin privilege on given resources", true);
			}
		} else {

			XXService xService = daoManager.getXXService().findByName(policy.getService());
			XXServiceDef xServiceDef = daoManager.getXXServiceDef().getById(xService.getType());

			if (isAdmin) {
				if (EmbeddedServiceDefsUtil.KMS_IMPL_CLASS_NAME.equals(xServiceDef.getImplclassname())) {
					throw restErrorUtil.createRESTException(
							"KMS Policies/Services/Service-Defs are not accessible for user '" + userName + "'.",
							MessageEnums.OPER_NO_PERMISSION);
				}
			} else if (isKeyAdmin) {
				if (!EmbeddedServiceDefsUtil.KMS_IMPL_CLASS_NAME.equals(xServiceDef.getImplclassname())) {
					throw restErrorUtil.createRESTException(
							"Only KMS Policies/Services/Service-Defs are accessible for user '" + userName + "'.",
							MessageEnums.OPER_NO_PERMISSION);
				}
			}
		}
	}

	private RangerPolicyEngineOptions getDelegatedAdminPolicyEngineOptions() {
		RangerPolicyEngineOptions opts = new RangerPolicyEngineOptions();

		final String propertyPrefix = "ranger.admin";

		opts.configureDelegateAdmin(RangerConfiguration.getInstance(), propertyPrefix);

		return opts;
	}

	private RangerPolicyEngineOptions getPolicySearchRangerAdminPolicyEngineOptions() {
		RangerPolicyEngineOptions opts = new RangerPolicyEngineOptions();

		final String propertyPrefix = "ranger.admin";

		opts.configureRangerAdminForPolicySearch(RangerConfiguration.getInstance(), propertyPrefix);
		return opts;
	}

	private RangerPolicyEngineOptions getDefaultRangerAdminPolicyEngineOptions() {
		RangerPolicyEngineOptions opts = new RangerPolicyEngineOptions();

		final String propertyPrefix = "ranger.admin";

		opts.configureDefaultRangerAdmin(RangerConfiguration.getInstance(), propertyPrefix);
		return opts;
	}

	private boolean hasAdminAccess(RangerPolicy policy, String userName, Set<String> userGroups) {
		boolean isAllowed = false;

		RangerPolicyEngine policyEngine = getDelegatedAdminPolicyEngine(policy.getService());

		if(policyEngine != null) {
			isAllowed = policyEngine.isAccessAllowed(policy, userName, userGroups, RangerPolicyEngine.ADMIN_ACCESS);
		}

		return isAllowed;
	}
	private boolean hasAdminAccess(String serviceName, String userName, Set<String> userGroups, RangerAccessResource resource) {
		boolean isAllowed = false;

		RangerPolicyEngine policyEngine = getDelegatedAdminPolicyEngine(serviceName);

		if(policyEngine != null) {
			isAllowed = policyEngine.isAccessAllowed(resource, userName, userGroups, RangerPolicyEngine.ADMIN_ACCESS);
		}

		return isAllowed;
	}

	private RangerPolicyEngine getDelegatedAdminPolicyEngine(String serviceName) {
		return RangerPolicyEngineCacheForEngineOptions.getInstance().getPolicyEngine(serviceName, svcStore, delegateAdminOptions);
	}

	private RangerPolicyEngine getPolicySearchPolicyEngine(String serviceName) throws Exception {
		return RangerPolicyEngineCacheForEngineOptions.getInstance().getPolicyEngine(serviceName, svcStore, policySearchAdminOptions);
	}

	private RangerPolicyEngine getPolicyEngine(String serviceName) throws Exception {

		ServicePolicies policies = svcStore.getServicePoliciesIfUpdated(serviceName, -1L);

		RangerPolicyEngine ret = new RangerPolicyEngineImpl("ranger-admin", policies, defaultAdminOptions);

		return ret;
	}

	@GET
	@Path("/checksso")
	@Produces(MediaType.TEXT_PLAIN)
	public String checkSSO() {
		return String.valueOf(bizUtil.isSSOEnabled());
	}
	
	@GET
	@Path("/csrfconf")
	@Produces({ "application/json"})
	public HashMap<String, Object> getCSRFProperties() {
		return getCSRFPropertiesMap();
	}

        @GET
        @Path("/metrics/type/{type}")
        @Produces({ "application/json", "application/xml" })
        @PreAuthorize("@rangerPreAuthSecurityHandler.isAPIAccessible(\""+ RangerAPIList.GET_METRICS_BY_TYPE + "\")")
        public String getMetricByType(@PathParam("type") String type) {
                if (LOG.isDebugEnabled()) {
                        LOG.debug("==> ServiceREST.getMetricByType(serviceDefName=" + type + ")");
                }
                // as of now we are allowing only users with Admin role to access this
                // API
                bizUtil.checkSystemAdminAccess();
                bizUtil.blockAuditorRoleUser();
                String ret = null;
                try {
                        ret = svcStore.getMetricByType(type);
                } catch (WebApplicationException excp) {
                        throw excp;
                } catch (Throwable excp) {
                        LOG.error("getMetricByType(" + type + ") failed", excp);
                        throw restErrorUtil.createRESTException(excp.getMessage());
                }
                if (ret == null) {
                        throw restErrorUtil.createRESTException(HttpServletResponse.SC_NOT_FOUND, "Not found", true);
                }

                if (LOG.isDebugEnabled()) {
                        LOG.debug("<== ServiceREST.getMetricByType(" + type + "): " + ret);
                }
                return ret;
        }

	private HashMap<String, Object> getCSRFPropertiesMap() {
		HashMap<String, Object> map = new HashMap<String, Object>();
		map.put(isCSRF_ENABLED, PropertiesUtil.getBooleanProperty(isCSRF_ENABLED, true));
		map.put(CUSTOM_HEADER_PARAM, PropertiesUtil.getProperty(CUSTOM_HEADER_PARAM, RangerCSRFPreventionFilter.HEADER_DEFAULT));
		map.put(BROWSER_USER_AGENT_PARAM, PropertiesUtil.getProperty(BROWSER_USER_AGENT_PARAM, RangerCSRFPreventionFilter.BROWSER_USER_AGENTS_DEFAULT));
		map.put(CUSTOM_METHODS_TO_IGNORE_PARAM, PropertiesUtil.getProperty(CUSTOM_METHODS_TO_IGNORE_PARAM, RangerCSRFPreventionFilter.METHODS_TO_IGNORE_DEFAULT));
		return map;
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
			int    startIndex = filter.getStartIndex();
			int    pageSize   = filter.getMaxRows();
			int    toIndex    = Math.min(startIndex + pageSize, totalCount);
			String sortType   = filter.getSortType();
			String sortBy     = filter.getSortBy();

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

	private ServicePolicies filterServicePolicies(ServicePolicies servicePolicies) {
		ServicePolicies ret = null;
		boolean containsDisabledResourcePolicies = false;
		boolean containsDisabledTagPolicies = false;

		if (servicePolicies != null) {
			List<RangerPolicy> policies = null;

			policies = servicePolicies.getPolicies();
			if (CollectionUtils.isNotEmpty(policies)) {
				for (RangerPolicy policy : policies) {
					if (!policy.getIsEnabled()) {
						containsDisabledResourcePolicies = true;
						break;
					}
				}
			}

			if (servicePolicies.getTagPolicies() != null) {
				policies = servicePolicies.getTagPolicies().getPolicies();
				if (CollectionUtils.isNotEmpty(policies)) {
					for (RangerPolicy policy : policies) {
						if (!policy.getIsEnabled()) {
							containsDisabledTagPolicies = true;
							break;
						}
					}
				}
			}

			if (!containsDisabledResourcePolicies && !containsDisabledTagPolicies) {
				ret = servicePolicies;
			} else {
				ret = new ServicePolicies();

				ret.setServiceDef(servicePolicies.getServiceDef());
				ret.setServiceId(servicePolicies.getServiceId());
				ret.setServiceName(servicePolicies.getServiceName());
				ret.setPolicyVersion(servicePolicies.getPolicyVersion());
				ret.setPolicyUpdateTime(servicePolicies.getPolicyUpdateTime());
				ret.setPolicies(servicePolicies.getPolicies());
				ret.setTagPolicies(servicePolicies.getTagPolicies());

				if (containsDisabledResourcePolicies) {
					List<RangerPolicy> filteredPolicies = new ArrayList<RangerPolicy>();
					for (RangerPolicy policy : servicePolicies.getPolicies()) {
						if (policy.getIsEnabled()) {
							filteredPolicies.add(policy);
						}
					}
					ret.setPolicies(filteredPolicies);
				}

				if (containsDisabledTagPolicies) {
					ServicePolicies.TagPolicies tagPolicies = new ServicePolicies.TagPolicies();

					tagPolicies.setServiceDef(servicePolicies.getTagPolicies().getServiceDef());
					tagPolicies.setServiceId(servicePolicies.getTagPolicies().getServiceId());
					tagPolicies.setServiceName(servicePolicies.getTagPolicies().getServiceName());
					tagPolicies.setPolicyVersion(servicePolicies.getTagPolicies().getPolicyVersion());
					tagPolicies.setPolicyUpdateTime(servicePolicies.getTagPolicies().getPolicyUpdateTime());

					List<RangerPolicy> filteredPolicies = new ArrayList<RangerPolicy>();
					for (RangerPolicy policy : servicePolicies.getTagPolicies().getPolicies()) {
						if (policy.getIsEnabled()) {
							filteredPolicies.add(policy);
						}
					}
					tagPolicies.setPolicies(filteredPolicies);

					ret.setTagPolicies(tagPolicies);
				}
			}
		}

		return ret;
	}

	private void validateGrantRevokeRequest(GrantRevokeRequest request){
		if( request!=null){
			if(CollectionUtils.isEmpty(request.getUsers()) && CollectionUtils.isEmpty(request.getGroups())){
				throw restErrorUtil.createGrantRevokeRESTException( "Grantee users/groups list is empty");
			}
			String grantor=request.getGrantor();
			if(grantor==null || userMgr.getXUserByUserName(grantor) == null) {
				throw restErrorUtil.createGrantRevokeRESTException( "Grantor user "+grantor+" doesn't exist");
			}
			for(String userName:request.getUsers()){
				if(userMgr.getXUserByUserName(userName) == null) {
					throw restErrorUtil.createGrantRevokeRESTException( "Grantee user "+userName+" doesn't exist");
				}
			}
			for(String groupName:request.getGroups()){
				if(userMgr.getGroupByGroupName(groupName)== null) {
					throw restErrorUtil.createGrantRevokeRESTException( "Grantee group "+groupName+" doesn't exist");
				}
			}
		}
	}

	private Map<String, Object> getOptions(HttpServletRequest request) {
	    Map<String, Object> ret = null;
	    if (request != null) {
	        String isForceRenameOption = request.getParameter(ServiceStore.OPTION_FORCE_RENAME);
	        if (StringUtils.isNotBlank(isForceRenameOption)) {
	            ret = new HashMap<String, Object>();
	            ret.put(ServiceStore.OPTION_FORCE_RENAME, Boolean.valueOf(isForceRenameOption));
            }
        }
        return ret;
    }
	
	private RangerService hideCriticalServiceDetailsForRoleUser(RangerService rangerService){
		RangerService ret = rangerService;
		
		ret.setConfigs(null);
		ret.setDescription(null);
		ret.setCreatedBy(null);
		ret.setUpdatedBy(null);
		ret.setCreateTime(null);
		ret.setUpdateTime(null);
		ret.setPolicyVersion(null);
		ret.setPolicyUpdateTime(null);
		ret.setTagVersion(null);
		ret.setTagUpdateTime(null);
		ret.setVersion(null);
		
		return ret;
	}

        void ensureAdminAndAuditAccess(RangerPolicy policy) {
                boolean isAdmin = bizUtil.isAdmin();
                boolean isKeyAdmin = bizUtil.isKeyAdmin();
                String userName = bizUtil.getCurrentUserLoginId();
                boolean isAuditAdmin = bizUtil.isAuditAdmin();
                boolean isAuditKeyAdmin = bizUtil.isAuditKeyAdmin();
                boolean isSvcAdmin = isAdmin || svcStore.isServiceAdminUser(policy.getService(), userName);
                if (!isAdmin && !isKeyAdmin && !isSvcAdmin && !isAuditAdmin && !isAuditKeyAdmin) {
                        boolean isAllowed = false;

                        Set<String> userGroups = userMgr.getGroupsForUser(userName);
                        isAllowed = hasAdminAccess(policy, userName, userGroups);

                        if (!isAllowed) {
                                throw restErrorUtil.createRESTException(HttpServletResponse.SC_UNAUTHORIZED,"User '"
                                                                                + userName+ "' does not have delegated-admin privilege on given resources",true);
                        }
                } else {

                        XXService xService = daoManager.getXXService().findByName(policy.getService());
                        XXServiceDef xServiceDef = daoManager.getXXServiceDef().getById(xService.getType());

                        if (isAdmin || isAuditAdmin) {
                                if (EmbeddedServiceDefsUtil.KMS_IMPL_CLASS_NAME.equals(xServiceDef.getImplclassname())) {
                                        throw restErrorUtil.createRESTException(
                                                        "KMS Policies/Services/Service-Defs are not accessible for user '"
                                                                        + userName + "'.",MessageEnums.OPER_NO_PERMISSION);
                                }
                        } else if (isKeyAdmin || isAuditKeyAdmin) {
                                if (!EmbeddedServiceDefsUtil.KMS_IMPL_CLASS_NAME.equals(xServiceDef.getImplclassname())) {
                                        throw restErrorUtil.createRESTException("Only KMS Policies/Services/Service-Defs are accessible for user '"
                                                                        + userName + "'.",MessageEnums.OPER_NO_PERMISSION);
                                }
                        }
                }
        }
}
