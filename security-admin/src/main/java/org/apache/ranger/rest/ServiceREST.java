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

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ranger.admin.client.datatype.RESTResponse;
import org.apache.ranger.biz.AssetMgr;
import org.apache.ranger.biz.RangerBizUtil;
import org.apache.ranger.biz.ServiceDBStore;
import org.apache.ranger.biz.ServiceMgr;
import org.apache.ranger.biz.XUserMgr;
import org.apache.ranger.common.MessageEnums;
import org.apache.ranger.common.RESTErrorUtil;
import org.apache.ranger.common.RangerConfigUtil;
import org.apache.ranger.common.RangerSearchUtil;
import org.apache.ranger.common.ServiceUtil;
import org.apache.ranger.entity.XXPolicyExportAudit;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItem;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItemAccess;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyResource;
import org.apache.ranger.plugin.model.validation.RangerPolicyValidator;
import org.apache.ranger.plugin.model.validation.RangerServiceDefValidator;
import org.apache.ranger.plugin.model.validation.RangerServiceValidator;
import org.apache.ranger.plugin.model.validation.RangerValidatorFactory;
import org.apache.ranger.plugin.model.validation.RangerValidator.Action;
import org.apache.ranger.plugin.model.RangerService;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.policyengine.RangerPolicyDb;
import org.apache.ranger.plugin.policyengine.RangerPolicyDbCache;
import org.apache.ranger.plugin.policyengine.RangerPolicyEngine;
import org.apache.ranger.plugin.policyengine.RangerAccessResource;
import org.apache.ranger.plugin.policyengine.RangerAccessResourceImpl;
import org.apache.ranger.plugin.policyevaluator.RangerDefaultPolicyEvaluator;
import org.apache.ranger.plugin.policyevaluator.RangerPolicyEvaluator;
import org.apache.ranger.plugin.service.ResourceLookupContext;
import org.apache.ranger.plugin.util.GrantRevokeRequest;
import org.apache.ranger.plugin.util.SearchFilter;
import org.apache.ranger.plugin.util.ServicePolicies;
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

	// this indirection for validation via a factory exists only for testability
	// TODO move the instantiation to DI framework?
	RangerValidatorFactory validatorFactory = new RangerValidatorFactory(); 

	public ServiceREST() {
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
			RangerServiceDefValidator validator = validatorFactory.getServiceDefValidator(svcStore);
			validator.validate(serviceDef, Action.CREATE);
			ret = svcStore.createServiceDef(serviceDef);
		} catch(Exception excp) {
			LOG.error("createServiceDef(" + serviceDef + ") failed", excp);

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
			RangerServiceDefValidator validator = validatorFactory.getServiceDefValidator(svcStore);
			validator.validate(serviceDef, Action.UPDATE);
			ret = svcStore.updateServiceDef(serviceDef);
		} catch(Exception excp) {
			LOG.error("updateServiceDef(" + serviceDef + ") failed", excp);

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
			RangerServiceDefValidator validator = validatorFactory.getServiceDefValidator(svcStore);
			validator.validate(id, Action.DELETE);
			svcStore.deleteServiceDef(id);
		} catch(Exception excp) {
			LOG.error("deleteServiceDef(" + id + ") failed", excp);

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
			LOG.error("getServiceDef(" + id + ") failed", excp);

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
			LOG.error("getServiceDefByName(" + name + ") failed", excp);

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
	public RangerServiceDefList getServiceDefs(@Context HttpServletRequest request) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceREST.getServiceDefs()");
		}

		RangerServiceDefList ret = null;

		SearchFilter filter = searchUtil.getSearchFilter(request, serviceDefService.sortFields);

		try {
			ret = svcStore.getPaginatedServiceDefs(filter);
		} catch (Exception excp) {
			LOG.error("getServiceDefs() failed", excp);

			throw restErrorUtil.createRESTException(HttpServletResponse.SC_BAD_REQUEST, excp.getMessage(), true);
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceREST.getServiceDefs(): count=" + (ret == null ? 0 : ret.getListSize()));
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
			RangerServiceValidator validator = validatorFactory.getServiceValidator(svcStore);
			validator.validate(service, Action.CREATE);
			
			ret = svcStore.createService(service);
		} catch(Exception excp) {
			LOG.error("createService(" + service + ") failed", excp);

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
			RangerServiceValidator validator = validatorFactory.getServiceValidator(svcStore);
			validator.validate(service, Action.UPDATE);
			ret = svcStore.updateService(service);
		} catch(Exception excp) {
			LOG.error("updateService(" + service + ") failed", excp);

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
			RangerServiceValidator validator = validatorFactory.getServiceValidator(svcStore);
			validator.validate(id, Action.DELETE);
			svcStore.deleteService(id);
		} catch(Exception excp) {
			LOG.error("deleteService(" + id + ") failed", excp);

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
			LOG.error("getService(" + id + ") failed", excp);

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
			LOG.error("getServiceByName(" + name + ") failed", excp);

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
	public RangerServiceList getServices(@Context HttpServletRequest request) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceREST.getServices()");
		}

		RangerServiceList ret = null;

		SearchFilter filter = searchUtil.getSearchFilter(request, svcService.sortFields);

		try {
			ret = svcStore.getPaginatedServices(filter);
		} catch (Exception excp) {
			LOG.error("getServices() failed", excp);

			throw restErrorUtil.createRESTException(HttpServletResponse.SC_BAD_REQUEST, excp.getMessage(), true);
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

		List<RangerService> ret = null;

		try {
			ret = svcStore.getServices(filter);
		} catch(Exception excp) {
			LOG.error("getServices() failed", excp);

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
			List<RangerService> services = getServices(request).getServices();
			
			ret = new Long(services == null ? 0 : services.size());
		} catch(Exception excp) {
			LOG.error("countServices() failed", excp);

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
			ret = serviceMgr.validateConfig(service, svcStore);
		} catch(Exception excp) {
			LOG.error("validateConfig(" + service + ") failed", excp);

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
			ret = serviceMgr.lookupResource(serviceName,context, svcStore);
		} catch(Exception excp) {
			LOG.error("lookupResource(" + serviceName + ", " + context + ") failed", excp);

			throw restErrorUtil.createRESTException(HttpServletResponse.SC_BAD_REQUEST, excp.getMessage(), true);
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

		RESTResponse ret = new RESTResponse();
			
		if (serviceUtil.isValidateHttpsAuthentication(serviceName, request)) {

			try {
				String               userName   = grantRequest.getGrantor();
				Set<String>          userGroups = userMgr.getGroupsForUser(userName);
				RangerAccessResource resource   = new RangerAccessResourceImpl(grantRequest.getResource());
	
				boolean isAdmin = isAdminForResource(userName, userGroups, serviceName, resource);
	
				if(!isAdmin) {
					throw restErrorUtil.createRESTException(HttpServletResponse.SC_UNAUTHORIZED, "", true);
				}
	
				RangerPolicy policy = getExactMatchPolicyForResource(serviceName, resource);
		
				if(policy != null) {
					boolean policyUpdated = false;
	
					// replace all existing privileges for users and groups
					if(grantRequest.getReplaceExistingPermissions()) {
						List<RangerPolicyItem> policyItems = policy.getPolicyItems();
	
						int numOfItems = policyItems.size();
		
						for(int i = 0; i < numOfItems; i++) {
							RangerPolicyItem policyItem = policyItems.get(i);
		
							if(CollectionUtils.containsAny(policyItem.getUsers(), grantRequest.getUsers())) {
								policyItem.getUsers().removeAll(grantRequest.getUsers());
	
								policyUpdated = true;
							}
	
							if(CollectionUtils.containsAny(policyItem.getGroups(), grantRequest.getGroups())) {
								policyItem.getGroups().removeAll(grantRequest.getGroups());
	
								policyUpdated = true;
							}
	
							if(CollectionUtils.isEmpty(policyItem.getUsers()) && CollectionUtils.isEmpty(policyItem.getGroups())) {
								policyItems.remove(i);
								numOfItems--;
								i--;
	
								policyUpdated = true;
							}
						}
	
						if(compactPolicy(policy)) {
							policyUpdated = true;
						}
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
						updatePolicy(policy);
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
		
					createPolicy(policy);
				}
			} catch(WebApplicationException excp) {
				throw excp;
			} catch(Exception excp) {
				LOG.error("grantAccess(" + serviceName + ", " + grantRequest + ") failed", excp);
	
				throw restErrorUtil.createRESTException(HttpServletResponse.SC_BAD_REQUEST, excp.getMessage(), true);
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

		RESTResponse ret = new RESTResponse();

		if (serviceUtil.isValidateHttpsAuthentication(serviceName,request)) {

			try {
				String               userName   = revokeRequest.getGrantor();
				Set<String>          userGroups =  userMgr.getGroupsForUser(userName);
				RangerAccessResource resource   = new RangerAccessResourceImpl(revokeRequest.getResource());
	
				boolean isAdmin = isAdminForResource(userName, userGroups, serviceName, resource);
				
				if(!isAdmin) {
					throw restErrorUtil.createRESTException(HttpServletResponse.SC_UNAUTHORIZED, "", true);
				}
	
				RangerPolicy policy = getExactMatchPolicyForResource(serviceName, resource);
				
				if(policy != null) {
					boolean policyUpdated = false;
	
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
	
					if(policyUpdated) {
						updatePolicy(policy);
					}
				} else {
					// nothing to revoke!
				}
			} catch(WebApplicationException excp) {
				throw excp;
			} catch(Exception excp) {
				LOG.error("revokeAccess(" + serviceName + ", " + revokeRequest + ") failed", excp);
	
				throw restErrorUtil.createRESTException(HttpServletResponse.SC_BAD_REQUEST, excp.getMessage(), true);
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

		RangerPolicy ret = null;
		
		try {
			// RangerPolicyValidator validator = validatorFactory.getPolicyValidator(svcStore);
			// validator.validate(policy, Action.CREATE, bizUtil.isAdmin());

			ensureAdminAccess(policy.getService(), policy.getResources());

			ret = svcStore.createPolicy(policy);
		} catch(Exception excp) {
			LOG.error("createPolicy(" + policy + ") failed", excp);

			if(excp instanceof WebApplicationException) {
				throw (WebApplicationException)excp;
			}

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
			// RangerPolicyValidator validator = validatorFactory.getPolicyValidator(svcStore);
			// validator.validate(policy, Action.UPDATE, bizUtil.isAdmin());

			ensureAdminAccess(policy.getService(), policy.getResources());

			ret = svcStore.updatePolicy(policy);
		} catch(Exception excp) {
			LOG.error("updatePolicy(" + policy + ") failed", excp);

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
			// RangerPolicyValidator validator = validatorFactory.getPolicyValidator(svcStore);
			// validator.validate(id, Action.DELETE);

			RangerPolicy policy = svcStore.getPolicy(id);

			ensureAdminAccess(policy.getService(), policy.getResources());

			svcStore.deletePolicy(id);
		} catch(Exception excp) {
			LOG.error("deletePolicy(" + id + ") failed", excp);

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

			if(ret != null) {
				ensureAdminAccess(ret.getService(), ret.getResources());
			}
		} catch(Exception excp) {
			LOG.error("getPolicy(" + id + ") failed", excp);

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
	public RangerPolicyList getPolicies(@Context HttpServletRequest request) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceREST.getPolicies()");
		}

		RangerPolicyList ret = null;

		SearchFilter filter = searchUtil.getSearchFilter(request, policyService.sortFields);

		try {
			ret = svcStore.getPaginatedPolicies(filter);

			applyAdminAccessFilter(ret);
		} catch (Exception excp) {
			LOG.error("getPolicies() failed", excp);

			throw restErrorUtil.createRESTException(HttpServletResponse.SC_BAD_REQUEST, excp.getMessage(), true);
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

		List<RangerPolicy> ret = null;

		try {
			ret = svcStore.getPolicies(filter);

			applyAdminAccessFilter(ret);
		} catch(Exception excp) {
			LOG.error("getPolicies() failed", excp);

			throw restErrorUtil.createRESTException(HttpServletResponse.SC_BAD_REQUEST, excp.getMessage(), true);
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

		Long ret = null;

		try {
			List<RangerPolicy> policies = getPolicies(request).getPolicies();

			applyAdminAccessFilter(policies);
			
			ret = new Long(policies == null ? 0 : policies.size());
		} catch(Exception excp) {
			LOG.error("countPolicies() failed", excp);

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
	public RangerPolicyList getServicePolicies(@PathParam("id") Long serviceId,
			@Context HttpServletRequest request) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceREST.getServicePolicies(" + serviceId + ")");
		}

		RangerPolicyList ret = null;

		SearchFilter filter = searchUtil.getSearchFilter(request, policyService.sortFields);

		try {
			ret = svcStore.getPaginatedServicePolicies(serviceId, filter);

			applyAdminAccessFilter(ret);
		} catch (Exception excp) {
			LOG.error("getServicePolicies(" + serviceId + ") failed", excp);

			throw restErrorUtil.createRESTException(HttpServletResponse.SC_BAD_REQUEST, excp.getMessage(), true);
		}

		if (ret == null) {
			LOG.info("No Policies found for given service id: " + serviceId);
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceREST.getServicePolicies(" + serviceId + "): count="
					+ ret.getListSize());
		}
		return ret;
	}

	@GET
	@Path("/policies/service/name/{name}")
	@Produces({ "application/json", "application/xml" })
	public RangerPolicyList getServicePolicies(@PathParam("name") String serviceName,
			@Context HttpServletRequest request) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceREST.getServicePolicies(" + serviceName + ")");
		}

		RangerPolicyList ret = null;

		SearchFilter filter = searchUtil.getSearchFilter(request, policyService.sortFields);

		try {
			ret = svcStore.getPaginatedServicePolicies(serviceName, filter);

			applyAdminAccessFilter(ret);
		} catch (Exception excp) {
			LOG.error("getServicePolicies(" + serviceName + ") failed", excp);

			throw restErrorUtil.createRESTException(HttpServletResponse.SC_BAD_REQUEST, excp.getMessage(), true);
		}

		if (ret == null) {
			LOG.info("No Policies found for given service name: " + serviceName);
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceREST.getServicePolicies(" + serviceName + "): count="
					+ ret.getListSize());
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

		if (serviceUtil.isValidateHttpsAuthentication(serviceName, request)) {
			
			try {
				ret = svcStore.getServicePoliciesIfUpdated(serviceName, lastKnownVersion);
	
				if(ret == null) {
					httpCode = HttpServletResponse.SC_NOT_MODIFIED;
					logMsg   = "No change since last update";
				} else {
					httpCode = HttpServletResponse.SC_OK;
					logMsg   = "Returning " + (ret.getPolicies() != null ? ret.getPolicies().size() : 0) + " policies. Policy version=" + ret.getPolicyVersion();
				}
			} catch(Exception excp) {
				LOG.error("getServicePoliciesIfUpdated(" + serviceName + ", " + lastKnownVersion + ") failed", excp);
	
				httpCode = HttpServletResponse.SC_BAD_REQUEST;
				logMsg   = excp.getMessage();
			} finally {
				createPolicyDownloadAudit(serviceName, lastKnownVersion, pluginId, ret, httpCode, request);
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

	private boolean isAdminForResource(String userName, Set<String> userGroups, String serviceName, RangerAccessResource resource) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceREST.isAdminForResource(" + userName + ", " + serviceName + ", " + resource + ")");
		}

		boolean ret = false;
		
		List<RangerPolicy> policies = getServicePolicies(serviceName, null).getPolicies();

		if(!CollectionUtils.isEmpty(policies)) {
			for(RangerPolicy policy : policies) {
				if(!isMatch(policy, resource)) {
					continue;
				}

				if(CollectionUtils.isEmpty(policy.getPolicyItems())) {
					continue;
				}

				for(RangerPolicyItem policyItem : policy.getPolicyItems()) {
					if(! policyItem.getDelegateAdmin()) {
						continue;
					}

					if(policyItem.getUsers().contains(userName) ||
					   policyItem.getGroups().contains(RangerPolicyEngine.GROUP_PUBLIC) ||
					   CollectionUtils.containsAny(policyItem.getGroups(), userGroups)) {
						ret = true;
						break;
					}
				}

				if(ret) {
					break;
				}
			}
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceREST.isAdminForResource(" + userName + ", " + serviceName + ", " + resource + "): " + ret);
		}

		return ret;
	}

	private RangerPolicy getExactMatchPolicyForResource(String serviceName, RangerAccessResource resource) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceREST.getExactMatchPolicyForResource(" + serviceName + ", " + resource + ")");
		}

		RangerPolicy ret = null;

		List<RangerPolicy> policies = getServicePolicies(serviceName, null).getPolicies();

		if(!CollectionUtils.isEmpty(policies)) {
			for(RangerPolicy policy : policies) {
				if(isSingleAndExactMatch(policy, resource)) {
					ret = policy;

					break;
				}
			}
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceREST.getExactMatchPolicyForResource(" + serviceName + ", " + resource + "): " + ret);
		}

		return ret;
	}

	private boolean isMatch(RangerPolicy policy, RangerAccessResource resource) throws Exception {
		boolean ret = false;

		String        serviceName = policy.getService();
		RangerService service     = getServiceByName(serviceName);

		if(service == null) {
			throw new Exception(serviceName + ": service does not exist");
		}

		RangerServiceDef serviceDef = getServiceDefByName(service.getType());

		if(serviceDef == null) {
			throw new Exception(serviceName + ": unknown service-type");
		}

		RangerPolicyEvaluator policyEvaluator = new RangerDefaultPolicyEvaluator();

		policyEvaluator.init(policy, serviceDef);

		ret = policyEvaluator.isMatch(resource);

		return ret;
	}

	private boolean isSingleAndExactMatch(RangerPolicy policy, RangerAccessResource resource) throws Exception {
		boolean ret = false;

		String        serviceName = policy.getService();
		RangerService service     = getServiceByName(serviceName);

		if(service == null) {
			throw new Exception(serviceName + ": service does not exist");
		}

		RangerServiceDef serviceDef = getServiceDefByName(service.getType());

		if(serviceDef == null) {
			throw new Exception(serviceName + ": unknown service-type");
		}

		RangerPolicyEvaluator policyEvaluator = new RangerDefaultPolicyEvaluator();

		policyEvaluator.init(policy, serviceDef);

		ret = policyEvaluator.isSingleAndExactMatch(resource);

		return ret;
	}

	private boolean compactPolicy(RangerPolicy policy) {
		boolean ret = false;

		List<RangerPolicyItem> policyItems = policy.getPolicyItems();

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

	@GET
	@Path("/policies/eventTime")
	@Produces({ "application/json", "application/xml" })
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

		RangerPolicy policy = svcStore.getPolicyFromEventTime(eventTimeStr, policyId);

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceREST.getPolicyFromEventTime()");
		}

		return policy;
	}

	@GET
	@Path("/policy/{policyId}/versionList")
	public VXString getPolicyVersionList(@PathParam("policyId") Long policyId) {
		return svcStore.getPolicyVersionList(policyId);
	}

	@GET
	@Path("/policy/{policyId}/version/{versionNo}")
	@Produces({ "application/json", "application/xml" })
	public RangerPolicy getPolicyForVersionNumber(@PathParam("policyId") Long policyId,
			@PathParam("versionNo") int versionNo) {
		return svcStore.getPolicyForVersionNumber(policyId, versionNo);
	}

	private void applyAdminAccessFilter(RangerPolicyList policies) {
		if(policies != null && !CollectionUtils.isEmpty(policies.getList())) {
			applyAdminAccessFilter(policies.getPolicies());
		}
	}

	private void applyAdminAccessFilter(List<RangerPolicy> policies) {
		boolean isAdmin = bizUtil.isAdmin();

		if(!isAdmin && !CollectionUtils.isEmpty(policies)) {
			String                      userName   = bizUtil.getCurrentUserLoginId();
			Set<String>                 userGroups = userMgr.getGroupsForUser(userName);
			Map<String, RangerPolicyDb> policyDbs  = new HashMap<String, RangerPolicyDb>();

			for(int i = 0; i < policies.size(); i++) {
				RangerPolicy   policy      = policies.get(i);
				String         serviceName = policy.getService();
				RangerPolicyDb policyDb    = policyDbs.get(serviceName);

				if(policyDb == null) {
					policyDb = RangerPolicyDbCache.getInstance().getPolicyDb(policy.getService(), svcStore);

					if(policyDb != null) {
						policyDbs.put(serviceName, policyDb);
					}
				}

				boolean hasAdminAccess = hasAdminAccess(serviceName, policy.getResources(), policyDb, userName, userGroups);

				if(!hasAdminAccess) {
					policies.remove(i);
					i--;
				}
			}
		}
	}
	
	private void ensureAdminAccess(String serviceName, Map<String, RangerPolicyResource> resources) {
		boolean isAdmin = bizUtil.isAdmin();

		if(!isAdmin) {
			RangerPolicyDb policyDb   = RangerPolicyDbCache.getInstance().getPolicyDb(serviceName, svcStore);
			String         userName   = bizUtil.getCurrentUserLoginId();
			Set<String>    userGroups = userMgr.getGroupsForUser(userName);

			boolean isAllowed = hasAdminAccess(serviceName, resources, policyDb, userName, userGroups);

			if(!isAllowed) {
				throw restErrorUtil.createRESTException(HttpServletResponse.SC_UNAUTHORIZED,
						"User '" + userName + "' does not have delegated-admin privilege on given resources", true);
			}
		}
	}

	private boolean hasAdminAccess(String serviceName, Map<String, RangerPolicyResource> resources, RangerPolicyDb policyDb, String userName, Set<String> userGroups) {
		boolean isAllowed = false;

		if(policyDb != null) {
			isAllowed = policyDb.isAccessAllowed(resources, userName, userGroups, RangerPolicyEngine.ADMIN_ACCESS);
		}

		return isAllowed;
	}

}
