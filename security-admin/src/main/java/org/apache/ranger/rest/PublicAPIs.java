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

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;

import org.apache.log4j.Logger;
import org.apache.ranger.biz.AssetMgr;
import org.apache.ranger.biz.ServiceDBStore;
import org.apache.ranger.common.RESTErrorUtil;
import org.apache.ranger.common.RangerConstants;
import org.apache.ranger.common.RangerSearchUtil;
import org.apache.ranger.common.SearchCriteria;
import org.apache.ranger.common.ServiceUtil;
import org.apache.ranger.common.StringUtil;
import org.apache.ranger.common.annotation.RangerAnnotationClassName;
import org.apache.ranger.common.annotation.RangerAnnotationJSMgrName;
import org.apache.ranger.db.RangerDaoManager;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerService;
import org.apache.ranger.plugin.util.SearchFilter;
import org.apache.ranger.service.RangerPolicyService;
import org.apache.ranger.service.XAssetService;
import org.apache.ranger.service.XPolicyService;
import org.apache.ranger.service.XRepositoryService;
import org.apache.ranger.service.XResourceService;
import org.apache.ranger.view.VXAsset;
import org.apache.ranger.view.VXLong;
import org.apache.ranger.view.VXPolicy;
import org.apache.ranger.view.VXPolicyList;
import org.apache.ranger.view.VXRepository;
import org.apache.ranger.view.VXRepositoryList;
import org.apache.ranger.view.VXResource;
import org.apache.ranger.view.VXResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

@Path("public")
@Component
@Scope("request")
@RangerAnnotationJSMgrName("PublicMgr")
@Transactional(propagation = Propagation.REQUIRES_NEW)
public class PublicAPIs {
	static Logger logger = Logger.getLogger(PublicAPIs.class);

	@Autowired
	RangerSearchUtil searchUtil;

	@Autowired
	AssetMgr assetMgr;

	@Autowired
	XAssetService xAssetService;

	@Autowired
	RESTErrorUtil restErrorUtil;

	@Autowired
	XRepositoryService xRepositoryService;

	@Autowired
	XResourceService xResourceService;

	@Autowired
	XPolicyService xPolicyService;

	@Autowired
	RangerPolicyService policyService;

	@Autowired
	StringUtil stringUtil;

	@Autowired
	RangerDaoManager xaDaoMgr;
	
	@Autowired
	ServiceUtil serviceUtil;
	
	@Autowired
	ServiceREST serviceREST;

	@Autowired
	AssetREST assetREST;

	@Autowired
	ServiceDBStore svcStore;
	
	
	@GET
	@Path("/api/repository/{id}")
	@Produces({ "application/json", "application/xml" })
	public VXRepository getRepository(@PathParam("id") Long id) {
		if(logger.isDebugEnabled()) {
			logger.debug("==> PublicAPIs.getRepository(" + id + ")");
		}
		
		RangerService service = serviceREST.getService(id);
		
		VXRepository ret = serviceUtil.toVXRepository(service);

		if(logger.isDebugEnabled()) {
			logger.debug("<= PublicAPIs.getRepository(" + id + ")");
		}
		return ret;
	}
	
	
	@POST
	@Path("/api/repository/")
	@Produces({ "application/json", "application/xml" })
	public VXRepository createRepository(VXRepository vXRepository) {
		if(logger.isDebugEnabled()) {
			logger.debug("==> PublicAPIs.createRepository(" + vXRepository + ")");
		}
		
		VXAsset vXAsset  = serviceUtil.publicObjecttoVXAsset(vXRepository);
		
		RangerService service = serviceUtil.toRangerService(vXAsset);

		RangerService createdService = serviceREST.createService(service);
		
		VXAsset retvXAsset = serviceUtil.toVXAsset(createdService);
		
		VXRepository ret = serviceUtil.vXAssetToPublicObject(retvXAsset);
		
		if(logger.isDebugEnabled()) {
			logger.debug("<== PublicAPIs.createRepository(" + ret + ")");
		}
		
		return ret;
	}

	
	@PUT
	@Path("/api/repository/{id}")
	@Produces({ "application/json", "application/xml" })
	public VXRepository updateRepository(VXRepository vXRepository,
			@PathParam("id") Long id) {
		
		if(logger.isDebugEnabled()) {
			logger.debug("==> PublicAPIs.updateRepository(" + id + ")");
		}
		
		vXRepository.setId(id);
		
		VXAsset vXAsset  = serviceUtil.publicObjecttoVXAsset(vXRepository);

		RangerService service = serviceUtil.toRangerService(vXAsset);

		RangerService updatedService = serviceREST.updateService(service);
		
		VXAsset retvXAsset = serviceUtil.toVXAsset(updatedService);
		
		VXRepository ret = serviceUtil.vXAssetToPublicObject(retvXAsset);
		
		if(logger.isDebugEnabled()) {
			logger.debug("<== PublicAPIs.updateRepository(" + ret + ")");
		}
		
		return ret;
	}
	


	
	@DELETE
	@Path("/api/repository/{id}")
	@PreAuthorize("hasRole('ROLE_SYS_ADMIN')")
	@RangerAnnotationClassName(class_name = VXAsset.class)
	public void deleteRepository(@PathParam("id") Long id,
			@Context HttpServletRequest request) {

		if(logger.isDebugEnabled()) {
			logger.debug("==> PublicAPIs.deleteRepository(" + id + ")");
		}
		
		String forceStr = request.getParameter("force");
		boolean force = true;
		if (!stringUtil.isEmpty(forceStr)) {
			force = Boolean.parseBoolean(forceStr.trim());
		}
				
		serviceREST.deleteService(id);
		
		if(logger.isDebugEnabled()) {
			logger.debug("<== PublicAPIs.deleteRepository(" + id + ")");
		}
	}
	

	/*// @POST
	// @Path("/api/repository/testConfig")
	// @Produces({ "application/xml", "application/json" })
	public VXResponse testConfig(VXRepository vXRepository) {
		VXAsset vXAsset = xRepositoryService.mapPublicToXAObject(vXRepository);
		return assetMgr.testConfig(vXAsset);
	}*/
	
	
	// @POST
	// @Path("/api/repository/testConfig")
	// @Produces({ "application/xml", "application/json" })
	public VXResponse testConfig(VXRepository vXRepository) {
		VXAsset vXAsset = serviceUtil.publicObjecttoVXAsset(vXRepository);
		return serviceREST.validateConfig(serviceUtil.toRangerService(vXAsset));
	}
	
	
	
	@GET
	@Path("/api/repository/")
	@Produces({ "application/json", "application/xml" })
	public VXRepositoryList searchRepositories(
			@Context HttpServletRequest request) {
	
		if(logger.isDebugEnabled()) {
			logger.debug("==> PublicAPIs.searchRepositories()");
		}
		
		SearchCriteria searchCriteria = searchUtil.extractCommonCriterias(
				request, xAssetService.sortFields);
		searchUtil.extractString(request, searchCriteria, "name",
				"Repository Name", null);
		searchUtil.extractBoolean(request, searchCriteria, "status",
				"Activation Status");
		searchUtil.extractString(request, searchCriteria, "type",
				"Repository Type", null);

		searchCriteria = serviceUtil.getMappedSearchParams(request,
				searchCriteria);
		List<RangerService> serviceList = serviceREST.getServices(request);

		VXRepositoryList ret = null;

		if (serviceList != null) {
			ret = serviceUtil.rangerServiceListToPublicObjectList(serviceList);
		}
		if(logger.isDebugEnabled()) {
			logger.debug("<== PublicAPIs.searchRepositories(): count=" + (ret == null ? 0 : ret.getListSize()));
		}
			
		return ret;
	}

	
	@GET
	@Path("/api/repository/count")
	@Produces({ "application/json", "application/xml" })
	public VXLong countRepositories(@Context HttpServletRequest request) {
		SearchCriteria searchCriteria = searchUtil.extractCommonCriterias(
				request, xAssetService.sortFields);
	
		if(logger.isDebugEnabled()) {
			logger.debug("==> PublicAPIs.countRepositories()");
		}
		
        ArrayList<Integer> valueList = new ArrayList<Integer>();
        valueList.add(RangerConstants.STATUS_DISABLED);
        valueList.add(RangerConstants.STATUS_ENABLED);
        searchCriteria.addParam("status", valueList);
        
        VXLong ret = new VXLong();
        
        ret.setValue(serviceREST.countServices(request));
		
        if(logger.isDebugEnabled()) {
			logger.debug("<== PublicAPIs.countRepositories(): count=" + ret);
		}
        
        return ret;
	}	
	

	
	@GET
	@Path("/api/policy/{id}")
	@Produces({ "application/json", "application/xml" })
	public VXPolicy getPolicy(@PathParam("id") Long id) {
		
		if(logger.isDebugEnabled()) {
			logger.debug("==> PublicAPIs.getPolicy() " + id);
		}
		
		RangerPolicy  policy  = null;
		RangerService service = null;

		policy = serviceREST.getPolicy(id);
		
		if(policy != null) {
			service = serviceREST.getServiceByName(policy.getService());
		}

		VXPolicy ret = serviceUtil.toVXPolicy(policy, service);
	
		if(logger.isDebugEnabled()) {
			logger.debug("<== PublicAPIs.getPolicy()" + ret);
		}
		
		return ret;
	}
	

	@POST
	@Path("/api/policy")
	@Produces({ "application/json", "application/xml" })
	public VXPolicy createPolicy(VXPolicy vXPolicy) {
		
		if(logger.isDebugEnabled()) {
			logger.debug("==> PublicAPIs.createPolicy()");
		}
		
		RangerService service       = serviceREST.getServiceByName(vXPolicy.getRepositoryName());
		
		RangerPolicy  policy        = serviceUtil.toRangerPolicy(vXPolicy,service);

		if(logger.isDebugEnabled()) {
			logger.debug("RANGERPOLICY: " + policy.toString());
		}
		
		RangerPolicy  createdPolicy = serviceREST.createPolicy(policy);

		VXPolicy ret = serviceUtil.toVXPolicy(createdPolicy, service);
		
		if(logger.isDebugEnabled()) {
			logger.debug("<== PublicAPIs.createPolicy(" + policy + "): " + ret);
		}
	
		return ret;
	}

	@PUT
	@Path("/api/policy/{id}")
	@Produces({ "application/json", "application/xml" })
	public VXPolicy updatePolicy(VXPolicy vXPolicy, @PathParam("id") Long id) {
		
		if(logger.isDebugEnabled()) {
			logger.debug("==> PublicAPIs.updatePolicy(): "  + vXPolicy );
		}
		
		vXPolicy.setId(id);
		
		RangerService service       = serviceREST.getServiceByName(vXPolicy.getRepositoryName());
		
		RangerPolicy  policy        = serviceUtil.toRangerPolicy(vXPolicy,service);

		RangerPolicy  updatedPolicy = serviceREST.createPolicy(policy);

		VXPolicy ret = serviceUtil.toVXPolicy(updatedPolicy, service);

		if(logger.isDebugEnabled()) {
			logger.debug("<== AssetREST.updatePolicy(" + policy + "): " + ret);
		}
	
		return ret;
	}

	@DELETE
	@Path("/api/policy/{id}")
	@PreAuthorize("hasRole('ROLE_SYS_ADMIN')")
	@RangerAnnotationClassName(class_name = VXResource.class)
	public void deletePolicy(@PathParam("id") Long id,
			@Context HttpServletRequest request) {
		
		if(logger.isDebugEnabled()) {
			logger.debug("==> PublicAPIs.deletePolicy(): "  + id );
		}
		
		serviceREST.deletePolicy(id);
		
		if(logger.isDebugEnabled()) {
			logger.debug("<== PublicAPIs.deletePolicy(): "  + id );
		}
	}

	@GET
	@Path("/api/policy")
	@Produces({ "application/json", "application/xml" })
	public VXPolicyList searchPolicies(@Context HttpServletRequest request) {
		
		if(logger.isDebugEnabled()) {
			logger.debug("==> PublicAPIs.searchPolicies(): ");
		}

		SearchFilter filter = searchUtil.getSearchFilterFromLegacyRequest(request, policyService.sortFields);

		List<RangerPolicy> rangerPolicyList = serviceREST.getPolicies(filter);
		
		VXPolicyList vXPolicyList = null;

		if (rangerPolicyList != null) {
			vXPolicyList = serviceUtil.rangerPolicyListToPublic(rangerPolicyList);
		}
		
		if(logger.isDebugEnabled()) {
			logger.debug("<== PublicAPIs.searchPolicies(): "  + vXPolicyList );
		}
		
		return vXPolicyList;
	}

	@GET
	@Path("/api/policy/count")
	@Produces({ "application/xml", "application/json" })
	public VXLong countPolicies(@Context HttpServletRequest request) {
		
		if(logger.isDebugEnabled()) {
			logger.debug("==> PublicAPIs.countPolicies(): ");
		}
		
		Long policyCount = serviceREST.countPolicies(request);
		
		VXLong vXlong = new VXLong();
		vXlong.setValue(policyCount);
		
		if(logger.isDebugEnabled()) {
			logger.debug("<== PublicAPIs.countPolicies(): "  + request );
		}
		
		return vXlong;
	}

}
