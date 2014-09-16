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

 package com.xasecure.rest;

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
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.xasecure.biz.AssetMgr;
import com.xasecure.common.AppConstants;
import com.xasecure.common.MessageEnums;
import com.xasecure.common.RESTErrorUtil;
import com.xasecure.common.SearchCriteria;
import com.xasecure.common.StringUtil;
import com.xasecure.common.XAConstants;
import com.xasecure.common.XASearchUtil;
import com.xasecure.common.annotation.XAAnnotationClassName;
import com.xasecure.common.annotation.XAAnnotationJSMgrName;
import com.xasecure.db.XADaoManager;
import com.xasecure.service.AbstractBaseResourceService;
import com.xasecure.service.XPolicyService;
import com.xasecure.service.XRepositoryService;
import com.xasecure.service.XAssetService;
import com.xasecure.service.XResourceService;
import com.xasecure.view.VXAsset;
import com.xasecure.view.VXAssetList;
import com.xasecure.view.VXLong;
import com.xasecure.view.VXPolicy;
import com.xasecure.view.VXPolicyList;
import com.xasecure.view.VXRepository;
import com.xasecure.view.VXRepositoryList;
import com.xasecure.view.VXResource;
import com.xasecure.view.VXResourceList;
import com.xasecure.view.VXResponse;

@Path("public")
@Component
@Scope("request")
@XAAnnotationJSMgrName("PublicMgr")
@Transactional(propagation = Propagation.REQUIRES_NEW)
public class PublicAPIs {
	static Logger logger = Logger.getLogger(PublicAPIs.class);

	@Autowired
	XASearchUtil searchUtil;

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
	StringUtil stringUtil;

	@Autowired
	XADaoManager xaDaoMgr;

	@GET
	@Path("/api/repository/{id}")
	@Produces({ "application/xml", "application/json" })
	public VXRepository getRepository(@PathParam("id") Long id) {
		VXAsset vXAsset = assetMgr.getXAsset(id);
		return xRepositoryService.mapXAToPublicObject(vXAsset);
	}

	@POST
	@Path("/api/repository/")
	@Produces({ "application/xml", "application/json" })
	public VXRepository createRepository(VXRepository vXRepository) {
		VXAsset vXAsset = xRepositoryService.mapPublicToXAObject(vXRepository);
		vXAsset = assetMgr.createXAsset(vXAsset);
		return xRepositoryService.mapXAToPublicObject(vXAsset);
	}

	@PUT
	@Path("/api/repository/{id}")
	@Produces({ "application/xml", "application/json" })
	public VXRepository updateRepository(VXRepository vXRepository,
			@PathParam("id") Long id) {
		vXRepository.setId(id);
		VXAsset vXAsset = xRepositoryService.mapPublicToXAObject(vXRepository);
		vXAsset = assetMgr.updateXAsset(vXAsset);
		return xRepositoryService.mapXAToPublicObject(vXAsset);
	}

	@DELETE
	@Path("/api/repository/{id}")
	@PreAuthorize("hasRole('ROLE_SYS_ADMIN')")
	@XAAnnotationClassName(class_name = VXAsset.class)
	public void deleteRepository(@PathParam("id") Long id,
			@Context HttpServletRequest request) {

		String forceStr = request.getParameter("force");
		boolean force = true;
		if (!stringUtil.isEmpty(forceStr)) {
			force = Boolean.parseBoolean(forceStr.trim());
		}
		assetMgr.deleteXAsset(id, force);
	}

	// @POST
	// @Path("/api/repository/testConfig")
	// @Produces({ "application/xml", "application/json" })
	public VXResponse testConfig(VXRepository vXRepository) {
		VXAsset vXAsset = xRepositoryService.mapPublicToXAObject(vXRepository);
		return assetMgr.testConfig(vXAsset);
	}

	@GET
	@Path("/api/repository/")
	@Produces({ "application/xml", "application/json" })
	public VXRepositoryList searchRepositories(
			@Context HttpServletRequest request) {
		SearchCriteria searchCriteria = searchUtil.extractCommonCriterias(
				request, xAssetService.sortFields);
		searchUtil.extractString(request, searchCriteria, "name",
				"Repository Name", null);
		searchUtil.extractBoolean(request, searchCriteria, "status",
				"Activation Status");
		searchUtil.extractString(request, searchCriteria, "type",
				"Repository Type", null);

		searchCriteria = xRepositoryService.getMappedSearchParams(request,
				searchCriteria);
		VXAssetList vXAssetList = assetMgr.searchXAssets(searchCriteria);

		return xRepositoryService.mapToVXRepositoryList(vXAssetList);
	}

	@GET
	@Path("/api/repository/count")
	@Produces({ "application/xml", "application/json" })
	public VXLong countRepositories(@Context HttpServletRequest request) {
		SearchCriteria searchCriteria = searchUtil.extractCommonCriterias(
				request, xAssetService.sortFields);
		return assetMgr.getXAssetSearchCount(searchCriteria);
	}

	@GET
	@Path("/api/policy/{id}")
	@Produces({ "application/xml", "application/json" })
	public VXPolicy getPolicy(@PathParam("id") Long id) {
		VXResource vXResource = assetMgr.getXResource(id);
		return xPolicyService.mapXAToPublicObject(vXResource);
	}

	@POST
	@Path("/api/policy")
	@Produces({ "application/xml", "application/json" })
	public VXPolicy createPolicy(VXPolicy vXPolicy) {
		VXResource vXResource = xPolicyService.mapPublicToXAObject(vXPolicy,
				AbstractBaseResourceService.OPERATION_CREATE_CONTEXT);
		vXResource = assetMgr.createXResource(vXResource);
		vXResource.setPermMapList(xPolicyService.updatePermGroup(vXResource));
		return xPolicyService.mapXAToPublicObject(vXResource);

	}

	@PUT
	@Path("/api/policy/{id}")
	@Produces({ "application/xml", "application/json" })
	public VXPolicy updatePolicy(VXPolicy vXPolicy, @PathParam("id") Long id) {
		vXPolicy.setId(id);
		VXResource vXResource = xPolicyService.mapPublicToXAObject(vXPolicy,
				AbstractBaseResourceService.OPERATION_UPDATE_CONTEXT);
		vXResource = assetMgr.updateXResource(vXResource);
		vXResource.setPermMapList(xPolicyService.updatePermGroup(vXResource));
		return xPolicyService.mapXAToPublicObject(vXResource);
	}

	@DELETE
	@Path("/api/policy/{id}")
	@PreAuthorize("hasRole('ROLE_SYS_ADMIN')")
	@XAAnnotationClassName(class_name = VXResource.class)
	public void deletePolicy(@PathParam("id") Long id,
			@Context HttpServletRequest request) {
		String forceStr = request.getParameter("force");
		boolean force = true;
		if (!stringUtil.isEmpty(forceStr)) {
			force = Boolean.parseBoolean(forceStr.trim());
		}
		assetMgr.deleteXResource(id, force);
	}

	@GET
	@Path("/api/policy")
	@Produces({ "application/xml", "application/json" })
	public VXPolicyList searchPolicies(@Context HttpServletRequest request) {
		SearchCriteria searchCriteria = searchUtil.extractCommonCriterias(
				request, xResourceService.sortFields);

		String resourceName = request.getParameter("resourceName");
		if (!stringUtil.isEmpty(resourceName)) {
			searchCriteria.getParamList().put("name", resourceName);
		}
		searchUtil.extractString(request, searchCriteria, "policyName",
				"Policy name", StringUtil.VALIDATION_TEXT);
		searchUtil.extractString(request, searchCriteria, "columns",
				"Column name", StringUtil.VALIDATION_TEXT);
		searchUtil.extractString(request, searchCriteria, "columnFamilies",
				"Column Family", StringUtil.VALIDATION_TEXT);
		searchUtil.extractString(request, searchCriteria, "tables", "Tables",
				StringUtil.VALIDATION_TEXT);
		searchUtil.extractString(request, searchCriteria, "udfs", "UDFs",
				StringUtil.VALIDATION_TEXT);
		searchUtil.extractString(request, searchCriteria, "databases",
				"Databases", StringUtil.VALIDATION_TEXT);
		searchUtil.extractString(request, searchCriteria, "groupName",
				"Group Name", StringUtil.VALIDATION_TEXT);

		String repositoryType = request.getParameter("repositoryType");
		if (!stringUtil.isEmpty(repositoryType)) {
			searchCriteria.getParamList().put("assetType",
					AppConstants.getEnumFor_AssetType(repositoryType));
		}

		String isRec = request.getParameter("isRecursive");
		if (isRec != null) {
			boolean isRecursiveBool = restErrorUtil.parseBoolean(isRec,
					"Invalid value for " + "isRecursive",
					MessageEnums.INVALID_INPUT_DATA, null, "isRecursive");
			int isRecursive = (isRecursiveBool == true) ? XAConstants.BOOL_TRUE
					: XAConstants.BOOL_FALSE;
			searchCriteria.getParamList().put("isRecursive", isRecursive);
		}
			
		searchUtil.extractString(request, searchCriteria, "userName",
				"User Name", StringUtil.VALIDATION_TEXT);
		searchUtil.extractString(request, searchCriteria, "repositoryName",
				"Repository Name", StringUtil.VALIDATION_TEXT);
		searchCriteria.setDistinct(true);

		VXResourceList vXResourceList = assetMgr
				.searchXResources(searchCriteria);
		return xPolicyService.mapToVXPolicyList(vXResourceList);
	}

	@GET
	@Path("/api/policy/count")
	@Produces({ "application/xml", "application/json" })
	public VXLong countPolicies(@Context HttpServletRequest request) {
		SearchCriteria searchCriteria = searchUtil.extractCommonCriterias(
				request, xResourceService.sortFields);

		return assetMgr.getXResourceSearchCount(searchCriteria);
	}

}
