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

 package org.apache.ranger.biz;

import java.io.File;
import java.io.IOException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import javax.naming.InvalidNameException;
import javax.naming.ldap.LdapName;
import javax.naming.ldap.Rdn;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.apache.ranger.common.AppConstants;
import org.apache.ranger.common.ContextUtil;
import org.apache.ranger.common.DateUtil;
import org.apache.ranger.common.JSONUtil;
import org.apache.ranger.common.MessageEnums;
import org.apache.ranger.common.PropertiesUtil;
import org.apache.ranger.common.RangerCommonEnums;
import org.apache.ranger.common.RangerConstants;
import org.apache.ranger.common.SearchCriteria;
import org.apache.ranger.common.StringUtil;
import org.apache.ranger.common.UserSessionBase;
import org.apache.ranger.db.RangerDaoManager;
import org.apache.ranger.entity.XXAsset;
import org.apache.ranger.entity.XXGroup;
import org.apache.ranger.entity.XXPermMap;
import org.apache.ranger.entity.XXPolicyExportAudit;
import org.apache.ranger.entity.XXPortalUser;
import org.apache.ranger.entity.XXPortalUserRole;
import org.apache.ranger.entity.XXResource;
import org.apache.ranger.entity.XXTrxLog;
import org.apache.ranger.entity.XXUser;
import org.apache.ranger.service.XAccessAuditService;
import org.apache.ranger.service.XAuditMapService;
import org.apache.ranger.service.XGroupService;
import org.apache.ranger.service.XPermMapService;
import org.apache.ranger.service.XPolicyService;
import org.apache.ranger.service.XTrxLogService;
import org.apache.ranger.service.XUserService;
import org.apache.ranger.solr.SolrAccessAuditsService;
import org.apache.ranger.util.RestUtil;
import org.apache.ranger.view.*;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionCallback;
import org.springframework.transaction.support.TransactionTemplate;

@Component
public class AssetMgr extends AssetMgrBase {
	
	
	@Autowired
	XPermMapService xPermMapService;
	
	@Autowired
	XAuditMapService xAuditMapService;

	@Autowired
	JSONUtil jsonUtil;

	@Autowired
	RangerBizUtil msBizUtil;

	@Autowired
	StringUtil stringUtil;

	@Autowired
	RangerDaoManager rangerDaoManager;

	@Autowired
	XUserService xUserService;

	@Autowired
	RangerBizUtil xaBizUtil;

	@Autowired
	XTrxLogService xTrxLogService;

	@Autowired
	XAccessAuditService xAccessAuditService;

	@Autowired
	XGroupService xGroupService;
	
	@Autowired
	XUserMgr xUserMgr;

	@Autowired
	SolrAccessAuditsService solrAccessAuditsService;

	@Autowired
	@Qualifier(value = "transactionManager")
	PlatformTransactionManager txManager;
	
	@Autowired
	XPolicyService xPolicyService;
	
	static Logger logger = Logger.getLogger(AssetMgr.class);

	@Override
	public VXResource createXResource(VXResource vXResource) {
		UserSessionBase session = ContextUtil.getCurrentUserSession();
		if (session == null) {
			logger.error("Trying to create/update policy without log-in.");
			throw restErrorUtil.create403RESTException("Resource "
					+ "creation/updation not allowed");
		}
			
		Long assetId = vXResource.getAssetId();
		XXAsset xAsset = rangerDaoManager.getXXAsset().getById(assetId);
		if (xAsset == null) {
			logger.error("Repository not found for assetId : " + assetId);
			throw restErrorUtil.create403RESTException("Repository for which"
					+ " the policy is created, doesn't exist.");
		}		
		
		if(xAsset.getActiveStatus()==RangerCommonEnums.ACT_STATUS_DISABLED){
			if(!session.isUserAdmin()){
				logger.error("Trying to create/update policy in disabled repository");
				throw restErrorUtil.createRESTException("Resource "
						+ "creation/updation not allowed in disabled repository",MessageEnums.OPER_NO_PERMISSION);
			}
		}
		// Create resource path for HIVE/Hbase policy.
		if (xAsset.getAssetType() == AppConstants.ASSET_HIVE) {
			createResourcePathForHive(vXResource);
		} else if (xAsset.getAssetType() == AppConstants.ASSET_HBASE) {
			createResourcePathForHbase(vXResource);
		}else if (xAsset.getAssetType() == AppConstants.ASSET_KNOX) {
			createResourcePathForKnox(vXResource);
		}else if (xAsset.getAssetType() == AppConstants.ASSET_STORM) {
			createResourcePathForStorm(vXResource);
		}
		
		String resourceName = vXResource.getName();
		String[] orgResNameList = stringUtil.split(resourceName, ",");
		List<String> newResNameList = new ArrayList<String>();
		for(String resName : orgResNameList) {
			if(resName.length() > 1 && (resName.substring(resName.length()-1).equalsIgnoreCase("/"))) {
				resName = resName.substring(0, resName.length()-1);
				newResNameList.add(resName);
				logger.info("Resource Name is not valid : " +resName + " Ignoring last /");
			} else {
				newResNameList.add(resName);
			}
		}
		String updResName = StringUtils.join(newResNameList, ",");
		vXResource.setName(updResName);
		
		SearchCriteria searchCriteria=new SearchCriteria();
		searchCriteria.getParamList().put("assetId", vXResource.getAssetId());
		searchCriteria.getParamList().put("fullname", vXResource.getName());
		
		if (xAsset.getAssetType() == AppConstants.ASSET_HIVE) {
			if(stringUtil.isEmpty(vXResource.getUdfs())) {
				searchCriteria.addParam("tableType", vXResource.getTableType());
				searchCriteria.addParam("columnType", vXResource.getColumnType());
			} else {
				searchCriteria.addParam("udfs", vXResource.getUdfs());
			}
		} else if (xAsset.getAssetType() == AppConstants.ASSET_HDFS) {
			searchCriteria.addParam("isRecursive", vXResource.getIsRecursive());
		}
		
		VXResourceList vXResourceList=xResourceService.searchXResources(searchCriteria);		
		if(vXResourceList!=null && vXResourceList.getListSize()>0){
			logger.error("policy already exist with resource "+vXResource.getName());
			throw restErrorUtil.createRESTException("policy already exist with resource "
					+vXResource.getName(),MessageEnums.ERROR_DUPLICATE_OBJECT);
		}
		
		VXResponse vXResponse = xaBizUtil.hasPermission(vXResource,
				AppConstants.XA_PERM_TYPE_ADMIN);
		if (vXResponse.getStatusCode() == VXResponse.STATUS_ERROR) {
			throw restErrorUtil.createRESTException(vXResponse);
		}

		if (vXResource.getCheckParentPermission() != RangerConstants.BOOL_FALSE) {
			// check parent access for user and group
			xResourceService.checkAccess(vXResource);
		}
		vXResourceList=null;
		if(vXResource.getPolicyName()!=null && !vXResource.getPolicyName().trim().isEmpty()){			
			searchCriteria=new SearchCriteria();		
			searchCriteria.getParamList().put("fullPolicyName", vXResource.getPolicyName());
			vXResourceList=xResourceService.searchXResourcesWithoutLogin(searchCriteria);
			//if policyname already exist then set null to generate from system
			if(vXResourceList!=null && vXResourceList.getListSize()>0){
				logger.error("policy already exist with name "+vXResource.getPolicyName());
				//logger.info("A system generated policy name shall be assigned to "+vXResource.getPolicyName());
				//vXResource.setPolicyName(null);
				throw restErrorUtil.createRESTException("policy already exist with name "
						+vXResource.getPolicyName(),MessageEnums.ERROR_DUPLICATE_OBJECT);
			}
		}
		
		int tempPoliciesCount=0;
		String tempPolicyName=null;
		vXResourceList=null;
		if(vXResource.getPolicyName()==null ||vXResource.getPolicyName().trim().isEmpty()){
			searchCriteria=new SearchCriteria();
			searchCriteria.getParamList().put("assetId", vXResource.getAssetId());
			vXResourceList=xResourceService.searchXResourcesWithoutLogin(searchCriteria);			
			if(vXResourceList!=null && vXResourceList.getListSize()>0){
				tempPoliciesCount=vXResourceList.getListSize();
			}	
			vXResourceList=null;
			while(true){
				tempPoliciesCount++;
				tempPolicyName=xAsset.getName()+"-"+tempPoliciesCount+"-"+DateUtil.dateToString(DateUtil.getUTCDate(),"yyyyMMddHHmmss");
				vXResource.setPolicyName(tempPolicyName);
				searchCriteria=new SearchCriteria();		
				searchCriteria.getParamList().put("policyName", vXResource.getPolicyName());
				vXResourceList=xResourceService.searchXResourcesWithoutLogin(searchCriteria);
				//if policy name not exist then list will be empty and generated policyname will valid 
				if(vXResourceList==null|| vXResourceList.getListSize()==0){
					break;
				}
			}
		}
		
		
		vXResource = xResourceService.createResource(vXResource);
		
		List<VXPermMap> permMapList = vXResource.getPermMapList();
		List<VXAuditMap> auditMapList = vXResource.getAuditList();

		List<XXTrxLog> trxLogList = xResourceService.getTransactionLog(
				vXResource, "create");
		for (VXPermMap vXPermMap : permMapList) {
			trxLogList.addAll(xPermMapService.getTransactionLog(vXPermMap,
					"create"));
		}
		for (VXAuditMap vXAuditMap : auditMapList) {
			trxLogList.addAll(xAuditMapService.getTransactionLog(vXAuditMap,
					"create"));
		}

		xaBizUtil.createTrxLog(trxLogList);
		
		return vXResource;
	}

	@Override
	public VXResource updateXResource(VXResource vXResource) {

		UserSessionBase currentUserSession = ContextUtil
				.getCurrentUserSession();
		if (currentUserSession == null) {
			throw restErrorUtil.createRESTException("Resource updation not "
					+ "allowed",MessageEnums.OPER_NO_PERMISSION);
		}

		if (vXResource == null) {
			return null;
		}

		Long assetId = vXResource.getAssetId();
		XXAsset xAsset = rangerDaoManager.getXXAsset().getById(assetId);
		if (xAsset == null) {
			throw restErrorUtil.createRESTException("The repository for which "
					+ "you're updating policy, doesn't exist.",
					MessageEnums.INVALID_INPUT_DATA);
		}
		
		if(xAsset.getActiveStatus()==RangerCommonEnums.STATUS_DISABLED){
			if(!currentUserSession.isUserAdmin()){
				logger.error("Trying to update policy in disabled repository");
				throw restErrorUtil.createRESTException("Resource "
						+ "updation not allowed in disabled repository",MessageEnums.OPER_NO_PERMISSION);
			}
		}

		if (xAsset.getAssetType() == AppConstants.ASSET_HIVE) {
			createResourcePathForHive(vXResource);
		} else if (xAsset.getAssetType() == AppConstants.ASSET_HBASE) {
			createResourcePathForHbase(vXResource);
		}else if (xAsset.getAssetType() == AppConstants.ASSET_KNOX) {
			createResourcePathForKnox(vXResource);
		}else if (xAsset.getAssetType() == AppConstants.ASSET_STORM) {
			createResourcePathForStorm(vXResource);
		}
		
		SearchCriteria searchCriteria = new SearchCriteria();
		searchCriteria.getParamList().put("assetId", vXResource.getAssetId());
		searchCriteria.getParamList().put("fullname", vXResource.getName());
		
		if (xAsset.getAssetType() == AppConstants.ASSET_HIVE) {
			if(stringUtil.isEmpty(vXResource.getUdfs())) {
				searchCriteria.addParam("tableType", vXResource.getTableType());
				searchCriteria.addParam("columnType", vXResource.getColumnType());
			} else {
				searchCriteria.addParam("udfs", vXResource.getUdfs());
			}
		} else if (xAsset.getAssetType() == AppConstants.ASSET_HDFS) {
			searchCriteria.addParam("isRecursive", vXResource.getIsRecursive());
		}
		
		VXResourceList vXResourceList=xResourceService.searchXResourcesWithoutLogin(searchCriteria);		
		if(vXResourceList!=null && vXResourceList.getListSize()>0){
			if(vXResource!=null && vXResource.getId()!=null){
				for(VXResource vXResourceTemp :vXResourceList.getList()){
					if(vXResourceTemp!=null && !(vXResource.getId().equals(vXResourceTemp.getId()))){
							logger.error("policy already exists with name "+vXResource.getName());
							throw restErrorUtil.createRESTException("policy already exists with name "
									+vXResource.getName(),MessageEnums.ERROR_DUPLICATE_OBJECT);					
						}
				}
			}
		}	
		
		VXResponse vXResponse = xaBizUtil.hasPermission(vXResource,
				AppConstants.XA_PERM_TYPE_ADMIN);
		if (vXResponse.getStatusCode() == VXResponse.STATUS_ERROR) {
			throw restErrorUtil.createRESTException(vXResponse);
		}

		if (vXResource.getCheckParentPermission() != RangerConstants.BOOL_FALSE) {
			// check parent access for user and group
			xResourceService.checkAccess(vXResource);
		}
		
		//policyName creation and validation logic start here
		if(vXResource.getPolicyName()!=null && !vXResource.getPolicyName().trim().isEmpty()){ 				
			searchCriteria=new SearchCriteria();		
			searchCriteria.getParamList().put("fullPolicyName", vXResource.getPolicyName());
			vXResourceList=xResourceService.searchXResourcesWithoutLogin(searchCriteria);	
			if(vXResourceList!=null && vXResourceList.getListSize()>0){
				if(vXResource!=null && vXResource.getId()!=null){
					for (VXResource newVXResource : vXResourceList.getList()) {
						if(newVXResource!=null && newVXResource.getId()!=null){
							if(!vXResource.getId().equals(newVXResource.getId()) && vXResource.getPolicyName().trim().equalsIgnoreCase((newVXResource.getPolicyName()!=null?newVXResource.getPolicyName().trim():newVXResource.getPolicyName()))){
								logger.error("policy already exists with name "+vXResource.getPolicyName());
//								logger.info("A system generated policy name shall be assigned to "+vXResource.getPolicyName());
//								vXResource.setPolicyName(null);
//								break;
								throw restErrorUtil.createRESTException("policy already exists with name "
										+vXResource.getPolicyName(),MessageEnums.ERROR_DUPLICATE_OBJECT);
							}
						}
					}
				}
			}
		}
		
		int tempPoliciesCount=0;
		int totalPoliciesCount=1;
		String tempPolicyName=null;
		vXResourceList=null;
		if(vXResource.getPolicyName()==null || vXResource.getPolicyName().trim().isEmpty()){
			searchCriteria=new SearchCriteria();
			searchCriteria.getParamList().put("assetId", vXResource.getAssetId());
			vXResourceList=xResourceService.searchXResourcesWithoutLogin(searchCriteria);
			if(vXResourceList!=null && vXResourceList.getListSize()>0){
				totalPoliciesCount=vXResourceList.getListSize();
				tempPoliciesCount++;
				for(VXResource newVXResource : vXResourceList.getList()) {	
					if(newVXResource!=null && newVXResource.getId()!=null){
						if(vXResource.getId().equals(newVXResource.getId())){					
							break;
						}
					}
					tempPoliciesCount++;
				}
				tempPolicyName=xAsset.getName()+"-"+tempPoliciesCount+"-"+DateUtil.dateToString(DateUtil.getUTCDate(),"yyyyMMddHHmmss");
				vXResource.setPolicyName(tempPolicyName);	
				vXResourceList=null;
			}else{
				tempPoliciesCount++;
				tempPolicyName=xAsset.getName()+"-"+tempPoliciesCount+"-"+DateUtil.dateToString(DateUtil.getUTCDate(),"yyyyMMddHHmmss");
				vXResource.setPolicyName(tempPolicyName);				
			}
			while(true){
				searchCriteria=new SearchCriteria();		
				searchCriteria.getParamList().put("policyName", vXResource.getPolicyName());
				vXResourceList=xResourceService.searchXResourcesWithoutLogin(searchCriteria);				
				if(vXResourceList==null || vXResourceList.getListSize()==0){
					break;
				}else{
					tempPolicyName=xAsset.getName()+"-"+totalPoliciesCount+"-"+DateUtil.dateToString(DateUtil.getUTCDate(),"yyyyMMddHHmmss");
					vXResource.setPolicyName(tempPolicyName);					
				}				
			}
			
		}
		
		//policyName creation and validation logic end here
	
		XXResource xResouce = rangerDaoManager.getXXResource().getById(
				vXResource.getId());
		
		List<XXTrxLog> trxLogList = xResourceService.getTransactionLog(
				vXResource, xResouce, "update");

		VXResource resource = super.updateXResource(vXResource);

		List<VXPermMap> newPermMapList = vXResource.getPermMapList();
		searchCriteria = new SearchCriteria();
		searchCriteria.addParam("resourceId", vXResource.getId());
		VXPermMapList prevPermMaps = xPermMapService
				.searchXPermMaps(searchCriteria);
		List<VXPermMap> prevPermMapList = new ArrayList<VXPermMap>();
		if (prevPermMaps != null) {
			prevPermMapList = prevPermMaps.getVXPermMaps();
		}

		List<VXPermMap> permMaps = new ArrayList<VXPermMap>();
		List<VXAuditMap> auditMaps = new ArrayList<VXAuditMap>();

		List<VXPermMap> permMapsToDelete = new ArrayList<VXPermMap>();
		List<VXAuditMap> auditMapsToDelete = new ArrayList<VXAuditMap>();

		// Create and update permissions
		if (newPermMapList != null) {
			for (VXPermMap newObj : newPermMapList) {
				if (newObj.getUserId() == null && newObj.getGroupId() == null
						&& !stringUtil.isEmpty(newObj.getUserName())) {
					XXUser xxUser = rangerDaoManager.getXXUser().findByUserName(
							newObj.getUserName());
					if (xxUser != null) {
						newObj.setUserId(xxUser.getId());
					} else {
						VXUser vxUser = new VXUser();
						vxUser.setName(newObj.getUserName());
						vxUser.setDescription(newObj.getUserName());
						vxUser = xUserService.createResource(vxUser);
						newObj.setUserId(vxUser.getId());
					}
				}
				newObj.setResourceId(resource.getId());
				if (newObj.getId() != null) {
					/**
					 * Considering the new objects won't have id however the
					 * existing ones will have id.
					 */
					for (VXPermMap oldObj : prevPermMapList) {
						if (oldObj.getId().equals(newObj.getId())) {
							if (oldObj.getPermType() != newObj.getPermType()) {
								// This should never be the case because we're
								// not supporting perm map update.
								// doNothing...
								logger.error(
										"Invalid use case: shouldn't be happening, need to debug.",
										new IllegalStateException());
								break;
							} else {
								xPermMapService.updateResource(newObj);
								trxLogList.addAll(xPermMapService
										.getTransactionLog(newObj, oldObj,
												"update"));
							}
						}
					}
					// newObj = xPermMapService.updateResource(newObj);
				} else {
					newObj = xPermMapService.createResource(newObj);
					trxLogList.addAll(xPermMapService.getTransactionLog(newObj,
							"create"));
				}
				permMaps.add(newObj);
			}
		}

		// Delete old removed permissions
		if (prevPermMapList != null) {
			for (VXPermMap oldObj : prevPermMapList) {
				boolean found = false;
				if (newPermMapList != null) {
					for (VXPermMap newObj : newPermMapList) {
						if (newObj.getId() != null
								&& newObj.getId().equals(oldObj.getId())) {
							found = true;
							break;
						}
					}
				}
				if (!found) {
					trxLogList.addAll(xPermMapService.getTransactionLog(oldObj,
							"delete"));
					permMapsToDelete.add(oldObj);
					// xPermMapService.deleteResource(oldObj.getId());
				}
			}
		}

		List<VXAuditMap> newAuditMapList = vXResource.getAuditList();
		VXAuditMapList vXAuditMaps = xAuditMapService
				.searchXAuditMaps(searchCriteria);
		List<VXAuditMap> prevAuditMapList = new ArrayList<VXAuditMap>();
		if (vXAuditMaps != null && vXAuditMaps.getResultSize() != 0) {
			prevAuditMapList = vXAuditMaps.getList();
		}

		// Create and update permissions
		if (newAuditMapList != null) {
			for (VXAuditMap newObj : newAuditMapList) {
				if (newObj.getId() != null) {
					/**
					 * Considering the new objects won't have id however the
					 * existing ones will have id.
					 */
					for (VXAuditMap oldObj : prevAuditMapList) {
						if (oldObj.getId().equals(newObj.getId())
								&& oldObj.getAuditType() != newObj
										.getAuditType()) {
							// This should never be the case because we're not
							// supporting perm map update.
							// doNothing...
							logger.error(
									"Invalid use case: shouldn't be happening, need to debug.",
									new IllegalStateException());
							break;
							// } else {
							// trxLogList.addAll(xAuditMapService.getTransactionLog(newObj,
							// oldObj, "update"));
						}
					}
					// newObj = xAuditMapService.updateResource(newObj);
				} else {
					newObj = xAuditMapService.createResource(newObj);
					trxLogList.addAll(xAuditMapService.getTransactionLog(
							newObj, "create"));
				}
				auditMaps.add(newObj);
			}
		}

		// Delete old removed permissions
		if (prevAuditMapList != null) {
			for (VXAuditMap oldObj : prevAuditMapList) {
				boolean found = false;
				if (newAuditMapList != null) {
					for (VXAuditMap newObj : newAuditMapList) {
						if (newObj.getId() != null
								&& newObj.getId().equals(oldObj.getId())) {
							found = true;
							break;
						}
					}
				}

				if (!found) {
					trxLogList.addAll(xAuditMapService.getTransactionLog(
							oldObj, "delete"));
					auditMapsToDelete.add(oldObj);
					// xAuditMapService.deleteResource(oldObj.getId());
				}
			}
		}

		xaBizUtil.createTrxLog(trxLogList);

		for (VXPermMap permMap : permMapsToDelete) {
			xPermMapService.deleteResource(permMap.getId());
		}

		for (VXAuditMap auditMap : auditMapsToDelete) {
			xAuditMapService.deleteResource(auditMap.getId());
		}
		resource.setPermMapList(permMaps);
		resource.setAuditList(auditMaps);
		return resource;
	}

	@Override
	public void deleteXResource(Long id, boolean force) {
		
		VXResource vResource = xResourceService.readResource(id);
		UserSessionBase currentUserSession = ContextUtil
				.getCurrentUserSession();
		if (currentUserSession == null) {
			throw restErrorUtil.createRESTException("Resource deletion not "
					+ "allowed",MessageEnums.OPER_NO_PERMISSION);
		}

		Long assetId = vResource.getAssetId();
		XXAsset xAsset = rangerDaoManager.getXXAsset().getById(assetId);
		if (xAsset == null) {
			throw restErrorUtil.createRESTException("The repository for which "
					+ "you're deleting policy, doesn't exist.",
					MessageEnums.INVALID_INPUT_DATA);
		}

		if(xAsset.getActiveStatus()==RangerCommonEnums.STATUS_DISABLED){
			if(!currentUserSession.isUserAdmin()){
				logger.error("Trying to delete policy in disabled repository");
				throw restErrorUtil.createRESTException("Resource "
						+ "deletion not allowed in disabled repository",MessageEnums.OPER_NO_PERMISSION);
			}
		}
		
		SearchCriteria searchCriteria = new SearchCriteria();
		searchCriteria.addParam("resourceId", id);

		VXPermMapList permMaps = xPermMapService
				.searchXPermMaps(searchCriteria);
		VXAuditMapList vXAuditMapsList = xAuditMapService
				.searchXAuditMaps(searchCriteria);

		List<XXTrxLog> trxLogList = xResourceService.getTransactionLog(
				vResource, "delete");

		for (VXPermMap vxPermMap : permMaps.getVXPermMaps()) {
			trxLogList.addAll(xPermMapService.getTransactionLog(vxPermMap,
					"delete"));
		}

		for (VXAuditMap vXAuditMaps : vXAuditMapsList.getVXAuditMaps()) {
			trxLogList.addAll(xAuditMapService.getTransactionLog(vXAuditMaps,
					"delete"));
		}

		xaBizUtil.createTrxLog(trxLogList);

		for (VXPermMap vxPermMap : permMaps.getVXPermMaps()) {
			xPermMapService.deleteResource(vxPermMap.getId());
		}

		for (VXAuditMap vXAuditMaps : vXAuditMapsList.getVXAuditMaps()) {
			xAuditMapService.deleteResource(vXAuditMaps.getId());
		}

		xResourceService.deleteResource(id);
	}

	public File getXResourceFile(Long id, String fileType) {
		VXResource xResource = xResourceService.readResource(id);
		if (xResource == null) {
			throw this.restErrorUtil.createRESTException(
					"serverMsg.datasourceIdEmpty" + "id " + id,
					MessageEnums.DATA_NOT_FOUND, id, "dataSourceId",
					"DataSource not found with " + "id " + id);
		}
		
		return getXResourceFile(xResource, fileType);
	}

	public File getXResourceFile(VXResource xResource, String fileType) {
		File file = null;
		try {
			if (fileType != null) {
				if (fileType.equalsIgnoreCase("json")) {
					file = jsonUtil.writeJsonToFile(xResource,
							xResource.getName());
				} else {
					throw restErrorUtil.createRESTException(
							"Please send the supported filetype.",
							MessageEnums.INVALID_INPUT_DATA);
				}
			} else {
				throw restErrorUtil
						.createRESTException(
								"Please send the file format in which you want to export.",
								MessageEnums.DATA_NOT_FOUND);
			}
		} catch (JsonGenerationException e) {
			throw this.restErrorUtil.createRESTException(
					"serverMsg.jsonGeneration" + " : " + e.getMessage(),
					MessageEnums.ERROR_SYSTEM);
		} catch (JsonMappingException e) {
			throw this.restErrorUtil.createRESTException(
					"serverMsg.jsonMapping" + " : " + e.getMessage(),
					MessageEnums.ERROR_SYSTEM);
		} catch (IOException e) {
			throw this.restErrorUtil.createRESTException(
					"serverMsg.ioException" + " : " + e.getMessage(),
					MessageEnums.ERROR_SYSTEM);
		}

		return file;
	}

	public String getLatestRepoPolicy(VXAsset xAsset, List<VXResource> xResourceList, Long updatedTime,
			X509Certificate[] certchain, boolean httpEnabled, String epoch,
			String ipAddress, boolean isSecure, String count, String agentId) {
		if(xAsset==null){
			logger.error("Requested repository not found");
			throw restErrorUtil.createRESTException("No Data Found.",
					MessageEnums.DATA_NOT_FOUND);
		}
		if (xResourceList == null) {
			logger.error("ResourceList is found");
			throw restErrorUtil.createRESTException("No Data Found.",
					MessageEnums.DATA_NOT_FOUND);
		}
		if(xAsset.getActiveStatus()==RangerCommonEnums.ACT_STATUS_DISABLED){
			logger.error("Requested repository is disabled");
			throw restErrorUtil.createRESTException("Unauthorized access.",
					MessageEnums.OPER_NO_EXPORT);
		}

		HashMap<String, Object> updatedRepo = new HashMap<String, Object>();
		updatedRepo.put("repository_name", xAsset.getName());
		
		XXPolicyExportAudit policyExportAudit = new XXPolicyExportAudit();
		policyExportAudit.setRepositoryName(xAsset.getName());

		if (agentId != null && !agentId.isEmpty()) {
			policyExportAudit.setAgentId(agentId);
		}

		policyExportAudit.setClientIP(ipAddress);

		if (epoch != null && !epoch.trim().isEmpty() && !epoch.equalsIgnoreCase("null")) {
			policyExportAudit.setRequestedEpoch(Long.parseLong(epoch));
		} else {
			policyExportAudit.setRequestedEpoch(0l);
		}

		if (!httpEnabled) {
			if (!isSecure) {
				policyExportAudit
						.setHttpRetCode(HttpServletResponse.SC_BAD_REQUEST);
				createPolicyAudit(policyExportAudit);

				throw restErrorUtil.createRESTException("Unauthorized access -"
						+ " only https allowed",
						MessageEnums.OPER_NOT_ALLOWED_FOR_ENTITY);
			}

			if (certchain == null || certchain.length == 0) {

				policyExportAudit
						.setHttpRetCode(HttpServletResponse.SC_BAD_REQUEST);
				createPolicyAudit(policyExportAudit);

				throw restErrorUtil.createRESTException("Unauthorized access -"
						+ " unable to get client certificate",
						MessageEnums.OPER_NOT_ALLOWED_FOR_ENTITY);
			}
		}

		Long policyCount = restErrorUtil.parseLong(count, "Invalid value for "
				+ "policyCount", MessageEnums.INVALID_INPUT_DATA, null,
				"policyCount");

		String commonName = null;

		if (certchain != null) {
			X509Certificate clientCert = certchain[0];
			String dn = clientCert.getSubjectX500Principal().getName();

			try {
				LdapName ln = new LdapName(dn);
				for (Rdn rdn : ln.getRdns()) {
					if (rdn.getType().equalsIgnoreCase("CN")) {
						commonName = rdn.getValue() + "";
						break;
					}
				}
				if (commonName == null) {
					policyExportAudit
							.setHttpRetCode(javax.servlet.http.HttpServletResponse.SC_BAD_REQUEST);
					createPolicyAudit(policyExportAudit);

					throw restErrorUtil.createRESTException(
							"Unauthorized access - Unable to find Common Name from ["
									+ dn + "]",
							MessageEnums.OPER_NOT_ALLOWED_FOR_ENTITY);
				}
			} catch (InvalidNameException e) {
				policyExportAudit
						.setHttpRetCode(javax.servlet.http.HttpServletResponse.SC_BAD_REQUEST);
				createPolicyAudit(policyExportAudit);

				logger.error("Invalid Common Name.", e);
				throw restErrorUtil.createRESTException(
						"Unauthorized access - Invalid Common Name",
						MessageEnums.OPER_NOT_ALLOWED_FOR_ENTITY);
			}
		}

		if (policyCount == null) {
			policyCount = 0l;
		}

		if (commonName != null) {
			String config = xAsset.getConfig();
			Map<String, String> configMap = jsonUtil.jsonToMap(config);
			String cnFromConfig = configMap.get("commonNameForCertificate");

			if (cnFromConfig == null
					|| !commonName.equalsIgnoreCase(cnFromConfig)) {
				policyExportAudit
						.setHttpRetCode(javax.servlet.http.HttpServletResponse.SC_BAD_REQUEST);
				createPolicyAudit(policyExportAudit);

				throw restErrorUtil.createRESTException(
						"Unauthorized access. expected [" + cnFromConfig
								+ "], found [" + commonName + "]",
						MessageEnums.OPER_NOT_ALLOWED_FOR_ENTITY);
			}
		}

		long epochTime = epoch != null ? Long.parseLong(epoch) : 0;

		if(epochTime == updatedTime) {
			int resourceListSz = xResourceList.size() ;
			
			if (policyCount == resourceListSz) {
				policyExportAudit
						.setHttpRetCode(javax.servlet.http.HttpServletResponse.SC_NOT_MODIFIED);
				createPolicyAudit(policyExportAudit);

				throw restErrorUtil.createRESTException(
						HttpServletResponse.SC_NOT_MODIFIED,
						"No change since last update", false);
			}
		}

		List<HashMap<String, Object>> resourceList = new ArrayList<HashMap<String, Object>>();

		// HDFS Repository
		if (xAsset.getAssetType() == AppConstants.ASSET_HDFS) {
			for (VXResource xResource : xResourceList) {
				HashMap<String, Object> resourceMap = new HashMap<String, Object>();
				resourceMap.put("id", xResource.getId());
				resourceMap.put("resource", xResource.getName());
				resourceMap.put("isRecursive",
						getBooleanValue(xResource.getIsRecursive()));
				resourceMap.put("policyStatus", RangerCommonEnums
						.getLabelFor_ActiveStatus(xResource
								.getResourceStatus()));
				// resourceMap.put("isEncrypt",
				// AKAConstants.getLabelFor_BooleanValue(xResource.getIsEncrypt()));
				populatePermMap(xResource, resourceMap, AppConstants.ASSET_HDFS);
				List<VXAuditMap> xAuditMaps = xResource.getAuditList();
				if (xAuditMaps.size() != 0) {
					resourceMap.put("audit", 1);
				} else {
					resourceMap.put("audit", 0);
				}

				resourceList.add(resourceMap);
			}
		} else if (xAsset.getAssetType() == AppConstants.ASSET_HIVE) {
			for (VXResource xResource : xResourceList) {
				HashMap<String, Object> resourceMap = new HashMap<String, Object>();
				resourceMap.put("id", xResource.getId());
				resourceMap.put("database_name", xResource.getDatabases());
				resourceMap.put("policyStatus", RangerCommonEnums
						.getLabelFor_ActiveStatus(xResource
								.getResourceStatus()));
				resourceMap.put("tablePolicyType", AppConstants
						.getLabelFor_PolicyType(xResource.getTableType()));
				resourceMap.put("columnPolicyType", AppConstants
						.getLabelFor_PolicyType(xResource.getColumnType()));
				int resourceType = xResource.getResourceType();
				if (resourceType == AppConstants.RESOURCE_UDF) {
					resourceMap.put("udf_name", xResource.getUdfs());
				} else if (resourceType == AppConstants.RESOURCE_COLUMN) {
					resourceMap.put("table_name", xResource.getTables());
					resourceMap.put("column_name", xResource.getColumns());
				} else if (resourceType == AppConstants.RESOURCE_TABLE) {
					resourceMap.put("table_name", xResource.getTables());
				}

				populatePermMap(xResource, resourceMap, AppConstants.ASSET_HIVE);
				
				List<VXAuditMap> xAuditMaps = xResource.getAuditList();
				if (xAuditMaps.size() != 0) {
					resourceMap.put("audit", 1);
				} else {
					resourceMap.put("audit", 0);
				}
				resourceList.add(resourceMap);
			}
		}

		else if (xAsset.getAssetType() == AppConstants.ASSET_HBASE) {
			for (VXResource xResource : xResourceList) {
				HashMap<String, Object> resourceMap = new HashMap<String, Object>();

				resourceMap.put("id", xResource.getId());
				resourceMap.put("table_name", xResource.getTables());
				resourceMap.put("column_name", xResource.getColumns());
				resourceMap.put("column_families",
						xResource.getColumnFamilies());
				resourceMap.put("policyStatus", RangerCommonEnums
						.getLabelFor_ActiveStatus(xResource
								.getResourceStatus()));
				if (xResource.getIsEncrypt() == 1) {
					resourceMap.put("encrypt", 1);
				} else {
					resourceMap.put("encrypt", 0);
				}
				// resourceMap.put("isEncrypt",
				// AKAConstants.getLabelFor_BooleanValue(xResource.getIsEncrypt()));
				populatePermMap(xResource, resourceMap, AppConstants.ASSET_HBASE);
				List<VXAuditMap> xAuditMaps = xResource.getAuditList();
				if (xAuditMaps.size() != 0) {
					resourceMap.put("audit", 1);
				} else {
					resourceMap.put("audit", 0);
				}
				resourceList.add(resourceMap);
			}
		}
		else if (xAsset.getAssetType() == AppConstants.ASSET_KNOX) {
			for (VXResource xResource : xResourceList) {
				HashMap<String, Object> resourceMap = new HashMap<String, Object>();

				resourceMap.put("id", xResource.getId());
				resourceMap.put("topology_name", xResource.getTopologies()) ;
				resourceMap.put("service_name", xResource.getServices()) ;
				resourceMap.put("policyStatus", RangerCommonEnums
						.getLabelFor_ActiveStatus(xResource
								.getResourceStatus()));
				if (xResource.getIsEncrypt() == 1) {
					resourceMap.put("encrypt", 1);
				} else {
					resourceMap.put("encrypt", 0);
				}
				// resourceMap.put("isEncrypt",
				// AKAConstants.getLabelFor_BooleanValue(xResource.getIsEncrypt()));
				populatePermMap(xResource, resourceMap, AppConstants.ASSET_KNOX);
				List<VXAuditMap> xAuditMaps = xResource.getAuditList();
				if (xAuditMaps.size() != 0) {
					resourceMap.put("audit", 1);
				} else {
					resourceMap.put("audit", 0);
				}
				resourceList.add(resourceMap);
			}
			
        }
        else if (xAsset.getAssetType() == AppConstants.ASSET_STORM) {
                for (VXResource xResource : xResourceList) {
                        HashMap<String, Object> resourceMap = new HashMap<String, Object>();

                        resourceMap.put("id", xResource.getId());
                        resourceMap.put("topology_name", xResource.getTopologies()) ;
                        resourceMap.put("policyStatus", RangerCommonEnums
                                        .getLabelFor_ActiveStatus(xResource
                                                        .getResourceStatus()));
                        if (xResource.getIsEncrypt() == 1) {
                                resourceMap.put("encrypt", 1);
                        } else {
                                resourceMap.put("encrypt", 0);
                        }
                        populatePermMap(xResource, resourceMap, AppConstants.ASSET_STORM);
                        List<VXAuditMap> xAuditMaps = xResource.getAuditList();
                        if (xAuditMaps.size() != 0) {
                                resourceMap.put("audit", 1);
                        } else {
                                resourceMap.put("audit", 0);
                        }
                        resourceList.add(resourceMap);
                }
		} else {
			policyExportAudit
					.setHttpRetCode(javax.servlet.http.HttpServletResponse.SC_BAD_REQUEST);
			createPolicyAudit(policyExportAudit);
			throw restErrorUtil.createRESTException(
					"The operation isn't yet supported for the repository",
					MessageEnums.OPER_NOT_ALLOWED_FOR_ENTITY);
		}

		policyCount = Long.valueOf(resourceList.size());
		updatedRepo.put("last_updated", updatedTime);
		updatedRepo.put("policyCount", policyCount);
		updatedRepo.put("acl", resourceList);
		
		String updatedPolicyStr = jsonUtil.readMapToString(updatedRepo);

//		File file = null;
//		try {
//			file = jsonUtil.writeMapToFile(updatedRepo, repository);
//		} catch (JsonGenerationException e) {
//			logger.error("Error exporting policies for repository : "
//					+ repository, e);
//		} catch (JsonMappingException e) {
//			logger.error("Error exporting policies for repository : "
//					+ repository, e);
//		} catch (IOException e) {
//			logger.error("Error exporting policies for repository : "
//					+ repository, e);
//		}

		policyExportAudit
				.setHttpRetCode(javax.servlet.http.HttpServletResponse.SC_OK);
		createPolicyAudit(policyExportAudit);

		return updatedPolicyStr;
	}

	@Override
	public VXAsset createXAsset(VXAsset vXAsset) {
		UserSessionBase usb = ContextUtil.getCurrentUserSession();
		if (usb != null && usb.isUserAdmin()) {

			String defaultConfig = vXAsset.getConfig();
			defaultConfig=xAssetService.getConfigWithEncryptedPassword(defaultConfig,false);
			vXAsset.setConfig(defaultConfig);
			VXAsset createdVXAsset = (VXAsset) xAssetService
					.createResource(vXAsset);
			String udpatedConfig = vXAsset.getConfig();
			createdVXAsset.setConfig(defaultConfig);

			createDefaultPolicy(createdVXAsset, vXAsset.getConfig());
			createDefaultUDFPolicy(createdVXAsset, vXAsset.getConfig());
			createdVXAsset.setConfig(udpatedConfig);

			List<XXTrxLog> trxLogList = xAssetService.getTransactionLog(
					createdVXAsset, "create");
			xaBizUtil.createTrxLog(trxLogList);
			return createdVXAsset;
		} else {
                logger.debug("User id : " + (usb != null ? usb.getUserId() : "<UNKNOWN>") + " doesn't have "
                        + "admin access to create repository.");

                throw restErrorUtil
                        .createRESTException(
                                "Sorry, you don't have permission to perform the operation",
                                MessageEnums.OPER_NOT_ALLOWED_FOR_ENTITY);

        }
	}

	private void createDefaultPolicy(VXAsset vXAsset, String config) {
		int assetType = vXAsset.getAssetType();

		Map<String, String> configMap = jsonUtil.jsonToMap(config);
		String userName = stringUtil.getValidUserName(configMap.get("username"));

		VXResource vXResource = new VXResource();
		vXResource.setAssetId(vXAsset.getId());
		vXResource.setAssetName(vXAsset.getName());
		vXResource.setResourceStatus(AppConstants.STATUS_ENABLED);
		String tempPolicyName=vXAsset.getName()+"-"+1+"-"+DateUtil.dateToString(DateUtil.getUTCDate(),"yyyyMMddHHmmss");
		vXResource.setPolicyName(tempPolicyName);
		if (assetType == AppConstants.ASSET_HDFS) {
			vXResource.setName("/");
			vXResource.setIsRecursive(AppConstants.BOOL_TRUE);
			vXResource.setResourceType(AppConstants.RESOURCE_PATH);
		} else if (assetType == AppConstants.ASSET_HIVE) {
			vXResource.setDatabases("*");
			vXResource.setTables("*");
			vXResource.setColumns("*");
			vXResource.setName("/*/*/*");	
			vXResource.setResourceType(AppConstants.RESOURCE_COLUMN);
		} else if (assetType == AppConstants.ASSET_HBASE) {
			vXResource.setTables("*");
			vXResource.setColumnFamilies("*");
			vXResource.setColumns("*");
			vXResource.setName("/*/*/*");
			vXResource.setResourceType(AppConstants.RESOURCE_COLUMN);
		} else if (assetType == AppConstants.ASSET_KNOX) {
			vXResource.setTopologies("*");
			vXResource.setServices("*");
			vXResource.setName("/*/*");
			vXResource.setResourceType(AppConstants.RESOURCE_SERVICE_NAME);
		} else if (assetType == AppConstants.ASSET_STORM) {
			vXResource.setTopologies("*");
			vXResource.setName("/*");
			vXResource.setResourceType(AppConstants.RESOURCE_TOPOLOGY);
		}

		vXResource = xResourceService.createResource(vXResource);

		if (userName != null && !userName.isEmpty()) {
			XXUser xxUser = rangerDaoManager.getXXUser().findByUserName(userName);
			VXUser vXUser;
			if (xxUser != null) {
				vXUser = xUserService.populateViewBean(xxUser);
			} else {
				vXUser = new VXUser();
				vXUser.setName(userName);
				vXUser.setUserSource(RangerCommonEnums.USER_EXTERNAL);
				vXUser=xUserMgr.createXUser(vXUser);
				//vXUser = xUserService.createResource(vXUser);
			}
			
			Random rand = new Random();
			String permGrp = new Date() + " : " + rand.nextInt(9999);
			
			VXPermMap vXPermMap = new VXPermMap();
			vXPermMap.setUserId(vXUser.getId());
			vXPermMap.setResourceId(vXResource.getId());
			vXPermMap.setPermGroup(permGrp);
			xPermMapService.createResource(vXPermMap);
			
			if (assetType == AppConstants.ASSET_KNOX) {
				String permGroup = new Date() + " : " + rand.nextInt(9999);

				VXPermMap permAdmin = new VXPermMap();
				permAdmin.setPermFor(AppConstants.XA_PERM_FOR_USER);
				permAdmin.setPermType(AppConstants.XA_PERM_TYPE_ADMIN);
				permAdmin.setUserId(vXUser.getId());
				permAdmin.setPermGroup(permGroup);
				permAdmin.setResourceId(vXResource.getId());
				xPermMapService.createResource(permAdmin);

				VXPermMap permAllow = new VXPermMap();
				permAllow.setPermFor(AppConstants.XA_PERM_FOR_USER);
				permAllow.setPermType(AppConstants.XA_PERM_TYPE_ALLOW);
				permAllow.setUserId(vXUser.getId());
				permAllow.setPermGroup(permGroup);
				permAllow.setResourceId(vXResource.getId());
				xPermMapService.createResource(permAllow);
			}
		}
		
		VXAuditMap vXAuditMap = new VXAuditMap();
		vXAuditMap.setAuditType(AppConstants.XA_AUDIT_TYPE_ALL);
		vXAuditMap.setResourceId(vXResource.getId());
		vXAuditMap = xAuditMapService.createResource(vXAuditMap);
		vXResource=xResourceService.readResource(vXResource.getId());
		List<VXPermMap> permMapList = vXResource.getPermMapList();
		List<VXAuditMap> auditMapList = vXResource.getAuditList();

		List<XXTrxLog> trxLogList = xResourceService.getTransactionLog(
				vXResource, "create");
		for (VXPermMap vXPermMap : permMapList) {
			trxLogList.addAll(xPermMapService.getTransactionLog(vXPermMap,
					"create"));
		}
		for (VXAuditMap vXAuditMapObj : auditMapList) {
			trxLogList.addAll(xAuditMapService.getTransactionLog(vXAuditMapObj,
					"create"));
		}

		xaBizUtil.createTrxLog(trxLogList);


	}

	@Override
	public VXAsset updateXAsset(VXAsset vXAsset) {
		UserSessionBase usb = ContextUtil.getCurrentUserSession();
		if (usb != null && usb.isUserAdmin()) {
			String newConfig=vXAsset.getConfig();
			HashMap<String, String> configMap = (HashMap<String, String>) jsonUtil
					.jsonToMap(newConfig);
			String password = configMap.get("password");
			String hiddenPasswordString = PropertiesUtil.getProperty("ranger.password.hidden", "*****");
			if (password != null && !password.equals(hiddenPasswordString)) {
				String defaultConfig = vXAsset.getConfig();
				defaultConfig=xAssetService.getConfigWithEncryptedPassword(defaultConfig,true);
				vXAsset.setConfig(defaultConfig);
			}
			XXAsset xAsset = rangerDaoManager.getXXAsset()
					.getById(vXAsset.getId());
			
			if (xAsset.getActiveStatus() == RangerCommonEnums.STATUS_DELETED) {
				logger.error("Trying to update Asset which is soft deleted");
				throw restErrorUtil.createRESTException(
						"Repository that you want to update does not exist.",
						MessageEnums.DATA_NOT_FOUND, xAsset.getId(), null,
						"Repository not exist for this Id : " + xAsset.getId());
			}
			
			List<XXTrxLog> trxLogList = xAssetService.getTransactionLog(
					vXAsset, xAsset, "update");
			vXAsset = (VXAsset) xAssetService.updateResource(vXAsset);
			// update default policy permission and user
			updateDefaultPolicy(vXAsset, vXAsset.getConfig());
			// TODO : Log in transaction log table
			xaBizUtil.createTrxLog(trxLogList);
			return vXAsset;
		} else {
			throw restErrorUtil.createRESTException(
					"serverMsg.modelMgrBaseUpdateModel",
					MessageEnums.OPER_NOT_ALLOWED_FOR_ENTITY);
		}
	}

	@Override
	public void deleteXAsset(Long id, boolean force) {
		UserSessionBase usb = ContextUtil.getCurrentUserSession();
		if (usb != null && usb.isUserAdmin() && force) {
			VXAsset vxAsset = xAssetService.readResource(id);
			
			if (vxAsset.getActiveStatus() == RangerCommonEnums.STATUS_DELETED) {
				logger.error("Trying to delete Asset which is already soft deleted");
				throw restErrorUtil.createRESTException(
						"Repository not found or its already deleted, for Id : "
								+ id, MessageEnums.DATA_NOT_FOUND, id, null,
						"Repository not exist for this Id : " + id);
			}
			
			SearchCriteria searchCriteria = new SearchCriteria();
			searchCriteria.addParam("assetId", id);
			VXResourceList resources = searchXResources(searchCriteria);
			if (resources != null && resources.getResultSize() != 0) {
				for (VXResource resource : resources.getList()) {
					deleteXResource(resource.getId(), true);
				}
			}
			vxAsset.setActiveStatus(RangerCommonEnums.STATUS_DELETED);
			xAssetService.updateResource(vxAsset);
			List<XXTrxLog> trxLogList = xAssetService.getTransactionLog(
					vxAsset, "delete");
			xaBizUtil.createTrxLog(trxLogList);
		} else {
			throw restErrorUtil.createRESTException(
					"serverMsg.modelMgrBaseDeleteModel",
					MessageEnums.OPER_NOT_ALLOWED_FOR_ENTITY);
		}
	}

	private void createResourcePathForHive(VXResource vXResource) {

		String[] databases = (vXResource.getDatabases() == null || vXResource
				.getDatabases().equalsIgnoreCase("")) ? null : stringUtil
				.split(vXResource.getDatabases(), ",");
		String[] tables = (vXResource.getTables() == null || vXResource
				.getTables().equalsIgnoreCase("")) ? new String[0] : stringUtil.split(
				vXResource.getTables(), ",");
		String[] udfs = (vXResource.getUdfs() == null || vXResource.getUdfs()
				.equalsIgnoreCase("")) ? new String[0] : stringUtil.split(
				vXResource.getUdfs(), ",");
		String[] columns = (vXResource.getColumns() == null || vXResource
				.getColumns().equalsIgnoreCase("")) ? new String[0] : stringUtil.split(
				vXResource.getColumns(), ",");

		StringBuilder stringBuilder = new StringBuilder();

//		int resourceType = vXResource.getResourceType();
		int resourceType = xPolicyService.getResourceType(vXResource);

		if (databases == null) {
			logger.error("Invalid resources for hive policy.");
			throw restErrorUtil.createRESTException("Please provide the"
					+ " valid resources.", MessageEnums.INVALID_INPUT_DATA);
		}

		switch (resourceType) {

		case AppConstants.RESOURCE_COLUMN:
			for (String column : columns) {
				for (String table : tables) {
					for (String database : databases) {
						stringBuilder.append("/" + database + "/" + table + "/"
								+ column + ",");
					}
				}
			}
			break;

		case AppConstants.RESOURCE_TABLE:
			for (String table : tables) {
				for (String database : databases) {
					stringBuilder.append("/" + database + "/" + table + ",");
				}
			}
			break;

		case AppConstants.RESOURCE_UDF:
			for (String udf : udfs) {
				for (String database : databases) {
					stringBuilder.append("/" + database + "/" + udf + ",");
				}
			}

			break;

		case AppConstants.RESOURCE_DB:
			for (String database : databases) {
				stringBuilder.append("/" + database + ",");
			}
			break;

		default:
			logger.error("Invalid resource type : " + resourceType
					+ " for hive policy.");
			throw restErrorUtil.createRESTException("Please provide the"
					+ " valid resource type.", MessageEnums.INVALID_INPUT_DATA);
		}

		int lastIndexOfSeperator = stringBuilder.lastIndexOf(",");
		if (lastIndexOfSeperator > 0) {
			String name = stringBuilder.substring(0,
					stringBuilder.lastIndexOf(","));
			vXResource.setName(name);
		} else {
			vXResource.setName(stringBuilder.toString());
		}
	}

	private void createResourcePathForHbase(VXResource vXResource) {

		String[] tables = (vXResource.getTables() == null || vXResource
				.getTables().equalsIgnoreCase("")) ? null : stringUtil.split(
				vXResource.getTables(), ",");
		String[] columnFamilies = (vXResource.getColumnFamilies() == null || vXResource
				.getColumnFamilies().equalsIgnoreCase("")) ? new String[0] : stringUtil
				.split(vXResource.getColumnFamilies(), ",");
		String[] columns = (vXResource.getColumns() == null || vXResource
				.getColumns().equalsIgnoreCase("")) ? new String[0] : stringUtil.split(
				vXResource.getColumns(), ",");

		StringBuilder stringBuilder = new StringBuilder();

//		int resourceType = vXResource.getResourceType();
		int resourceType = xPolicyService.getResourceType(vXResource);

		if (tables == null) {
			logger.error("Invalid resources for hbase policy.");
			throw restErrorUtil.createRESTException("Please provide the"
					+ " valid resources.", MessageEnums.INVALID_INPUT_DATA);
		}

		switch (resourceType) {

		case AppConstants.RESOURCE_COLUMN:
			for (String column : columns) {
				for (String columnFamily : columnFamilies) {
					for (String table : tables) {
						stringBuilder.append("/" + table + "/" + columnFamily
								+ "/" + column + ",");
					}
				}
			}
			break;

		case AppConstants.RESOURCE_COL_FAM:
			for (String columnFamily : columnFamilies) {
				for (String table : tables) {
					stringBuilder
							.append("/" + table + "/" + columnFamily + ",");
				}
			}
			break;

		case AppConstants.RESOURCE_TABLE:
			for (String table : tables) {
				stringBuilder.append("/" + table + ",");
			}
			break;

		default:
			logger.error("Invalid resource type : " + resourceType
					+ " for hbase policy.");
			throw restErrorUtil.createRESTException("Please provide the"
					+ " valid resource type.", MessageEnums.INVALID_INPUT_DATA);
		}

		int lastIndexOfSeperator = stringBuilder.lastIndexOf(",");
		if (lastIndexOfSeperator > 0) {
			String name = stringBuilder.substring(0,
					stringBuilder.lastIndexOf(","));
			vXResource.setName(name);
		} else {
			vXResource.setName(stringBuilder.toString());
		}
	}
	private void createResourcePathForKnox(VXResource vXResource) {

		String[] topologies = (vXResource.getTopologies() == null || vXResource
				.getTopologies().equalsIgnoreCase("")) ? null : stringUtil.split(
				vXResource.getTopologies(), ",");
		String[] serviceNames = (vXResource.getServices() == null || vXResource
				.getServices().equalsIgnoreCase("")) ? new String[0] : stringUtil
				.split(vXResource.getServices(), ",");

		StringBuilder stringBuilder = new StringBuilder();

//		int resourceType = vXResource.getResourceType();
		int resourceType = xPolicyService.getResourceType(vXResource);

		if (topologies == null) {
			logger.error("Invalid resources for knox policy.");
			throw restErrorUtil.createRESTException("Please provide the"
					+ " valid resources.", MessageEnums.INVALID_INPUT_DATA);
		}

		switch (resourceType) {

		case AppConstants.RESOURCE_SERVICE_NAME:
			for (String serviceName : serviceNames) {
				for (String topology : topologies) {
					stringBuilder
							.append("/" + topology + "/" + serviceName + ",");
				}
			}
			break;

		case AppConstants.RESOURCE_TOPOLOGY:
			for (String topology : topologies) {
				stringBuilder.append("/" + topology + ",");
			}
			break;

		default:
			logger.error("Invalid resource type : " + resourceType
					+ " for hbase policy.");
			throw restErrorUtil.createRESTException("Please provide the"
					+ " valid resource type.", MessageEnums.INVALID_INPUT_DATA);
		}

		int lastIndexOfSeperator = stringBuilder.lastIndexOf(",");
		if (lastIndexOfSeperator > 0) {
			String name = stringBuilder.substring(0,
					stringBuilder.lastIndexOf(","));
			vXResource.setName(name);
		} else {
			vXResource.setName(stringBuilder.toString());
		}
	}
	private void createResourcePathForStorm(VXResource vXResource) {

		String[] topologies = (vXResource.getTopologies() == null || vXResource
				.getTopologies().equalsIgnoreCase("")) ? null : stringUtil.split(
				vXResource.getTopologies(), ",");
		
		String[] serviceNames = (vXResource.getServices() == null || vXResource
		                 .getServices().equalsIgnoreCase("")) ? new String[0] : stringUtil
		                .split(vXResource.getServices(), ",");

		StringBuilder stringBuilder = new StringBuilder();

//		int resourceType = vXResource.getResourceType();
		int resourceType = xPolicyService.getResourceType(vXResource);

		if (topologies == null) {
			logger.error("Invalid resources for Storm policy.");
			throw restErrorUtil.createRESTException("Please provide the"
					+ " valid resources.", MessageEnums.INVALID_INPUT_DATA);
		}

		switch (resourceType) {

		case AppConstants.RESOURCE_TOPOLOGY:
			for (String topology : topologies) {
				stringBuilder.append("/" + topology + ",");
			}
			break;

		case AppConstants.RESOURCE_SERVICE_NAME:
			for (String serviceName : serviceNames) {
				for (String topology : topologies) {
					stringBuilder.append("/" + topology + "/" + serviceName + ",");
				}
			}
		break;

		default:
			logger.error("Invalid resource type : " + resourceType
					+ " for Storm policy.");
			throw restErrorUtil.createRESTException("Please provide the"
					+ " valid resource type.", MessageEnums.INVALID_INPUT_DATA);
		}

		int lastIndexOfSeperator = stringBuilder.lastIndexOf(",");
		if (lastIndexOfSeperator > 0) {
			String name = stringBuilder.substring(0,
					stringBuilder.lastIndexOf(","));
			vXResource.setName(name);
		} else {
			vXResource.setName(stringBuilder.toString());
		}
	}
	@SuppressWarnings("unchecked")
	private HashMap<String, Object> populatePermMap(VXResource xResource,
			HashMap<String, Object> resourceMap, int assetType) {
		List<VXPermMap> xPermMapList = xResource.getPermMapList();

		Set<Long> groupList = new HashSet<Long>();
		for (VXPermMap xPermMap : xPermMapList) {
			groupList.add(xPermMap.getId());
		}

		List<HashMap<String, Object>> sortedPermMapGroupList = new ArrayList<HashMap<String, Object>>();

		// Loop for adding group perms
		for (VXPermMap xPermMap : xPermMapList) {
			String groupKey = xPermMap.getPermGroup();
			if (groupKey != null) {
				boolean found = false;
				for (HashMap<String, Object> sortedPermMap : sortedPermMapGroupList) {
					if (sortedPermMap.containsValue(groupKey)) {
						found = true;

						Long groupId = xPermMap.getGroupId();
						Long userId = xPermMap.getUserId();

						if (groupId != null) {
							Set<String> groups = (Set<String>) sortedPermMap.get("groups");

							if(groups != null){
								groups.add(xPermMap.getGroupName());
								sortedPermMap.put("groups", groups);
							}
						} else if (userId != null) {
							Set<String> users = (Set<String>) sortedPermMap.get("users");

							if (users != null) {
								users.add(xPermMap.getUserName());
								sortedPermMap.put("users", users);								
							}
						}

						Set<String> access = (Set<String>) sortedPermMap
								.get("access");
						String perm = AppConstants
								.getLabelFor_XAPermType(xPermMap.getPermType());
						access.add(perm);
						sortedPermMap.put("access", access);
					}
				}
				if (!found) {
					HashMap<String, Object> sortedPermMap = new HashMap<String, Object>();
					sortedPermMap.put("groupKey", xPermMap.getPermGroup());

					Set<String> permSet = new HashSet<String>();
					String perm = AppConstants.getLabelFor_XAPermType(xPermMap
							.getPermType());
					permSet.add(perm);
					
					sortedPermMap.put("access", permSet);
					
					if(assetType == AppConstants.ASSET_KNOX){
						String[] ipAddrList = new String[0];
						if(xPermMap.getIpAddress() != null){
							ipAddrList = xPermMap.getIpAddress().split(",");
							sortedPermMap.put("ipAddress", ipAddrList);
						}else
							sortedPermMap.put("ipAddress",ipAddrList);
					}
					
					Long groupId = xPermMap.getGroupId();
					Long userId = xPermMap.getUserId();

					if (groupId != null) {
						Set<String> groupSet = new HashSet<String>();
						String group = xPermMap.getGroupName();
						groupSet.add(group);
						sortedPermMap.put("groups", groupSet);
					} else if (userId != null) {
						Set<String> userSet = new HashSet<String>();
						String user = xPermMap.getUserName();
						userSet.add(user);
						sortedPermMap.put("users", userSet);
					}

					sortedPermMapGroupList.add(sortedPermMap);
				}
			}
		}

		for (HashMap<String, Object> sortedPermMap : sortedPermMapGroupList) {
			sortedPermMap.remove("groupKey");
		}

		for (HashMap<String, Object> sortedPermMap : sortedPermMapGroupList) {
			sortedPermMap.remove("groupKey");
		}

		resourceMap.put("permission", sortedPermMapGroupList);
		return resourceMap;
	}

	private String getBooleanValue(int elementValue) {
		if (elementValue == 1) {
			return "1"; // BOOL_TRUE
		}
		return "0"; // BOOL_FALSE
	}

	public void updateDefaultPolicy(VXAsset vXAsset, String config) {
		int assetType = vXAsset.getAssetType();
		Map<String, String> configMap = jsonUtil.jsonToMap(config);
		String userName = stringUtil.getValidUserName(configMap.get("username"));
		VXResource vxResource = fetchDefaultPolicyForAsset(vXAsset.getId(),
				assetType);
		if (vxResource != null) {
			UpdateDefaultPolicyUserAndPerm(vxResource, userName);
		}
	}

	public void UpdateDefaultPolicyUserAndPerm(VXResource vXResource,
			String userName) {
		if (userName != null && !userName.isEmpty()) {
			XXUser xxUser = rangerDaoManager.getXXUser().findByUserName(userName);
			VXUser vXUser;
			if (xxUser != null) {
				vXUser = xUserService.populateViewBean(xxUser);
			} else {
				vXUser = new VXUser();
				vXUser.setName(userName);
				// FIXME hack : unnecessary.
				vXUser.setDescription(userName);
				vXUser = xUserService.createResource(vXUser);
			}
			// fetch old permission and consider only one permission for default
			// policy
			List<XXPermMap> xxPermMapList = rangerDaoManager.getXXPermMap()
					.findByResourceId(vXResource.getId());
			VXPermMap vXPermMap = null;
			if (xxPermMapList != null && xxPermMapList.size() != 0) {
				vXPermMap = xPermMapService.populateViewBean(xxPermMapList
						.get(0));
			}

			if (vXPermMap == null) {
				// create new permission
				vXPermMap = new VXPermMap();
				vXPermMap.setUserId(vXUser.getId());
				vXPermMap.setResourceId(vXResource.getId());
			} else {
				// update old permission after updating userid
				vXPermMap.setUserId(vXUser.getId());
				xPermMapService.updateResource(vXPermMap);
			}

		}

	}

	public VXResource fetchDefaultPolicyForAsset(Long assetId, int assetType) {
		String resourceName = "";
		List<XXResource> xxResourceList = new ArrayList<XXResource>();
		if (assetType == AppConstants.ASSET_HDFS) {
			resourceName = "/*";
			xxResourceList = rangerDaoManager.getXXResource()
					.findByResourceNameAndAssetIdAndRecursiveFlag(resourceName,
							assetId, AppConstants.BOOL_TRUE);
		} else if (assetType == AppConstants.ASSET_HIVE) {
			resourceName = "/*/*/*";
			xxResourceList = rangerDaoManager.getXXResource()
					.findByResourceNameAndAssetIdAndResourceType(resourceName,
							assetId, AppConstants.RESOURCE_UNKNOWN);
		} else if (assetType == AppConstants.ASSET_HBASE) {
			resourceName = "/*/*/*";
			xxResourceList = rangerDaoManager.getXXResource()
					.findByResourceNameAndAssetIdAndResourceType(resourceName,
							assetId, AppConstants.RESOURCE_UNKNOWN);
		}
		if (xxResourceList == null) {
			return null;
		}
		XXResource xxResource = null;
		for (XXResource resource : xxResourceList) {
			if (resource.getName().equals(resourceName)) {
				xxResource = resource;
				break;
			}
		}

		if (xxResource != null) {
			return xResourceService.populateViewBean(xxResource);
		}
		return null;

	}

	public XXPolicyExportAudit createPolicyAudit(
			final XXPolicyExportAudit xXPolicyExportAudit) {
		TransactionTemplate txTemplate = new TransactionTemplate(txManager);
		txTemplate
				.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRED);
		XXPolicyExportAudit policyExportAudit = (XXPolicyExportAudit) txTemplate
				.execute(new TransactionCallback<Object>() {
					public Object doInTransaction(TransactionStatus status) {
						if (xXPolicyExportAudit.getHttpRetCode() == HttpServletResponse.SC_NOT_MODIFIED) {
							boolean logNotModified = PropertiesUtil.getBooleanProperty("ranger.log.SC_NOT_MODIFIED", false);
							if (!logNotModified) {
								logger.debug("Not logging HttpServletResponse."
										+ "SC_NOT_MODIFIED, to enable, update "
										+ ": ranger.log.SC_NOT_MODIFIED");
								return null;
							}
						}
						return rangerDaoManager.getXXPolicyExportAudit().create(
								xXPolicyExportAudit);
					}
				});
		return policyExportAudit;
	}

	public VXTrxLogList getReportLogs(SearchCriteria searchCriteria) {
		if (!xaBizUtil.isAdmin()) {
			throw restErrorUtil.create403RESTException("Permission Denied !");
		}

		if (searchCriteria == null) {
			searchCriteria = new SearchCriteria();
		}

		if (searchCriteria.getParamList() != null
				&& searchCriteria.getParamList().size() > 0) {
			int clientTimeOffsetInMinute = RestUtil.getClientTimeOffset();
			java.util.Date temp = null;
			DateUtil dateUtil = new DateUtil();
			if (searchCriteria.getParamList().containsKey("startDate")) {
				temp = (java.util.Date) searchCriteria.getParamList().get(
						"startDate");
				temp = dateUtil.getDateFromGivenDate(temp, 0, 0, 0, 0);
				temp = dateUtil.addTimeOffset(temp, clientTimeOffsetInMinute);
				searchCriteria.getParamList().put("startDate", temp);
			}
			if (searchCriteria.getParamList().containsKey("endDate")) {
				temp = (java.util.Date) searchCriteria.getParamList().get(
						"endDate");
				temp = dateUtil.getDateFromGivenDate(temp, 0, 23, 59, 59);
				temp = dateUtil.addTimeOffset(temp, clientTimeOffsetInMinute);
				searchCriteria.getParamList().put("endDate", temp);
			}
			if (searchCriteria.getParamList().containsKey("owner")) {
				XXPortalUser xXPortalUser= rangerDaoManager.getXXPortalUser().findByLoginId(
						(searchCriteria.getParamList().get("owner").toString()));
				if(xXPortalUser!=null){
					searchCriteria.getParamList().put("owner", xXPortalUser.getId());
				}else{
					searchCriteria.getParamList().put("owner", 0);
				}
				
			}

		}

		VXTrxLogList vXTrxLogList = xTrxLogService
				.searchXTrxLogs(searchCriteria);
		Long count=xTrxLogService
				.searchXTrxLogsCount(searchCriteria);
		vXTrxLogList.setTotalCount(count);
		 
		List<VXTrxLog> newList = validateXXTrxLogList(vXTrxLogList.getVXTrxLogs());
		vXTrxLogList.setVXTrxLogs(newList);
		return vXTrxLogList;
	}

	public VXAccessAuditList getAccessLogs(SearchCriteria searchCriteria) {

        if (searchCriteria == null) {
            searchCriteria = new SearchCriteria();
        }
        if (searchCriteria.getParamList() != null
                && searchCriteria.getParamList().size() > 0) {
            int clientTimeOffsetInMinute = RestUtil.getClientTimeOffset();
            java.util.Date temp = null;
            DateUtil dateUtil = new DateUtil();
            if (searchCriteria.getParamList().containsKey("startDate")) {
                temp = (java.util.Date) searchCriteria.getParamList().get(
                        "startDate");
                temp = dateUtil.getDateFromGivenDate(temp, 0, 0, 0, 0);
                temp = dateUtil.addTimeOffset(temp, clientTimeOffsetInMinute);
                searchCriteria.getParamList().put("startDate", temp);
            }
            if (searchCriteria.getParamList().containsKey("endDate")) {
                temp = (java.util.Date) searchCriteria.getParamList().get(
                        "endDate");
                temp = dateUtil.getDateFromGivenDate(temp, 0, 23, 59, 59);
                temp = dateUtil.addTimeOffset(temp, clientTimeOffsetInMinute);
                searchCriteria.getParamList().put("endDate", temp);
            }

        }
        if (searchCriteria.getSortType() == null) {
            searchCriteria.setSortType("desc");
        } else if (!searchCriteria.getSortType().equalsIgnoreCase("asc") && !searchCriteria.getSortType().equalsIgnoreCase("desc")) {
            searchCriteria.setSortType("desc");
        }
        if (xaBizUtil.getAuditDBType().equalsIgnoreCase(RangerBizUtil.AUDIT_STORE_SOLR)) {
            return solrAccessAuditsService.searchXAccessAudits(searchCriteria);
        } else {
            return xAccessAuditService.searchXAccessAudits(searchCriteria);
        }
    }


	public VXTrxLogList getTransactionReport(String transactionId) {
		List<XXTrxLog> xTrxLogList = rangerDaoManager.getXXTrxLog()
				.findByTransactionId(transactionId);
		VXTrxLogList vXTrxLogList = new VXTrxLogList();
		List<VXTrxLog> trxLogList = new ArrayList<VXTrxLog>();
		
		for(XXTrxLog xTrxLog : xTrxLogList) {
		        trxLogList.add(xTrxLogService.populateViewBean(xTrxLog));
		}
		
		List<VXTrxLog> vXTrxLogs = validateXXTrxLogList(trxLogList);
		vXTrxLogList.setVXTrxLogs(vXTrxLogs);
		return vXTrxLogList;
	}
	public List<VXTrxLog> validateXXTrxLogList(List<VXTrxLog> xTrxLogList) {
		
		List<VXTrxLog> vXTrxLogs = new ArrayList<VXTrxLog>();
		for (VXTrxLog xTrxLog : xTrxLogList) {
			VXTrxLog vXTrxLog = new VXTrxLog();
			vXTrxLog = xTrxLog;
			if(vXTrxLog.getPreviousValue()==null || vXTrxLog.getPreviousValue().equalsIgnoreCase("null")){
				vXTrxLog.setPreviousValue("");
			}
			if(vXTrxLog.getAttributeName()!=null && vXTrxLog.getAttributeName().equalsIgnoreCase("Password")){
				vXTrxLog.setPreviousValue("*********");
				vXTrxLog.setNewValue("***********");
			}
			if(vXTrxLog.getAttributeName()!=null && vXTrxLog.getAttributeName().equalsIgnoreCase("Connection Configurations")){
				if(vXTrxLog.getPreviousValue()!=null && vXTrxLog.getPreviousValue().contains("password")){
					String tempPreviousStr=vXTrxLog.getPreviousValue();					
					String tempPreviousArr[]=vXTrxLog.getPreviousValue().split(",");					
					for(int i=0;i<tempPreviousArr.length;i++){
						if(tempPreviousArr[i].contains("{\"password")){
							vXTrxLog.setPreviousValue(tempPreviousStr.replace(tempPreviousArr[i], "{\"password\":\"*****\"}"));
							break;
						}else if(tempPreviousArr[i].contains("\"password") && tempPreviousArr[i].contains("}")){
							vXTrxLog.setPreviousValue(tempPreviousStr.replace(tempPreviousArr[i], "\"password\":\"******\"}"));
							break;
						}else if(tempPreviousArr[i].contains("\"password")){
							vXTrxLog.setPreviousValue(tempPreviousStr.replace(tempPreviousArr[i], "\"password\":\"******\""));
							break;
						}
					}			
				}
				if(vXTrxLog.getNewValue()!=null && vXTrxLog.getNewValue().contains("password")){
					String tempNewStr=vXTrxLog.getNewValue();
					String tempNewArr[]=vXTrxLog.getNewValue().split(",");
					for(int i=0;i<tempNewArr.length;i++){
						if(tempNewArr[i].contains("{\"password")){
							vXTrxLog.setNewValue(tempNewStr.replace(tempNewArr[i], "{\"password\":\"*****\"}"));
							break;
						}else if(tempNewArr[i].contains("\"password") && tempNewArr[i].contains("}")){
							vXTrxLog.setNewValue(tempNewStr.replace(tempNewArr[i], "\"password\":\"******\"}"));
							break;
						}else if(tempNewArr[i].contains("\"password")){
							vXTrxLog.setNewValue(tempNewStr.replace(tempNewArr[i], "\"password\":\"******\""));
							break;
						}
					}	
				}
			}			
			vXTrxLogs.add(vXTrxLog);
		}
		return vXTrxLogs;
	}
	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.apache.ranger.biz.AssetMgrBase#searchXPolicyExportAudits(org.apache.ranger.
	 * common.SearchCriteria)
	 */
	@Override
	public VXPolicyExportAuditList searchXPolicyExportAudits(
			SearchCriteria searchCriteria) {

		if (searchCriteria == null) {
			searchCriteria = new SearchCriteria();
		}

        if (searchCriteria.getParamList() != null
                && searchCriteria.getParamList().size() > 0) {

            int clientTimeOffsetInMinute = RestUtil.getClientTimeOffset();
            java.util.Date temp = null;
            DateUtil dateUtil = new DateUtil();
            if (searchCriteria.getParamList().containsKey("startDate")) {
                temp = (java.util.Date) searchCriteria.getParamList().get(
                        "startDate");
                temp = dateUtil.getDateFromGivenDate(temp, 0, 0, 0, 0);
                temp = dateUtil.addTimeOffset(temp, clientTimeOffsetInMinute);
                searchCriteria.getParamList().put("startDate", temp);
            }
            if (searchCriteria.getParamList().containsKey("endDate")) {
                temp = (java.util.Date) searchCriteria.getParamList().get(
                        "endDate");
                temp = dateUtil.getDateFromGivenDate(temp, 0, 23, 59, 59);
                temp = dateUtil.addTimeOffset(temp, clientTimeOffsetInMinute);
                searchCriteria.getParamList().put("endDate", temp);
            }
        }
        return xPolicyExportAuditService.searchXPolicyExportAudits(searchCriteria);
    }
	
	public VXAsset getXAsset(Long id){
		UserSessionBase currentUserSession = ContextUtil
				.getCurrentUserSession();
		VXAsset vXAsset=null;
		if (currentUserSession.isUserAdmin()) {
			vXAsset = xAssetService.readResource(id);
			if (vXAsset.getActiveStatus() == RangerCommonEnums.STATUS_DELETED) {
				logger.error("Trying to read Asset which is soft deleted");
				throw restErrorUtil.createRESTException(
						"Repository not found for this Id : " + id,
						MessageEnums.DATA_NOT_FOUND, id, null,
						"Repository does not exist for this Id : " + id);
			}
		}else{			
			XXAsset  xXAsset=rangerDaoManager.getXXAsset().getById(id);	
			
			if (xXAsset.getActiveStatus() == RangerCommonEnums.STATUS_DELETED) {
				logger.error("Trying to read Asset which is soft deleted");
				throw restErrorUtil.createRESTException(
						"Repository not found for this Id : " + id,
						MessageEnums.DATA_NOT_FOUND, id, null,
						"Repository does not exist for this Id : " + id);
			}
			
			vXAsset=xAssetService.populateViewBean(xXAsset);
			/*List<XXResource>  xXResourceList=rangerDaoManager
					.getXXResource().findByAssetId(id);
			for (XXResource xXResource : xXResourceList) {
				VXResponse vXResponse = xaBizUtil.hasPermission(xResourceService.populateViewBean(xXResource), 
						AppConstants.XA_PERM_TYPE_ADMIN);
				if(vXResponse.getStatusCode() == VXResponse.STATUS_SUCCESS){
					XXAsset  xXAsset=rangerDaoManager.getXXAsset().getById(id);		
					vXAsset=xAssetService.populateViewBean(xXAsset);
					break;
				}
			}*/
		}		
		return vXAsset;
	}

	private void createDefaultUDFPolicy(VXAsset vXAsset, String config) {
		int assetType = vXAsset.getAssetType();
		if (assetType != AppConstants.ASSET_HIVE) {
			return ;
		}
		Map<String, String> configMap = jsonUtil.jsonToMap(config);
		String userName = stringUtil.getValidUserName(configMap.get("username"));
		VXResource vXResource = new VXResource();
		vXResource.setAssetId(vXAsset.getId());
		vXResource.setAssetName(vXAsset.getName());
		vXResource.setResourceStatus(AppConstants.STATUS_ENABLED);
		String tempPolicyName=vXAsset.getName()+"-"+2+"-"+DateUtil.dateToString(DateUtil.getUTCDate(),"yyyyMMddHHmmss");
		vXResource.setPolicyName(tempPolicyName);
		if (assetType == AppConstants.ASSET_HIVE) {
			vXResource.setDatabases("*");
			vXResource.setTables("");
			vXResource.setColumns("");
			vXResource.setUdfs("*");
			vXResource.setName("/*/*");
			vXResource.setResourceType(AppConstants.RESOURCE_UDF);
		}
		vXResource = xResourceService.createResource(vXResource);
		if (userName != null && !userName.isEmpty()) {
			XXUser xxUser = rangerDaoManager.getXXUser().findByUserName(userName);
			VXUser vXUser;
			if (xxUser != null) {
				vXUser = xUserService.populateViewBean(xxUser);
			} else {
				vXUser = new VXUser();
				vXUser.setName(userName);
				vXUser.setUserSource(RangerCommonEnums.USER_EXTERNAL);
				vXUser=xUserMgr.createXUser(vXUser);
				//vXUser = xUserService.createResource(vXUser);
			}
			Random rand = new Random();
			String permGrp = new Date() + " : " + rand.nextInt(9999);
			
			VXPermMap vXPermMap = new VXPermMap();
			vXPermMap.setUserId(vXUser.getId());
			vXPermMap.setResourceId(vXResource.getId());
			vXPermMap.setPermGroup(permGrp);
			xPermMapService.createResource(vXPermMap);
		}
		VXAuditMap vXAuditMap = new VXAuditMap();
		vXAuditMap.setAuditType(AppConstants.XA_AUDIT_TYPE_ALL);
		vXAuditMap.setResourceId(vXResource.getId());
		vXAuditMap = xAuditMapService.createResource(vXAuditMap);
		vXResource=xResourceService.readResource(vXResource.getId());
		List<VXPermMap> permMapList = vXResource.getPermMapList();
		List<VXAuditMap> auditMapList = vXResource.getAuditList();
		List<XXTrxLog> trxLogList = xResourceService.getTransactionLog(
				vXResource, "create");
		for (VXPermMap vXPermMap : permMapList) {
			trxLogList.addAll(xPermMapService.getTransactionLog(vXPermMap,
					"create"));
		}
		for (VXAuditMap vXAuditMapObj : auditMapList) {
			trxLogList.addAll(xAuditMapService.getTransactionLog(vXAuditMapObj,
					"create"));
		}
		xaBizUtil.createTrxLog(trxLogList);
	}
	
	public boolean isValidHttpsAuthentication(String repository,
			X509Certificate[] certchain, boolean httpEnabled,
			String ipAddress, boolean isSecure) {
		boolean isValidAuthentication=false;
		if (repository == null || repository.isEmpty()) {			
			logger.error("Repository name not provided");
			throw restErrorUtil.createRESTException("Unauthorized access.",
					MessageEnums.OPER_NOT_ALLOWED_FOR_ENTITY);
		}
		XXAsset xAsset = rangerDaoManager.getXXAsset().findByAssetName(repository);
		if(xAsset==null){
			logger.error("Requested repository not found");
			throw restErrorUtil.createRESTException("No Data Found.",
					MessageEnums.DATA_NOT_FOUND);
		}
		if(xAsset.getActiveStatus()==RangerCommonEnums.ACT_STATUS_DISABLED){
			logger.error("Requested repository is disabled");
			throw restErrorUtil.createRESTException("Unauthorized access.",
					MessageEnums.OPER_NOT_ALLOWED_FOR_STATE);
		}		
		if (!httpEnabled) {
			if (!isSecure) {
				throw restErrorUtil.createRESTException("Unauthorized access -"
						+ " only https allowed",
						MessageEnums.OPER_NOT_ALLOWED_FOR_ENTITY);
			}
			if (certchain == null || certchain.length == 0) {
				throw restErrorUtil.createRESTException("Unauthorized access -"
						+ " unable to get client certificate",
						MessageEnums.OPER_NOT_ALLOWED_FOR_ENTITY);
			}
		}		
		String commonName = null;
		if (certchain != null) {
			X509Certificate clientCert = certchain[0];
			String dn = clientCert.getSubjectX500Principal().getName();
			try {
				LdapName ln = new LdapName(dn);
				for (Rdn rdn : ln.getRdns()) {
					if (rdn.getType().equalsIgnoreCase("CN")) {
						commonName = rdn.getValue() + "";
						break;
					}
				}
				if (commonName == null) {
					throw restErrorUtil.createRESTException(
							"Unauthorized access - Unable to find Common Name from ["
									+ dn + "]",
							MessageEnums.OPER_NOT_ALLOWED_FOR_ENTITY);
				}
			} catch (InvalidNameException e) {
				logger.error("Invalid Common Name.", e);
				throw restErrorUtil.createRESTException(
						"Unauthorized access - Invalid Common Name",
						MessageEnums.OPER_NOT_ALLOWED_FOR_ENTITY);
			}
		}		
		if (commonName != null) {
			String config = xAsset.getConfig();
			Map<String, String> configMap = jsonUtil.jsonToMap(config);
			String cnFromConfig = configMap.get("commonNameForCertificate");
			if (cnFromConfig == null
					|| !commonName.equalsIgnoreCase(cnFromConfig)) {
				throw restErrorUtil.createRESTException(
						"Unauthorized access. expected [" + cnFromConfig
								+ "], found [" + commonName + "]",
						MessageEnums.OPER_NOT_ALLOWED_FOR_ENTITY);
			}
		}
		isValidAuthentication=true;
		return isValidAuthentication;
	}
	
	public VXResource grantXResource(VXResource vXResource,VXPolicy vXPolicy) {
		if(vXResource==null){
			return vXResource;
		}
		
		//checks user exists or not
		XXUser xUser = rangerDaoManager.getXXUser().findByUserName(vXResource.getOwner());		
		if(xUser==null){
			throw restErrorUtil.createRESTException("User " +vXResource.getOwner() + " is Not Found",
					MessageEnums.DATA_NOT_FOUND);
		}	
		XXPortalUser xXPortalUser= rangerDaoManager.getXXPortalUser().findByLoginId(vXResource.getOwner());
		if(xXPortalUser==null){
			throw restErrorUtil.createRESTException("User " +vXResource.getOwner() + " is Not Found",
					MessageEnums.DATA_NOT_FOUND);
		}
		//checks repository exists or not
		XXAsset xAsset = rangerDaoManager.getXXAsset().findByAssetName(vXResource.getAssetName());
		if (xAsset == null) {
			logger.error("Repository not found for asset : " + vXResource.getAssetName());
			throw restErrorUtil.createRESTException("Repository for which"
					+ " the policy is created, doesn't exist.",MessageEnums.DATA_NOT_FOUND);
		}	
		//checks repository active or not
		if(xAsset.getActiveStatus()==RangerCommonEnums.ACT_STATUS_DISABLED){			
				logger.error("Trying to create/update policy in disabled repository");
				throw restErrorUtil.createRESTException("Resource "
						+ "creation/updation not allowed in disabled repository",MessageEnums.OPER_NO_PERMISSION);
			
		}
		vXResource.setAssetId(xAsset.getId());
		vXResource.setAssetType(xAsset.getAssetType());
		//create resource name/path for HIVE/Hbase policy.
		if (xAsset.getAssetType() == AppConstants.ASSET_HIVE) {
			createResourcePathForHive(vXResource);
			vXResource.setIsRecursive(0);
		} else if (xAsset.getAssetType() == AppConstants.ASSET_HBASE) {
			createResourcePathForHbase(vXResource);
			vXResource.setIsRecursive(0);
		}else{
			logger.error("Invalid repository for grant operation" );
			throw restErrorUtil.createRESTException(vXResource.getAssetName() +" is not a " 
					+ " valid repository for grant operation",MessageEnums.OPER_NO_PERMISSION);
		}
		
		//check whether resource contains multiple path or not
		if(!stringUtil.isEmpty(vXResource.getName())){
			String[] resources=vXResource.getName().trim().split(",");
			if(resources!=null && resources.length>1){
				logger.error("More than one resource found for grant operation in policy : " + vXResource.getName());
				throw restErrorUtil.createRESTException("We did not find exact match for this resource : " + vXResource.getName(),MessageEnums.INVALID_INPUT_DATA);
			}
		}else{
			throw restErrorUtil.createRESTException("Invalid Resource Name : " + vXResource.getName(),MessageEnums.INVALID_INPUT_DATA);
		}
		
		//checks user is admin in resource or not
		List<XXResource> xResourceList=rangerDaoManager.getXXResource().findByAssetId(xAsset.getId());		
		if(xResourceList!=null){
			boolean isAdmin=false;
			List<XXPortalUserRole> xXPortalUserRoleList = rangerDaoManager.getXXPortalUserRole().findByParentId(xXPortalUser.getId());
			if(xXPortalUserRoleList!=null && xXPortalUserRoleList.size()>0){
				for(XXPortalUserRole xXPortalUserRole: xXPortalUserRoleList){
					if(xXPortalUserRole.getUserRole().equalsIgnoreCase(RangerConstants.ROLE_SYS_ADMIN)){
						isAdmin=true;
						break;
					}
				}
			}			

			if(!isAdmin){
				if (xAsset.getAssetType() == AppConstants.ASSET_HIVE) {
					String[] requestResNameList = vXResource.getName().trim().split(",");
					if (stringUtil.isEmpty(vXResource.getUdfs())) {
						int reqTableType = vXResource.getTableType();
						int reqColumnType = vXResource.getColumnType();
						for (String resourceName : requestResNameList) {
							isAdmin=xaBizUtil.matchHivePolicy(resourceName,xResourceList, xUser.getId(),AppConstants.XA_PERM_TYPE_ADMIN,reqTableType,reqColumnType, false);
							if (isAdmin) {
								break;
							}
						}
					} else {
						for (String resourceName : requestResNameList) {
							isAdmin=xaBizUtil.matchHivePolicy(resourceName,xResourceList, xUser.getId(),AppConstants.XA_PERM_TYPE_ADMIN);
							if (isAdmin) {
								break;
							}
						}
					}						
				}else if (xAsset.getAssetType() == AppConstants.ASSET_HBASE) {
					isAdmin=xaBizUtil.matchHbasePolicy(vXResource.getName(),xResourceList,null, xUser.getId(),AppConstants.XA_PERM_TYPE_ADMIN);
				}
			}
			if (!isAdmin) {
				throw restErrorUtil.createRESTException("You're not permitted to perform "
							+ "grant operation for resource path : " + vXResource.getName(),MessageEnums.OPER_NO_PERMISSION);
			}
		}
		xResourceList=null;//explicit
		//check whether resource exist or not
		SearchCriteria searchCriteria=new SearchCriteria();
		if (xAsset.getAssetType() == AppConstants.ASSET_HIVE) {
			searchCriteria.getParamList().put("assetId", vXResource.getAssetId());
			searchCriteria.getParamList().put("fullname", vXResource.getName());
			searchCriteria.getParamList().put("udfs", vXResource.getUdfs());
			searchCriteria.getParamList().put("tableType", vXResource.getTableType());
			searchCriteria.getParamList().put("columnType", vXResource.getColumnType());
		}else if (xAsset.getAssetType() == AppConstants.ASSET_HBASE) {
			searchCriteria.getParamList().put("assetId", vXResource.getAssetId());
			searchCriteria.getParamList().put("fullname", vXResource.getName());
		}
		
		VXResourceList vXResourceList=xResourceService.searchXResourcesWithoutLogin(searchCriteria);
		searchCriteria=null;		
		//generate policy name if resource does not exist
		if(vXResourceList==null || vXResourceList.getListSize()==0){
			int tempPoliciesCount=0;
			String tempPolicyName=null;
			VXResourceList vXResourceListTemp=null;
			if(vXResource.getPolicyName()==null ||vXResource.getPolicyName().trim().isEmpty()){
				searchCriteria=new SearchCriteria();
				searchCriteria.getParamList().put("assetId", vXResource.getAssetId());
				vXResourceListTemp=xResourceService.searchXResourcesWithoutLogin(searchCriteria);			
				if(vXResourceListTemp!=null && vXResourceListTemp.getListSize()>0){
					tempPoliciesCount=vXResourceListTemp.getListSize();
				}	
				vXResourceListTemp=null;
				while(true){
					tempPoliciesCount++;
					tempPolicyName=xAsset.getName()+"-"+tempPoliciesCount+"-"+DateUtil.dateToString(DateUtil.getUTCDate(),"yyyyMMddHHmmss");
					vXResource.setPolicyName(tempPolicyName);
					searchCriteria=new SearchCriteria();		
					searchCriteria.getParamList().put("policyName", vXResource.getPolicyName());
					vXResourceListTemp=xResourceService.searchXResourcesWithoutLogin(searchCriteria);
					//if policy name not exist then list will be empty and generated policyname will valid 
					if(vXResourceListTemp==null|| vXResourceListTemp.getListSize()==0){
						break;
					}
				}
			}			
		}else{
			for(VXResource vXResourceDB:vXResourceList.getVXResources()){
				if(vXResourceDB!=null){
					vXResource.setId(vXResourceDB.getId());
					vXResource.setPolicyName(vXResourceDB.getPolicyName());
					break;
				}
			}			
		}		
		
		//update addedby and updated by in permmap and auditmap
		List<VXPermMap> permMapList=vXResource.getPermMapList();
		List<VXAuditMap> auditMapList = vXResource.getAuditList();
		VXPermMap vXPermMapTemp=null;
		VXAuditMap vXAuditMapTemp=null;
		XXUser xxUser=null;
		XXGroup xxGroup=null;
		for (int i=0;i< permMapList.size();i++) {
			vXPermMapTemp=permMapList.get(i);
			if(vXPermMapTemp==null){
				continue;
			}
			if(stringUtil.isEmpty(vXPermMapTemp.getOwner())){
				vXPermMapTemp.setOwner(vXResource.getOwner());
			}
			if(stringUtil.isEmpty(vXPermMapTemp.getUpdatedBy())){ 
				vXPermMapTemp.setUpdatedBy(vXResource.getUpdatedBy());
			}
			if(vXPermMapTemp.getPermFor()==AppConstants.XA_PERM_FOR_USER){
				if(vXPermMapTemp.getUserId()==null && !stringUtil.isEmpty(vXPermMapTemp.getUserName())){
					xxUser = rangerDaoManager.getXXUser().findByUserName(vXPermMapTemp.getUserName());
					if (xxUser != null) {
						vXPermMapTemp.setUserId(xxUser.getId());
					} else{
						throw restErrorUtil.createRESTException("User : "+ vXPermMapTemp.getUserName() + " is Not Found",
								MessageEnums.DATA_NOT_FOUND);
					}
				}
			}
			if(vXPermMapTemp.getPermFor()==AppConstants.XA_PERM_FOR_GROUP){
				if(vXPermMapTemp.getGroupId()==null && !stringUtil.isEmpty(vXPermMapTemp.getGroupName())){
					xxGroup = rangerDaoManager.getXXGroup().findByGroupName(
							vXPermMapTemp.getGroupName());
					if (xxGroup != null) {
						vXPermMapTemp.setGroupId(xxGroup.getId());
					}else{
						throw restErrorUtil.createRESTException("Group : "+ vXPermMapTemp.getGroupName() + " is Not Found",
								MessageEnums.DATA_NOT_FOUND);
					} 
				}
			}
			permMapList.set(i, vXPermMapTemp);				
		}			
		for (int i=0;i< auditMapList.size();i++) {
			vXAuditMapTemp=auditMapList.get(i);
			if(vXAuditMapTemp!=null && stringUtil.isEmpty(vXAuditMapTemp.getOwner())){
				vXAuditMapTemp.setOwner(vXResource.getOwner());
			}
			if(vXAuditMapTemp!=null && stringUtil.isEmpty(vXAuditMapTemp.getUpdatedBy())){ 
				vXAuditMapTemp.setUpdatedBy(vXResource.getUpdatedBy());
			}
			auditMapList.set(i, vXAuditMapTemp);
		}
		vXResource.setPermMapList(permMapList);
		vXResource.setAuditList(auditMapList);		
		
		//create 	
		List<XXTrxLog> trxLogList=null ;
		if(vXResourceList==null || vXResourceList.getListSize()==0){			
			vXResource = xResourceService.createResource(vXResource);
			List<VXPermMap> newPermMapList = vXResource.getPermMapList();
			List<VXAuditMap> newAuditMapList = vXResource.getAuditList();
			trxLogList= xResourceService.getTransactionLog(vXResource, "create");	
			for (VXPermMap vXPermMap : newPermMapList) {
				trxLogList.addAll(xPermMapService.getTransactionLog(vXPermMap,
						"create"));
			}
			for (VXAuditMap vXAuditMap : newAuditMapList) {
				trxLogList.addAll(xAuditMapService.getTransactionLog(vXAuditMap,
						"create"));
			}			
		}
		
		if(vXResourceList!=null && vXResourceList.getListSize()>0){					
			//replace perm map if true
			if(vXPolicy.isReplacePerm()){
				XXResource xXResource = rangerDaoManager.getXXResource().getById(vXResource.getId());
				VXResource vXResourceDBObj=xResourceService.populateViewBean(xXResource);
				List<XXTrxLog> trxLogListDelete = xResourceService.getTransactionLog(
						vXResourceDBObj, xXResource, "delete");
				List<VXPermMap> permMapListtoDelete=vXResourceDBObj.getPermMapList();
				List<String> permMapDeleteKeys=new ArrayList<String>();				
				String userKey=null;				
				for(VXPermMap permMapTemp :permMapList){					
					if(permMapTemp==null||permMapTemp.getPermFor()==0||(permMapTemp.getUserId()==null && permMapTemp.getGroupId()==null)){
						continue;					
					}
					userKey=null;
					if(permMapTemp.getPermFor()==AppConstants.XA_PERM_FOR_USER){
						userKey=permMapTemp.getPermFor()+"_"+permMapTemp.getUserId();
					}
					if(permMapTemp.getPermFor()==AppConstants.XA_PERM_FOR_GROUP){
						userKey=permMapTemp.getPermFor()+"_"+permMapTemp.getGroupId();
					}
					if(!permMapDeleteKeys.contains(userKey) && !stringUtil.isEmpty(userKey)){
						permMapDeleteKeys.add(userKey);
					}
				}
				
				if(permMapListtoDelete != null) {
					for (VXPermMap permMap : permMapListtoDelete) {
						if(permMap==null || permMap.getPermFor()==0 || (permMap.getUserId()==null && permMap.getGroupId()==null)){
							continue;					
						}
						userKey=null;
						if(permMap.getPermFor()==AppConstants.XA_PERM_FOR_USER){
							userKey=permMap.getPermFor()+"_"+permMap.getUserId();
						}
						if(permMap.getPermFor()==AppConstants.XA_PERM_FOR_GROUP){
							userKey=permMap.getPermFor()+"_"+permMap.getGroupId();
						}
						if(permMapDeleteKeys.contains(userKey)){
							xPermMapService.deleteResource(permMap.getId());
							trxLogListDelete.addAll(xPermMapService.getTransactionLog(permMap,"delete"));
						}					
					}//permission deletion processing end
					xaBizUtil.createTrxLog(trxLogListDelete);	
				}
			}
		}
		
		//update case
		if(vXResourceList!=null && vXResourceList.getListSize()>0){
			XXResource xXResource = rangerDaoManager.getXXResource().getById(vXResource.getId());
			vXResource.setCreateDate(xXResource.getCreateTime());
			vXResource.setUpdateDate(xXResource.getUpdateTime());
			trxLogList = xResourceService.getTransactionLog(vXResource, xXResource, "update");
			//VXResource resource = super.updateXResource(vXResource);			
			searchCriteria = new SearchCriteria();
			searchCriteria.addParam("resourceId", vXResource.getId());
			VXPermMapList prevPermMaps = xPermMapService.searchXPermMaps(searchCriteria);
			List<VXPermMap> prevPermMapList = new ArrayList<VXPermMap>();
			List<VXPermMap> newPermMapList = vXResource.getPermMapList();
			List<VXPermMap> permMapsAdded = new ArrayList<VXPermMap>();
			//List<VXAuditMap> prevAuditMapList = new ArrayList<VXAuditMap>();			
			if (prevPermMaps != null) {
				prevPermMapList = prevPermMaps.getVXPermMaps();
			}
			// permission deletion processing start
			String newKey=null;
			String oldKey=null;
			boolean isFound=false;
			VXPermMap newObj=null;
			VXPermMap oldObj =null;
			if (newPermMapList != null && prevPermMapList!=null) {
				for (int i=0;i<newPermMapList.size();i++) {
					newObj=newPermMapList.get(i);
					newObj.setResourceId(vXResource.getId());
					isFound=false;
					if(newObj==null||newObj.getResourceId()==null||newObj.getPermFor()==0||newObj.getPermType()==0 || (newObj.getUserId()==null&&newObj.getGroupId()==null)){
						continue;					
					}
					newKey=null;
					if(newObj.getPermFor()==AppConstants.XA_PERM_FOR_USER){
						newKey=newObj.getResourceId()+"_"+newObj.getPermFor()+"_"+newObj.getUserId()+"_"+newObj.getPermType();
					}
					if(newObj.getPermFor()==AppConstants.XA_PERM_FOR_GROUP){
						newKey=newObj.getResourceId()+"_"+newObj.getPermFor()+"_"+newObj.getGroupId()+"_"+newObj.getPermType();
					}	
					isFound=false;
					oldObj =null;
					for (int j=0;j<prevPermMapList.size();j++) {
						oldObj=prevPermMapList.get(j);
						if(oldObj==null||oldObj.getResourceId()==null||oldObj.getPermFor()==0||oldObj.getPermType()==0|| (oldObj.getUserId()==null&&oldObj.getGroupId()==null)){
							continue;					
						}
						oldKey=null;
						if(oldObj.getPermFor()==AppConstants.XA_PERM_FOR_USER){
							oldKey=oldObj.getResourceId()+"_"+oldObj.getPermFor()+"_"+oldObj.getUserId()+"_"+oldObj.getPermType();
						}
						if(oldObj.getPermFor()==AppConstants.XA_PERM_FOR_GROUP){
							oldKey=oldObj.getResourceId()+"_"+oldObj.getPermFor()+"_"+oldObj.getGroupId()+"_"+oldObj.getPermType();
						}
						if(stringUtil.isEmpty(newKey)|| stringUtil.isEmpty(oldKey)){
							continue;
						}
						if(newKey.equals(oldKey)){
							isFound=true;	
							break;
						}
					}//inner for
					if(!isFound){
						newObj = xPermMapService.createResource(newObj);
						trxLogList.addAll(xPermMapService.getTransactionLog(newObj,"create"));
						permMapsAdded.add(newObj);
					}
				}//outer for			
			}// delete permissions list populate end
			else{
				throw restErrorUtil.createRESTException("No permission list received for with current grant request",MessageEnums.DATA_NOT_FOUND);
			}
			if(prevPermMapList!=null && permMapsAdded!=null){
				for(VXPermMap vXPermMap:permMapsAdded){
					prevPermMapList.add(vXPermMap);
				}
				if(permMapsAdded.size()>0){
					vXResource.setUpdateDate(DateUtil.getUTCDate());
				}
			}			
			vXResource.setPermMapList(prevPermMapList);			
			//resource.setAuditList(prevAuditMapList);
		}//update close
		
		//update addedby and updatedby for trx log
		XXTrxLog xXTrxLog=null;
		if(trxLogList!=null){
			for (int i=0;i< trxLogList.size();i++) {
				xXTrxLog=trxLogList.get(i);
				if(xXTrxLog!=null){
					if(xXTrxLog.getAddedByUserId()==null || xXTrxLog.getAddedByUserId()==0){
						xXTrxLog.setAddedByUserId(xXPortalUser.getId());
					}
					if(xXTrxLog.getUpdatedByUserId()==null || xXTrxLog.getUpdatedByUserId()==0){
						xXTrxLog.setUpdatedByUserId(xXPortalUser.getId());
					}
				}
				trxLogList.set(i, xXTrxLog);				
			}
		}		
		xaBizUtil.createTrxLog(trxLogList);	

		return vXResource;
	}
	
	public VXResource revokeXResource(VXResource vXResource) {
		if(vXResource==null){
			return vXResource;
		}
		//checks user exists or not
		XXUser xUser = rangerDaoManager.getXXUser().findByUserName(vXResource.getOwner());		
		if(xUser==null){
			throw restErrorUtil.createRESTException("User " +vXResource.getOwner() + " is Not Found",
					MessageEnums.DATA_NOT_FOUND);
		}
		XXPortalUser xXPortalUser= rangerDaoManager.getXXPortalUser().findByLoginId(vXResource.getOwner());		
		if(xXPortalUser==null){
			throw restErrorUtil.createRESTException("User " +vXResource.getOwner() + " is Not Found",
					MessageEnums.DATA_NOT_FOUND);
		}
		
		//checks repository exists or not
		XXAsset xAsset = rangerDaoManager.getXXAsset().findByAssetName(vXResource.getAssetName());
		if (xAsset == null) {
			logger.error("Repository not found for asset : " + vXResource.getAssetName());
			throw restErrorUtil.createRESTException("Repository for which"
					+ " the policy is created, doesn't exist.",MessageEnums.DATA_NOT_FOUND);
		}	
		//checks repository active or not
		if(xAsset.getActiveStatus()==RangerCommonEnums.ACT_STATUS_DISABLED){			
				logger.error("Trying to delete policy in disabled repository");
				throw restErrorUtil.createRESTException("revoke "
						+ " not allowed in disabled repository",MessageEnums.OPER_NO_PERMISSION);
			
		}
		vXResource.setAssetId(xAsset.getId());
		vXResource.setAssetType(xAsset.getAssetType());
		//create resource name/path for HIVE/Hbase policy.
		if (xAsset.getAssetType() == AppConstants.ASSET_HIVE) {
			createResourcePathForHive(vXResource);
		} else if (xAsset.getAssetType() == AppConstants.ASSET_HBASE) {
			createResourcePathForHbase(vXResource);
		}else{
			logger.error("Invalid repository type for grant operation : ");
			throw restErrorUtil.createRESTException(vXResource.getAssetName() +" is not a " 
					+ " valid repository for revoke operation",MessageEnums.OPER_NO_PERMISSION);
		}
		
		//check whether resource exist or not
		SearchCriteria searchCriteria=new SearchCriteria();
		if (xAsset.getAssetType() == AppConstants.ASSET_HIVE) {
			searchCriteria.getParamList().put("assetId", vXResource.getAssetId());
			searchCriteria.getParamList().put("fullname", vXResource.getName());
			searchCriteria.getParamList().put("udfs", vXResource.getUdfs());
			searchCriteria.getParamList().put("tableType", vXResource.getTableType());
			searchCriteria.getParamList().put("columnType", vXResource.getColumnType());
		}else if (xAsset.getAssetType() == AppConstants.ASSET_HBASE) {
			searchCriteria.getParamList().put("assetId", vXResource.getAssetId());
			searchCriteria.getParamList().put("fullname", vXResource.getName());
		}
		
		VXResourceList vXResourceList=xResourceService.searchXResourcesWithoutLogin(searchCriteria);			
		//throw error if resource does not exist
		if(vXResourceList==null || vXResourceList.getListSize()==0){
			logger.error("Resource path not found : " + vXResource.getName());
			throw restErrorUtil.createRESTException("Resource for which"
					+ " revoke is requested, doesn't exist.",MessageEnums.DATA_NOT_FOUND);
		}else{
			for(VXResource vXResourceDB:vXResourceList.getVXResources()){
				if(vXResourceDB!=null){
					vXResource.setId(vXResourceDB.getId());
					vXResource.setPolicyName(vXResourceDB.getPolicyName());
					break;
				}
			}			
		}
		//check whether resource contains multiple path or not
		if(!stringUtil.isEmpty(vXResource.getName())){
			String[] resources=vXResource.getName().trim().split(",");
			if(resources!=null && resources.length>1){
				logger.error("More than one resource found for revoke operation in policy : " + vXResource.getName());
				throw restErrorUtil.createRESTException("We did not find exact match for this resource : " + vXResource.getName(),MessageEnums.INVALID_INPUT_DATA);
			}
		}else{
			throw restErrorUtil.createRESTException("Invalid Resource Name : " + vXResource.getName(),MessageEnums.INVALID_INPUT_DATA);
		}
		
		//checks grantor is admin in resource or not
		List<XXPortalUserRole> xXPortalUserRoleList = rangerDaoManager.getXXPortalUserRole().findByParentId(xXPortalUser.getId());
		List<XXResource> xResourceList=rangerDaoManager.getXXResource().findByAssetId(xAsset.getId());		
		if(xResourceList!=null){
			boolean isAdmin=false;
			if(xXPortalUserRoleList!=null && xXPortalUserRoleList.size()>0){
				for(XXPortalUserRole xXPortalUserRole: xXPortalUserRoleList){
					if(xXPortalUserRole.getUserRole().equalsIgnoreCase(RangerConstants.ROLE_SYS_ADMIN)){
						isAdmin=true;
						break;
					}
				}
			}			
			if(!isAdmin){
				if (xAsset.getAssetType() == AppConstants.ASSET_HIVE) {
					String[] requestResNameList = vXResource.getName().trim().split(",");
					if (stringUtil.isEmpty(vXResource.getUdfs())) {
						int reqTableType = vXResource.getTableType();
						int reqColumnType = vXResource.getColumnType();
						for (String resourceName : requestResNameList) {
							isAdmin=xaBizUtil.matchHivePolicy(resourceName,xResourceList, xUser.getId(),AppConstants.XA_PERM_TYPE_ADMIN,reqTableType,reqColumnType, false);
							if (isAdmin) {
								break;
							}
						}
					} else {
						for (String resourceName : requestResNameList) {
							isAdmin=xaBizUtil.matchHivePolicy(resourceName,xResourceList, xUser.getId(),AppConstants.XA_PERM_TYPE_ADMIN);
							if (isAdmin) {
								break;
							}
						}
					}						
				}else if (xAsset.getAssetType() == AppConstants.ASSET_HBASE) {
					isAdmin=xaBizUtil.matchHbasePolicy(vXResource.getName(),xResourceList,null, xUser.getId(),AppConstants.XA_PERM_TYPE_ADMIN);
				}
			}
			if (!isAdmin) {
				throw restErrorUtil.createRESTException("You're not permitted to perform "
							+ "revoke operation for resource path : " + vXResource.getName(),MessageEnums.OPER_NO_PERMISSION);
			}
		}				
		
		//update addedby and updated by in permmap and auditmap			
		List<VXPermMap> permMapList = vXResource.getPermMapList();	
		if(permMapList==null || permMapList.size()==0){
			throw restErrorUtil.createRESTException("No permission list received for with current revoke request",MessageEnums.DATA_NOT_FOUND);
		}
		VXPermMap vXPermMapTemp=null;		
		XXUser xxUser =null;
		XXGroup xxGroup =null;
		for (int i=0;i< permMapList.size();i++) {
			vXPermMapTemp=permMapList.get(i);
			if(vXPermMapTemp!=null){
				vXPermMapTemp.setResourceId(vXResource.getId());			
				if(stringUtil.isEmpty(vXPermMapTemp.getOwner())){
					vXPermMapTemp.setOwner(vXResource.getOwner());
				}
				if(stringUtil.isEmpty(vXPermMapTemp.getUpdatedBy())){
					vXPermMapTemp.setUpdatedBy(vXResource.getUpdatedBy());
				}
				if(vXPermMapTemp.getPermFor()==AppConstants.XA_PERM_FOR_USER){
					if(vXPermMapTemp.getUserId()==null && !stringUtil.isEmpty(vXPermMapTemp.getUserName())){
						xxUser = rangerDaoManager.getXXUser().findByUserName(vXPermMapTemp.getUserName());
						if (xxUser != null) {
							vXPermMapTemp.setUserId(xxUser.getId());
						} else{
							throw restErrorUtil.createRESTException("User : "+ vXPermMapTemp.getUserName() + " is Not Found",
									MessageEnums.DATA_NOT_FOUND);
						}
					}
				}
				if(vXPermMapTemp.getPermFor()==AppConstants.XA_PERM_FOR_GROUP){
					if(vXPermMapTemp.getGroupId()==null && !stringUtil.isEmpty(vXPermMapTemp.getGroupName())){
						xxGroup = rangerDaoManager.getXXGroup().findByGroupName(
								vXPermMapTemp.getGroupName());
						if (xxGroup != null) {
							vXPermMapTemp.setGroupId(xxGroup.getId());
						}else{
							throw restErrorUtil.createRESTException("Group : "+ vXPermMapTemp.getGroupName() + " is Not Found",
									MessageEnums.DATA_NOT_FOUND);
						} 
					}
				}		
			}	
			permMapList.set(i, vXPermMapTemp);	
		}		
		vXResource.setPermMapList(permMapList);
		
		//permission deletion preprocessing
		XXResource xResource = rangerDaoManager.getXXResource().getById(
				vXResource.getId());
		vXResource.setCreateDate(xResource.getCreateTime());
		vXResource.setUpdateDate(xResource.getUpdateTime());
		List<XXTrxLog> trxLogList = xResourceService.getTransactionLog(
				vXResource, xResource, "delete");

		List<VXPermMap> newPermMapList = vXResource.getPermMapList();
		List<VXPermMap> prevPermMapList = new ArrayList<VXPermMap>();
		List<VXPermMap> permMapsToDelete = new ArrayList<VXPermMap>();
		searchCriteria = new SearchCriteria();
		searchCriteria.addParam("resourceId", vXResource.getId());
		VXPermMapList prevPermMaps = xPermMapService.searchXPermMaps(searchCriteria);		
		if (prevPermMaps != null) {
			prevPermMapList = prevPermMaps.getVXPermMaps();
		}		
		// permission deletion processing start
		String newKey=null;
		String oldKey=null;
		boolean isFound=false;
		VXPermMap newObj=null;
		VXPermMap oldObj=null;
		if (newPermMapList != null && prevPermMapList!=null) {
			for (int i=0;i<newPermMapList.size();i++) {
				newObj=newPermMapList.get(i);				
				if(newObj==null||newObj.getResourceId()==null||newObj.getPermFor()==0||newObj.getPermType()==0 || (newObj.getUserId()==null&&newObj.getGroupId()==null)){
					continue;					
				}
				newKey=null;
				if(newObj.getPermFor()==AppConstants.XA_PERM_FOR_USER){
					newKey=newObj.getResourceId()+"_"+newObj.getPermFor()+"_"+newObj.getUserId()+"_"+newObj.getPermType();
				}
				if(newObj.getPermFor()==AppConstants.XA_PERM_FOR_GROUP){
					newKey=newObj.getResourceId()+"_"+newObj.getPermFor()+"_"+newObj.getGroupId()+"_"+newObj.getPermType();
				}	
				isFound=false;
				oldObj=null;
				for (int j=0;j<prevPermMapList.size();j++) {
					oldObj=prevPermMapList.get(j);
					if(oldObj==null||oldObj.getResourceId()==null||oldObj.getPermFor()==0||oldObj.getPermType()==0|| (oldObj.getUserId()==null&&oldObj.getGroupId()==null)){
						continue;					
					}
					oldKey=null;
					if(oldObj.getPermFor()==AppConstants.XA_PERM_FOR_USER){
						oldKey=oldObj.getResourceId()+"_"+oldObj.getPermFor()+"_"+oldObj.getUserId()+"_"+oldObj.getPermType();
					}
					if(oldObj.getPermFor()==AppConstants.XA_PERM_FOR_GROUP){
						oldKey=oldObj.getResourceId()+"_"+oldObj.getPermFor()+"_"+oldObj.getGroupId()+"_"+oldObj.getPermType();
					}
					if(stringUtil.isEmpty(newKey)|| stringUtil.isEmpty(oldKey)){
						continue;
					}
					if(newKey.equals(oldKey)){
						isFound=true;
						prevPermMapList.remove(j);
						break;
					}
				}//inner for
				if(oldObj!=null){
					if(isFound){					
						permMapsToDelete.add(oldObj);
					}
				}
			}//outer for			
		}// delete permissions list populate end		

		for (VXPermMap permMap : permMapsToDelete) {
			if(permMap!=null){
				xPermMapService.deleteResource(permMap.getId());
				trxLogList.addAll(xPermMapService.getTransactionLog(permMap,"delete"));
			}
		}//permission deletion processing end
		
		if(permMapsToDelete.size()>0){
			vXResource.setUpdateDate(DateUtil.getUTCDate());
		}
		//update addedby and updatedby for trx log
		XXTrxLog xXTrxLog=null;
		if(trxLogList!=null){
			for (int i=0;i< trxLogList.size();i++) {
				xXTrxLog=trxLogList.get(i);
				if(xXTrxLog!=null){
					if(xXTrxLog.getAddedByUserId()==null || xXTrxLog.getAddedByUserId()==0){
						xXTrxLog.setAddedByUserId(xXPortalUser.getId());
					}
					if(xXTrxLog.getUpdatedByUserId()==null || xXTrxLog.getUpdatedByUserId()==0){
						xXTrxLog.setUpdatedByUserId(xXPortalUser.getId());
					}
				}
				trxLogList.set(i, xXTrxLog);				
			}
		}
		
		xaBizUtil.createTrxLog(trxLogList);		
		vXResource.setPermMapList(prevPermMapList);		
		
		return vXResource;
	}
    
	@Override
	public VXLong getXResourceSearchCount(SearchCriteria searchCriteria) {

		VXResourceList resList = super.searchXResources(searchCriteria);

		int count = resList.getListSize();
		VXLong vXLong = new VXLong();
		vXLong.setValue(count);
		return vXLong;
	}
    
}
