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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.naming.InvalidNameException;
import javax.naming.ldap.LdapName;
import javax.naming.ldap.Rdn;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;
import org.apache.ranger.common.AppConstants;
import org.apache.ranger.common.DateUtil;
import org.apache.ranger.common.JSONUtil;
import org.apache.ranger.common.MessageEnums;
import org.apache.ranger.common.PropertiesUtil;
import org.apache.ranger.common.RangerCommonEnums;
import org.apache.ranger.common.SearchCriteria;
import org.apache.ranger.common.StringUtil;
import org.apache.ranger.db.RangerDaoManager;
import org.apache.ranger.entity.XXPermMap;
import org.apache.ranger.entity.XXPolicyExportAudit;
import org.apache.ranger.entity.XXPortalUser;
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
							.setHttpRetCode(HttpServletResponse.SC_BAD_REQUEST);
					createPolicyAudit(policyExportAudit);

					throw restErrorUtil.createRESTException(
							"Unauthorized access - Unable to find Common Name from ["
									+ dn + "]",
							MessageEnums.OPER_NOT_ALLOWED_FOR_ENTITY);
				}
			} catch (InvalidNameException e) {
				policyExportAudit
						.setHttpRetCode(HttpServletResponse.SC_BAD_REQUEST);
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
						.setHttpRetCode(HttpServletResponse.SC_BAD_REQUEST);
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
						.setHttpRetCode(HttpServletResponse.SC_NOT_MODIFIED);
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
					.setHttpRetCode(HttpServletResponse.SC_BAD_REQUEST);
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
				.setHttpRetCode(HttpServletResponse.SC_OK);
		createPolicyAudit(policyExportAudit);

		return updatedPolicyStr;
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
							vXTrxLog.setNewValue(tempNewStr.replace(tempNewArr[i], "{\"password\":\"*****\""));
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
}
