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

package org.apache.ranger.common;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.ranger.db.RangerDaoManager;
import org.apache.ranger.entity.XXGroup;
import org.apache.ranger.entity.XXUser;
import org.apache.ranger.plugin.model.RangerBaseModelObject;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItemAccess;
import org.apache.ranger.plugin.model.RangerService;
import org.apache.ranger.view.VXAsset;
import org.apache.ranger.view.VXAuditMap;
import org.apache.ranger.view.VXDataObject;
import org.apache.ranger.view.VXPermMap;
import org.apache.ranger.view.VXResource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class ServiceUtil {
	
	static Map<String, Integer> mapServiceTypeToAssetType = new HashMap<String, Integer>();
	static Map<String, Integer> mapAccessTypeToPermType   = new HashMap<String, Integer>();
	
	@Autowired
	JSONUtil jsonUtil;

	@Autowired
	RangerDaoManager xaDaoMgr;

	static {
		mapServiceTypeToAssetType.put("hdfs",  new Integer(RangerCommonEnums.ASSET_HDFS));
		mapServiceTypeToAssetType.put("hbase", new Integer(RangerCommonEnums.ASSET_HBASE));
		mapServiceTypeToAssetType.put("hive",  new Integer(RangerCommonEnums.ASSET_HIVE));
		mapServiceTypeToAssetType.put("knox",  new Integer(RangerCommonEnums.ASSET_KNOX));
		mapServiceTypeToAssetType.put("storm", new Integer(RangerCommonEnums.ASSET_STORM));

		mapAccessTypeToPermType.put("Unknown", 0);
		mapAccessTypeToPermType.put("Reset", 1);
		mapAccessTypeToPermType.put("Read", 2);
		mapAccessTypeToPermType.put("Write", 3);
		mapAccessTypeToPermType.put("Create", 4);
		mapAccessTypeToPermType.put("Delete", 5);
		mapAccessTypeToPermType.put("Admin", 6);
		mapAccessTypeToPermType.put("Obfuscate", 7);
		mapAccessTypeToPermType.put("Mask", 8);
		mapAccessTypeToPermType.put("Execute", 9);
		mapAccessTypeToPermType.put("Select", 10);
		mapAccessTypeToPermType.put("Update", 11);
		mapAccessTypeToPermType.put("Drop", 12);
		mapAccessTypeToPermType.put("Alter", 13);
		mapAccessTypeToPermType.put("Index", 14);
		mapAccessTypeToPermType.put("Lock", 15);
		mapAccessTypeToPermType.put("All", 16);
		mapAccessTypeToPermType.put("Allow", 17);
		mapAccessTypeToPermType.put("submitTopology", 18);
		mapAccessTypeToPermType.put("fileUpload", 19);
		mapAccessTypeToPermType.put("getNimbusConf", 20);
		mapAccessTypeToPermType.put("getClusterInfo", 21);
		mapAccessTypeToPermType.put("fileDownload", 22);
		mapAccessTypeToPermType.put("killTopology", 23);
		mapAccessTypeToPermType.put("rebalance", 24);
		mapAccessTypeToPermType.put("activate", 25);
		mapAccessTypeToPermType.put("deactivate", 26);
		mapAccessTypeToPermType.put("getTopologyConf", 27);
		mapAccessTypeToPermType.put("getTopology", 28);
		mapAccessTypeToPermType.put("getUserTopology", 29);
		mapAccessTypeToPermType.put("getTopologyInfo", 30);
		mapAccessTypeToPermType.put("uploadNewCredentials", 31);
	}

	public RangerService toRangerService(VXAsset asset) {
		if(asset == null) {
			return null;
		}

		RangerService ret = new RangerService();

		dataObjectToRangerObject(asset, ret);

		ret.setType(toServiceType(asset.getAssetType()));
		ret.setName(asset.getName());
		ret.setDescription(asset.getDescription());
		ret.setIsEnabled(asset.getActiveStatus() == RangerCommonEnums.STATUS_ENABLED);
		ret.setConfigs(jsonUtil.jsonToMap(asset.getConfig()));

		return ret;
	}

	public VXAsset toVXAsset(RangerService service) {
		if(service == null) {
			return null;
		}

		VXAsset ret = new VXAsset();

		rangerObjectToDataObject(service, ret);

		ret.setAssetType(toAssetType(service.getType()));
		ret.setName(service.getName());
		ret.setDescription(service.getDescription());
		ret.setActiveStatus(service.getIsEnabled() ? RangerCommonEnums.STATUS_ENABLED : RangerCommonEnums.STATUS_DISABLED);
		ret.setConfig(jsonUtil.readMapToString(service.getConfigs()));

		return ret;
	}

	public RangerPolicy toRangerPolicy(VXResource resource, RangerService service) {
		if(resource == null) {
			return null;
		}

		RangerPolicy ret = new RangerPolicy();

		dataObjectToRangerObject(resource, ret);

		if(service != null) {
			ret.setService(service.getName());
		} else {
			ret.setService(resource.getAssetName());
		}

		ret.setName(resource.getPolicyName());
		ret.setDescription(resource.getDescription());
		ret.setIsEnabled(resource.getResourceStatus() == RangerCommonEnums.STATUS_ENABLED);
		ret.setIsAuditEnabled(resource.getAuditList() != null && resource.getAuditList().size() > 0);

		Boolean isRecursive      = resource.getIsRecursive() == RangerCommonEnums.BOOL_TRUE;
		Boolean isTableExcludes  = resource.getTableType() == RangerCommonEnums.POLICY_EXCLUSION;
		Boolean isColumnExcludes = resource.getColumnType() == RangerCommonEnums.POLICY_EXCLUSION;

		toRangerResourceList(resource.getName(), "path", Boolean.FALSE, isRecursive, ret.getResources());
		toRangerResourceList(resource.getTables(), "table", isTableExcludes, isRecursive, ret.getResources());
		toRangerResourceList(resource.getColumnFamilies(), "column-family", Boolean.FALSE, isRecursive, ret.getResources());
		toRangerResourceList(resource.getColumns(), "column", isColumnExcludes, isRecursive, ret.getResources());
		toRangerResourceList(resource.getDatabases(), "database", Boolean.FALSE, isRecursive, ret.getResources());
		toRangerResourceList(resource.getUdfs(), "udf", Boolean.FALSE, isRecursive, ret.getResources());
		toRangerResourceList(resource.getTopologies(), "topology", Boolean.FALSE, isRecursive, ret.getResources());
		toRangerResourceList(resource.getServices(), "service", Boolean.FALSE, isRecursive, ret.getResources());

		HashMap<String, List<VXPermMap>> sortedPermMap = new HashMap<String, List<VXPermMap>>();

		// re-group the list with permGroup as the key
		if (resource.getPermMapList() != null) {
			for(VXPermMap permMap : resource.getPermMapList()) {
				String          permGrp    = permMap.getPermGroup();
				List<VXPermMap> sortedList = sortedPermMap.get(permGrp);

				if(sortedList == null) {
					sortedList = new ArrayList<VXPermMap>();
					sortedPermMap.put(permGrp, sortedList);
				}

				sortedList.add(permMap);
			}
		}

		for (Entry<String, List<VXPermMap>> entry : sortedPermMap.entrySet()) {
			List<String>                 userList   = new ArrayList<String>();
			List<String>                 groupList  = new ArrayList<String>();
			List<RangerPolicyItemAccess> accessList = new ArrayList<RangerPolicyItemAccess>();
			String                       ipAddress  = null;

			for(VXPermMap permMap : entry.getValue()) {
				if(permMap.getPermFor() == AppConstants.XA_PERM_FOR_USER) {
					String userName = getUserName(permMap);

					if (! userList.contains(userName)) {
						userList.add(userName);
					}
				} else if(permMap.getPermFor() == AppConstants.XA_PERM_FOR_GROUP) {
					String groupName = getGroupName(permMap);

					if (! groupList.contains(groupName)) {
						groupList.add(groupName);
					}					
				} 

				accessList.add(new RangerPolicyItemAccess(toAccessType(permMap.getPermType())));

				ipAddress = permMap.getIpAddress();
			}
			
			RangerPolicy.RangerPolicyItem policyItem = new RangerPolicy.RangerPolicyItem();

			policyItem.setUsers(userList);
			policyItem.setGroups(groupList);
			policyItem.setAccesses(accessList);
			
			if(ipAddress != null && !ipAddress.isEmpty()) {
				RangerPolicy.RangerPolicyItemCondition ipCondition = new RangerPolicy.RangerPolicyItemCondition("ipaddress", ipAddress);

				policyItem.getConditions().add(ipCondition);
			}
			
			ret.getPolicyItems().add(policyItem);
		}

		return ret;
	}

	public VXResource toVXResource(RangerPolicy policy, RangerService service) {
		if(policy == null || service == null) {
			return null;
		}

		VXResource ret = new VXResource();

		rangerObjectToDataObject(policy, ret);

		ret.setAssetName(policy.getService());
		ret.setAssetId(service.getId());
		ret.setAssetType(toAssetType(service.getType()));
		ret.setPolicyName(policy.getName());
		ret.setDescription(policy.getDescription());
		ret.setResourceStatus(policy.getIsEnabled() ? RangerCommonEnums.STATUS_ENABLED : RangerCommonEnums.STATUS_DISABLED);

		List<VXAuditMap> auditList = null;
		if(policy.getIsAuditEnabled()) {
			VXAuditMap auditMap = new VXAuditMap();

			auditMap.setResourceId(policy.getId());
			auditMap.setAuditType(1);

			auditList = new ArrayList<VXAuditMap>();
			auditList.add(auditMap);
		}
		ret.setAuditList(auditList);

		for(Map.Entry<String, RangerPolicy.RangerPolicyResource> e : policy.getResources().entrySet()) {
			RangerPolicy.RangerPolicyResource res       = e.getValue();
			String                            resType   = e.getKey();
			String                            resString = getResourceString(res.getValues());

			if(resType.equalsIgnoreCase("path")) {
				ret.setName(resString);
				ret.setIsRecursive(Boolean.TRUE.equals(res.getIsRecursive()) ? RangerCommonEnums.BOOL_TRUE : RangerCommonEnums.BOOL_FALSE);
			} else if(resType.equalsIgnoreCase("table")) {
				ret.setTables(resString);
				ret.setTableType(Boolean.TRUE.equals(res.getIsExcludes()) ? RangerCommonEnums.POLICY_EXCLUSION : RangerCommonEnums.POLICY_INCLUSION);
			} else if(resType.equalsIgnoreCase("column-family")) {
				ret.setColumnFamilies(resString);
			} else if(resType.equalsIgnoreCase("column")) {
				ret.setColumns(resString);
				ret.setColumnType(Boolean.TRUE.equals(res.getIsExcludes()) ? RangerCommonEnums.POLICY_EXCLUSION : RangerCommonEnums.POLICY_INCLUSION);
			} else if(resType.equalsIgnoreCase("database")) {
				ret.setDatabases(resString);
			} else if(resType.equalsIgnoreCase("udf")) {
				ret.setUdfs(resString);
			} else if(resType.equalsIgnoreCase("topology")) {
				ret.setTopologies(resString);
			} else if(resType.equalsIgnoreCase("service")) {
				ret.setServices(resString);
			}
		}

		List<VXPermMap> permMapList = new ArrayList<VXPermMap>();

		int permGroup = 0;
		for(RangerPolicy.RangerPolicyItem policyItem : policy.getPolicyItems()) {
			String ipAddress = null;
			
			for(RangerPolicy.RangerPolicyItemCondition condition : policyItem.getConditions()) {
				if(condition.getType() == "ipaddress") {
					ipAddress = condition.getValue();
				}

				if(ipAddress != null && !ipAddress.isEmpty()) {
					break; // only 1 IP-address per permMap
				}
			}

			for(String userName : policyItem.getUsers()) {
				for(RangerPolicyItemAccess access : policyItem.getAccesses()) {
					VXPermMap permMap = new VXPermMap();

					permMap.setPermFor(AppConstants.XA_PERM_FOR_USER);
					permMap.setPermGroup(new Integer(permGroup).toString());
					permMap.setUserName(userName);
					permMap.setUserId(getUserId(userName));
					permMap.setPermType(toPermType(access.getType()));
					permMap.setIpAddress(ipAddress);

					permMapList.add(permMap);
				}
			}
			permGroup++;

			for(String groupName : policyItem.getGroups()) {
				for(RangerPolicyItemAccess access : policyItem.getAccesses()) {
					VXPermMap permMap = new VXPermMap();

					permMap.setPermFor(AppConstants.XA_PERM_FOR_GROUP);
					permMap.setPermGroup(new Integer(permGroup).toString());
					permMap.setGroupName(groupName);
					permMap.setGroupId(getGroupId(groupName));
					permMap.setPermType(toPermType(access.getType()));
					permMap.setIpAddress(ipAddress);

					permMapList.add(permMap);
				}
			}
			permGroup++;
		}
		ret.setPermMapList(permMapList);

		return ret;
	}

	private Map<String, RangerPolicy.RangerPolicyResource> toRangerResourceList(String resourceString, String resourceType, Boolean isExcludes, Boolean isRecursive, Map<String, RangerPolicy.RangerPolicyResource> resources) {
		Map<String, RangerPolicy.RangerPolicyResource> ret = resources == null ? new HashMap<String, RangerPolicy.RangerPolicyResource>() : resources;

		if(resourceString != null) {
			RangerPolicy.RangerPolicyResource resource = ret.get(resourceType);

			if(resource == null) {
				resource = new RangerPolicy.RangerPolicyResource();
				resource.setIsExcludes(isExcludes);
				resource.setIsRecursive(isRecursive);

				ret.put(resourceType, resource);
			}

			for(String res : resourceString.split(",")) {
				resource.getValues().add(res);
			}
		}

		return ret;
	}

	public static String toServiceType(int assetType) {
		String ret = null;

		for(Map.Entry<String, Integer> e : mapServiceTypeToAssetType.entrySet()) {
			if(e.getValue().intValue() == assetType) {
				ret = e.getKey();

				break;
			}
		}

		return ret;
	}

	public static Integer toAssetType(String serviceType) {
		Integer ret = mapServiceTypeToAssetType.get(serviceType);

		return ret;
	}

	public static String toAccessType(int permType) {
		String ret = null;

		for(Map.Entry<String, Integer> e : mapAccessTypeToPermType.entrySet()) {
			if(e.getValue().intValue() == permType) {
				ret = e.getKey();

				break;
			}
		}

		return ret;
	}

	public static Integer toPermType(String accessType) {
		Integer ret = null;

		for(Map.Entry<String, Integer> e : mapAccessTypeToPermType.entrySet()) {
			if(e.getKey().equalsIgnoreCase(accessType)) {
				ret = e.getValue();

				break;
			}
		}

		return ret;
	}

	private RangerBaseModelObject dataObjectToRangerObject(VXDataObject dataObject, RangerBaseModelObject rangerObject) {
		RangerBaseModelObject ret = rangerObject;

		ret.setId(dataObject.getId());
		ret.setCreateTime(dataObject.getCreateDate());
		ret.setUpdateTime(dataObject.getUpdateDate());
		ret.setCreatedBy(dataObject.getOwner());
		ret.setUpdatedBy(dataObject.getUpdatedBy());

		return ret;
	}

	private VXDataObject rangerObjectToDataObject(RangerBaseModelObject rangerObject, VXDataObject dataObject) {
		VXDataObject ret = dataObject;

		ret.setId(rangerObject.getId());
		ret.setCreateDate(rangerObject.getCreateTime());
		ret.setUpdateDate(rangerObject.getUpdateTime());
		ret.setOwner(rangerObject.getCreatedBy());
		ret.setUpdatedBy(rangerObject.getUpdatedBy());

		return ret;
	}
	
	private String getResourceString(List<String> values) {
		String ret = null;

		if(values != null) {
			for(String value : values) {
				if(ret == null) {
					ret = value;
				} else if(value != null) {
					ret += ("," + value);
				}
			}
		}

		return ret;
	}

	private String getUserName(VXPermMap permMap) {
		String userName = permMap.getUserName();

		if(userName == null || userName.isEmpty()) {
			Long userId = permMap.getUserId();

			if(userId != null) {
				XXUser xxUser = xaDaoMgr.getXXUser().getById(userId);

				if(xxUser != null) {
					userName = xxUser.getName();
				}
			}
		}

		return userName;
	}

	private String getGroupName(VXPermMap permMap) {
		String groupName = permMap.getGroupName();

		if(groupName == null || groupName.isEmpty()) {
			Long groupId = permMap.getGroupId();

			if(groupId != null) {
				XXGroup xxGroup = xaDaoMgr.getXXGroup().getById(groupId);

				if(xxGroup != null) {
					groupName = xxGroup.getName();
				}
			}
		}
		
		return groupName;
		
	}

	private Long getUserId(String userName) {
		Long userId = null;

		if(userName != null) {
			XXUser xxUser = xaDaoMgr.getXXUser().findByUserName(userName);
	
			if(xxUser != null) {
				userId = xxUser.getId();
			}
		}

		return userId;
	}

	private Long getGroupId(String groupName) {
		Long groupId = null;

		if(groupName != null) {
			XXGroup xxGroup = xaDaoMgr.getXXGroup().findByGroupName(groupName);

			if(xxGroup != null) {
				groupId = xxGroup.getId();
			}
		}

		return groupId;
	}
}
