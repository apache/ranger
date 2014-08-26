/**
 * 
 */
package com.xasecure.service;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.xasecure.common.AppConstants;
import com.xasecure.common.DateUtil;
import com.xasecure.common.MessageEnums;
import com.xasecure.common.PropertiesUtil;
import com.xasecure.common.RESTErrorUtil;
import com.xasecure.common.SearchCriteria;
import com.xasecure.common.StringUtil;
import com.xasecure.db.XADaoManager;
import com.xasecure.entity.XXAsset;
import com.xasecure.entity.XXGroup;
import com.xasecure.entity.XXPermMap;
import com.xasecure.entity.XXResource;
import com.xasecure.entity.XXUser;
import com.xasecure.view.VXAuditMap;
import com.xasecure.view.VXAuditMapList;
import com.xasecure.view.VXPermMap;
import com.xasecure.view.VXPermMapList;
import com.xasecure.view.VXPermObj;
import com.xasecure.view.VXPolicy;
import com.xasecure.view.VXPolicyList;
import com.xasecure.view.VXResource;
import com.xasecure.view.VXResourceList;

/**
 * @author tushar
 * 
 */

@Service
public class XPolicyService extends PublicAPIServiceBase<VXResource, VXPolicy> {
	Logger logger = Logger.getLogger(XPolicyService.class);

	@Autowired
	RESTErrorUtil restErrorUtil;

	@Autowired
	StringUtil stringUtil;

	@Autowired
	XADaoManager xaDaoMgr;

	@Autowired
	XPermMapService xPermMapService;

	@Autowired
	XAuditMapService xAuditMapService;

	@Autowired
	XResourceService xResourceService;

	String version;

	public XPolicyService() {
		version = PropertiesUtil.getProperty("maven.project.version", "");
	}

	public VXPolicy mapXAToPublicObject(VXResource vXResource) {

		VXPolicy vXPolicy = new VXPolicy();
		vXPolicy = super.mapBaseAttributesToPublicObject(vXResource, vXPolicy);

		vXPolicy.setPolicyName(vXResource.getPolicyName());
		vXPolicy.setResourceName(vXResource.getName());
		vXPolicy.setDescription(vXResource.getDescription());
		vXPolicy.setRepositoryName(vXResource.getAssetName());
		vXPolicy.setRepositoryType(AppConstants
				.getLabelFor_AssetType(vXResource.getAssetType()));
		vXPolicy.setPermMapList(mapPermMapToPermObj(vXResource.getPermMapList()));
		vXPolicy.setTables(vXResource.getTables());
		vXPolicy.setColumnFamilies(vXResource.getColumnFamilies());
		vXPolicy.setColumns(vXResource.getColumns());
		vXPolicy.setDatabases(vXResource.getDatabases());
		vXPolicy.setUdfs(vXResource.getUdfs());
		vXPolicy.setTableType(AppConstants.getLabelFor_PolicyType(vXResource
				.getTableType()));
		vXPolicy.setColumnType(AppConstants.getLabelFor_PolicyType(vXResource
				.getColumnType()));
		vXPolicy.setTopologies(vXResource.getTopologies());
		vXPolicy.setServices(vXResource.getServices());

		boolean enable = true;
		if (vXResource.getResourceStatus() == AppConstants.STATUS_DISABLED
				|| vXResource.getResourceStatus() == AppConstants.STATUS_DELETED) {
			enable = false;
		}
		vXPolicy.setEnabled(enable);
		vXPolicy.setRecursive(AppConstants
				.getBooleanFor_BooleanValue(vXResource.getIsRecursive()));

		boolean auditEnable = true;
		if (stringUtil.isEmpty(vXResource.getAuditList())) {
			auditEnable = false;
		}
		vXPolicy.setAuditEnabled(auditEnable);
		vXPolicy.setVersion(version);

		return vXPolicy;
	}

	public VXResource mapPublicToXAObject(VXPolicy vXPolicy,
			int operationContext) {
		VXResource vXResource = new VXResource();
		vXResource = super.mapBaseAttributesToXAObject(vXPolicy, vXResource);

		vXResource.setName(vXPolicy.getResourceName());
		vXResource.setPolicyName(vXPolicy.getPolicyName());
		vXResource.setDescription(vXPolicy.getDescription());
		vXResource.setResourceType(getResourceType(vXPolicy));

		XXAsset xAsset = xaDaoMgr.getXXAsset().findByAssetName(
				vXPolicy.getRepositoryName());
		if (xAsset == null) {
			throw restErrorUtil.createRESTException("The repository for which "
					+ "you're updating policy, doesn't exist.",
					MessageEnums.INVALID_INPUT_DATA);
		}
		vXResource.setAssetId(xAsset.getId());

		if (operationContext == AbstractBaseResourceService.OPERATION_UPDATE_CONTEXT) {
			XXResource xxResource = xaDaoMgr.getXXResource().getById(
					vXPolicy.getId());
			if (xxResource == null) {
				logger.error("No policy found with given Id : "
						+ vXPolicy.getId());
				throw restErrorUtil
						.createRESTException("No Policy found with given Id : "
								+ vXResource.getId(),
								MessageEnums.DATA_NOT_FOUND);
			}
			/*
			 * While updating public object we wont have createDate/updateDate,
			 * so create time, addedById, updatedById, etc. we ll have to take
			 * from existing object
			 */

			xxResource.setUpdateTime(DateUtil.getUTCDate());
			xResourceService
					.mapBaseAttributesToViewBean(xxResource, vXResource);

			SearchCriteria scAuditMap = new SearchCriteria();
			scAuditMap.addParam("resourceId", xxResource.getId());
			VXAuditMapList vXAuditMapList = xAuditMapService
					.searchXAuditMaps(scAuditMap);

			List<VXAuditMap> auditList = new ArrayList<VXAuditMap>();

			if (vXAuditMapList.getListSize() > 0 && vXPolicy.isAuditEnabled()) {
				auditList.addAll(vXAuditMapList.getVXAuditMaps());
			} else if (vXAuditMapList.getListSize() == 0
					&& vXPolicy.isAuditEnabled()) {
				VXAuditMap vXAuditMap = new VXAuditMap();
				vXAuditMap.setAuditType(AppConstants.XA_AUDIT_TYPE_ALL);
				auditList.add(vXAuditMap);

			}

			List<VXPermMap> permMapList = mapPermObjToPermList(
					vXPolicy.getPermMapList(), vXPolicy);

			vXResource.setAuditList(auditList);
			vXResource.setPermMapList(permMapList);

		} else if (operationContext == AbstractBaseResourceService.OPERATION_CREATE_CONTEXT) {
		
			if (vXPolicy.isAuditEnabled()) {
				VXAuditMap vXAuditMap = new VXAuditMap();
				vXAuditMap.setAuditType(AppConstants.XA_AUDIT_TYPE_ALL);
				List<VXAuditMap> auditList = new ArrayList<VXAuditMap>();
				auditList.add(vXAuditMap);

				vXResource.setAuditList(auditList);
			}
			if (!stringUtil.isEmpty(vXPolicy.getPermMapList())) {
				List<VXPermMap> permMapList = mapPermObjToPermList(vXPolicy
						.getPermMapList());
				vXResource.setPermMapList(permMapList);
			}
		}

		vXResource.setIsRecursive(AppConstants.getEnumFor_BooleanValue(vXPolicy
				.isRecursive()));
		vXResource.setDatabases(vXPolicy.getDatabases());
		vXResource.setTables(vXPolicy.getTables());
		vXResource.setColumnFamilies(vXPolicy.getColumnFamilies());
		vXResource.setColumns(vXPolicy.getColumns());
		vXResource.setUdfs(vXPolicy.getUdfs());
		vXResource.setAssetName(vXPolicy.getRepositoryName());
		vXResource.setAssetType(AppConstants.getEnumFor_AssetType(vXPolicy
				.getRepositoryType()));

		int resourceStatus = AppConstants.STATUS_ENABLED;
		if (!vXPolicy.isEnabled()) {
			resourceStatus = AppConstants.STATUS_DISABLED;
		}
		vXResource.setResourceStatus(resourceStatus);
		// Allowing to create policy without checking parent permission
		vXResource.setCheckParentPermission(AppConstants.BOOL_FALSE);
		vXResource.setTopologies(vXPolicy.getTopologies());
		vXResource.setServices(vXPolicy.getServices());
		vXResource.setTableType(AppConstants.getEnumFor_PolicyType(vXPolicy
				.getTableType()));
		vXResource.setColumnType(AppConstants.getEnumFor_PolicyType(vXPolicy
				.getColumnType()));

		return vXResource;
	}

	private List<VXPermMap> mapPermObjToPermList(List<VXPermObj> permObjList,
			VXPolicy vXPolicy) {

		Long resId = vXPolicy.getId();
		List<VXPermMap> permMapList = new ArrayList<VXPermMap>();
		List<VXPermMap> updPermMapList = new ArrayList<VXPermMap>();
		Map<String, VXPermMap> newPermMap = new LinkedHashMap<String, VXPermMap>();
		Random rand = new Random();

		Map<String, XXPermMap> prevPermMap = getPrevPermMap(resId);

		if (permObjList == null) {
			permObjList = new ArrayList<VXPermObj>();
		}
		for (VXPermObj permObj : permObjList) {
			String permGrp = new Date() + " : " + rand.nextInt(9999);

			if (!stringUtil.isEmpty(permObj.getUserList())) {
				int permFor = AppConstants.XA_PERM_FOR_USER;

				for (String user : permObj.getUserList()) {

					XXUser xxUser = xaDaoMgr.getXXUser().findByUserName(user);
					if (xxUser == null) {
						logger.error("No User found with this name : " + user);
						throw restErrorUtil.createRESTException(
								"No User found with name : " + user,
								MessageEnums.DATA_NOT_FOUND);
					}
					Long userId = xxUser.getId();
					for (String permission : permObj.getPermList()) {

						int permType = AppConstants
								.getEnumFor_XAPermType(permission);
						VXPermMap vXPermMap = new VXPermMap();
						vXPermMap.setPermFor(AppConstants.XA_PERM_FOR_USER);
						vXPermMap.setPermGroup(permGrp);
						vXPermMap.setPermType(permType);
						vXPermMap.setUserId(xxUser.getId());
						vXPermMap.setResourceId(resId);
						permMapList.add(vXPermMap);

						StringBuilder uniqueKey = new StringBuilder();
						uniqueKey.append(resId + "_");
						uniqueKey.append(permFor + "_");
						uniqueKey.append(userId + "_");
						uniqueKey.append(permType);
						newPermMap.put(uniqueKey.toString(), vXPermMap);
					}
				}
			}
			if (!stringUtil.isEmpty(permObj.getGroupList())) {
				int permFor = AppConstants.XA_PERM_FOR_GROUP;

				for (String group : permObj.getGroupList()) {

					XXGroup xxGroup = xaDaoMgr.getXXGroup().findByGroupName(
							group);
					if (xxGroup == null) {
						logger.error("No UserGroup found with this name : "
								+ group);
						throw restErrorUtil.createRESTException(
								"No User found with name : " + group,
								MessageEnums.DATA_NOT_FOUND);
					}
					Long grpId = xxGroup.getId();
					for (String permission : permObj.getPermList()) {

						int permType = AppConstants
								.getEnumFor_XAPermType(permission);
						VXPermMap vXPermMap = new VXPermMap();
						vXPermMap.setPermFor(AppConstants.XA_PERM_FOR_GROUP);
						vXPermMap.setPermGroup(permGrp);
						vXPermMap.setPermType(permType);
						vXPermMap.setGroupId(xxGroup.getId());
						vXPermMap.setResourceId(resId);
						permMapList.add(vXPermMap);

						StringBuilder uniqueKey = new StringBuilder();
						uniqueKey.append(resId + "_");
						uniqueKey.append(permFor + "_");
						uniqueKey.append(grpId + "_");
						uniqueKey.append(permType);
						newPermMap.put(uniqueKey.toString(), vXPermMap);
					}
				}
			}
		}

		// Create Newly added permissions and Remove deleted permissions from DB
		if (prevPermMap.size() == 0) {
			updPermMapList.addAll(permMapList);
		} else {
			for (Entry<String, VXPermMap> entry : newPermMap.entrySet()) {
				if (!prevPermMap.containsKey(entry.getKey())) {
					updPermMapList.add(entry.getValue());
				} else {
					VXPermMap vPMap = xPermMapService
							.populateViewBean(prevPermMap.get(entry.getKey()));
					updPermMapList.add(vPMap);
				}
			}
		}
		return updPermMapList;
	}

	private Map<String, XXPermMap> getPrevPermMap(Long resId) {
		List<XXPermMap> xxPermMapList = xaDaoMgr.getXXPermMap()
				.findByResourceId(resId);

		Map<String, XXPermMap> prevPermMap = new LinkedHashMap<String, XXPermMap>();

		for (XXPermMap xxPermMap : xxPermMapList) {
			int permFor = xxPermMap.getPermFor();
			Long userId = xxPermMap.getUserId();
			Long grpId = xxPermMap.getGroupId();
			int permType = xxPermMap.getPermType();

			StringBuilder uniqueKey = new StringBuilder();
			uniqueKey.append(resId + "_");
			uniqueKey.append(permFor + "_");

			if (userId != null) {
				uniqueKey.append(userId + "_");
			} else if (grpId != null) {
				uniqueKey.append(grpId + "_");
			}
			uniqueKey.append(permType);
			prevPermMap.put(uniqueKey.toString(), xxPermMap);
		}

		return prevPermMap;
	}

	public List<VXPermObj> mapPermMapToPermObj(List<VXPermMap> permMapList) {

		List<VXPermObj> permObjList = new ArrayList<VXPermObj>();
		HashMap<String, List<VXPermMap>> sortedPemMap = new HashMap<String, List<VXPermMap>>();

		if (permMapList != null) {
			for (VXPermMap vXPermMap : permMapList) {

				String permGrp = vXPermMap.getPermGroup();
				List<VXPermMap> sortedList = sortedPemMap.get(permGrp);
				if (sortedList == null) {
					sortedList = new ArrayList<VXPermMap>();
					sortedPemMap.put(permGrp, sortedList);
				}
				sortedList.add(vXPermMap);
			}
		}

		for (Entry<String, List<VXPermMap>> entry : sortedPemMap.entrySet()) {
			VXPermObj vXPermObj = new VXPermObj();
			List<String> userList = new ArrayList<String>();
			List<String> groupList = new ArrayList<String>();
			List<String> permList = new ArrayList<String>();
			String ipAddress = "";

			List<VXPermMap> permListForGrp = entry.getValue();

			for (VXPermMap permMap : permListForGrp) {
				if (permMap.getPermFor() == AppConstants.XA_PERM_FOR_USER) {
					if (!userList.contains(permMap.getUserName())) {
						userList.add(permMap.getUserName());
					}
				} else if (permMap.getPermFor() == AppConstants.XA_PERM_FOR_GROUP) {
					if (!groupList.contains(permMap.getGroupName())) {
						groupList.add(permMap.getGroupName());
					}					
				} 
				String perm = AppConstants.getLabelFor_XAPermType(permMap
						.getPermType());
				if (!permList.contains(perm)) {
					permList.add(perm);
				}
				ipAddress = permMap.getIpAddress();
			}
			if (!userList.isEmpty()) {
				vXPermObj.setUserList(userList);
			}
			if (!groupList.isEmpty()) {
				vXPermObj.setGroupList(groupList);
			}
			vXPermObj.setPermList(permList);
			vXPermObj.setIpAddress(ipAddress);

			permObjList.add(vXPermObj);
		}
		return permObjList;
	}

	public VXPolicyList mapToVXPolicyList(VXResourceList vXResourceList) {

		List<VXPolicy> policyList = new ArrayList<VXPolicy>();
		for (VXResource vXAsset : vXResourceList.getVXResources()) {
			VXPolicy vXRepo = mapXAToPublicObject(vXAsset);
			policyList.add(vXRepo);
		}
		VXPolicyList vXPolicyList = new VXPolicyList(policyList);
		return vXPolicyList;
	}

	private List<VXPermMap> mapPermObjToPermList(List<VXPermObj> permObjList) {

		List<VXPermMap> permMapList = new ArrayList<VXPermMap>();
		Random rand = new Random();

		for (VXPermObj permObj : permObjList) {

			if (!stringUtil.isEmpty(permObj.getUserList())) {
				String permGrp = new Date() + " : " + rand.nextInt(9999);
				for (String user : permObj.getUserList()) {

					XXUser xxUser = xaDaoMgr.getXXUser().findByUserName(user);
					if (xxUser == null) {
						logger.error("No User found with this name : " + user);
						throw restErrorUtil.createRESTException(
								"No User found with name : " + user,
								MessageEnums.DATA_NOT_FOUND);
					}
					for (String permission : permObj.getPermList()) {

						VXPermMap vXPermMap = new VXPermMap();
						int permType = AppConstants
								.getEnumFor_XAPermType(permission);
						vXPermMap.setPermFor(AppConstants.XA_PERM_FOR_USER);
						vXPermMap.setPermGroup(permGrp);
						vXPermMap.setPermType(permType);
						vXPermMap.setUserId(xxUser.getId());

						permMapList.add(vXPermMap);
					}
				}
			}
			if (!stringUtil.isEmpty(permObj.getGroupList())) {
				String permGrp = new Date() + " : " + rand.nextInt(9999);
				for (String group : permObj.getGroupList()) {

					XXGroup xxGroup = xaDaoMgr.getXXGroup().findByGroupName(
							group);
					if (xxGroup == null) {
						logger.error("No UserGroup found with this name : "
								+ group);
						throw restErrorUtil.createRESTException(
								"No User found with name : " + group,
								MessageEnums.DATA_NOT_FOUND);
					}

					for (String permission : permObj.getPermList()) {

						VXPermMap vXPermMap = new VXPermMap();
						int permType = AppConstants
								.getEnumFor_XAPermType(permission);
						vXPermMap.setPermFor(AppConstants.XA_PERM_FOR_GROUP);
						vXPermMap.setPermGroup(permGrp);
						vXPermMap.setPermType(permType);
						vXPermMap.setGroupId(xxGroup.getId());

						permMapList.add(vXPermMap);
					}
				}
			}
		}
		return permMapList;
	}

	public List<VXPermMap> updatePermGroup(VXResource vXResource) {

		XXResource xxResource = xaDaoMgr.getXXResource().getById(
				vXResource.getId());
		if (xxResource == null) {
			logger.info("Resource : " + vXResource.getPolicyName()
					+ " Not Found, while updating PermGroup");
			throw restErrorUtil.createRESTException(
					"Resource Not found to update PermGroup",
					MessageEnums.DATA_NOT_FOUND);
		}
		Long resId = vXResource.getId();
		List<VXPermMap> updatedPermMapList = new ArrayList<VXPermMap>();

		SearchCriteria searchCriteria = new SearchCriteria();
		searchCriteria.addParam("resourceId", resId);
		VXPermMapList currentPermMaps = xPermMapService
				.searchXPermMaps(searchCriteria);

		List<VXPermMap> currentPermMapList = currentPermMaps.getVXPermMaps();
		HashMap<String, List<Integer>> userPermMap = new HashMap<String, List<Integer>>();

		for (VXPermMap currentPermMap : currentPermMapList) {
			Long userId = currentPermMap.getUserId();
			Long groupId = currentPermMap.getGroupId();
			int permFor = currentPermMap.getPermFor();
			int permType = currentPermMap.getPermType();
			String uniKey = resId + "_" + permFor;
			if (permFor == AppConstants.XA_PERM_FOR_GROUP) {
				uniKey = uniKey + "_" + groupId;
			} else if (permFor == AppConstants.XA_PERM_FOR_USER) {
				uniKey = uniKey + "_" + userId;
			}

			List<Integer> permList = userPermMap.get(uniKey);
			if (permList == null) {
				permList = new ArrayList<Integer>();
				userPermMap.put(uniKey, permList);
			}
			permList.add(permType);
		}

		List<List<String>> masterKeyList = new ArrayList<List<String>>();
		List<String> proceedKeyList = new ArrayList<String>();
		for (Entry<String, List<Integer>> upMap : userPermMap.entrySet()) {

			if (proceedKeyList.contains(upMap.getKey())) {
				continue;
			}

			List<String> keyList = new ArrayList<String>();
			keyList.add(upMap.getKey());
			proceedKeyList.add(upMap.getKey());

			for (Entry<String, List<Integer>> entry : userPermMap.entrySet()) {

				if (proceedKeyList.contains(entry.getKey())) {
					continue;
				}

				boolean result = compareTwoListElements(upMap.getValue(),
						entry.getValue());
				if (result) {
					keyList.add(entry.getKey());
					proceedKeyList.add(entry.getKey());
				}
			}
			masterKeyList.add(keyList);
		}

		for (List<String> keyList : masterKeyList) {
			Random rand = new Random();
			String permGrp = new Date() + " : " + rand.nextInt(9999);
			for (String key : keyList) {

				SearchCriteria scPermMap = new SearchCriteria();
				String[] keyEle = StringUtils.split(key, "_");
				if (keyEle != null && keyEle.length == 3) {

					int permFor = Integer.parseInt(keyEle[1]);
					int ugId = Integer.parseInt(keyEle[2]);
					scPermMap.addParam("resourceId", resId);
					scPermMap.addParam("permFor", permFor);

					if (permFor == AppConstants.XA_PERM_FOR_GROUP) {
						scPermMap.addParam("groupId", ugId);
					} else if (permFor == AppConstants.XA_PERM_FOR_USER) {
						scPermMap.addParam("userId", ugId);
					}

					VXPermMapList permList = xPermMapService
							.searchXPermMaps(scPermMap);
					for (VXPermMap vXPerm : permList.getVXPermMaps()) {
						vXPerm.setPermGroup(permGrp);
						xPermMapService.updateResource(vXPerm);
						updatedPermMapList.add(vXPerm);
					}
				} else {
					logger.info("variable : keyEle, should fulfill the checked"
							+ " condition, but its not fulfilling required "
							+ "condition. Ignoring appropriate permMap from"
							+ " updating permGroup. Key : " + key
							+ "Resource Id : " + resId);
				}
			}
		}
		return updatedPermMapList;
	}

	private boolean compareTwoListElements(List<?> list1, List<?> list2) {
		if (list1 == null || list2 == null) {
			return false;
		}
		if (list1.size() != list2.size()) {
			return false;
		}
		int listSize = list1.size();
		for (int i = 0; i < listSize; i++) {
			Object obj1 = list1.get(i);
			if (!list2.contains(obj1)) {
				return false;
			}
		}
		return true;
	}

	public int getResourceType(VXPolicy vXPolicy) {
		int resourceType = AppConstants.RESOURCE_PATH;
		if (vXPolicy == null) {
			return resourceType;
		}
		if (!stringUtil.isEmpty(vXPolicy.getDatabases())) {
			resourceType = AppConstants.RESOURCE_DB;
			if (!stringUtil.isEmpty(vXPolicy.getTables())) {
				resourceType = AppConstants.RESOURCE_TABLE;
			}
			if (!stringUtil.isEmpty(vXPolicy.getColumns())) {
				resourceType = AppConstants.RESOURCE_COLUMN;
			}
			if (!stringUtil.isEmpty(vXPolicy.getUdfs())) {
				resourceType = AppConstants.RESOURCE_UDF;
			}
		} else if (!stringUtil.isEmpty(vXPolicy.getTables())) {
			resourceType = AppConstants.RESOURCE_TABLE;
			if (!stringUtil.isEmpty(vXPolicy.getColumnFamilies())) {
				resourceType = AppConstants.RESOURCE_COL_FAM;
			}
			if (!stringUtil.isEmpty(vXPolicy.getColumns())) {
				resourceType = AppConstants.RESOURCE_COLUMN;
			}
		} else if (!stringUtil.isEmpty(vXPolicy.getTopologies())) {
			resourceType = AppConstants.RESOURCE_TOPOLOGY;
			if (!stringUtil.isEmpty(vXPolicy.getServices())) {
				resourceType = AppConstants.RESOURCE_SERVICE_NAME;
			}
		}
		return resourceType;
	}
}