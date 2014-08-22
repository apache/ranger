/**
 * 
 */
package com.xasecure.service;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.Random;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.xasecure.common.AppConstants;
import com.xasecure.common.PropertiesUtil;
import com.xasecure.common.RESTErrorUtil;
import com.xasecure.common.SearchCriteria;
import com.xasecure.common.StringUtil;
import com.xasecure.db.XADaoManager;
import com.xasecure.entity.XXGroup;
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

	String version;

	public XPolicyService() {
		version = PropertiesUtil.getProperty("maven.project.version", "");
	}

	public VXPolicy mapXAToPublicObject(VXResource vXResource) {

		VXPolicy vXPolicy = new VXPolicy();
		vXPolicy = super.mapBaseAttributesToPublicObject(vXResource, vXPolicy);

		vXPolicy.setPolicyName(vXResource.getPolicyName());
		vXPolicy.setResourceName(vXResource.getName());
		vXPolicy.setResourceType(AppConstants
				.getLabelFor_ResourceType(vXResource.getResourceType()));
		vXPolicy.setDescription(vXResource.getDescription());
		vXPolicy.setRepositoryId(vXResource.getAssetId());
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

		vXPolicy.setCheckParentPermission(AppConstants
				.getBooleanFor_BooleanValue(vXResource
						.getCheckParentPermission()));

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
		vXResource.setResourceType(AppConstants
				.getEnumFor_ResourceType(vXPolicy.getResourceType()));
		vXResource.setAssetId(vXPolicy.getRepositoryId());

		if (operationContext == AbstractBaseResourceService.OPERATION_UPDATE_CONTEXT) {

			SearchCriteria scAuditMap = new SearchCriteria();
			scAuditMap.addParam("resourceId", vXPolicy.getId());
			VXAuditMapList vXAuditMapList = xAuditMapService
					.searchXAuditMaps(scAuditMap);
			vXResource.setAuditList(vXAuditMapList.getVXAuditMaps());

			SearchCriteria scPermMap = new SearchCriteria();
			scPermMap.addParam("resourceId", vXPolicy.getId());
			VXPermMapList vXPermMapList = xPermMapService
					.searchXPermMaps(scPermMap);
			vXResource.setPermMapList(vXPermMapList.getVXPermMaps());
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
		vXResource.setCheckParentPermission(AppConstants
				.getEnumFor_BooleanValue(vXPolicy.getCheckParentPermission()));
		vXResource.setTopologies(vXPolicy.getTopologies());
		vXResource.setServices(vXPolicy.getServices());
		vXResource.setTableType(AppConstants.getEnumFor_PolicyType(vXPolicy
				.getTableType()));
		vXResource.setColumnType(AppConstants.getEnumFor_PolicyType(vXPolicy
				.getColumnType()));

		return vXResource;
	}

	private List<VXPermMap> mapPermObjToPermList(List<VXPermObj> permObjList) {

		List<VXPermMap> permMapList = new ArrayList<VXPermMap>();
		Random rand = new Random();

		for (VXPermObj permObj : permObjList) {

			if (!stringUtil.isEmpty(permObj.getUserList())) {
				for (String user : permObj.getUserList()) {

					String permGrp = new Date() + " : " + rand.nextInt(9999);

					XXUser xxUser = xaDaoMgr.getXXUser().findByUserName(user);
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
				for (String group : permObj.getGroupList()) {

					String permGrp = new Date() + " : " + rand.nextInt(9999);

					XXGroup xxGroup = xaDaoMgr.getXXGroup().findByGroupName(
							group);
					for (String permission : permObj.getPermList()) {

						VXPermMap vXPermMap = new VXPermMap();
						int permType = AppConstants
								.getEnumFor_XAPermType(permission);
						vXPermMap.setPermFor(AppConstants.XA_PERM_FOR_GROUP);
						vXPermMap.setPermGroup(permGrp);
						vXPermMap.setPermType(permType);
						vXPermMap.setUserId(xxGroup.getId());

						permMapList.add(vXPermMap);
					}
				}
			}
		}
		return permMapList;
	}

	public List<VXPermObj> mapPermMapToPermObj(List<VXPermMap> permMapList) {

		List<VXPermObj> permObjList = new ArrayList<VXPermObj>();
		HashMap<String, List<VXPermMap>> sortedPemMap = new HashMap<String, List<VXPermMap>>();

		for (VXPermMap vXPermMap : permMapList) {

			String permGrp = vXPermMap.getPermGroup();
			List<VXPermMap> sortedList = sortedPemMap.get(permGrp);
			if (sortedList == null) {
				sortedList = new ArrayList<VXPermMap>();
				sortedPemMap.put(permGrp, sortedList);
			}
			sortedList.add(vXPermMap);
		}

		for (Entry<String, List<VXPermMap>> entry : sortedPemMap.entrySet()) {
			VXPermObj vXPermObj = new VXPermObj();
			List<String> userList = new ArrayList<String>();
			List<String> groupList = new ArrayList<String>();
			List<String> permList = new ArrayList<String>();
			String ipAddress = "";

			for (VXPermMap permMap : entry.getValue()) {
				if (permMap.getPermFor() == AppConstants.XA_PERM_FOR_USER) {
					userList.add(permMap.getUserName());
				} else if (permMap.getPermFor() == AppConstants.XA_PERM_FOR_GROUP) {
					groupList.add(permMap.getGroupName());
				}
				permList.add(AppConstants.getLabelFor_XAPermType(permMap
						.getPermType()));
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

}