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

 package org.apache.ranger.db;

/**
 *
 */

import javax.persistence.EntityManager;
import org.apache.log4j.Logger;
import org.apache.ranger.common.AppConstants;
import org.apache.ranger.common.db.BaseDao;

public abstract class RangerDaoManagerBase {
	private static final Logger logger = Logger.getLogger(RangerDaoManagerBase.class);

	abstract public EntityManager getEntityManager();

	public RangerDaoManagerBase() {
	}

	public BaseDao<?> getDaoForClassType(int classType) {
		if (classType == AppConstants.CLASS_TYPE_AUTH_SESS) {
			return getXXAuthSession();
		}
		if (classType == AppConstants.CLASS_TYPE_USER_PROFILE) {
			return getXXPortalUser();
		}
		if (classType == AppConstants.CLASS_TYPE_XA_ASSET) {
			return getXXAsset();
		}
		if (classType == AppConstants.CLASS_TYPE_XA_RESOURCE) {
			return getXXResource();
		}
		if (classType == AppConstants.CLASS_TYPE_XA_CRED_STORE) {
			return getXXCredentialStore();
		}
		if (classType == AppConstants.CLASS_TYPE_XA_GROUP) {
			return getXXGroup();
		}
		if (classType == AppConstants.CLASS_TYPE_XA_USER) {
			return getXXUser();
		}
		if (classType == AppConstants.CLASS_TYPE_XA_GROUP_USER) {
			return getXXGroupUser();
		}
		if (classType == AppConstants.CLASS_TYPE_XA_GROUP_GROUP) {
			return getXXGroupGroup();
		}
		if (classType == AppConstants.CLASS_TYPE_XA_PERM_MAP) {
			return getXXPermMap();
		}
		if (classType == AppConstants.CLASS_TYPE_XA_AUDIT_MAP) {
			return getXXAuditMap();
		}
		if (classType == AppConstants.CLASS_TYPE_XA_POLICY_EXPORT_AUDIT) {
			return getXXPolicyExportAudit();
		}
		if (classType == AppConstants.CLASS_TYPE_TRX_LOG) {
			return getXXTrxLog();
		}
		if (classType == AppConstants.CLASS_TYPE_XA_ACCESS_AUDIT) {
			return getXXAccessAudit();
		}

		if (classType == AppConstants.CLASS_TYPE_RANGER_POLICY) {
			return getXXPolicy();
		}
		if (classType == AppConstants.CLASS_TYPE_XA_SERVICE) {
			return getXXService();
		}
		if (classType == AppConstants.CLASS_TYPE_RANGER_POLICY_ITEM) {
			return getXXPolicyItem();
		}
		if (classType == AppConstants.CLASS_TYPE_XA_SERVICE_DEF) {
			return getXXServiceDef();
		}
		if (classType == AppConstants.CLASS_TYPE_XA_SERVICE_CONFIG_DEF) {
			return getXXServiceConfigDef();
		}
		if (classType == AppConstants.CLASS_TYPE_XA_RESOURCE_DEF) {
			return getXXResourceDef();
		}
		if (classType == AppConstants.CLASS_TYPE_XA_ACCESS_TYPE_DEF) {
			return getXXAccessTypeDef();
		}
		if (classType == AppConstants.CLASS_TYPE_XA_ACCESS_TYPE_DEF_GRANTS) {
			return getXXAccessTypeDefGrants();
		}
		if (classType == AppConstants.CLASS_TYPE_RANGER_POLICY_CONDITION_DEF) {
			return getXXPolicyConditionDef();
		}
		if (classType == AppConstants.CLASS_TYPE_XA_ENUM_DEF) {
			return getXXEnumDef();
		}
		if (classType == AppConstants.CLASS_TYPE_XA_ENUM_ELEMENT_DEF) {
			return getXXEnumElementDef();
		}
		if (classType == AppConstants.CLASS_TYPE_XA_SERVICE_CONFIG_MAP) {
			return getXXServiceConfigMap();
		}
		if (classType == AppConstants.CLASS_TYPE_RANGER_POLICY_RESOURCE) {
			return getXXPolicyResource();
		}
		if (classType == AppConstants.CLASS_TYPE_RANGER_POLICY_RESOURCE_MAP) {
			return getXXPolicyResourceMap();
		}
		if (classType == AppConstants.CLASS_TYPE_RANGER_POLICY_ITEM_ACCESS) {
			return getXXPolicyItemAccess();
		}
		if (classType == AppConstants.CLASS_TYPE_RANGER_POLICY_ITEM_CONDITION) {
			return getXXPolicyItemCondition();
		}
		if (classType == AppConstants.CLASS_TYPE_RANGER_POLICY_ITEM_USER_PERM) {
			return getXXPolicyItemUserPerm();
		}
		if (classType == AppConstants.CLASS_TYPE_RANGER_POLICY_ITEM_GRP_PERM) {
			return getXXPolicyItemGroupPerm();
		}
		if (classType == AppConstants.CLASS_TYPE_XA_DATA_HIST) {
			return getXXDataHist();
		}
		if (classType == AppConstants.CLASS_TYPE_RANGER_POLICY_WITH_ASSIGNED_ID) {
			return getXXPolicyWithAssignedId();
		}
		if (classType == AppConstants.CLASS_TYPE_RANGER_SERVICE_WITH_ASSIGNED_ID) {
			return getXXServiceWithAssignedId();
		}
		if (classType == AppConstants.CLASS_TYPE_RANGER_MODULE_DEF) {
			return getXXModuleDef();
		}
		if (classType == AppConstants.CLASS_TYPE_RANGER_USER_PERMISSION) {
			return getXXUserPermission();
		}
		if (classType == AppConstants.CLASS_TYPE_RANGER_GROUP_PERMISSION) {
			return getXXUserPermission();
		}
		if (classType == AppConstants.CLASS_TYPE_RANGER_SERVICE_DEF_WITH_ASSIGNED_ID) {
			return getXXServiceDefWithAssignedId();
		}
		
		if (classType == AppConstants.CLASS_TYPE_XA_TAG_DEF) {
			return getXXTagDef();
		}
		if (classType == AppConstants.CLASS_TYPE_XA_TAG_ATTR_DEF) {
			return getXXTagAttributeDef();
		}
		if (classType == AppConstants.CLASS_TYPE_XA_SERVICE_RESOURCE) {
			return getXXServiceResource();
		}
		if (classType == AppConstants.CLASS_TYPE_XA_SERVICE_RESOURCE_ELEMENT) {
			return getXXServiceResourceElement();
		}
		if (classType == AppConstants.CLASS_TYPE_XA_SERVICE_RESOURCE_ELEMENT_VALUE) {
			return getXXServiceResourceElementValue();
		}
		if (classType == AppConstants.CLASS_TYPE_XA_TAG) {
			return getXXTag();
		}
		if (classType == AppConstants.CLASS_TYPE_XA_TAG_ATTR) {
			return getXXTagAttribute();
		}
		if (classType == AppConstants.CLASS_TYPE_XA_TAG_RESOURCE_MAP) {
			return getXXTagResourceMap();
		}
		if (classType == AppConstants.CLASS_TYPE_XA_DATAMASK_DEF) {
			return getXXDataMaskTypeDef();
		}
		if (classType == AppConstants.CLASS_TYPE_RANGER_POLICY_ITEM_DATAMASK_INFO) {
			return getXXPolicyItemDataMaskInfo();
		}
		if (classType== AppConstants.CLASS_TYPE_RANGER_POLICY_ITEM_ROWFILTER_INFO) {
			return getXXPolicyItemRowFilterInfo();
		}
		if (classType== AppConstants.CLASS_TYPE_XA_SERVICE_VERSION_INFO) {
			return getXXServiceVersionInfo();
		}
		logger.error("No DaoManager found for classType=" + classType, new Throwable());
		return null;
	}

	public BaseDao<?> getDaoForClassName(String className) {
		if (className.equals("XXDBBase")) {
			return getXXDBBase();
		}
		if (className.equals("XXAuthSession")) {
			return getXXAuthSession();
		}
		if (className.equals("XXPortalUser")) {
			return getXXPortalUser();
		}
		if (className.equals("XXPortalUserRole")) {
			return getXXPortalUserRole();
		}
		if (className.equals("XXAsset")) {
			return getXXAsset();
		}
		if (className.equals("XXResource")) {
			return getXXResource();
		}
		if (className.equals("XXCredentialStore")) {
			return getXXCredentialStore();
		}
		if (className.equals("XXGroup")) {
			return getXXGroup();
		}
		if (className.equals("XXUser")) {
			return getXXUser();
		}
		if (className.equals("XXGroupUser")) {
			return getXXGroupUser();
		}
		if (className.equals("XXGroupGroup")) {
			return getXXGroupGroup();
		}
		if (className.equals("XXPermMap")) {
			return getXXPermMap();
		}
		if (className.equals("XXAuditMap")) {
			return getXXAuditMap();
		}
		if (className.equals("XXPolicyExportAudit")) {
			return getXXPolicyExportAudit();
		}
		if (className.equals("XXTrxLog")) {
			return getXXTrxLog();
		}
		if (className.equals("XXAccessAudit")) {
			return getXXAccessAudit();
		}
		if (className.equals("XXPolicy")) {
			return getXXPolicy();
		}
		if (className.equals("XXService")) {
			return getXXService();
		}
		if (className.equals("XXPolicyItem")) {
			return getXXPolicyItem();
		}
		if (className.equals("XXServiceDef")) {
			return getXXServiceDef();
		}
		if (className.equals("XXServiceConfigDef")) {
			return getXXServiceConfigDef();
		}
		if (className.equals("XXResourceDef")) {
			return getXXResourceDef();
		}
		if (className.equals("XXAccessTypeDef")) {
			return getXXAccessTypeDef();
		}
		if (className.equals("XXAccessTypeDefGrants")) {
			return getXXAccessTypeDefGrants();
		}
		if (className.equals("XXPolicyConditionDef")) {
			return getXXPolicyConditionDef();
		}
		if (className.equals("XXEnumDef")) {
			return getXXEnumDef();
		}
		if (className.equals("XXEnumElementDef")) {
			return getXXEnumElementDef();
		}
		if (className.equals("XXServiceConfigMap")) {
			return getXXServiceConfigMap();
		}
		if (className.equals("XXPolicyResource")) {
			return getXXPolicyResource();
		}
		if (className.equals("XXPolicyResourceMap")) {
			return getXXPolicyResourceMap();
		}
		if (className.equals("XXPolicyItemAccess")) {
			return getXXPolicyItemAccess();
		}
		if (className.equals("XXPolicyItemCondition")) {
			return getXXPolicyItemCondition();
		}
		if (className.equals("XXPolicyItemUserPerm")) {
			return getXXPolicyItemUserPerm();
		}
		if (className.equals("XXPolicyItemGroupPerm")) {
			return getXXPolicyItemGroupPerm();
		}
		if (className.equals("XXDataHist")) {
			return getXXDataHist();
		}
		if (className.equals("XXPolicyWithAssignedId")) {
			return getXXPolicyWithAssignedId();
		}
		if (className.equals("XXServiceWithAssignedId")) {
			return getXXServiceWithAssignedId();
		}
		if (className.equals("XXModuleDef")) {
			return getXXModuleDef();
		}
		if (className.equals("XXUserPermission")) {
			return getXXUserPermission();
		}
		if (className.equals("XXGroupPermission")) {
			return getXXGroupPermission();
		}
		if (className.equals("XXServiceDefWithAssignedId")) {
			return getXXServiceDefWithAssignedId();
		}

		if (className.equals("XXTagDef")) {
			return getXXTagDef();
		}
		if (className.equals("XXTagAttributeDef")) {
			return getXXTagAttributeDef();
		}
		if (className.equals("XXServiceResource")) {
			return getXXServiceResource();
		}
		if (className.equals("XXServiceResourceElement")) {
			return getXXServiceResourceElement();
		}
		if (className.equals("XXServiceResourceElementValue")) {
			return getXXServiceResourceElementValue();
		}
		if (className.equals("XXTag")) {
			return getXXTag();
		}
		if (className.equals("XXTagAttribute")) {
			return getXXTagAttribute();
		}
		if (className.equals("XXTagResourceMap")) {
			return getXXTagResourceMap();
		}
		if (className.equals("XXDataMaskTypeDef")) {
			return getXXDataMaskTypeDef();
		}
		if (className.equals("XXPolicyItemDataMaskInfo")) {
			return getXXPolicyItemDataMaskInfo();
		}
		if (className.equals("XXPolicyItemRowFilterInfo")) {
			return getXXPolicyItemRowFilterInfo();
		}
		if (className.equals("XXServiceVersionInfo")) {
			return getXXServiceVersionInfo();
		}
		if (className.equals("XXPluginInfo")) {
			return getXXPluginInfo();
		}
		if (className.equals("XXPolicyRefCondition")) {
			return getXXPolicyRefCondition();
		}
		if (className.equals("XXPolicyRefGroup")) {
			return getXXPolicyRefGroup();
		}
		if (className.equals("XXPolicyRefDataMaskType")) {
			return getXXPolicyRefDataMaskType();
		}
		if (className.equals("XXPolicyRefResource")) {
			return getXXPolicyRefResource();
		}
		if (className.equals("XXPolicyRefUser")) {
			return getXXPolicyRefUser();
		}
		if (className.equals("XXPolicyRefAccessType")) {
			return getXXPolicyRefAccessType();
		}
		logger.error("No DaoManager found for className=" + className, new Throwable());
		return null;
	}

	public XXDBBaseDao getXXDBBase() {
		return new XXDBBaseDao(this);
	}

	public XXAuthSessionDao getXXAuthSession() {
		return new XXAuthSessionDao(this);
	}

	public XXPortalUserDao getXXPortalUser() {
		return new XXPortalUserDao(this);
	}

	public XXPortalUserRoleDao getXXPortalUserRole() {
		return new XXPortalUserRoleDao(this);
	}

	public XXAssetDao getXXAsset() {
		return new XXAssetDao(this);
	}

	public XXResourceDao getXXResource() {
		return new XXResourceDao(this);
	}

	public XXCredentialStoreDao getXXCredentialStore() {
		return new XXCredentialStoreDao(this);
	}

	public XXGroupDao getXXGroup() {
		return new XXGroupDao(this);
	}

	public XXUserDao getXXUser() {
		return new XXUserDao(this);
	}

	public XXGroupUserDao getXXGroupUser() {
		return new XXGroupUserDao(this);
	}

	public XXGroupGroupDao getXXGroupGroup() {
		return new XXGroupGroupDao(this);
	}

	public XXPermMapDao getXXPermMap() {
		return new XXPermMapDao(this);
	}

	public XXAuditMapDao getXXAuditMap() {
		return new XXAuditMapDao(this);
	}

	public XXPolicyExportAuditDao getXXPolicyExportAudit() {
		return new XXPolicyExportAuditDao(this);
	}

	public XXTrxLogDao getXXTrxLog() {
		return new XXTrxLogDao(this);
	}

	public XXAccessAuditDao getXXAccessAudit() {
		//Load appropriate class based on audit store
		//TODO: Need to fix this, currently hard coding Solr
		
		return new XXAccessAuditDao(this);
	}

	public XXPolicyDao getXXPolicy() {
		return new XXPolicyDao(this);
	}

	public XXServiceDao getXXService() {
		return new XXServiceDao(this);
	}

	public XXPolicyItemDao getXXPolicyItem() {
		return new XXPolicyItemDao(this);
	}

	public XXServiceDefDao getXXServiceDef() {
		return new XXServiceDefDao(this);
	}

	public XXServiceConfigDefDao getXXServiceConfigDef() {
		return new XXServiceConfigDefDao(this);
	}

	public XXResourceDefDao getXXResourceDef() {
		return new XXResourceDefDao(this);
	}

	public XXAccessTypeDefDao getXXAccessTypeDef() {
		return new XXAccessTypeDefDao(this);
	}

	public XXAccessTypeDefGrantsDao getXXAccessTypeDefGrants() {
		return new XXAccessTypeDefGrantsDao(this);
	}

	public XXPolicyConditionDefDao getXXPolicyConditionDef() {
		return new XXPolicyConditionDefDao(this);
	}

	public XXContextEnricherDefDao getXXContextEnricherDef() {
		return new XXContextEnricherDefDao(this);
	}

	public XXEnumDefDao getXXEnumDef() {
		return new XXEnumDefDao(this);
	}

	public XXEnumElementDefDao getXXEnumElementDef() {
		return new XXEnumElementDefDao(this);
	}
	
	public XXServiceConfigMapDao getXXServiceConfigMap() {
		return new XXServiceConfigMapDao(this);
	}
	
	public XXPolicyResourceDao getXXPolicyResource() {
		return new XXPolicyResourceDao(this);
	}
	
	public XXPolicyResourceMapDao getXXPolicyResourceMap() {
		return new XXPolicyResourceMapDao(this);
	}
	
	public XXPolicyItemAccessDao getXXPolicyItemAccess() {
		return new XXPolicyItemAccessDao(this);
	}

	public XXPolicyItemConditionDao getXXPolicyItemCondition() {
		return new XXPolicyItemConditionDao(this);
	}
	
	public XXPolicyItemUserPermDao getXXPolicyItemUserPerm() {
		return new XXPolicyItemUserPermDao(this);
	}
	
	public XXPolicyItemGroupPermDao getXXPolicyItemGroupPerm() {
		return new XXPolicyItemGroupPermDao(this);
	}

	public XXDataHistDao getXXDataHist() {
		return new XXDataHistDao(this);
	}
	
	public XXPolicyWithAssignedIdDao getXXPolicyWithAssignedId() {
		return new XXPolicyWithAssignedIdDao(this);
	}
	
	public XXServiceWithAssignedIdDao getXXServiceWithAssignedId() {
		return new XXServiceWithAssignedIdDao(this);
	}

	public XXModuleDefDao getXXModuleDef(){
		return new XXModuleDefDao(this);
	}

	public XXUserPermissionDao getXXUserPermission(){
		return new XXUserPermissionDao(this);
	}

	public XXGroupPermissionDao getXXGroupPermission(){
		return new XXGroupPermissionDao(this);
	}
	
	public XXServiceDefWithAssignedIdDao getXXServiceDefWithAssignedId() {
		return new XXServiceDefWithAssignedIdDao(this);
	}

	public XXTagDefDao getXXTagDef() {
		return new XXTagDefDao(this);
	}

	public XXTagAttributeDefDao getXXTagAttributeDef() {
		return new XXTagAttributeDefDao(this);
	}

	public XXServiceResourceDao getXXServiceResource() {
		return new XXServiceResourceDao(this);
	}

	public XXServiceResourceElementDao getXXServiceResourceElement() {
		return new XXServiceResourceElementDao(this);
	}

	public XXServiceResourceElementValueDao getXXServiceResourceElementValue() {
		return new XXServiceResourceElementValueDao(this);
	}

	public XXTagDao getXXTag() {
		return new XXTagDao(this);
	}

	public XXTagAttributeDao getXXTagAttribute() {
		return new XXTagAttributeDao(this);
	}

	public XXTagResourceMapDao getXXTagResourceMap() {
		return new XXTagResourceMapDao(this);
	}

	public XXDataMaskTypeDefDao getXXDataMaskTypeDef() { return new XXDataMaskTypeDefDao(this); }

	public XXPolicyItemDataMaskInfoDao getXXPolicyItemDataMaskInfo() {
		return new XXPolicyItemDataMaskInfoDao(this);
	}

	public XXPolicyItemRowFilterInfoDao getXXPolicyItemRowFilterInfo() {
		return new XXPolicyItemRowFilterInfoDao(this);
	}

	public XXServiceVersionInfoDao getXXServiceVersionInfo() {
		return new XXServiceVersionInfoDao(this);
	}

	public XXPluginInfoDao getXXPluginInfo() {
		return new XXPluginInfoDao(this);
	}

	public XXPolicyRefConditionDao getXXPolicyRefCondition() {
		return new XXPolicyRefConditionDao(this);
	}

	public XXPolicyRefGroupDao getXXPolicyRefGroup() {
		return new XXPolicyRefGroupDao(this);
	}

	public XXPolicyRefDataMaskTypeDao getXXPolicyRefDataMaskType() {
		return new XXPolicyRefDataMaskTypeDao(this);
	}

	public XXPolicyRefResourceDao getXXPolicyRefResource() {
		return new XXPolicyRefResourceDao(this);
	}

	public XXPolicyRefUserDao getXXPolicyRefUser() {
		return new XXPolicyRefUserDao(this);
	}

	public XXPolicyRefAccessTypeDao getXXPolicyRefAccessType() {
		return new XXPolicyRefAccessTypeDao(this);
	}
}

