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

 package org.apache.ranger.service;

import org.apache.ranger.entity.XXPortalUser;
import org.apache.ranger.view.VXPortalUser;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;

@Service
@Scope("singleton")
public class XPortalUserService extends
		XPortalUserServiceBase<XXPortalUser, VXPortalUser> {

	@Override
	protected void validateForCreate(VXPortalUser vObj) {
		// TODO Auto-generated method stub

	}

	@Override
	protected void validateForUpdate(VXPortalUser vObj, XXPortalUser mObj) {
		// TODO Auto-generated method stub

	}

	public void updateXXPortalUserReferences(long xXPortalUserId){
		daoManager.getXXAsset().updateUserIDReference("added_by_id", xXPortalUserId);
		daoManager.getXXAsset().updateUserIDReference("upd_by_id", xXPortalUserId);
		daoManager.getXXAuditMap().updateUserIDReference("added_by_id", xXPortalUserId);
		daoManager.getXXAuditMap().updateUserIDReference("upd_by_id", xXPortalUserId);
		daoManager.getXXAuthSession().updateUserIDReference("added_by_id", xXPortalUserId);
		daoManager.getXXAuthSession().updateUserIDReference("upd_by_id", xXPortalUserId);
		daoManager.getXXCredentialStore().updateUserIDReference("added_by_id", xXPortalUserId);
		daoManager.getXXCredentialStore().updateUserIDReference("upd_by_id", xXPortalUserId);
		daoManager.getXXGroup().updateUserIDReference("added_by_id", xXPortalUserId);
		daoManager.getXXGroup().updateUserIDReference("upd_by_id", xXPortalUserId);
		daoManager.getXXGroupGroup().updateUserIDReference("added_by_id", xXPortalUserId);
		daoManager.getXXGroupGroup().updateUserIDReference("upd_by_id", xXPortalUserId);
		daoManager.getXXGroupUser().updateUserIDReference("added_by_id", xXPortalUserId);
		daoManager.getXXGroupUser().updateUserIDReference("upd_by_id", xXPortalUserId);
		daoManager.getXXPermMap().updateUserIDReference("added_by_id", xXPortalUserId);
		daoManager.getXXPermMap().updateUserIDReference("upd_by_id", xXPortalUserId);
		daoManager.getXXPolicyExportAudit().updateUserIDReference("added_by_id", xXPortalUserId);
		daoManager.getXXPolicyExportAudit().updateUserIDReference("upd_by_id", xXPortalUserId);
		daoManager.getXXPortalUser().updateUserIDReference("added_by_id", xXPortalUserId);
		daoManager.getXXPortalUser().updateUserIDReference("upd_by_id", xXPortalUserId);
		daoManager.getXXPortalUserRole().updateUserIDReference("added_by_id", xXPortalUserId);
		daoManager.getXXPortalUserRole().updateUserIDReference("upd_by_id", xXPortalUserId);
		daoManager.getXXResource().updateUserIDReference("added_by_id", xXPortalUserId);
		daoManager.getXXResource().updateUserIDReference("upd_by_id", xXPortalUserId);
		daoManager.getXXUser().updateUserIDReference("added_by_id", xXPortalUserId);
		daoManager.getXXUser().updateUserIDReference("upd_by_id", xXPortalUserId);
		//0.5
		daoManager.getXXServiceDef().updateUserIDReference("added_by_id", xXPortalUserId);
		daoManager.getXXServiceDef().updateUserIDReference("upd_by_id", xXPortalUserId);
		daoManager.getXXService().updateUserIDReference("added_by_id", xXPortalUserId);
		daoManager.getXXService().updateUserIDReference("upd_by_id", xXPortalUserId);
		daoManager.getXXPolicy().updateUserIDReference("added_by_id", xXPortalUserId);
		daoManager.getXXPolicy().updateUserIDReference("upd_by_id", xXPortalUserId);
		daoManager.getXXServiceConfigDef().updateUserIDReference("added_by_id", xXPortalUserId);
		daoManager.getXXServiceConfigDef().updateUserIDReference("upd_by_id", xXPortalUserId);
		daoManager.getXXResourceDef().updateUserIDReference("added_by_id", xXPortalUserId);
		daoManager.getXXResourceDef().updateUserIDReference("upd_by_id", xXPortalUserId);
		daoManager.getXXAccessTypeDef().updateUserIDReference("added_by_id", xXPortalUserId);
		daoManager.getXXAccessTypeDef().updateUserIDReference("upd_by_id", xXPortalUserId);
		daoManager.getXXAccessTypeDefGrants().updateUserIDReference("added_by_id", xXPortalUserId);
		daoManager.getXXAccessTypeDefGrants().updateUserIDReference("upd_by_id", xXPortalUserId);
		daoManager.getXXPolicyConditionDef().updateUserIDReference("added_by_id", xXPortalUserId);
		daoManager.getXXPolicyConditionDef().updateUserIDReference("upd_by_id", xXPortalUserId);
		daoManager.getXXContextEnricherDef().updateUserIDReference("added_by_id", xXPortalUserId);
		daoManager.getXXContextEnricherDef().updateUserIDReference("upd_by_id", xXPortalUserId);
		daoManager.getXXEnumDef().updateUserIDReference("added_by_id", xXPortalUserId);
		daoManager.getXXEnumDef().updateUserIDReference("upd_by_id", xXPortalUserId);
		daoManager.getXXEnumElementDef().updateUserIDReference("added_by_id", xXPortalUserId);
		daoManager.getXXEnumElementDef().updateUserIDReference("upd_by_id", xXPortalUserId);
		daoManager.getXXServiceConfigMap().updateUserIDReference("added_by_id", xXPortalUserId);
		daoManager.getXXServiceConfigMap().updateUserIDReference("upd_by_id", xXPortalUserId);
		daoManager.getXXPolicyResource().updateUserIDReference("added_by_id", xXPortalUserId);
		daoManager.getXXPolicyResource().updateUserIDReference("upd_by_id", xXPortalUserId);
		daoManager.getXXPolicyResourceMap().updateUserIDReference("added_by_id", xXPortalUserId);
		daoManager.getXXPolicyResourceMap().updateUserIDReference("upd_by_id", xXPortalUserId);
		daoManager.getXXPolicyItem().updateUserIDReference("added_by_id", xXPortalUserId);
		daoManager.getXXPolicyItem().updateUserIDReference("upd_by_id", xXPortalUserId);
		daoManager.getXXPolicyItemAccess().updateUserIDReference("added_by_id", xXPortalUserId);
		daoManager.getXXPolicyItemAccess().updateUserIDReference("upd_by_id", xXPortalUserId);
		daoManager.getXXPolicyItemCondition().updateUserIDReference("added_by_id", xXPortalUserId);
		daoManager.getXXPolicyItemCondition().updateUserIDReference("upd_by_id", xXPortalUserId);
		daoManager.getXXPolicyItemUserPerm().updateUserIDReference("added_by_id", xXPortalUserId);
		daoManager.getXXPolicyItemUserPerm().updateUserIDReference("upd_by_id", xXPortalUserId);
		daoManager.getXXPolicyItemGroupPerm().updateUserIDReference("added_by_id", xXPortalUserId);
		daoManager.getXXPolicyItemGroupPerm().updateUserIDReference("upd_by_id", xXPortalUserId);
		daoManager.getXXModuleDef().updateUserIDReference("added_by_id", xXPortalUserId);
		daoManager.getXXModuleDef().updateUserIDReference("upd_by_id", xXPortalUserId);
		daoManager.getXXUserPermission().updateUserIDReference("added_by_id", xXPortalUserId);
		daoManager.getXXUserPermission().updateUserIDReference("upd_by_id", xXPortalUserId);
		daoManager.getXXGroupPermission().updateUserIDReference("added_by_id", xXPortalUserId);
		daoManager.getXXGroupPermission().updateUserIDReference("upd_by_id", xXPortalUserId);
		//0.6
		daoManager.getXXTagDef().updateUserIDReference("added_by_id", xXPortalUserId);
		daoManager.getXXTagDef().updateUserIDReference("upd_by_id", xXPortalUserId);
		daoManager.getXXServiceResource().updateUserIDReference("added_by_id", xXPortalUserId);
		daoManager.getXXServiceResource().updateUserIDReference("upd_by_id", xXPortalUserId);
		daoManager.getXXTag().updateUserIDReference("added_by_id", xXPortalUserId);
		daoManager.getXXTag().updateUserIDReference("upd_by_id", xXPortalUserId);
		daoManager.getXXTagResourceMap().updateUserIDReference("added_by_id", xXPortalUserId);
		daoManager.getXXTagResourceMap().updateUserIDReference("upd_by_id", xXPortalUserId);
		//1.0
		daoManager.getXXDataMaskTypeDef().updateUserIDReference("added_by_id", xXPortalUserId);
		daoManager.getXXDataMaskTypeDef().updateUserIDReference("upd_by_id", xXPortalUserId);
		daoManager.getXXPolicyItemDataMaskInfo().updateUserIDReference("added_by_id", xXPortalUserId);
		daoManager.getXXPolicyItemDataMaskInfo().updateUserIDReference("upd_by_id", xXPortalUserId);
		daoManager.getXXPolicyItemRowFilterInfo().updateUserIDReference("added_by_id", xXPortalUserId);
		daoManager.getXXPolicyItemRowFilterInfo().updateUserIDReference("upd_by_id", xXPortalUserId);
		daoManager.getXXUgsyncAuditInfo().updateUserIDReference("added_by_id", xXPortalUserId);
		daoManager.getXXUgsyncAuditInfo().updateUserIDReference("upd_by_id", xXPortalUserId);
		daoManager.getXXPolicyLabels().updateUserIDReference("added_by_id", xXPortalUserId);
		daoManager.getXXPolicyLabels().updateUserIDReference("upd_by_id", xXPortalUserId);
		daoManager.getXXPolicyLabelMap().updateUserIDReference("added_by_id", xXPortalUserId);
		daoManager.getXXPolicyLabelMap().updateUserIDReference("upd_by_id", xXPortalUserId);
		daoManager.getXXPolicyRefCondition().updateUserIDReference("added_by_id", xXPortalUserId);
		daoManager.getXXPolicyRefCondition().updateUserIDReference("upd_by_id", xXPortalUserId);
		daoManager.getXXPolicyRefGroup().updateUserIDReference("added_by_id", xXPortalUserId);
		daoManager.getXXPolicyRefGroup().updateUserIDReference("upd_by_id", xXPortalUserId);
		daoManager.getXXPolicyRefDataMaskType().updateUserIDReference("added_by_id", xXPortalUserId);
		daoManager.getXXPolicyRefDataMaskType().updateUserIDReference("upd_by_id", xXPortalUserId);
		daoManager.getXXPolicyRefResource().updateUserIDReference("added_by_id", xXPortalUserId);
		daoManager.getXXPolicyRefResource().updateUserIDReference("upd_by_id", xXPortalUserId);
		daoManager.getXXPolicyRefUser().updateUserIDReference("added_by_id", xXPortalUserId);
		daoManager.getXXPolicyRefUser().updateUserIDReference("upd_by_id", xXPortalUserId);
		daoManager.getXXPolicyRefAccessType().updateUserIDReference("added_by_id", xXPortalUserId);
		daoManager.getXXPolicyRefAccessType().updateUserIDReference("upd_by_id", xXPortalUserId);
		//2.0
		//Note: skipping x_policy_change_log table as it does not have 'added_by_id' and 'upd_by_id' fields
		daoManager.getXXSecurityZoneRefGroup().updateUserIDReference("added_by_id", xXPortalUserId);
		daoManager.getXXSecurityZoneRefGroup().updateUserIDReference("upd_by_id", xXPortalUserId);
		daoManager.getXXSecurityZoneRefUser().updateUserIDReference("added_by_id", xXPortalUserId);
		daoManager.getXXSecurityZoneRefUser().updateUserIDReference("upd_by_id", xXPortalUserId);
		daoManager.getXXSecurityZoneRefResource().updateUserIDReference("added_by_id", xXPortalUserId);
		daoManager.getXXSecurityZoneRefResource().updateUserIDReference("upd_by_id", xXPortalUserId);
		daoManager.getXXSecurityZoneRefTagService().updateUserIDReference("added_by_id", xXPortalUserId);
		daoManager.getXXSecurityZoneRefTagService().updateUserIDReference("upd_by_id", xXPortalUserId);
		daoManager.getXXSecurityZoneRefService().updateUserIDReference("added_by_id", xXPortalUserId);
		daoManager.getXXSecurityZoneRefService().updateUserIDReference("upd_by_id", xXPortalUserId);
		daoManager.getXXGlobalState().updateUserIDReference("added_by_id", xXPortalUserId);
		daoManager.getXXGlobalState().updateUserIDReference("upd_by_id", xXPortalUserId);
		daoManager.getXXSecurityZoneDao().updateUserIDReference("added_by_id", xXPortalUserId);
		daoManager.getXXSecurityZoneDao().updateUserIDReference("upd_by_id", xXPortalUserId);
		daoManager.getXXRoleRefRole().updateUserIDReference("added_by_id", xXPortalUserId);
		daoManager.getXXRoleRefRole().updateUserIDReference("upd_by_id", xXPortalUserId);
		daoManager.getXXRoleRefGroup().updateUserIDReference("added_by_id", xXPortalUserId);
		daoManager.getXXRoleRefGroup().updateUserIDReference("upd_by_id", xXPortalUserId);
		daoManager.getXXRoleRefUser().updateUserIDReference("added_by_id", xXPortalUserId);
		daoManager.getXXRoleRefUser().updateUserIDReference("upd_by_id", xXPortalUserId);
		daoManager.getXXPolicyRefRole().updateUserIDReference("added_by_id", xXPortalUserId);
		daoManager.getXXPolicyRefRole().updateUserIDReference("upd_by_id", xXPortalUserId);
		daoManager.getXXRole().updateUserIDReference("added_by_id", xXPortalUserId);
		daoManager.getXXRole().updateUserIDReference("upd_by_id", xXPortalUserId);
	}
}
