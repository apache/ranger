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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ranger.db.RangerDaoManager;
import org.apache.ranger.entity.XXAccessTypeDef;
import org.apache.ranger.entity.XXDataMaskTypeDef;
import org.apache.ranger.entity.XXGroup;
import org.apache.ranger.entity.XXPolicy;
import org.apache.ranger.entity.XXPolicyConditionDef;
import org.apache.ranger.entity.XXPolicyRefAccessType;
import org.apache.ranger.entity.XXPolicyRefCondition;
import org.apache.ranger.entity.XXPolicyRefDataMaskType;
import org.apache.ranger.entity.XXPolicyRefGroup;
import org.apache.ranger.entity.XXPolicyRefResource;
import org.apache.ranger.entity.XXPolicyRefRole;
import org.apache.ranger.entity.XXPolicyRefUser;
import org.apache.ranger.entity.XXResourceDef;
import org.apache.ranger.entity.XXRole;
import org.apache.ranger.entity.XXServiceDef;
import org.apache.ranger.entity.XXUser;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerPolicy.RangerDataMaskPolicyItem;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItem;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItemAccess;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItemCondition;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItemDataMaskInfo;
import org.apache.ranger.plugin.model.RangerRole;
import org.apache.ranger.service.RangerAuditFields;
import org.apache.ranger.service.XUserService;
import org.apache.ranger.view.VXGroup;
import org.apache.ranger.view.VXUser;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;


@Component
public class PolicyRefUpdater {

	private static final Log LOG = LogFactory.getLog(PolicyRefUpdater.class);

	@Autowired
	RangerDaoManager daoMgr;

	@Autowired
	RangerAuditFields<?> rangerAuditFields;

	@Autowired
	XUserMgr xUserMgr;


	@Autowired
	XUserService xUserService;

	 @Autowired
	 RoleDBStore roleStore;

	public void createNewPolMappingForRefTable(RangerPolicy policy, XXPolicy xPolicy, XXServiceDef xServiceDef) throws Exception {
		if(policy == null) {
			return;
		}

		cleanupRefTables(policy);

		final Set<String> resourceNames   = policy.getResources().keySet();
		final Set<String> roleNames       = new HashSet<>();
		final Set<String> groupNames      = new HashSet<>();
		final Set<String> userNames       = new HashSet<>();
		final Set<String> accessTypes     = new HashSet<>();
		final Set<String> conditionTypes  = new HashSet<>();
		final Set<String> dataMaskTypes   = new HashSet<>();
		boolean oldBulkMode = RangerBizUtil.isBulkMode();

		List<RangerPolicy.RangerPolicyItemCondition> rangerPolicyConditions = policy.getConditions();
		if (CollectionUtils.isNotEmpty(rangerPolicyConditions)) {
			for (RangerPolicy.RangerPolicyItemCondition condition : rangerPolicyConditions) {
				conditionTypes.add(condition.getType());
			}
		}

		for (List<? extends RangerPolicyItem> policyItems :  getAllPolicyItems(policy)) {
			if (CollectionUtils.isEmpty(policyItems)) {
				continue;
			}

			for (RangerPolicyItem policyItem : policyItems) {
				roleNames.addAll(policyItem.getRoles());
				groupNames.addAll(policyItem.getGroups());
				userNames.addAll(policyItem.getUsers());

				if (CollectionUtils.isNotEmpty(policyItem.getAccesses())) {
					for (RangerPolicyItemAccess access : policyItem.getAccesses()) {
						accessTypes.add(access.getType());
					}
				}

				if (CollectionUtils.isNotEmpty(policyItem.getConditions())) {
					for (RangerPolicyItemCondition condition : policyItem.getConditions()) {
						conditionTypes.add(condition.getType());
					}
				}

				if (policyItem instanceof RangerDataMaskPolicyItem) {
					RangerPolicyItemDataMaskInfo dataMaskInfo = ((RangerDataMaskPolicyItem) policyItem).getDataMaskInfo();

					dataMaskTypes.add(dataMaskInfo.getDataMaskType());
				}
			}
		}

		List<XXPolicyRefResource> xPolResources = new ArrayList<>();
		for (String resource : resourceNames) {
			XXResourceDef xResDef = daoMgr.getXXResourceDef().findByNameAndPolicyId(resource, policy.getId());

			if (xResDef == null) {
				throw new Exception(resource + ": is not a valid resource-type. policy='"+  policy.getName() + "' service='"+ policy.getService() + "'");
			}

			XXPolicyRefResource xPolRes = rangerAuditFields.populateAuditFields(new XXPolicyRefResource(), xPolicy);

			xPolRes.setPolicyId(policy.getId());
			xPolRes.setResourceDefId(xResDef.getId());
			xPolRes.setResourceName(resource);

			xPolResources.add(xPolRes);
		}
		daoMgr.getXXPolicyRefResource().batchCreate(xPolResources);

		List<XXPolicyRefRole> xPolRoles = new ArrayList<>();
		for (String role : roleNames) {
			if (StringUtils.isBlank(role)) {
				continue;
			}

			XXRole xRole = daoMgr.getXXRole().findByRoleName(role);
			Long roleId = null;
			if (xRole != null) {
				roleId = xRole.getId();
			}
			else {
				RangerBizUtil.setBulkMode(false);
				roleId = createRoleForPolicy(role);
			}
			XXPolicyRefRole xPolRole = rangerAuditFields.populateAuditFields(new XXPolicyRefRole(), xPolicy);

			xPolRole.setPolicyId(policy.getId());
			xPolRole.setRoleId(roleId);
			xPolRole.setRoleName(role);

			xPolRoles.add(xPolRole);
		}
		RangerBizUtil.setBulkMode(oldBulkMode);
		daoMgr.getXXPolicyRefRole().batchCreate(xPolRoles);

		List<XXPolicyRefGroup> xPolGroups = new ArrayList<>();
		for (String group : groupNames) {
			if (StringUtils.isBlank(group)) {
				continue;
			}

			XXGroup xGroup = daoMgr.getXXGroup().findByGroupName(group);
			Long groupId = null;
			if (xGroup != null) {
				groupId = xGroup.getId();
			}
			else {
				RangerBizUtil.setBulkMode(false);
				groupId = createGroupForPolicy(group);
			}

			XXPolicyRefGroup xPolGroup = rangerAuditFields.populateAuditFields(new XXPolicyRefGroup(), xPolicy);

			xPolGroup.setPolicyId(policy.getId());
			xPolGroup.setGroupId(groupId);
			xPolGroup.setGroupName(group);

			xPolGroups.add(xPolGroup);
		}
		RangerBizUtil.setBulkMode(oldBulkMode);
		daoMgr.getXXPolicyRefGroup().batchCreate(xPolGroups);

		List<XXPolicyRefUser> xPolUsers = new ArrayList<>();
		for (String user : userNames) {
			if (StringUtils.isBlank(user)) {
				continue;
			}

			XXUser xUser = daoMgr.getXXUser().findByUserName(user);
			Long userId = null;
			if(xUser != null){
				userId = xUser.getId();
			}
			else {
				RangerBizUtil.setBulkMode(false);

				userId = createUserForPolicy(user);
			}

			XXPolicyRefUser xPolUser = rangerAuditFields.populateAuditFields(new XXPolicyRefUser(), xPolicy);

			xPolUser.setPolicyId(policy.getId());
			xPolUser.setUserId(userId);
			xPolUser.setUserName(user);

			xPolUsers.add(xPolUser);
		}
		RangerBizUtil.setBulkMode(oldBulkMode);
		daoMgr.getXXPolicyRefUser().batchCreate(xPolUsers);

		List<XXPolicyRefAccessType> xPolAccesses = new ArrayList<>();
		for (String accessType : accessTypes) {
			XXAccessTypeDef xAccTypeDef = daoMgr.getXXAccessTypeDef().findByNameAndServiceId(accessType, xPolicy.getService());

			if (xAccTypeDef == null) {
				throw new Exception(accessType + ": is not a valid access-type. policy='" + policy.getName() + "' service='" + policy.getService() + "'");
			}

			XXPolicyRefAccessType xPolAccess = rangerAuditFields.populateAuditFields(new XXPolicyRefAccessType(), xPolicy);

			xPolAccess.setPolicyId(policy.getId());
			xPolAccess.setAccessDefId(xAccTypeDef.getId());
			xPolAccess.setAccessTypeName(accessType);

			xPolAccesses.add(xPolAccess);
		}
		daoMgr.getXXPolicyRefAccessType().batchCreate(xPolAccesses);

		List<XXPolicyRefCondition> xPolConds = new ArrayList<>();
		for (String condition : conditionTypes) {
			XXPolicyConditionDef xPolCondDef = daoMgr.getXXPolicyConditionDef().findByServiceDefIdAndName(xServiceDef.getId(), condition);

			if (xPolCondDef == null) {
				throw new Exception(condition + ": is not a valid condition-type. policy='"+  xPolicy.getName() + "' service='"+ xPolicy.getService() + "'");
			}

			XXPolicyRefCondition xPolCond = rangerAuditFields.populateAuditFields(new XXPolicyRefCondition(), xPolicy);

			xPolCond.setPolicyId(policy.getId());
			xPolCond.setConditionDefId(xPolCondDef.getId());
			xPolCond.setConditionName(condition);

			xPolConds.add(xPolCond);
		}
		daoMgr.getXXPolicyRefCondition().batchCreate(xPolConds);

		List<XXPolicyRefDataMaskType> xxDataMaskInfos = new ArrayList<>();
		for (String dataMaskType : dataMaskTypes ) {
			XXDataMaskTypeDef dataMaskDef = daoMgr.getXXDataMaskTypeDef().findByNameAndServiceId(dataMaskType, xPolicy.getService());

			if (dataMaskDef == null) {
				throw new Exception(dataMaskType + ": is not a valid datamask-type. policy='" + policy.getName() + "' service='" + policy.getService() + "'");
			}

			XXPolicyRefDataMaskType xxDataMaskInfo = new XXPolicyRefDataMaskType();

			xxDataMaskInfo.setPolicyId(policy.getId());
			xxDataMaskInfo.setDataMaskDefId(dataMaskDef.getId());
			xxDataMaskInfo.setDataMaskTypeName(dataMaskType);

			xxDataMaskInfos.add(xxDataMaskInfo);
		}
		daoMgr.getXXPolicyRefDataMaskType().batchCreate(xxDataMaskInfos);
	}

	private Long createUserForPolicy(String user) {
		LOG.warn("User specified in policy does not exist in ranger admin, creating new user, User = " + user);
		VXUser vxUser = new VXUser();
		vxUser.setName(user);
		vxUser.setDescription(user);
		vxUser.setUserSource(1);
		vxUser.setPassword(user+"12345");
		vxUser.setUserRoleList(Arrays.asList("ROLE_USER"));
		VXUser createdXUser= xUserMgr.createXUser(vxUser);
		return createdXUser.getId();
	}

	private Long createGroupForPolicy(String group) {
		LOG.warn("Group specified in policy does not exist in ranger admin, creating new group, Group = " + group);
		VXGroup vxGroup = new VXGroup();
		vxGroup.setName(group);
		VXGroup vxGroupCreated= xUserMgr.createXGroup(vxGroup);
		return vxGroupCreated.getId();
	}

	private Long createRoleForPolicy(String role) throws Exception {
		LOG.warn("Role specified in policy does not exist in ranger admin, creating new role = " + role);

		RangerRole rRole = new RangerRole(role, null, null, null, null);

		xUserMgr.checkAdminAccess();

		RangerRole createdRole= roleStore.createRole(rRole);
		return createdRole.getId();
	}


	public Boolean cleanupRefTables(RangerPolicy policy) {
		final Long policyId = policy == null ? null : policy.getId();

		if (policyId == null) {
			return false;
		}

		daoMgr.getXXPolicyRefResource().deleteByPolicyId(policyId);
		daoMgr.getXXPolicyRefRole().deleteByPolicyId(policyId);
		daoMgr.getXXPolicyRefGroup().deleteByPolicyId(policyId);
		daoMgr.getXXPolicyRefUser().deleteByPolicyId(policyId);
		daoMgr.getXXPolicyRefAccessType().deleteByPolicyId(policyId);
		daoMgr.getXXPolicyRefCondition().deleteByPolicyId(policyId);
		daoMgr.getXXPolicyRefDataMaskType().deleteByPolicyId(policyId);

		return true;
	}

	static List<List<? extends RangerPolicyItem>> getAllPolicyItems(RangerPolicy policy) {
		List<List<? extends RangerPolicyItem>> ret = new ArrayList<>();

		if (CollectionUtils.isNotEmpty(policy.getPolicyItems())) {
			ret.add(policy.getPolicyItems());
		}

		if (CollectionUtils.isNotEmpty(policy.getDenyPolicyItems())) {
			ret.add(policy.getDenyPolicyItems());
		}

		if (CollectionUtils.isNotEmpty(policy.getAllowExceptions())) {
			ret.add(policy.getAllowExceptions());
		}

		if (CollectionUtils.isNotEmpty(policy.getDenyExceptions())) {
			ret.add(policy.getDenyExceptions());
		}

		if (CollectionUtils.isNotEmpty(policy.getDataMaskPolicyItems())) {
			ret.add(policy.getDataMaskPolicyItems());
		}

		if (CollectionUtils.isNotEmpty(policy.getRowFilterPolicyItems())) {
			ret.add(policy.getRowFilterPolicyItems());
		}

		return ret;
	}
}
