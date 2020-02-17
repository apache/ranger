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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ranger.common.RangerCommonEnums;
import org.apache.ranger.db.RangerDaoManager;
import org.apache.ranger.entity.XXAccessTypeDef;
import org.apache.ranger.entity.XXDataMaskTypeDef;
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
import org.apache.ranger.entity.XXServiceDef;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerPolicy.RangerDataMaskPolicyItem;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItem;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItemAccess;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItemCondition;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItemDataMaskInfo;
import org.apache.ranger.plugin.model.RangerRole;
import org.apache.ranger.service.XUserService;
import org.apache.ranger.view.VXGroup;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;


@Component
public class PolicyRefUpdater {

	private static final Log LOG = LogFactory.getLog(PolicyRefUpdater.class);

	@Autowired
	RangerDaoManager daoMgr;

	@Autowired
	XUserMgr xUserMgr;

	@Autowired
	XUserService xUserService;

	 @Autowired
	 RoleDBStore roleStore;

	@Autowired
	RangerBizUtil rangerBizUtil;

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

			XXPolicyRefResource xPolRes = new XXPolicyRefResource();

			xPolRes.setPolicyId(policy.getId());
			xPolRes.setResourceDefId(xResDef.getId());
			xPolRes.setResourceName(resource);

			xPolResources.add(xPolRes);
		}
		daoMgr.getXXPolicyRefResource().batchCreate(xPolResources);

		final Set<String> filteredRoleNames = roleNames.stream()
				.filter(str -> !StringUtils.isBlank(str)).collect(Collectors.toSet());
		Map<String, Long> roleNameIdMap = daoMgr.getXXRole().getIdsByRoleNames(filteredRoleNames);
		if (roleNameIdMap.size() != filteredRoleNames.size()) {
			RangerBizUtil.setBulkMode(false);
			for (String roleName : filteredRoleNames) {
				if (roleNameIdMap.containsKey(roleName)) {
					continue;
				}

				Long roleId = createRoleForPolicy(roleName);
				roleNameIdMap.put(roleName, roleId);
			}
		}

		List<XXPolicyRefRole> xPolRoles = new ArrayList<>();
		for (String role : roleNames) {
			XXPolicyRefRole xPolRole = new XXPolicyRefRole();

			xPolRole.setPolicyId(policy.getId());
			xPolRole.setRoleId(roleNameIdMap.get(role));
			xPolRole.setRoleName(role);

			xPolRoles.add(xPolRole);
		}
		RangerBizUtil.setBulkMode(oldBulkMode);
		daoMgr.getXXPolicyRefRole().batchCreate(xPolRoles);

		final Set<String> filteredGroupNames = groupNames.stream()
				.filter(str -> !StringUtils.isBlank(str)).collect(Collectors.toSet());
		Map<String, Long> groupNameIdMap = daoMgr.getXXGroup().getIdsByGroupNames(filteredGroupNames);
		if (groupNameIdMap.size() != filteredGroupNames.size()) {
			RangerBizUtil.setBulkMode(false);
			for (String groupName : filteredGroupNames) {
				if (groupNameIdMap.containsKey(groupName)) {
					continue;
				}

				Long groupId = createGroupForPolicy(groupName);
				groupNameIdMap.put(groupName, groupId);
			}
		}

		List<XXPolicyRefGroup> xPolGroups = new ArrayList<>();
		for (String group : filteredGroupNames) {
			XXPolicyRefGroup xPolGroup = new XXPolicyRefGroup();

			xPolGroup.setPolicyId(policy.getId());
			xPolGroup.setGroupId(groupNameIdMap.get(group));
			xPolGroup.setGroupName(group);

			xPolGroups.add(xPolGroup);
		}
		RangerBizUtil.setBulkMode(oldBulkMode);
		daoMgr.getXXPolicyRefGroup().batchCreate(xPolGroups);

		final Set<String> filteredUserNames = userNames.stream()
				.filter(str -> !StringUtils.isBlank(str)).collect(Collectors.toSet());
		Map<String, Long> userNameIdMap = daoMgr.getXXUser().getIdsByUserNames(filteredUserNames);
		if (userNameIdMap.size() != filteredUserNames.size()) {
			RangerBizUtil.setBulkMode(false);
			for (String userName : filteredUserNames) {
				if (userNameIdMap.containsKey(userName)) {
					continue;
				}

				Long userId = createUserForPolicy(userName);
				userNameIdMap.put(userName, userId);
			}
		}

		List<XXPolicyRefUser> xPolUsers = new ArrayList<>();
		for (String user : filteredUserNames) {
			XXPolicyRefUser xPolUser = new XXPolicyRefUser();

			xPolUser.setPolicyId(policy.getId());
			xPolUser.setUserId(userNameIdMap.get(user));
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

			XXPolicyRefAccessType xPolAccess = new XXPolicyRefAccessType();

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

			XXPolicyRefCondition xPolCond = new XXPolicyRefCondition();

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
		return xUserMgr.createExternalUser(user).getId();
	}

	private Long createGroupForPolicy(String group) {
		LOG.warn("Group specified in policy does not exist in ranger admin, creating new group, Group = " + group);
		VXGroup vxGroup = new VXGroup();
		vxGroup.setName(group);
		vxGroup.setGroupSource(RangerCommonEnums.GROUP_EXTERNAL);
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
