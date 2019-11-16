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
import java.util.Set;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
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
import org.apache.ranger.service.RangerAuditFields;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class PolicyRefUpdater {

	@Autowired
	RangerDaoManager daoMgr;

	@Autowired
	RangerAuditFields<?> rangerAuditFields;

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

			if (xRole == null) {
				throw new Exception(role + ": role does not exist. policy='"+  policy.getName() + "' service='"+ policy.getService() + "' role='" + role + "'");
			}

			XXPolicyRefRole xPolRole = rangerAuditFields.populateAuditFields(new XXPolicyRefRole(), xPolicy);

			xPolRole.setPolicyId(policy.getId());
			xPolRole.setRoleId(xRole.getId());
			xPolRole.setRoleName(role);

			xPolRoles.add(xPolRole);
		}
		daoMgr.getXXPolicyRefRole().batchCreate(xPolRoles);

		List<XXPolicyRefGroup> xPolGroups = new ArrayList<>();
		for (String group : groupNames) {
			if (StringUtils.isBlank(group)) {
				continue;
			}

			XXGroup xGroup = daoMgr.getXXGroup().findByGroupName(group);

			if (xGroup == null) {
				throw new Exception(group + ": group does not exist. policy='"+  policy.getName() + "' service='"+ policy.getService() + "' group='" + group + "'");
			}

			XXPolicyRefGroup xPolGroup = rangerAuditFields.populateAuditFields(new XXPolicyRefGroup(), xPolicy);

			xPolGroup.setPolicyId(policy.getId());
			xPolGroup.setGroupId(xGroup.getId());
			xPolGroup.setGroupName(group);

			xPolGroups.add(xPolGroup);
		}
		daoMgr.getXXPolicyRefGroup().batchCreate(xPolGroups);

		List<XXPolicyRefUser> xPolUsers = new ArrayList<>();
		for (String user : userNames) {
			if (StringUtils.isBlank(user)) {
				continue;
			}

			XXUser xUser = daoMgr.getXXUser().findByUserName(user);

			if (xUser == null) {
				throw new Exception(user + ": user does not exist. policy='"+  policy.getName() + "' service='"+ policy.getService() + "' user='" + user +"'");
			}

			XXPolicyRefUser xPolUser = rangerAuditFields.populateAuditFields(new XXPolicyRefUser(), xPolicy);

			xPolUser.setPolicyId(policy.getId());
			xPolUser.setUserId(xUser.getId());
			xPolUser.setUserName(user);

			xPolUsers.add(xPolUser);
		}
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
