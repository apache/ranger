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
import org.apache.ranger.db.XXPolicyRefAccessTypeDao;
import org.apache.ranger.db.XXPolicyRefConditionDao;
import org.apache.ranger.db.XXPolicyRefDataMaskTypeDao;
import org.apache.ranger.db.XXPolicyRefGroupDao;
import org.apache.ranger.db.XXPolicyRefResourceDao;
import org.apache.ranger.db.XXPolicyRefUserDao;
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
import org.apache.ranger.entity.XXPolicyRefUser;
import org.apache.ranger.entity.XXResourceDef;
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
		final Set<String> groupNames      = new HashSet<>();
		final Set<String> userNames       = new HashSet<>();
		final Set<String> accessTypes     = new HashSet<>();
		final Set<String> conditionTypes  = new HashSet<>();
		final Set<String> dataMaskTypes   = new HashSet<>();

		for (List<? extends RangerPolicyItem> policyItems :  getAllPolicyItems(policy)) {
			if (CollectionUtils.isEmpty(policyItems)) {
				continue;
			}

			for (RangerPolicyItem policyItem : policyItems) {
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

		for (String resource : resourceNames) {
			XXResourceDef xResDef = daoMgr.getXXResourceDef().findByNameAndPolicyId(resource, policy.getId());

			if (xResDef == null) {
				throw new Exception(resource + ": is not a valid resource-type. policy='"+  policy.getName() + "' service='"+ policy.getService() + "'");
			}

			XXPolicyRefResource xPolRes = rangerAuditFields.populateAuditFields(new XXPolicyRefResource(), xPolicy);

			xPolRes.setPolicyId(policy.getId());
			xPolRes.setResourceDefId(xResDef.getId());
			xPolRes.setResourceName(resource);

			daoMgr.getXXPolicyRefResource().create(xPolRes);
		}

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

			daoMgr.getXXPolicyRefGroup().create(xPolGroup);
		}

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

			daoMgr.getXXPolicyRefUser().create(xPolUser);
		}

		for (String accessType : accessTypes) {
			XXAccessTypeDef xAccTypeDef = daoMgr.getXXAccessTypeDef().findByNameAndServiceId(accessType, xPolicy.getService());

			if (xAccTypeDef == null) {
				throw new Exception(accessType + ": is not a valid access-type. policy='" + policy.getName() + "' service='" + policy.getService() + "'");
			}

			XXPolicyRefAccessType xPolAccess = rangerAuditFields.populateAuditFields(new XXPolicyRefAccessType(), xPolicy);

			xPolAccess.setPolicyId(policy.getId());
			xPolAccess.setAccessDefId(xAccTypeDef.getId());
			xPolAccess.setAccessTypeName(accessType);

			daoMgr.getXXPolicyRefAccessType().create(xPolAccess);
		}

		for (String condition : conditionTypes) {
			XXPolicyConditionDef xPolCondDef = daoMgr.getXXPolicyConditionDef().findByServiceDefIdAndName(xServiceDef.getId(), condition);

			if (xPolCondDef == null) {
				throw new Exception(condition + ": is not a valid condition-type. policy='"+  xPolicy.getName() + "' service='"+ xPolicy.getService() + "'");
			}

			XXPolicyRefCondition xPolCond = rangerAuditFields.populateAuditFields(new XXPolicyRefCondition(), xPolicy);

			xPolCond.setPolicyId(policy.getId());
			xPolCond.setConditionDefId(xPolCondDef.getId());
			xPolCond.setConditionName(condition);

			daoMgr.getXXPolicyRefCondition().create(xPolCond);
		}

		for (String dataMaskType : dataMaskTypes ) {
			XXDataMaskTypeDef dataMaskDef = daoMgr.getXXDataMaskTypeDef().findByNameAndServiceId(dataMaskType, xPolicy.getService());

			if (dataMaskDef == null) {
				throw new Exception(dataMaskType + ": is not a valid datamask-type. policy='" + policy.getName() + "' service='" + policy.getService() + "'");
			}

			XXPolicyRefDataMaskType xxDataMaskInfo = new XXPolicyRefDataMaskType();

			xxDataMaskInfo.setPolicyId(policy.getId());
			xxDataMaskInfo.setDataMaskDefId(dataMaskDef.getId());
			xxDataMaskInfo.setDataMaskTypeName(dataMaskType);

			daoMgr.getXXPolicyRefDataMaskType().create(xxDataMaskInfo);
		}
	}

	public Boolean cleanupRefTables(RangerPolicy policy) {
		final Long policyId = policy == null ? null : policy.getId();

		if (policyId == null) {
			return false;
		}

		XXPolicyRefResourceDao     xPolResDao      = daoMgr.getXXPolicyRefResource();
		XXPolicyRefGroupDao        xPolGroupDao    = daoMgr.getXXPolicyRefGroup();
		XXPolicyRefUserDao         xPolUserDao     = daoMgr.getXXPolicyRefUser();
		XXPolicyRefAccessTypeDao   xPolAccessDao   = daoMgr.getXXPolicyRefAccessType();
		XXPolicyRefConditionDao    xPolCondDao     = daoMgr.getXXPolicyRefCondition();
		XXPolicyRefDataMaskTypeDao xPolDataMaskDao = daoMgr.getXXPolicyRefDataMaskType();

		for (XXPolicyRefResource resource : xPolResDao.findByPolicyId(policyId)) {
			xPolResDao.remove(resource);
		}

		for(XXPolicyRefGroup group : xPolGroupDao.findByPolicyId(policyId)) {
			xPolGroupDao.remove(group);
		}

		for(XXPolicyRefUser user : xPolUserDao.findByPolicyId(policyId)) {
			xPolUserDao.remove(user);
		}

		for(XXPolicyRefAccessType access : xPolAccessDao.findByPolicyId(policyId)) {
			xPolAccessDao.remove(access);
		}

		for(XXPolicyRefCondition condVal : xPolCondDao.findByPolicyId(policyId)) {
			xPolCondDao.remove(condVal);
		}

		for(XXPolicyRefDataMaskType dataMask : xPolDataMaskDao.findByPolicyId(policyId)) {
			xPolDataMaskDao.remove(dataMask);
		}

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
