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

import javax.servlet.http.HttpServletResponse;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ranger.common.RESTErrorUtil;
import org.apache.ranger.common.RangerCommonEnums;
import org.apache.ranger.common.db.RangerTransactionSynchronizationAdapter;
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
import org.apache.ranger.entity.XXTrxLog;
import org.apache.ranger.entity.XXUser;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerPolicy.RangerDataMaskPolicyItem;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItem;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItemAccess;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItemCondition;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItemDataMaskInfo;
import org.apache.ranger.plugin.model.RangerRole;
import org.apache.ranger.service.RangerAuditFields;
import org.apache.ranger.service.RangerTransactionService;
import org.apache.ranger.service.XGroupService;
import org.apache.ranger.service.XUserService;
import org.apache.ranger.view.VXGroup;
import org.apache.ranger.view.VXResponse;
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

	@Autowired
	RangerBizUtil rangerBizUtil;

	@Autowired
	XGroupService xGroupService;

	@Autowired
	RangerTransactionSynchronizationAdapter rangerTransactionSynchronizationAdapter;

	@Autowired
	RangerTransactionService transactionService;

	@Autowired
	RESTErrorUtil restErrorUtil;

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

		for (String group : groupNames) {
			if (StringUtils.isBlank(group)) {
				continue;
			}

			XXGroup xGroup = daoMgr.getXXGroup().findByGroupName(group);
			Long groupId = null;
			if (xGroup != null) {
				groupId = xGroup.getId();
				groupPolicyAssociation(xPolicy,groupId,group );
			}
			else {
				if(rangerBizUtil.checkAdminAccess()) {
					createGroupForPolicy(group, xPolicy);
				}else {
					VXResponse gjResponse = new VXResponse();
					gjResponse.setStatusCode(HttpServletResponse.SC_BAD_REQUEST);
					gjResponse.setMsgDesc("Operation denied. Group name: "+group + " specified in policy does not exist in ranger admin.");
					throw restErrorUtil.generateRESTException(gjResponse);
				}
			}
		}

		for (String user : userNames) {
			if (StringUtils.isBlank(user)) {
				continue;
			}

			XXUser xUser = daoMgr.getXXUser().findByUserName(user);
			Long userId = null;
			if(xUser != null){
				userId = xUser.getId();
				userPolicyAssociation(xPolicy,userId, user );
			}
			else {
				if(rangerBizUtil.checkAdminAccess()) {
					createUserForPolicy(user,xPolicy);
				}else {
					VXResponse gjResponse = new VXResponse();
					gjResponse.setStatusCode(HttpServletResponse.SC_BAD_REQUEST);
					gjResponse.setMsgDesc("Operation denied. User name: "+user + " specified in policy does not exist in ranger admin.");
					throw restErrorUtil.generateRESTException(gjResponse);
				}
			}

		}

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

	private void createUserForPolicy(String user, XXPolicy xPolicy) {
		LOG.warn("User specified in policy does not exist in ranger admin, creating new user, User = " + user);
		final PolicyUserCreateContext policyUserCreateContext = new PolicyUserCreateContext(user, xPolicy);
		Runnable CreateAndAssociateUser = new Runnable () {
			@Override
			public void run() {
				Runnable realTask = new Runnable () {
					@Override
					public void run() {
						doCreateAndAssociatePolicyUser(policyUserCreateContext);
					}
				};
				transactionService.scheduleToExecuteInOwnTransaction(realTask, 0L);
			}
		};
		rangerTransactionSynchronizationAdapter.executeOnTransactionCommit(CreateAndAssociateUser);
	}

	private void createGroupForPolicy(String group, XXPolicy xPolicy) {
		LOG.warn("Group specified in policy does not exist in ranger admin, creating new group, Group = " + group);
		VXGroup vxGroup = new VXGroup();
		vxGroup.setName(group);
		vxGroup.setDescription(group);
		vxGroup.setGroupSource(RangerCommonEnums.GROUP_EXTERNAL);
		final PolicyGroupCreateContext policyGroupCreateContext = new PolicyGroupCreateContext(vxGroup, xPolicy);
		Runnable createAndAssociatePolicyGroup = new Runnable() {
			@Override
			public void run() {
				Runnable realTask = new Runnable() {
					@Override
					public void run() {
						doCreateAndAssociatePolicyGroup(policyGroupCreateContext);
					}
				};
				transactionService.scheduleToExecuteInOwnTransaction(realTask, 0L);
			}
		};
		rangerTransactionSynchronizationAdapter.executeOnTransactionCommit(createAndAssociatePolicyGroup);

	}

	private Long createRoleForPolicy(String role) throws Exception {
		LOG.warn("Role specified in policy does not exist in ranger admin, creating new role = " + role);

		if (rangerBizUtil.checkAdminAccess()) {
			RangerRole rRole = new RangerRole(role, null, null, null, null);
			RangerRole createdRole = roleStore.createRole(rRole, false);
			return createdRole.getId();
		} else {
			VXResponse gjResponse = new VXResponse();
			gjResponse.setStatusCode(HttpServletResponse.SC_BAD_REQUEST);
			gjResponse.setMsgDesc(
					"Operation denied. Role name: " + role + " specified in policy does not exist in ranger admin.");
			throw restErrorUtil.generateRESTException(gjResponse);
		}
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

	public static List<List<? extends RangerPolicyItem>> getAllPolicyItems(RangerPolicy policy) {
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

	public void groupPolicyAssociation(XXPolicy xPolicy, Long groupId, String groupName) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("===> PolicyRefUpdater.groupPolicyAssociation()");
		}

		XXPolicyRefGroup xPolGroup = rangerAuditFields.populateAuditFields(new XXPolicyRefGroup(), xPolicy);

		xPolGroup.setPolicyId(xPolicy.getId());
		xPolGroup.setGroupId(groupId);
		xPolGroup.setGroupName(groupName);
		daoMgr.getXXPolicyRefGroup().create(xPolGroup);
	}

	private static final class PolicyGroupCreateContext {
		final VXGroup group;
		final XXPolicy xPolicy;

		PolicyGroupCreateContext(VXGroup group, XXPolicy xPolicy) {
			this.group = group;
			this.xPolicy = xPolicy;
		}

		@Override
		public String toString() {
			return "{group=" + group + ", xPolicy=" + xPolicy + "}";
		}
	}

	void doAssociatePolicyGroup(final PolicyGroupCreateContext context) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("===> PolicyRefUpdater.doAssociatePolicyGroup()");
		}
		XXGroup xGroup = daoMgr.getXXGroup().findByGroupName(context.group.getName());

		if (xGroup == null) {
			LOG.error("No Group created!! Irrecoverable error! PolicyGroupContext:[" + context + "]");
		} else {
			try {
				groupPolicyAssociation(context.xPolicy, xGroup.getId(), context.group.getName());
			} catch (Exception exception) {
				LOG.error("Failed to associate group and policy, PolicyGroupContext:[" + context + "]", exception);
			}
		}
	}

	void doCreateAndAssociatePolicyGroup(final PolicyGroupCreateContext context) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("===> PolicyRefUpdater.doCreateAndAssociatePolicyGroup()");
		}
		XXGroup xGroup = daoMgr.getXXGroup().findByGroupName(context.group.getName());

		if (xGroup != null) {
			groupPolicyAssociation(context.xPolicy, xGroup.getId(), context.group.getName());
		} else {
			try {
				// Create group
				VXGroup vXGroup = xGroupService.createXGroupWithOutLogin(context.group);
				if (null != vXGroup) {
					List<XXTrxLog> trxLogList = xGroupService.getTransactionLog(vXGroup, "create");
					for(XXTrxLog xTrxLog : trxLogList) {
						xTrxLog.setAddedByUserId(context.xPolicy.getAddedByUserId());
						xTrxLog.setUpdatedByUserId(context.xPolicy.getAddedByUserId());
					}
					rangerBizUtil.createTrxLog(trxLogList);
				}
			} catch (Exception exception) {
				LOG.error("Failed to create Group or to associate group and policy, PolicyGroupContext:[" + context + "]",
						exception);
			} finally {
				// This transaction may still fail at commit time because another transaction
				// has already created the group
				// So, associate the group to policy in a different transaction
				Runnable associatePolicyGroup = new Runnable() {
					@Override
					public void run() {
						Runnable realTask = new Runnable() {
							@Override
							public void run() {
								doAssociatePolicyGroup(context);
							}
						};
						transactionService.scheduleToExecuteInOwnTransaction(realTask, 0L);
					}
				};
				rangerTransactionSynchronizationAdapter.executeOnTransactionCompletion(associatePolicyGroup);

			}
		}
	}

	private static final class PolicyUserCreateContext {
		final String userName;
		final XXPolicy xPolicy;

		PolicyUserCreateContext(String userName, XXPolicy xPolicy) {
			this.userName = userName;
			this.xPolicy = xPolicy;
		}

		@Override
		public String toString() {
			return "{userName=" + userName + ", xPolicy=" + xPolicy + "}";
		}
	}

	public void userPolicyAssociation(XXPolicy xPolicy, Long userId, String userName) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("===> PolicyRefUpdater.userPolicyAssociation()");
		}

		XXPolicyRefUser xPolUser = rangerAuditFields.populateAuditFields(new XXPolicyRefUser(), xPolicy);

		xPolUser.setPolicyId(xPolicy.getId());
		xPolUser.setUserId(userId);
		xPolUser.setUserName(userName);
		daoMgr.getXXPolicyRefUser().create(xPolUser);
		if(LOG.isDebugEnabled()) {
			LOG.debug("<=== PolicyRefUpdater.userPolicyAssociation()");
		}
	}

	void doAssociatePolicyUser(final PolicyUserCreateContext context) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("===> PolicyRefUpdater.doAssociatePolicyUser()");
		}
		XXUser xUser = daoMgr.getXXUser().findByUserName(context.userName);

		if (xUser == null) {
			LOG.error("No User created!! Irrecoverable error! PolicyUserContext:[" + context + "]");
		} else {
			try {
				userPolicyAssociation(context.xPolicy, xUser.getId(), context.userName);
			} catch (Exception exception) {
				LOG.error("Failed to associate user and policy, PolicyUserContext:[" + context + "]", exception);
			}
		}
	}

	void doCreateAndAssociatePolicyUser(final PolicyUserCreateContext context) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("===> PolicyRefUpdater.doCreateAndAssociatePolicyUser()");
		}
		XXUser xUser = daoMgr.getXXUser().findByUserName(context.userName);

		if (xUser != null) {
			userPolicyAssociation(context.xPolicy, xUser.getId(), context.userName);
		} else {
			try {
				// Create External user
				xUserMgr.createServiceConfigUser(context.userName);
			} catch (Exception exception) {
				LOG.error("Failed to create User or to associate user and policy, PolicyUserContext:[" + context + "]",
						exception);
			} finally {
				// This transaction may still fail at commit time because another transaction
				// has already created the user
				// So, associate the user to policy in a different transaction
				Runnable associatePolicyUser = new Runnable() {
					@Override
					public void run() {
						Runnable realTask = new Runnable() {
							@Override
							public void run() {
								doAssociatePolicyUser(context);
							}
						};
						transactionService.scheduleToExecuteInOwnTransaction(realTask, 0L);
					}
				};
				rangerTransactionSynchronizationAdapter.executeOnTransactionCompletion(associatePolicyUser);
			}
		}

	}
}
