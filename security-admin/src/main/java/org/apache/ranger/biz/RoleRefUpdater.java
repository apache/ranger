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

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ranger.common.MessageEnums;
import org.apache.ranger.common.RESTErrorUtil;
import org.apache.ranger.common.RangerCommonEnums;
import org.apache.ranger.common.db.RangerTransactionSynchronizationAdapter;
import org.apache.ranger.db.RangerDaoManager;
import org.apache.ranger.db.XXRoleRefGroupDao;
import org.apache.ranger.db.XXRoleRefRoleDao;
import org.apache.ranger.db.XXRoleRefUserDao;
import org.apache.ranger.entity.XXGroup;
import org.apache.ranger.entity.XXRole;
import org.apache.ranger.entity.XXRoleRefGroup;
import org.apache.ranger.entity.XXRoleRefRole;
import org.apache.ranger.entity.XXRoleRefUser;
import org.apache.ranger.entity.XXTrxLog;
import org.apache.ranger.entity.XXUser;
import org.apache.ranger.plugin.model.RangerRole;
import org.apache.ranger.service.RangerAuditFields;
import org.apache.ranger.service.RangerTransactionService;
import org.apache.ranger.service.XGroupService;
import org.apache.ranger.service.XUserService;
import org.apache.ranger.view.VXGroup;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;


@Component
public class RoleRefUpdater {
	private static final Log LOG = LogFactory.getLog(RoleRefUpdater.class);

	@Autowired
	RangerDaoManager daoMgr;

	@Autowired
	RangerAuditFields<?> rangerAuditFields;

	@Autowired
	RESTErrorUtil restErrorUtil;

	@Autowired
    XUserMgr xUserMgr;

    @Autowired
    XUserService xUserService;

    @Autowired
    XGroupService xGroupService;

    @Autowired
    RangerTransactionSynchronizationAdapter rangerTransactionSynchronizationAdapter;

	@Autowired
	RangerTransactionService transactionService;

	@Autowired
	RangerBizUtil xaBizUtil;

	public void createNewRoleMappingForRefTable(RangerRole rangerRole, Boolean createNonExistUserGroup) throws Exception {
		if (rangerRole == null) {
			return;
		}

		cleanupRefTables(rangerRole);
		final Long roleId = rangerRole.getId();

		final Set<String> roleUsers = new HashSet<>();
		final Set<String> roleGroups = new HashSet<>();
		final Set<String> roleRoles = new HashSet<>();

		for (RangerRole.RoleMember user : rangerRole.getUsers()) {
			roleUsers.add(user.getName());
		}
		for (RangerRole.RoleMember group : rangerRole.getGroups()) {
			roleGroups.add(group.getName());
		}
		for (RangerRole.RoleMember role : rangerRole.getRoles()) {
			roleRoles.add(role.getName());
		}

		if (CollectionUtils.isNotEmpty(roleUsers)) {
			for (String roleUser : roleUsers) {

				if (StringUtils.isBlank(roleUser)) {
					continue;
				}
				Long userId = null;
				XXUser xUser = daoMgr.getXXUser().findByUserName(roleUser);

				if (xUser == null) {
					if (createNonExistUserGroup && xaBizUtil.checkAdminAccess()) {
						LOG.warn("User specified in role does not exist in ranger admin, creating new user, User = "
								+ roleUser);
						// Schedule another transaction and let this transaction complete (and commit) successfully!
						final RoleUserCreateContext roleUserCreateContext = new RoleUserCreateContext(roleUser, roleId);
						Runnable CreateAndAssociateUser = new Runnable () {
							@Override
							public void run() {
								Runnable realTask = new Runnable () {
									@Override
									public void run() {
										doCreateAndAssociateRoleUser(roleUserCreateContext);
									}
								};
								transactionService.scheduleToExecuteInOwnTransaction(realTask, 0L);
							}
                        };
						rangerTransactionSynchronizationAdapter.executeOnTransactionCommit(CreateAndAssociateUser);

					} else {
						throw restErrorUtil.createRESTException("user with name: " + roleUser + " does not exist ",
								MessageEnums.INVALID_INPUT_DATA);
					}
				}else {
					userId = xUser.getId();
				}

				if(null != userId) {
					userRoleAssociation(roleId,userId,roleUser);
				}
			}
		}

		if (CollectionUtils.isNotEmpty(roleGroups)) {
			for (String roleGroup : roleGroups) {

				if (StringUtils.isBlank(roleGroup)) {
					continue;
				}
				Long groupId = null;
				XXGroup xGroup = daoMgr.getXXGroup().findByGroupName(roleGroup);

				if (xGroup == null) {
					if (createNonExistUserGroup && xaBizUtil.checkAdminAccess()) {
						LOG.warn("Group specified in role does not exist in ranger admin, creating new group, Group = "
								+ roleGroup);
						VXGroup vxGroupNew = new VXGroup();
						vxGroupNew.setName(roleGroup);
						vxGroupNew.setDescription(roleGroup);
						vxGroupNew.setGroupSource(RangerCommonEnums.GROUP_EXTERNAL);
						// Schedule another transaction and let this transaction complete (and commit) successfully!
						final RoleGroupCreateContext roleGroupCreateContext = new RoleGroupCreateContext(vxGroupNew, roleId);

						Runnable createAndAssociateRoleGroup = new Runnable() {
                            @Override
                            public void run() {
                                Runnable realTask = new Runnable() {
                                    @Override
                                    public void run() {
                                        doCreateAndAssociateRoleGroup(roleGroupCreateContext);
                                    }
                                };
                                transactionService.scheduleToExecuteInOwnTransaction(realTask, 0L);
                            }
                        };
						rangerTransactionSynchronizationAdapter.executeOnTransactionCommit(createAndAssociateRoleGroup);

					} else {
						throw restErrorUtil.createRESTException("group with name: " + roleGroup + " does not exist ",
								MessageEnums.INVALID_INPUT_DATA);
					}
				}else {
					groupId = xGroup.getId();
				}

				if(null != groupId) {
					groupRoleAssociation(roleId, groupId, roleGroup);
				}
			}
		}

		if (CollectionUtils.isNotEmpty(roleRoles)) {
			for (String roleRole : roleRoles) {

				if (StringUtils.isBlank(roleRole)) {
					continue;
				}

				XXRole xRole = daoMgr.getXXRole().findByRoleName(roleRole);

				if (xRole == null) {
					throw restErrorUtil.createRESTException("Role with name: " + roleRole + " does not exist ",
							MessageEnums.INVALID_INPUT_DATA);
				}

				XXRoleRefRole xRoleRefRole = rangerAuditFields.populateAuditFieldsForCreate(new XXRoleRefRole());

				xRoleRefRole.setRoleId(roleId);
				xRoleRefRole.setSubRoleId(xRole.getId());
				xRoleRefRole.setSubRoleName(roleRole);
				xRoleRefRole.setSubRoleType(0);
				daoMgr.getXXRoleRefRole().create(xRoleRefRole);
			}
		}

	}

	public Boolean cleanupRefTables(RangerRole rangerRole) {
		final Long roleId = rangerRole.getId();

		if (roleId == null) {
			return false;
		}

		XXRoleRefUserDao xRoleUserDao = daoMgr.getXXRoleRefUser();
		XXRoleRefGroupDao xRoleGroupDao = daoMgr.getXXRoleRefGroup();
		XXRoleRefRoleDao xRoleRoleDao = daoMgr.getXXRoleRefRole();

		for (XXRoleRefUser xxRoleRefUser : xRoleUserDao.findByRoleId(roleId)) {
			xRoleUserDao.remove(xxRoleRefUser);
		}

		for (XXRoleRefGroup xxRoleRefGroup : xRoleGroupDao.findByRoleId(roleId)) {
			xRoleGroupDao.remove(xxRoleRefGroup);
		}

		for (XXRoleRefRole xxRoleRefRole : xRoleRoleDao.findByRoleId(roleId)) {
			xRoleRoleDao.remove(xxRoleRefRole);
		}
		return true;
	}

	public void groupRoleAssociation(Long roleId, Long groupId, String groupName) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("===> groupRoleAssociation()");
		}

		XXRoleRefGroup xRoleRefGroup = rangerAuditFields.populateAuditFieldsForCreate(new XXRoleRefGroup());
		xRoleRefGroup.setRoleId(roleId);
		xRoleRefGroup.setGroupId(groupId);
		xRoleRefGroup.setGroupName(groupName);
		xRoleRefGroup.setGroupType(0);
		daoMgr.getXXRoleRefGroup().create(xRoleRefGroup);
	}

	private static final class RoleGroupCreateContext {
		final VXGroup group;
		final Long roleId;

		RoleGroupCreateContext(VXGroup group, Long roleId) {
			this.group = group;
			this.roleId = roleId;
		}

		@Override
		public String toString() {
			return "{group=" + group + ", roleId=" + roleId + "}";
		}
	}

	void doCreateAndAssociateRoleGroup(final RoleGroupCreateContext context) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("===> doCreateAndAssociateRoleGroup()");
		}
		XXGroup xGroup = daoMgr.getXXGroup().findByGroupName(context.group.getName());

		if (xGroup != null) {
			groupRoleAssociation(context.roleId, xGroup.getId(), context.group.getName());
		} else {
			try {
				// Create group
				VXGroup vXGroup = xGroupService.createXGroupWithOutLogin(context.group);
				if (null != vXGroup) {
					List<XXTrxLog> trxLogList = xGroupService.getTransactionLog(vXGroup, "create");
					xaBizUtil.createTrxLog(trxLogList);
				}
			} catch (Exception exception) {
				LOG.error("Failed to create Group or to associate group and role, RoleGroupContext:[" + context + "]",
						exception);
			} finally {
				// This transaction may still fail at commit time because another transaction
				// has already created the group
				// So, associate the group to role in a different transaction
				Runnable associateRoleGroup = new Runnable() {
                    @Override
					public void run() {
						Runnable realTask = new Runnable() {
							@Override
							public void run() {
								doAssociateRoleGroup(context);
							}
						};
						transactionService.scheduleToExecuteInOwnTransaction(realTask, 0L);
					}
                };
				rangerTransactionSynchronizationAdapter.executeOnTransactionCompletion(associateRoleGroup);
			}
		}
	}

	void doAssociateRoleGroup(final RoleGroupCreateContext context) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("===> doAssociateRoleGroup()");
		}
		XXGroup xGroup = daoMgr.getXXGroup().findByGroupName(context.group.getName());

		if (xGroup == null) {
			LOG.error("No Group created!! Irrecoverable error! RoleGroupContext:[" + context + "]");
		} else {
			try {
				groupRoleAssociation(context.roleId, xGroup.getId(), context.group.getName());
			} catch (Exception exception) {
				LOG.error("Failed to associate group and role, RoleGroupContext:[" + context + "]", exception);
			}
		}
	}

	private static final class RoleUserCreateContext {
		final String userName;
		final Long roleId;

		RoleUserCreateContext(String userName, Long roleId) {
			this.userName = userName;
			this.roleId = roleId;
		}

		@Override
		public String toString() {
			return "{userName=" + userName + ", roleId=" + roleId + "}";
		}
	}

	public void userRoleAssociation(Long roleId, Long userId, String userName) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("===> userRoleAssociation()");
		}
		XXRoleRefUser xRoleRefUser = rangerAuditFields.populateAuditFieldsForCreate(new XXRoleRefUser());
		xRoleRefUser.setRoleId(roleId);
		xRoleRefUser.setUserId(userId);
		xRoleRefUser.setUserName(userName);
		xRoleRefUser.setUserType(0);
		daoMgr.getXXRoleRefUser().create(xRoleRefUser);
		if(LOG.isDebugEnabled()) {
			LOG.debug("<=== userRoleAssociation()");
		}
	}

	void doCreateAndAssociateRoleUser(final RoleUserCreateContext context) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("===> doCreateAndAssociateRoleUser()");
		}
		XXUser xUser = daoMgr.getXXUser().findByUserName(context.userName);

		if (xUser != null) {
			userRoleAssociation(context.roleId, xUser.getId(), context.userName);
		} else {
			try {
				// Create External user
				xUserMgr.createServiceConfigUser(context.userName);
			} catch (Exception exception) {
				LOG.error("Failed to create User or to associate user and role, RoleUserContext:[" + context + "]",
						exception);
			} finally {
				// This transaction may still fail at commit time because another transaction
				// has already created the user
				// So, associate the user to role in a different transaction
				Runnable associateRoleUser = new Runnable() {
					@Override
					public void run() {
						Runnable realTask = new Runnable() {
							@Override
							public void run() {
								doAssociateRoleUser(context);
							}
						};
						transactionService.scheduleToExecuteInOwnTransaction(realTask, 0L);
					}
                };
				rangerTransactionSynchronizationAdapter.executeOnTransactionCompletion(associateRoleUser);
			}
		}

	}

	void doAssociateRoleUser(final RoleUserCreateContext context) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("===> doAssociateRoleUser()");
		}
		XXUser xUser = daoMgr.getXXUser().findByUserName(context.userName);

		if (xUser == null) {
			LOG.error("No User created!! Irrecoverable error! RoleUserContext:[" + context + "]");
		} else {
			try {
				userRoleAssociation(context.roleId, xUser.getId(), context.userName);
			} catch (Exception exception) {
				LOG.error("Failed to associate user and role, RoleUserContext:[" + context + "]", exception);
			}
		}
	}

}
