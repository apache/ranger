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

import java.util.Map;
import java.util.Set;

import org.apache.commons.collections.CollectionUtils;
import org.apache.ranger.common.MessageEnums;
import org.apache.ranger.common.PropertiesUtil;
import org.apache.ranger.common.SearchField;
import org.apache.ranger.common.db.RangerTransactionSynchronizationAdapter;
import org.apache.ranger.entity.XXGroup;
import org.apache.ranger.entity.XXGroupUser;
import org.apache.ranger.entity.XXPortalUser;
import org.apache.ranger.ugsyncutil.model.GroupUserInfo;
import org.apache.ranger.view.VXGroupUser;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;

@Service
@Scope("singleton")
public class XGroupUserService extends
		XGroupUserServiceBase<XXGroupUser, VXGroupUser> {

	private final Long createdByUserId;

	@Autowired
	RangerTransactionSynchronizationAdapter transactionSynchronizationAdapter;
	

	public XGroupUserService() {
		searchFields.add(new SearchField("xUserId", "obj.userId",
				SearchField.DATA_TYPE.INTEGER, SearchField.SEARCH_TYPE.FULL));
		searchFields.add(new SearchField("xGroupId", "obj.parentGroupId",
				SearchField.DATA_TYPE.INTEGER, SearchField.SEARCH_TYPE.FULL));
		createdByUserId = PropertiesUtil.getLongProperty("ranger.xuser.createdByUserId", 1);
		
	}

	@Override
	protected void validateForCreate(VXGroupUser vObj) {
		// TODO Auto-generated method stub

	}

	@Override
	protected void validateForUpdate(VXGroupUser vObj, XXGroupUser mObj) {
		// TODO Auto-generated method stub

	}
	
	public VXGroupUser createXGroupUserWithOutLogin(VXGroupUser vxGroupUser) {
		boolean groupUserMappingExists = true;
		XXGroupUser xxGroupUser = daoManager.getXXGroupUser().findByGroupNameAndUserId(vxGroupUser.getName(), vxGroupUser.getUserId());
		if (xxGroupUser == null) {
			xxGroupUser = new XXGroupUser();
			groupUserMappingExists = false;
		}

		XXGroup xGroup = daoManager.getXXGroup().findByGroupName(vxGroupUser.getName());
		vxGroupUser.setParentGroupId(xGroup.getId());
		xxGroupUser = mapViewToEntityBean(vxGroupUser, xxGroupUser, 0);
		XXPortalUser xXPortalUser = daoManager.getXXPortalUser().getById(createdByUserId);
		if (xXPortalUser != null) {
			xxGroupUser.setAddedByUserId(createdByUserId);
			xxGroupUser.setUpdatedByUserId(createdByUserId);
		}
		if (groupUserMappingExists) {
			xxGroupUser = getDao().update(xxGroupUser);
		} else {
			xxGroupUser = getDao().create(xxGroupUser);
		}
		vxGroupUser = postCreate(xxGroupUser);
		return vxGroupUser;
	}

	public void createOrDeleteXGroupUsers(GroupUserInfo groupUserInfo, Map<String, Long> usersFromDB) {
		if (logger.isDebugEnabled()) {
			logger.debug("==>> createOrDeleteXGroupUsers for " + groupUserInfo.getGroupName());
			Long mb = 1024L * 1024L;
			logger.debug("==>> createOrDeleteXGroupUsers: Max memory = " + Runtime.getRuntime().maxMemory() / mb + " Free memory = " + Runtime.getRuntime().freeMemory() / mb
					+ " Total memory = " + Runtime.getRuntime().totalMemory() / mb);
		}
		String groupName = groupUserInfo.getGroupName();
		if (CollectionUtils.isEmpty(groupUserInfo.getAddUsers()) && CollectionUtils.isEmpty(groupUserInfo.getDelUsers())) {
			logger.info("Group memberships for source are empty for " + groupName);
			return;
		}
		XXGroup xxGroup = daoManager.getXXGroup().findByGroupName(groupName);
		if (xxGroup == null) {
			if (logger.isDebugEnabled()) {
				logger.debug("createOrDeleteXGroupUsers(): groupname =  " + groupName
						+ " doesn't exist in database. Hence ignoring group membership updates");
			}
			return;
		}
		/* findUsersByGroupName returns all the entries from x_group_users table for a given group name and corresponding usernames from x_user table.
			Return Map has username as key and XXGroupUser object as value.
		 */

		Map<String, XXGroupUser> groupUsers = daoManager.getXXGroupUser().findUsersByGroupName(groupName);

		if (CollectionUtils.isNotEmpty(groupUserInfo.getAddUsers())) {
			Set<String> addUsers = groupUserInfo.getAddUsers();
			if (logger.isDebugEnabled()) {
				logger.debug("No. of new users in group" + groupName + " = " + addUsers.size());
			}
			for (String username : addUsers) {
				if (usersFromDB.containsKey(username)) {
					// Add or update group user mapping only if the user exists in x_user table.
					transactionSynchronizationAdapter.executeOnTransactionCommit(new GroupUserMappingUpdator(groupName, xxGroup.getId(), username, usersFromDB.get(username), groupUsers.get(username), false));
				}
			}
		}

		if (CollectionUtils.isNotEmpty(groupUserInfo.getDelUsers())) {
			Set<String> delUsers = groupUserInfo.getDelUsers();
			if (logger.isDebugEnabled()) {
				logger.debug("No. of deleted users in group" + groupName + " = " + delUsers.size());
			}

			for (String username : delUsers) {
				if (usersFromDB.containsKey(username)) {
					// delete group user mapping only if the user exists in x_user table..
					transactionSynchronizationAdapter.executeOnTransactionCommit(new GroupUserMappingUpdator(groupName, xxGroup.getId(), username, usersFromDB.get(username), groupUsers.get(username), true));
				}
			}
		}

		if (logger.isDebugEnabled()) {
			logger.debug("<<== createOrDeleteXGroupUsers for " + groupUserInfo.getGroupName());
			Long mb = 1024L * 1024L;
			logger.debug("<<== createOrDeleteXGroupUsers: Max memory = " + Runtime.getRuntime().maxMemory() / mb + " Free memory = " + Runtime.getRuntime().freeMemory() / mb
					+ " Total memory = " + Runtime.getRuntime().totalMemory() / mb);
		}
	}

	public VXGroupUser readResourceWithOutLogin(Long id) {
		XXGroupUser resource = getDao().getById(id);
		if (resource == null) {
			// Returns code 400 with DATA_NOT_FOUND as the error message
			throw restErrorUtil.createRESTException(getResourceName()
							+ " not found", MessageEnums.DATA_NOT_FOUND, id, null,
					"preRead: " + id + " not found.");
		}

		VXGroupUser view = populateViewBean(resource);
		return view;
	}

	private class GroupUserMappingUpdator implements Runnable {
		private String groupName;
		private Long groupId;
		private String userName;
		private Long userId;
		private XXGroupUser xxGroupUser;
		private boolean isDelete;

		GroupUserMappingUpdator(String groupName, Long groupId, String userName, Long userId, XXGroupUser xxGroupUser, boolean isDelete) {
			this.groupName = groupName;
			this.groupId = groupId;
			this.userName = userName;
			this.userId = userId;
			this.xxGroupUser = xxGroupUser;
			this.isDelete = isDelete;
		}

		@Override
		public void run() {
			updateGroupUserMappings();
		}

		private void updateGroupUserMappings() {
			if (logger.isDebugEnabled()) {
				logger.debug("==> GroupUserMappingUpdator.updateGroupUserMappings(" + groupName + ", " + userName + ")");
			}

			if (isDelete) {
				if (xxGroupUser != null) {
					getDao().remove(xxGroupUser.getId());
					if (logger.isDebugEnabled()) {
						logger.debug("createOrDeleteXGroupUsers(): deleted group user mapping with groupname =  " + groupName
								+ " username = " + userName);
					}
				}
			} else {
				boolean groupUserMappingExists = true;
				if (xxGroupUser == null) {
					xxGroupUser = new XXGroupUser();
					groupUserMappingExists = false;
				}
				XXPortalUser xXPortalUser = daoManager.getXXPortalUser().getById(createdByUserId);
				if (xXPortalUser != null) {
					xxGroupUser.setAddedByUserId(createdByUserId);
					xxGroupUser.setUpdatedByUserId(createdByUserId);
				}

				if (groupUserMappingExists) {
					xxGroupUser = getDao().update(xxGroupUser);
				} else {
					VXGroupUser vXGroupUser = new VXGroupUser();
					vXGroupUser.setUserId(userId);
					vXGroupUser.setName(groupName);
					vXGroupUser.setParentGroupId(groupId);
					xxGroupUser = mapViewToEntityBean(vXGroupUser, xxGroupUser, 0);
					xxGroupUser = getDao().create(xxGroupUser);
				}
				if (logger.isDebugEnabled()) {
					logger.debug(String.format("createOrDeleteXGroupUsers(): Create or update group user mapping with groupname =  " + groupName
							+ " username = %s userId = %d", userName, xxGroupUser.getUserId()));
				}
			}
			if (logger.isDebugEnabled()) {
				logger.debug("<== GroupUserMappingUpdator.updateGroupUserMappings(" + groupName + ", " + userName + ")");
			}
		}
	}
	
}
