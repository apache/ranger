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
import java.util.Set;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.ranger.common.MessageEnums;
import org.apache.ranger.common.RESTErrorUtil;
import org.apache.ranger.db.RangerDaoManager;
import org.apache.ranger.db.XXRoleRefGroupDao;
import org.apache.ranger.db.XXRoleRefRoleDao;
import org.apache.ranger.db.XXRoleRefUserDao;
import org.apache.ranger.entity.XXGroup;
import org.apache.ranger.entity.XXRole;
import org.apache.ranger.entity.XXRoleRefGroup;
import org.apache.ranger.entity.XXRoleRefRole;
import org.apache.ranger.entity.XXRoleRefUser;
import org.apache.ranger.entity.XXUser;
import org.apache.ranger.plugin.model.RangerRole;
import org.apache.ranger.service.RangerAuditFields;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class RoleRefUpdater {
	@Autowired
	RangerDaoManager daoMgr;

	@Autowired
	RangerAuditFields<?> rangerAuditFields;

	@Autowired
	RESTErrorUtil restErrorUtil;

	public void createNewRoleMappingForRefTable(RangerRole rangerRole) throws Exception {
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

				XXUser xUser = daoMgr.getXXUser().findByUserName(roleUser);

				if (xUser == null) {
					throw restErrorUtil.createRESTException("user with name: " + roleUser + " does not exist ",
							MessageEnums.INVALID_INPUT_DATA);
				}

				XXRoleRefUser xRoleRefUser = rangerAuditFields.populateAuditFieldsForCreate(new XXRoleRefUser());

				xRoleRefUser.setRoleId(roleId);
				xRoleRefUser.setUserId(xUser.getId());
				xRoleRefUser.setUserName(roleUser);
				xRoleRefUser.setUserType(0);
				daoMgr.getXXRoleRefUser().create(xRoleRefUser);
			}
		}

		if (CollectionUtils.isNotEmpty(roleGroups)) {
			for (String roleGroup : roleGroups) {

				if (StringUtils.isBlank(roleGroup)) {
					continue;
				}

				XXGroup xGroup = daoMgr.getXXGroup().findByGroupName(roleGroup);

				if (xGroup == null) {
					throw restErrorUtil.createRESTException("group with name: " + roleGroup + " does not exist ",
							MessageEnums.INVALID_INPUT_DATA);
				}

				XXRoleRefGroup xRoleRefGroup = rangerAuditFields.populateAuditFieldsForCreate(new XXRoleRefGroup());

				xRoleRefGroup.setRoleId(roleId);
				xRoleRefGroup.setGroupId(xGroup.getId());
				xRoleRefGroup.setGroupName(roleGroup);
				xRoleRefGroup.setGroupType(0);
				daoMgr.getXXRoleRefGroup().create(xRoleRefGroup);
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
}
