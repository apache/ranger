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

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.ranger.common.MessageEnums;
import org.apache.ranger.common.RESTErrorUtil;
import org.apache.ranger.common.RangerCommonEnums;
import org.apache.ranger.common.db.BaseDao;
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
import org.apache.ranger.entity.XXUser;
import org.apache.ranger.plugin.model.RangerRole;
import org.apache.ranger.service.XGroupService;
import org.apache.ranger.view.VXGroup;
import org.apache.ranger.view.VXUser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.ranger.service.RangerBaseModelService.OPERATION_CREATE_CONTEXT;

@Component
public class RoleRefUpdater {
    private static final Logger LOG = LoggerFactory.getLogger(RoleRefUpdater.class);

    @Autowired
    RangerDaoManager daoMgr;

    @Autowired
    RESTErrorUtil restErrorUtil;

    @Autowired
    XUserMgr xUserMgr;

    @Autowired
    XGroupService xGroupService;

    @Autowired
    RangerTransactionSynchronizationAdapter rangerTransactionSynchronizationAdapter;

    @Autowired
    RoleDBStore roleStore;

    @Autowired
    RangerBizUtil xaBizUtil;

    public RangerDaoManager getRangerDaoManager() {
        return daoMgr;
    }

    public void createNewRoleMappingForRefTable(RangerRole rangerRole, Boolean createNonExistUserGroupRole, Boolean isRefTableCleanupRequired) {
        if (rangerRole == null) {
            return;
        }

        boolean oldBulkMode = RangerBizUtil.isBulkMode();

        final Long        roleId      = rangerRole.getId();
        final boolean     roleExists  = roleId != null;
        final Set<String> roleUsers   = new HashSet<>();
        final Set<String> roleGroups = new HashSet<>();
        final Set<String> roleRoles  = new HashSet<>();

        for (RangerRole.RoleMember user : rangerRole.getUsers()) {
            roleUsers.add(user.getName());
        }

        for (RangerRole.RoleMember group : rangerRole.getGroups()) {
            roleGroups.add(group.getName());
        }

        for (RangerRole.RoleMember role : rangerRole.getRoles()) {
            roleRoles.add(role.getName());
        }

        if (isRefTableCleanupRequired) {
            cleanupRefTablesForUpdate(rangerRole, roleUsers, roleGroups, roleRoles);
        }

        final boolean isCreateNonExistentUGRs = createNonExistUserGroupRole && xaBizUtil.checkAdminAccess();

        if (CollectionUtils.isNotEmpty(roleUsers)) {
            LOG.debug("New user entries to be inserted into x_role_ref_user for role ID {}: {}", roleId, roleUsers);

            Set<String> filteredUserNames = roleUsers.stream()
                    .filter(StringUtils::isNotBlank)
                    .collect(Collectors.toSet());

            List<XXRoleRefUser> xxRoleRefUsers = new ArrayList<>();
            Map<String, Long>   userNameIdMap  = daoMgr.getXXUser().getIdsByUserNames(filteredUserNames);

            for (String userName : filteredUserNames) {
                Long               userId      = userNameIdMap.get(userName);
                RoleUserAssociator associator = new RoleUserAssociator(userName, userId, roleId, roleExists);

                if (userId != null) {
                    XXRoleRefUser userRef = associator.getRoleRef();

                    if (userRef != null) {
                        xxRoleRefUsers.add(userRef);
                    }
                } else if (isCreateNonExistentUGRs) {
                    rangerTransactionSynchronizationAdapter.executeOnTransactionCommit(associator);
                } else {
                    throw restErrorUtil.createRESTException("user with name: " + userName + " does not exist ", MessageEnums.INVALID_INPUT_DATA);
                }
            }

            batchInsert(xxRoleRefUsers, daoMgr.getXXRoleRefUser(), oldBulkMode);
        }

        if (CollectionUtils.isNotEmpty(roleGroups)) {
            LOG.debug("New group entries to be inserted into x_role_ref_group for role ID {}: {}", roleId, roleGroups);

            Set<String> filteredGroupNames = roleGroups.stream()
                    .filter(StringUtils::isNotBlank)
                    .collect(Collectors.toSet());

            List<XXRoleRefGroup> xxRoleRefGroups = new ArrayList<>();
            Map<String, Long>    groupNameIdMap = daoMgr.getXXGroup().getIdsByGroupNames(filteredGroupNames);

            for (String groupName : filteredGroupNames) {
                Long                groupId     = groupNameIdMap.get(groupName);
                RoleGroupAssociator associator = new RoleGroupAssociator(groupName, groupId, roleId, roleExists);

                if (groupId != null) {
                    XXRoleRefGroup groupRef = associator.getRoleRef();

                    if (groupRef != null) {
                        xxRoleRefGroups.add(groupRef);
                    }
                } else if (isCreateNonExistentUGRs) {
                    rangerTransactionSynchronizationAdapter.executeOnTransactionCommit(associator);
                } else {
                    throw restErrorUtil.createRESTException("Group with name: " + groupName + " does not exist ", MessageEnums.INVALID_INPUT_DATA);
                }
            }

            batchInsert(xxRoleRefGroups, daoMgr.getXXRoleRefGroup(), oldBulkMode);
        }

        if (CollectionUtils.isNotEmpty(roleRoles)) {
            LOG.debug("New sub role entries to be inserted into x_role_ref_role for role ID {}: {}", roleId, roleRoles);

            Set<String> filteredSubRoleNames = roleRoles.stream()
                    .filter(StringUtils::isNotBlank)
                    .collect(Collectors.toSet());

            List<XXRoleRefRole> xxRoleRefRoles = new ArrayList<>();
            Map<String, Long>   subRoleNameIdMap = daoMgr.getXXRole().getIdsByRoleNames(filteredSubRoleNames);

            for (String subRoleName : filteredSubRoleNames) {
                Long               subRoleId  = subRoleNameIdMap.get(subRoleName);
                RoleRoleAssociator associator = new RoleRoleAssociator(subRoleName, subRoleId, roleId, roleExists);

                if (subRoleId != null) {
                    XXRoleRefRole roleRef = associator.getRoleRef();

                    if (roleRef != null) {
                        xxRoleRefRoles.add(roleRef);
                    }
                } else if (isCreateNonExistentUGRs) {
                    rangerTransactionSynchronizationAdapter.executeOnTransactionCommit(associator);
                } else {
                    throw restErrorUtil.createRESTException("Role with name: " + subRoleName + " does not exist ", MessageEnums.INVALID_INPUT_DATA);
                }
            }

            batchInsert(xxRoleRefRoles, daoMgr.getXXRoleRefRole(), oldBulkMode);
        }
    }

    public Boolean cleanupRefTables(RangerRole rangerRole) {
        final Long roleId = rangerRole.getId();

        if (roleId == null) {
            return false;
        }

        XXRoleRefUserDao  xRoleUserDao  = daoMgr.getXXRoleRefUser();
        XXRoleRefGroupDao xRoleGroupDao = daoMgr.getXXRoleRefGroup();
        XXRoleRefRoleDao  xRoleRoleDao  = daoMgr.getXXRoleRefRole();

        List<Long> xxRoleRefUserIds = xRoleUserDao.findIdsByRoleId(roleId);

        xRoleUserDao.deleteRoleRefUserByIds(xxRoleRefUserIds);

        List<Long> xxRoleRefGroupByIds = xRoleGroupDao.findIdsByRoleId(roleId);

        xRoleGroupDao.deleteRoleRefGroupByIds(xxRoleRefGroupByIds);

        List<Long> xxRoleRefRoleIds = xRoleRoleDao.findIdsByRoleId(roleId);

        xRoleRoleDao.deleteRoleRefRoleByIds(xxRoleRefRoleIds);

        return true;
    }

    public Boolean cleanupRefTablesForUpdate(RangerRole rangerRole, Set<String> roleUsers, Set<String> roleGroups, Set<String> roleRoles) {
        final Long roleId = rangerRole.getId();
        if (roleId == null) {
            return false;
        }

        cleanupRoleUsers(roleUsers, roleId, daoMgr.getXXRoleRefUser());
        cleanupRoleGroups(roleGroups, roleId, daoMgr.getXXRoleRefGroup());
        cleanupRoleRoles(roleRoles, roleId, daoMgr.getXXRoleRefRole());

        return true;
    }

    /**
     * Synchronizes the role-user associations for a given role by identifying and removing
     * users who are no longer associated with the role in the incoming data.
     * <p>
     * This method compares the current set of users assigned to the role (`roleUsers`)
     * with the existing role-user mappings stored in the database (`userNameIdMap`).
     * It determines which users should be deleted (present in the DB but not in `roleUsers`)
     * and removes their associations using the DAO.
     * <p>
     * After execution:
     * <ul>
     *   <li>Users present in both the database and `roleUsers` (i.e., common users) are left unchanged.</li>
     *   <li>Users present in the database but not in `roleUsers` are deleted.</li>
     *   <li>The `roleUsers` set is modified to remove users that are already associated, leaving only new users to be inserted (if needed later).</li>
     * </ul>
     *
     * @param roleUsers      Set of usernames that should be currently associated with the role.
     *                       This is the source of truth, typically coming from an external request.
     * @param roleId         ID of the role for which the cleanup is being performed.
     * @param dao            DAO for role reference users.
     */
    private void cleanupRoleUsers(Set<String> roleUsers, Long roleId, XXRoleRefUserDao dao) {
        Map<String, Long> userNameIdMap = dao.findUserNameIdMapByRoleId(roleId);
        if (userNameIdMap != null && !userNameIdMap.isEmpty()) {
            Set<String> existingUsers = userNameIdMap.keySet();
            Set<String> toDeleteUsers = new HashSet<>(existingUsers);
            Set<String> commonUsers = new HashSet<>(roleUsers);

            // Identify users present in both new and existing sets
            commonUsers.retainAll(toDeleteUsers);

            // Identify users to delete (in DB but not in new set)
            toDeleteUsers.removeAll(commonUsers);

            // Remove already existing users from the new set (they don’t need to be inserted again)
            roleUsers.removeAll(commonUsers);
            if (CollectionUtils.isNotEmpty(toDeleteUsers)) {
                List<Long> idsToDelete = toDeleteUsers.stream()
                    .map(userNameIdMap::get)
                    .filter(Objects::nonNull)
                    .collect(Collectors.toList());

                LOG.debug("Deleting user IDs from x_role_ref_user table: {} for role ID: {}", idsToDelete, roleId);
                dao.deleteRoleRefUserByIds(idsToDelete);
            }
        }
    }

    /**
     * Synchronizes role-group associations for a given role by identifying and removing
     * groups that are no longer associated with the role in the incoming data.
     * <p>
     * This method compares the current set of groups that should be associated with a role
     * (`roleGroups`) against the existing group-role mappings from the database (`groupNameIdMap`).
     * It determines which group associations should be deleted (present in DB but not in input set),
     * and deletes those associations using the DAO.
     * <p>
     * After execution:
     * <ul>
     *   <li>Groups present in both the input and DB (i.e., common groups) are preserved.</li>
     *   <li>Groups present in the DB but not in the input set are deleted.</li>
     *   <li>The `roleGroups` input set is modified to only include new groups to be inserted later (if any).</li>
     * </ul>
     *
     * @param roleGroups       Set of group names that should be associated with the role.
     *                         This is the latest source of truth (e.g., from an API or external input).
     * @param roleId           The ID of the role for which associations are being synced.
     * @param dao              DAO for role reference groups.
     */
    private void cleanupRoleGroups(Set<String> roleGroups, Long roleId, XXRoleRefGroupDao dao) {
        Map<String, Long> groupNameIdMap = dao.findGroupNameIdByRoleId(roleId);
        if (groupNameIdMap != null && !groupNameIdMap.isEmpty()) {
            // All groups currently associated in the DB
            Set<String> existingGroups = groupNameIdMap.keySet();

            // Create a mutable set to track deletions
            Set<String> toDeleteGroups = new HashSet<>(existingGroups);

            // Identify common groups between input and existing
            Set<String> commonGroups = new HashSet<>(roleGroups);
            commonGroups.retainAll(toDeleteGroups);

            // Remove common groups from both sets to isolate only new and obsolete entries
            toDeleteGroups.removeAll(commonGroups); // only those to be deleted
            roleGroups.removeAll(commonGroups);     // only those to be newly inserted

            if (CollectionUtils.isNotEmpty(toDeleteGroups)) {
                List<Long> idsToDelete = toDeleteGroups.stream()
                    .map(groupNameIdMap::get)
                    .filter(Objects::nonNull)
                    .collect(Collectors.toList());

                LOG.debug("Deleting group IDs from x_role_ref_group table: {} for role ID: {}", idsToDelete, roleId);
                dao.deleteRoleRefGroupByIds(idsToDelete);
            }
        }
    }

    /**
     * Synchronizes sub-role associations for a given parent role by identifying and removing
     * sub-roles that are no longer associated with the parent role in the incoming data.
     * <p>
     * This method compares the current set of sub-roles that should be associated with a role
     * (`roleRoles`) against the existing role-role mappings stored in the database (`subRoleNameIdMap`).
     * It determines which sub-role associations should be removed (those present in the DB but not in the new set)
     * and deletes them using the provided DAO.
     * <p>
     * After execution:
     * <ul>
     *   <li>Sub-roles that are present in both the input set and DB (common roles) are preserved.</li>
     *   <li>Sub-roles that are present in the DB but missing from the input set are deleted.</li>
     *   <li>The input set `roleRoles` is modified to retain only new sub-roles that may be inserted later.</li>
     * </ul>
     *
     * @param roleRoles         A set of sub-role names that should currently be associated with the parent role.
     *                          This represents the latest state (e.g., from an external request).
     * @param roleId            The ID of the parent role for which sub-role associations are being updated.
     * @param dao               DAO for role reference roles.
     */
    private void cleanupRoleRoles(Set<String> roleRoles, Long roleId, XXRoleRefRoleDao dao) {
        Map<String, Long> subRoleNameIdMap = dao.findSubRoleNameIdByRoleId(roleId);
        if (subRoleNameIdMap != null && !subRoleNameIdMap.isEmpty()) {
            // Get the current set of associated sub-roles from DB
            Set<String> existingRoles = subRoleNameIdMap.keySet();

            // Prepare to identify obsolete sub-roles
            Set<String> toDeleteRoles = new HashSet<>(existingRoles);

            // Identify roles that are common in both new and existing sets
            Set<String> commonRoles = new HashSet<>(roleRoles);
            commonRoles.retainAll(toDeleteRoles);

            // Remove common roles from the deletion list and the new input set
            toDeleteRoles.removeAll(commonRoles);
            roleRoles.removeAll(commonRoles);

            // Delete obsolete sub-role associations
            if (!toDeleteRoles.isEmpty()) {
                List<Long> idsToDelete = toDeleteRoles.stream()
                    .map(subRoleNameIdMap::get)
                    .filter(Objects::nonNull)
                    .collect(Collectors.toList());

                LOG.debug("Deleting role IDs from x_role_ref_role table: {} for role ID: {}", idsToDelete, roleId);
                dao.deleteRoleRefRoleByIds(idsToDelete);
            }
        }
    }

    private class RoleUserAssociator implements Runnable {
        private final String  name;
        private final Long    userId;
        private final Long    roleId;
        private final boolean roleExists;

        RoleUserAssociator(String name, Long userId, Long roleId, boolean roleExists) {
            this.name       = name;
            this.userId     = userId;
            this.roleId     = roleId;
            this.roleExists = roleExists;
        }

        public XXRoleRefUser getRoleRef() {
            Long id = resolveUserId(false);

            if (id != null && roleExists) {
                XXRoleRefUser xRoleRefUser = new XXRoleRefUser();

                xRoleRefUser.setRoleId(roleId);
                xRoleRefUser.setUserId(id);
                xRoleRefUser.setUserName(name);
                xRoleRefUser.setUserType(0);

                return xRoleRefUser;
            }

            return null;
        }

        public void createRoleRef(Long id) {
            if (roleExists) {
                XXRoleRefUser xRoleRefUser = new XXRoleRefUser();

                xRoleRefUser.setRoleId(roleId);
                xRoleRefUser.setUserId(id);
                xRoleRefUser.setUserName(name);
                xRoleRefUser.setUserType(0);

                daoMgr.getXXRoleRefUser().create(xRoleRefUser);
            } else {
                LOG.info("Role with id ={} does not exist, skipping role association!", roleId);
            }
        }

        @Override
        public void run() {
            Long id = resolveUserId(true);

            if (id != null) {
                createRoleRef(id);

                LOG.debug("Associated USER:{} with role id:[{}]", name, roleId);
            } else {
                throw new RuntimeException("Failed to associate USER:" + name + " with role id:[" + roleId + "]");
            }
        }

        private Long resolveUserId(boolean createIfAbsent) {
            Long ret = userId;

            if (ret == null) {
                XXUser xUser = daoMgr.getXXUser().findByUserName(name);

                if (xUser != null) {
                    ret = xUser.getId();
                } else if (createIfAbsent) {
                    ret = createUser();
                }
            }

            return ret;
        }

        private Long createUser() {
            LOG.warn("User specified in role does not exist in ranger admin, creating new user, name = {}", name);

            VXUser vXUser = xUserMgr.createServiceConfigUser(name);

            if (vXUser != null) {
                XXUser xUser = daoMgr.getXXUser().findByUserName(name);

                if (xUser == null) {
                    LOG.error("No User created!! Irrecoverable error! [{}]", name);
                } else {
                    return xUser.getId();
                }
            } else {
                LOG.warn("serviceConfigUser:[{}] creation failed. This may be a transient/spurious condition that may correct itself when transaction is committed", name);
            }

            return null;
        }
    }

    private class RoleGroupAssociator implements Runnable {
        private final String  name;
        private final Long    groupId;
        private final Long    roleId;
        private final boolean roleExists;

        RoleGroupAssociator(String name, Long groupId, Long roleId, boolean roleExists) {
            this.name       = name;
            this.groupId    = groupId;
            this.roleId     = roleId;
            this.roleExists = roleExists;
        }

        public XXRoleRefGroup getRoleRef() {
            Long id = resolveGroupId(false);

            if (id != null && roleExists) {
                XXRoleRefGroup xRoleRefGroup = new XXRoleRefGroup();

                xRoleRefGroup.setRoleId(roleId);
                xRoleRefGroup.setGroupId(id);
                xRoleRefGroup.setGroupName(name);
                xRoleRefGroup.setGroupType(0);

                return xRoleRefGroup;
            }

            return null;
        }

        public void createRoleRef(Long id) {
            if (roleExists) {
                XXRoleRefGroup xRoleRefGroup = new XXRoleRefGroup();

                xRoleRefGroup.setRoleId(roleId);
                xRoleRefGroup.setGroupId(id);
                xRoleRefGroup.setGroupName(name);
                xRoleRefGroup.setGroupType(0);

                daoMgr.getXXRoleRefGroup().create(xRoleRefGroup);
            } else {
                LOG.info("Role with id ={} does not exist, skipping role association!", roleId);
            }
        }

        @Override
        public void run() {
            Long id = resolveGroupId(true);

            if (id != null) {
                createRoleRef(id);

                LOG.debug("Associated GROUP:{} with role id:[{}]", name, roleId);
            } else {
                throw new RuntimeException("Failed to associate GROUP:" + name + " with role id:[" + roleId + "]");
            }
        }

        private Long resolveGroupId(boolean createIfAbsent) {
            Long ret = groupId;

            if (ret == null) {
                XXGroup xGroup = daoMgr.getXXGroup().findByGroupName(name);

                if (xGroup != null) {
                    ret = xGroup.getId();
                } else if (createIfAbsent) {
                    ret = createGroup();
                }
            }

            return ret;
        }

        private Long createGroup() {
            LOG.warn("Group specified in role does not exist in ranger admin, creating new group, name = {}", name);

            VXGroup vxGroup = new VXGroup();

            vxGroup.setName(name);
            vxGroup.setDescription(name);
            vxGroup.setGroupSource(RangerCommonEnums.GROUP_EXTERNAL);

            VXGroup vXGroup = xGroupService.createXGroupWithOutLogin(vxGroup);

            if (vXGroup != null) {
                xGroupService.createTransactionLog(vXGroup, null, OPERATION_CREATE_CONTEXT);

                return vXGroup.getId();
            }

            return null;
        }
    }

    private class RoleRoleAssociator implements Runnable {
        private final String  name;
        private final Long    subRoleId;
        private final Long    roleId;
        private final boolean roleExists;

        RoleRoleAssociator(String name, Long subRoleId, Long roleId, boolean roleExists) {
            this.name       = name;
            this.subRoleId  = subRoleId;
            this.roleId     = roleId;
            this.roleExists = roleExists;
        }

        public XXRoleRefRole getRoleRef() {
            Long id = resolveSubRoleId(false);

            if (id != null && roleExists) {
                XXRoleRefRole xRoleRefRole = new XXRoleRefRole();

                xRoleRefRole.setRoleId(roleId);
                xRoleRefRole.setSubRoleId(id);
                xRoleRefRole.setSubRoleName(name);
                xRoleRefRole.setSubRoleType(0);

                return xRoleRefRole;
            }

            return null;
        }

        public void createRoleRef(Long id) {
            if (roleExists) {
                XXRoleRefRole xRoleRefRole = new XXRoleRefRole();

                xRoleRefRole.setRoleId(roleId);
                xRoleRefRole.setSubRoleId(id);
                xRoleRefRole.setSubRoleName(name);
                xRoleRefRole.setSubRoleType(0);

                daoMgr.getXXRoleRefRole().create(xRoleRefRole);
            } else {
                LOG.info("Role with id ={} does not exist, skipping role association!", roleId);
            }
        }

        @Override
        public void run() {
            Long id = resolveSubRoleId(true);

            if (id != null) {
                createRoleRef(id);

                LOG.debug("Associated ROLE:{} with role id:[{}]", name, roleId);
            } else {
                throw new RuntimeException("Failed to associate ROLE:" + name + " with role id:[" + roleId + "]");
            }
        }

        private Long resolveSubRoleId(boolean createIfAbsent) {
            Long ret = subRoleId;

            if (ret == null) {
                XXRole xRole = daoMgr.getXXRole().findByRoleName(name);

                if (xRole != null) {
                    ret = xRole.getId();
                } else if (createIfAbsent) {
                    RangerBizUtil.setBulkMode(false);

                    ret = createSubRole();
                }
            }

            return ret;
        }

        private Long createSubRole() {
            LOG.warn("Sub-role specified in role does not exist in ranger admin, creating new role, name = {}", name);

            try {
                RangerRole rRole       = new RangerRole(name, null, null, null, null);
                RangerRole createdRole = roleStore.createRole(rRole, false, false);

                return createdRole.getId();
            } catch (Exception e) {
                LOG.error("Failed to create sub-role {}", name, e);

                return null;
            }
        }
    }

    private <T> void batchInsert(List<T> entities, BaseDao<T> dao, boolean oldBulkMode) {
        if (CollectionUtils.isNotEmpty(entities)) {
            long startTimeMs = System.currentTimeMillis();

            LOG.debug("Batch insert started for create/update {} with {} records.", dao.getClass().getSimpleName(), entities.size());

            RangerBizUtil.setBulkMode(false);
            dao.batchCreate(entities);
            RangerBizUtil.setBulkMode(oldBulkMode);

            LOG.debug("Batch insert completed for create/update {} with {} records in {} ms.", dao.getClass().getSimpleName(), entities.size(), (System.currentTimeMillis() - startTimeMs));
        }
    }
}
