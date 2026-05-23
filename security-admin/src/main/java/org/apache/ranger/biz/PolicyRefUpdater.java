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
import org.apache.ranger.common.RESTErrorUtil;
import org.apache.ranger.common.RangerCommonEnums;
import org.apache.ranger.common.db.BaseDao;
import org.apache.ranger.common.db.RangerTransactionSynchronizationAdapter;
import org.apache.ranger.db.RangerDaoManager;
import org.apache.ranger.db.XXPolicyRefGroupDao;
import org.apache.ranger.db.XXPolicyRefRoleDao;
import org.apache.ranger.db.XXPolicyRefUserDao;
import org.apache.ranger.entity.XXGroup;
import org.apache.ranger.entity.XXPolicy;
import org.apache.ranger.entity.XXPolicyRefAccessType;
import org.apache.ranger.entity.XXPolicyRefCondition;
import org.apache.ranger.entity.XXPolicyRefDataMaskType;
import org.apache.ranger.entity.XXPolicyRefGroup;
import org.apache.ranger.entity.XXPolicyRefResource;
import org.apache.ranger.entity.XXPolicyRefRole;
import org.apache.ranger.entity.XXPolicyRefUser;
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
import org.apache.ranger.plugin.util.ServiceDefUtil;
import org.apache.ranger.service.XGroupService;
import org.apache.ranger.view.VXGroup;
import org.apache.ranger.view.VXResponse;
import org.apache.ranger.view.VXUser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.servlet.http.HttpServletResponse;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.ranger.service.RangerBaseModelService.OPERATION_CREATE_CONTEXT;

@Component
public class PolicyRefUpdater {
    private static final Logger LOG = LoggerFactory.getLogger(PolicyRefUpdater.class);

    @Autowired
    RangerDaoManager daoMgr;

    @Autowired
    XUserMgr xUserMgr;

    @Autowired
    RoleDBStore roleStore;

    @Autowired
    RangerBizUtil rangerBizUtil;

    @Autowired
    XGroupService xGroupService;

    @Autowired
    RangerTransactionSynchronizationAdapter rangerTransactionSynchronizationAdapter;

    @Autowired
    RESTErrorUtil restErrorUtil;

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

    public void createNewPolMappingForRefTable(RangerPolicy policy, XXPolicy xPolicy, XXServiceDef xServiceDef, boolean createPrincipalsIfAbsent, boolean isCleanupRefTablesNeeded) throws Exception {
        if (policy == null) {
            return;
        }

        final Set<String> resourceNames  = policy.getResources().keySet();
        final Set<String> roleNames      = new HashSet<>();
        final Set<String> groupNames     = new HashSet<>();
        final Set<String> userNames      = new HashSet<>();
        final Set<String> accessTypes    = new HashSet<>();
        final Set<String> conditionTypes = new HashSet<>();
        final Set<String> dataMaskTypes  = new HashSet<>();
        boolean           oldBulkMode    = RangerBizUtil.isBulkMode();
        final Long policyId = policy.getId();

        List<RangerPolicy.RangerPolicyItemCondition> rangerPolicyConditions = policy.getConditions();

        if (CollectionUtils.isNotEmpty(rangerPolicyConditions)) {
            for (RangerPolicy.RangerPolicyItemCondition condition : rangerPolicyConditions) {
                conditionTypes.add(condition.getType());
            }
        }

        for (List<? extends RangerPolicyItem> policyItems : getAllPolicyItems(policy)) {
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

        if (isCleanupRefTablesNeeded) {
            cleanupRefTablesForUpdate(policyId, userNames, roleNames, groupNames);
        }

        if (CollectionUtils.isNotEmpty(resourceNames)) {
            List<XXPolicyRefResource> xPolResources = new ArrayList<>();
            Map<String, Long>         nameToId      = daoMgr.getXXResourceDef().findResourceDefIdsByNameAndPolicyId(resourceNames, policy.getId());

            for (String resource : resourceNames) {
                Long resourceDefId = nameToId.get(resource);

                if (resourceDefId == null) {
                    throw new Exception(resource + ": is not a valid resource-type. policy='" + policy.getName() + "' service='" + policy.getService() + "'");
                }

                XXPolicyRefResource xPolRes = new XXPolicyRefResource();

                xPolRes.setPolicyId(policy.getId());
                xPolRes.setResourceDefId(resourceDefId);
                xPolRes.setResourceName(resource);

                xPolResources.add(xPolRes);
            }

            batchInsert(xPolResources, daoMgr.getXXPolicyRefResource(), oldBulkMode);
        }

        if (createPrincipalsIfAbsent && !rangerBizUtil.checkAdminAccess()) {
            LOG.warn("policy={}: createPrincipalIfAbsent=true, but current user does not have admin privileges!", policy.getName());

            createPrincipalsIfAbsent = false;
        }

        if (CollectionUtils.isNotEmpty(roleNames)) {
            LOG.debug("x_policy_ref_role - New role entries to insert for policy ID {}: {}", policyId, roleNames);

            Set<String> filteredRoleNames = roleNames.stream()
                    .filter(StringUtils::isNotBlank)
                    .collect(Collectors.toSet());

            List<XXPolicyRefRole> xPolRoles = new ArrayList<>();
            Map<String, Long>     nameToId  = daoMgr.getXXRole().getIdsByRoleNames(filteredRoleNames);

            for (String roleName : filteredRoleNames) {
                Long                 roleId     = nameToId.get(roleName);
                PolicyRoleAssociator associator = new PolicyRoleAssociator(roleName, roleId, xPolicy);

                if (roleId != null) {
                    XXPolicyRefRole roleRef = associator.getPolicyRef();

                    if (roleRef != null) {
                        xPolRoles.add(roleRef);
                    }
                } else if (createPrincipalsIfAbsent) {
                    rangerTransactionSynchronizationAdapter.executeOnTransactionCommit(associator);
                } else {
                    VXResponse gjResponse = new VXResponse();

                    gjResponse.setStatusCode(HttpServletResponse.SC_BAD_REQUEST);
                    gjResponse.setMsgDesc("Operation denied. Role name: " + roleName + " specified in policy does not exist in ranger admin.");

                    throw restErrorUtil.generateRESTException(gjResponse);
                }
            }

            batchInsert(xPolRoles, daoMgr.getXXPolicyRefRole(), oldBulkMode);
        }

        if (CollectionUtils.isNotEmpty(groupNames)) {
            LOG.debug("x_policy_ref_group - New group entries to insert for policy ID {}: {}", policyId, groupNames);

            Set<String> filteredGroupNames = groupNames.stream()
                    .filter(StringUtils::isNotBlank)
                    .collect(Collectors.toSet());

            List<XXPolicyRefGroup> xPolGroups = new ArrayList<>();
            Map<String, Long>      nameToId   = daoMgr.getXXGroup().getIdsByGroupNames(filteredGroupNames);

            for (String groupName : filteredGroupNames) {
                Long                  groupId    = nameToId.get(groupName);
                PolicyGroupAssociator associator = new PolicyGroupAssociator(groupName, groupId, xPolicy);

                if (groupId != null) {
                    XXPolicyRefGroup groupRef = associator.getPolicyRef();

                    if (groupRef != null) {
                        xPolGroups.add(groupRef);
                    }
                } else if (createPrincipalsIfAbsent) {
                    rangerTransactionSynchronizationAdapter.executeOnTransactionCommit(associator);
                } else {
                    VXResponse gjResponse = new VXResponse();

                    gjResponse.setStatusCode(HttpServletResponse.SC_BAD_REQUEST);
                    gjResponse.setMsgDesc("Operation denied. Group name: " + groupName + " specified in policy does not exist in ranger admin.");

                    throw restErrorUtil.generateRESTException(gjResponse);
                }
            }

            batchInsert(xPolGroups, daoMgr.getXXPolicyRefGroup(), oldBulkMode);
        }

        if (CollectionUtils.isNotEmpty(userNames)) {
            LOG.debug("x_policy_ref_user - New user entries to insert for policy ID {}: {}", policyId, userNames);

            Set<String> filteredUserNames = userNames.stream()
                    .filter(StringUtils::isNotBlank)
                    .collect(Collectors.toSet());

            List<XXPolicyRefUser> xPolUsers = new ArrayList<>();
            Map<String, Long>     nameToId  = daoMgr.getXXUser().getIdsByUserNames(filteredUserNames);

            for (String userName : filteredUserNames) {
                Long                 userId     = nameToId.get(userName);
                PolicyUserAssociator associator = new PolicyUserAssociator(userName, userId, xPolicy);

                if (userId != null) {
                    XXPolicyRefUser userRef = associator.getPolicyRef();

                    if (userRef != null) {
                        xPolUsers.add(userRef);
                    }
                } else if (createPrincipalsIfAbsent) {
                    rangerTransactionSynchronizationAdapter.executeOnTransactionCommit(associator);
                } else {
                    VXResponse gjResponse = new VXResponse();

                    gjResponse.setStatusCode(HttpServletResponse.SC_BAD_REQUEST);
                    gjResponse.setMsgDesc("Operation denied. User name: " + userName + " specified in policy does not exist in ranger admin.");

                    throw restErrorUtil.generateRESTException(gjResponse);
                }
            }

            batchInsert(xPolUsers, daoMgr.getXXPolicyRefUser(), oldBulkMode);
        }

        // ignore built-in access-types while creating ref-table entries
        accessTypes.removeAll(ServiceDefUtil.ACCESS_TYPE_MARKERS);

        if (CollectionUtils.isNotEmpty(accessTypes)) {
            List<XXPolicyRefAccessType> xPolAccesses = new ArrayList<>();
            Map<String, Long>           nameToId     = daoMgr.getXXAccessTypeDef().findAccessTypeDefIdsByNamesAndServiceId(accessTypes, xPolicy.getService());

            for (String accessType : accessTypes) {
                Long accessDefId = nameToId.get(accessType);

                if (accessDefId == null) {
                    throw new Exception(accessType + ": is not a valid access-type. policy='" + policy.getName() + "' service='" + policy.getService() + "'");
                }

                XXPolicyRefAccessType xPolAccess = new XXPolicyRefAccessType();

                xPolAccess.setPolicyId(policy.getId());
                xPolAccess.setAccessDefId(accessDefId);
                xPolAccess.setAccessTypeName(accessType);

                xPolAccesses.add(xPolAccess);
            }

            batchInsert(xPolAccesses, daoMgr.getXXPolicyRefAccessType(), oldBulkMode);
        }

        if (CollectionUtils.isNotEmpty(conditionTypes)) {
            List<XXPolicyRefCondition> xPolConds = new ArrayList<>();
            Map<String, Long>          nameToId  = daoMgr.getXXPolicyConditionDef().findConditionDefIdsByServiceDefIdAndNames(xServiceDef.getId(), conditionTypes);

            for (String condition : conditionTypes) {
                Long conditionDefId = nameToId.get(condition);

                if (conditionDefId == null) {
                    if (StringUtils.equalsIgnoreCase(condition, ServiceDefUtil.IMPLICIT_CONDITION_EXPRESSION_NAME)) {
                        continue;
                    }

                    throw new Exception(condition + ": is not a valid condition-type. policy='" + xPolicy.getName() + "' service='" + xPolicy.getService() + "'");
                }

                XXPolicyRefCondition xPolCond = new XXPolicyRefCondition();

                xPolCond.setPolicyId(policy.getId());
                xPolCond.setConditionDefId(conditionDefId);
                xPolCond.setConditionName(condition);

                xPolConds.add(xPolCond);
            }

            batchInsert(xPolConds, daoMgr.getXXPolicyRefCondition(), oldBulkMode);
        }

        if (CollectionUtils.isNotEmpty(dataMaskTypes)) {
            List<XXPolicyRefDataMaskType> xxDataMaskInfos = new ArrayList<>();
            Map<String, Long>             nameToId        = daoMgr.getXXDataMaskTypeDef().findDataMaskTypeDefIdsByNamesAndServiceId(dataMaskTypes, xPolicy.getService());

            for (String dataMaskType : dataMaskTypes) {
                Long dataMaskDefId = nameToId.get(dataMaskType);

                if (dataMaskDefId == null) {
                    throw new Exception(dataMaskType + ": is not a valid datamask-type. policy='" + policy.getName() + "' service='" + policy.getService() + "'");
                }

                XXPolicyRefDataMaskType xxDataMaskInfo = new XXPolicyRefDataMaskType();

                xxDataMaskInfo.setPolicyId(policy.getId());
                xxDataMaskInfo.setDataMaskDefId(dataMaskDefId);
                xxDataMaskInfo.setDataMaskTypeName(dataMaskType);

                xxDataMaskInfos.add(xxDataMaskInfo);
            }

            batchInsert(xxDataMaskInfos, daoMgr.getXXPolicyRefDataMaskType(), oldBulkMode);
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

    /**
     * Cleans up reference tables for a given policy before updating it.
     * This includes handling policy reference users, roles, and other
     * associated reference entities like resources, groups, access types,
     * conditions, and data mask types.
     *
     * @param policyId      ID of the policy being updated.
     * @param policyUsers   Set of usernames associated with the updated policy.
     * @param policyRoles   Set of role names associated with the updated policy.
     * @param policyGroups   Set of group names associated with the updated policy.
     * @return              true if cleanup was successful.
     */
    public Boolean cleanupRefTablesForUpdate(Long policyId, Set<String> policyUsers, Set<String> policyRoles, Set<String> policyGroups) {
        cleanupPolicyRefUsers(policyUsers, policyId, daoMgr.getXXPolicyRefUser());
        cleanupPolicyRefRoles(policyRoles, policyId, daoMgr.getXXPolicyRefRole());
        cleanupPolicyRefGroups(policyGroups, policyId, daoMgr.getXXPolicyRefGroup());

        daoMgr.getXXPolicyRefResource().deleteByPolicyId(policyId);
        daoMgr.getXXPolicyRefAccessType().deleteByPolicyId(policyId);
        daoMgr.getXXPolicyRefCondition().deleteByPolicyId(policyId);
        daoMgr.getXXPolicyRefDataMaskType().deleteByPolicyId(policyId);

        return true;
    }

    /**
     * Identifies and deletes outdated user references for a given policy,
     * and prepares a list of new users that need to be inserted.
     *
     * @param policyUsers   Set of usernames from the new policy.
     * @param policyId      The ID of the policy.
     * @param dao           DAO for policy reference users.
     */
    public void cleanupPolicyRefUsers(Set<String> policyUsers, Long policyId, XXPolicyRefUserDao dao) {
        Map<String, Long> policyUserNameIdMap = dao.findUserNameIdByPolicyId(policyId);

        if (policyUserNameIdMap != null && !policyUserNameIdMap.isEmpty()) {
            Set<String> existingPolicyRefUsers = policyUserNameIdMap.keySet();
            Set<String> toDeletePolicyRefUsers = new HashSet<>(existingPolicyRefUsers);
            Set<String> commonUsers            = new HashSet<>(policyUsers);

            // Identify users present in both new and existing sets
            commonUsers.retainAll(toDeletePolicyRefUsers);

            // Identify users to delete (in DB but not in new set)
            toDeletePolicyRefUsers.removeAll(commonUsers);

            // Remove already existing users from the new set (they don’t need to be inserted again)
            policyUsers.removeAll(commonUsers);

            if (CollectionUtils.isNotEmpty(toDeletePolicyRefUsers)) {
                List<Long> idsToDelete = toDeletePolicyRefUsers.stream()
                        .map(policyUserNameIdMap::get)
                        .filter(Objects::nonNull)
                        .collect(Collectors.toList());

                LOG.debug("Deleting user IDs from x_policy_ref_user table: {} for policy ID: {}", idsToDelete, policyId);

                dao.deletePolicyRefUserByIds(idsToDelete);
            }
        }
    }

    /**
     * Identifies and deletes outdated role references for a given policy,
     * and prepares a list of new roles that need to be inserted.
     *
     * @param policyRoles   Set of role names from the new policy.
     * @param policyId      The ID of the policy.
     * @param dao           DAO for policy reference roles.
     */
    public void cleanupPolicyRefRoles(Set<String> policyRoles, Long policyId, XXPolicyRefRoleDao dao) {
        Map<String, Long> policyRoleNameIdMap = dao.findRoleNameIdByPolicyId(policyId);

        if (policyRoleNameIdMap != null && !policyRoleNameIdMap.isEmpty()) {
            Set<String> existingPolicyRefRoles = policyRoleNameIdMap.keySet();
            Set<String> toDeletePolicyRefRoles = new HashSet<>(existingPolicyRefRoles);
            Set<String> commonRoles            = new HashSet<>(policyRoles);

            // Identify roles present in both new and existing sets
            commonRoles.retainAll(toDeletePolicyRefRoles);

            // Identify roles to delete (in DB but not in new set)
            toDeletePolicyRefRoles.removeAll(commonRoles);

            // Remove already existing roles from the new set (they don’t need to be inserted again)
            policyRoles.removeAll(commonRoles);

            if (CollectionUtils.isNotEmpty(toDeletePolicyRefRoles)) {
                List<Long> idsToDelete = toDeletePolicyRefRoles.stream()
                        .map(policyRoleNameIdMap::get)
                        .filter(Objects::nonNull)
                        .collect(Collectors.toList());

                LOG.debug("Deleting Role IDs from x_policy_ref_role: {} for policy ID: {}", idsToDelete, policyId);

                dao.deletePolicyRefRoleByIds(idsToDelete);
            }
        }
    }

    /**
     * Identifies and deletes outdated group references for a given policy,
     * and prepares a list of new groups that need to be inserted.
     *
     * @param policyGroups   Set of group names from the new policy.
     * @param policyId       The ID of the policy.
     * @param dao            DAO for policy reference groups.
     */
    public void cleanupPolicyRefGroups(Set<String> policyGroups, Long policyId, XXPolicyRefGroupDao dao) {
        Map<String, Long> policyGroupNameIdMap = dao.findGroupNameByPolicyId(policyId);

        if (policyGroupNameIdMap != null && !policyGroupNameIdMap.isEmpty()) {
            Set<String> existingPolicyRefGroups = policyGroupNameIdMap.keySet();
            Set<String> toDeletePolicyRefGroups = new HashSet<>(existingPolicyRefGroups);
            Set<String> commonGroups            = new HashSet<>(policyGroups);

            // Identify groups present in both new and existing sets
            commonGroups.retainAll(toDeletePolicyRefGroups);

            // Identify groups to delete (in DB but not in new set)
            toDeletePolicyRefGroups.removeAll(commonGroups);

            // Remove already existing groups from the new set (they don’t need to be inserted again)
            policyGroups.removeAll(commonGroups);

            if (CollectionUtils.isNotEmpty(toDeletePolicyRefGroups)) {
                List<Long> idsToDelete = toDeletePolicyRefGroups.stream()
                        .map(policyGroupNameIdMap::get)
                        .filter(Objects::nonNull)
                        .collect(Collectors.toList());

                LOG.debug("Deleting Group IDs from x_policy_ref_group: {} for policy ID: {}", idsToDelete, policyId);

                dao.deletePolicyRefGroupByIds(idsToDelete);
            }
        }
    }

    public enum PRINCIPAL_TYPE { USER, GROUP, ROLE }

    private boolean doesPolicyExist(XXPolicy policy) {
        return daoMgr.getXXPolicy().getById(policy.getId()) != null;
    }

    private class PolicyRoleAssociator implements Runnable {
        private final String   name;
        private final Long     roleId;
        private final XXPolicy xPolicy;

        PolicyRoleAssociator(String name, Long roleId, XXPolicy xPolicy) {
            this.name    = name;
            this.roleId  = roleId;
            this.xPolicy = xPolicy;
        }

        public XXPolicyRefRole getPolicyRef() {
            Long id = resolveRoleId(false);

            if (id != null && doesPolicyExist(xPolicy)) {
                XXPolicyRefRole xPolRole = new XXPolicyRefRole();

                xPolRole.setPolicyId(xPolicy.getId());
                xPolRole.setRoleId(id);
                xPolRole.setRoleName(name);

                return xPolRole;
            }

            return null;
        }

        public void createPolicyRef(Long id) {
            if (doesPolicyExist(xPolicy)) {
                XXPolicyRefRole xPolRole = new XXPolicyRefRole();

                xPolRole.setPolicyId(xPolicy.getId());
                xPolRole.setRoleId(id);
                xPolRole.setRoleName(name);

                daoMgr.getXXPolicyRefRole().create(xPolRole);
            } else {
                LOG.info("Policy with id ={} does not exist, skipping policy association!", xPolicy.getId());
            }
        }

        @Override
        public void run() {
            Long id = resolveRoleId(true);

            if (id != null) {
                createPolicyRef(id);

                LOG.debug("Associated ROLE:{} with policy id:[{}]", name, xPolicy.getId());
            } else {
                throw new RuntimeException("Failed to associate ROLE:" + name + " with policy id:[" + xPolicy.getId() + "]");
            }
        }

        private Long resolveRoleId(boolean createIfAbsent) {
            Long ret = roleId;

            if (ret == null) {
                XXRole xRole = daoMgr.getXXRole().findByRoleName(name);

                if (xRole != null) {
                    ret = xRole.getId();
                } else if (createIfAbsent) {
                    RangerBizUtil.setBulkMode(false);

                    ret = createRole();
                }
            }

            return ret;
        }

        private Long createRole() {
            LOG.warn("Role specified in policy does not exist in ranger admin, creating new role, name = {}", name);

            try {
                RangerRole rRole       = new RangerRole(name, null, null, null, null);
                RangerRole createdRole = roleStore.createRole(rRole, false);

                return createdRole.getId();
            } catch (Exception e) {
                return null;
            }
        }
    }

    private class PolicyGroupAssociator implements Runnable {
        private final String   name;
        private final Long     groupId;
        private final XXPolicy xPolicy;

        PolicyGroupAssociator(String name, Long groupId, XXPolicy xPolicy) {
            this.name     = name;
            this.groupId  = groupId;
            this.xPolicy  = xPolicy;
        }

        public XXPolicyRefGroup getPolicyRef() {
            Long id = resolveGroupId(false);

            if (id != null && doesPolicyExist(xPolicy)) {
                XXPolicyRefGroup xPolGroup = new XXPolicyRefGroup();

                xPolGroup.setPolicyId(xPolicy.getId());
                xPolGroup.setGroupId(id);
                xPolGroup.setGroupName(name);

                return xPolGroup;
            }

            return null;
        }

        public void createPolicyRef(Long id) {
            if (doesPolicyExist(xPolicy)) {
                XXPolicyRefGroup xPolGroup = new XXPolicyRefGroup();

                xPolGroup.setPolicyId(xPolicy.getId());
                xPolGroup.setGroupId(id);
                xPolGroup.setGroupName(name);

                daoMgr.getXXPolicyRefGroup().create(xPolGroup);
            } else {
                LOG.info("Policy with id ={} does not exist, skipping policy association!", xPolicy.getId());
            }
        }

        @Override
        public void run() {
            Long id = resolveGroupId(true);

            if (id != null) {
                createPolicyRef(id);

                LOG.debug("Associated GROUP:{} with policy id:[{}]", name, xPolicy.getId());
            } else {
                throw new RuntimeException("Failed to associate GROUP:" + name + " with policy id:[" + xPolicy.getId() + "]");
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
            LOG.warn("Group specified in policy does not exist in ranger admin, creating new group, name = {}", name);

            VXGroup vxGroup = new VXGroup();

            vxGroup.setName(name);
            vxGroup.setDescription(name);
            vxGroup.setGroupSource(RangerCommonEnums.GROUP_EXTERNAL);

            VXGroup vXGroup = xGroupService.createXGroupWithOutLogin(vxGroup);

            if (vXGroup != null) {
                xGroupService.createTransactionLog(vXGroup, null, OPERATION_CREATE_CONTEXT, xPolicy.getAddedByUserId());

                return vXGroup.getId();
            }

            return null;
        }
    }

    private class PolicyUserAssociator implements Runnable {
        private final String   name;
        private final Long     userId;
        private final XXPolicy xPolicy;

        PolicyUserAssociator(String name, Long userId, XXPolicy xPolicy) {
            this.name    = name;
            this.userId  = userId;
            this.xPolicy = xPolicy;
        }

        public XXPolicyRefUser getPolicyRef() {
            Long id = resolveUserId(false);

            if (id != null && doesPolicyExist(xPolicy)) {
                XXPolicyRefUser xPolUser = new XXPolicyRefUser();

                xPolUser.setPolicyId(xPolicy.getId());
                xPolUser.setUserId(id);
                xPolUser.setUserName(name);

                return xPolUser;
            }

            return null;
        }

        public void createPolicyRef(Long id) {
            if (doesPolicyExist(xPolicy)) {
                XXPolicyRefUser xPolUser = new XXPolicyRefUser();

                xPolUser.setPolicyId(xPolicy.getId());
                xPolUser.setUserId(id);
                xPolUser.setUserName(name);

                daoMgr.getXXPolicyRefUser().create(xPolUser);
            } else {
                LOG.info("Policy with id ={} does not exist, skipping policy association!", xPolicy.getId());
            }
        }

        @Override
        public void run() {
            Long id = resolveUserId(true);

            if (id != null) {
                createPolicyRef(id);

                LOG.debug("Associated USER:{} with policy id:[{}]", name, xPolicy.getId());
            } else {
                throw new RuntimeException("Failed to associate USER:" + name + " with policy id:[" + xPolicy.getId() + "]");
            }
        }

        private Long resolveUserId(boolean createIfAbsent) {
            Long ret = userId;

            if (ret == null) {
                XXUser xUser = daoMgr.getXXUser().findByUserName(name);

                if (xUser != null) {
                    ret = xUser.getId();
                } else if (createIfAbsent) {
                    return createUser();
                }
            }

            return ret;
        }

        private Long createUser() {
            LOG.warn("User specified in policy does not exist in ranger admin, creating new user, name = {}", name);

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
