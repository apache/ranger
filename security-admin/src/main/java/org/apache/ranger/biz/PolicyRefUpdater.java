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
            Map<String, XXResourceDef> nameToResDefMap = daoMgr.getXXResourceDef().findXXResourceDefsByNameAndPolicyId(resourceNames, policy.getId());
            for (String resource : resourceNames) {
                XXResourceDef xResDef = nameToResDefMap.get(resource);
                if (xResDef == null) {
                    throw new Exception(resource + ": is not a valid resource-type. policy='" + policy.getName() + "' service='" + policy.getService() + "'");
                }
                XXPolicyRefResource xPolRes = new XXPolicyRefResource();
                xPolRes.setPolicyId(policy.getId());
                xPolRes.setResourceDefId(xResDef.getId());
                xPolRes.setResourceName(resource);
                xPolResources.add(xPolRes);
            }
            batchInsert(xPolResources, daoMgr.getXXPolicyRefResource(), oldBulkMode);
        }

        if (createPrincipalsIfAbsent && !rangerBizUtil.checkAdminAccess()) {
            LOG.warn("policy={}: createPrincipalIfAbsent=true, but current user does not have admin privileges!", policy.getName());

            createPrincipalsIfAbsent = false;
        }

        List<XXPolicyRefRole> xPolRoles = new ArrayList<>();
        if (CollectionUtils.isNotEmpty(roleNames)) {
            LOG.debug("x_policy_ref_role - New role entries to insert for policy ID {}: {}", policyId, roleNames);
            Set<String> filteredRoleNames = roleNames.stream()
                    .filter(str -> !StringUtils.isBlank(str))
                    .collect(Collectors.toSet());
            Map<String, Long> roleNameIdMap = daoMgr.getXXRole().getIdsByRoleNames(filteredRoleNames);
            for (String roleName : filteredRoleNames) {
                PolicyPrincipalAssociator associator = new PolicyPrincipalAssociator(PRINCIPAL_TYPE.ROLE, roleName, xPolicy, roleNameIdMap.get(roleName), null, null, xPolRoles);
                if (!associator.doAssociate(false)) {
                    if (createPrincipalsIfAbsent) {
                        rangerTransactionSynchronizationAdapter.executeOnTransactionCommit(associator);
                    } else {
                        VXResponse gjResponse = new VXResponse();

                        gjResponse.setStatusCode(HttpServletResponse.SC_BAD_REQUEST);
                        gjResponse.setMsgDesc("Operation denied. Role name: " + roleName + " specified in policy does not exist in ranger admin.");
                        throw restErrorUtil.generateRESTException(gjResponse);
                    }
                }
            }
            batchInsert(xPolRoles, daoMgr.getXXPolicyRefRole(), oldBulkMode);
        }

        if (CollectionUtils.isNotEmpty(groupNames)) {
            LOG.debug("x_policy_ref_group - New group entries to insert for policy ID {}: {}", policyId, groupNames);
            Set<String> filteredGroupNames = groupNames.stream()
                    .filter(str -> !StringUtils.isBlank(str))
                    .collect(Collectors.toSet());
            Map<String, Long> groupNameIdMap = daoMgr.getXXGroup().getIdsByGroupNames(filteredGroupNames);
            List<XXPolicyRefGroup> xPolGroups = new ArrayList<>();
            for (String groupName : filteredGroupNames) {
                PolicyPrincipalAssociator associator = new PolicyPrincipalAssociator(PRINCIPAL_TYPE.GROUP, groupName, xPolicy, groupNameIdMap.get(groupName), null, xPolGroups, null);
                if (!associator.doAssociate(false)) {
                    if (createPrincipalsIfAbsent) {
                        rangerTransactionSynchronizationAdapter.executeOnTransactionCommit(associator);
                    } else {
                        VXResponse gjResponse = new VXResponse();

                        gjResponse.setStatusCode(HttpServletResponse.SC_BAD_REQUEST);
                        gjResponse.setMsgDesc("Operation denied. Group name: " + groupName + " specified in policy does not exist in ranger admin.");

                        throw restErrorUtil.generateRESTException(gjResponse);
                    }
                }
            }
            batchInsert(xPolGroups, daoMgr.getXXPolicyRefGroup(), oldBulkMode);
        }

        if (CollectionUtils.isNotEmpty(userNames)) {
            LOG.debug("x_policy_ref_user - New user entries to insert for policy ID {}: {}", policyId, userNames);
            List<XXPolicyRefUser> xPolUsers = new ArrayList<>();
            Set<String> filteredUserNames = userNames.stream()
                    .filter(str -> !StringUtils.isBlank(str))
                    .collect(Collectors.toSet());
            Map<String, Long> userNameIdMap = daoMgr.getXXUser().getIdsByUserNames(filteredUserNames);
            for (String userName : filteredUserNames) {
                PolicyPrincipalAssociator associator = new PolicyPrincipalAssociator(PRINCIPAL_TYPE.USER, userName, xPolicy, userNameIdMap.get(userName), xPolUsers, null, null);
                if (!associator.doAssociate(false)) {
                    if (createPrincipalsIfAbsent) {
                        rangerTransactionSynchronizationAdapter.executeOnTransactionCommit(associator);
                    } else {
                        VXResponse gjResponse = new VXResponse();

                        gjResponse.setStatusCode(HttpServletResponse.SC_BAD_REQUEST);
                        gjResponse.setMsgDesc("Operation denied. User name: " + userName + " specified in policy does not exist in ranger admin.");

                        throw restErrorUtil.generateRESTException(gjResponse);
                    }
                }
            }
            batchInsert(xPolUsers, daoMgr.getXXPolicyRefUser(), oldBulkMode);
        }

        // ignore built-in access-types while creating ref-table entries
        accessTypes.removeAll(ServiceDefUtil.ACCESS_TYPE_MARKERS);
        if (CollectionUtils.isNotEmpty(accessTypes)) {
            List<XXPolicyRefAccessType> xPolAccesses = new ArrayList<>();
            Map<String, XXAccessTypeDef> nameToAccessTypeDefMap = daoMgr.getXXAccessTypeDef().findByNamesAndServiceId(accessTypes, xPolicy.getService());
            for (String accessType : accessTypes) {
                XXAccessTypeDef xAccTypeDef = nameToAccessTypeDefMap.get(accessType);
                if (xAccTypeDef == null) {
                    throw new Exception(accessType + ": is not a valid access-type. policy='" + policy.getName() + "' service='" + policy.getService() + "'");
                }

                XXPolicyRefAccessType xPolAccess = new XXPolicyRefAccessType();

                xPolAccess.setPolicyId(policy.getId());
                xPolAccess.setAccessDefId(xAccTypeDef.getId());
                xPolAccess.setAccessTypeName(accessType);

                xPolAccesses.add(xPolAccess);
            }
            batchInsert(xPolAccesses, daoMgr.getXXPolicyRefAccessType(), oldBulkMode);
        }

        if (CollectionUtils.isNotEmpty(conditionTypes)) {
            List<XXPolicyRefCondition> xPolConds = new ArrayList<>();
            Map<String, XXPolicyConditionDef> nameToConditionDefMap = daoMgr.getXXPolicyConditionDef().findByServiceDefIdAndNames(xServiceDef.getId(), conditionTypes);
            for (String condition : conditionTypes) {
                XXPolicyConditionDef xPolCondDef = nameToConditionDefMap.get(condition);
                if (xPolCondDef == null) {
                    if (StringUtils.equalsIgnoreCase(condition, ServiceDefUtil.IMPLICIT_CONDITION_EXPRESSION_NAME)) {
                        continue;
                    }
                    throw new Exception(condition + ": is not a valid condition-type. policy='" + xPolicy.getName() + "' service='" + xPolicy.getService() + "'");
                }

                XXPolicyRefCondition xPolCond = new XXPolicyRefCondition();
                xPolCond.setPolicyId(policy.getId());
                xPolCond.setConditionDefId(xPolCondDef.getId());
                xPolCond.setConditionName(condition);

                xPolConds.add(xPolCond);
            }
            batchInsert(xPolConds, daoMgr.getXXPolicyRefCondition(), oldBulkMode);
        }

        if (CollectionUtils.isNotEmpty(dataMaskTypes)) {
            List<XXPolicyRefDataMaskType> xxDataMaskInfos = new ArrayList<>();
            Map<String, XXDataMaskTypeDef> namesToDataMaskTypeDefMap = daoMgr.getXXDataMaskTypeDef().findByNamesAndServiceId(dataMaskTypes, xPolicy.getService());
            for (String dataMaskType : dataMaskTypes) {
                XXDataMaskTypeDef dataMaskDef = namesToDataMaskTypeDefMap.get(dataMaskType);
                if (dataMaskDef == null) {
                    throw new Exception(dataMaskType + ": is not a valid datamask-type. policy='" + policy.getName() + "' service='" + policy.getService() + "'");
                }

                XXPolicyRefDataMaskType xxDataMaskInfo = new XXPolicyRefDataMaskType();

                xxDataMaskInfo.setPolicyId(policy.getId());
                xxDataMaskInfo.setDataMaskDefId(dataMaskDef.getId());
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
        XXPolicyRefUserDao xPolicyRefUserDao = daoMgr.getXXPolicyRefUser();
        XXPolicyRefRoleDao xxPolicyRefRoleDao = daoMgr.getXXPolicyRefRole();
        XXPolicyRefGroupDao xxPolicyRefGroupDao = daoMgr.getXXPolicyRefGroup();

        Map<String, Long> policyUserNameIdMap   = xPolicyRefUserDao.findUserNameIdByPolicyId(policyId);
        Map<String, Long> policyRoleNameIdMap   = xxPolicyRefRoleDao.findRoleNameIdByPolicyId(policyId);
        Map<String, Long> policyGroupNameIdMap   = xxPolicyRefGroupDao.findGroupNameByPolicyId(policyId);
        cleanupPolicyRefUsers(policyUsers, policyUserNameIdMap, policyId, xPolicyRefUserDao);
        cleanupPolicyRefRoles(policyRoles, policyRoleNameIdMap, policyId, xxPolicyRefRoleDao);
        cleanupPolicyRefGroups(policyGroups, policyGroupNameIdMap, policyId, xxPolicyRefGroupDao);
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
     * @param policyUsers           Set of usernames from the new policy.
     * @param policyUserNameIdMap   Existing mapping of usernames to their DB IDs for the policy.
     * @param policyId              The ID of the policy.
     * @param dao                   DAO for policy reference users.
     */
    public void cleanupPolicyRefUsers(Set<String> policyUsers, Map<String, Long> policyUserNameIdMap, Long policyId, XXPolicyRefUserDao dao) {
        if (policyUserNameIdMap != null && !policyUserNameIdMap.isEmpty()) {
            Set<String> existingPolicyRefUsers = policyUserNameIdMap.keySet();
            Set<String> toDeletePolicyRefUsers = new HashSet<>(existingPolicyRefUsers);
            Set<String> commonUsers = new HashSet<>(policyUsers);
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
     * @param policyRoles           Set of role names from the new policy.
     * @param policyRoleNameIdMap   Existing mapping of role names to their DB IDs for the policy.
     * @param policyId              The ID of the policy.
     * @param dao                   DAO for policy reference roles.
     */
    public void cleanupPolicyRefRoles(Set<String> policyRoles, Map<String, Long> policyRoleNameIdMap, Long policyId, XXPolicyRefRoleDao dao) {
        if (policyRoleNameIdMap != null && !policyRoleNameIdMap.isEmpty()) {
            Set<String> existingPolicyRefRoles = policyRoleNameIdMap.keySet();
            Set<String> toDeletePolicyRefRoles = new HashSet<>(existingPolicyRefRoles);
            Set<String> commonRoles = new HashSet<>(policyRoles);
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
     * @param policyGroups           Set of group names from the new policy.
     * @param policyGroupNameIdMap   Existing mapping of group names to their DB IDs for the policy.
     * @param policyId              The ID of the policy.
     * @param dao                   DAO for policy reference groups.
     */
    public void cleanupPolicyRefGroups(Set<String> policyGroups, Map<String, Long> policyGroupNameIdMap, Long policyId, XXPolicyRefGroupDao dao) {
        if (policyGroupNameIdMap != null && !policyGroupNameIdMap.isEmpty()) {
            Set<String> existingPolicyRefGroups = policyGroupNameIdMap.keySet();
            Set<String> toDeletePolicyRefGroups = new HashSet<>(existingPolicyRefGroups);
            Set<String> commonGroups = new HashSet<>(policyGroups);
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

    private class PolicyPrincipalAssociator implements Runnable {
        final PRINCIPAL_TYPE type;
        final String         name;
        final XXPolicy       xPolicy;
        final Long id;
        final List<XXPolicyRefUser> xPolUsers;
        final List<XXPolicyRefGroup> xPolGroups;
        final List<XXPolicyRefRole> xPolRoles;

        public PolicyPrincipalAssociator(PRINCIPAL_TYPE type, String name, XXPolicy xPolicy, Long id, List<XXPolicyRefUser> xPolUsers, List<XXPolicyRefGroup> xPolGroups, List<XXPolicyRefRole> xPolRoles) {
            this.type    = type;
            this.name    = name;
            this.xPolicy = xPolicy;
            this.id = id;
            this.xPolUsers = xPolUsers;
            this.xPolGroups = xPolGroups;
            this.xPolRoles = xPolRoles;
        }

        @Override
        public void run() {
            if (doAssociate(true)) {
                LOG.debug("Associated {}:{} with policy id:[{}]", type.name(), name, xPolicy.getId());
            } else {
                throw new RuntimeException("Failed to associate " + type.name() + ":" + name + " with policy id:[" + xPolicy.getId() + "]");
            }
        }

        boolean doAssociate(boolean isAdmin) {
            LOG.debug("===> PolicyPrincipalAssociator.doAssociate({})", isAdmin);

            final boolean ret;

            Long id = createOrGetPrincipal(isAdmin);

            if (id != null) {
                // associate with policy
                createPolicyAssociation(id, name, isAdmin);

                ret = true;
            } else {
                ret = false;
            }

            LOG.debug("<=== PolicyPrincipalAssociator.doAssociate({}) : {}", isAdmin, ret);

            return ret;
        }

        private Long createOrGetPrincipal(final boolean createIfAbsent) {
            LOG.debug("===> PolicyPrincipalAssociator.createOrGetPrincipal({})", createIfAbsent);

            if (id != null) {
                return id;
            }

            Long ret = null;

            switch (type) {
                case USER: {
                    XXUser xUser = daoMgr.getXXUser().findByUserName(name);

                    if (xUser != null) {
                        ret = xUser.getId();
                    } else {
                        if (createIfAbsent) {
                            ret = createPrincipal(name);
                        }
                    }
                }
                break;
                case GROUP: {
                    XXGroup xGroup = daoMgr.getXXGroup().findByGroupName(name);

                    if (xGroup != null) {
                        ret = xGroup.getId();
                    } else {
                        if (createIfAbsent) {
                            ret = createPrincipal(name);
                        }
                    }
                }
                break;
                case ROLE: {
                    XXRole xRole = daoMgr.getXXRole().findByRoleName(name);

                    if (xRole != null) {
                        ret = xRole.getId();
                    } else {
                        if (createIfAbsent) {
                            RangerBizUtil.setBulkMode(false);
                            ret = createPrincipal(name);
                        }
                    }
                }
                break;
                default:
                    break;
            }

            LOG.debug("<=== PolicyPrincipalAssociator.createOrGetPrincipal({}) : {}", createIfAbsent, ret);

            return ret;
        }

        private Long createPrincipal(String user) {
            LOG.warn("User specified in policy does not exist in ranger admin, creating new user, Type: {}, name = {}", type.name(), user);

            LOG.debug("===> PolicyPrincipalAssociator.createPrincipal(type={}, name={})", type.name(), name);

            Long ret = null;

            switch (type) {
                case USER: {
                    // Create External user
                    VXUser vXUser = xUserMgr.createServiceConfigUser(name);
                    if (vXUser != null) {
                        XXUser xUser = daoMgr.getXXUser().findByUserName(name);

                        if (xUser == null) {
                            LOG.error("No User created!! Irrecoverable error! [{}]", name);
                        } else {
                            ret = xUser.getId();
                        }
                    } else {
                        LOG.warn("serviceConfigUser:[{}] creation failed. This may be a transient/spurious condition that may correct itself when transaction is committed", name);
                    }
                }
                break;
                case GROUP: {
                    // Create group
                    VXGroup vxGroup = new VXGroup();

                    vxGroup.setName(name);
                    vxGroup.setDescription(name);
                    vxGroup.setGroupSource(RangerCommonEnums.GROUP_EXTERNAL);

                    VXGroup vXGroup = xGroupService.createXGroupWithOutLogin(vxGroup);

                    if (vXGroup != null) {
                        xGroupService.createTransactionLog(vXGroup, null, OPERATION_CREATE_CONTEXT, xPolicy.getAddedByUserId());

                        ret = vXGroup.getId();
                    }
                }
                break;
                case ROLE: {
                    try {
                        RangerRole rRole       = new RangerRole(name, null, null, null, null);
                        RangerRole createdRole = roleStore.createRole(rRole, false);

                        ret = createdRole.getId();
                    } catch (Exception e) {
                        // Ignore
                    }
                }
                break;
                default:
                    break;
            }

            LOG.debug("<=== PolicyPrincipalAssociator.createPrincipal(type={}, name={}) : {}", type.name(), name, ret);

            return ret;
        }

        private boolean doesPolicyExist(XXPolicy xPolicy) {
            return daoMgr.getXXPolicy().getById(xPolicy.getId()) != null;
        }

        private void createPolicyAssociation(Long id, String name, boolean isAdmin) {
            LOG.debug("===> PolicyPrincipalAssociator.createPolicyAssociation(policyId={}, type={}, name={}, id={}, isAdmin={})", xPolicy.getId(), type.name(),
                    name, id, isAdmin);

            if (doesPolicyExist(xPolicy)) {
                switch (type) {
                    case USER: {
                        XXPolicyRefUser xPolUser = new XXPolicyRefUser();

                        xPolUser.setPolicyId(xPolicy.getId());
                        xPolUser.setUserId(id);
                        xPolUser.setUserName(name);
                        if (isAdmin) {
                            daoMgr.getXXPolicyRefUser().create(xPolUser);
                        } else {
                            xPolUsers.add(xPolUser);
                        }
                    }
                    break;
                    case GROUP: {
                        XXPolicyRefGroup xPolGroup = new XXPolicyRefGroup();

                        xPolGroup.setPolicyId(xPolicy.getId());
                        xPolGroup.setGroupId(id);
                        xPolGroup.setGroupName(name);
                        if (isAdmin) {
                            daoMgr.getXXPolicyRefGroup().create(xPolGroup);
                        } else {
                            xPolGroups.add(xPolGroup);
                        }
                    }
                    break;
                    case ROLE: {
                        XXPolicyRefRole xPolRole = new XXPolicyRefRole();

                        xPolRole.setPolicyId(xPolicy.getId());
                        xPolRole.setRoleId(id);
                        xPolRole.setRoleName(name);
                        if (isAdmin) {
                            daoMgr.getXXPolicyRefRole().create(xPolRole);
                        } else {
                            xPolRoles.add(xPolRole);
                        }
                    }
                    break;
                    default:
                        break;
                }
            } else {
                LOG.info("Policy with id ={} does not exist, skipping policy association!", xPolicy.getId());
            }

            LOG.debug("<=== PolicyPrincipalAssociator.createPolicyAssociation(policyId={}, type={}, name={}, id={})", xPolicy.getId(), type.name(), name, id);
        }
    }

    private <T> void batchInsert(List<T> entities, BaseDao<T> dao, boolean oldBulkMode) {
        if (CollectionUtils.isNotEmpty(entities)) {
            long startTimeMs = System.currentTimeMillis();
            LOG.debug("Batch insert started for create/update {} with {} records.", dao.getClass().getSimpleName(), entities.size());
            RangerBizUtil.setBulkMode(false);
            dao.batchCreate(entities);
            RangerBizUtil.setBulkMode(oldBulkMode);
            long timeTakenMs = System.currentTimeMillis() - startTimeMs;
            LOG.debug("Batch insert completed for create/update {} with {} records in {} ms.", dao.getClass().getSimpleName(), entities.size(), timeTakenMs);
        }
    }
}
