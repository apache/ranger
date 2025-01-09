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

package org.apache.ranger.rest;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.util.GrantRevokeRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

public class ServiceRESTUtil {
    private static final Logger LOG = LoggerFactory.getLogger(ServiceRESTUtil.class);

    private ServiceRESTUtil() {
        //To block instantiation
    }

    public static boolean processGrantRequest(RangerPolicy policy, GrantRevokeRequest grantRequest) {
        LOG.debug("==> ServiceRESTUtil.processGrantRequest()");

        boolean policyUpdated = false;

        // replace all existing privileges for users, groups, and roles
        if (grantRequest.getReplaceExistingPermissions()) {
            policyUpdated = removeUsersGroupsAndRolesFromPolicy(policy, grantRequest.getUsers(), grantRequest.getGroups(), grantRequest.getRoles());
        }

        //Build a policy and set up policyItem in it to mimic grant request
        RangerPolicy appliedPolicy = new RangerPolicy();

        RangerPolicy.RangerPolicyItem policyItem = new RangerPolicy.RangerPolicyItem();

        policyItem.setDelegateAdmin(grantRequest.getDelegateAdmin());
        policyItem.addUsers(grantRequest.getUsers());
        policyItem.addGroups(grantRequest.getGroups());
        policyItem.addRoles(grantRequest.getRoles());

        List<RangerPolicy.RangerPolicyItemAccess> accesses = new ArrayList<>();

        Set<String> accessTypes = grantRequest.getAccessTypes();
        for (String accessType : accessTypes) {
            accesses.add(new RangerPolicy.RangerPolicyItemAccess(accessType, true));
        }

        policyItem.setAccesses(accesses);

        appliedPolicy.addPolicyItem(policyItem);

        processApplyPolicy(policy, appliedPolicy);

        policyUpdated = true;

        LOG.debug("<== ServiceRESTUtil.processGrantRequest() : {}", policyUpdated);

        return policyUpdated;
    }

    public static boolean processRevokeRequest(RangerPolicy existingRangerPolicy, GrantRevokeRequest revokeRequest) {
        LOG.debug("==> ServiceRESTUtil.processRevokeRequest()");

        boolean policyUpdated = false;

        // remove all existing privileges for users and groups
        if (revokeRequest.getReplaceExistingPermissions()) {
            policyUpdated = removeUsersGroupsAndRolesFromPolicy(existingRangerPolicy, revokeRequest.getUsers(), revokeRequest.getGroups(), revokeRequest.getRoles());
        } else {
            //Build a policy and set up policyItem in it to mimic revoke request
            RangerPolicy appliedRangerPolicy = new RangerPolicy();

            RangerPolicy.RangerPolicyItem appliedRangerPolicyItem = new RangerPolicy.RangerPolicyItem();

            appliedRangerPolicyItem.setDelegateAdmin(revokeRequest.getDelegateAdmin());
            appliedRangerPolicyItem.addUsers(revokeRequest.getUsers());
            appliedRangerPolicyItem.addGroups(revokeRequest.getGroups());
            appliedRangerPolicyItem.addRoles(revokeRequest.getRoles());

            List<RangerPolicy.RangerPolicyItemAccess> appliedRangerPolicyItemAccess = new ArrayList<>();

            Set<String> appliedPolicyItemAccessType = revokeRequest.getAccessTypes();
            for (String accessType : appliedPolicyItemAccessType) {
                appliedRangerPolicyItemAccess.add(new RangerPolicy.RangerPolicyItemAccess(accessType, false));
            }

            appliedRangerPolicyItem.setAccesses(appliedRangerPolicyItemAccess);

            appliedRangerPolicy.addPolicyItem(appliedRangerPolicyItem);

            List<RangerPolicy.RangerPolicyItem> appliedRangerPolicyItems = appliedRangerPolicy.getPolicyItems();
            //processApplyPolicyForItemType(existingRangerPolicy, appliedRangerPolicy, PolicyTermType.ALLOW);
            if (CollectionUtils.isNotEmpty(appliedRangerPolicyItems)) {
                Set<String> users  = new HashSet<>();
                Set<String> groups = new HashSet<>();
                Set<String> roles  = new HashSet<>();

                Map<String, RangerPolicy.RangerPolicyItem[]> userPolicyItems  = new HashMap<String, RangerPolicy.RangerPolicyItem[]>();
                Map<String, RangerPolicy.RangerPolicyItem[]> groupPolicyItems = new HashMap<String, RangerPolicy.RangerPolicyItem[]>();
                Map<String, RangerPolicy.RangerPolicyItem[]> rolePolicyItems  = new HashMap<String, RangerPolicy.RangerPolicyItem[]>();

                // Extract users, groups, and roles specified in appliedPolicy items
                extractUsersGroupsAndRoles(appliedRangerPolicyItems, users, groups, roles);

                // Split existing policyItems for users, groups, and roles extracted from appliedPolicyItem into userPolicyItems, groupPolicyItems and rolePolicyItems
                splitExistingPolicyItems(existingRangerPolicy, users, userPolicyItems, groups, groupPolicyItems, roles, rolePolicyItems);

                for (RangerPolicy.RangerPolicyItem tempPolicyItem : appliedRangerPolicyItems) {
                    List<String> appliedPolicyItemsUser = tempPolicyItem.getUsers();
                    for (String user : appliedPolicyItemsUser) {
                        RangerPolicy.RangerPolicyItem[] rangerPolicyItems = userPolicyItems.get(user);
                        if (rangerPolicyItems != null && rangerPolicyItems.length > 0) {
                            if (rangerPolicyItems[PolicyTermType.ALLOW.ordinal()] != null) {
                                removeAccesses(rangerPolicyItems[PolicyTermType.ALLOW.ordinal()], tempPolicyItem.getAccesses());
                                if (!CollectionUtils.isEmpty(rangerPolicyItems[PolicyTermType.ALLOW.ordinal()].getAccesses())) {
                                    rangerPolicyItems[PolicyTermType.ALLOW.ordinal()].setDelegateAdmin(revokeRequest.getDelegateAdmin());
                                } else {
                                    rangerPolicyItems[PolicyTermType.ALLOW.ordinal()].setDelegateAdmin(Boolean.FALSE);
                                }
                            }
                            if (rangerPolicyItems[PolicyTermType.DENY_EXCEPTIONS.ordinal()] != null) {
                                removeAccesses(rangerPolicyItems[PolicyTermType.DENY_EXCEPTIONS.ordinal()], tempPolicyItem.getAccesses());
                                rangerPolicyItems[PolicyTermType.DENY_EXCEPTIONS.ordinal()].setDelegateAdmin(Boolean.FALSE);
                            }
                        }
                    }
                }
                for (RangerPolicy.RangerPolicyItem tempPolicyItem : appliedRangerPolicyItems) {
                    List<String> appliedPolicyItemsGroup = tempPolicyItem.getGroups();
                    for (String group : appliedPolicyItemsGroup) {
                        RangerPolicy.RangerPolicyItem[] rangerPolicyItems = groupPolicyItems.get(group);
                        if (rangerPolicyItems != null && rangerPolicyItems.length > 0) {
                            if (rangerPolicyItems[PolicyTermType.ALLOW.ordinal()] != null) {
                                removeAccesses(rangerPolicyItems[PolicyTermType.ALLOW.ordinal()], tempPolicyItem.getAccesses());
                                if (!CollectionUtils.isEmpty(rangerPolicyItems[PolicyTermType.ALLOW.ordinal()].getAccesses())) {
                                    rangerPolicyItems[PolicyTermType.ALLOW.ordinal()].setDelegateAdmin(revokeRequest.getDelegateAdmin());
                                } else {
                                    rangerPolicyItems[PolicyTermType.ALLOW.ordinal()].setDelegateAdmin(Boolean.FALSE);
                                }
                            }
                            if (rangerPolicyItems[PolicyTermType.DENY_EXCEPTIONS.ordinal()] != null) {
                                removeAccesses(rangerPolicyItems[PolicyTermType.DENY_EXCEPTIONS.ordinal()], tempPolicyItem.getAccesses());
                                rangerPolicyItems[PolicyTermType.DENY_EXCEPTIONS.ordinal()].setDelegateAdmin(Boolean.FALSE);
                            }
                        }
                    }
                }

                for (RangerPolicy.RangerPolicyItem tempPolicyItem : appliedRangerPolicyItems) {
                    List<String> appliedPolicyItemsRole = tempPolicyItem.getRoles();
                    for (String role : appliedPolicyItemsRole) {
                        RangerPolicy.RangerPolicyItem[] rangerPolicyItems = rolePolicyItems.get(role);
                        if (rangerPolicyItems != null && rangerPolicyItems.length > 0) {
                            if (rangerPolicyItems[PolicyTermType.ALLOW.ordinal()] != null) {
                                removeAccesses(rangerPolicyItems[PolicyTermType.ALLOW.ordinal()], tempPolicyItem.getAccesses());
                                if (!CollectionUtils.isEmpty(rangerPolicyItems[PolicyTermType.ALLOW.ordinal()].getAccesses())) {
                                    rangerPolicyItems[PolicyTermType.ALLOW.ordinal()].setDelegateAdmin(revokeRequest.getDelegateAdmin());
                                } else {
                                    rangerPolicyItems[PolicyTermType.ALLOW.ordinal()].setDelegateAdmin(Boolean.FALSE);
                                }
                            }
                            if (rangerPolicyItems[PolicyTermType.DENY_EXCEPTIONS.ordinal()] != null) {
                                removeAccesses(rangerPolicyItems[PolicyTermType.DENY_EXCEPTIONS.ordinal()], tempPolicyItem.getAccesses());
                                rangerPolicyItems[PolicyTermType.DENY_EXCEPTIONS.ordinal()].setDelegateAdmin(Boolean.FALSE);
                            }
                        }
                    }
                }
                // Add modified/new policyItems back to existing policy
                mergeProcessedPolicyItems(existingRangerPolicy, userPolicyItems, groupPolicyItems, rolePolicyItems);
                compactPolicy(existingRangerPolicy);
            }

            policyUpdated = true;
        }

        LOG.debug("<== ServiceRESTUtil.processRevokeRequest() : {}", policyUpdated);

        return policyUpdated;
    }

    public static void processApplyPolicy(RangerPolicy existingPolicy, RangerPolicy appliedPolicy) {
        LOG.debug("==> ServiceRESTUtil.processApplyPolicy()");

        // Check if applied policy or existing policy contains any conditions
        if (ServiceRESTUtil.containsRangerCondition(existingPolicy) || ServiceRESTUtil.containsRangerCondition(appliedPolicy)) {
            LOG.info("Applied policy [{}] or existing policy [{}] contains condition(s). Combining two policies.", appliedPolicy, existingPolicy);
            combinePolicy(existingPolicy, appliedPolicy);
        } else {
            processApplyPolicyForItemType(existingPolicy, appliedPolicy, PolicyTermType.ALLOW);
            processApplyPolicyForItemType(existingPolicy, appliedPolicy, PolicyTermType.DENY);
            processApplyPolicyForItemType(existingPolicy, appliedPolicy, PolicyTermType.ALLOW_EXCEPTIONS);
            processApplyPolicyForItemType(existingPolicy, appliedPolicy, PolicyTermType.DENY_EXCEPTIONS);
        }

        LOG.debug("<== ServiceRESTUtil.processApplyPolicy()");
    }

    public static void mergeExactMatchPolicyForResource(RangerPolicy existingPolicy, RangerPolicy appliedPolicy) {
        LOG.debug("==> ServiceRESTUtil.mergeExactMatchPolicyForResource()");
        mergeExactMatchPolicyForItemType(existingPolicy, appliedPolicy, PolicyTermType.ALLOW);
        mergeExactMatchPolicyForItemType(existingPolicy, appliedPolicy, PolicyTermType.DENY);
        mergeExactMatchPolicyForItemType(existingPolicy, appliedPolicy, PolicyTermType.ALLOW_EXCEPTIONS);
        mergeExactMatchPolicyForItemType(existingPolicy, appliedPolicy, PolicyTermType.DENY_EXCEPTIONS);
        LOG.debug("<== ServiceRESTUtil.mergeExactMatchPolicyForResource()");
    }

    static void addPolicyItemForUser(RangerPolicy.RangerPolicyItem[] items, int typeOfItems, String user, RangerPolicy.RangerPolicyItem policyItem) {
        if (items[typeOfItems] == null) {
            RangerPolicy.RangerPolicyItem newItem = new RangerPolicy.RangerPolicyItem();
            newItem.addUser(user);

            items[typeOfItems] = newItem;
        }

        addAccesses(items[typeOfItems], policyItem.getAccesses());

        if (policyItem.getDelegateAdmin()) {
            items[typeOfItems].setDelegateAdmin(Boolean.TRUE);
        }
    }

    static void addPolicyItemForGroup(RangerPolicy.RangerPolicyItem[] items, int typeOfItems, String group, RangerPolicy.RangerPolicyItem policyItem) {
        if (items[typeOfItems] == null) {
            RangerPolicy.RangerPolicyItem newItem = new RangerPolicy.RangerPolicyItem();
            newItem.addGroup(group);

            items[typeOfItems] = newItem;
        }

        addAccesses(items[typeOfItems], policyItem.getAccesses());

        if (policyItem.getDelegateAdmin()) {
            items[typeOfItems].setDelegateAdmin(Boolean.TRUE);
        }
    }

    static void addPolicyItemForRole(RangerPolicy.RangerPolicyItem[] items, int typeOfItems, String role, RangerPolicy.RangerPolicyItem policyItem) {
        if (items[typeOfItems] == null) {
            RangerPolicy.RangerPolicyItem newItem = new RangerPolicy.RangerPolicyItem();
            newItem.addRole(role);

            items[typeOfItems] = newItem;
        }

        addAccesses(items[typeOfItems], policyItem.getAccesses());

        if (policyItem.getDelegateAdmin()) {
            items[typeOfItems].setDelegateAdmin(Boolean.TRUE);
        }
    }

    static boolean containsRangerCondition(RangerPolicy policy) {
        boolean ret = false;

        LOG.debug("==> ServiceRESTUtil.containsRangerCondition({})", policy);

        if (policy != null) {
            if (CollectionUtils.isNotEmpty(policy.getConditions())) {
                ret = true;
            } else {
                List<RangerPolicy.RangerPolicyItem> allItems = new ArrayList<RangerPolicy.RangerPolicyItem>();

                allItems.addAll(policy.getPolicyItems());
                allItems.addAll(policy.getDenyPolicyItems());
                allItems.addAll(policy.getAllowExceptions());
                allItems.addAll(policy.getDenyExceptions());

                for (RangerPolicy.RangerPolicyItem policyItem : allItems) {
                    if (!policyItem.getConditions().isEmpty()) {
                        ret = true;
                        break;
                    }
                }
            }
        }

        LOG.debug("<== ServiceRESTUtil.containsRangerCondition({}):{}", policy, ret);

        return ret;
    }

    private static void combinePolicy(RangerPolicy existingPolicy, RangerPolicy appliedPolicy) {
        combinePolicyItems(existingPolicy, appliedPolicy, PolicyTermType.ALLOW);
        combinePolicyItems(existingPolicy, appliedPolicy, PolicyTermType.DENY);
        combinePolicyItems(existingPolicy, appliedPolicy, PolicyTermType.ALLOW_EXCEPTIONS);
        combinePolicyItems(existingPolicy, appliedPolicy, PolicyTermType.DENY_EXCEPTIONS);
    }

    private static void combinePolicyItems(RangerPolicy existingPolicy, RangerPolicy appliedPolicy, PolicyTermType polityItemType) {
        List<RangerPolicy.RangerPolicyItem> existingPolicyItems;
        List<RangerPolicy.RangerPolicyItem> appliedPolicyItems;

        switch (polityItemType) {
            case ALLOW:
                existingPolicyItems = existingPolicy.getPolicyItems();
                appliedPolicyItems = appliedPolicy.getPolicyItems();
                break;
            case DENY:
                existingPolicyItems = existingPolicy.getDenyPolicyItems();
                appliedPolicyItems = appliedPolicy.getDenyPolicyItems();
                break;
            case ALLOW_EXCEPTIONS:
                existingPolicyItems = existingPolicy.getAllowExceptions();
                appliedPolicyItems = appliedPolicy.getAllowExceptions();
                break;
            case DENY_EXCEPTIONS:
                existingPolicyItems = existingPolicy.getDenyExceptions();
                appliedPolicyItems = appliedPolicy.getDenyExceptions();
                break;
            default:
                existingPolicyItems = null;
                appliedPolicyItems = null;
                break;
        }

        if (CollectionUtils.isNotEmpty(appliedPolicyItems)) {
            if (CollectionUtils.isNotEmpty(existingPolicyItems)) {
                List<RangerPolicy.RangerPolicyItem> itemsToAdd = new ArrayList<>();
                for (RangerPolicy.RangerPolicyItem appliedPolicyItem : appliedPolicyItems) {
                    if (!existingPolicyItems.contains(appliedPolicyItem)) {
                        itemsToAdd.add(appliedPolicyItem);
                    }
                }
                existingPolicyItems.addAll(itemsToAdd);
            } else {
                switch (polityItemType) {
                    case ALLOW:
                        existingPolicy.setPolicyItems(appliedPolicyItems);
                        break;
                    case DENY:
                        existingPolicy.setDenyPolicyItems(appliedPolicyItems);
                        break;
                    case ALLOW_EXCEPTIONS:
                        existingPolicy.setAllowExceptions(appliedPolicyItems);
                        break;
                    case DENY_EXCEPTIONS:
                        existingPolicy.setDenyExceptions(appliedPolicyItems);
                        break;
                }
            }
        }
    }

    private static void processApplyPolicyForItemType(RangerPolicy existingPolicy, RangerPolicy appliedPolicy, PolicyTermType policyItemType) {
        LOG.debug("==> ServiceRESTUtil.processApplyPolicyForItemType()");

        List<RangerPolicy.RangerPolicyItem> appliedPolicyItems = null;

        switch (policyItemType) {
            case ALLOW:
                appliedPolicyItems = appliedPolicy.getPolicyItems();
                break;
            case DENY:
                appliedPolicyItems = appliedPolicy.getDenyPolicyItems();
                break;
            case ALLOW_EXCEPTIONS:
                appliedPolicyItems = appliedPolicy.getAllowExceptions();
                break;
            case DENY_EXCEPTIONS:
                appliedPolicyItems = appliedPolicy.getDenyExceptions();
                break;
            default:
                LOG.warn("processApplyPolicyForItemType(): invalid policyItemType={}", policyItemType);
        }

        if (CollectionUtils.isNotEmpty(appliedPolicyItems)) {
            Set<String> users  = new HashSet<>();
            Set<String> groups = new HashSet<>();
            Set<String> roles  = new HashSet<>();

            Map<String, RangerPolicy.RangerPolicyItem[]> userPolicyItems  = new HashMap<String, RangerPolicy.RangerPolicyItem[]>();
            Map<String, RangerPolicy.RangerPolicyItem[]> groupPolicyItems = new HashMap<String, RangerPolicy.RangerPolicyItem[]>();
            Map<String, RangerPolicy.RangerPolicyItem[]> rolePolicyItems  = new HashMap<String, RangerPolicy.RangerPolicyItem[]>();

            // Extract users, groups, and roles specified in appliedPolicy items
            extractUsersGroupsAndRoles(appliedPolicyItems, users, groups, roles);

            // Split existing policyItems for users, groups, and roles extracted from appliedPolicyItem into userPolicyItems, groupPolicyItems, and rolePolicyItems
            splitExistingPolicyItems(existingPolicy, users, userPolicyItems, groups, groupPolicyItems, roles, rolePolicyItems);

            // Apply policyItems of given type in appliedPolicy to policyItems extracted from existingPolicy
            applyPolicyItems(appliedPolicyItems, policyItemType, userPolicyItems, groupPolicyItems, rolePolicyItems);

            // Add modified/new policyItems back to existing policy
            mergeProcessedPolicyItems(existingPolicy, userPolicyItems, groupPolicyItems, rolePolicyItems);

            compactPolicy(existingPolicy);
        }

        LOG.debug("<== ServiceRESTUtil.processApplyPolicyForItemType()");
    }

    private static void mergeExactMatchPolicyForItemType(RangerPolicy existingPolicy, RangerPolicy appliedPolicy, PolicyTermType policyItemType) {
        LOG.debug("==> ServiceRESTUtil.mergeExactMatchPolicyForItemType()");
        List<RangerPolicy.RangerPolicyItem> appliedPolicyItems = null;
        switch (policyItemType) {
            case ALLOW:
                appliedPolicyItems = appliedPolicy.getPolicyItems();
                break;
            case DENY:
                appliedPolicyItems = appliedPolicy.getDenyPolicyItems();
                break;
            case ALLOW_EXCEPTIONS:
                appliedPolicyItems = appliedPolicy.getAllowExceptions();
                break;
            case DENY_EXCEPTIONS:
                appliedPolicyItems = appliedPolicy.getDenyExceptions();
                break;
            default:
                LOG.warn("mergeExactMatchPolicyForItemType(): invalid policyItemType={}", policyItemType);
        }

        if (CollectionUtils.isNotEmpty(appliedPolicyItems)) {
            Set<String> users  = new HashSet<>();
            Set<String> groups = new HashSet<>();
            Set<String> roles  = new HashSet<>();

            Map<String, RangerPolicy.RangerPolicyItem[]> userPolicyItems  = new HashMap<String, RangerPolicy.RangerPolicyItem[]>();
            Map<String, RangerPolicy.RangerPolicyItem[]> groupPolicyItems = new HashMap<String, RangerPolicy.RangerPolicyItem[]>();
            Map<String, RangerPolicy.RangerPolicyItem[]> rolePolicyItems  = new HashMap<String, RangerPolicy.RangerPolicyItem[]>();

            // Extract users and groups specified in appliedPolicy items
            extractUsersGroupsAndRoles(appliedPolicyItems, users, groups, roles);

            // Split existing policyItems for users and groups extracted from appliedPolicyItem into userPolicyItems and groupPolicyItems
            splitExistingPolicyItems(existingPolicy, users, userPolicyItems, groups, groupPolicyItems, roles, rolePolicyItems);
            // Apply policyItems of given type in appliedPlicy to policyItems extracted from existingPolicy
            mergePolicyItems(appliedPolicyItems, policyItemType, userPolicyItems, groupPolicyItems, rolePolicyItems);
            // Add modified/new policyItems back to existing policy
            mergeProcessedPolicyItems(existingPolicy, userPolicyItems, groupPolicyItems, rolePolicyItems);
            compactPolicy(existingPolicy);
        }

        LOG.debug("<== ServiceRESTUtil.mergeExactMatchPolicyForItemType()");
    }

    private static void extractUsersGroupsAndRoles(List<RangerPolicy.RangerPolicyItem> policyItems, Set<String> users, Set<String> groups, Set<String> roles) {
        LOG.debug("==> ServiceRESTUtil.extractUsersGroupsAndRoles()");
        if (CollectionUtils.isNotEmpty(policyItems)) {
            for (RangerPolicy.RangerPolicyItem policyItem : policyItems) {
                if (CollectionUtils.isNotEmpty(policyItem.getUsers())) {
                    users.addAll(policyItem.getUsers());
                }
                if (CollectionUtils.isNotEmpty(policyItem.getGroups())) {
                    groups.addAll(policyItem.getGroups());
                }

                if (CollectionUtils.isNotEmpty(policyItem.getRoles())) {
                    roles.addAll(policyItem.getRoles());
                }
            }
        }
        LOG.debug("<== ServiceRESTUtil.extractUsersGroupsAndRoles()");
    }

    private static void splitExistingPolicyItems(RangerPolicy existingPolicy, Set<String> users, Map<String, RangerPolicy.RangerPolicyItem[]> userPolicyItems, Set<String> groups, Map<String, RangerPolicy.RangerPolicyItem[]> groupPolicyItems, Set<String> roles, Map<String, RangerPolicy.RangerPolicyItem[]> rolePolicyItems) {
        if (existingPolicy == null || users == null || userPolicyItems == null || groups == null || groupPolicyItems == null || roles == null || rolePolicyItems == null) {
            return;
        }

        LOG.debug("==> ServiceRESTUtil.splitExistingPolicyItems()");

        List<RangerPolicy.RangerPolicyItem> allowItems          = existingPolicy.getPolicyItems();
        List<RangerPolicy.RangerPolicyItem> denyItems           = existingPolicy.getDenyPolicyItems();
        List<RangerPolicy.RangerPolicyItem> allowExceptionItems = existingPolicy.getAllowExceptions();
        List<RangerPolicy.RangerPolicyItem> denyExceptionItems  = existingPolicy.getDenyExceptions();

        for (String user : users) {
            RangerPolicy.RangerPolicyItem[] value = userPolicyItems.get(user);
            if (value == null) {
                value = new RangerPolicy.RangerPolicyItem[4];
                userPolicyItems.put(user, value);
            }

            RangerPolicy.RangerPolicyItem policyItem = null;

            policyItem                                       = splitAndGetConsolidatedPolicyItemForUser(allowItems, user);
            value[PolicyTermType.ALLOW.ordinal()]            = policyItem;
            policyItem                                       = splitAndGetConsolidatedPolicyItemForUser(denyItems, user);
            value[PolicyTermType.DENY.ordinal()]             = policyItem;
            policyItem                                       = splitAndGetConsolidatedPolicyItemForUser(allowExceptionItems, user);
            value[PolicyTermType.ALLOW_EXCEPTIONS.ordinal()] = policyItem;
            policyItem                                       = splitAndGetConsolidatedPolicyItemForUser(denyExceptionItems, user);
            value[PolicyTermType.DENY_EXCEPTIONS.ordinal()]  = policyItem;
        }

        for (String group : groups) {
            RangerPolicy.RangerPolicyItem[] value = groupPolicyItems.get(group);
            if (value == null) {
                value = new RangerPolicy.RangerPolicyItem[4];
                groupPolicyItems.put(group, value);
            }

            RangerPolicy.RangerPolicyItem policyItem = null;

            policyItem                                       = splitAndGetConsolidatedPolicyItemForGroup(allowItems, group);
            value[PolicyTermType.ALLOW.ordinal()]            = policyItem;
            policyItem                                       = splitAndGetConsolidatedPolicyItemForGroup(denyItems, group);
            value[PolicyTermType.DENY.ordinal()]             = policyItem;
            policyItem                                       = splitAndGetConsolidatedPolicyItemForGroup(allowExceptionItems, group);
            value[PolicyTermType.ALLOW_EXCEPTIONS.ordinal()] = policyItem;
            policyItem                                       = splitAndGetConsolidatedPolicyItemForGroup(denyExceptionItems, group);
            value[PolicyTermType.DENY_EXCEPTIONS.ordinal()]  = policyItem;
        }
        for (String role : roles) {
            RangerPolicy.RangerPolicyItem[] value = rolePolicyItems.get(role);
            if (value == null) {
                value = new RangerPolicy.RangerPolicyItem[4];
                rolePolicyItems.put(role, value);
            }

            RangerPolicy.RangerPolicyItem policyItem = null;

            policyItem                                       = splitAndGetConsolidatedPolicyItemForRole(allowItems, role);
            value[PolicyTermType.ALLOW.ordinal()]            = policyItem;
            policyItem                                       = splitAndGetConsolidatedPolicyItemForRole(denyItems, role);
            value[PolicyTermType.DENY.ordinal()]             = policyItem;
            policyItem                                       = splitAndGetConsolidatedPolicyItemForRole(allowExceptionItems, role);
            value[PolicyTermType.ALLOW_EXCEPTIONS.ordinal()] = policyItem;
            policyItem                                       = splitAndGetConsolidatedPolicyItemForRole(denyExceptionItems, role);
            value[PolicyTermType.DENY_EXCEPTIONS.ordinal()]  = policyItem;
        }

        LOG.debug("<== ServiceRESTUtil.splitExistingPolicyItems()");
    }

    private static RangerPolicy.RangerPolicyItem splitAndGetConsolidatedPolicyItemForUser(List<RangerPolicy.RangerPolicyItem> policyItems, String user) {
        LOG.debug("==> ServiceRESTUtil.splitAndGetConsolidatedPolicyItemForUser()");

        RangerPolicy.RangerPolicyItem ret = null;

        if (CollectionUtils.isNotEmpty(policyItems)) {
            for (RangerPolicy.RangerPolicyItem policyItem : policyItems) {
                List<String> users = policyItem.getUsers();
                if (users.contains(user)) {
                    if (ret == null) {
                        ret = new RangerPolicy.RangerPolicyItem();
                    }
                    ret.addUser(user);
                    if (policyItem.getDelegateAdmin()) {
                        ret.setDelegateAdmin(Boolean.TRUE);
                    }
                    addAccesses(ret, policyItem.getAccesses());

                    // Remove this user from existingPolicyItem
                    users.remove(user);
                }
            }
        }

        LOG.debug("<== ServiceRESTUtil.splitAndGetConsolidatedPolicyItemForUser()");

        return ret;
    }

    private static RangerPolicy.RangerPolicyItem splitAndGetConsolidatedPolicyItemForGroup(List<RangerPolicy.RangerPolicyItem> policyItems, String group) {
        LOG.debug("==> ServiceRESTUtil.splitAndGetConsolidatedPolicyItemForGroup()");

        RangerPolicy.RangerPolicyItem ret = null;

        if (CollectionUtils.isNotEmpty(policyItems)) {
            for (RangerPolicy.RangerPolicyItem policyItem : policyItems) {
                List<String> groups = policyItem.getGroups();
                if (groups.contains(group)) {
                    if (ret == null) {
                        ret = new RangerPolicy.RangerPolicyItem();
                    }
                    ret.addGroup(group);
                    if (policyItem.getDelegateAdmin()) {
                        ret.setDelegateAdmin(Boolean.TRUE);
                    }
                    addAccesses(ret, policyItem.getAccesses());

                    // Remove this group from existingPolicyItem
                    groups.remove(group);
                }
            }
        }

        LOG.debug("<== ServiceRESTUtil.splitAndGetConsolidatedPolicyItemForGroup()");

        return ret;
    }

    private static RangerPolicy.RangerPolicyItem splitAndGetConsolidatedPolicyItemForRole(List<RangerPolicy.RangerPolicyItem> policyItems, String role) {
        LOG.debug("==> ServiceRESTUtil.splitAndGetConsolidatedPolicyItemForGroup()");

        RangerPolicy.RangerPolicyItem ret = null;

        if (CollectionUtils.isNotEmpty(policyItems)) {
            for (RangerPolicy.RangerPolicyItem policyItem : policyItems) {
                List<String> roles = policyItem.getRoles();
                if (roles.contains(role)) {
                    if (ret == null) {
                        ret = new RangerPolicy.RangerPolicyItem();
                    }
                    ret.addRole(role);
                    if (policyItem.getDelegateAdmin()) {
                        ret.setDelegateAdmin(Boolean.TRUE);
                    }
                    addAccesses(ret, policyItem.getAccesses());

                    // Remove this role from existingPolicyItem
                    roles.remove(role);
                }
            }
        }

        LOG.debug("<== ServiceRESTUtil.splitAndGetConsolidatedPolicyItemForGroup()");

        return ret;
    }

    private static void applyPolicyItems(List<RangerPolicy.RangerPolicyItem> appliedPolicyItems, PolicyTermType policyItemType, Map<String, RangerPolicy.RangerPolicyItem[]> existingUserPolicyItems, Map<String, RangerPolicy.RangerPolicyItem[]> existingGroupPolicyItems, Map<String, RangerPolicy.RangerPolicyItem[]> existingRolePolicyItems) {
        LOG.debug("==> ServiceRESTUtil.applyPolicyItems()");

        for (RangerPolicy.RangerPolicyItem policyItem : appliedPolicyItems) {
            List<String> users = policyItem.getUsers();
            for (String user : users) {
                RangerPolicy.RangerPolicyItem[] existingPolicyItems = existingUserPolicyItems.get(user);

                if (existingPolicyItems == null) {
                    // Should not get here
                    LOG.warn("Should not have come here..");
                    existingPolicyItems = new RangerPolicy.RangerPolicyItem[4];
                    existingUserPolicyItems.put(user, existingPolicyItems);
                }

                addPolicyItemForUser(existingPolicyItems, policyItemType.ordinal(), user, policyItem);

                switch (policyItemType) {
                    case ALLOW:
                        RangerPolicy.RangerPolicyItem denyPolicyItem = existingPolicyItems[PolicyTermType.DENY.ordinal()];
                        if (denyPolicyItem != null) {
                            removeAccesses(existingPolicyItems[PolicyTermType.DENY.ordinal()], policyItem.getAccesses());
                            addPolicyItemForUser(existingPolicyItems, PolicyTermType.DENY_EXCEPTIONS.ordinal(), user, policyItem);
                        }
                        removeAccesses(existingPolicyItems[PolicyTermType.ALLOW_EXCEPTIONS.ordinal()], policyItem.getAccesses());
                        break;
                    case DENY:
                        RangerPolicy.RangerPolicyItem allowPolicyItem = existingPolicyItems[PolicyTermType.ALLOW.ordinal()];
                        if (allowPolicyItem != null) {
                            removeAccesses(existingPolicyItems[PolicyTermType.ALLOW.ordinal()], policyItem.getAccesses());
                            addPolicyItemForUser(existingPolicyItems, PolicyTermType.ALLOW_EXCEPTIONS.ordinal(), user, policyItem);
                        }
                        removeAccesses(existingPolicyItems[PolicyTermType.DENY_EXCEPTIONS.ordinal()], policyItem.getAccesses());
                        break;
                    case ALLOW_EXCEPTIONS:
                        removeAccesses(existingPolicyItems[PolicyTermType.ALLOW.ordinal()], policyItem.getAccesses());
                        break;
                    case DENY_EXCEPTIONS:
                        removeAccesses(existingPolicyItems[PolicyTermType.DENY.ordinal()], policyItem.getAccesses());
                        break;
                    default:
                        LOG.warn("Should not have come here..");
                        break;
                }
            }
        }

        for (RangerPolicy.RangerPolicyItem policyItem : appliedPolicyItems) {
            List<String> groups = policyItem.getGroups();
            for (String group : groups) {
                RangerPolicy.RangerPolicyItem[] existingPolicyItems = existingGroupPolicyItems.get(group);

                if (existingPolicyItems == null) {
                    // Should not get here
                    existingPolicyItems = new RangerPolicy.RangerPolicyItem[4];
                    existingGroupPolicyItems.put(group, existingPolicyItems);
                }

                addPolicyItemForGroup(existingPolicyItems, policyItemType.ordinal(), group, policyItem);

                switch (policyItemType) {
                    case ALLOW:
                        RangerPolicy.RangerPolicyItem denyPolicyItem = existingPolicyItems[PolicyTermType.DENY.ordinal()];
                        if (denyPolicyItem != null) {
                            removeAccesses(existingPolicyItems[PolicyTermType.DENY.ordinal()], policyItem.getAccesses());
                            addPolicyItemForGroup(existingPolicyItems, PolicyTermType.DENY_EXCEPTIONS.ordinal(), group, policyItem);
                        }
                        removeAccesses(existingPolicyItems[PolicyTermType.ALLOW_EXCEPTIONS.ordinal()], policyItem.getAccesses());
                        break;
                    case DENY:
                        RangerPolicy.RangerPolicyItem allowPolicyItem = existingPolicyItems[PolicyTermType.ALLOW.ordinal()];
                        if (allowPolicyItem != null) {
                            removeAccesses(existingPolicyItems[PolicyTermType.ALLOW.ordinal()], policyItem.getAccesses());
                            addPolicyItemForGroup(existingPolicyItems, PolicyTermType.ALLOW_EXCEPTIONS.ordinal(), group, policyItem);
                        }
                        removeAccesses(existingPolicyItems[PolicyTermType.DENY_EXCEPTIONS.ordinal()], policyItem.getAccesses());
                        break;
                    case ALLOW_EXCEPTIONS:
                        removeAccesses(existingPolicyItems[PolicyTermType.ALLOW.ordinal()], policyItem.getAccesses());
                        break;
                    case DENY_EXCEPTIONS:
                        removeAccesses(existingPolicyItems[PolicyTermType.DENY.ordinal()], policyItem.getAccesses());
                        break;
                    default:
                        break;
                }
            }
        }

        for (RangerPolicy.RangerPolicyItem policyItem : appliedPolicyItems) {
            List<String> roles = policyItem.getRoles();
            for (String role : roles) {
                RangerPolicy.RangerPolicyItem[] existingPolicyItems = existingRolePolicyItems.get(role);

                if (existingPolicyItems == null) {
                    // Should not get here
                    existingPolicyItems = new RangerPolicy.RangerPolicyItem[4];
                    existingRolePolicyItems.put(role, existingPolicyItems);
                }

                addPolicyItemForRole(existingPolicyItems, policyItemType.ordinal(), role, policyItem);

                switch (policyItemType) {
                    case ALLOW:
                        RangerPolicy.RangerPolicyItem denyPolicyItem = existingPolicyItems[PolicyTermType.DENY.ordinal()];
                        if (denyPolicyItem != null) {
                            removeAccesses(existingPolicyItems[PolicyTermType.DENY.ordinal()], policyItem.getAccesses());
                            addPolicyItemForRole(existingPolicyItems, PolicyTermType.DENY_EXCEPTIONS.ordinal(), role, policyItem);
                        }
                        removeAccesses(existingPolicyItems[PolicyTermType.ALLOW_EXCEPTIONS.ordinal()], policyItem.getAccesses());
                        break;
                    case DENY:
                        RangerPolicy.RangerPolicyItem allowPolicyItem = existingPolicyItems[PolicyTermType.ALLOW.ordinal()];
                        if (allowPolicyItem != null) {
                            removeAccesses(existingPolicyItems[PolicyTermType.ALLOW.ordinal()], policyItem.getAccesses());
                            addPolicyItemForRole(existingPolicyItems, PolicyTermType.ALLOW_EXCEPTIONS.ordinal(), role, policyItem);
                        }
                        removeAccesses(existingPolicyItems[PolicyTermType.DENY_EXCEPTIONS.ordinal()], policyItem.getAccesses());
                        break;
                    case ALLOW_EXCEPTIONS:
                        removeAccesses(existingPolicyItems[PolicyTermType.ALLOW.ordinal()], policyItem.getAccesses());
                        break;
                    case DENY_EXCEPTIONS:
                        removeAccesses(existingPolicyItems[PolicyTermType.DENY.ordinal()], policyItem.getAccesses());
                        break;
                    default:
                        break;
                }
            }
        }

        LOG.debug("<== ServiceRESTUtil.applyPolicyItems()");
    }

    private static void mergePolicyItems(List<RangerPolicy.RangerPolicyItem> appliedPolicyItems, PolicyTermType policyItemType, Map<String, RangerPolicy.RangerPolicyItem[]> existingUserPolicyItems, Map<String, RangerPolicy.RangerPolicyItem[]> existingGroupPolicyItems, Map<String, RangerPolicy.RangerPolicyItem[]> existingRolePolicyItems) {
        LOG.debug("==> ServiceRESTUtil.mergePolicyItems()");
        for (RangerPolicy.RangerPolicyItem policyItem : appliedPolicyItems) {
            List<String> users = policyItem.getUsers();
            for (String user : users) {
                RangerPolicy.RangerPolicyItem[] items = existingUserPolicyItems.get(user);
                if (items == null) {
                    // Should not get here
                    LOG.warn("Should not have come here..");
                    items = new RangerPolicy.RangerPolicyItem[4];
                    existingUserPolicyItems.put(user, items);
                }
                addPolicyItemForUser(items, policyItemType.ordinal(), user, policyItem);
            }
        }

        for (RangerPolicy.RangerPolicyItem policyItem : appliedPolicyItems) {
            List<String> groups = policyItem.getGroups();
            for (String group : groups) {
                RangerPolicy.RangerPolicyItem[] items = existingGroupPolicyItems.get(group);
                if (items == null) {
                    // Should not get here
                    items = new RangerPolicy.RangerPolicyItem[4];
                    existingGroupPolicyItems.put(group, items);
                }
                addPolicyItemForGroup(items, policyItemType.ordinal(), group, policyItem);
            }
        }

        for (RangerPolicy.RangerPolicyItem policyItem : appliedPolicyItems) {
            List<String> roles = policyItem.getRoles();
            for (String role : roles) {
                RangerPolicy.RangerPolicyItem[] items = existingRolePolicyItems.get(role);
                if (items == null) {
                    // Should not get here
                    items = new RangerPolicy.RangerPolicyItem[4];
                    existingRolePolicyItems.put(role, items);
                }
                addPolicyItemForRole(items, policyItemType.ordinal(), role, policyItem);
            }
        }

        LOG.debug("<== ServiceRESTUtil.mergePolicyItems()");
    }

    private static void mergeProcessedPolicyItems(RangerPolicy existingPolicy, Map<String, RangerPolicy.RangerPolicyItem[]> userPolicyItems, Map<String, RangerPolicy.RangerPolicyItem[]> groupPolicyItems, Map<String, RangerPolicy.RangerPolicyItem[]> rolePolicyItems) {
        LOG.debug("==> ServiceRESTUtil.mergeProcessedPolicyItems()");

        for (Map.Entry<String, RangerPolicy.RangerPolicyItem[]> entry : userPolicyItems.entrySet()) {
            RangerPolicy.RangerPolicyItem[] items = entry.getValue();

            RangerPolicy.RangerPolicyItem item = null;

            item = items[PolicyTermType.ALLOW.ordinal()];
            if (item != null) {
                existingPolicy.addPolicyItem(item);
            }

            item = items[PolicyTermType.DENY.ordinal()];
            if (item != null) {
                existingPolicy.addDenyPolicyItem(item);
            }

            item = items[PolicyTermType.ALLOW_EXCEPTIONS.ordinal()];
            if (item != null) {
                existingPolicy.addAllowException(item);
            }

            item = items[PolicyTermType.DENY_EXCEPTIONS.ordinal()];
            if (item != null) {
                existingPolicy.addDenyException(item);
            }
        }

        for (Map.Entry<String, RangerPolicy.RangerPolicyItem[]> entry : groupPolicyItems.entrySet()) {
            RangerPolicy.RangerPolicyItem[] items = entry.getValue();

            RangerPolicy.RangerPolicyItem item = null;

            item = items[PolicyTermType.ALLOW.ordinal()];
            if (item != null) {
                existingPolicy.addPolicyItem(item);
            }

            item = items[PolicyTermType.DENY.ordinal()];
            if (item != null) {
                existingPolicy.addDenyPolicyItem(item);
            }

            item = items[PolicyTermType.ALLOW_EXCEPTIONS.ordinal()];
            if (item != null) {
                existingPolicy.addAllowException(item);
            }

            item = items[PolicyTermType.DENY_EXCEPTIONS.ordinal()];
            if (item != null) {
                existingPolicy.addDenyException(item);
            }
        }

        for (Map.Entry<String, RangerPolicy.RangerPolicyItem[]> entry : rolePolicyItems.entrySet()) {
            RangerPolicy.RangerPolicyItem[] items = entry.getValue();

            RangerPolicy.RangerPolicyItem item = null;

            item = items[PolicyTermType.ALLOW.ordinal()];
            if (item != null) {
                existingPolicy.addPolicyItem(item);
            }

            item = items[PolicyTermType.DENY.ordinal()];
            if (item != null) {
                existingPolicy.addDenyPolicyItem(item);
            }

            item = items[PolicyTermType.ALLOW_EXCEPTIONS.ordinal()];
            if (item != null) {
                existingPolicy.addAllowException(item);
            }

            item = items[PolicyTermType.DENY_EXCEPTIONS.ordinal()];
            if (item != null) {
                existingPolicy.addDenyException(item);
            }
        }

        LOG.debug("<== ServiceRESTUtil.mergeProcessedPolicyItems()");
    }

    private static boolean addAccesses(RangerPolicy.RangerPolicyItem policyItem, List<RangerPolicy.RangerPolicyItemAccess> accesses) {
        LOG.debug("==> ServiceRESTUtil.addAccesses()");

        boolean ret = false;

        for (RangerPolicy.RangerPolicyItemAccess access : accesses) {
            RangerPolicy.RangerPolicyItemAccess policyItemAccess = null;
            String                              accessType       = access.getType();

            for (RangerPolicy.RangerPolicyItemAccess itemAccess : policyItem.getAccesses()) {
                if (StringUtils.equals(itemAccess.getType(), accessType)) {
                    policyItemAccess = itemAccess;
                    break;
                }
            }

            if (policyItemAccess != null) {
                if (!policyItemAccess.getIsAllowed()) {
                    policyItemAccess.setIsAllowed(Boolean.TRUE);
                    ret = true;
                }
            } else {
                policyItem.addAccess(new RangerPolicy.RangerPolicyItemAccess(accessType, Boolean.TRUE));
                ret = true;
            }
        }

        LOG.debug("<== ServiceRESTUtil.addAccesses() {}", ret);
        return ret;
    }

    private static boolean removeAccesses(RangerPolicy.RangerPolicyItem policyItem, List<RangerPolicy.RangerPolicyItemAccess> accesses) {
        LOG.debug("==> ServiceRESTUtil.removeAccesses()");

        boolean ret = false;

        if (policyItem != null) {
            for (RangerPolicy.RangerPolicyItemAccess access : accesses) {
                String accessType = access.getType();

                int numOfAccesses = policyItem.getAccesses().size();

                for (int i = 0; i < numOfAccesses; i++) {
                    RangerPolicy.RangerPolicyItemAccess itemAccess = policyItem.getAccesses().get(i);

                    if (StringUtils.equals(itemAccess.getType(), accessType)) {
                        policyItem.getAccesses().remove(i);
                        numOfAccesses--;
                        i--;

                        ret = true;
                    }
                }
            }
        }
        LOG.debug("<== ServiceRESTUtil.removeAccesses() {}", ret);
        return ret;
    }

    private static void compactPolicy(RangerPolicy policy) {
        policy.setPolicyItems(mergePolicyItems(policy.getPolicyItems()));
        policy.setDenyPolicyItems(mergePolicyItems(policy.getDenyPolicyItems()));
        policy.setAllowExceptions(mergePolicyItems(policy.getAllowExceptions()));
        policy.setDenyExceptions(mergePolicyItems(policy.getDenyExceptions()));
    }

    private static List<RangerPolicy.RangerPolicyItem> mergePolicyItems(List<RangerPolicy.RangerPolicyItem> policyItems) {
        List<RangerPolicy.RangerPolicyItem> ret = new ArrayList<RangerPolicy.RangerPolicyItem>();

        if (CollectionUtils.isNotEmpty(policyItems)) {
            Map<String, RangerPolicy.RangerPolicyItem> matchedPolicyItems = new HashMap<>();

            for (RangerPolicy.RangerPolicyItem policyItem : policyItems) {
                if ((CollectionUtils.isEmpty(policyItem.getUsers()) && CollectionUtils.isEmpty(policyItem.getGroups()) && CollectionUtils.isEmpty(policyItem.getRoles())) ||
                        (CollectionUtils.isEmpty(policyItem.getAccesses()) && !policyItem.getDelegateAdmin())) {
                    continue;
                }

                if (policyItem.getConditions().size() > 1) {
                    ret.add(policyItem);
                    continue;
                }
                TreeSet<String> accesses = new TreeSet<>();

                for (RangerPolicy.RangerPolicyItemAccess access : policyItem.getAccesses()) {
                    accesses.add(access.getType());
                }
                if (policyItem.getDelegateAdmin()) {
                    accesses.add("delegateAdmin");
                }

                String allAccessesString = accesses.toString();

                RangerPolicy.RangerPolicyItem matchingPolicyItem = matchedPolicyItems.get(allAccessesString);

                if (matchingPolicyItem != null) {
                    addDistinctUsers(policyItem.getUsers(), matchingPolicyItem);
                    addDistinctGroups(policyItem.getGroups(), matchingPolicyItem);
                    addDistinctRoles(policyItem.getRoles(), matchingPolicyItem);
                } else {
                    matchedPolicyItems.put(allAccessesString, policyItem);
                }
            }

            for (Map.Entry<String, RangerPolicy.RangerPolicyItem> entry : matchedPolicyItems.entrySet()) {
                ret.add(entry.getValue());
            }
        }

        return ret;
    }

    private static void addDistinctUsers(List<String> users, RangerPolicy.RangerPolicyItem policyItem) {
        for (String user : users) {
            if (!policyItem.getUsers().contains(user)) {
                policyItem.addUser(user);
            }
        }
    }

    private static void addDistinctGroups(List<String> groups, RangerPolicy.RangerPolicyItem policyItem) {
        for (String group : groups) {
            if (!policyItem.getGroups().contains(group)) {
                policyItem.addGroup(group);
            }
        }
    }

    private static void addDistinctRoles(List<String> roles, RangerPolicy.RangerPolicyItem policyItem) {
        for (String role : roles) {
            if (!policyItem.getRoles().contains(role)) {
                policyItem.addRole(role);
            }
        }
    }

    private static boolean removeUsersGroupsAndRolesFromPolicy(RangerPolicy policy, Set<String> users, Set<String> groups, Set<String> roles) {
        boolean policyUpdated = false;

        List<RangerPolicy.RangerPolicyItem> policyItems = policy.getPolicyItems();

        int numOfItems = policyItems.size();

        for (int i = 0; i < numOfItems; i++) {
            RangerPolicy.RangerPolicyItem policyItem = policyItems.get(i);

            if (CollectionUtils.containsAny(policyItem.getUsers(), users)) {
                policyItem.getUsers().removeAll(users);

                policyUpdated = true;
            }

            if (CollectionUtils.containsAny(policyItem.getGroups(), groups)) {
                policyItem.getGroups().removeAll(groups);

                policyUpdated = true;
            }

            if (CollectionUtils.containsAny(policyItem.getRoles(), roles)) {
                policyItem.getRoles().removeAll(roles);

                policyUpdated = true;
            }

            if (CollectionUtils.isEmpty(policyItem.getUsers()) && CollectionUtils.isEmpty(policyItem.getGroups()) && CollectionUtils.isEmpty(policyItem.getRoles())) {
                policyItems.remove(i);
                numOfItems--;
                i--;

                policyUpdated = true;
            }
        }

        return policyUpdated;
    }

    private enum PolicyTermType {
        ALLOW, DENY, ALLOW_EXCEPTIONS, DENY_EXCEPTIONS
    }
}
