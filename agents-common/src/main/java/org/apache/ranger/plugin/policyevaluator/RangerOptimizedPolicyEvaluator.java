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

package org.apache.ranger.plugin.policyevaluator;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.ranger.plugin.model.RangerBaseModelObject;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.policyengine.RangerAccessRequest;
import org.apache.ranger.plugin.policyengine.RangerAccessResource;
import org.apache.ranger.plugin.policyengine.RangerPolicyEngine;
import org.apache.ranger.plugin.policyengine.RangerPolicyEngineOptions;
import org.apache.ranger.plugin.util.RangerAccessRequestUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class RangerOptimizedPolicyEvaluator extends RangerDefaultPolicyEvaluator {
    private static final Logger LOG = LoggerFactory.getLogger(RangerOptimizedPolicyEvaluator.class);

    private Set<String> roles          = RangerBaseModelObject.nullSafeSet(null);
    private Set<String> groups         = RangerBaseModelObject.nullSafeSet(null);
    private Set<String> users          = RangerBaseModelObject.nullSafeSet(null);
    private Set<String> accessPerms    = RangerBaseModelObject.nullSafeSet(null);
    private boolean     delegateAdmin;
    private boolean     hasAllPerms;
    private boolean     hasPublicGroup;
    private boolean     hasCurrentUser;
    private boolean     hasResourceOwner;
    private boolean     hasAllEvaluatorsInitialized;

    // For computation of priority
    private static final String RANGER_POLICY_EVAL_MATCH_ANY_PATTERN_STRING   = "*";
    private static final String RANGER_POLICY_EVAL_MATCH_ONE_CHARACTER_STRING = "?";

    private static final int RANGER_POLICY_EVAL_SCORE_DEFAULT                         = 10000;

    private static final int RANGER_POLICY_EVAL_SCORE_MAX_DISCOUNT_RESOURCE          = 100;
    private static final int RANGER_POLICY_EVAL_SCORE_MAX_DISCOUNT_USERSGROUPS       =  25;
    private static final int RANGER_POLICY_EVAL_SCORE_MAX_DISCOUNT_ACCESS_TYPES      =  25;
    private static final int RANGER_POLICY_EVAL_SCORE_MAX_DISCOUNT_CUSTOM_CONDITIONS =  25;

    private static final int RANGER_POLICY_EVAL_SCORE_RESOURCE_DISCOUNT_MATCH_ANY_WILDCARD               = 25;
    private static final int RANGER_POLICY_EVAL_SCORE_RESOURCE_DISCOUNT_HAS_MATCH_ANY_WILDCARD           = 10;
    private static final int RANGER_POLICY_EVAL_SCORE_RESOURCE_DISCOUNT_HAS_MATCH_ONE_CHARACTER_WILDCARD =  5;
    private static final int RANGER_POLICY_EVAL_SCORE_RESOURCE_DISCOUNT_IS_EXCLUDES                      =  5;
    private static final int RANGER_POLICY_EVAL_SCORE_RESORUCE_DISCOUNT_IS_RECURSIVE                     =  5;
    private static final int RANGER_POLICY_EVAL_SCORE_CUSTOM_CONDITION_PENALTY                           =  5;
    private static final int RANGER_POLICY_EVAL_SCORE_DYNAMIC_RESOURCE_EVAL_PENALTY                      =  20;

    @Override
    public void init(RangerPolicy policy, RangerServiceDef serviceDef, RangerPolicyEngineOptions options) {
        if(LOG.isDebugEnabled()) {
            LOG.debug("==> RangerOptimizedPolicyEvaluator.init()");
        }

        super.init(policy, serviceDef, options);

        preprocessPolicyItems(policy.getPolicyItems());
        preprocessPolicyItems(policy.getDenyPolicyItems());
        preprocessPolicyItems(policy.getAllowExceptions());
        preprocessPolicyItems(policy.getDenyExceptions());
        preprocessPolicyItems(policy.getDataMaskPolicyItems());
        preprocessPolicyItems(policy.getRowFilterPolicyItems());

        hasAllPerms = checkIfHasAllPerms();

        for (String user : users) {
            if (!hasCurrentUser && RangerPolicyEngine.USER_CURRENT.equalsIgnoreCase(user)) {
                hasCurrentUser = true;
            }
            if (!hasResourceOwner && RangerPolicyEngine.RESOURCE_OWNER.equalsIgnoreCase(user)) {
                hasResourceOwner = true;
            }
            if (hasCurrentUser && hasResourceOwner) {
                break;
            }
        }

        if (policy.getIsDenyAllElse()) {
            hasPublicGroup = true;
        } else {
            for (String group : groups) {
                if (RangerPolicyEngine.GROUP_PUBLIC.equalsIgnoreCase(group)) {
                    hasPublicGroup = true;
                    break;
                }
            }
        }

        setEvalOrder(computeEvalOrder());

        if(LOG.isDebugEnabled()) {
            LOG.debug("<== RangerOptimizedPolicyEvaluator.init()");
        }
    }

    static class LevelResourceNames implements Comparable<LevelResourceNames> {
        final int level;
        final RangerPolicy.RangerPolicyResource policyResource;

        public LevelResourceNames(int level, RangerPolicy.RangerPolicyResource policyResource) {
            this.level = level;
            this.policyResource = policyResource;
        }

        @Override
        public int compareTo(LevelResourceNames other) {
            // Sort in ascending order of level numbers
            return Integer.compare(this.level, other.level);
        }

        @Override
        public boolean equals(Object other) {
            boolean ret = false;
            if (other != null && (other instanceof LevelResourceNames)) {
                ret = this == other || compareTo((LevelResourceNames) other) == 0;
            }
            return ret;
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(this.level);
        }
    }

    public int computeEvalOrder() {
        if(LOG.isDebugEnabled()) {
            LOG.debug("==> RangerOptimizedPolicyEvaluator.computeEvalOrder()");
        }

        int evalOrder = RANGER_POLICY_EVAL_SCORE_DEFAULT;

        RangerServiceDef                         serviceDef   = getServiceDef();
        List<RangerServiceDef.RangerResourceDef> resourceDefs = serviceDef.getResources();
        RangerPolicy                             policy       = getPolicy();
        List<LevelResourceNames>                 tmpList      = new ArrayList<>();

        for (Map.Entry<String, RangerPolicy.RangerPolicyResource> kv : policy.getResources().entrySet()) {
            String                            resourceName   = kv.getKey();
            RangerPolicy.RangerPolicyResource policyResource = kv.getValue();
            List<String>                      resourceValues = policyResource.getValues();

            if(CollectionUtils.isNotEmpty(resourceValues)) {
	            for (RangerServiceDef.RangerResourceDef resourceDef : resourceDefs) {
	                if (resourceName.equals(resourceDef.getName())) {
		                tmpList.add(new LevelResourceNames(resourceDef.getLevel(), policyResource));
	                    break;
	                }
	            }
            }
        }
        Collections.sort(tmpList); // Sort in ascending order of levels

        int resourceDiscount = 0;
        for (LevelResourceNames item : tmpList) {
            // Expect lowest level first
            boolean foundStarWildcard     = false;
            boolean foundQuestionWildcard = false;
            boolean foundMatchAny         = false;

            for (String resourceName : item.policyResource.getValues()) {
                if (resourceName.isEmpty() || RANGER_POLICY_EVAL_MATCH_ANY_PATTERN_STRING.equals(resourceName)) {
                    foundMatchAny = true;
                    break;
                } else if (resourceName.contains(RANGER_POLICY_EVAL_MATCH_ANY_PATTERN_STRING)) {
                    foundStarWildcard = true;
                } else if (resourceName.contains(RANGER_POLICY_EVAL_MATCH_ONE_CHARACTER_STRING)) {
                    foundQuestionWildcard = true;
                }
            }
            if (foundMatchAny) {
                resourceDiscount += RANGER_POLICY_EVAL_SCORE_RESOURCE_DISCOUNT_MATCH_ANY_WILDCARD;
            } else {
                if (foundStarWildcard) {
                    resourceDiscount += RANGER_POLICY_EVAL_SCORE_RESOURCE_DISCOUNT_HAS_MATCH_ANY_WILDCARD;
                } else if (foundQuestionWildcard) {
                    resourceDiscount += RANGER_POLICY_EVAL_SCORE_RESOURCE_DISCOUNT_HAS_MATCH_ONE_CHARACTER_WILDCARD;
                }

                RangerPolicy.RangerPolicyResource resource = item.policyResource;

                if (resource.getIsExcludes()) {
                    resourceDiscount += RANGER_POLICY_EVAL_SCORE_RESOURCE_DISCOUNT_IS_EXCLUDES;
                }

                if (resource.getIsRecursive()) {
                    resourceDiscount += RANGER_POLICY_EVAL_SCORE_RESORUCE_DISCOUNT_IS_RECURSIVE;
                }
            }
        }
        if (needsDynamicEval()) {
            evalOrder += RANGER_POLICY_EVAL_SCORE_DYNAMIC_RESOURCE_EVAL_PENALTY;
        }

        evalOrder -= Math.min(RANGER_POLICY_EVAL_SCORE_MAX_DISCOUNT_RESOURCE, resourceDiscount);

        if (hasPublicGroup || hasCurrentUser) {
            evalOrder -= RANGER_POLICY_EVAL_SCORE_MAX_DISCOUNT_USERSGROUPS;
        } else {
            evalOrder -= Math.min(groups.size() + users.size(), RANGER_POLICY_EVAL_SCORE_MAX_DISCOUNT_USERSGROUPS);
        }

        evalOrder -= Math.round(((float)RANGER_POLICY_EVAL_SCORE_MAX_DISCOUNT_ACCESS_TYPES * accessPerms.size()) / serviceDef.getAccessTypes().size());

        int customConditionsDiscount = RANGER_POLICY_EVAL_SCORE_MAX_DISCOUNT_CUSTOM_CONDITIONS - (RANGER_POLICY_EVAL_SCORE_CUSTOM_CONDITION_PENALTY * this.getCustomConditionsCount());
        if(customConditionsDiscount > 0) {
            evalOrder -= customConditionsDiscount;
        }

        if(LOG.isDebugEnabled()) {
            LOG.debug("<== RangerOptimizedPolicyEvaluator.computeEvalOrder(), policyName:" + policy.getName() + ", priority:" + evalOrder);
        }

        return evalOrder;
    }

    @Override
    protected boolean isAccessAllowed(String user, Set<String> userGroups, Set<String> roles, String owner, String accessType) {
        if(LOG.isDebugEnabled()) {
            LOG.debug("==> RangerOptimizedPolicyEvaluator.isAccessAllowed(" + user + ", " + userGroups + ", " + roles + ", " + owner + ", " + accessType + ")");
        }

        boolean ret = hasMatchablePolicyItem(user, userGroups, roles, owner, accessType) && super.isAccessAllowed(user, userGroups, roles, owner, accessType);

        if(LOG.isDebugEnabled()) {
            LOG.debug("<== RangerOptimizedPolicyEvaluator.isAccessAllowed(" + user + ", " + userGroups + ", " + roles + ", " + owner + ", " + accessType + "): " + ret);
        }

        return ret;
    }

    @Override
    protected boolean hasMatchablePolicyItem(RangerAccessRequest request) {
        boolean ret = false;

        if (checkIfAllEvaluatorsInitialized()) {
            if (hasPublicGroup || hasCurrentUser || isOwnerMatch(request) || users.contains(request.getUser()) || CollectionUtils.containsAny(groups, request.getUserGroups()) || (CollectionUtils.isNotEmpty(roles) && CollectionUtils.containsAny(roles, RangerAccessRequestUtil.getCurrentUserRolesFromContext(request.getContext())))) {
                if (hasAllPerms || request.isAccessTypeAny()) {
                    ret = true;
                } else {
                    ret = accessPerms.contains(request.getAccessType());

                    if (!ret) {
                        Set<String> allRequestedAccesses = RangerAccessRequestUtil.getAllRequestedAccessTypes(request);
                        ret = CollectionUtils.containsAny(accessPerms, allRequestedAccesses);
                    }

                    if (!ret) {
                        if (request.isAccessTypeDelegatedAdmin()) {
                            ret = delegateAdmin;
                        }
                    }
                }
            }
        } else {
            ret = true;
        }

        return ret;
    }

    private boolean isOwnerMatch(RangerAccessRequest request) {
        boolean ret = false;

        if (hasResourceOwner) {
            RangerAccessResource accessedResource = request.getResource();
            String resourceOwner = accessedResource != null ? accessedResource.getOwnerUser() : null;
            String user = request.getUser();

            if (user != null && resourceOwner != null && user.equals(resourceOwner)) {
                ret = true;
            }
        }

        return ret;
    }

    private boolean hasMatchablePolicyItem(String user, Set<String> userGroups, Set<String> rolesFromContext, String owner, String accessType) {
        boolean ret = false;

        if (checkIfAllEvaluatorsInitialized()) {
            boolean hasRole = false;
            if (CollectionUtils.isNotEmpty(roles)) {
                if (CollectionUtils.isNotEmpty(rolesFromContext)) {
                    hasRole = CollectionUtils.containsAny(roles, rolesFromContext);
                }
            }

            if (hasPublicGroup || hasCurrentUser || users.contains(user) || CollectionUtils.containsAny(groups, userGroups) || hasRole || (hasResourceOwner && StringUtils.equals(user, owner))) {
                if (hasAllPerms) {
                    ret = true;
                } else {
                    boolean isAccessTypeAny = StringUtils.isEmpty(accessType) || StringUtils.equals(accessType, RangerPolicyEngine.ANY_ACCESS);
                    ret = isAccessTypeAny || accessPerms.contains(accessType);

                    if (!ret) {
                        if (StringUtils.equals(accessType, RangerPolicyEngine.ADMIN_ACCESS)) {
                            ret = delegateAdmin;
                        }
                    }
                }
            }
        } else {
            ret = true;
        }

        return ret;
    }

    private static Set<String> addAll(Set<String> coll, Collection<String> toAdd) {
        if (CollectionUtils.isNotEmpty(toAdd)) {
            if (CollectionUtils.isEmpty(coll)) {
                coll = new HashSet<>(toAdd);
            } else {
                coll.addAll(toAdd);
            }
        }

        return coll;
    }

    private static Set<String> add(Set<String> coll, String value) {
        if (CollectionUtils.isEmpty(coll)) {
            coll = new HashSet<>();
        }

        coll.add(value);

        return coll;
    }

    private void preprocessPolicyItems(List<? extends RangerPolicy.RangerPolicyItem> policyItems) {
        if(CollectionUtils.isNotEmpty(policyItems)) {
	        for (RangerPolicy.RangerPolicyItem item : policyItems) {
	            delegateAdmin = delegateAdmin || item.getDelegateAdmin();

	            List<RangerPolicy.RangerPolicyItemAccess> policyItemAccesses = item.getAccesses();
	            for(RangerPolicy.RangerPolicyItemAccess policyItemAccess : policyItemAccesses) {

	                if (policyItemAccess.getIsAllowed()) {
                        accessPerms = add(accessPerms, policyItemAccess.getType());
	                }
	            }

                roles = addAll(roles, item.getRoles());
                groups = addAll(groups, item.getGroups());
                users = addAll(users, item.getUsers());
	        }
        }
    }

	private boolean checkIfHasAllPerms() {
        if(LOG.isDebugEnabled()) {
            LOG.debug("==> RangerOptimizedPolicyEvaluator.checkIfHasAllPerms()");
        }
        boolean result = true;

        if (getPolicy().getIsDenyAllElse()) {
            hasAllPerms = true;
        } else {
            List<RangerServiceDef.RangerAccessTypeDef> serviceAccessTypes = getServiceDef().getAccessTypes();
            for (RangerServiceDef.RangerAccessTypeDef serviceAccessType : serviceAccessTypes) {
                if (!accessPerms.contains(serviceAccessType.getName())) {
                    result = false;
                    break;
                }
            }
        }

        if(LOG.isDebugEnabled()) {
            LOG.debug("==> RangerOptimizedPolicyEvaluator.checkIfHasAllPerms(), " + result);
        }

        return result;
    }

    private boolean checkIfAllEvaluatorsInitialized() {
        if (!hasAllEvaluatorsInitialized) {
            hasAllEvaluatorsInitialized = checkIfWithImpliedGrantsInitialized (getAllowEvaluators()) &&
                    checkIfWithImpliedGrantsInitialized(getAllowExceptionEvaluators()) &&
                    checkIfWithImpliedGrantsInitialized(getDenyEvaluators()) &&
                    checkIfWithImpliedGrantsInitialized(getDenyExceptionEvaluators()) &&
                    checkIfWithImpliedGrantsInitialized(getDataMaskEvaluators()) &&
                    checkIfWithImpliedGrantsInitialized(getRowFilterEvaluators());
        }
        return hasAllEvaluatorsInitialized;
    }

    private boolean checkIfWithImpliedGrantsInitialized(List<? extends RangerPolicyItemEvaluator> evaluators) {
        boolean ret = true;
        for (RangerPolicyItemEvaluator evaluator: evaluators) {
            if (evaluator.getWithImpliedGrants() == null) {
                ret = false;
                break;
            } else {
                for (RangerPolicy.RangerPolicyItemAccess access : evaluator.getWithImpliedGrants().getAccesses()) {
                    if (access.getIsAllowed()) {
                        if (CollectionUtils.isEmpty(accessPerms)) {
                            accessPerms = new HashSet<>();
                        }

                        accessPerms.add(access.getType());
                    }
                }
            }
        }
        return ret;
    }

}
