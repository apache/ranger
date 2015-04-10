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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.policyengine.RangerAccessRequest;
import org.apache.ranger.plugin.policyengine.RangerAccessResult;
import org.apache.ranger.plugin.policyengine.RangerPolicyEngine;
import org.apache.ranger.plugin.policyengine.RangerPolicyEngineOptions;

import java.util.*;
import java.lang.Math;

public class RangerOptimizedPolicyEvaluator extends RangerDefaultPolicyEvaluator {
    private static final Log LOG = LogFactory.getLog(RangerOptimizedPolicyEvaluator.class);

    private Set<String> groups         = null;
    private Set<String> users          = null;
    private Set<String> accessPerms    = null;
    private boolean     delegateAdmin  = false;
    private boolean     hasAllPerms    = false;
    private boolean     hasPublicGroup = false;


    // For computation of priority
    private static final String RANGER_POLICY_EVAL_MATCH_ANY_PATTERN_STRING                   = "*";
    private static final String RANGER_POLICY_EVAL_MATCH_ONE_CHARACTER_STRING                 = "?";
    private static final int RANGER_POLICY_EVAL_MATCH_ANY_WILDCARD_PREMIUM                    = 25;
    private static final int RANGER_POLICY_EVAL_CONTAINS_MATCH_ANY_WILDCARD_PREMIUM           = 10;
    private static final int RANGER_POLICY_EVAL_CONTAINS_MATCH_ONE_CHARACTER_WILDCARD_PREMIUM = 10;
    private static final int RANGER_POLICY_EVAL_HAS_EXCLUDES_PREMIUM                          = 25;
    private static final int RANGER_POLICY_EVAL_IS_RECURSIVE_PREMIUM                          = 25;
    private static final int RANGER_POLICY_EVAL_PUBLIC_GROUP_ACCESS_PREMIUM                   = 25;
    private static final int RANGER_POLICY_EVAL_ALL_ACCESS_TYPES_PREMIUM                      = 25;
    private static final int RANGER_POLICY_EVAL_RESERVED_SLOTS_NUMBER                         = 10000;
    private static final int RANGER_POLICY_EVAL_RESERVED_SLOTS_PER_LEVEL_NUMBER               = 1000;

    @Override
    public void init(RangerPolicy policy, RangerServiceDef serviceDef, RangerPolicyEngineOptions options) {
        if(LOG.isDebugEnabled()) {
            LOG.debug("==> RangerOptimizedPolicyEvaluator.init()");
        }

        super.init(policy, serviceDef, options);

        accessPerms = new HashSet<String>();
        groups = new HashSet<String>();
        users = new HashSet<String>();

        for (RangerPolicy.RangerPolicyItem item : policy.getPolicyItems()) {
            delegateAdmin = delegateAdmin || item.getDelegateAdmin();

            List<RangerPolicy.RangerPolicyItemAccess> policyItemAccesses = item.getAccesses();
            for(RangerPolicy.RangerPolicyItemAccess policyItemAccess : policyItemAccesses) {

                if (policyItemAccess.getIsAllowed()) {
                    String accessType = policyItemAccess.getType();
                    accessPerms.add(accessType);
                }
            }

            groups.addAll(item.getGroups());
            users.addAll(item.getUsers());
        }

        hasAllPerms = checkIfHasAllPerms();

        for (String group : groups) {
            if (group.equalsIgnoreCase(RangerPolicyEngine.GROUP_PUBLIC)) {
                hasPublicGroup = true;
            }
        }

        setEvalOrder(computeEvalOrder());

        if(LOG.isDebugEnabled()) {
            LOG.debug("<== RangerOptimizedPolicyEvaluator.init()");
        }
    }

    public int computeEvalOrder() {
        if(LOG.isDebugEnabled()) {
            LOG.debug("==> RangerOptimizedPolicyEvaluator.computeEvalOrder()");
        }
        RangerServiceDef serviceDef = getServiceDef();
        RangerPolicy policy = getPolicy();

        class LevelResourceNames implements Comparable<LevelResourceNames> {
            int level;
            RangerPolicy.RangerPolicyResource policyResource;

            @Override
            public int compareTo(LevelResourceNames other) {
                // Sort in ascending order of level numbers
                return Integer.compare(this.level, other.level);
            }
        }
        List<LevelResourceNames> tmpList = new ArrayList<LevelResourceNames>();

        List<RangerServiceDef.RangerResourceDef> resourceDefs = serviceDef.getResources();

        for (Map.Entry<String, RangerPolicy.RangerPolicyResource> keyValuePair : policy.getResources().entrySet()) {
            String serviceDefResourceName = keyValuePair.getKey();
            RangerPolicy.RangerPolicyResource policyResource = keyValuePair.getValue();
            List<String> policyResourceNames = policyResource.getValues();

            RangerServiceDef.RangerResourceDef found = null;
            for (RangerServiceDef.RangerResourceDef resourceDef : resourceDefs) {
                if (serviceDefResourceName.equals(resourceDef.getName())) {
                    found = resourceDef;
                    break;
                }
            }
            if (found != null) {
                int level = found.getLevel();
                if (policyResourceNames != null) {
                    LevelResourceNames item = new LevelResourceNames();
                    item.level = level;
                    item.policyResource = policyResource;
                    tmpList.add(item);
                }

            }

        }
        Collections.sort(tmpList); // Sort in ascending order of levels

        CharSequence matchesAnySeq = RANGER_POLICY_EVAL_MATCH_ANY_PATTERN_STRING.subSequence(0, 1);
        CharSequence matchesSingleCharacterSeq = RANGER_POLICY_EVAL_MATCH_ONE_CHARACTER_STRING.subSequence(0, 1);

        int priorityLevel = RANGER_POLICY_EVAL_RESERVED_SLOTS_NUMBER;
        boolean seenFirstMatchAny = false;

        for (LevelResourceNames item : tmpList) {
            // Expect lowest level first
            List<String> resourceNames = item.policyResource.getValues();
            boolean foundStarWildcard = false;
            boolean foundQuestionWildcard = false;
            boolean foundMatchAny = false;

            for (String resourceName : resourceNames) {
                if (resourceName.isEmpty() ||resourceName.equals(RANGER_POLICY_EVAL_MATCH_ANY_PATTERN_STRING)) {
                    foundMatchAny = true;
                    break;
                }
                if (resourceName.contains(matchesAnySeq))
                    foundStarWildcard = true;
                else if (resourceName.contains(matchesSingleCharacterSeq))
                    foundQuestionWildcard = true;
            }
            if (foundMatchAny) {
                if (seenFirstMatchAny)
                    priorityLevel -= RANGER_POLICY_EVAL_MATCH_ANY_WILDCARD_PREMIUM;
                else {
                    seenFirstMatchAny = true;
                }
            } else {
                priorityLevel +=  RANGER_POLICY_EVAL_RESERVED_SLOTS_PER_LEVEL_NUMBER;
                if (foundStarWildcard) priorityLevel -= RANGER_POLICY_EVAL_CONTAINS_MATCH_ANY_WILDCARD_PREMIUM;
                else if (foundQuestionWildcard) priorityLevel -= RANGER_POLICY_EVAL_CONTAINS_MATCH_ONE_CHARACTER_WILDCARD_PREMIUM;

                RangerPolicy.RangerPolicyResource resource = item.policyResource;
                if (resource.getIsExcludes()) priorityLevel -= RANGER_POLICY_EVAL_HAS_EXCLUDES_PREMIUM;
                if (resource.getIsRecursive()) priorityLevel -= RANGER_POLICY_EVAL_IS_RECURSIVE_PREMIUM;
            }
        }

        if (hasPublicGroup) {
            priorityLevel -= RANGER_POLICY_EVAL_PUBLIC_GROUP_ACCESS_PREMIUM;
        } else {
            priorityLevel -= groups.size();
        }
        priorityLevel -= users.size();

        priorityLevel -= Math.round(((float)RANGER_POLICY_EVAL_ALL_ACCESS_TYPES_PREMIUM * accessPerms.size()) / serviceDef.getAccessTypes().size());

        if(LOG.isDebugEnabled()) {
            LOG.debug("<== RangerOptimizedPolicyEvaluator.computeEvalOrder(), policyName:" + policy.getName() + ", priority:" + priorityLevel);
        }
        return priorityLevel;
    }

	@Override
	protected boolean isAccessAllowed(String user, Set<String> userGroups, String accessType) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerOptimizedPolicyEvaluator.isAccessAllowed(" + user + ", " + userGroups + ", " + accessType + ")");
		}

		boolean ret = false;

		if (hasPublicGroup || users.contains(user) || CollectionUtils.containsAny(groups, userGroups)) {
			if (StringUtils.isEmpty(accessType)) {
				accessType = RangerPolicyEngine.ANY_ACCESS;
			}

			boolean isAnyAccess   = StringUtils.equals(accessType, RangerPolicyEngine.ANY_ACCESS);
			boolean isAdminAccess = StringUtils.equals(accessType, RangerPolicyEngine.ADMIN_ACCESS);

            if (isAnyAccess || (isAdminAccess && delegateAdmin) || hasAllPerms || accessPerms.contains(accessType)) {
                ret = super.isAccessAllowed(user, userGroups, accessType);
            }
        }

        if(LOG.isDebugEnabled()) {
            LOG.debug("<== RangerOptimizedPolicyEvaluator.isAccessAllowed(" + user + ", " + userGroups + ", " + accessType + "): " + ret);
        }

		return ret;
	}

	@Override
    protected void evaluatePolicyItemsForAccess(RangerPolicy policy, RangerAccessRequest request, RangerAccessResult result) {
        if(LOG.isDebugEnabled()) {
            LOG.debug("==> RangerOptimizedPolicyEvaluator.evaluatePolicyItemsForAccess()");
        }

        if (hasPublicGroup || users.contains(request.getUser()) || CollectionUtils.containsAny(groups, request.getUserGroups())) {
            // No need to reject based on users and groups

            if (request.isAccessTypeAny() || (request.isAccessTypeDelegatedAdmin() && delegateAdmin) || hasAllPerms || accessPerms.contains(request.getAccessType())) {
                // No need to reject based on aggregated access permissions
                super.evaluatePolicyItemsForAccess(policy, request, result);
            }
        }
        if(LOG.isDebugEnabled()) {
            LOG.debug("<== RangerOptimizedPolicyEvaluator.evaluatePolicyItemsForAccess()");
        }

    }
    private boolean checkIfHasAllPerms() {
        if(LOG.isDebugEnabled()) {
            LOG.debug("==> RangerOptimizedPolicyEvaluator.checkIfHasAllPerms()");
        }
        boolean result = true;

        List<RangerServiceDef.RangerAccessTypeDef> serviceAccessTypes = getServiceDef().getAccessTypes();
        for (RangerServiceDef.RangerAccessTypeDef serviceAccessType : serviceAccessTypes) {
            if(! accessPerms.contains(serviceAccessType.getName())) {
		result = false;
                break;
            }
        }

        if(LOG.isDebugEnabled()) {
            LOG.debug("==> RangerOptimizedPolicyEvaluator.checkIfHasAllPerms(), " + result);
        }

        return result;
    }

}
