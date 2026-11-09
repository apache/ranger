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

import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerPolicy.RangerConstraintPolicyItem;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItemConstraintInfo;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.policyengine.RangerAccessResult;
import org.apache.ranger.plugin.policyengine.RangerConstraintResult;
import org.apache.ranger.plugin.policyengine.RangerPolicyEngineOptions;
import org.apache.ranger.plugin.policyresourcematcher.RangerPolicyResourceMatcher;

public class RangerDefaultConstraintPolicyItemEvaluator extends RangerDefaultPolicyItemEvaluator implements RangerConstraintPolicyItemEvaluator {
    private final RangerConstraintPolicyItem constraintPolicyItem;

    public RangerDefaultConstraintPolicyItemEvaluator(RangerServiceDef serviceDef, RangerPolicy policy, RangerConstraintPolicyItem policyItem, int policyItemIndex, RangerPolicyEngineOptions options) {
        super(serviceDef, policy, policyItem, RangerPolicyItemEvaluator.POLICY_ITEM_TYPE_CONSTRAINT, policyItemIndex, options);

        constraintPolicyItem = policyItem;
    }

    @Override
    public void updateAccessResult(RangerPolicyEvaluator policyEvaluator, RangerAccessResult result, RangerPolicyResourceMatcher.MatchType matchType) {
        // Access allow/deny is already decided. A constraint item must not change it.
    }

    @Override
    public void collectConstraint(RangerAccessResult result, RangerPolicyResourceMatcher.MatchType matchType) {
        if (result == null || constraintPolicyItem == null) {
            return;
        }

        RangerPolicyItemConstraintInfo constraintInfo = constraintPolicyItem.getConstraintInfo();

        if (constraintInfo == null) {
            return;
        }

        int policyPriority = policy == null || policy.getPolicyPriority() == null ? RangerPolicy.POLICY_PRIORITY_NORMAL : policy.getPolicyPriority();

        result.addConstraint(new RangerConstraintResult(constraintInfo.getConstraintName(), constraintInfo.getParameters(), policyId, policy == null ? null : policy.getVersion(), policyPriority, matchType));
    }
}
