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

package org.apache.ranger.plugin.policyengine;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ranger.authorization.hadoop.config.RangerConfiguration;
import org.apache.ranger.plugin.conditionevaluator.RangerConditionEvaluator;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyResource;
import org.apache.ranger.plugin.policyevaluator.RangerCachedPolicyEvaluator;
import org.apache.ranger.plugin.policyevaluator.RangerDefaultPolicyEvaluator;
import org.apache.ranger.plugin.policyevaluator.RangerOptimizedPolicyEvaluator;
import org.apache.ranger.plugin.policyevaluator.RangerPolicyEvaluator;

import java.util.Map;
import java.util.Set;

public class RangerPolicyEvaluatorFacade implements RangerPolicyEvaluator, Comparable<RangerPolicyEvaluatorFacade> {
    private static final Log LOG = LogFactory.getLog(RangerPolicyEvaluatorFacade.class);

    RangerDefaultPolicyEvaluator delegate = null;
    int computedPolicyEvalOrder           = 0;

    RangerPolicyEvaluatorFacade() {
        super();

        String evaluatorType = RangerConfiguration.getInstance().get("ranger.policyengine.evaluator.type", "cached");

        if(StringUtils.isEmpty(evaluatorType) || StringUtils.equalsIgnoreCase(evaluatorType, "cached")) {
            delegate = new RangerCachedPolicyEvaluator();
        } else {
            delegate = new RangerOptimizedPolicyEvaluator();
        }
    }

    RangerPolicyEvaluator getPolicyEvaluator() {
        return delegate;
    }

    @Override
    public void init(RangerPolicy policy, RangerServiceDef serviceDef) {
        if(LOG.isDebugEnabled()) {
            LOG.debug("==> RangerPolicyEvaluatorFacade.init()");
        }

        delegate.init(policy, serviceDef);

        computedPolicyEvalOrder = computePolicyEvalOrder();

        if(LOG.isDebugEnabled()) {
            LOG.debug("<== RangerPolicyEvaluatorFacade.init()");
        }
    }

    @Override
    public RangerPolicy getPolicy() {
        return delegate.getPolicy();
    }

    @Override
    public RangerServiceDef getServiceDef() {
        return delegate.getServiceDef();
    }

    @Override
    public void evaluate(RangerAccessRequest request, RangerAccessResult result) {
        delegate.evaluate(request, result);
    }

    @Override
    public boolean isMatch(RangerAccessResource resource) {
        return delegate.isMatch(resource);
    }

    @Override
    public boolean isSingleAndExactMatch(RangerAccessResource resource) {
        return delegate.isSingleAndExactMatch(resource);
    }

    @Override
    public boolean isAccessAllowed(Map<String, RangerPolicyResource> resources, String user, Set<String> userGroups, String accessType) {
        return delegate.isAccessAllowed(resources, user, userGroups, accessType);
    }

    @Override
    public int compareTo(RangerPolicyEvaluatorFacade other) {
        if(LOG.isDebugEnabled()) {
            LOG.debug("==> RangerPolicyEvaluatorFacade.compareTo()");
        }

        int result;

        if (this.getComputedPolicyEvalOrder() == other.getComputedPolicyEvalOrder()) {
            Map<String, RangerConditionEvaluator> myConditionEvaluators    = this.delegate.getConditionEvaluators();
            Map<String, RangerConditionEvaluator> otherConditionEvaluators = other.delegate.getConditionEvaluators();

            int myConditionEvaluatorCount    = myConditionEvaluators == null ? 0 : myConditionEvaluators.size();
            int otherConditionEvaluatorCount = otherConditionEvaluators == null ? 0 : otherConditionEvaluators.size();

            result = Integer.compare(myConditionEvaluatorCount, otherConditionEvaluatorCount);
        } else {
            result = Integer.compare(computedPolicyEvalOrder, other.computedPolicyEvalOrder);
        }

        if(LOG.isDebugEnabled()) {
            LOG.debug("<== RangerPolicyEvaluatorFacade.compareTo(), result:" + result);
        }

        return result;
    }

    private int getComputedPolicyEvalOrder() {
        return computedPolicyEvalOrder;
    }

    private int computePolicyEvalOrder() {
        if(LOG.isDebugEnabled()) {
            LOG.debug("==> RangerPolicyEvaluatorFacade.computePolicyEvalOrder()");
        }

        int result = delegate.computePolicyEvalOrder();

        if(LOG.isDebugEnabled()) {
            LOG.debug("<==RangerPolicyEvaluatorFacade.computePolicyEvalOrder(), result:" + result);
        }

        return result;
    }
}
