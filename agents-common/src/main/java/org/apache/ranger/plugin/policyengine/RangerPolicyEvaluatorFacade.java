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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ranger.plugin.conditionevaluator.RangerConditionEvaluator;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.policyevaluator.RangerDefaultPolicyEvaluator;
import org.apache.ranger.plugin.policyevaluator.RangerOptimizedPolicyEvaluator;
import org.apache.ranger.plugin.policyevaluator.RangerPolicyEvaluator;

import java.util.Map;

public class RangerPolicyEvaluatorFacade implements RangerPolicyEvaluator, Comparable<RangerPolicyEvaluatorFacade> {
    private static final Log LOG = LogFactory.getLog(RangerPolicyEvaluatorFacade.class);

    RangerDefaultPolicyEvaluator delegate  =   null;
    int computedPolicyEvalOrder            =   0;
    boolean useCachePolicyEngine         = false;

    RangerPolicyEvaluatorFacade(boolean useCachePolicyEngine) {
        super();
        this.useCachePolicyEngine = useCachePolicyEngine;
        delegate = new RangerOptimizedPolicyEvaluator();
    }

    RangerPolicyEvaluator getPolicyEvaluator() {
        return delegate;
    }

    @Override
    public void init(RangerPolicy policy, RangerServiceDef serviceDef) {
        if(LOG.isDebugEnabled()) {
            LOG.debug("==> RangerPolicyEvaluatorFacade.init(), useCachePolicyEngine:" + useCachePolicyEngine);
        }
        delegate.init(policy, serviceDef);
        computedPolicyEvalOrder = computePolicyEvalOrder();
        if(LOG.isDebugEnabled()) {
            LOG.debug("<== RangerPolicyEvaluatorFacade.init(), useCachePolicyEngine:" + useCachePolicyEngine);
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
    public boolean isMatch(RangerResource resource) {
        return false;
    }

    @Override
    public boolean isSingleAndExactMatch(RangerResource resource) {
        return false;
    }

    @Override
    public int compareTo(RangerPolicyEvaluatorFacade other) {
        if(LOG.isDebugEnabled()) {
            LOG.debug("==> RangerPolicyEvaluatorFacade.compareTo()");
        }
        int result;

        if (this.getComputedPolicyEvalOrder() == other.getComputedPolicyEvalOrder()) {
            Map<String, RangerConditionEvaluator> myConditionEvaluators = this.delegate.getConditionEvaluators();
            Map<String, RangerConditionEvaluator> otherConditionEvaluators = other.delegate.getConditionEvaluators();

            int myConditionEvaluatorCount = myConditionEvaluators == null ? 0 : myConditionEvaluators.size();
            int otherConditionEvaluatorCount = otherConditionEvaluators == null ? 0 : otherConditionEvaluators.size();

            result = Integer.compare(myConditionEvaluatorCount, otherConditionEvaluatorCount);
        } else {
            int myComputedPriority = this.getComputedPolicyEvalOrder();
            int otherComputedPriority = other.getComputedPolicyEvalOrder();
            result = Integer.compare(myComputedPriority, otherComputedPriority);
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
