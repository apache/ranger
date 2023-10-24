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
import org.apache.ranger.plugin.conditionevaluator.RangerConditionEvaluator;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItemCondition;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItem;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.model.RangerServiceDef.RangerPolicyConditionDef;
import org.apache.ranger.plugin.policyengine.RangerPolicyEngineOptions;
import org.apache.ranger.plugin.util.RangerPerfTracer;
import org.apache.ranger.plugin.util.ServiceDefUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

//
// this class should have been named RangerConditionEvaluatorFactory
//
public class RangerCustomConditionEvaluator {
    private static final Logger LOG                           = LoggerFactory.getLogger(RangerCustomConditionEvaluator.class);
    private static final Logger PERF_POLICY_INIT_LOG          = RangerPerfTracer.getPerfLogger("policy.init");
    private static final Logger PERF_POLICYITEM_INIT_LOG      = RangerPerfTracer.getPerfLogger("policyitem.init");
    private static final Logger PERF_POLICYCONDITION_INIT_LOG = RangerPerfTracer.getPerfLogger("policycondition.init");


    public static RangerCustomConditionEvaluator getInstance() {
        return RangerCustomConditionEvaluator.SingletonHolder.s_instance;
    }

    private RangerCustomConditionEvaluator() {
    }

    public List<RangerConditionEvaluator> getPolicyConditionEvaluators(RangerPolicy policy, RangerServiceDef serviceDef, RangerPolicyEngineOptions options) {
        RangerPerfTracer perf     = null;
        String           parentId = "policyId=" + policy.getId() ;

        if (RangerPerfTracer.isPerfTraceEnabled(PERF_POLICY_INIT_LOG)) {
            perf = RangerPerfTracer.getPerfTracer(PERF_POLICY_INIT_LOG, "RangerPolicyEvaluator.getPolicyConditionEvaluators(" + parentId + ")");
        }

        List<RangerConditionEvaluator> ret = getConditionEvaluators(parentId, policy.getConditions(), serviceDef, options);

        RangerPerfTracer.log(perf);

        return ret;
    }

    public List<RangerConditionEvaluator> getPolicyItemConditionEvaluators(RangerPolicy policy, RangerPolicyItem policyItem, RangerServiceDef serviceDef, RangerPolicyEngineOptions options, int policyItemIndex) {
        RangerPerfTracer perf     = null;
        String           parentId = "policyId=" + policy.getId() + ", policyItemIndex=" + policyItemIndex;

        if (RangerPerfTracer.isPerfTraceEnabled(PERF_POLICYITEM_INIT_LOG)) {
            perf = RangerPerfTracer.getPerfTracer(PERF_POLICYITEM_INIT_LOG, "RangerPolicyItemEvaluator.getPolicyItemConditionEvaluators(" + parentId + ")");
        }

        List<RangerConditionEvaluator> ret = getConditionEvaluators(parentId, policyItem.getConditions(), serviceDef, options);

        RangerPerfTracer.log(perf);

        return ret;
    }

    public List<RangerConditionEvaluator> getConditionEvaluators(String parentId, List<RangerPolicyItemCondition> conditions, RangerServiceDef serviceDef, RangerPolicyEngineOptions options) {
        final List<RangerConditionEvaluator> ret;

        if (!getConditionsDisabledOption(options) && CollectionUtils.isNotEmpty(conditions)) {
            ret = new ArrayList<>(conditions.size());

            for (RangerPolicyItemCondition condition : conditions) {
                RangerPolicyConditionDef conditionDef = ServiceDefUtil.getConditionDef(serviceDef, condition.getType());

                if (conditionDef == null) {
                    LOG.error("RangerCustomConditionEvaluator.getConditionEvaluators(" + parentId + "): conditionDef '" + condition.getType() + "' not found. Ignoring the condition");

                    continue;
                }

                RangerConditionEvaluator conditionEvaluator = getConditionEvaluator(parentId, condition, conditionDef, serviceDef, options);

                if (conditionEvaluator != null) {
                    ret.add(conditionEvaluator);
                }
            }
        } else {
            ret = Collections.emptyList();
        }

        return  ret;
    }

    public RangerConditionEvaluator getConditionEvaluator(String parentId, RangerPolicyItemCondition condition, RangerPolicyConditionDef conditionDef, RangerServiceDef serviceDef, RangerPolicyEngineOptions options) {
        final RangerConditionEvaluator ret;

        if (condition != null && conditionDef != null && !getConditionsDisabledOption(options)) {
            ret = newConditionEvaluator(conditionDef.getEvaluator());

            if (ret != null) {
                ret.setServiceDef(serviceDef);
                ret.setConditionDef(conditionDef);
                ret.setPolicyItemCondition(condition);

                RangerPerfTracer perf = null;

                if (RangerPerfTracer.isPerfTraceEnabled(PERF_POLICYCONDITION_INIT_LOG)) {
                    perf = RangerPerfTracer.getPerfTracer(PERF_POLICYCONDITION_INIT_LOG, "RangerConditionEvaluator.init(" + parentId + ", policyConditionType=" + condition.getType() + ")");
                }

                ret.init();

                RangerPerfTracer.log(perf);
            } else {
                LOG.error("RangerCustomConditionEvaluator.getConditionEvaluator(" + parentId + "): failed to init ConditionEvaluator '" + condition.getType() + "'; evaluatorClassName='" + conditionDef.getEvaluator() + "'");
            }
        } else {
            ret = null;
        }

        return ret;
    }

    private RangerConditionEvaluator newConditionEvaluator(String className) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> RangerCustomConditionEvaluator.newConditionEvaluator(" + className + ")");
        }

        RangerConditionEvaluator evaluator = null;

        try {
            @SuppressWarnings("unchecked")
            Class<RangerConditionEvaluator> evaluatorClass = (Class<RangerConditionEvaluator>)Class.forName(className);

            evaluator = evaluatorClass.newInstance();
        } catch (Throwable t) {
            LOG.error("RangerCustomConditionEvaluator.newConditionEvaluator(" + className + "): error instantiating evaluator", t);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== RangerCustomConditionEvaluator.newConditionEvaluator(" + className + "): " + evaluator);
        }

        return evaluator;
    }

    private boolean getConditionsDisabledOption(RangerPolicyEngineOptions options) {
        return options != null && options.disableCustomConditions;
    }

    private static class SingletonHolder {
        private static final RangerCustomConditionEvaluator s_instance = new RangerCustomConditionEvaluator();
    }
}
