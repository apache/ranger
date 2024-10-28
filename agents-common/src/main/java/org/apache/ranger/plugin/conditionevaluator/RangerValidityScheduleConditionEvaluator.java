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

package org.apache.ranger.plugin.conditionevaluator;

import org.apache.ranger.authorization.utils.JsonUtils;
import org.apache.ranger.plugin.model.RangerValiditySchedule;
import org.apache.ranger.plugin.policyengine.RangerAccessRequest;
import org.apache.ranger.plugin.policyevaluator.RangerValidityScheduleEvaluator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;


public class RangerValidityScheduleConditionEvaluator extends RangerAbstractConditionEvaluator {
    private static final Logger LOG = LoggerFactory.getLogger(RangerValidityScheduleConditionEvaluator.class);

    private List<RangerValidityScheduleEvaluator> evaluators = Collections.emptyList();

    @Override
    public void init() {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> RangerValidityScheduleConditionEvaluator.init({})", condition);
        }

        super.init();

        if (condition != null && condition.getValues() != null && !condition.getValues().isEmpty()) {
            evaluators = new ArrayList<>(condition.getValues().size());

            for (String scheduleStr : condition.getValues()) {
                try {
                    RangerValiditySchedule schedule = JsonUtils.jsonToObject(scheduleStr, RangerValiditySchedule.class);

                    if (schedule != null) {
                        evaluators.add(new RangerValidityScheduleEvaluator(schedule));
                    }
                } catch (Exception excp) {
                    LOG.error("RangerValidityScheduleConditionEvaluator.init({}): failed to initialize schedule {}", condition, scheduleStr, excp);
                }

            }
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== RangerValidityScheduleConditionEvaluator.init({}): evaluator={}", condition, evaluators);
        }
    }

    @Override
    public boolean isMatched(RangerAccessRequest request) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> RangerValidityScheduleConditionEvaluator.isMatched({})", request);
        }

        final boolean ret;

        if (evaluators.isEmpty() || request.getAccessTime() == null) {
            ret = true;
        } else {
            final long accessTime = request.getAccessTime().getTime();

            ret = evaluators.stream().filter(evaluator -> evaluator.isApplicable(accessTime)).findFirst().orElse(null) != null;
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== RangerValidityScheduleConditionEvaluator.isMatched({}): condition={}, ret={}", request, condition, ret);
        }

        return ret;
    }
}
