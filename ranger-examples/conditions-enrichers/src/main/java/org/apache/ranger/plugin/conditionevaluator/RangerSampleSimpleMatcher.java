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

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.ranger.plugin.policyengine.RangerAccessRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * This is a sample implementation of a condition Evaluator.  It works in conjunction with the sample context enricher
 * <code>RangerSampleProjectProvider</code>.  This is how it would be specified in the service definition:
 * {
 * ...
 * ... service definition
 * ...
 * "policyConditions": [
 * {
 * "itemId": 1,
 * "name": "user-in-project",
 * "evaluator": "org.apache.ranger.plugin.conditionevaluator.RangerSimpleMatcher",
 * "evaluatorOptions": { CONTEXT_NAME=’PROJECT’},
 * "validationRegEx":"",
 * "validationMessage": "",
 * "uiHint":"",
 * "label": "Project Matcher",
 * "description": "Projects"
 * }
 * }
 * <p>
 * Name of this class is specified via the "evaluator" of the policy condition definition.  Significant evaluator option
 * for this evaluator is the CONTEXT_NAME which indicates the name under which it would look for value for the condition.
 * It is also use to lookup the condition values specified in the policy.  This example uses CONTEXT_NAME of PROJECT
 * which matches the value under which context is enriched by its companion class <code>RangerSampleProjectProvider</code>.
 * <p>
 * Note that the same Condition Evaluator can be used to process Context enrichment done by <code>RangerSampleCountryProvider</code>
 * provided the CONTEXT_NAME evaluator option is set to COUNTRY which is same as the value used by its companion Context
 * Enricher <code>RangerSampleCountryProvider</code>.  Which serves as an example of how a single Condition Evaluator
 * implementation can be used to model multiple policy conditions.
 * <p>
 * For matching context value against policy values it uses <code>FilenameUtils.wildcardMatch()</code> which allows policy authors
 * flexibility to specify policy conditions using wildcards.  Take a look at
 * org.apache.ranger.plugin.conditionevaluator.RangerSampleSimpleMatcherTest#testIsMatched_happyPath() testIsMatched_happyPath
 * test for examples of what sorts of matching is afforded by this use.
 */
public class RangerSampleSimpleMatcher extends RangerAbstractConditionEvaluator {
    private static final Logger LOG = LoggerFactory.getLogger(RangerSampleSimpleMatcher.class);

    public static final String CONTEXT_NAME = "CONTEXT_NAME";

    private       boolean      allowAny;
    private       String       contextName;
    private final List<String> values = new ArrayList<>();

    @Override
    public void init() {
        LOG.debug("==> RangerSampleSimpleMatcher.init({})", condition);

        super.init();

        if (condition == null) {
            LOG.debug("init: null policy condition! Will match always!");

            allowAny = true;
        } else if (conditionDef == null) {
            LOG.debug("init: null policy condition definition! Will match always!");

            allowAny = true;
        } else if (CollectionUtils.isEmpty(condition.getValues())) {
            LOG.debug("init: empty conditions collection on policy condition!  Will match always!");

            allowAny = true;
        } else if (MapUtils.isEmpty(conditionDef.getEvaluatorOptions())) {
            LOG.debug("init: Evaluator options were empty.  Can't determine what value to use from context.  Will match always.");

            allowAny = true;
        } else if (StringUtils.isEmpty(conditionDef.getEvaluatorOptions().get(CONTEXT_NAME))) {
            LOG.debug("init: CONTEXT_NAME is not specified in evaluator options.  Can't determine what value to use from context.  Will match always.");

            allowAny = true;
        } else {
            contextName = conditionDef.getEvaluatorOptions().get(CONTEXT_NAME);

            values.addAll(condition.getValues());
        }

        LOG.debug("<== RangerSampleSimpleMatcher.init({}): values[{}]", condition, values);
    }

    @Override
    public boolean isMatched(RangerAccessRequest request) {
        LOG.debug("==> RangerSampleSimpleMatcher.isMatched({})", request);

        boolean matched = false;

        if (allowAny) {
            matched = true;
        } else {
            String requestValue = extractValue(request, contextName);

            if (StringUtils.isNotBlank(requestValue)) {
                for (String policyValue : values) {
                    if (FilenameUtils.wildcardMatch(requestValue, policyValue)) {
                        matched = true;
                        break;
                    }
                }
            }
        }

        LOG.debug("<== RangerSampleSimpleMatcher.isMatched({}): {}", request, matched);

        return matched;
    }

    String extractValue(final RangerAccessRequest request, String key) {
        LOG.debug("==> RangerSampleSimpleMatcher.extractValue({})", request);

        String value = null;

        if (request == null) {
            LOG.debug("isMatched: Unexpected: null request.  Returning null!");
        } else if (request.getContext() == null) {
            LOG.debug("isMatched: Context map of request is null.  Ok. Returning null!");
        } else if (CollectionUtils.isEmpty(request.getContext().entrySet())) {
            LOG.debug("isMatched: Missing context on request.  Ok. Condition isn't applicable.  Returning null!");
        } else if (!request.getContext().containsKey(key)) {
            LOG.debug("isMatched: Unexpected: Context did not have data for condition[{}]. Returning null!", key);
        } else {
            value = (String) request.getContext().get(key);
        }

        LOG.debug("<== RangerSampleSimpleMatcher.extractValue({}): {}", request, value);

        return value;
    }
}
