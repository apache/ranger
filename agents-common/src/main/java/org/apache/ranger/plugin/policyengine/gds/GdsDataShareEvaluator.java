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

package org.apache.ranger.plugin.policyengine.gds;

import org.apache.commons.lang.StringUtils;
import org.apache.ranger.plugin.conditionevaluator.RangerConditionEvaluator;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.model.validation.RangerServiceDefHelper;
import org.apache.ranger.plugin.policyengine.RangerAccessRequest;
import org.apache.ranger.plugin.policyengine.RangerResourceTrie;
import org.apache.ranger.plugin.policyevaluator.RangerCustomConditionEvaluator;
import org.apache.ranger.plugin.policyresourcematcher.RangerResourceEvaluator;
import org.apache.ranger.plugin.util.RangerResourceEvaluatorsRetriever;
import org.apache.ranger.plugin.util.ServiceGdsInfo.DataShareInfo;
import org.apache.ranger.plugin.util.ServiceGdsInfo.SharedResourceInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class GdsDataShareEvaluator {
    private static final Logger LOG = LoggerFactory.getLogger(GdsDataShareEvaluator.class);

    public static final GdsDataShareEvalOrderComparator EVAL_ORDER_COMPARATOR = new GdsDataShareEvalOrderComparator();

    private final DataShareInfo                                               dsh;
    private final String                                                      name;
    private final String                                                      zoneName;
    private final Map<String, RangerResourceTrie<GdsSharedResourceEvaluator>> resourceTries  = new HashMap<>();
    private final List<GdsDshidEvaluator>                                     dsidEvaluators = new ArrayList<>();
    private final RangerConditionEvaluator                                    conditionEvaluator;

    public GdsDataShareEvaluator(DataShareInfo dsh, List<SharedResourceInfo> resources, RangerServiceDefHelper serviceDefHelper) {
        LOG.debug("==> GdsDataShareEvaluator({}, {})", dsh, resources);

        this.dsh                = dsh;
        this.name               = StringUtils.isBlank(dsh.getName()) ? StringUtils.EMPTY : dsh.getName();
        this.zoneName           = StringUtils.isBlank(dsh.getZoneName()) ? StringUtils.EMPTY : dsh.getZoneName();
        this.conditionEvaluator = RangerCustomConditionEvaluator.getInstance().getExpressionEvaluator(dsh.getConditionExpr(), serviceDefHelper.getServiceDef());

        if (resources != null) {
            Set<String>                   resourceKeys = new HashSet<>();
            List<RangerResourceEvaluator> evaluators   = new ArrayList<>(resources.size());

            for (SharedResourceInfo resource : resources) {
                GdsSharedResourceEvaluator evaluator = new GdsSharedResourceEvaluator(resource, dsh.getDefaultAccessTypes(), serviceDefHelper);

                evaluators.add(evaluator);

                resourceKeys.addAll(evaluator.getResourceKeys());
            }

            for (String resourceKey : resourceKeys) {
                RangerServiceDef.RangerResourceDef resourceDef  = serviceDefHelper.getResourceDef(resourceKey);
                RangerResourceTrie                 resourceTrie = new RangerResourceTrie<>(resourceDef, evaluators);

                resourceTries.put(resourceKey, resourceTrie);
            }
        }

        LOG.debug("<== GdsDataShareEvaluator({}, {})", dsh, resources);
    }

    public Long getId() {
        return dsh.getId();
    }

    public String getName() {
        return name;
    }

    public String getZoneName() {
        return zoneName;
    }

    public void addDshidEvaluator(GdsDshidEvaluator dhidEvaluator) {
        dsidEvaluators.add(dhidEvaluator);
    }

    public void evaluate(RangerAccessRequest request, GdsAccessResult result) {
        LOG.debug("==> GdsDataShareEvaluator.evaluate({}, {})", request, result);

        Collection<GdsSharedResourceEvaluator> evaluators = RangerResourceEvaluatorsRetriever.getEvaluators(resourceTries, request.getResource().getAsMap(), request.getResourceElementMatchingScopes());

        if (evaluators == null) {
            evaluators = Collections.emptyList();
        } else if (evaluators.size() > 1) {
            List<GdsSharedResourceEvaluator> list = new ArrayList<>(evaluators);

            list.sort(GdsSharedResourceEvaluator.EVAL_ORDER_COMPARATOR);

            evaluators = list;
        }

        LOG.debug("GdsDataShareEvaluator.evaluate({}): found {} evaluators", request, evaluators.size());

        if (!evaluators.isEmpty()) {
            boolean isAllowed = conditionEvaluator == null || conditionEvaluator.isMatched(request);

            if (isAllowed) {
                // find if any of the shared resources allow the request
                for (GdsSharedResourceEvaluator evaluator : evaluators) {
                    isAllowed = evaluator.isAllowed(request);

                    if (isAllowed) {
                        break;
                    }
                }

                if (isAllowed) { // now find dsidEvaluators that allow the request and collect their datasetIds
                    for (GdsDshidEvaluator dsidEvaluator : dsidEvaluators) {
                        if (!result.hasDataset(dsidEvaluator.getDatasetId())) {
                            if (dsidEvaluator.isAllowed(request)) {
                                result.addDataset(dsidEvaluator.getDatasetId());
                            }
                        }
                    }
                }
            } else {
                LOG.debug("GdsDataShareEvaluator.evaluate({}): conditions {} didn't match. Skipped", request, dsh.getConditionExpr());
            }
        }

        LOG.debug("<== GdsDataShareEvaluator.evaluate({}, {})", request, result);
    }

    public static class GdsDataShareEvalOrderComparator implements Comparator<GdsDataShareEvaluator> {
        @Override
        public int compare(GdsDataShareEvaluator me, GdsDataShareEvaluator other) {
            int ret = 0;

            if (me != null && other != null) {
                ret = me.getName().compareTo(other.dsh.getName());

                if (ret == 0) {
                    ret = me.getId().compareTo(other.getId());
                }
            } else if (me != null) {
                ret = -1;
            } else if (other != null) {
                ret = 1;
            }

            return ret;
        }
    }
}
