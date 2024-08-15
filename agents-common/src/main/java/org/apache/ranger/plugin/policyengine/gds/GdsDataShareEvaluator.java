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
import org.apache.ranger.plugin.model.RangerServiceDef.RangerResourceDef;
import org.apache.ranger.plugin.model.validation.RangerServiceDefHelper;
import org.apache.ranger.plugin.policyengine.RangerAccessRequest;
import org.apache.ranger.plugin.policyengine.RangerPluginContext;
import org.apache.ranger.plugin.policyengine.RangerResourceACLs;
import org.apache.ranger.plugin.policyengine.RangerResourceTrie;
import org.apache.ranger.plugin.policyevaluator.RangerCustomConditionEvaluator;
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
    private final RangerConditionEvaluator                                    conditionEvaluator;
    private final List<GdsSharedResourceEvaluator>                            evaluators;
    private final Map<String, RangerResourceTrie<GdsSharedResourceEvaluator>> resourceTries;
    private final List<GdsDshidEvaluator>                                     dsidEvaluators = new ArrayList<>();

    public GdsDataShareEvaluator(DataShareInfo dsh, List<SharedResourceInfo> resources, RangerServiceDefHelper serviceDefHelper, RangerPluginContext pluginContext) {
        LOG.debug("==> GdsDataShareEvaluator({}, {})", dsh, resources);

        this.dsh                = dsh;
        this.name               = StringUtils.isBlank(dsh.getName()) ? StringUtils.EMPTY : dsh.getName();
        this.zoneName           = StringUtils.isBlank(dsh.getZoneName()) ? StringUtils.EMPTY : dsh.getZoneName();
        this.conditionEvaluator = RangerCustomConditionEvaluator.getInstance().getExpressionEvaluator(dsh.getConditionExpr(), serviceDefHelper.getServiceDef());

        if (resources != null) {
            Set<String> resourceKeys = new HashSet<>();

            evaluators    = new ArrayList<>(resources.size());
            resourceTries = new HashMap<>();

            for (SharedResourceInfo resource : resources) {
                GdsSharedResourceEvaluator evaluator = new GdsSharedResourceEvaluator(resource, dsh.getDefaultAccessTypes(), serviceDefHelper, pluginContext);

                evaluators.add(evaluator);

                resourceKeys.addAll(evaluator.getResourceKeys());
            }

            for (String resourceKey : resourceKeys) {
                RangerResourceDef                              resourceDef  = serviceDefHelper.getResourceDef(resourceKey);
                RangerResourceTrie<GdsSharedResourceEvaluator> resourceTrie = new RangerResourceTrie<>(resourceDef, evaluators);

                resourceTries.put(resourceKey, resourceTrie);
            }
        } else {
            evaluators    = Collections.emptyList();
            resourceTries = Collections.emptyMap();
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

    public List<GdsSharedResourceEvaluator> getSharedResourceEvaluators() { return evaluators; }

    public boolean isInDataset(long datasetId) {
        boolean ret = false;

        for (GdsDshidEvaluator dsidEvaluator : dsidEvaluators) {
            if (dsidEvaluator.getDatasetId().equals(datasetId)) {
                ret = true;

                break;
            }
        }

        return ret;
    }

    public boolean isInProject(long projectId) {
        boolean ret = false;

        for (GdsDshidEvaluator dsidEvaluator : dsidEvaluators) {
            if (dsidEvaluator.getDatasetEvaluator().isInProject(projectId)) {
                ret = true;

                break;
            }
        }

        return ret;
    }

    public void evaluate(RangerAccessRequest request, GdsAccessResult result, Set<Long> datasetIds) {
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
                        if (!datasetIds.contains(dsidEvaluator.getDatasetId())) {
                            if (dsidEvaluator.isAllowed(request)) {
                                datasetIds.add(dsidEvaluator.getDatasetId());
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

    public void getResourceACLs(RangerAccessRequest request, RangerResourceACLs acls) {
        LOG.debug("==> GdsDataShareEvaluator.getResourceACLs({}, {})", request, acls);

        List<GdsSharedResourceEvaluator> evaluators = getResourceEvaluators(request);

        if (!evaluators.isEmpty()) {
            boolean isConditional = conditionEvaluator != null;

            for (GdsSharedResourceEvaluator evaluator : evaluators) {
                evaluator.getResourceACLs(request, acls, isConditional, dsidEvaluators);
            }
        }

        LOG.debug("<== GdsDataShareEvaluator.getResourceACLs({}, {})", request, acls);
    }

    void addDshidEvaluator(GdsDshidEvaluator dhidEvaluator) {
        dsidEvaluators.add(dhidEvaluator);
    }

    private List<GdsSharedResourceEvaluator> getResourceEvaluators(RangerAccessRequest request) {
        final List<GdsSharedResourceEvaluator> ret;

        Collection<GdsSharedResourceEvaluator> evaluators = RangerResourceEvaluatorsRetriever.getEvaluators(resourceTries, request.getResource().getAsMap(), request.getResourceElementMatchingScopes());

        if (evaluators == null || evaluators.isEmpty()) {
            ret = Collections.emptyList();
        } else if (evaluators.size() > 1) {
            ret = new ArrayList<>(evaluators);

            ret.sort(GdsSharedResourceEvaluator.EVAL_ORDER_COMPARATOR);
        } else {
            ret = Collections.singletonList(evaluators.iterator().next());
        }

        LOG.debug("GdsDataShareEvaluator.getResourceEvaluators({}): found {} evaluators", request, ret.size());

        return ret;
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
