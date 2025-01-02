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
import org.apache.ranger.plugin.model.validation.RangerServiceDefHelper;
import org.apache.ranger.plugin.policyengine.RangerAccessRequest;
import org.apache.ranger.plugin.policyengine.RangerResourceACLs;
import org.apache.ranger.plugin.policyevaluator.RangerCustomConditionEvaluator;
import org.apache.ranger.plugin.util.ServiceGdsInfo.DataShareInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

public class GdsDataShareEvaluator {
    private static final Logger LOG = LoggerFactory.getLogger(GdsDataShareEvaluator.class);

    public static final GdsDataShareEvalOrderComparator EVAL_ORDER_COMPARATOR = new GdsDataShareEvalOrderComparator();

    private final DataShareInfo                   dsh;
    private final String                          name;
    private final String                          zoneName;
    private final RangerConditionEvaluator        conditionEvaluator;
    private final Set<GdsSharedResourceEvaluator> evaluators      = new TreeSet<>(GdsSharedResourceEvaluator.EVAL_ORDER_COMPARATOR); // keep sorted
    private final List<GdsDshidEvaluator>         dshidEvaluators = new ArrayList<>();

    public GdsDataShareEvaluator(DataShareInfo dsh, RangerServiceDefHelper serviceDefHelper) {
        LOG.debug("==> GdsDataShareEvaluator({})", dsh);

        this.dsh                = dsh;
        this.name               = StringUtils.isBlank(dsh.getName()) ? StringUtils.EMPTY : dsh.getName();
        this.zoneName           = StringUtils.isBlank(dsh.getZoneName()) ? StringUtils.EMPTY : dsh.getZoneName();
        this.conditionEvaluator = RangerCustomConditionEvaluator.getInstance().getExpressionEvaluator(dsh.getConditionExpr(), serviceDefHelper.getServiceDef());

        LOG.debug("<== GdsDataShareEvaluator({})", dsh);
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

    public Set<String> getDefaultAccessTypes() {
        return dsh.getDefaultAccessTypes();
    }

    public Set<GdsSharedResourceEvaluator> getResourceEvaluators() {
        return evaluators;
    }

    public List<GdsDshidEvaluator> getDshidEvaluators() {
        return dshidEvaluators;
    }

    public List<GdsSharedResourceEvaluator> getResourceEvaluators(RangerAccessRequest request) {
        final List<GdsSharedResourceEvaluator> ret;
        final boolean                          isAllowed = conditionEvaluator == null || conditionEvaluator.isMatched(request);

        if (isAllowed) {
            ret = evaluators.stream().filter(e -> e.isAllowed(request)).collect(Collectors.toList());
        } else {
            ret = Collections.emptyList();
        }

        return ret;
    }

    public boolean isInDataset(Long datasetId) {
        return dshidEvaluators.stream().anyMatch(e -> e.getDatasetId().equals(datasetId) && e.isActive());
    }

    public boolean isInProject(Long projectId) {
        return dshidEvaluators.stream().anyMatch(e -> e.getDatasetEvaluator().isInProject(projectId) && e.isActive());
    }

    public void collectDatasets(RangerAccessRequest request, Map<GdsDatasetEvaluator, Set<GdsDataShareEvaluator>> datasetsToEval) {
        LOG.debug("==> GdsDataShareEvaluator.collectDatasets({}, {})", request, datasetsToEval);

        boolean isAllowed = conditionEvaluator == null || conditionEvaluator.isMatched(request);

        if (isAllowed) {
            dshidEvaluators.stream().filter(dshid -> dshid.isAllowed(request) && dshid.getDatasetEvaluator().isActive()).forEach(dshid -> datasetsToEval.computeIfAbsent(dshid.getDatasetEvaluator(), s -> new TreeSet<>(GdsDataShareEvaluator.EVAL_ORDER_COMPARATOR)).add(this));
        }

        LOG.debug("<== GdsDataShareEvaluator.collectDatasets({}, {})", request, datasetsToEval);
    }

    public void getResourceACLs(RangerAccessRequest request, RangerResourceACLs acls) {
        LOG.debug("==> GdsDataShareEvaluator.getResourceACLs({}, {})", request, acls);

        List<GdsSharedResourceEvaluator> evaluators = getResourceEvaluators(request);

        if (!evaluators.isEmpty()) {
            boolean isConditional = conditionEvaluator != null;

            for (GdsSharedResourceEvaluator evaluator : evaluators) {
                evaluator.getResourceACLs(request, acls, isConditional, dshidEvaluators);
            }
        }

        LOG.debug("<== GdsDataShareEvaluator.getResourceACLs({}, {})", request, acls);
    }

    void addResourceEvaluator(GdsSharedResourceEvaluator evaluator) {
        evaluators.add(evaluator);
    }

    void addDshidEvaluator(GdsDshidEvaluator dhidEvaluator) {
        dshidEvaluators.add(dhidEvaluator);
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
