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
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.policyengine.RangerAccessRequest;
import org.apache.ranger.plugin.policyengine.RangerAccessRequestImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessResourceImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessResult;
import org.apache.ranger.plugin.policyengine.RangerPolicyEngineOptions;
import org.apache.ranger.plugin.policyengine.RangerResourceACLs;
import org.apache.ranger.plugin.policyevaluator.RangerOptimizedPolicyEvaluator;
import org.apache.ranger.plugin.policyevaluator.RangerPolicyEvaluator;
import org.apache.ranger.plugin.policyevaluator.RangerValidityScheduleEvaluator;
import org.apache.ranger.plugin.policyresourcematcher.RangerPolicyResourceMatcher;
import org.apache.ranger.plugin.util.RangerAccessRequestUtil;
import org.apache.ranger.plugin.util.ServiceGdsInfo.DatasetInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Set;

public class GdsDatasetEvaluator {
    private static final Logger LOG = LoggerFactory.getLogger(GdsDatasetEvaluator.class);

    public static final GdsDatasetEvalOrderComparator EVAL_ORDER_COMPARATOR = new GdsDatasetEvalOrderComparator();

    private final DatasetInfo                     dataset;
    private final RangerServiceDef                gdsServiceDef;
    private final String                          name;
    private final RangerValidityScheduleEvaluator scheduleEvaluator;
    private final List<GdsDipEvaluator>           dipEvaluators = new ArrayList<>();
    private final List<RangerPolicyEvaluator>     policyEvaluators;

    public GdsDatasetEvaluator(DatasetInfo dataset, RangerServiceDef gdsServiceDef, RangerPolicyEngineOptions options) {
        LOG.debug("==> GdsDatasetEvaluator()");

        this.dataset       = dataset;
        this.gdsServiceDef = gdsServiceDef;
        this.name          = StringUtils.isBlank(dataset.getName()) ? StringUtils.EMPTY : dataset.getName();

        if (dataset.getValiditySchedule() != null) {
            scheduleEvaluator = new RangerValidityScheduleEvaluator(dataset.getValiditySchedule());
        } else {
            scheduleEvaluator = null;
        }

        if (dataset.getPolicies() != null) {
            policyEvaluators = new ArrayList<>(dataset.getPolicies().size());

            for (RangerPolicy policy : dataset.getPolicies()) {
                RangerPolicyEvaluator evaluator = new RangerOptimizedPolicyEvaluator();

                evaluator.init(policy, gdsServiceDef, options);

                policyEvaluators.add(evaluator);
            }
        } else {
            policyEvaluators = Collections.emptyList();
        }

        LOG.debug("<== GdsDatasetEvaluator()");
    }

    public Long getId() {
        return dataset.getId();
    }

    public String getName() {
        return name;
    }

    public boolean isInProject(Long projectId) {
        return dipEvaluators.stream().anyMatch(e -> e.getProjectId().equals(projectId) && e.isActive());
    }

    public boolean isInAnyProject(Set<String> projectNames) {
        return dipEvaluators.stream().anyMatch(e -> projectNames.contains(e.getProjectEvaluator().getName()) && e.isActive());
    }

    public boolean isActive() {
        return scheduleEvaluator == null || scheduleEvaluator.isApplicable(System.currentTimeMillis());
    }

    public void evaluate(RangerAccessRequest request, GdsAccessResult result, Collection<GdsProjectEvaluator> projectsToEval) {
        LOG.debug("==> GdsDatasetEvaluator.evaluate({}, {}, {})", request, result, projectsToEval);

        if (isActive()) {
            result.addDataset(getName());

            if (!policyEvaluators.isEmpty()) {
                GdsDatasetAccessRequest datasetRequest = new GdsDatasetAccessRequest(getId(), gdsServiceDef, request);
                RangerAccessResult      datasetResult  = datasetRequest.createAccessResult();

                try {
                    RangerAccessRequestUtil.setAccessTypeResults(datasetRequest.getContext(), null);
                    RangerAccessRequestUtil.setAccessTypeACLResults(datasetRequest.getContext(), null);

                    policyEvaluators.forEach(e -> e.evaluate(datasetRequest, datasetResult));
                } finally {
                    RangerAccessRequestUtil.setAccessTypeResults(datasetRequest.getContext(), null);
                    RangerAccessRequestUtil.setAccessTypeACLResults(datasetRequest.getContext(), null);
                }

                if (datasetResult.getIsAllowed()) {
                    result.addAllowedByDataset(getName());
                }

                if (!result.getIsAllowed()) {
                    if (datasetResult.getIsAllowed()) {
                        result.setIsAllowed(true);
                        result.setPolicyId(datasetResult.getPolicyId());
                        result.setPolicyVersion(datasetResult.getPolicyVersion());
                    }
                }

                if (!result.getIsAudited()) {
                    result.setIsAudited(datasetResult.getIsAudited());
                }
            }

            dipEvaluators.stream().filter(e -> e.isAllowed(request) && e.getProjectEvaluator().isActive()).forEach(dip -> projectsToEval.add(dip.getProjectEvaluator()));
        }

        LOG.debug("<== GdsDatasetEvaluator.evaluate({}, {}, {})", request, result, projectsToEval);
    }

    public void getResourceACLs(RangerAccessRequest request, RangerResourceACLs acls, boolean isConditional, Set<String> allowedAccessTypes) {
        if (isActive()) {
            acls.getDatasets().add(getName());

            if (!policyEvaluators.isEmpty()) {
                GdsDatasetAccessRequest datasetRequest = new GdsDatasetAccessRequest(getId(), gdsServiceDef, request);

                for (RangerPolicyEvaluator policyEvaluator : policyEvaluators) {
                    policyEvaluator.getResourceACLs(datasetRequest, acls, isConditional, allowedAccessTypes, RangerPolicyResourceMatcher.MatchType.SELF, null);
                }
            }

            for (GdsDipEvaluator dipEvaluator : dipEvaluators) {
                dipEvaluator.getResourceACLs(request, acls, isConditional, allowedAccessTypes);
            }
        }
    }

    public boolean hasReference(Set<String> users, Set<String> groups, Set<String> roles) {
        boolean ret = false;

        for (RangerPolicyEvaluator policyEvaluator : policyEvaluators) {
            ret = policyEvaluator.hasReference(users, groups, roles);

            if (ret) {
                break;
            }
        }

        return ret;
    }

    void addDipEvaluator(GdsDipEvaluator dipEvaluator) {
        dipEvaluators.add(dipEvaluator);
    }

    private static class GdsDatasetAccessRequest extends RangerAccessRequestImpl {
        public GdsDatasetAccessRequest(Long datasetId, RangerServiceDef gdsServiceDef, RangerAccessRequest request) {
            super.setResource(new RangerDatasetResource(datasetId, gdsServiceDef, request.getResource().getOwnerUser()));

            super.setUser(request.getUser());
            super.setUserGroups(request.getUserGroups());
            super.setUserRoles(request.getUserRoles());
            super.setAction(request.getAction());
            super.setAccessType(request.getAccessType());
            super.setAccessTime(request.getAccessTime());
            super.setRequestData(request.getRequestData());
            super.setContext(request.getContext());
            super.setClientType(request.getClientType());
            super.setClientIPAddress(request.getClientIPAddress());
            super.setRemoteIPAddress(request.getRemoteIPAddress());
            super.setForwardedAddresses(request.getForwardedAddresses());
            super.setSessionId(request.getSessionId());
            super.setResourceMatchingScope(request.getResourceMatchingScope());
        }

        public RangerAccessResult createAccessResult() {
            return new RangerAccessResult(RangerPolicy.POLICY_TYPE_ACCESS, GdsPolicyEngine.GDS_SERVICE_NAME, getResource().getServiceDef(), this);
        }
    }

    public static class RangerDatasetResource extends RangerAccessResourceImpl {
        public RangerDatasetResource(Long datasetd, RangerServiceDef gdsServiceDef, String ownerUser) {
            super.setValue(GdsPolicyEngine.RESOURCE_NAME_DATASET_ID, datasetd.toString());
            super.setServiceDef(gdsServiceDef);
            super.setOwnerUser(ownerUser);
        }
    }

    public static class GdsDatasetEvalOrderComparator implements Comparator<GdsDatasetEvaluator> {
        @Override
        public int compare(GdsDatasetEvaluator me, GdsDatasetEvaluator other) {
            int ret = 0;

            if (me != null && other != null) {
                ret = me.getName().compareTo(other.getName());

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
