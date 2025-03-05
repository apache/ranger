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
import org.apache.ranger.plugin.util.ServiceGdsInfo.ProjectInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Set;

public class GdsProjectEvaluator {
    private static final Logger LOG = LoggerFactory.getLogger(GdsProjectEvaluator.class);

    public static final GdsProjectEvalOrderComparator EVAL_ORDER_COMPARATOR = new GdsProjectEvalOrderComparator();

    private final ProjectInfo                     project;
    private final RangerServiceDef                gdsServiceDef;
    private final String                          name;
    private final RangerValidityScheduleEvaluator scheduleEvaluator;
    private final List<RangerPolicyEvaluator>     policyEvaluators;

    public GdsProjectEvaluator(ProjectInfo project, RangerServiceDef gdsServiceDef, RangerPolicyEngineOptions options) {
        LOG.debug("==> GdsProjectEvaluator({})", project);

        this.project       = project;
        this.gdsServiceDef = gdsServiceDef;
        this.name          = StringUtils.isBlank(project.getName()) ? StringUtils.EMPTY : project.getName();

        if (project.getValiditySchedule() != null) {
            scheduleEvaluator = new RangerValidityScheduleEvaluator(project.getValiditySchedule());
        } else {
            scheduleEvaluator = null;
        }

        if (project.getPolicies() != null) {
            policyEvaluators = new ArrayList<>(project.getPolicies().size());

            for (RangerPolicy policy : project.getPolicies()) {
                RangerPolicyEvaluator evaluator = new RangerOptimizedPolicyEvaluator();

                evaluator.init(policy, gdsServiceDef, options);

                policyEvaluators.add(evaluator);
            }
        } else {
            policyEvaluators = Collections.emptyList();
        }

        LOG.debug("<== GdsProjectEvaluator({})", project);
    }

    public Long getId() {
        return project.getId();
    }

    public String getName() {
        return name;
    }

    public boolean isActive() {
        return scheduleEvaluator == null || scheduleEvaluator.isApplicable(System.currentTimeMillis());
    }

    public void evaluate(RangerAccessRequest request, GdsAccessResult result) {
        LOG.debug("==> GdsDatasetEvaluator.evaluate({}, {})", request, result);

        if (isActive()) {
            result.addProject(getName());

            if (!policyEvaluators.isEmpty()) {
                GdsProjectAccessRequest projectRequest = new GdsProjectAccessRequest(getId(), gdsServiceDef, request);
                RangerAccessResult      projectResult  = projectRequest.createAccessResult();

                try {
                    RangerAccessRequestUtil.setAllRequestedAccessTypes(projectRequest.getContext(), null);
                    RangerAccessRequestUtil.setAccessTypeACLResults(projectRequest.getContext(), null);

                    policyEvaluators.forEach(e -> e.evaluate(projectRequest, projectResult));
                } finally {
                    RangerAccessRequestUtil.setAccessTypeResults(projectRequest.getContext(), null);
                    RangerAccessRequestUtil.setAccessTypeACLResults(projectRequest.getContext(), null);
                }

                if (projectResult.getIsAllowed()) {
                    result.addAllowedByProject(getName());
                }

                if (!result.getIsAllowed()) {
                    if (projectResult.getIsAllowed()) {
                        result.setIsAllowed(true);
                        result.setPolicyId(projectResult.getPolicyId());
                        result.setPolicyVersion(projectResult.getPolicyVersion());
                    }
                }

                if (!result.getIsAudited()) {
                    result.setIsAudited(projectResult.getIsAudited());
                }
            }
        }

        LOG.debug("<== GdsDatasetEvaluator.evaluate({}, {})", request, result);
    }

    public void getResourceACLs(RangerAccessRequest request, RangerResourceACLs acls, boolean isConditional, Set<String> allowedAccessTypes) {
        if (isActive()) {
            acls.getProjects().add(getName());

            if (!policyEvaluators.isEmpty()) {
                GdsProjectAccessRequest projectRequest = new GdsProjectAccessRequest(getId(), gdsServiceDef, request);

                for (RangerPolicyEvaluator policyEvaluator : policyEvaluators) {
                    policyEvaluator.getResourceACLs(projectRequest, acls, isConditional, allowedAccessTypes, RangerPolicyResourceMatcher.MatchType.SELF, null);
                }
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

    private static class GdsProjectAccessRequest extends RangerAccessRequestImpl {
        public GdsProjectAccessRequest(Long projectId, RangerServiceDef gdsServiceDef, RangerAccessRequest request) {
            super.setResource(new RangerProjectResource(projectId, gdsServiceDef, request.getResource().getOwnerUser()));

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

    public static class RangerProjectResource extends RangerAccessResourceImpl {
        public RangerProjectResource(Long projectd, RangerServiceDef gdsServiceDef, String ownerUser) {
            super.setValue(GdsPolicyEngine.RESOURCE_NAME_PROJECT_ID, projectd.toString());
            super.setServiceDef(gdsServiceDef);
            super.setOwnerUser(ownerUser);
        }
    }

    public static class GdsProjectEvalOrderComparator implements Comparator<GdsProjectEvaluator> {
        @Override
        public int compare(GdsProjectEvaluator me, GdsProjectEvaluator other) {
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
