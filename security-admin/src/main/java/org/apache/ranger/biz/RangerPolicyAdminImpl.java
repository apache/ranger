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

package org.apache.ranger.biz;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ranger.plugin.contextenricher.RangerTagForEval;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyResource;
import org.apache.ranger.plugin.policyengine.PolicyEngine;
import org.apache.ranger.plugin.policyengine.RangerAccessRequest;
import org.apache.ranger.plugin.policyengine.RangerAccessRequestImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessRequestProcessor;
import org.apache.ranger.plugin.policyengine.RangerAccessResource;
import org.apache.ranger.plugin.policyengine.RangerPluginContext;
import org.apache.ranger.plugin.policyengine.RangerPolicyEngine;
import org.apache.ranger.plugin.policyengine.RangerPolicyRepository;
import org.apache.ranger.plugin.policyengine.RangerTagResource;
import org.apache.ranger.plugin.policyevaluator.RangerPolicyEvaluator;
import org.apache.ranger.plugin.policyresourcematcher.RangerPolicyResourceMatcher;
import org.apache.ranger.plugin.service.RangerDefaultRequestProcessor;
import org.apache.ranger.plugin.util.GrantRevokeRequest;
import org.apache.ranger.plugin.util.RangerAccessRequestUtil;
import org.apache.ranger.plugin.util.RangerPerfTracer;
import org.apache.ranger.plugin.util.RangerRoles;
import org.apache.ranger.plugin.util.ServicePolicies;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class RangerPolicyAdminImpl implements RangerPolicyAdmin {
    private static final Log LOG = LogFactory.getLog(RangerPolicyAdminImpl.class);

    private static final Log PERF_POLICYENGINE_REQUEST_LOG = RangerPerfTracer.getPerfLogger("policyengine.request");

    private final PolicyEngine                 policyEngine;
    private final RangerAccessRequestProcessor requestProcessor;

    static public RangerPolicyAdmin getPolicyAdmin(final RangerPolicyAdminImpl other, final ServicePolicies servicePolicies) {
        RangerPolicyAdmin ret = null;

        if (other != null && servicePolicies != null) {
            PolicyEngine policyEngine = other.policyEngine.cloneWithDelta(servicePolicies);

            if (policyEngine != null) {
                ret = new RangerPolicyAdminImpl(policyEngine);
            }
        }

        return ret;
    }

    RangerPolicyAdminImpl(ServicePolicies servicePolicies, RangerPluginContext pluginContext, RangerRoles roles) {
        this.policyEngine     = new PolicyEngine(servicePolicies, pluginContext, roles);
        this.requestProcessor = new RangerDefaultRequestProcessor(policyEngine);
    }

    private RangerPolicyAdminImpl(final PolicyEngine policyEngine) {
        this.policyEngine     = policyEngine;
        this.requestProcessor = new RangerDefaultRequestProcessor(policyEngine);
    }

    @Override
    public boolean isAccessAllowed(RangerAccessResource resource, String zoneName, String user, Set<String> userGroups, String accessType) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> RangerPolicyAdminImpl.isAccessAllowed(" + resource + ", " + zoneName + ", " + user + ", " + userGroups + ", " + accessType + ")");
        }

        boolean          ret  = false;
        RangerPerfTracer perf = null;

        if(RangerPerfTracer.isPerfTraceEnabled(PERF_POLICYENGINE_REQUEST_LOG)) {
            perf = RangerPerfTracer.getPerfTracer(PERF_POLICYENGINE_REQUEST_LOG, "RangerPolicyAdminImpl.isAccessAllowed(user=" + user + ",accessType=" + accessType + "resource=" + resource.getAsString() + ")");
        }

        final RangerPolicyRepository matchedRepository = policyEngine.getRepositoryForZone(zoneName);

        if (matchedRepository != null) {
            Set<String> roles = getRolesFromUserAndGroups(user, userGroups);

            for (RangerPolicyEvaluator evaluator : matchedRepository.getLikelyMatchPolicyEvaluators(resource, RangerPolicy.POLICY_TYPE_ACCESS)) {
                ret = evaluator.isAccessAllowed(resource, user, userGroups, roles, accessType);

                if (ret) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Access granted by policy:[" + evaluator.getPolicy() + "]");
                    }

                    break;
                }
            }

        }

        RangerPerfTracer.log(perf);

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== RangerPolicyAdminImpl.isAccessAllowed(" + resource + ", " + zoneName + ", " + user + ", " + userGroups + ", " + accessType + "): " + ret);
        }

        return ret;
    }

    @Override
    public boolean isAccessAllowed(RangerPolicy policy, String user, Set<String> userGroups, Set<String> roles, String accessType) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> RangerPolicyAdminImpl.isAccessAllowed(" + policy.getId() + ", " + user + ", " + userGroups + ", " + roles + ", " + accessType + ")");
        }

        boolean          ret  = false;
        RangerPerfTracer perf = null;

        if(RangerPerfTracer.isPerfTraceEnabled(PERF_POLICYENGINE_REQUEST_LOG)) {
            perf = RangerPerfTracer.getPerfTracer(PERF_POLICYENGINE_REQUEST_LOG, "RangerPolicyEngine.isAccessAllowed(user=" + user + "," + userGroups + ", roles=" + roles + ",accessType=" + accessType + ")");
        }

        final RangerPolicyRepository matchedRepository = policyEngine.getRepositoryForMatchedZone(policy);

        if (matchedRepository != null) {
            for (RangerPolicyEvaluator evaluator : matchedRepository.getPolicyEvaluators()) {
                ret = evaluator.isAccessAllowed(policy, user, userGroups, roles, accessType);

                if (ret) {
                    break;
                }
            }
        }

        RangerPerfTracer.log(perf);

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== RangerPolicyAdminImpl.isAccessAllowed(" + policy.getId() + ", " + user + ", " + userGroups + ", " + roles + ", " + accessType + "): " + ret);
        }

        return ret;
    }

    @Override
    public List<RangerPolicy> getExactMatchPolicies(RangerAccessResource resource, String zoneName, Map<String, Object> evalContext) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> RangerPolicyAdminImpl.getExactMatchPolicies(" + resource + ", " + zoneName + ", " + evalContext  + ")");
        }

        List<RangerPolicy>     ret              = null;

        RangerPolicyRepository policyRepository = policyEngine.getRepositoryForZone(zoneName);

        if (policyRepository != null) {
            for (RangerPolicyEvaluator evaluator : policyRepository.getPolicyEvaluators()) {
                if (evaluator.isCompleteMatch(resource, evalContext)) {
                    if (ret == null) {
                        ret = new ArrayList<>();
                    }

                    ret.add(evaluator.getPolicy());
                }
            }
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<==> RangerPolicyAdminImpl.getExactMatchPolicies(" + resource + ", " + zoneName + ", " + evalContext  + "): " + ret);
        }

        return ret;
    }

    @Override
    public List<RangerPolicy> getExactMatchPolicies(RangerPolicy policy, Map<String, Object> evalContext) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> RangerPolicyAdminImpl.getExactMatchPolicies(" + policy + ", " + evalContext + ")");
        }

        List<RangerPolicy>     ret              = null;
        RangerPolicyRepository policyRepository = policyEngine.getRepositoryForMatchedZone(policy);

        if (policyRepository != null) {
            Map<String, RangerPolicyResource> resources = policy.getResources();

            for (RangerPolicyEvaluator evaluator : policyRepository.getPolicyEvaluators()) {
                if (evaluator.isCompleteMatch(resources, evalContext)) {
                    if (ret == null) {
                        ret = new ArrayList<>();
                    }

                    ret.add(evaluator.getPolicy());
                }
            }
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== RangerPolicyAdminImpl.getExactMatchPolicies(" + policy + ", " + evalContext + "): " + ret);
        }

        return ret;
    }

    @Override
    public List<RangerPolicy> getMatchingPolicies(RangerAccessResource resource) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> RangerPolicyAdminImpl.getMatchingPolicies(" + resource + ")");
        }

        List<RangerPolicy> ret = getMatchingPolicies(resource, RangerPolicyEngine.ANY_ACCESS);

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== RangerPolicyAdminImpl.getMatchingPolicies(" + resource + ") : " + ret.size());
        }

        return ret;
    }

    @Override
    public long getPolicyVersion() { return policyEngine.getPolicyVersion(); }

    @Override
    public long getRoleVersion() { return policyEngine.getRoleVersion(); }

    @Override
    public String getServiceName() { return policyEngine.getServiceName(); }

    @Override
    public void setRoles(RangerRoles roles) {
        policyEngine.setRoles(roles);
    }

    @Override
    public Set<String> getRolesFromUserAndGroups(String user, Set<String> groups) {
        return policyEngine.getPluginContext().getAuthContext().getRolesForUserAndGroups(user, groups);
    }

    @Override
    public String getUniquelyMatchedZoneName(GrantRevokeRequest grantRevokeRequest) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> RangerPolicyAdminImpl.getUniquelyMatchedZoneName(" + grantRevokeRequest + ")");
        }

        String ret = policyEngine.getUniquelyMatchedZoneName(grantRevokeRequest.getResource());

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== RangerPolicyAdminImpl.getUniquelyMatchedZoneName(" + grantRevokeRequest + ") : " + ret);
        }

        return ret;
    }

    // This API is used only by test-code; checks only policies within default security-zone
    @Override
    public boolean isAccessAllowedByUnzonedPolicies(Map<String, RangerPolicyResource> resources, String user, Set<String> userGroups, String accessType) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> RangerPolicyAdminImpl.isAccessAllowedByUnzonedPolicies(" + resources + ", " + user + ", " + userGroups + ", " + accessType + ")");
        }

        boolean          ret  = false;
        RangerPerfTracer perf = null;

        if(RangerPerfTracer.isPerfTraceEnabled(PERF_POLICYENGINE_REQUEST_LOG)) {
            perf = RangerPerfTracer.getPerfTracer(PERF_POLICYENGINE_REQUEST_LOG, "RangerPolicyEngine.isAccessAllowed(user=" + user + "," + userGroups + ",accessType=" + accessType + ")");
        }

        for (RangerPolicyEvaluator evaluator : policyEngine.getPolicyRepository().getPolicyEvaluators()) {
            ret = evaluator.isAccessAllowed(resources, user, userGroups, accessType);

            if (ret) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Access granted by policy:[" + evaluator.getPolicy() + "]");
                }
                break;
            }
        }

        RangerPerfTracer.log(perf);

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== RangerPolicyAdminImpl.isAccessAllowedByUnzonedPolicies(" + resources + ", " + user + ", " + userGroups + ", " + accessType + "): " + ret);
        }

        return ret;
    }

    // This API is used only by test-code; checks only policies within default security-zone
    @Override
    public List<RangerPolicy> getAllowedUnzonedPolicies(String user, Set<String> userGroups, String accessType) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> RangerPolicyAdminImpl.getAllowedByUnzonedPolicies(" + user + ", " + userGroups + ", " + accessType + ")");
        }

        List<RangerPolicy> ret = new ArrayList<>();

        // TODO: run through evaluator in tagPolicyRepository as well
        for (RangerPolicyEvaluator evaluator : policyEngine.getPolicyRepository().getPolicyEvaluators()) {
            RangerPolicy policy = evaluator.getPolicy();

            boolean isAccessAllowed = isAccessAllowedByUnzonedPolicies(policy.getResources(), user, userGroups, accessType);

            if (isAccessAllowed) {
                ret.add(policy);
            }
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== RangerPolicyAdminImpl.getAllowedByUnzonedPolicies(" + user + ", " + userGroups + ", " + accessType + "): policyCount=" + ret.size());
        }

        return ret;
    }

    void releaseResources(boolean isForced) {
        if (policyEngine != null) {
            policyEngine.preCleanup(isForced);
        }
    }

    private List<RangerPolicy> getMatchingPolicies(RangerAccessResource resource, String accessType) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> RangerPolicyAdminImpl.getMatchingPolicies(" + resource + ", " + accessType + ")");
        }

        List<RangerPolicy>      ret     = new ArrayList<>();
        RangerAccessRequestImpl request = new RangerAccessRequestImpl(resource, accessType, null, null, null);

        requestProcessor.preProcess(request);

        Set<String> zoneNames = policyEngine.getMatchedZonesForResourceAndChildren(resource);

        if (CollectionUtils.isEmpty(zoneNames)) {
            getMatchingPoliciesForZone(request, null, ret);
        } else {
            for (String zoneName : zoneNames) {
                getMatchingPoliciesForZone(request, zoneName, ret);
            }
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== RangerPolicyAdminImpl.getMatchingPolicies(" + resource + ", " + accessType + ") : " + ret.size());
        }

        return ret;
    }

    private void getMatchingPoliciesForZone(RangerAccessRequest request, String zoneName, List<RangerPolicy> ret) {
        final RangerPolicyRepository matchedRepository = policyEngine.getRepositoryForZone(zoneName);

        if (matchedRepository != null) {
            if (policyEngine.hasTagPolicies(policyEngine.getTagPolicyRepository())) {
                Set<RangerTagForEval> tags = RangerAccessRequestUtil.getRequestTagsFromContext(request.getContext());

                if (CollectionUtils.isNotEmpty(tags)) {
                    final boolean useTagPoliciesFromDefaultZone = !policyEngine.isResourceZoneAssociatedWithTagService(zoneName);

                    for (RangerTagForEval tag : tags) {
                        RangerAccessResource        tagResource      = new RangerTagResource(tag.getType(), policyEngine.getTagPolicyRepository().getServiceDef());
                        List<RangerPolicyEvaluator> likelyEvaluators = policyEngine.getTagPolicyRepository().getLikelyMatchPolicyEvaluators(tagResource);

                        for (RangerPolicyEvaluator evaluator : likelyEvaluators) {
                            String policyZoneName = evaluator.getPolicy().getZoneName();

                            if (useTagPoliciesFromDefaultZone) {
                                if (StringUtils.isNotEmpty(policyZoneName)) {
                                    if (LOG.isDebugEnabled()) {
                                        LOG.debug("Tag policy [zone:" + policyZoneName + "] does not belong to default zone. Not evaluating this policy:[" + evaluator.getPolicy() + "]");
                                    }

                                    continue;
                                }
                            } else {
                                if (!StringUtils.equals(zoneName, policyZoneName)) {
                                    if (LOG.isDebugEnabled()) {
                                        LOG.debug("Tag policy [zone:" + policyZoneName + "] does not belong to the zone:[" + zoneName + "] of the accessed resource. Not evaluating this policy:[" + evaluator.getPolicy() + "]");
                                    }

                                    continue;
                                }
                            }

                            RangerPolicyResourceMatcher matcher = evaluator.getPolicyResourceMatcher();

                            if (matcher != null &&
                                    (request.isAccessTypeAny() ? matcher.isMatch(tagResource, RangerPolicyResourceMatcher.MatchScope.ANY, null) : matcher.isMatch(tagResource, null))) {
                                ret.add(evaluator.getPolicy());
                            }
                        }

                    }
                }
            }


            if (policyEngine.hasResourcePolicies(matchedRepository)) {
                List<RangerPolicyEvaluator> likelyEvaluators = matchedRepository.getLikelyMatchPolicyEvaluators(request.getResource());

                for (RangerPolicyEvaluator evaluator : likelyEvaluators) {
                    RangerPolicyResourceMatcher matcher = evaluator.getPolicyResourceMatcher();

                    if (matcher != null &&
                            (request.isAccessTypeAny() ? matcher.isMatch(request.getResource(), RangerPolicyResourceMatcher.MatchScope.ANY, null) : matcher.isMatch(request.getResource(), null))) {
                        ret.add(evaluator.getPolicy());
                    }
                }
            }

        }
    }
}

