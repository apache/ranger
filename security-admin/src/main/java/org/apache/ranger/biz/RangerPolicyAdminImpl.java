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
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ranger.plugin.contextenricher.RangerTagForEval;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyResource;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.policyengine.PolicyEngine;
import org.apache.ranger.plugin.policyengine.RangerAccessRequest;
import org.apache.ranger.plugin.policyengine.RangerAccessRequestImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessRequestProcessor;
import org.apache.ranger.plugin.policyengine.RangerAccessResource;
import org.apache.ranger.plugin.policyengine.RangerPluginContext;
import org.apache.ranger.plugin.policyengine.RangerPolicyEngine;
import org.apache.ranger.plugin.policyengine.RangerPolicyRepository;
import org.apache.ranger.plugin.policyengine.RangerTagAccessRequest;
import org.apache.ranger.plugin.policyengine.RangerTagResource;
import org.apache.ranger.plugin.policyevaluator.RangerPolicyEvaluator;
import org.apache.ranger.plugin.policyresourcematcher.RangerPolicyResourceMatcher;
import org.apache.ranger.plugin.resourcematcher.RangerAbstractResourceMatcher;
import org.apache.ranger.plugin.service.RangerDefaultRequestProcessor;
import org.apache.ranger.plugin.store.ServiceStore;
import org.apache.ranger.plugin.util.GrantRevokeRequest;
import org.apache.ranger.plugin.util.RangerAccessRequestUtil;
import org.apache.ranger.plugin.util.RangerPerfTracer;
import org.apache.ranger.plugin.util.RangerReadWriteLock;
import org.apache.ranger.plugin.util.RangerRoles;
import org.apache.ranger.plugin.util.ServiceDefUtil;
import org.apache.ranger.plugin.util.ServicePolicies;
import org.apache.ranger.plugin.util.StringTokenReplacer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class RangerPolicyAdminImpl implements RangerPolicyAdmin {
    private static final Log LOG = LogFactory.getLog(RangerPolicyAdminImpl.class);

    private static final Log PERF_POLICYENGINE_REQUEST_LOG = RangerPerfTracer.getPerfLogger("policyengine.request");

    private final PolicyEngine                 policyEngine;
    private final RangerAccessRequestProcessor requestProcessor;
    private final static Map<String, Object>          wildcardEvalContext = new HashMap<String, Object>() {
        @Override
        public Object get(Object key) { return RangerAbstractResourceMatcher.WILDCARD_ASTERISK; }
    };
    private       ServiceDBStore               serviceDBStore;

    static {
        wildcardEvalContext.put(RangerAbstractResourceMatcher.WILDCARD_ASTERISK, RangerAbstractResourceMatcher.WILDCARD_ASTERISK);
    }

    static public RangerPolicyAdmin getPolicyAdmin(final RangerPolicyAdminImpl other, final ServicePolicies servicePolicies) {
        RangerPolicyAdmin ret = null;

        if (other != null && servicePolicies != null) {
            PolicyEngine policyEngine = other.policyEngine.cloneWithDelta(servicePolicies);

            if (policyEngine != null) {
                if (policyEngine == other.policyEngine) {
                    ret = other;
                } else {
                    ret = new RangerPolicyAdminImpl(policyEngine);
                }
            }
        }

        return ret;
    }

    RangerPolicyAdminImpl(ServicePolicies servicePolicies, RangerPluginContext pluginContext, RangerRoles roles) {
        this.policyEngine     = new PolicyEngine(servicePolicies, pluginContext, roles, ServiceDBStore.SUPPORTS_IN_PLACE_POLICY_UPDATES);
        this.requestProcessor = new RangerDefaultRequestProcessor(policyEngine);
    }

    private RangerPolicyAdminImpl(final PolicyEngine policyEngine) {
        this.policyEngine     = policyEngine;
        this.requestProcessor = new RangerDefaultRequestProcessor(policyEngine);
    }

    @Override
    public void setServiceStore(ServiceStore svcStore) {
        if (svcStore instanceof ServiceDBStore) {
            this.serviceDBStore = (ServiceDBStore) svcStore;
        }
    }

    @Override
    public boolean isDelegatedAdminAccessAllowed(RangerAccessResource resource, String zoneName, String user, Set<String> userGroups, Set<String> accessTypes) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> RangerPolicyAdminImpl.isDelegatedAdminAccessAllowed(" + resource + ", " + zoneName + ", " + user + ", " + userGroups + ", " + accessTypes + ")");
        }

        boolean          ret  = false;
        RangerPerfTracer perf = null;

        if(RangerPerfTracer.isPerfTraceEnabled(PERF_POLICYENGINE_REQUEST_LOG)) {
            perf = RangerPerfTracer.getPerfTracer(PERF_POLICYENGINE_REQUEST_LOG, "RangerPolicyAdminImpl.isDelegatedAdminAccessAllowed(user=" + user + ",accessTypes=" + accessTypes + "resource=" + resource.getAsString() + ")");
        }

        try (RangerReadWriteLock.RangerLock readLock = policyEngine.getReadLock()) {

            if (LOG.isDebugEnabled()) {
                if (readLock.isLockingEnabled()) {
                    LOG.debug("Acquired lock - " + readLock);
                }
            }

            final RangerPolicyRepository matchedRepository = policyEngine.getRepositoryForZone(zoneName);

            if (matchedRepository != null) {
                Set<String> roles = getRolesFromUserAndGroups(user, userGroups);
                Set<String> requestedAccesses = new HashSet<>(accessTypes);

                RangerAccessRequestImpl request = new RangerAccessRequestImpl();
                request.setResource(resource);

                for (RangerPolicyEvaluator evaluator : matchedRepository.getLikelyMatchPolicyEvaluators(request, RangerPolicy.POLICY_TYPE_ACCESS)) {

                    Set<String> allowedAccesses = evaluator.getAllowedAccesses(resource, user, userGroups, roles, requestedAccesses);
                    if (CollectionUtils.isNotEmpty(allowedAccesses)) {
                        requestedAccesses.removeAll(allowedAccesses);
                        if (CollectionUtils.isEmpty(requestedAccesses)) {
                            if (LOG.isDebugEnabled()) {
                                LOG.debug("Access granted by policy:[" + evaluator.getPolicy() + "]");
                            }
                            ret = true;
                            break;
                        }
                    }
                }

            }

        }
        RangerPerfTracer.log(perf);

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== RangerPolicyAdminImpl.isDelegatedAdminAccessAllowed(" + resource + ", " + zoneName + ", " + user + ", " + userGroups + ", " + accessTypes + "): " + ret);
        }

        return ret;
    }

    @Override
    public boolean isDelegatedAdminAccessAllowedForRead(RangerPolicy policy, String user, Set<String> userGroups, Set<String> roles, Map<String, Object> evalContext) {
        return isDelegatedAdminAccessAllowed(policy, user, userGroups, roles, true, evalContext);
    }

    @Override
    public boolean isDelegatedAdminAccessAllowedForModify(RangerPolicy policy, String user, Set<String> userGroups, Set<String> roles, Map<String, Object> evalContext) {
        boolean ret = isDelegatedAdminAccessAllowed(policy, user, userGroups, roles, false, evalContext);
        if (ret) {
            // Get old policy from policy-engine
            RangerPolicy oldPolicy = null;
            if (policy.getId() != null) {
                try {
                    oldPolicy = serviceDBStore.getPolicy(policy.getId());
                } catch (Exception e) {
                    // Ignore
                }
            }
            if (oldPolicy != null) {
                ret = isDelegatedAdminAccessAllowed(oldPolicy, user, userGroups, roles, false, evalContext);
            }
        }
        return ret;
    }

    boolean isDelegatedAdminAccessAllowed(RangerPolicy policy, String user, Set<String> userGroups, Set<String> roles, boolean isRead, Map<String, Object> evalContext) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> RangerPolicyAdminImpl.isDelegatedAdminAccessAllowed(" + policy.getId() + ", " + user + ", " + userGroups + ", " + roles + ", " + isRead + ", " + evalContext + ")");
        }

        boolean          ret  = false;
        RangerPerfTracer perf = null;

        if(RangerPerfTracer.isPerfTraceEnabled(PERF_POLICYENGINE_REQUEST_LOG)) {
            perf = RangerPerfTracer.getPerfTracer(PERF_POLICYENGINE_REQUEST_LOG, "RangerPolicyEngine.isDelegatedAdminAccessAllowed(user=" + user + "," + userGroups + ", roles=" + roles + ")");
        }

        try (RangerReadWriteLock.RangerLock readLock = policyEngine.getReadLock()) {

            if (LOG.isDebugEnabled()) {
                if (readLock.isLockingEnabled()) {
                    LOG.debug("Acquired lock - " + readLock);
                }
            }

            final RangerPolicyRepository matchedRepository = policyEngine.getRepositoryForMatchedZone(policy);

            if (matchedRepository != null) {
                // RANGER-3082
                // Convert policy resources to by substituting macros with ASTERISK
                Map<String, RangerPolicyResource> modifiedPolicyResources = getPolicyResourcesWithMacrosReplaced(policy.getResources(), wildcardEvalContext);
                Set<String> accessTypes = getAllAccessTypes(policy, getServiceDef());

                if (LOG.isDebugEnabled()) {
                    LOG.debug("Checking admin-access for the access-types:[" + accessTypes + "]");
                }

                for (RangerPolicyEvaluator evaluator : matchedRepository.getPolicyEvaluators()) {
                    Set<String> allowedAccesses = evaluator.getAllowedAccesses(modifiedPolicyResources, user, userGroups, roles, accessTypes, evalContext);

                    if (allowedAccesses == null) {
                        continue;
                    }

                    boolean isAllowedAccessesModified = accessTypes.removeAll(allowedAccesses);

                    if (isRead && isAllowedAccessesModified) {
                        ret = true;
                        break;
                    }

                    if (CollectionUtils.isEmpty(accessTypes)) {
                        ret = true;
                        break;
                    }
                }
                if (!ret && CollectionUtils.isNotEmpty(accessTypes)) {
                    LOG.info("Accesses : " + accessTypes + " are not authorized for the policy:[" + policy.getId() + "] by any of delegated-admin policies");
                }

            }

        }

        RangerPerfTracer.log(perf);

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== RangerPolicyAdminImpl.isDelegatedAdminAccessAllowed(" + policy.getId() + ", " + user + ", " + userGroups + ", " + roles + ", " + isRead + ", " + evalContext + "): " + ret);
        }

        return ret;
    }

    @Override
    public List<RangerPolicy> getExactMatchPolicies(RangerAccessResource resource, String zoneName, Map<String, Object> evalContext) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> RangerPolicyAdminImpl.getExactMatchPolicies(" + resource + ", " + zoneName + ", " + evalContext  + ")");
        }

        List<RangerPolicy>     ret              = null;

        try (RangerReadWriteLock.RangerLock readLock = policyEngine.getReadLock()) {
            if (LOG.isDebugEnabled()) {
                if (readLock.isLockingEnabled()) {
                    LOG.debug("Acquired lock - " + readLock);
                }
            }

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

        try (RangerReadWriteLock.RangerLock readLock = policyEngine.getReadLock()) {

            if (LOG.isDebugEnabled()) {
                if (readLock.isLockingEnabled()) {
                    LOG.debug("Acquired lock - " + readLock);
                }
            }

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
        List<RangerPolicy> ret;

        try (RangerReadWriteLock.RangerLock readLock = policyEngine.getReadLock()) {
            if (LOG.isDebugEnabled()) {
                if (readLock.isLockingEnabled()) {
                    LOG.debug("Acquired lock - " + readLock);
                }
            }
            ret = getMatchingPolicies(resource, RangerPolicyEngine.ANY_ACCESS);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== RangerPolicyAdminImpl.getMatchingPolicies(" + resource + ") : " + ret.size());
        }

        return ret;
    }

    @Override
    public long getPolicyVersion() {
        long ret;

        try (RangerReadWriteLock.RangerLock readLock = policyEngine.getReadLock()) {
            if (LOG.isDebugEnabled()) {
                if (readLock.isLockingEnabled()) {
                    LOG.debug("Acquired lock - " + readLock);
                }
            }
            ret = policyEngine.getPolicyVersion();
        }
        return ret;
    }

    @Override
    public long getRoleVersion() {
        long ret;
        try (RangerReadWriteLock.RangerLock readLock = policyEngine.getReadLock()) {
            if (LOG.isDebugEnabled()) {
                if (readLock.isLockingEnabled()) {
                    LOG.debug("Acquired lock - " + readLock);
                }
            }
            ret = policyEngine.getRoleVersion();
        }
        return ret;
    }

    @Override
    public String getServiceName() {
        String ret;

        try (RangerReadWriteLock.RangerLock readLock = policyEngine.getReadLock()) {
            if (LOG.isDebugEnabled()) {
                if (readLock.isLockingEnabled()) {
                    LOG.debug("Acquired lock - " + readLock);
                }
            }
            ret = policyEngine.getServiceName();
        }
        return ret;
    }


    @Override
    public RangerServiceDef getServiceDef() {
        RangerServiceDef ret;
        try (RangerReadWriteLock.RangerLock readLock = policyEngine.getReadLock()) {
            if (LOG.isDebugEnabled()) {
                if (readLock.isLockingEnabled()) {
                    LOG.debug("Acquired lock - " + readLock);
                }
            }
            ret = policyEngine.getServiceDef();
        }
        return ret;
    }
    @Override
    public void setRoles(RangerRoles roles) {
        try (RangerReadWriteLock.RangerLock writeLock = policyEngine.getWriteLock()) {
            if (LOG.isDebugEnabled()) {
                if (writeLock.isLockingEnabled()) {
                    LOG.debug("Acquired lock - " + writeLock);
                }
            }
            policyEngine.setRoles(roles);
        }
    }

    @Override
    public Set<String> getRolesFromUserAndGroups(String user, Set<String> groups) {
        Set<String> ret;
        try (RangerReadWriteLock.RangerLock readLock = policyEngine.getReadLock()) {
            if (LOG.isDebugEnabled()) {
                if (readLock.isLockingEnabled()) {
                    LOG.debug("Acquired lock - " + readLock);
                }
            }
            ret = policyEngine.getPluginContext().getAuthContext().getRolesForUserAndGroups(user, groups);
        }
        return ret;
    }

    @Override
    public String getUniquelyMatchedZoneName(GrantRevokeRequest grantRevokeRequest) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> RangerPolicyAdminImpl.getUniquelyMatchedZoneName(" + grantRevokeRequest + ")");
        }
        String ret;

        try (RangerReadWriteLock.RangerLock readLock = policyEngine.getReadLock()) {
            if (LOG.isDebugEnabled()) {
                if (readLock.isLockingEnabled()) {
                    LOG.debug("Acquired lock - " + readLock);
                }
            }
            ret = policyEngine.getUniquelyMatchedZoneName(grantRevokeRequest.getResource());
        }

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
                        RangerAccessRequest         tagRequest       = new RangerTagAccessRequest(tag, policyEngine.getTagPolicyRepository().getServiceDef(), request);
                        List<RangerPolicyEvaluator> likelyEvaluators = policyEngine.getTagPolicyRepository().getLikelyMatchPolicyEvaluators(tagRequest);

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
                List<RangerPolicyEvaluator> likelyEvaluators = matchedRepository.getLikelyMatchPolicyEvaluators(request);

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

    private Map<String, RangerPolicyResource> getPolicyResourcesWithMacrosReplaced(Map<String, RangerPolicyResource> resources, Map<String, Object> evalContext) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> RangerPolicyAdminImpl.getPolicyResourcesWithMacrosReplaced(" + resources  + ", " + evalContext + ")");
        }

        final Map<String, RangerPolicyResource>  ret;

        Collection<String> resourceKeys = resources == null ? null : resources.keySet();

        if (CollectionUtils.isNotEmpty(resourceKeys)) {
            ret = new HashMap<>();

            for (String resourceName : resourceKeys) {
                RangerPolicyResource resourceValues = resources.get(resourceName);
                List<String>         values         = resourceValues == null ? null : resourceValues.getValues();

                if (CollectionUtils.isNotEmpty(values)) {
                    StringTokenReplacer tokenReplacer = policyEngine.getStringTokenReplacer(resourceName);

                    if (tokenReplacer != null) {
                        List<String> modifiedValues = new ArrayList<>();

                        for (String value : values) {
                            // RANGER-3082 - replace macros in value with ASTERISK
                            String modifiedValue = tokenReplacer.replaceTokens(value, evalContext);
                            modifiedValues.add(modifiedValue);
                        }

                        RangerPolicyResource modifiedPolicyResource = new RangerPolicyResource(modifiedValues, resourceValues.getIsExcludes(), resourceValues.getIsRecursive());
                        ret.put(resourceName, modifiedPolicyResource);
                    } else {
                        ret.put(resourceName, resourceValues);
                    }
                } else {
                    ret.put(resourceName, resourceValues);
                }
            }
        } else {
            ret = resources;
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== RangerPolicyEngineImpl.getPolicyResourcesWithMacrosReplaced(" + resources  + ", " + evalContext + "): " + ret);
        }

        return ret;
    }

    private Set<String> getAllAccessTypes(RangerPolicy policy, RangerServiceDef serviceDef) {
        Set<String> ret = new HashSet<>();

        Map<String, Collection<String>> expandedAccesses = ServiceDefUtil.getExpandedImpliedGrants(serviceDef);

        if (MapUtils.isNotEmpty(expandedAccesses)) {

            Integer policyType = policy.getPolicyType() == null ? RangerPolicy.POLICY_TYPE_ACCESS : policy.getPolicyType();

            if (policyType == RangerPolicy.POLICY_TYPE_ACCESS) {
                for (RangerPolicy.RangerPolicyItem item : policy.getPolicyItems()) {
                    List<RangerPolicy.RangerPolicyItemAccess> accesses = item.getAccesses();
                    for (RangerPolicy.RangerPolicyItemAccess access : accesses) {
                        ret.addAll(expandedAccesses.get(access.getType()));
                    }
                }
                for (RangerPolicy.RangerPolicyItem item : policy.getDenyPolicyItems()) {
                    List<RangerPolicy.RangerPolicyItemAccess> accesses = item.getAccesses();
                    for (RangerPolicy.RangerPolicyItemAccess access : accesses) {
                        ret.addAll(expandedAccesses.get(access.getType()));
                    }
                }
                for (RangerPolicy.RangerPolicyItem item : policy.getAllowExceptions()) {
                    List<RangerPolicy.RangerPolicyItemAccess> accesses = item.getAccesses();
                    for (RangerPolicy.RangerPolicyItemAccess access : accesses) {
                        ret.addAll(expandedAccesses.get(access.getType()));
                    }
                }
                for (RangerPolicy.RangerPolicyItem item : policy.getDenyExceptions()) {
                    List<RangerPolicy.RangerPolicyItemAccess> accesses = item.getAccesses();
                    for (RangerPolicy.RangerPolicyItemAccess access : accesses) {
                        ret.addAll(expandedAccesses.get(access.getType()));
                    }
                }
            } else if (policyType == RangerPolicy.POLICY_TYPE_DATAMASK) {
                for (RangerPolicy.RangerPolicyItem item : policy.getDataMaskPolicyItems()) {
                    List<RangerPolicy.RangerPolicyItemAccess> accesses = item.getAccesses();
                    for (RangerPolicy.RangerPolicyItemAccess access : accesses) {
                        ret.addAll(expandedAccesses.get(access.getType()));
                    }
                }
            } else if (policyType == RangerPolicy.POLICY_TYPE_ROWFILTER) {
                for (RangerPolicy.RangerPolicyItem item : policy.getRowFilterPolicyItems()) {
                    List<RangerPolicy.RangerPolicyItemAccess> accesses = item.getAccesses();
                    for (RangerPolicy.RangerPolicyItemAccess access : accesses) {
                        ret.addAll(expandedAccesses.get(access.getType()));
                    }
                }
            } else {
                LOG.error("Unknown policy-type :[" + policyType + "], returning empty access-type set");
            }
        }
        return ret;
    }

}

