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

package org.apache.ranger.plugin.policyengine;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.ListUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ranger.plugin.contextenricher.RangerContextEnricher;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerPolicyDelta;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.model.validation.RangerZoneResourceMatcher;
import org.apache.ranger.plugin.policyevaluator.RangerPolicyEvaluator;
import org.apache.ranger.plugin.policyresourcematcher.RangerPolicyResourceMatcher;
import org.apache.ranger.plugin.service.RangerAuthContext;
import org.apache.ranger.plugin.store.EmbeddedServiceDefsUtil;
import org.apache.ranger.plugin.util.RangerPerfTracer;
import org.apache.ranger.plugin.util.RangerPolicyDeltaUtil;
import org.apache.ranger.plugin.util.RangerRoles;
import org.apache.ranger.plugin.util.ServicePolicies;

public class PolicyEngine {
    private static final Log LOG = LogFactory.getLog(PolicyEngine.class);

    private static final Log PERF_POLICYENGINE_INIT_LOG       = RangerPerfTracer.getPerfLogger("policyengine.init");
    private static final Log PERF_POLICYENGINE_REBALANCE_LOG  = RangerPerfTracer.getPerfLogger("policyengine.rebalance");

    private final RangerPolicyRepository              policyRepository;
    private final RangerPolicyRepository              tagPolicyRepository;
    private final List<RangerContextEnricher>         allContextEnrichers;
    private final RangerPluginContext                 pluginContext;
    private final Map<String, RangerPolicyRepository> zonePolicyRepositories = new HashMap<>();
    private final Map<String, RangerResourceTrie>     resourceZoneTrie = new HashMap<>();
    private final Map<String, String>                 zoneTagServiceMap = new HashMap<>();
    private       boolean                             useForwardedIPAddress;
    private       String[]                            trustedProxyAddresses;

    public boolean getUseForwardedIPAddress() {
        return useForwardedIPAddress;
    }

    public void setUseForwardedIPAddress(boolean useForwardedIPAddress) {
        this.useForwardedIPAddress = useForwardedIPAddress;
    }

    public String[] getTrustedProxyAddresses() {
        return trustedProxyAddresses;
    }

    public void setTrustedProxyAddresses(String[] trustedProxyAddresses) {
        this.trustedProxyAddresses = trustedProxyAddresses;
    }

    public long getRoleVersion() { return this.pluginContext.getAuthContext().getRoleVersion(); }

    public void setRoles(RangerRoles roles) { this.pluginContext.getAuthContext().setRoles(roles); }

    public String getServiceName() {
        return policyRepository.getServiceName();
    }

    public RangerServiceDef getServiceDef() {
        return policyRepository.getServiceDef();
    }

    public long getPolicyVersion() {
        return policyRepository.getPolicyVersion();
    }

    public RangerPolicyRepository getPolicyRepository() {
        return policyRepository;
    }

    public RangerPolicyRepository getTagPolicyRepository() {
        return tagPolicyRepository;
    }

    public Map<String, RangerPolicyRepository> getZonePolicyRepositories() { return zonePolicyRepositories; }

    public List<RangerContextEnricher> getAllContextEnrichers() { return allContextEnrichers; }

    public RangerPluginContext getPluginContext() { return pluginContext; }

    @Override
    public String toString() {
        return toString(new StringBuilder()).toString();
    }

    @Override
    protected void finalize() throws Throwable {
        try {
            cleanup();
        } finally {
            super.finalize();
        }
    }

    public StringBuilder toString(StringBuilder sb) {
        if (sb == null) {
            sb = new StringBuilder();
        }

        sb.append("PolicyEngine={");

        sb.append("serviceName={").append(this.getServiceName()).append("} ");

        sb.append("policyRepository={");
        if (policyRepository != null) {
            policyRepository.toString(sb);
        }
        sb.append("} ");

        sb.append("tagPolicyRepository={");
        if (tagPolicyRepository != null) {
            tagPolicyRepository.toString(sb);
        }
        sb.append("} ");

        sb.append("}");

        return sb;
    }

    public List<RangerPolicy> getResourcePolicies(String zoneName) {
        RangerPolicyRepository zoneResourceRepository = zonePolicyRepositories.get(zoneName);

        return zoneResourceRepository == null ? ListUtils.EMPTY_LIST : zoneResourceRepository.getPolicies();
    }

    Map<String, RangerResourceTrie> getResourceZoneTrie() {
        return resourceZoneTrie;
    }

    public PolicyEngine(ServicePolicies servicePolicies, RangerPluginContext pluginContext, RangerRoles roles) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> PolicyEngine(" + ", " + servicePolicies + ", " + pluginContext + ")");
        }

        RangerPerfTracer perf = null;

        if(RangerPerfTracer.isPerfTraceEnabled(PERF_POLICYENGINE_INIT_LOG)) {
            perf = RangerPerfTracer.getPerfTracer(PERF_POLICYENGINE_INIT_LOG, "RangerPolicyEngine.init(hashCode=" + Integer.toHexString(System.identityHashCode(this)) + ")");

            long freeMemory  = Runtime.getRuntime().freeMemory();
            long totalMemory = Runtime.getRuntime().totalMemory();

            PERF_POLICYENGINE_INIT_LOG.debug("In-Use memory: " + (totalMemory - freeMemory) + ", Free memory:" + freeMemory);
        }

        this.pluginContext = pluginContext;

        this.pluginContext.setAuthContext(new RangerAuthContext(null, roles));

        RangerPolicyEngineOptions options = pluginContext.getConfig().getPolicyEngineOptions();

        if(StringUtils.isBlank(options.evaluatorType) || StringUtils.equalsIgnoreCase(options.evaluatorType, RangerPolicyEvaluator.EVALUATOR_TYPE_AUTO)) {
            options.evaluatorType = RangerPolicyEvaluator.EVALUATOR_TYPE_OPTIMIZED;
        }

        policyRepository = new RangerPolicyRepository(servicePolicies, this.pluginContext);

        ServicePolicies.TagPolicies tagPolicies = servicePolicies.getTagPolicies();

        if (!options.disableTagPolicyEvaluation
                && tagPolicies != null
                && !StringUtils.isEmpty(tagPolicies.getServiceName())
                && tagPolicies.getServiceDef() != null
                && !CollectionUtils.isEmpty(tagPolicies.getPolicies())) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("PolicyEngine : Building tag-policy-repository for tag-service " + tagPolicies.getServiceName());
            }

            tagPolicyRepository = new RangerPolicyRepository(tagPolicies, this.pluginContext, servicePolicies.getServiceDef(), servicePolicies.getServiceName());
        } else {
            if (LOG.isDebugEnabled()) {
                LOG.debug("PolicyEngine : No tag-policy-repository for service " + servicePolicies.getServiceName());
            }

            tagPolicyRepository = null;
        }

        List<RangerContextEnricher> tmpList;
        List<RangerContextEnricher> tagContextEnrichers      = tagPolicyRepository == null ? null :tagPolicyRepository.getContextEnrichers();
        List<RangerContextEnricher> resourceContextEnrichers = policyRepository.getContextEnrichers();

        if (CollectionUtils.isEmpty(tagContextEnrichers)) {
            tmpList = resourceContextEnrichers;
        } else if (CollectionUtils.isEmpty(resourceContextEnrichers)) {
            tmpList = tagContextEnrichers;
        } else {
            tmpList = new ArrayList<>(tagContextEnrichers);

            tmpList.addAll(resourceContextEnrichers);
        }

        this.allContextEnrichers = tmpList;

        if (MapUtils.isNotEmpty(servicePolicies.getSecurityZones())) {
            buildZoneTrie(servicePolicies);

            for (Map.Entry<String, ServicePolicies.SecurityZoneInfo> zone : servicePolicies.getSecurityZones().entrySet()) {
                RangerPolicyRepository policyRepository = new RangerPolicyRepository(servicePolicies, this.pluginContext, zone.getKey());

                zonePolicyRepositories.put(zone.getKey(), policyRepository);
            }
        }

        RangerPerfTracer.log(perf);

        if (PERF_POLICYENGINE_INIT_LOG.isDebugEnabled()) {
            long freeMemory  = Runtime.getRuntime().freeMemory();
            long totalMemory = Runtime.getRuntime().totalMemory();

            PERF_POLICYENGINE_INIT_LOG.debug("In-Use memory: " + (totalMemory - freeMemory) + ", Free memory:" + freeMemory);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== PolicyEngine()");
        }
    }

    public PolicyEngine cloneWithDelta(ServicePolicies servicePolicies) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> cloneWithDelta(" + Arrays.toString(servicePolicies.getPolicyDeltas().toArray()) + ", " + servicePolicies.getPolicyVersion() + ")");
        }

        final PolicyEngine ret;
        RangerPerfTracer   perf = null;

        if(RangerPerfTracer.isPerfTraceEnabled(PERF_POLICYENGINE_INIT_LOG)) {
            perf = RangerPerfTracer.getPerfTracer(PERF_POLICYENGINE_INIT_LOG, "RangerPolicyEngine.cloneWithDelta()");
        }

        RangerServiceDef serviceDef    = this.getServiceDef();
        String           serviceType   = (serviceDef != null) ? serviceDef.getName() : "";
        boolean          isValidDeltas = false;

        if (CollectionUtils.isNotEmpty(servicePolicies.getPolicyDeltas()) || MapUtils.isNotEmpty(servicePolicies.getSecurityZones())) {
            isValidDeltas = CollectionUtils.isEmpty(servicePolicies.getPolicyDeltas()) || RangerPolicyDeltaUtil.isValidDeltas(servicePolicies.getPolicyDeltas(), serviceType);

            if (isValidDeltas) {
                if (MapUtils.isNotEmpty(servicePolicies.getSecurityZones())) {
                    for (Map.Entry<String, ServicePolicies.SecurityZoneInfo> entry : servicePolicies.getSecurityZones().entrySet()) {
                        if (!RangerPolicyDeltaUtil.isValidDeltas(entry.getValue().getPolicyDeltas(), serviceType)) {
                            if (LOG.isDebugEnabled()) {
                                LOG.debug("Invalid policy-deltas for security zone:[" + entry.getKey() + "]");
                            }

                            isValidDeltas = false;
                            break;
                        }
                    }
                }
            }
        }

        if (isValidDeltas) {
            ret = new PolicyEngine(this, servicePolicies);
        } else {
            ret = null;
        }

        RangerPerfTracer.log(perf);

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== cloneWithDelta(" + Arrays.toString(servicePolicies.getPolicyDeltas().toArray()) + ", " + servicePolicies.getPolicyVersion() + ")");
        }
        return ret;
    }

    public RangerPolicyRepository getRepositoryForMatchedZone(RangerPolicy policy) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> PolicyEngine.getRepositoryForMatchedZone(" + policy + ")");
        }

        String                       zoneName = policy.getZoneName();
        final RangerPolicyRepository ret      = getRepositoryForZone(zoneName);

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== PolicyEngine.getRepositoryForMatchedZone(" + policy + ")");
        }

        return ret;
    }

    public Set<String> getMatchedZonesForResourceAndChildren(RangerAccessResource accessResource) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> PolicyEngine.getMatchedZonesForResourceAndChildren(" + accessResource + ")");
        }

        Set<String> ret = null;

        if (MapUtils.isNotEmpty(this.resourceZoneTrie)) {
            ret = getMatchedZonesForResourceAndChildren(accessResource.getAsMap(), accessResource);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== PolicyEngine.getMatchedZonesForResourceAndChildren(" + accessResource + ") : " + ret);
        }

        return ret;
    }

    public String getUniquelyMatchedZoneName(Map<String, ?> resourceAsMap) {
        String ret = null;
        Set<String> matchedZones = getMatchedZonesForResourceAndChildren(resourceAsMap, convertToAccessResource(resourceAsMap));
        if (CollectionUtils.isNotEmpty(matchedZones) && matchedZones.size() == 1) {
            String[] matchedZonesArray = new String[1];
            matchedZones.toArray(matchedZonesArray);
            ret = matchedZonesArray[0];
        }
        return ret;
    }

    public RangerPolicyRepository getRepositoryForZone(String zoneName) {
        final RangerPolicyRepository ret;

        if (LOG.isDebugEnabled()) {
            LOG.debug("zoneName:[" + zoneName + "]");
        }

        if (StringUtils.isNotEmpty(zoneName)) {
            ret = getZonePolicyRepositories().get(zoneName);
        } else {
            ret = getPolicyRepository();
        }

        if (ret == null) {
            LOG.error("policyRepository for zoneName:[" + zoneName + "],  serviceName:[" + getServiceName() + "], policyVersion:[" + getPolicyVersion() + "] is null!! ERROR!");
        }

        return ret;
    }

    public boolean hasTagPolicies(RangerPolicyRepository tagPolicyRepository) {
        return tagPolicyRepository != null && CollectionUtils.isNotEmpty(tagPolicyRepository.getPolicies());
    }

    public boolean hasResourcePolicies(RangerPolicyRepository policyRepository) {
        return policyRepository != null && CollectionUtils.isNotEmpty(policyRepository.getPolicies());
    }

    public boolean isResourceZoneAssociatedWithTagService(String resourceZoneName) {
        final boolean ret;

        if (StringUtils.isNotEmpty(resourceZoneName) && tagPolicyRepository != null && zoneTagServiceMap.get(resourceZoneName) != null) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Accessed resource is in a zone:[" + resourceZoneName + "] which is associated with the tag-service:[" + tagPolicyRepository.getServiceName() + "]");
            }

            ret = true;
        } else {
            ret = false;
        }

        return ret;
    }

    public void preCleanup(boolean isForced) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> PolicyEngine.preCleanup(isForced=" + isForced + ")");
        }

        if (policyRepository != null) {
            policyRepository.preCleanup(isForced);
        }

        if (tagPolicyRepository != null) {
            tagPolicyRepository.preCleanup(isForced);
        }

        if (MapUtils.isNotEmpty(this.zonePolicyRepositories)) {
            for (Map.Entry<String, RangerPolicyRepository> entry : this.zonePolicyRepositories.entrySet()) {
                entry.getValue().preCleanup(isForced);
            }
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== PolicyEngine.preCleanup(isForced=" + isForced + ")");
        }
    }

    private Set<String> getMatchedZonesForResourceAndChildren(Map<String, ?> resource, RangerAccessResource accessResource) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> PolicyEngine.getMatchedZonesForResourceAndChildren(" + resource + ", " + accessResource + ")");
        }

        Set<String> ret = null;

        if (MapUtils.isNotEmpty(this.resourceZoneTrie)) {
            List<Set<RangerZoneResourceMatcher>> zoneMatchersList = null;
            Set<RangerZoneResourceMatcher>       smallestList     = null;

            for (Map.Entry<String, ?> entry : resource.entrySet()) {
                String                                        resourceDefName = entry.getKey();
                Object                                        resourceValues  = entry.getValue();
                RangerResourceTrie<RangerZoneResourceMatcher> trie            = resourceZoneTrie.get(resourceDefName);

                if (trie == null) {
                    continue;
                }

                Set<RangerZoneResourceMatcher> matchedZones = trie.getEvaluatorsForResource(resourceValues);

                if (LOG.isDebugEnabled()) {
                    LOG.debug("ResourceDefName:[" + resourceDefName + "], values:[" + resourceValues + "], matched-zones:[" + matchedZones + "]");
                }

                if (CollectionUtils.isEmpty(matchedZones)) { // no policies for this resource, bail out
                    zoneMatchersList = null;
                    smallestList     = null;

                    break;
                }

                if (smallestList == null) {
                    smallestList = matchedZones;
                } else {
                    if (zoneMatchersList == null) {
                        zoneMatchersList = new ArrayList<>();

                        zoneMatchersList.add(smallestList);
                    }

                    zoneMatchersList.add(matchedZones);

                    if (smallestList.size() > matchedZones.size()) {
                        smallestList = matchedZones;
                    }
                }
            }
            if (smallestList != null) {
                final Set<RangerZoneResourceMatcher> intersection;

                if (zoneMatchersList != null) {
                    intersection = new HashSet<>(smallestList);

                    for (Set<RangerZoneResourceMatcher> zoneMatchers : zoneMatchersList) {
                        if (zoneMatchers != smallestList) {
                            // remove zones from intersection that are not in zoneMatchers
                            intersection.retainAll(zoneMatchers);

                            if (CollectionUtils.isEmpty(intersection)) { // if no zoneMatcher exists, bail out and return empty list
                                break;
                            }
                        }
                    }
                } else {
                    intersection = smallestList;
                }

                if (LOG.isDebugEnabled()) {
                    LOG.debug("Resource:[" + resource + "], matched-zones:[" + intersection + "]");
                }

                if (intersection.size() > 0) {
                    ret = new HashSet<>();

                    for (RangerZoneResourceMatcher zoneMatcher : intersection) {
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("Trying to match resource:[" + accessResource + "] using zoneMatcher:[" + zoneMatcher + "]");
                        }

                        // These are potential matches. Try to really match them
                        if (zoneMatcher.getPolicyResourceMatcher().isMatch(accessResource, RangerPolicyResourceMatcher.MatchScope.ANY, null)) {
                            if (LOG.isDebugEnabled()) {
                                LOG.debug("Matched resource:[" + accessResource + "] using zoneMatcher:[" + zoneMatcher + "]");
                            }

                            // Actual match happened
                            ret.add(zoneMatcher.getSecurityZoneName());
                        } else {
                            if (LOG.isDebugEnabled()) {
                                LOG.debug("Did not match resource:[" + accessResource + "] using zoneMatcher:[" + zoneMatcher + "]");
                            }
                        }
                    }

                    if (LOG.isDebugEnabled()) {
                        LOG.debug("The following zone-names matched resource:[" + accessResource + "]: " + ret);
                    }
                }
            }
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== PolicyEngine.getMatchedZonesForResourceAndChildren(" + resource + ", " + accessResource + ") : " + ret);
        }

        return ret;
    }

    private RangerAccessResource convertToAccessResource(Map<String, ?> resource) {
        RangerAccessResourceImpl ret = new RangerAccessResourceImpl();

        ret.setServiceDef(getServiceDef());

        for (Map.Entry<String, ?> entry : resource.entrySet()) {
            ret.setValue(entry.getKey(), entry.getValue());
        }

        return ret;
    }


    private PolicyEngine(final PolicyEngine other, ServicePolicies servicePolicies) {
        this.useForwardedIPAddress = other.useForwardedIPAddress;
        this.trustedProxyAddresses = other.trustedProxyAddresses;
        this.pluginContext         = other.pluginContext;

        long                    policyVersion                   = servicePolicies.getPolicyVersion() != null ? servicePolicies.getPolicyVersion() : -1L;
        List<RangerPolicyDelta> defaultZoneDeltas               = new ArrayList<>();
        List<RangerPolicyDelta> defaultZoneDeltasForTagPolicies = new ArrayList<>();

        if (MapUtils.isNotEmpty(servicePolicies.getSecurityZones())) {
            buildZoneTrie(servicePolicies);

            Map<String, List<RangerPolicyDelta>> zoneDeltasMap = new HashMap<>();

            for (Map.Entry<String, ServicePolicies.SecurityZoneInfo> zone : servicePolicies.getSecurityZones().entrySet()) {
                List<RangerPolicyDelta> deltas = zone.getValue().getPolicyDeltas();

                for (RangerPolicyDelta delta : deltas) {
                    String zoneName = delta.getZoneName();

                    if (StringUtils.isNotEmpty(zoneName)) {
                        List<RangerPolicyDelta> zoneDeltas = zoneDeltasMap.get(zoneName);

                        if (zoneDeltas == null) {
                            zoneDeltas = new ArrayList<>();
                            zoneDeltasMap.put(zoneName, zoneDeltas);
                        }

                        zoneDeltas.add(delta);
                    } else {
                        LOG.warn("policyDelta : [" + delta + "] does not belong to any zone. Should not have come here.");
                    }
                }
            }

            for (Map.Entry<String, List<RangerPolicyDelta>> entry : zoneDeltasMap.entrySet()) {
                final String                  zoneName        = entry.getKey();
                final List<RangerPolicyDelta> zoneDeltas      = entry.getValue();
                final RangerPolicyRepository  otherRepository = other.zonePolicyRepositories.get(zoneName);
                final RangerPolicyRepository  policyRepository;

                if (CollectionUtils.isNotEmpty(zoneDeltas)) {
                    if (otherRepository == null) {
                        List<RangerPolicy> policies = new ArrayList<>();

                        for (RangerPolicyDelta delta : zoneDeltas) {
                            if (delta.getChangeType() == RangerPolicyDelta.CHANGE_TYPE_POLICY_CREATE) {
                                policies.add(delta.getPolicy());
                            } else {
                                LOG.warn("Expected changeType:[" + RangerPolicyDelta.CHANGE_TYPE_POLICY_CREATE + "], found policy-change-delta:[" + delta +"]");
                            }
                        }

                        servicePolicies.getSecurityZones().get(zoneName).setPolicies(policies);

                        policyRepository = new RangerPolicyRepository(servicePolicies, this.pluginContext, zoneName);
                    } else {
                        policyRepository = new RangerPolicyRepository(otherRepository, zoneDeltas, policyVersion);
                    }
                } else {
                    policyRepository = shareWith(otherRepository);
                }

                zonePolicyRepositories.put(zoneName, policyRepository);
            }
        }

        List<RangerPolicyDelta> unzonedDeltas = servicePolicies.getPolicyDeltas();

        for (RangerPolicyDelta delta : unzonedDeltas) {
            if (servicePolicies.getServiceDef().getName().equals(delta.getServiceType())) {
                defaultZoneDeltas.add(delta);
            } else {
                defaultZoneDeltasForTagPolicies.add(delta);
            }
        }

        if (other.policyRepository != null && CollectionUtils.isNotEmpty(defaultZoneDeltas)) {
            this.policyRepository = new RangerPolicyRepository(other.policyRepository, defaultZoneDeltas, policyVersion);
        } else {
            this.policyRepository = shareWith(other.policyRepository);
        }

        if (servicePolicies.getTagPolicies() != null && CollectionUtils.isNotEmpty(defaultZoneDeltasForTagPolicies)) {
            if (other.tagPolicyRepository == null) {
                // Only creates are expected
                List<RangerPolicy> tagPolicies = new ArrayList<>();

                for (RangerPolicyDelta delta : defaultZoneDeltasForTagPolicies) {
                    if (delta.getChangeType() == RangerPolicyDelta.CHANGE_TYPE_POLICY_CREATE) {
                        tagPolicies.add(delta.getPolicy());
                    } else {
                        LOG.warn("Expected changeType:[" + RangerPolicyDelta.CHANGE_TYPE_POLICY_CREATE + "], found policy-change-delta:[" + delta + "]");
                    }
                }

                servicePolicies.getTagPolicies().setPolicies(tagPolicies);

                this.tagPolicyRepository = new RangerPolicyRepository(servicePolicies.getTagPolicies(), this.pluginContext, servicePolicies.getServiceDef(), servicePolicies.getServiceName());
            } else {
                this.tagPolicyRepository = new RangerPolicyRepository(other.tagPolicyRepository, defaultZoneDeltasForTagPolicies, policyVersion);
            }
        } else {
            this.tagPolicyRepository = shareWith(other.tagPolicyRepository);
        }

        List<RangerContextEnricher> tmpList;
        List<RangerContextEnricher> tagContextEnrichers      = tagPolicyRepository == null ? null :tagPolicyRepository.getContextEnrichers();
        List<RangerContextEnricher> resourceContextEnrichers = policyRepository.getContextEnrichers();

        if (CollectionUtils.isEmpty(tagContextEnrichers)) {
            tmpList = resourceContextEnrichers;
        } else if (CollectionUtils.isEmpty(resourceContextEnrichers)) {
            tmpList = tagContextEnrichers;
        } else {
            tmpList = new ArrayList<>(tagContextEnrichers);

            tmpList.addAll(resourceContextEnrichers);
        }

        this.allContextEnrichers = tmpList;

        reorderPolicyEvaluators();
    }

    private void buildZoneTrie(ServicePolicies servicePolicies) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> PolicyEngine.buildZoneTrie()");
        }

        Map<String, ServicePolicies.SecurityZoneInfo> securityZones = servicePolicies.getSecurityZones();

        if (MapUtils.isNotEmpty(securityZones)) {
            RangerServiceDef                serviceDef = servicePolicies.getServiceDef();
            List<RangerZoneResourceMatcher> matchers   = new ArrayList<>();

            for (Map.Entry<String, ServicePolicies.SecurityZoneInfo> securityZone : securityZones.entrySet()) {
                String                           zoneName    = securityZone.getKey();
                ServicePolicies.SecurityZoneInfo zoneDetails = securityZone.getValue();

                if (LOG.isDebugEnabled()) {
                    LOG.debug("Building matchers for zone:[" + zoneName +"]");
                }

                for (Map<String, List<String>> resource : zoneDetails.getResources()) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Building matcher for resource:[" + resource + "] in zone:[" + zoneName +"]");
                    }

                    Map<String, RangerPolicy.RangerPolicyResource> policyResources = new HashMap<>();

                    for (Map.Entry<String, List<String>> entry : resource.entrySet()) {
                        String                            resourceDefName = entry.getKey();
                        List<String>                      resourceValues  = entry.getValue();
                        RangerPolicy.RangerPolicyResource policyResource  = new RangerPolicy.RangerPolicyResource();
                        policyResource.setIsExcludes(false);
                        policyResource.setIsRecursive(EmbeddedServiceDefsUtil.isRecursiveEnabled(serviceDef, resourceDefName));
                        policyResource.setValues(resourceValues);
                        policyResources.put(resourceDefName, policyResource);
                    }

                    matchers.add(new RangerZoneResourceMatcher(zoneName, policyResources, serviceDef));

                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Built matcher for resource:[" + resource +"] in zone:[" + zoneName + "]");
                    }
                }

                if (LOG.isDebugEnabled()) {
                    LOG.debug("Built all matchers for zone:[" + zoneName +"]");
                }

                if (zoneDetails.getContainsAssociatedTagService()) {
                    zoneTagServiceMap.put(zoneName, zoneName);
                }
            }

            if (LOG.isDebugEnabled()) {
                LOG.debug("Built matchers for all Zones");
            }

            for (RangerServiceDef.RangerResourceDef resourceDef : serviceDef.getResources()) {
                resourceZoneTrie.put(resourceDef.getName(), new RangerResourceTrie<>(resourceDef, matchers));
            }
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== PolicyEngine.buildZoneTrie()");
        }
    }

    private RangerPolicyRepository shareWith(RangerPolicyRepository other) {
        if (other != null) {
            other.setShared();
        }

        return other;
    }
    private void reorderPolicyEvaluators() {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> reorderEvaluators()");
        }

        RangerPerfTracer perf = null;

        if(RangerPerfTracer.isPerfTraceEnabled(PERF_POLICYENGINE_REBALANCE_LOG)) {
            perf = RangerPerfTracer.getPerfTracer(PERF_POLICYENGINE_REBALANCE_LOG, "RangerPolicyEngine.reorderEvaluators()");
        }

        if (tagPolicyRepository != null) {
            tagPolicyRepository.reorderPolicyEvaluators();
        }
        if (policyRepository != null) {
            policyRepository.reorderPolicyEvaluators();
        }

        RangerPerfTracer.log(perf);

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== reorderEvaluators()");
        }
    }

    private void cleanup() {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> PolicyEngine.cleanup()");
        }

        RangerPerfTracer perf = null;

        if(RangerPerfTracer.isPerfTraceEnabled(PERF_POLICYENGINE_INIT_LOG)) {
            perf = RangerPerfTracer.getPerfTracer(PERF_POLICYENGINE_INIT_LOG, "RangerPolicyEngine.cleanUp(hashCode=" + Integer.toHexString(System.identityHashCode(this)) + ")");
        }

        preCleanup(false);

        if (policyRepository != null) {
            policyRepository.cleanup();
        }

        if (tagPolicyRepository != null) {
            tagPolicyRepository.cleanup();
        }

        if (MapUtils.isNotEmpty(this.zonePolicyRepositories)) {
            for (Map.Entry<String, RangerPolicyRepository> entry : this.zonePolicyRepositories.entrySet()) {
                entry.getValue().cleanup();
            }
        }

        RangerPerfTracer.log(perf);

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== PolicyEngine.cleanup()");
        }
    }
}

