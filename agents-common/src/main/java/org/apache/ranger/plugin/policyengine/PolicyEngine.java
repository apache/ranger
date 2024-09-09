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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.ranger.plugin.contextenricher.RangerContextEnricher;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerPolicyDelta;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.model.validation.RangerServiceDefHelper;
import org.apache.ranger.plugin.policyevaluator.RangerPolicyEvaluator;
import org.apache.ranger.plugin.resourcematcher.RangerAbstractResourceMatcher;
import org.apache.ranger.plugin.service.RangerAuthContext;
import org.apache.ranger.plugin.util.RangerPerfTracer;
import org.apache.ranger.plugin.util.RangerPolicyDeltaUtil;
import org.apache.ranger.plugin.util.RangerReadWriteLock;
import org.apache.ranger.plugin.util.RangerRoles;
import org.apache.ranger.plugin.util.ServiceDefUtil;
import org.apache.ranger.plugin.util.ServicePolicies;
import org.apache.ranger.plugin.util.StringTokenReplacer;
import org.apache.ranger.plugin.util.RangerUserStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PolicyEngine {
    private static final Logger LOG = LoggerFactory.getLogger(PolicyEngine.class);

    private static final Logger PERF_POLICYENGINE_INIT_LOG       = RangerPerfTracer.getPerfLogger("policyengine.init");
    private static final Logger PERF_POLICYENGINE_REBALANCE_LOG  = RangerPerfTracer.getPerfLogger("policyengine.rebalance");

    private final RangerServiceDefHelper              serviceDefHelper;
    private final RangerPolicyRepository              policyRepository;
    private final RangerPolicyRepository              tagPolicyRepository;
    private final List<RangerContextEnricher>         allContextEnrichers;
    private final RangerPluginContext                 pluginContext;
    private final Map<String, RangerPolicyRepository> zonePolicyRepositories = new HashMap<>();
    private final RangerSecurityZoneMatcher           zoneMatcher;
    private       boolean                             useForwardedIPAddress;
    private       String[]                            trustedProxyAddresses;
    private final Map<String, StringTokenReplacer>    tokenReplacers = new HashMap<>();
    private final RangerReadWriteLock                 lock;


    public RangerReadWriteLock.RangerLock getReadLock() {
        return lock.getReadLock();
    }

    public RangerReadWriteLock.RangerLock getWriteLock() {
        return lock.getWriteLock();
    }

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

    public RangerServiceDefHelper getServiceDefHelper() { return serviceDefHelper; }

    public RangerPolicyRepository getPolicyRepository() {
        return policyRepository;
    }

    public RangerPolicyRepository getTagPolicyRepository() {
        return tagPolicyRepository;
    }

    public Map<String, RangerPolicyRepository> getZonePolicyRepositories() { return zonePolicyRepositories; }

    public List<RangerContextEnricher> getAllContextEnrichers() { return allContextEnrichers; }

    public RangerPluginContext getPluginContext() { return pluginContext; }

    public StringTokenReplacer getStringTokenReplacer(String resourceName) {
        return tokenReplacers.get(resourceName);
    }

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

        sb.append(lock.toString());

        sb.append("}");

        return sb;
    }

    public List<RangerPolicy> getResourcePolicies(String zoneName) {
        RangerPolicyRepository zoneResourceRepository = zonePolicyRepositories.get(zoneName);

        return zoneResourceRepository == null ? Collections.emptyList() : zoneResourceRepository.getPolicies();
    }

    RangerSecurityZoneMatcher getZoneMatcher() {
        return zoneMatcher;
    }

    public PolicyEngine(ServicePolicies servicePolicies, RangerPluginContext pluginContext, RangerRoles roles, boolean isUseReadWriteLock) {
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

        normalizeServiceDefs(servicePolicies);
        pluginContext.cleanResourceMatchers();

        this.pluginContext = pluginContext;
        this.lock          = new RangerReadWriteLock(isUseReadWriteLock);
        this.zoneMatcher   = new RangerSecurityZoneMatcher(servicePolicies.getSecurityZones(), servicePolicies.getServiceDef(), pluginContext);

        Boolean                  hasPolicyDeltas      = RangerPolicyDeltaUtil.hasPolicyDeltas(servicePolicies);

        if (hasPolicyDeltas != null) {
            if (hasPolicyDeltas.equals(Boolean.TRUE)) {
                LOG.info("Policy engine will" + (isUseReadWriteLock ? " " : " not ") + "perform in place update while processing policy-deltas.");
            } else {
                LOG.info("Policy engine will" + (isUseReadWriteLock ? " " : " not ") + "perform in place update while processing policies.");
            }
        }

        RangerAuthContext currAuthContext = pluginContext.getAuthContext();
        RangerUserStore   userStore       = currAuthContext != null ? currAuthContext.getUserStoreUtil().getUserStore() : null;
        RangerAuthContext authContext     = new RangerAuthContext(null, zoneMatcher, roles, userStore);

        this.pluginContext.setAuthContext(authContext);

        RangerPolicyEngineOptions options = pluginContext.getConfig().getPolicyEngineOptions();

        if(StringUtils.isBlank(options.evaluatorType) || StringUtils.equalsIgnoreCase(options.evaluatorType, RangerPolicyEvaluator.EVALUATOR_TYPE_AUTO)) {
            options.evaluatorType = RangerPolicyEvaluator.EVALUATOR_TYPE_OPTIMIZED;
        }

        policyRepository = new RangerPolicyRepository(servicePolicies, this.pluginContext);
        serviceDefHelper = new RangerServiceDefHelper(policyRepository.getServiceDef(), false);

        ServicePolicies.TagPolicies tagPolicies = servicePolicies.getTagPolicies();

        if (!options.disableTagPolicyEvaluation
                && tagPolicies != null
                && !StringUtils.isEmpty(tagPolicies.getServiceName())
                && tagPolicies.getServiceDef() != null) {
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
            for (Map.Entry<String, ServicePolicies.SecurityZoneInfo> zone : servicePolicies.getSecurityZones().entrySet()) {
                RangerPolicyRepository policyRepository = new RangerPolicyRepository(servicePolicies, this.pluginContext, zone.getKey());

                zonePolicyRepositories.put(zone.getKey(), policyRepository);
            }
        }

        for (RangerServiceDef.RangerResourceDef resourceDef : getServiceDef().getResources()) {
            Map<String, String> matchOptions = resourceDef.getMatcherOptions();

            if (RangerAbstractResourceMatcher.getOptionReplaceTokens(matchOptions)) {
                String delimiterPrefix = RangerAbstractResourceMatcher.getOptionDelimiterPrefix(matchOptions);
                char delimiterStart = RangerAbstractResourceMatcher.getOptionDelimiterStart(matchOptions);
                char delimiterEnd = RangerAbstractResourceMatcher.getOptionDelimiterEnd(matchOptions);
                char escapeChar = RangerAbstractResourceMatcher.getOptionDelimiterEscape(matchOptions);

                StringTokenReplacer tokenReplacer = new StringTokenReplacer(delimiterStart, delimiterEnd, escapeChar, delimiterPrefix);
                tokenReplacers.put(resourceDef.getName(), tokenReplacer);
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

        try (RangerReadWriteLock.RangerLock writeLock = getWriteLock()) {
            if (LOG.isDebugEnabled()) {
                if (writeLock.isLockingEnabled()) {
                    LOG.debug("Acquired lock - " + writeLock);
                }
            }

            RangerServiceDef serviceDef = this.getServiceDef();
            String serviceType = (serviceDef != null) ? serviceDef.getName() : "";
            boolean isValidDeltas = false;

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
                if (writeLock.isLockingEnabled()) {
                    updatePolicyEngine(servicePolicies);
                    ret = this;
                } else {
                    ret = new PolicyEngine(this, servicePolicies);
                }
            } else {
                ret = null;
            }
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

    public Set<String> getMatchedZonesForResourceAndChildren(Map<String, ?> resource) {
        Set<String> ret = zoneMatcher.getZonesForResourceAndChildren(resource);

        if (LOG.isDebugEnabled()) {
            LOG.debug("getMatchedZonesForResourceAndChildren(resource={}): ret={}", resource, ret);
        }

        return ret;
    }

    public Set<String> getMatchedZonesForResourceAndChildren(RangerAccessResource resource) {
        Set<String> ret = zoneMatcher.getZonesForResourceAndChildren(resource);

        if (LOG.isDebugEnabled()) {
            LOG.debug("getMatchedZonesForResourceAndChildren(resource={}): ret={}", resource, ret);
        }

        return ret;
    }

    public String getUniquelyMatchedZoneName(Map<String, ?> resourceAsMap) {
        Set<String> matchedZones = zoneMatcher.getZonesForResourceAndChildren(resourceAsMap);
        String      ret          = (matchedZones != null && matchedZones.size() == 1) ? matchedZones.iterator().next() : null;

        if (LOG.isDebugEnabled()) {
            LOG.debug("getUniquelyMatchedZoneName(" + resourceAsMap + "): matchedZones=" + matchedZones + ", ret=" + ret);
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

        if (StringUtils.isNotEmpty(resourceZoneName) && tagPolicyRepository != null && zoneMatcher.hasTagService(resourceZoneName)) {
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

    private void normalizeServiceDefs(ServicePolicies servicePolicies) {
        RangerServiceDef serviceDef = servicePolicies.getServiceDef();

        if (serviceDef != null) {
            ServiceDefUtil.normalize(serviceDef);

            RangerServiceDef tagServiceDef = servicePolicies.getTagPolicies() != null ? servicePolicies.getTagPolicies().getServiceDef() : null;

            if (tagServiceDef != null) {
                ServiceDefUtil.normalizeAccessTypeDefs(ServiceDefUtil.normalize(tagServiceDef), serviceDef.getName());
            }
        }
    }

    private PolicyEngine(final PolicyEngine other, ServicePolicies servicePolicies) {
        this.useForwardedIPAddress = other.useForwardedIPAddress;
        this.trustedProxyAddresses = other.trustedProxyAddresses;
        this.serviceDefHelper      = other.serviceDefHelper;
        this.pluginContext         = other.pluginContext;
        this.lock                  = other.lock;
        this.zoneMatcher           = new RangerSecurityZoneMatcher(servicePolicies.getSecurityZones(), servicePolicies.getServiceDef(), pluginContext);

        long                    policyVersion                   = servicePolicies.getPolicyVersion() != null ? servicePolicies.getPolicyVersion() : -1L;
        List<RangerPolicyDelta> defaultZoneDeltas               = new ArrayList<>();
        List<RangerPolicyDelta> defaultZoneDeltasForTagPolicies = new ArrayList<>();

        getDeltasSortedByZones(other, servicePolicies, defaultZoneDeltas, defaultZoneDeltasForTagPolicies);

        if (other.policyRepository != null && CollectionUtils.isNotEmpty(defaultZoneDeltas)) {
            this.policyRepository = new RangerPolicyRepository(other.policyRepository, defaultZoneDeltas, policyVersion);
        } else {
            this.policyRepository = shareWith(other.policyRepository);
        }

        if (MapUtils.isEmpty(zonePolicyRepositories) && MapUtils.isNotEmpty(other.zonePolicyRepositories)) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Existing engine contains some zonePolicyRepositories and new engine contains no zonePolicyRepositories");
            }
            for (Map.Entry<String, RangerPolicyRepository> entry : other.zonePolicyRepositories.entrySet()) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Copying over zoneRepository for zone :[" + entry.getKey() + "]");
                }
                RangerPolicyRepository otherZonePolicyRepository = entry.getValue();
                RangerPolicyRepository zonePolicyRepository = shareWith(otherZonePolicyRepository);
                this.zonePolicyRepositories.put(entry.getKey(), zonePolicyRepository);
            }
        } else {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Existing engine contains no zonePolicyRepositories or new engine contains some zonePolicyRepositories");
                LOG.debug("Not copying zoneRepositories from existing engine, as they are already copied or modified");
            }
        }

        if (servicePolicies.getTagPolicies() != null && CollectionUtils.isNotEmpty(defaultZoneDeltasForTagPolicies)) {
            if (other.tagPolicyRepository == null) {

                if (LOG.isDebugEnabled()) {
                    LOG.debug("Current policy-engine does not have any tagPolicyRepository");
                }
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
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Current policy-engine has a tagPolicyRepository");
                }
                this.tagPolicyRepository = new RangerPolicyRepository(other.tagPolicyRepository, defaultZoneDeltasForTagPolicies, policyVersion);
            }
        } else {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Either no associated tag repository or no changes to tag policies");
            }
            this.tagPolicyRepository = shareWith(other.tagPolicyRepository);
        }

        List<RangerContextEnricher> tmpList;
        List<RangerContextEnricher> tagContextEnrichers      = tagPolicyRepository == null ? null :tagPolicyRepository.getContextEnrichers();
        List<RangerContextEnricher> resourceContextEnrichers = policyRepository == null ? null : policyRepository.getContextEnrichers();

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

    void updatePolicyEngine(ServicePolicies servicePolicies) {
        List<RangerPolicyDelta> defaultZoneDeltas               = new ArrayList<>();
        List<RangerPolicyDelta> defaultZoneDeltasForTagPolicies = new ArrayList<>();

        getDeltasSortedByZones(this, servicePolicies, defaultZoneDeltas, defaultZoneDeltasForTagPolicies);

        if (this.policyRepository != null && CollectionUtils.isNotEmpty(defaultZoneDeltas)) {
            this.policyRepository.reinit(defaultZoneDeltas);
        }

        if (servicePolicies.getTagPolicies() != null && CollectionUtils.isNotEmpty(defaultZoneDeltasForTagPolicies)) {
            if (this.tagPolicyRepository != null) {
                this.tagPolicyRepository.reinit(defaultZoneDeltasForTagPolicies);
            } else {
                LOG.error("No previous tagPolicyRepository to update! Should not have come here!!");
            }
        }

        reorderPolicyEvaluators();
    }

    private void getDeltasSortedByZones(PolicyEngine current, ServicePolicies servicePolicies, List<RangerPolicyDelta> defaultZoneDeltas, List<RangerPolicyDelta> defaultZoneDeltasForTagPolicies) {

        if (LOG.isDebugEnabled()) {
            LOG.debug("==> getDeltasSortedByZones()");
        }

        long                    policyVersion                   = servicePolicies.getPolicyVersion() != null ? servicePolicies.getPolicyVersion() : -1L;

        if (CollectionUtils.isNotEmpty(defaultZoneDeltas)) {
            LOG.warn("Emptying out defaultZoneDeltas!");
            defaultZoneDeltas.clear();
        }
        if (CollectionUtils.isNotEmpty(defaultZoneDeltasForTagPolicies)) {
            LOG.warn("Emptying out defaultZoneDeltasForTagPolicies!");
            defaultZoneDeltasForTagPolicies.clear();
        }

        if (MapUtils.isNotEmpty(servicePolicies.getSecurityZones())) {
            Map<String, List<RangerPolicyDelta>> zoneDeltasMap = new HashMap<>();

            for (Map.Entry<String, ServicePolicies.SecurityZoneInfo> zone : servicePolicies.getSecurityZones().entrySet()) {
                String                  zoneName   = zone.getKey();
                List<RangerPolicyDelta> deltas     = zone.getValue().getPolicyDeltas();
                List<RangerPolicyDelta> zoneDeltas = new ArrayList<>();

                if (StringUtils.isNotEmpty(zoneName)) {
                    zoneDeltasMap.put(zoneName, zoneDeltas);

                    for (RangerPolicyDelta delta : deltas) {
                        zoneDeltas = zoneDeltasMap.get(zoneName);
                        zoneDeltas.add(delta);
                    }
                }
            }

            if (LOG.isDebugEnabled()) {
                LOG.debug("Security zones found in the service-policies:[" + zoneDeltasMap.keySet() + "]");
            }

            for (Map.Entry<String, List<RangerPolicyDelta>> entry : zoneDeltasMap.entrySet()) {
                final String                  zoneName        = entry.getKey();
                final List<RangerPolicyDelta> zoneDeltas      = entry.getValue();
                final RangerPolicyRepository  otherRepository = current.zonePolicyRepositories.get(zoneName);
                final RangerPolicyRepository  policyRepository;

                if (LOG.isDebugEnabled()) {
                    LOG.debug("zoneName:[" + zoneName + "], zoneDeltas:[" + Arrays.toString(zoneDeltas.toArray()) + "], doesOtherRepositoryExist:[" + (otherRepository != null) + "]");
                }

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

                        policyRepository = new RangerPolicyRepository(servicePolicies, current.pluginContext, zoneName);
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

        if (LOG.isDebugEnabled()) {
            LOG.debug("ServicePolicies.policyDeltas:[" + Arrays.toString(servicePolicies.getPolicyDeltas().toArray()) + "]");
        }

        for (RangerPolicyDelta delta : unzonedDeltas) {
            if (servicePolicies.getServiceDef().getName().equals(delta.getServiceType())) {
                defaultZoneDeltas.add(delta);
            } else {
                defaultZoneDeltasForTagPolicies.add(delta);
            }
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("defaultZoneDeltas:[" + Arrays.toString(defaultZoneDeltas.toArray()) + "]");
            LOG.debug("defaultZoneDeltasForTagPolicies:[" + Arrays.toString(defaultZoneDeltasForTagPolicies.toArray()) + "]");
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== getDeltasSortedByZones()");
        }
    }
}

