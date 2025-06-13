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

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.ranger.authorization.hadoop.config.RangerPluginConfig;
import org.apache.ranger.authorization.utils.StringUtil;
import org.apache.ranger.plugin.contextenricher.RangerTagForEval;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.model.validation.RangerServiceDefHelper;
import org.apache.ranger.plugin.policyengine.gds.GdsAccessResult;
import org.apache.ranger.plugin.policyevaluator.RangerPolicyEvaluator;
import org.apache.ranger.plugin.policyresourcematcher.RangerPolicyResourceMatcher.MatchType;
import org.apache.ranger.plugin.service.RangerDefaultRequestProcessor;
import org.apache.ranger.plugin.util.GrantRevokeRequest;
import org.apache.ranger.plugin.util.RangerAccessRequestUtil;
import org.apache.ranger.plugin.util.RangerCommonConstants;
import org.apache.ranger.plugin.util.RangerPerfTracer;
import org.apache.ranger.plugin.util.RangerReadWriteLock;
import org.apache.ranger.plugin.util.RangerRoles;
import org.apache.ranger.plugin.util.ServicePolicies;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.ranger.plugin.policyengine.PolicyEvaluatorForTag.MATCH_TYPE_COMPARATOR;

public class RangerPolicyEngineImpl implements RangerPolicyEngine {
    private static final Logger LOG = LoggerFactory.getLogger(RangerPolicyEngineImpl.class);

    private static final Logger PERF_POLICYENGINE_REQUEST_LOG  = RangerPerfTracer.getPerfLogger("policyengine.request");
    private static final Logger PERF_POLICYENGINE_AUDIT_LOG    = RangerPerfTracer.getPerfLogger("policyengine.audit");
    private static final Logger PERF_POLICYENGINE_GET_ACLS_LOG = RangerPerfTracer.getPerfLogger("policyengine.getResourceACLs");

    private final PolicyEngine                 policyEngine;
    private final RangerAccessRequestProcessor requestProcessor;
    private final ServiceConfig                serviceConfig;

    public RangerPolicyEngineImpl(ServicePolicies servicePolicies, RangerPluginContext pluginContext, RangerRoles roles) {
        final boolean isUseReadWriteLock;

        Configuration config = pluginContext != null ? pluginContext.getConfig() : null;

        if (config != null) {
            boolean isDeltasSupported = config.getBoolean(pluginContext.getConfig().getPropertyPrefix() + RangerCommonConstants.PLUGIN_CONFIG_SUFFIX_POLICY_DELTA, RangerCommonConstants.PLUGIN_CONFIG_SUFFIX_POLICY_DELTA_DEFAULT);

            isUseReadWriteLock = isDeltasSupported && config.getBoolean(pluginContext.getConfig().getPropertyPrefix() + RangerCommonConstants.PLUGIN_CONFIG_SUFFIX_IN_PLACE_POLICY_UPDATES, RangerCommonConstants.PLUGIN_CONFIG_SUFFIX_IN_PLACE_POLICY_UPDATES_DEFAULT);
        } else {
            isUseReadWriteLock = false;
        }

        policyEngine     = new PolicyEngine(servicePolicies, pluginContext, roles, isUseReadWriteLock);
        serviceConfig    = new ServiceConfig(servicePolicies.getServiceConfig());
        requestProcessor = new RangerDefaultRequestProcessor(policyEngine);
    }

    private RangerPolicyEngineImpl(final PolicyEngine policyEngine, RangerPolicyEngineImpl other) {
        this.policyEngine     = policyEngine;
        this.requestProcessor = new RangerDefaultRequestProcessor(policyEngine);
        this.serviceConfig    = new ServiceConfig(other.serviceConfig);
    }

    public static RangerPolicyEngine getPolicyEngine(final RangerPolicyEngineImpl other, final ServicePolicies servicePolicies) {
        RangerPolicyEngine ret = null;

        if (other != null && servicePolicies != null) {
            PolicyEngine policyEngine = other.policyEngine.cloneWithDelta(servicePolicies);

            if (policyEngine != null) {
                if (policyEngine == other.policyEngine) {
                    ret = other;
                } else {
                    ret = new RangerPolicyEngineImpl(policyEngine, other);
                }
            }
        }

        return ret;
    }

    @Override
    public String toString() {
        return policyEngine.toString();
    }

    @Override
    public void setUseForwardedIPAddress(boolean useForwardedIPAddress) {
        try (RangerReadWriteLock.RangerLock writeLock = policyEngine.getWriteLock()) {
            if (writeLock.isLockingEnabled()) {
                LOG.debug("Acquired lock - {}", writeLock);
            }

            policyEngine.setUseForwardedIPAddress(useForwardedIPAddress);
        }
    }

    @Override
    public void setTrustedProxyAddresses(String[] trustedProxyAddresses) {
        try (RangerReadWriteLock.RangerLock writeLock = policyEngine.getWriteLock()) {
            if (writeLock.isLockingEnabled()) {
                LOG.debug("Acquired lock - {}", writeLock);
            }

            policyEngine.setTrustedProxyAddresses(trustedProxyAddresses);
        }
    }

    @Override
    public RangerServiceDef getServiceDef() {
        final RangerServiceDef ret;

        try (RangerReadWriteLock.RangerLock readLock = policyEngine.getReadLock()) {
            if (readLock.isLockingEnabled()) {
                LOG.debug("Acquired lock - {}", readLock);
            }

            ret = policyEngine.getServiceDef();
        }
        return ret;
    }

    @Override
    public RangerServiceDefHelper getServiceDefHelper() {
        final RangerServiceDefHelper ret;

        try (RangerReadWriteLock.RangerLock readLock = policyEngine.getReadLock()) {
            if (readLock.isLockingEnabled()) {
                LOG.debug("Acquired lock - {}", readLock);
            }

            ret = policyEngine.getServiceDefHelper();
        }
        return ret;
    }

    @Override
    public long getPolicyVersion() {
        long ret;

        try (RangerReadWriteLock.RangerLock readLock = policyEngine.getReadLock()) {
            if (readLock.isLockingEnabled()) {
                LOG.debug("Acquired lock - {}", readLock);
            }

            ret = policyEngine.getPolicyVersion();
        }
        return ret;
    }

    @Override
    public long getRoleVersion() {
        return policyEngine.getRoleVersion();
    }

    @Override
    public void setRoles(RangerRoles roles) {
        try (RangerReadWriteLock.RangerLock writeLock = policyEngine.getWriteLock()) {
            if (writeLock.isLockingEnabled()) {
                LOG.debug("Acquired lock - {}", writeLock);
            }

            policyEngine.setRoles(roles);
        }
    }

    @Override
    public RangerAccessResult evaluatePolicies(RangerAccessRequest request, int policyType, RangerAccessResultProcessor resultProcessor) {
        LOG.debug("==> RangerPolicyEngineImpl.evaluatePolicies({}, policyType={})", request, policyType);

        RangerPerfTracer perf = null;

        if (RangerPerfTracer.isPerfTraceEnabled(PERF_POLICYENGINE_REQUEST_LOG)) {
            String requestHashCode = Integer.toHexString(System.identityHashCode(request)) + "_" + policyType;

            perf = RangerPerfTracer.getPerfTracer(PERF_POLICYENGINE_REQUEST_LOG, "RangerPolicyEngine.evaluatePolicies(requestHashCode=" + requestHashCode + ")");

            LOG.info("RangerPolicyEngineImpl.evaluatePolicies({}, {})", requestHashCode, request);
        }

        RangerAccessResult ret;

        try (RangerReadWriteLock.RangerLock readLock = policyEngine.getReadLock()) {
            if (readLock.isLockingEnabled()) {
                LOG.debug("Acquired lock - {}", readLock);
            }

            requestProcessor.preProcess(request);

            ret = zoneAwareAccessEvaluationWithNoAudit(request, policyType);

            if (resultProcessor != null) {
                RangerPerfTracer perfAuditTracer = null;

                if (RangerPerfTracer.isPerfTraceEnabled(PERF_POLICYENGINE_AUDIT_LOG)) {
                    String requestHashCode = Integer.toHexString(System.identityHashCode(request)) + "_" + policyType;

                    perfAuditTracer = RangerPerfTracer.getPerfTracer(PERF_POLICYENGINE_AUDIT_LOG, "RangerPolicyEngine.processAudit(requestHashCode=" + requestHashCode + ")");
                }

                resultProcessor.processResult(ret);

                RangerPerfTracer.log(perfAuditTracer);
            }
        }

        RangerPerfTracer.log(perf);

        LOG.debug("<== RangerPolicyEngineImpl.evaluatePolicies({}, policyType={}): {}", request, policyType, ret);

        return ret;
    }

    @Override
    public Collection<RangerAccessResult> evaluatePolicies(Collection<RangerAccessRequest> requests, int policyType, RangerAccessResultProcessor resultProcessor) {
        LOG.debug("==> RangerPolicyEngineImpl.evaluatePolicies({}, policyType={})", requests, policyType);

        Collection<RangerAccessResult> ret = new ArrayList<>();

        try (RangerReadWriteLock.RangerLock readLock = policyEngine.getReadLock()) {
            if (readLock.isLockingEnabled()) {
                LOG.debug("Acquired lock - {}", readLock);
            }

            if (requests != null) {
                for (RangerAccessRequest request : requests) {
                    requestProcessor.preProcess(request);

                    RangerAccessResult result = zoneAwareAccessEvaluationWithNoAudit(request, policyType);

                    ret.add(result);
                }
            }

            if (resultProcessor != null) {
                resultProcessor.processResults(ret);
            }
        }

        LOG.debug("<== RangerPolicyEngineImpl.evaluatePolicies({}, policyType={}): {}", requests, policyType, ret);

        return ret;
    }

    @Override
    public void evaluateAuditPolicies(RangerAccessResult result) {
        LOG.debug("==> RangerPolicyEngineImpl.evaluateAuditPolicies(result={})", result);

        try (RangerReadWriteLock.RangerLock readLock = policyEngine.getReadLock()) {
            if (readLock.isLockingEnabled()) {
                LOG.debug("Acquired lock - {}", readLock);
            }

            RangerPolicyRepository tagPolicyRepository      = policyEngine.getTagPolicyRepository();
            RangerPolicyRepository policyRepository         = policyEngine.getPolicyRepository();
            RangerAccessRequest    request                  = result.getAccessRequest();
            boolean                savedIsAuditedDetermined = result.getIsAuditedDetermined();
            boolean                savedIsAudited           = result.getIsAudited();

            result.setIsAudited(false);
            result.setIsAuditedDetermined(false);

            try {
                if (tagPolicyRepository != null) {
                    evaluateTagAuditPolicies(request, result, tagPolicyRepository);
                }

                if (!result.getIsAuditedDetermined() && policyRepository != null) {
                    evaluateResourceAuditPolicies(request, result, policyRepository);
                }
            } finally {
                if (!result.getIsAuditedDetermined()) {
                    result.setIsAudited(savedIsAudited);
                    result.setIsAuditedDetermined(savedIsAuditedDetermined);
                }
            }
        }

        LOG.debug("<== RangerPolicyEngineImpl.evaluateAuditPolicies(result={})", result);
    }

    @Override
    public RangerResourceACLs getResourceACLs(RangerAccessRequest request) {
        return getResourceACLs(request, null);
    }

    @Override
    public RangerResourceACLs getResourceACLs(RangerAccessRequest request, Integer requestedPolicyType) {
        LOG.debug("==> RangerPolicyEngineImpl.getResourceACLs(request={}, policyType={})", request, requestedPolicyType);

        RangerResourceACLs ret  = new RangerResourceACLs();
        RangerPerfTracer   perf = null;

        if (RangerPerfTracer.isPerfTraceEnabled(PERF_POLICYENGINE_GET_ACLS_LOG)) {
            perf = RangerPerfTracer.getPerfTracer(PERF_POLICYENGINE_GET_ACLS_LOG, "RangerPolicyEngine.getResourceACLs(requestHashCode=" + request.getResource().getAsString() + ")");
        }

        try (RangerReadWriteLock.RangerLock readLock = policyEngine.getReadLock()) {
            if (readLock.isLockingEnabled()) {
                LOG.debug("Acquired lock - {}", readLock);
            }

            requestProcessor.preProcess(request);

            String zoneName = RangerAccessRequestUtil.getResourceZoneNameFromContext(request.getContext());

            LOG.debug("zoneName:[{}]", zoneName);

            int[] policyTypes = requestedPolicyType == null ? RangerPolicy.POLICY_TYPES : new int[] {requestedPolicyType};

            for (int policyType : policyTypes) {
                // if resource isn't applicable for the policyType, skip evaluating policies and gathering ACLs
                // for example, following resources are not applicable for listed policy-types
                //   - database: masking/row-filter policies
                //   - table:    masking policies
                //   - column:   row-filter policies
                boolean requireExactMatch = (policyType == RangerPolicy.POLICY_TYPE_DATAMASK) || (policyType == RangerPolicy.POLICY_TYPE_ROWFILTER);

                if (!policyEngine.getServiceDefHelper().isValidHierarchy(policyType, request.getResource().getKeys(), requireExactMatch)) {
                    continue;
                }

                List<RangerPolicyEvaluator> allEvaluators           = new ArrayList<>();
                Map<Long, MatchType>        tagMatchTypeMap         = new HashMap<>();
                Set<Long>                   policyIdForTemporalTags = new HashSet<>();

                getResourceACLEvaluatorsForZone(request, zoneName, policyType, allEvaluators, tagMatchTypeMap, policyIdForTemporalTags);

                allEvaluators.sort(RangerPolicyEvaluator.EVAL_ORDER_COMPARATOR);

                if (CollectionUtils.isEmpty(allEvaluators)) {
                    continue;
                }

                Integer policyPriority = null;

                for (RangerPolicyEvaluator evaluator : allEvaluators) {
                    if (policyPriority == null) {
                        policyPriority = evaluator.getPolicyPriority();
                    }

                    if (policyPriority != evaluator.getPolicyPriority()) {
                        if (policyType == RangerPolicy.POLICY_TYPE_ACCESS) {
                            ret.finalizeAcls();
                        }

                        policyPriority = evaluator.getPolicyPriority();
                    }

                    boolean   isTemporalTagPolicy = policyIdForTemporalTags.contains(evaluator.getPolicyId());
                    MatchType tagMatchType        = tagMatchTypeMap.get(evaluator.getPolicyId());

                    // tag assigned to ANCESTORS must apply to SELF as well, to be consistent with policy evaluation in RangerDefaultPolicyEvaluator.evaluate()
                    if (tagMatchType == MatchType.ANCESTOR) {
                        tagMatchType = MatchType.SELF;
                    }

                    evaluator.getResourceACLs(request, ret, isTemporalTagPolicy, null, tagMatchType, policyEngine);
                }

                ret.finalizeAcls();
            }
        }

        RangerPerfTracer.logAlways(perf);

        LOG.debug("<== RangerPolicyEngineImpl.getResourceACLs(request={}, policyType={}) : ret={}", request, requestedPolicyType, ret);

        return ret;
    }

    @Override
    public Set<String> getRolesFromUserAndGroups(String user, Set<String> groups) {
        Set<String> ret;
        try (RangerReadWriteLock.RangerLock readLock = policyEngine.getReadLock()) {
            if (readLock.isLockingEnabled()) {
                LOG.debug("Acquired lock - {}", readLock);
            }

            ret = policyEngine.getPluginContext().getAuthContext().getRolesForUserAndGroups(user, groups);
        }

        return ret;
    }

    @Override
    public RangerRoles getRangerRoles() {
        return policyEngine.getPluginContext().getAuthContext().getRangerRolesUtil().getRoles();
    }

    @Override
    public RangerPluginContext getPluginContext() {
        return policyEngine.getPluginContext();
    }

    @Override
    public String getUniquelyMatchedZoneName(GrantRevokeRequest grantRevokeRequest) {
        LOG.debug("==> RangerPolicyEngineImpl.getUniquelyMatchedZoneName({})", grantRevokeRequest);

        String ret;

        try (RangerReadWriteLock.RangerLock readLock = policyEngine.getReadLock()) {
            if (readLock.isLockingEnabled()) {
                LOG.debug("Acquired lock - {}", readLock);
            }

            ret = policyEngine.getUniquelyMatchedZoneName(grantRevokeRequest.getResource());
        }

        LOG.debug("<== RangerPolicyEngineImpl.getUniquelyMatchedZoneName({}) : {}", grantRevokeRequest, ret);

        return ret;
    }

    @Override
    public List<RangerPolicy> getResourcePolicies(String zoneName) {
        List<RangerPolicy> ret;

        try (RangerReadWriteLock.RangerLock readLock = policyEngine.getReadLock()) {
            if (readLock.isLockingEnabled()) {
                LOG.debug("Acquired lock - {}", readLock);
            }

            List<RangerPolicy> oldPolicies = policyEngine.getResourcePolicies(zoneName);

            ret = CollectionUtils.isNotEmpty(oldPolicies) ? new ArrayList<>(oldPolicies) : oldPolicies;
        }

        return ret;
    }

    @Override
    public List<RangerPolicy> getResourcePolicies() {
        List<RangerPolicy> ret;

        try (RangerReadWriteLock.RangerLock readLock = policyEngine.getReadLock()) {
            if (readLock.isLockingEnabled()) {
                LOG.debug("Acquired lock - {}", readLock);
            }

            RangerPolicyRepository policyRepository = policyEngine.getPolicyRepository();
            List<RangerPolicy>     oldPolicies      = policyRepository == null ? Collections.emptyList() : policyRepository.getPolicies();

            ret = CollectionUtils.isNotEmpty(oldPolicies) ? new ArrayList<>(oldPolicies) : oldPolicies;
        }

        return ret;
    }

    @Override
    public List<RangerPolicy> getTagPolicies() {
        List<RangerPolicy> ret;

        try (RangerReadWriteLock.RangerLock readLock = policyEngine.getReadLock()) {
            if (readLock.isLockingEnabled()) {
                LOG.debug("Acquired lock - {}", readLock);
            }

            RangerPolicyRepository tagPolicyRepository = policyEngine.getTagPolicyRepository();
            List<RangerPolicy>     oldPolicies         = tagPolicyRepository == null ? Collections.emptyList() : tagPolicyRepository.getPolicies();

            ret = CollectionUtils.isNotEmpty(oldPolicies) ? new ArrayList<>(oldPolicies) : oldPolicies;
        }

        return ret;
    }

    // This API is used only used by test code
    @Override
    public RangerResourceAccessInfo getResourceAccessInfo(RangerAccessRequest request) {
        LOG.debug("==> RangerPolicyEngineImpl.getResourceAccessInfo({})", request);

        requestProcessor.preProcess(request);

        RangerResourceAccessInfo ret       = new RangerResourceAccessInfo(request);
        Set<String>              zoneNames = RangerAccessRequestUtil.getResourceZoneNamesFromContext(request.getContext());

        LOG.debug("zoneNames:[{}]", zoneNames);

        if (CollectionUtils.isEmpty(zoneNames)) {
            getResourceAccessInfoForZone(request, ret, null);
        } else {
            for (String zoneName : zoneNames) {
                getResourceAccessInfoForZone(request, ret, zoneName);
            }
        }

        LOG.debug("<== RangerPolicyEngineImpl.getResourceAccessInfo({}): {}", request, ret);

        return ret;
    }

    public void releaseResources(boolean isForced) {
        LOG.debug("==> RangerPolicyEngineImpl.releaseResources(isForced={})", isForced);

        PolicyEngine policyEngine = this.policyEngine;

        if (policyEngine != null) {
            policyEngine.preCleanup(isForced);
        } else {
            LOG.debug("Cannot preCleanup policy-engine as it is null!");
        }

        LOG.debug("<== RangerPolicyEngineImpl.releaseResources(isForced={})", isForced);
    }

    public boolean isServiceAdmin(String userName) {
        boolean ret = serviceConfig.isServiceAdmin(userName);

        if (!ret) {
            RangerPluginConfig pluginConfig = policyEngine.getPluginContext().getConfig();

            ret = pluginConfig.isServiceAdmin(userName);
        }

        return ret;
    }

    public PolicyEngine getPolicyEngine() {
        return policyEngine;
    }

    public RangerAccessRequestProcessor getRequestProcessor() {
        return requestProcessor;
    }

    private RangerAccessResult zoneAwareAccessEvaluationWithNoAudit(RangerAccessRequest request, int policyType) {
        LOG.debug("==> RangerPolicyEngineImpl.zoneAwareAccessEvaluationWithNoAudit({}, policyType={})", request, policyType);

        RangerAccessResult     ret                 = null;
        RangerPolicyRepository tagPolicyRepository = policyEngine.getTagPolicyRepository();
        Set<String>            zoneNames           = RangerAccessRequestUtil.getResourceZoneNamesFromContext(request.getContext());

        LOG.debug("zoneNames:[{}]", zoneNames);

        if (CollectionUtils.isEmpty(zoneNames) || (zoneNames.size() > 1 && !request.isAccessTypeAny())) {
            // Evaluate default policies
            RangerPolicyRepository policyRepository = policyEngine.getRepositoryForZone(null);

            ret = evaluatePoliciesNoAudit(request, policyType, null, policyRepository, tagPolicyRepository);

            ret.setZoneName(null);
        } else if (zoneNames.size() == 1 || request.isAccessTypeAny()) {
            // Evaluate zone specific policies
            for (String zoneName : zoneNames) {
                RangerPolicyRepository policyRepository = policyEngine.getRepositoryForZone(zoneName);

                ret = evaluatePoliciesNoAudit(request, policyType, zoneName, policyRepository, tagPolicyRepository);

                ret.setZoneName(zoneName);

                if (ret.getIsAllowed()) {
                    LOG.debug("Zone:[{}] allowed access. Completed processing other zones", zoneName);

                    break;
                }
            }
        }

        if (request.isAccessTypeAny() && (request.getResource() == null || CollectionUtils.isEmpty(request.getResource().getKeys())) && ret != null && !ret.getIsAllowed() && MapUtils.isNotEmpty(policyEngine.getZonePolicyRepositories())) {
            // resource is empty and access is ANY
            LOG.debug("Process all security-zones");

            RangerAccessResult accessResult;

            for (Map.Entry<String, RangerPolicyRepository> entry : policyEngine.getZonePolicyRepositories().entrySet()) {
                String                 someZone         = entry.getKey();
                RangerPolicyRepository policyRepository = entry.getValue();

                LOG.debug("Evaluating policies for zone:[{}]", someZone);

                if (policyRepository != null) {
                    accessResult = evaluatePoliciesNoAudit(request, policyType, someZone, policyRepository, tagPolicyRepository);

                    if (accessResult.getIsAllowed()) {
                        LOG.debug("Zone:[{}] allowed access. Completed processing other zones", someZone);

                        accessResult.setZoneName(someZone);

                        ret = accessResult;

                        break;
                    }
                }
            }
        }

        updateFromGdsResult(ret);

        LOG.debug("<== RangerPolicyEngineImpl.zoneAwareAccessEvaluationWithNoAudit({}, policyType ={}): {}", request, policyType, ret);

        return ret;
    }

    private RangerAccessResult evaluatePoliciesNoAudit(RangerAccessRequest request, int policyType, String zoneName, RangerPolicyRepository policyRepository, RangerPolicyRepository tagPolicyRepository) {
        LOG.debug("==> RangerPolicyEngineImpl.evaluatePoliciesNoAudit({}, policyType={}, zoneName={})", request, policyType, zoneName);

        final RangerAccessResult ret;

        if (request.isAccessTypeAny()) {
            Set<String> allRequestedAccesses = getServiceDefHelper().getAllAccessTypes();

            RangerAccessRequestUtil.setAllRequestedAccessTypes(request.getContext(), allRequestedAccesses);
            RangerAccessRequestUtil.setIsAnyAccessInContext(request.getContext(), Boolean.TRUE);

            if (!request.ignoreDescendantDeny()) {
                Set<Set<String>> accessGroups = allRequestedAccesses.stream().map(Collections::singleton).collect(Collectors.toSet());

                RangerAccessRequestUtil.setAllRequestedAccessTypeGroups(request, accessGroups);
            }
        }

        ret = evaluatePoliciesForOneAccessTypeNoAudit(request, policyType, zoneName, policyRepository, tagPolicyRepository);

        LOG.debug("<== RangerPolicyEngineImpl.evaluatePoliciesNoAudit({}, policyType={}, zoneName={}): {}", request, policyType, zoneName, ret);

        return ret;
    }

    private RangerAccessResult evaluatePoliciesForOneAccessTypeNoAudit(RangerAccessRequest request, int policyType, String zoneName, RangerPolicyRepository policyRepository, RangerPolicyRepository tagPolicyRepository) {
        LOG.debug("==> RangerPolicyEngineImpl.evaluatePoliciesForOneAccessTypeNoAudit({}, policyType={}, zoneName={})", request, policyType, zoneName);

        final boolean            isSuperUser = isSuperUser(request.getUser(), request.getUserGroups());
        final Date               accessTime  = request.getAccessTime() != null ? request.getAccessTime() : new Date();
        final RangerAccessResult ret         = createAccessResult(request, policyType);

        if (isSuperUser || StringUtils.equals(request.getAccessType(), RangerPolicyEngine.SUPER_USER_ACCESS)) {
            ret.setIsAllowed(isSuperUser);
            ret.setIsAccessDetermined(true);
            ret.setPolicyId(-1);
            ret.setPolicyPriority(Integer.MAX_VALUE);
            ret.setReason("superuser");
        }

        evaluateTagPolicies(request, policyType, zoneName, tagPolicyRepository, ret);

        if (LOG.isDebugEnabled()) {
            if (ret.getIsAccessDetermined() && ret.getIsAuditedDetermined()) {
                if (!ret.getIsAllowed()) {
                    LOG.debug("RangerPolicyEngineImpl.evaluatePoliciesNoAudit() - audit determined and access denied by a tag policy. Higher priority resource policies will be evaluated to check for allow, request={}, result={}", request, ret);
                } else {
                    LOG.debug("RangerPolicyEngineImpl.evaluatePoliciesNoAudit() - audit determined and access allowed by a tag policy. Same or higher priority resource policies will be evaluated to check for deny, request={}, result={}", request, ret);
                }
            }
        }

        boolean isAllowedByTags          = ret.getIsAccessDetermined() && ret.getIsAllowed();
        boolean isDeniedByTags           = ret.getIsAccessDetermined() && !ret.getIsAllowed();
        boolean evaluateResourcePolicies = policyEngine.hasResourcePolicies(policyRepository);

        if (evaluateResourcePolicies) {
            boolean findAuditByResource = !ret.getIsAuditedDetermined();
            boolean foundInCache        = findAuditByResource && policyRepository.setAuditEnabledFromCache(request, ret);

            ret.setIsAccessDetermined(false); // discard result by tag-policies, to evaluate resource policies for possible override

            List<RangerPolicyEvaluator> evaluators = policyRepository.getLikelyMatchPolicyEvaluators(request, policyType);

            for (RangerPolicyEvaluator evaluator : evaluators) {
                if (!evaluator.isApplicable(accessTime)) {
                    continue;
                }

                if (isDeniedByTags) {
                    if (ret.getPolicyPriority() >= evaluator.getPolicyPriority()) {
                        ret.setIsAccessDetermined(true);
                    }
                } else if (ret.getIsAllowed()) {
                    if (policyType == RangerPolicy.POLICY_TYPE_ACCESS) {
                        // for access, allow decision made earlier by a policy with higher priority will be final
                        if (ret.getPolicyPriority() > evaluator.getPolicyPriority()) {
                            ret.setIsAccessDetermined(true);
                        }
                    } else {
                        // for other types (mask/row-filter), decision made earlier by a policy with same priority or higher will be final
                        if (ret.getPolicyPriority() >= evaluator.getPolicyPriority()) {
                            ret.setIsAccessDetermined(true);
                        }
                    }
                }

                ret.incrementEvaluatedPoliciesCount();
                evaluator.evaluate(request, ret);

                if (ret.getIsAllowed()) {
                    if (!evaluator.hasDeny()) { // No more deny policies left
                        ret.setIsAccessDetermined(true);
                    }
                }

                if (ret.getIsAuditedDetermined() && ret.getIsAccessDetermined()) {
                    break;            // Break out of policy-evaluation loop
                }
            }

            if (!ret.getIsAccessDetermined()) {
                if (isDeniedByTags) {
                    ret.setIsAllowed(false);
                } else if (isAllowedByTags) {
                    ret.setIsAllowed(true);
                } else {
                    updateFromGdsResult(ret);
                }
                if (!ret.getIsAllowed() &&
                        !getIsFallbackSupported()) {
                    ret.setIsAccessDetermined(true);
                }
            }

            if (ret.getIsAllowed()) {
                ret.setIsAccessDetermined(true);
            }
            RangerAccessRequestUtil.setAccessTypeResults(request.getContext(), null);
            RangerAccessRequestUtil.setAccessTypeACLResults(request.getContext(), null);
            RangerAccessRequestUtil.setIsAnyAccessInContext(request.getContext(), null);

            if (findAuditByResource && !foundInCache) {
                policyRepository.storeAuditEnabledInCache(request, ret);
            }
        }

        LOG.debug("<== RangerPolicyEngineImpl.evaluatePoliciesForOneAccessTypeNoAudit({}, policyType={}, zoneName={}): {}", request, policyType, zoneName, ret);

        return ret;
    }

    private void evaluateTagPolicies(final RangerAccessRequest request, int policyType, String zoneName, RangerPolicyRepository tagPolicyRepository, RangerAccessResult result) {
        LOG.debug("==> RangerPolicyEngineImpl.evaluateTagPolicies({}, policyType={}, zoneName={}, {})", request, policyType, zoneName, result);

        Date                        accessTime       = request.getAccessTime() != null ? request.getAccessTime() : new Date();
        Set<RangerTagForEval>       tags             = RangerAccessRequestUtil.getRequestTagsFromContext(request.getContext());
        List<PolicyEvaluatorForTag> policyEvaluators = tagPolicyRepository == null ? null : tagPolicyRepository.getLikelyMatchPolicyEvaluators(request, tags, policyType, accessTime);

        if (CollectionUtils.isNotEmpty(policyEvaluators)) {
            final boolean useTagPoliciesFromDefaultZone = !policyEngine.isResourceZoneAssociatedWithTagService(zoneName);

            for (PolicyEvaluatorForTag policyEvaluator : policyEvaluators) {
                RangerPolicyEvaluator evaluator      = policyEvaluator.getEvaluator();
                String                policyZoneName = evaluator.getPolicy().getZoneName();

                if (useTagPoliciesFromDefaultZone) {
                    if (StringUtils.isNotEmpty(policyZoneName)) {
                        LOG.debug("Tag policy [zone:{}] does not belong to default zone. Not evaluating this policy:[{}]", policyZoneName, evaluator.getPolicy());

                        continue;
                    }
                } else {
                    if (!StringUtils.equals(zoneName, policyZoneName)) {
                        LOG.debug("Tag policy [zone:{}] does not belong to the zone:[{}] of the accessed resource. Not evaluating this policy:[{}]", policyZoneName, zoneName, evaluator.getPolicy());

                        continue;
                    }
                }

                RangerTagForEval    tag            = policyEvaluator.getTag();
                RangerAccessRequest tagEvalRequest = new RangerTagAccessRequest(tag, tagPolicyRepository.getServiceDef(), request);
                RangerAccessResult  tagEvalResult  = createAccessResult(tagEvalRequest, policyType);

                LOG.debug("RangerPolicyEngineImpl.evaluateTagPolicies: Evaluating policies for tag ({})", tag.getType());

                tagEvalResult.setAccessResultFrom(result);
                tagEvalResult.setAuditResultFrom(result);

                result.incrementEvaluatedPoliciesCount();

                evaluator.evaluate(tagEvalRequest, tagEvalResult);

                if (tagEvalResult.getIsAllowed()) {
                    if (!evaluator.hasDeny()) { // No Deny policies left now
                        tagEvalResult.setIsAccessDetermined(true);
                    }
                }

                if (tagEvalResult.getIsAudited()) {
                    result.setAuditResultFrom(tagEvalResult);
                }

                if (!result.getIsAccessDetermined()) {
                    if (tagEvalResult.getIsAccessDetermined()) {
                        result.setAccessResultFrom(tagEvalResult);
                    } else {
                        if (!result.getIsAllowed() && tagEvalResult.getIsAllowed()) {
                            result.setAccessResultFrom(tagEvalResult);
                        }
                    }
                }

                if (result.getIsAuditedDetermined() && result.getIsAccessDetermined()) {
                    break;            // Break out of policy-evaluation loop
                }
            }
        }

        if (result.getIsAllowed()) {
            result.setIsAccessDetermined(true);
        }

        LOG.debug("<== RangerPolicyEngineImpl.evaluateTagPolicies({}, policyType={}, zoneName={}, {})", request, policyType, zoneName, result);
    }

    private RangerAccessResult createAccessResult(RangerAccessRequest request, int policyType) {
        RangerPolicyRepository repository = policyEngine.getPolicyRepository();
        RangerAccessResult     ret        = new RangerAccessResult(policyType, repository.getServiceName(), repository.getServiceDef(), request);

        switch (repository.getAuditModeEnum()) {
            case AUDIT_ALL:
                ret.setIsAudited(true);
                break;

            case AUDIT_NONE:
                ret.setIsAudited(false);
                break;

            default:
                if (CollectionUtils.isEmpty(repository.getPolicies()) && policyEngine.getTagPolicyRepository() == null) {
                    ret.setIsAudited(true);
                }

                break;
        }

        if (isAuditExcludedUser(request.getUser(), request.getUserGroups(), RangerAccessRequestUtil.getCurrentUserRolesFromContext(request.getContext()))) {
            ret.setIsAudited(false);
        }

        return ret;
    }

    private boolean isAuditExcludedUser(String userName, Set<String> userGroups, Set<String> userRoles) {
        boolean ret = serviceConfig.isAuditExcludedUser(userName);

        if (!ret) {
            RangerPluginConfig pluginConfig = policyEngine.getPluginContext().getConfig();

            ret = pluginConfig.isAuditExcludedUser(userName);

            if (!ret && userGroups != null && !userGroups.isEmpty()) {
                ret = serviceConfig.hasAuditExcludedGroup(userGroups) || pluginConfig.hasAuditExcludedGroup(userGroups);
            }

            if (!ret && userRoles != null && !userRoles.isEmpty()) {
                ret = serviceConfig.hasAuditExcludedRole(userRoles) || pluginConfig.hasAuditExcludedRole(userRoles);
            }
        }

        return ret;
    }

    private boolean isSuperUser(String userName, Set<String> userGroups) {
        boolean ret = serviceConfig.isSuperUser(userName);

        if (!ret) {
            RangerPluginConfig pluginConfig = policyEngine.getPluginContext().getConfig();

            ret = pluginConfig.isSuperUser(userName);

            if (!ret && userGroups != null && !userGroups.isEmpty()) {
                ret = serviceConfig.hasSuperGroup(userGroups) || pluginConfig.hasSuperGroup(userGroups);
            }
        }

        return ret;
    }

    private void getResourceACLEvaluatorsForZone(RangerAccessRequest request, String zoneName, int policyType, List<RangerPolicyEvaluator> allEvaluators, Map<Long, MatchType> tagMatchTypeMap, Set<Long> policyIdForTemporalTags) {
        final RangerPolicyRepository matchedRepository = policyEngine.getRepositoryForZone(zoneName);

        if (matchedRepository == null) {
            LOG.error("policyRepository for zoneName:[{}], serviceName:[{}], policyVersion:[{}] is null!! ERROR!", zoneName, policyEngine.getPolicyRepository().getServiceName(), getPolicyVersion());
        } else {
            Set<RangerTagForEval>       tags                = RangerAccessRequestUtil.getRequestTagsFromContext(request.getContext());
            List<PolicyEvaluatorForTag> tagPolicyEvaluators = policyEngine.getTagPolicyRepository() == null ? null : policyEngine.getTagPolicyRepository().getLikelyMatchPolicyEvaluators(request, tags, policyType, null);

            if (CollectionUtils.isNotEmpty(tagPolicyEvaluators)) {
                tagPolicyEvaluators.sort(MATCH_TYPE_COMPARATOR);

                final boolean useTagPoliciesFromDefaultZone = !policyEngine.isResourceZoneAssociatedWithTagService(zoneName);

                for (PolicyEvaluatorForTag tagEvaluator : tagPolicyEvaluators) {
                    RangerPolicyEvaluator evaluator      = tagEvaluator.getEvaluator();
                    String                policyZoneName = evaluator.getPolicy().getZoneName();

                    if (useTagPoliciesFromDefaultZone) {
                        if (StringUtils.isNotEmpty(policyZoneName)) {
                            LOG.debug("Tag policy [zone:{}] does not belong to default zone. Not evaluating this policy:[{}]", policyZoneName, evaluator.getPolicy());

                            continue;
                        }
                    } else {
                        if (!StringUtils.equals(zoneName, policyZoneName)) {
                            LOG.debug("Tag policy [zone:{}] does not belong to the zone:[{}] of the accessed resource. Not evaluating this policy:[{}]", policyZoneName, zoneName, evaluator.getPolicy());

                            continue;
                        }
                    }

                    RangerTagForEval tag = tagEvaluator.getTag();

                    // avoid an evaluator making into the list multiple times when the same tag is associated with the resource multiple times
                    // highest precedence matchType will be recorded in tagMatchTypeMap, since tagPolicyEvaluators is sorted by matchType
                    if (tagMatchTypeMap.putIfAbsent(evaluator.getPolicyId(), tag.getMatchType()) == null) {
                        allEvaluators.add(evaluator);
                    }

                    if (CollectionUtils.isNotEmpty(tag.getValidityPeriods())) {
                        policyIdForTemporalTags.add(evaluator.getPolicyId());
                    }
                }
            }

            List<RangerPolicyEvaluator> resourcePolicyEvaluators = matchedRepository.getLikelyMatchPolicyEvaluators(request, policyType);

            allEvaluators.addAll(resourcePolicyEvaluators);
        }
    }

    private void getResourceAccessInfoForZone(RangerAccessRequest request, RangerResourceAccessInfo ret, String zoneName) {
        final RangerPolicyRepository matchedRepository = policyEngine.getRepositoryForZone(zoneName);

        if (matchedRepository == null) {
            LOG.error("policyRepository for zoneName:[{}], serviceName:[{}], policyVersion:[{}] is null!! ERROR!", zoneName, policyEngine.getPolicyRepository().getServiceName(), getPolicyVersion());
        } else {
            List<RangerPolicyEvaluator> tagPolicyEvaluators = policyEngine.getTagPolicyRepository() == null ? null : policyEngine.getTagPolicyRepository().getPolicyEvaluators();

            if (CollectionUtils.isNotEmpty(tagPolicyEvaluators)) {
                Set<RangerTagForEval> tags = RangerAccessRequestUtil.getRequestTagsFromContext(request.getContext());

                if (CollectionUtils.isNotEmpty(tags)) {
                    final boolean useTagPoliciesFromDefaultZone = !policyEngine.isResourceZoneAssociatedWithTagService(zoneName);

                    for (RangerTagForEval tag : tags) {
                        RangerAccessRequest         tagEvalRequest = new RangerTagAccessRequest(tag, policyEngine.getTagPolicyRepository().getServiceDef(), request);
                        List<RangerPolicyEvaluator> evaluators     = policyEngine.getTagPolicyRepository().getLikelyMatchPolicyEvaluators(tagEvalRequest, RangerPolicy.POLICY_TYPE_ACCESS);

                        for (RangerPolicyEvaluator evaluator : evaluators) {
                            String policyZoneName = evaluator.getPolicy().getZoneName();

                            if (useTagPoliciesFromDefaultZone) {
                                if (StringUtils.isNotEmpty(policyZoneName)) {
                                    LOG.debug("Tag policy [zone:{}] does not belong to default zone. Not evaluating this policy:[{}]", policyZoneName, evaluator.getPolicy());

                                    continue;
                                }
                            } else {
                                if (!StringUtils.equals(zoneName, policyZoneName)) {
                                    LOG.debug("Tag policy [zone:{}] does not belong to the zone:[{}] of the accessed resource. Not evaluating this policy:[{}]", policyZoneName, zoneName, evaluator.getPolicy());

                                    continue;
                                }
                            }

                            evaluator.getResourceAccessInfo(tagEvalRequest, ret);
                        }
                    }
                }
            }

            List<RangerPolicyEvaluator> resPolicyEvaluators = matchedRepository.getLikelyMatchPolicyEvaluators(request, RangerPolicy.POLICY_TYPE_ACCESS);

            if (CollectionUtils.isNotEmpty(resPolicyEvaluators)) {
                for (RangerPolicyEvaluator evaluator : resPolicyEvaluators) {
                    evaluator.getResourceAccessInfo(request, ret);
                }
            }

            ret.getAllowedUsers().removeAll(ret.getDeniedUsers());
            ret.getAllowedGroups().removeAll(ret.getDeniedGroups());
        }
    }

    private void evaluateTagAuditPolicies(RangerAccessRequest request, RangerAccessResult result, RangerPolicyRepository tagPolicyRepository) {
        LOG.debug("==> RangerPolicyEngineImpl.evaluateTagAuditPolicies(request={}, result={})", request, result);

        Set<RangerTagForEval> tags = RangerAccessRequestUtil.getRequestTagsFromContext(request.getContext());

        if (CollectionUtils.isNotEmpty(tags)) {
            Date                        accessTime = request.getAccessTime() != null ? request.getAccessTime() : new Date();
            List<PolicyEvaluatorForTag> evaluators = tagPolicyRepository.getLikelyMatchPolicyEvaluators(request, tags, RangerPolicy.POLICY_TYPE_AUDIT, accessTime);

            if (CollectionUtils.isNotEmpty(evaluators)) {
                for (PolicyEvaluatorForTag policyEvaluator : evaluators) {
                    RangerPolicyEvaluator evaluator      = policyEvaluator.getEvaluator();
                    RangerTagForEval      tag            = policyEvaluator.getTag();
                    RangerAccessRequest   tagEvalRequest = new RangerTagAccessRequest(tag, tagPolicyRepository.getServiceDef(), request);
                    RangerAccessResult    tagEvalResult  = createAccessResult(tagEvalRequest, RangerPolicy.POLICY_TYPE_AUDIT);

                    LOG.debug("RangerPolicyEngineImpl.evaluateTagAuditPolicies: Evaluating Audit policies for tag ({}) Tag Evaluator: {}", tag.getType(), policyEvaluator);

                    tagEvalResult.setAccessResultFrom(result);

                    result.incrementEvaluatedPoliciesCount();

                    evaluator.evaluate(tagEvalRequest, tagEvalResult);

                    if (tagEvalResult.getIsAuditedDetermined()) {
                        result.setIsAudited(tagEvalResult.getIsAudited());
                        break;
                    }
                }
            }
        }

        LOG.debug("<== RangerPolicyEngineImpl.evaluateTagAuditPolicies(request={}, result={})", request, result);
    }

    private boolean evaluateResourceAuditPolicies(RangerAccessRequest request, RangerAccessResult result, RangerPolicyRepository policyRepository) {
        LOG.debug("==> RangerPolicyEngineImpl.evaluateResourceAuditPolicies(request={}, result={})", request, result);

        boolean                     ret        = false;
        List<RangerPolicyEvaluator> evaluators = policyRepository.getLikelyMatchAuditPolicyEvaluators(request);

        if (CollectionUtils.isNotEmpty(evaluators)) {
            for (RangerPolicyEvaluator evaluator : evaluators) {
                LOG.debug("==> RangerPolicyEngineImpl.evaluateResourceAuditPolicies(): Evaluating RangerPolicyEvaluator...: {}", evaluator);

                result.incrementEvaluatedPoliciesCount();

                evaluator.evaluate(request, result);

                if (result.getIsAuditedDetermined()) {
                    ret = true;

                    break;
                }
            }
        }

        LOG.debug("<== RangerPolicyEngineImpl.evaluateResourceAuditPolicies(request={}, result={}): ret={}", request, result, ret);

        return ret;
    }

    private boolean getIsFallbackSupported() {
        return policyEngine.getPluginContext().getConfig().getIsFallbackSupported();
    }

    private void updateFromGdsResult(RangerAccessResult result) {
        LOG.debug("==> updateFromGdsResult(result={})", result);

        RangerAccessRequest request   = result.getAccessRequest();
        GdsAccessResult     gdsResult = request != null ? RangerAccessRequestUtil.getGdsResultFromContext(request.getContext()) : null;

        if (gdsResult != null) {
            if (result.getPolicyType() == RangerPolicy.POLICY_TYPE_ACCESS) {
                if (!result.getIsAccessDetermined() && gdsResult.getIsAllowed()) {
                    result.setIsAllowed(true);
                    result.setIsAccessDetermined(true);
                    result.setPolicyId(gdsResult.getPolicyId());
                    result.setPolicyVersion(gdsResult.getPolicyVersion());
                    result.setPolicyPriority(RangerPolicy.POLICY_PRIORITY_NORMAL);
                }
            }

            if (!result.getIsAuditedDetermined() && gdsResult.getIsAudited()) {
                result.setIsAudited(true);
            }

            result.setDatasets(gdsResult.getDatasets());
            result.setProjects(gdsResult.getProjects());
        } else {
            LOG.debug("updateFromGdsResult(): no GdsAccessResult found in request context({})", request);
        }

        LOG.debug("<== updateFromGdsResult(result={})", result);
    }

    private static class ServiceConfig {
        private final Set<String> auditExcludedUsers;
        private final Set<String> auditExcludedGroups;
        private final Set<String> auditExcludedRoles;
        private final Set<String> superUsers;
        private final Set<String> superGroups;
        private final Set<String> serviceAdmins;

        public ServiceConfig(Map<String, String> svcConfig) {
            if (svcConfig != null) {
                auditExcludedUsers  = StringUtil.toSet(svcConfig.get(RangerPolicyEngine.PLUGIN_AUDIT_EXCLUDE_USERS));
                auditExcludedGroups = StringUtil.toSet(svcConfig.get(RangerPolicyEngine.PLUGIN_AUDIT_EXCLUDE_GROUPS));
                auditExcludedRoles  = StringUtil.toSet(svcConfig.get(RangerPolicyEngine.PLUGIN_AUDIT_EXCLUDE_ROLES));
                superUsers          = StringUtil.toSet(svcConfig.get(RangerPolicyEngine.PLUGIN_SUPER_USERS));
                superGroups         = StringUtil.toSet(svcConfig.get(RangerPolicyEngine.PLUGIN_SUPER_GROUPS));
                serviceAdmins       = StringUtil.toSet(svcConfig.get(RangerPolicyEngine.PLUGIN_SERVICE_ADMINS));
            } else {
                auditExcludedUsers  = Collections.emptySet();
                auditExcludedGroups = Collections.emptySet();
                auditExcludedRoles  = Collections.emptySet();
                superUsers          = Collections.emptySet();
                superGroups         = Collections.emptySet();
                serviceAdmins       = Collections.emptySet();
            }
        }

        public ServiceConfig(ServiceConfig other) {
            auditExcludedUsers  = other == null || CollectionUtils.isEmpty(other.auditExcludedUsers) ? Collections.emptySet() : new HashSet<>(other.auditExcludedUsers);
            auditExcludedGroups = other == null || CollectionUtils.isEmpty(other.auditExcludedGroups) ? Collections.emptySet() : new HashSet<>(other.auditExcludedGroups);
            auditExcludedRoles  = other == null || CollectionUtils.isEmpty(other.auditExcludedRoles) ? Collections.emptySet() : new HashSet<>(other.auditExcludedRoles);
            superUsers          = other == null || CollectionUtils.isEmpty(other.superUsers) ? Collections.emptySet() : new HashSet<>(other.superUsers);
            superGroups         = other == null || CollectionUtils.isEmpty(other.superGroups) ? Collections.emptySet() : new HashSet<>(other.superGroups);
            serviceAdmins       = other == null || CollectionUtils.isEmpty(other.serviceAdmins) ? Collections.emptySet() : new HashSet<>(other.serviceAdmins);
        }

        public boolean isAuditExcludedUser(String userName) {
            return auditExcludedUsers.contains(userName);
        }

        public boolean hasAuditExcludedGroup(Set<String> userGroups) {
            return userGroups != null && !userGroups.isEmpty() && !auditExcludedGroups.isEmpty() && CollectionUtils.containsAny(userGroups, auditExcludedGroups);
        }

        public boolean hasAuditExcludedRole(Set<String> userRoles) {
            return userRoles != null && !userRoles.isEmpty() && !auditExcludedRoles.isEmpty() && CollectionUtils.containsAny(userRoles, auditExcludedRoles);
        }

        public boolean isSuperUser(String userName) {
            return superUsers.contains(userName);
        }

        public boolean hasSuperGroup(Set<String> userGroups) {
            return userGroups != null && !userGroups.isEmpty() && !superGroups.isEmpty() && CollectionUtils.containsAny(userGroups, superGroups);
        }

        public boolean isServiceAdmin(String userName) {
            return serviceAdmins.contains(userName);
        }
    }
}
