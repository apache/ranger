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
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ranger.plugin.contextenricher.RangerAbstractContextEnricher;
import org.apache.ranger.plugin.contextenricher.RangerContextEnricher;
import org.apache.ranger.plugin.contextenricher.RangerTagEnricher;
import org.apache.ranger.plugin.contextenricher.RangerTagForEval;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItemDataMaskInfo;
import org.apache.ranger.plugin.model.RangerPolicyDelta;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.model.validation.RangerServiceDefHelper;
import org.apache.ranger.plugin.policyevaluator.RangerAbstractPolicyEvaluator;
import org.apache.ranger.plugin.policyevaluator.RangerCachedPolicyEvaluator;
import org.apache.ranger.plugin.policyevaluator.RangerOptimizedPolicyEvaluator;
import org.apache.ranger.plugin.policyevaluator.RangerPolicyEvaluator;
import org.apache.ranger.plugin.store.AbstractServiceStore;
import org.apache.ranger.plugin.util.RangerPerfTracer;
import org.apache.ranger.plugin.util.ServiceDefUtil;
import org.apache.ranger.plugin.util.ServicePolicies;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class RangerPolicyRepository {
    private static final Log LOG = LogFactory.getLog(RangerPolicyRepository.class);

    private static final Log PERF_CONTEXTENRICHER_INIT_LOG = RangerPerfTracer.getPerfLogger("contextenricher.init");
    private static final Log PERF_TRIE_OP_LOG              = RangerPerfTracer.getPerfLogger("resourcetrie.retrieval");

    enum AuditModeEnum {
        AUDIT_ALL, AUDIT_NONE, AUDIT_DEFAULT
    }

    private final String                            serviceName;
    private final String                            zoneName;
    private final String                            appId;
    private final RangerPolicyEngineOptions         options;
    private final RangerPluginContext               pluginContext;
    private final RangerServiceDef                  serviceDef;
    private final List<RangerPolicy>                policies;
    private final long                              policyVersion;
    private final List<RangerContextEnricher>       contextEnrichers;
    private final AuditModeEnum                     auditModeEnum;
    private final Map<String, AuditInfo>            accessAuditCache;
    private final String                            componentServiceName;
    private final RangerServiceDef                  componentServiceDef;
    private final Map<String, RangerResourceTrie>   policyResourceTrie;
    private final Map<String, RangerResourceTrie>   dataMaskResourceTrie;
    private final Map<String, RangerResourceTrie>   rowFilterResourceTrie;
    private       List<RangerPolicyEvaluator>       policyEvaluators;
    private       List<RangerPolicyEvaluator>       dataMaskPolicyEvaluators;
    private       List<RangerPolicyEvaluator>       rowFilterPolicyEvaluators;
    private       Map<Long, RangerPolicyEvaluator>  policyEvaluatorsMap;
    private       boolean                           isContextEnrichersShared = false;
    private       boolean                           isPreCleaned             = false;

    RangerPolicyRepository(final RangerPolicyRepository other, final List<RangerPolicyDelta> deltas, long policyVersion) {
        this.serviceName               = other.serviceName;
        this.zoneName                  = other.zoneName;
        this.appId                     = other.appId;
        this.options                   = other.options;
        this.pluginContext             = other.pluginContext;
        this.serviceDef                = other.serviceDef;
        this.policies                  = new ArrayList<>(other.policies);
        this.policyEvaluators          = new ArrayList<>(other.policyEvaluators);
        this.dataMaskPolicyEvaluators  = new ArrayList<>(other.dataMaskPolicyEvaluators);
        this.rowFilterPolicyEvaluators = new ArrayList<>(other.rowFilterPolicyEvaluators);
        this.auditModeEnum             = other.auditModeEnum;
        this.componentServiceName      = other.componentServiceName;
        this.componentServiceDef       = other.componentServiceDef;
        this.policyEvaluatorsMap       = new HashMap<>(other.policyEvaluatorsMap);

        if (other.policyResourceTrie != null) {
            this.policyResourceTrie = new HashMap<>();

            for (Map.Entry<String, RangerResourceTrie> entry : other.policyResourceTrie.entrySet()) {
                policyResourceTrie.put(entry.getKey(), new RangerResourceTrie(entry.getValue()));
            }
        } else {
            this.policyResourceTrie = null;
        }

        if (other.dataMaskResourceTrie != null) {
            this.dataMaskResourceTrie = new HashMap<>();

            for (Map.Entry<String, RangerResourceTrie> entry : other.dataMaskResourceTrie.entrySet()) {
                dataMaskResourceTrie.put(entry.getKey(), new RangerResourceTrie(entry.getValue()));
            }
        } else {
            this.dataMaskResourceTrie = null;
        }

        if (other.rowFilterResourceTrie != null) {
            this.rowFilterResourceTrie = new HashMap<>();

            for (Map.Entry<String, RangerResourceTrie> entry : other.rowFilterResourceTrie.entrySet()) {
                rowFilterResourceTrie.put(entry.getKey(), new RangerResourceTrie(entry.getValue()));
            }
        } else {
            this.rowFilterResourceTrie = null;
        }

        if (other.accessAuditCache != null) {
            int auditResultCacheSize = other.accessAuditCache.size();

            this.accessAuditCache = Collections.synchronizedMap(new CacheMap<String, AuditInfo>(auditResultCacheSize));
        } else {
            this.accessAuditCache = null;
        }

        boolean[] flags = new boolean[RangerPolicy.POLICY_TYPES.length];

        for (RangerPolicyDelta delta : deltas) {
            final Integer changeType  = delta.getChangeType();
            final String  serviceType = delta.getServiceType();
            final Long    policyId    = delta.getPolicyId();
            final Integer policyType  = delta.getPolicyType();

            if (!serviceType.equals(this.serviceDef.getName())) {
                continue;
            }

            RangerPolicyEvaluator evaluator = null;

            switch (changeType) {
                case RangerPolicyDelta.CHANGE_TYPE_POLICY_CREATE:
                    if (delta.getPolicy() == null) {
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("Could not find policy for policy-id:[" + policyId + "]");
                        }

                        continue;
                    }
                    break;

                case RangerPolicyDelta.CHANGE_TYPE_POLICY_UPDATE:
                    evaluator = getPolicyEvaluator(policyId);

                    if (evaluator == null) {
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("Could not find evaluator for policy-id:[" + policyId + "]");
                        }
                    }
                    break;

                case RangerPolicyDelta.CHANGE_TYPE_POLICY_DELETE:
                    evaluator = getPolicyEvaluator(policyId);
                    if (evaluator == null) {
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("Could not find evaluator for policy-id:[" + policyId + "]");
                        }
                    }
                    break;

                default:
                    LOG.error("Unknown changeType:[" + changeType + "], Ignoring");
                    break;
            }

            evaluator = update(delta, evaluator);

            if (evaluator != null) {
                switch (changeType) {
                    case RangerPolicyDelta.CHANGE_TYPE_POLICY_CREATE:
                        policyEvaluatorsMap.put(policyId, evaluator);
                        break;

                    case RangerPolicyDelta.CHANGE_TYPE_POLICY_UPDATE:
                        policyEvaluatorsMap.put(policyId, evaluator);
                        break;

                    case RangerPolicyDelta.CHANGE_TYPE_POLICY_DELETE:
                        policyEvaluatorsMap.remove(policyId);
                        break;

                    default:
                        break;
                }

                flags[policyType] = true;
            }
        }

        for (int policyType = 0; policyType < flags.length; policyType++) {
            if (flags[policyType]) {
                Map<String, RangerResourceTrie> trie = getTrie(policyType);

                if (trie != null) {
                    for (Map.Entry<String, RangerResourceTrie> entry : trie.entrySet()) {
                        entry.getValue().wrapUpUpdate();
                    }
                }
            }
        }

        if (StringUtils.isEmpty(zoneName)) {
            if (CollectionUtils.isNotEmpty(other.getPolicies())) {
                if (CollectionUtils.isNotEmpty(this.getPolicies())) {
                    this.contextEnrichers = shareWith(other);
                } else {
                    this.contextEnrichers = null;
                }
            } else {
                if (CollectionUtils.isNotEmpty(this.policies)) {
                    this.contextEnrichers = Collections.unmodifiableList(buildContextEnrichers(options));
                } else {
                    this.contextEnrichers = null;
                }
            }
        } else {
            this.contextEnrichers = null;
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("other.serviceName=" + other.serviceName + ", other.isContextEnrichersShared=" + other.isContextEnrichersShared + ", Context-enrichers are " + (CollectionUtils.isNotEmpty(contextEnrichers) ? " not empty " : "empty "));
        }

        this.policyVersion = policyVersion;
    }

    RangerPolicyRepository(ServicePolicies servicePolicies, RangerPluginContext pluginContext) {
        this(servicePolicies, pluginContext, null);
    }

    RangerPolicyRepository(ServicePolicies servicePolicies, RangerPluginContext pluginContext, String zoneName) {
        super();

        this.componentServiceName = this.serviceName = servicePolicies.getServiceName();
        this.componentServiceDef  = this.serviceDef = ServiceDefUtil.normalize(servicePolicies.getServiceDef());
        this.zoneName             = zoneName;
        this.appId                = pluginContext.getConfig().getAppId();
        this.options              = new RangerPolicyEngineOptions(pluginContext.getConfig().getPolicyEngineOptions());
        this.pluginContext        = pluginContext;

        if (StringUtils.isEmpty(zoneName)) {
            this.policies = Collections.unmodifiableList(servicePolicies.getPolicies());
        } else {
            this.policies = Collections.unmodifiableList(servicePolicies.getSecurityZones().get(zoneName).getPolicies());
        }
        this.policyVersion = servicePolicies.getPolicyVersion() != null ? servicePolicies.getPolicyVersion() : -1;

        String auditMode = servicePolicies.getAuditMode();

        if (StringUtils.equals(auditMode, RangerPolicyEngine.AUDIT_ALL)) {
            auditModeEnum = AuditModeEnum.AUDIT_ALL;
        } else if (StringUtils.equals(auditMode, RangerPolicyEngine.AUDIT_NONE)) {
            auditModeEnum = AuditModeEnum.AUDIT_NONE;
        } else {
            auditModeEnum = AuditModeEnum.AUDIT_DEFAULT;
        }

        if (auditModeEnum == AuditModeEnum.AUDIT_DEFAULT) {
            String propertyName = "ranger.plugin." + serviceName + ".policyengine.auditcachesize";

            if (options.cacheAuditResults) {
                final int RANGER_POLICYENGINE_AUDITRESULT_CACHE_SIZE = 64 * 1024;

                int auditResultCacheSize = pluginContext.getConfig().getInt(propertyName, RANGER_POLICYENGINE_AUDITRESULT_CACHE_SIZE);
                accessAuditCache = Collections.synchronizedMap(new CacheMap<String, AuditInfo>(auditResultCacheSize));
            } else {
                accessAuditCache = null;
            }
        } else {
            this.accessAuditCache = null;
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("RangerPolicyRepository : building policy-repository for service[" + serviceName + "], and zone:[" + zoneName + "] with auditMode[" + auditModeEnum + "]");
        }

        init(options);

        if (StringUtils.isEmpty(zoneName)) {
            this.contextEnrichers = Collections.unmodifiableList(buildContextEnrichers(options));
        } else {
            this.contextEnrichers = null;
        }

        if (options.disableTrieLookupPrefilter) {
            policyResourceTrie    = null;
            dataMaskResourceTrie  = null;
            rowFilterResourceTrie = null;
        } else {
            policyResourceTrie    = createResourceTrieMap(policyEvaluators, options.optimizeTrieForRetrieval);
            dataMaskResourceTrie  = createResourceTrieMap(dataMaskPolicyEvaluators, options.optimizeTrieForRetrieval);
            rowFilterResourceTrie = createResourceTrieMap(rowFilterPolicyEvaluators, options.optimizeTrieForRetrieval);
        }
    }

    RangerPolicyRepository(ServicePolicies.TagPolicies tagPolicies, RangerPluginContext pluginContext,
                           RangerServiceDef componentServiceDef, String componentServiceName) {
        super();

        this.serviceName          = tagPolicies.getServiceName();
        this.componentServiceName = componentServiceName;
        this.zoneName             = null;
        this.serviceDef           = ServiceDefUtil.normalizeAccessTypeDefs(ServiceDefUtil.normalize(tagPolicies.getServiceDef()), componentServiceDef.getName());
        this.componentServiceDef  = componentServiceDef;
        this.appId                = pluginContext.getConfig().getAppId();
        this.options              = new RangerPolicyEngineOptions(pluginContext.getConfig().getPolicyEngineOptions());
        this.pluginContext        = pluginContext;
        this.policies             = Collections.unmodifiableList(normalizeAndPrunePolicies(tagPolicies.getPolicies(), componentServiceDef.getName()));
        this.policyVersion        = tagPolicies.getPolicyVersion() != null ? tagPolicies.getPolicyVersion() : -1;

        String auditMode = tagPolicies.getAuditMode();

        if (StringUtils.equals(auditMode, RangerPolicyEngine.AUDIT_ALL)) {
            auditModeEnum = AuditModeEnum.AUDIT_ALL;
        } else if (StringUtils.equals(auditMode, RangerPolicyEngine.AUDIT_NONE)) {
            auditModeEnum = AuditModeEnum.AUDIT_NONE;
        } else {
            auditModeEnum = AuditModeEnum.AUDIT_DEFAULT;
        }

        this.accessAuditCache = null;

        if (LOG.isDebugEnabled()) {
            LOG.debug("RangerPolicyRepository : building tag-policy-repository for tag service:[" + serviceName +"], with auditMode[" + auditModeEnum +"]");
        }

        init(options);

        if (StringUtils.isEmpty(zoneName)) {
            this.contextEnrichers = Collections.unmodifiableList(buildContextEnrichers(options));
        } else {
            this.contextEnrichers = null;
        }

        if (options.disableTrieLookupPrefilter) {
            policyResourceTrie    = null;
            dataMaskResourceTrie  = null;
            rowFilterResourceTrie = null;
        } else {
            policyResourceTrie    = createResourceTrieMap(policyEvaluators, options.optimizeTrieForRetrieval);
            dataMaskResourceTrie  = createResourceTrieMap(dataMaskPolicyEvaluators, options.optimizeTrieForRetrieval);
            rowFilterResourceTrie = createResourceTrieMap(rowFilterPolicyEvaluators, options.optimizeTrieForRetrieval);
        }
    }

    @Override
    public String toString( ) {
        StringBuilder sb = new StringBuilder();

        toString(sb);

        return sb.toString();
    }

    public StringBuilder toString(StringBuilder sb) {
        if (sb == null) {
            sb = new StringBuilder();
        }

        sb.append("RangerPolicyRepository={");

        sb.append("serviceName={").append(serviceName).append("} ");
        sb.append("zoneName={").append(zoneName).append("} ");
        sb.append("serviceDef={").append(serviceDef).append("} ");
        sb.append("appId={").append(appId).append("} ");

        sb.append("policyEvaluators={");
        if (policyEvaluators != null) {
            for (RangerPolicyEvaluator policyEvaluator : policyEvaluators) {
                if (policyEvaluator != null) {
                    sb.append(policyEvaluator).append(" ");
                }
            }
        }
        sb.append("} ");

        sb.append("dataMaskPolicyEvaluators={");
        if (this.dataMaskPolicyEvaluators != null) {
            for (RangerPolicyEvaluator policyEvaluator : dataMaskPolicyEvaluators) {
                if (policyEvaluator != null) {
                    sb.append(policyEvaluator).append(" ");
                }
            }
        }
        sb.append("} ");

        sb.append("rowFilterPolicyEvaluators={");
        if (this.rowFilterPolicyEvaluators != null) {
            for (RangerPolicyEvaluator policyEvaluator : rowFilterPolicyEvaluators) {
                if (policyEvaluator != null) {
                    sb.append(policyEvaluator).append(" ");
                }
            }
        }
        sb.append("} ");

        sb.append("contextEnrichers={");
        if (contextEnrichers != null) {
            for (RangerContextEnricher contextEnricher : contextEnrichers) {
                if (contextEnricher != null) {
                    sb.append(contextEnricher).append(" ");
                }
            }
        }
        sb.append("} ");

        sb.append("} ");

        return sb;
    }

    List<RangerContextEnricher> shareWith(RangerPolicyRepository other) {
        if (other != null && other.contextEnrichers != null) {
            other.setShared();
        }
        return other == null ? null : other.contextEnrichers;
    }

    void setShared() {
        isContextEnrichersShared = true;
    }

    void preCleanup(boolean isForced) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> preCleanup(isForced=" + isForced + " )");
            LOG.debug("Repository holds [" + (CollectionUtils.isEmpty(this.contextEnrichers) ? 0 : this.contextEnrichers.size()) + "] enrichers. isPreCleaned=" + isPreCleaned);
        }
        if (!isPreCleaned) {
            if (CollectionUtils.isNotEmpty(this.contextEnrichers) && (!isContextEnrichersShared || isForced)) {
                isPreCleaned = true;
                if (LOG.isDebugEnabled()) {
                    LOG.debug("preCleaning context-enrichers");
                }
                for (RangerContextEnricher enricher : this.contextEnrichers) {
                    enricher.preCleanup();
                }
            } else {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("No preCleaning of context-enrichers; Context-enrichers are " + (CollectionUtils.isNotEmpty(contextEnrichers) ? " not empty " : "empty ") + ", isContextEnrichersShared=" + isContextEnrichersShared + ", isForced=" + isForced + ")");
                }
            }
        } else {
            if (LOG.isDebugEnabled()) {
                LOG.debug("preCleanup() already done. No need to do it again");
            }
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("<== preCleanup(isForced=" + isForced + " )");
        }
    }

    void cleanup() {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> cleanup()");
        }
        preCleanup(false);

        if (CollectionUtils.isNotEmpty(this.contextEnrichers) && !isContextEnrichersShared) {
            for (RangerContextEnricher enricher : this.contextEnrichers) {
                enricher.cleanup();
            }
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("<== cleanup()");
        }
    }

    @Override
    protected void finalize() throws Throwable {
        try {
            cleanup();
        } finally {
            super.finalize();
        }
    }

    void reorderPolicyEvaluators() {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> reorderEvaluators()");
        }

        if (policyResourceTrie == null) {
            policyEvaluators = getReorderedPolicyEvaluators(policyEvaluators);
        }

        if (dataMaskResourceTrie == null) {
            dataMaskPolicyEvaluators = getReorderedPolicyEvaluators(dataMaskPolicyEvaluators);
        }

        if (rowFilterResourceTrie == null) {
            rowFilterPolicyEvaluators = getReorderedPolicyEvaluators(rowFilterPolicyEvaluators);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== reorderEvaluators()");
        }
    }

    public String getServiceName() { return serviceName; }

    String getZoneName() { return zoneName; }

    public RangerServiceDef getServiceDef() {
        return serviceDef;
    }

    List<RangerPolicy> getPolicies() {
        return policies;
    }

    long getPolicyVersion() {
        return policyVersion;
    }

    AuditModeEnum getAuditModeEnum() { return auditModeEnum; }

    boolean setAuditEnabledFromCache(RangerAccessRequest request, RangerAccessResult result) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> RangerPolicyRepository.setAuditEnabledFromCache()");
        }

        final AuditInfo auditInfo = accessAuditCache != null ? accessAuditCache.get(request.getResource().getAsString()) : null;

        if (auditInfo != null) {
            result.setIsAudited(auditInfo.getIsAudited());
            result.setAuditPolicyId(auditInfo.getAuditPolicyId());
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== RangerPolicyRepository.setAuditEnabledFromCache():" + (auditInfo != null));
        }

        return auditInfo != null;
    }

    void storeAuditEnabledInCache(RangerAccessRequest request, RangerAccessResult result) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> RangerPolicyRepository.storeAuditEnabledInCache()");
        }

        if (accessAuditCache != null && result.getIsAuditedDetermined()) {
            accessAuditCache.put(request.getResource().getAsString(), new AuditInfo(result.getIsAudited(), result.getAuditPolicyId()));
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== RangerPolicyRepository.storeAuditEnabledInCache()");
        }
    }

    List<RangerContextEnricher> getContextEnrichers() { return contextEnrichers; }

    List<RangerPolicyEvaluator> getPolicyEvaluators(int policyType) {
        switch (policyType) {
            case RangerPolicy.POLICY_TYPE_ACCESS:
                return getPolicyEvaluators();
            case RangerPolicy.POLICY_TYPE_DATAMASK:
                return getDataMaskPolicyEvaluators();
            case RangerPolicy.POLICY_TYPE_ROWFILTER:
                return getRowFilterPolicyEvaluators();
            default:
                return getPolicyEvaluators();
        }
    }

    public List<RangerPolicyEvaluator> getPolicyEvaluators() {
        return policyEvaluators;
    }

    List<RangerPolicyEvaluator> getDataMaskPolicyEvaluators() {
        return dataMaskPolicyEvaluators;
    }

    List<RangerPolicyEvaluator> getRowFilterPolicyEvaluators() {
	    return rowFilterPolicyEvaluators;
    }

    String getAppId() { return appId; }

    RangerPolicyEngineOptions getOptions() { return options; }

    List<PolicyEvaluatorForTag> getLikelyMatchPolicyEvaluators(Set<RangerTagForEval> tags, int policyType, Date accessTime) {
        List<PolicyEvaluatorForTag> ret = Collections.EMPTY_LIST;

        if (CollectionUtils.isNotEmpty(tags) && getServiceDef() != null) {

            ret = new ArrayList<PolicyEvaluatorForTag>();

            for (RangerTagForEval tag : tags) {
            	if (tag.isApplicable(accessTime)) {
		            RangerAccessResource resource = new RangerTagResource(tag.getType(), getServiceDef());
		            List<RangerPolicyEvaluator> evaluators = getLikelyMatchPolicyEvaluators(resource, policyType);

		            if (CollectionUtils.isNotEmpty(evaluators)) {
			            for (RangerPolicyEvaluator evaluator : evaluators) {
			                if (evaluator.isApplicable(accessTime)) {
                                ret.add(new PolicyEvaluatorForTag(evaluator, tag));
                            }
			            }
		            }
	            } else {
            		if (LOG.isDebugEnabled()) {
            			LOG.debug("Tag:[" + tag.getType() + "] is not applicable at accessTime:[" + accessTime +"]");
		            }
	            }
            }

            if (CollectionUtils.isNotEmpty(ret)) {
                switch (policyType) {
                    case RangerPolicy.POLICY_TYPE_ACCESS:
                        Collections.sort(ret, PolicyEvaluatorForTag.EVAL_ORDER_COMPARATOR);
                        break;
                    case RangerPolicy.POLICY_TYPE_DATAMASK:
                        Collections.sort(ret, PolicyEvaluatorForTag.NAME_COMPARATOR);
                        break;
                    case RangerPolicy.POLICY_TYPE_ROWFILTER:
                        Collections.sort(ret, PolicyEvaluatorForTag.NAME_COMPARATOR);
                        break;
                    default:
                        LOG.warn("Unknown policy-type:[" + policyType + "]. Ignoring..");
                        break;
                }
            }
        }
        return ret;
    }

    public List<RangerPolicyEvaluator> getLikelyMatchPolicyEvaluators(RangerAccessResource resource) {
        List<RangerPolicyEvaluator> ret = new ArrayList<>();

        for (int policyType : RangerPolicy.POLICY_TYPES) {
            List<RangerPolicyEvaluator> evaluators = getLikelyMatchPolicyEvaluators(resource, policyType);
            if (CollectionUtils.isNotEmpty(evaluators)) {
                ret.addAll(evaluators);
            }
        }
        return ret;
    }

    public List<RangerPolicyEvaluator> getLikelyMatchPolicyEvaluators(RangerAccessResource resource, int policyType) {
        switch (policyType) {
            case RangerPolicy.POLICY_TYPE_ACCESS:
                return getLikelyMatchAccessPolicyEvaluators(resource);
            case RangerPolicy.POLICY_TYPE_DATAMASK:
                return getLikelyMatchDataMaskPolicyEvaluators(resource);
            case RangerPolicy.POLICY_TYPE_ROWFILTER:
                return getLikelyMatchRowFilterPolicyEvaluators(resource);
            default:
                return Collections.EMPTY_LIST;
        }
    }


    Map<Long, RangerPolicyEvaluator> getPolicyEvaluatorsMap() { return policyEvaluatorsMap; }

    RangerPolicyEvaluator getPolicyEvaluator(Long id) {
        return policyEvaluatorsMap.get(id);
    }

    private List<RangerPolicyEvaluator> getLikelyMatchAccessPolicyEvaluators(RangerAccessResource resource) {
       String resourceStr = resource == null ? null : resource.getAsString();

       return policyResourceTrie == null || StringUtils.isEmpty(resourceStr)  ? getPolicyEvaluators() : getLikelyMatchPolicyEvaluators(policyResourceTrie, resource);
    }

    private List<RangerPolicyEvaluator> getLikelyMatchDataMaskPolicyEvaluators(RangerAccessResource resource) {
        String resourceStr = resource == null ? null : resource.getAsString();

        return dataMaskResourceTrie == null || StringUtils.isEmpty(resourceStr)  ? getDataMaskPolicyEvaluators() : getLikelyMatchPolicyEvaluators(dataMaskResourceTrie, resource);
    }

    private List<RangerPolicyEvaluator> getLikelyMatchRowFilterPolicyEvaluators(RangerAccessResource resource) {
        String resourceStr = resource == null ? null : resource.getAsString();

        return rowFilterResourceTrie == null || StringUtils.isEmpty(resourceStr)  ? getRowFilterPolicyEvaluators() : getLikelyMatchPolicyEvaluators(rowFilterResourceTrie, resource);
    }

    private List<RangerPolicyEvaluator> getLikelyMatchPolicyEvaluators(Map<String, RangerResourceTrie> resourceTrie, RangerAccessResource resource) {
        List<RangerPolicyEvaluator> ret          = Collections.EMPTY_LIST;

        RangerPerfTracer perf = null;

        if(RangerPerfTracer.isPerfTraceEnabled(PERF_TRIE_OP_LOG)) {
            perf = RangerPerfTracer.getPerfTracer(PERF_TRIE_OP_LOG, "RangerPolicyRepository.getLikelyMatchEvaluators(resource=" + resource.getAsString() + ")");
        }

        Set<String>                 resourceKeys = resource == null ? null : resource.getKeys();

        if(CollectionUtils.isNotEmpty(resourceKeys)) {
            Set<RangerPolicyEvaluator>       evaluators = null;
            List<Set<RangerPolicyEvaluator>> resourceEvaluatorsSet = null;
            Set<RangerPolicyEvaluator>       smallestSet = null;

            for (String resourceName : resourceKeys) {
                RangerResourceTrie trie = resourceTrie.get(resourceName);

                if (trie == null) { // if no trie exists for this resource level, ignore and continue to next level
                    continue;
                }

                Set<RangerPolicyEvaluator> resourceEvaluators = trie.getEvaluatorsForResource(resource.getValue(resourceName));

                if (CollectionUtils.isEmpty(resourceEvaluators)) { // no policies for this resource, bail out
                    resourceEvaluatorsSet = null;
                    smallestSet = null;
                    break;
                }

                if (smallestSet == null) {
                    smallestSet = resourceEvaluators;
                } else {
                    if (resourceEvaluatorsSet == null) {
                        resourceEvaluatorsSet = new ArrayList<>();
                        resourceEvaluatorsSet.add(smallestSet);
                    }
                    resourceEvaluatorsSet.add(resourceEvaluators);

                    if (smallestSet.size() > resourceEvaluators.size()) {
                        smallestSet = resourceEvaluators;
                    }
                }
            }

            if (resourceEvaluatorsSet != null) {
                evaluators = new HashSet<>(smallestSet);
                for (Set<RangerPolicyEvaluator> resourceEvaluators : resourceEvaluatorsSet) {
                    if (resourceEvaluators != smallestSet) {
                        // remove policies from ret that are not in resourceEvaluators
                        evaluators.retainAll(resourceEvaluators);

                        if (CollectionUtils.isEmpty(evaluators)) { // if no policy exists, bail out and return empty list
                            evaluators = null;
                            break;
                        }
                    }
                }
            } else {
                evaluators = smallestSet;
            }

            if (evaluators != null) {
                ret = new ArrayList<>(evaluators);
                ret.sort(RangerPolicyEvaluator.EVAL_ORDER_COMPARATOR);
            }
        }

        RangerPerfTracer.logAlways(perf);

        if(LOG.isDebugEnabled()) {
            LOG.debug("<== RangerPolicyRepository.getLikelyMatchPolicyEvaluators(" + resource.getAsString() + "): evaluatorCount=" + ret.size());
        }

        return ret;
    }

    private List<RangerPolicy> normalizeAndPrunePolicies(List<RangerPolicy> rangerPolicies, final String componentType) {
        if (CollectionUtils.isNotEmpty(rangerPolicies) && StringUtils.isNotBlank(componentType)) {
            List<RangerPolicy> policiesToPrune = null;

            for (RangerPolicy policy : rangerPolicies) {
                if (isPolicyNeedsPruning(policy, componentType)) {

                    if(policiesToPrune == null) {
                        policiesToPrune = new ArrayList<>();
                    }

                    policiesToPrune.add(policy);
                }
            }

            if(policiesToPrune != null) {
                rangerPolicies.removeAll(policiesToPrune);
            }
        }

        return rangerPolicies;
    }

    private boolean isPolicyNeedsPruning(RangerPolicy policy, final String componentType) {

        normalizeAndPrunePolicyItems(policy.getPolicyItems(), componentType);
        normalizeAndPrunePolicyItems(policy.getDenyPolicyItems(), componentType);
        normalizeAndPrunePolicyItems(policy.getAllowExceptions(), componentType);
        normalizeAndPrunePolicyItems(policy.getDenyExceptions(), componentType);
        normalizeAndPrunePolicyItems(policy.getDataMaskPolicyItems(), componentType);
        normalizeAndPrunePolicyItems(policy.getRowFilterPolicyItems(), componentType);

        if (!policy.getIsAuditEnabled() &&
                CollectionUtils.isEmpty(policy.getPolicyItems()) &&
                CollectionUtils.isEmpty(policy.getDenyPolicyItems()) &&
                CollectionUtils.isEmpty(policy.getAllowExceptions()) &&
                CollectionUtils.isEmpty(policy.getDenyExceptions()) &&
                CollectionUtils.isEmpty(policy.getDataMaskPolicyItems()) &&
                CollectionUtils.isEmpty(policy.getRowFilterPolicyItems())) {
            return true;
        } else {
            return false;
        }
    }

    private List<? extends RangerPolicy.RangerPolicyItem> normalizeAndPrunePolicyItems(List<? extends RangerPolicy.RangerPolicyItem> policyItems, final String componentType) {
        if(CollectionUtils.isNotEmpty(policyItems)) {
            final String                        prefix       = componentType + AbstractServiceStore.COMPONENT_ACCESSTYPE_SEPARATOR;
            List<RangerPolicy.RangerPolicyItem> itemsToPrune = null;

            for (RangerPolicy.RangerPolicyItem policyItem : policyItems) {
                List<RangerPolicy.RangerPolicyItemAccess> policyItemAccesses = policyItem.getAccesses();

                if (CollectionUtils.isNotEmpty(policyItemAccesses)) {
                    List<RangerPolicy.RangerPolicyItemAccess> accessesToPrune = null;

                    for (RangerPolicy.RangerPolicyItemAccess access : policyItemAccesses) {
                        String accessType = access.getType();

                        if (StringUtils.startsWith(accessType, prefix)) {
                            String newAccessType = StringUtils.removeStart(accessType, prefix);

                            access.setType(newAccessType);
                        } else if (accessType.contains(AbstractServiceStore.COMPONENT_ACCESSTYPE_SEPARATOR)) {
                            if(accessesToPrune == null) {
                                accessesToPrune = new ArrayList<>();
                            }

                            accessesToPrune.add(access);
                        }
                    }

                    if(accessesToPrune != null) {
                        policyItemAccesses.removeAll(accessesToPrune);
                    }

                    if (policyItemAccesses.isEmpty() && !policyItem.getDelegateAdmin()) {
                        if(itemsToPrune == null) {
                            itemsToPrune = new ArrayList<>();
                        }

                        itemsToPrune.add(policyItem);

                        continue;
                    }
                }

                if (policyItem instanceof RangerPolicy.RangerDataMaskPolicyItem) {
                    RangerPolicyItemDataMaskInfo dataMaskInfo = ((RangerPolicy.RangerDataMaskPolicyItem) policyItem).getDataMaskInfo();
                    String                       maskType     = dataMaskInfo.getDataMaskType();

                    if (StringUtils.startsWith(maskType, prefix)) {
                        dataMaskInfo.setDataMaskType(StringUtils.removeStart(maskType, prefix));
                    } else if (maskType.contains(AbstractServiceStore.COMPONENT_ACCESSTYPE_SEPARATOR)) {
                        if (itemsToPrune == null) {
                            itemsToPrune = new ArrayList<>();
                        }

                        itemsToPrune.add(policyItem);
                    }
                }
            }

            if(itemsToPrune != null) {
                policyItems.removeAll(itemsToPrune);
            }
        }

        return policyItems;
    }

    private static boolean isDelegateAdminPolicy(RangerPolicy policy) {
        boolean ret =      hasDelegateAdminItems(policy.getPolicyItems())
                || hasDelegateAdminItems(policy.getDenyPolicyItems())
                || hasDelegateAdminItems(policy.getAllowExceptions())
                || hasDelegateAdminItems(policy.getDenyExceptions());

        return ret;
    }

    private static boolean hasDelegateAdminItems(List<RangerPolicy.RangerPolicyItem> items) {
        boolean ret = false;

        if (CollectionUtils.isNotEmpty(items)) {
            for (RangerPolicy.RangerPolicyItem item : items) {
                if(item.getDelegateAdmin()) {
                    ret = true;

                    break;
                }
            }
        }
        return ret;
    }

    private static boolean skipBuildingPolicyEvaluator(RangerPolicy policy, RangerPolicyEngineOptions options) {
        boolean ret = false;
        if (!policy.getIsEnabled()) {
            ret = true;
        } else if (options.evaluateDelegateAdminOnly && !isDelegateAdminPolicy(policy)) {
            ret = true;
        }
        return ret;
    }

    private void init(RangerPolicyEngineOptions options) {
        RangerServiceDefHelper serviceDefHelper = new RangerServiceDefHelper(serviceDef, false);
        options.setServiceDefHelper(serviceDefHelper);

        List<RangerPolicyEvaluator> policyEvaluators = new ArrayList<>();
        List<RangerPolicyEvaluator> dataMaskPolicyEvaluators  = new ArrayList<>();
        List<RangerPolicyEvaluator> rowFilterPolicyEvaluators = new ArrayList<>();

        for (RangerPolicy policy : policies) {
            if (skipBuildingPolicyEvaluator(policy, options)) {
                continue;
            }

            RangerPolicyEvaluator evaluator = buildPolicyEvaluator(policy, serviceDef, options);

            if (evaluator != null) {
                if(policy.getPolicyType() == null || policy.getPolicyType() == RangerPolicy.POLICY_TYPE_ACCESS) {
                    policyEvaluators.add(evaluator);
                } else if(policy.getPolicyType() == RangerPolicy.POLICY_TYPE_DATAMASK) {
                    dataMaskPolicyEvaluators.add(evaluator);
                } else if(policy.getPolicyType() == RangerPolicy.POLICY_TYPE_ROWFILTER) {
                    rowFilterPolicyEvaluators.add(evaluator);
                } else {
                    LOG.warn("RangerPolicyEngine: ignoring policy id=" + policy.getId() + " - invalid policyType '" + policy.getPolicyType() + "'");
                }
            }
        }
        if (LOG.isInfoEnabled()) {
            LOG.info("This policy engine contains " + (policyEvaluators.size()+dataMaskPolicyEvaluators.size()+rowFilterPolicyEvaluators.size()) + " policy evaluators");
        }
        RangerPolicyEvaluator.PolicyEvalOrderComparator comparator = new RangerPolicyEvaluator.PolicyEvalOrderComparator();
        Collections.sort(policyEvaluators, comparator);
        this.policyEvaluators = Collections.unmodifiableList(policyEvaluators);

        Collections.sort(dataMaskPolicyEvaluators, comparator);
        this.dataMaskPolicyEvaluators = Collections.unmodifiableList(dataMaskPolicyEvaluators);

        Collections.sort(rowFilterPolicyEvaluators, comparator);
        this.rowFilterPolicyEvaluators = Collections.unmodifiableList(rowFilterPolicyEvaluators);

        this.policyEvaluatorsMap = createPolicyEvaluatorsMap();

        if(LOG.isDebugEnabled()) {
            LOG.debug("policy evaluation order: " + this.policyEvaluators.size() + " policies");

            int order = 0;
            for(RangerPolicyEvaluator policyEvaluator : this.policyEvaluators) {
                RangerPolicy policy = policyEvaluator.getPolicy();

                LOG.debug("policy evaluation order: #" + (++order) + " - policy id=" + policy.getId() + "; name=" + policy.getName() + "; evalOrder=" + policyEvaluator.getEvalOrder());
            }

            LOG.debug("dataMask policy evaluation order: " + this.dataMaskPolicyEvaluators.size() + " policies");
            order = 0;
            for(RangerPolicyEvaluator policyEvaluator : this.dataMaskPolicyEvaluators) {
                RangerPolicy policy = policyEvaluator.getPolicy();

                LOG.debug("dataMask policy evaluation order: #" + (++order) + " - policy id=" + policy.getId() + "; name=" + policy.getName() + "; evalOrder=" + policyEvaluator.getEvalOrder());
            }

            LOG.debug("rowFilter policy evaluation order: " + this.rowFilterPolicyEvaluators.size() + " policies");
            order = 0;
            for(RangerPolicyEvaluator policyEvaluator : this.rowFilterPolicyEvaluators) {
                RangerPolicy policy = policyEvaluator.getPolicy();

                LOG.debug("rowFilter policy evaluation order: #" + (++order) + " - policy id=" + policy.getId() + "; name=" + policy.getName() + "; evalOrder=" + policyEvaluator.getEvalOrder());
            }
        }
    }

    private List<RangerContextEnricher> buildContextEnrichers(RangerPolicyEngineOptions  options) {
        List<RangerContextEnricher> contextEnrichers = new ArrayList<>();

        if (StringUtils.isEmpty(zoneName) && CollectionUtils.isNotEmpty(serviceDef.getContextEnrichers())) {
            for (RangerServiceDef.RangerContextEnricherDef enricherDef : serviceDef.getContextEnrichers()) {
                if (enricherDef == null) {
                    continue;
                }
                if (!options.disableContextEnrichers || options.enableTagEnricherWithLocalRefresher && StringUtils.equals(enricherDef.getEnricher(), RangerTagEnricher.class.getName())) {
                    // This will be true only if the engine is initialized within ranger-admin
                    RangerServiceDef.RangerContextEnricherDef contextEnricherDef = enricherDef;

                    if (options.enableTagEnricherWithLocalRefresher && StringUtils.equals(enricherDef.getEnricher(), RangerTagEnricher.class.getName())) {
                        contextEnricherDef = new RangerServiceDef.RangerContextEnricherDef(enricherDef.getItemId(), enricherDef.getName(), "org.apache.ranger.common.RangerAdminTagEnricher", null);
                    }

                    RangerContextEnricher contextEnricher = buildContextEnricher(contextEnricherDef);

                    if (contextEnricher != null) {
                        contextEnrichers.add(contextEnricher);
                    }
                }
            }
        }
        return contextEnrichers;
    }

    private RangerContextEnricher buildContextEnricher(RangerServiceDef.RangerContextEnricherDef enricherDef) {
        if(LOG.isDebugEnabled()) {
            LOG.debug("==> RangerPolicyRepository.buildContextEnricher(" + enricherDef + ")");
        }

        RangerContextEnricher ret = null;

        Map<String, String> enricherDefOptions = enricherDef.getEnricherOptions();

        String isEnabledAsString = enricherDefOptions.get("IsEnabled");

        if (!StringUtils.equalsIgnoreCase(isEnabledAsString, "false")) {
            RangerPerfTracer perf = null;

            if (RangerPerfTracer.isPerfTraceEnabled(PERF_CONTEXTENRICHER_INIT_LOG)) {
                perf = RangerPerfTracer.getPerfTracer(PERF_CONTEXTENRICHER_INIT_LOG, "RangerContextEnricher.init(appId=" + appId + ",name=" + enricherDef.getName() + ")");
            }

            String name = enricherDef != null ? enricherDef.getName() : null;
            String clsName = enricherDef != null ? enricherDef.getEnricher() : null;

            if (!StringUtils.isEmpty(clsName)) {
                try {
                    @SuppressWarnings("unchecked")
                    Class<RangerContextEnricher> enricherClass = (Class<RangerContextEnricher>) Class.forName(clsName);

                    ret = enricherClass.newInstance();
                } catch (Exception excp) {
                    LOG.error("failed to instantiate context enricher '" + clsName + "' for '" + name + "'", excp);
                }
            }

            if (ret != null) {
                ret.setEnricherDef(enricherDef);
                ret.setServiceName(componentServiceName);
                ret.setServiceDef(componentServiceDef);
                ret.setAppId(appId);
                if (ret instanceof RangerAbstractContextEnricher) {
                    RangerAbstractContextEnricher abstractContextEnricher = (RangerAbstractContextEnricher) ret;
                    abstractContextEnricher.setPluginContext(pluginContext);
                }
                ret.init();
            }

            RangerPerfTracer.log(perf);
        }

        if(LOG.isDebugEnabled()) {
            LOG.debug("<== RangerPolicyRepository.buildContextEnricher(" + enricherDef + "): " + ret);
        }
        return ret;
    }

    private RangerPolicyEvaluator buildPolicyEvaluator(RangerPolicy policy, RangerServiceDef serviceDef, RangerPolicyEngineOptions options) {
        if(LOG.isDebugEnabled()) {
            LOG.debug("==> RangerPolicyRepository.buildPolicyEvaluator(" + policy + "," + serviceDef + ", " + options + ")");
        }

        scrubPolicy(policy);
        RangerAbstractPolicyEvaluator ret;

        if(StringUtils.equalsIgnoreCase(options.evaluatorType, RangerPolicyEvaluator.EVALUATOR_TYPE_CACHED)) {
            ret = new RangerCachedPolicyEvaluator();
        } else {
            ret = new RangerOptimizedPolicyEvaluator();
        }

        ret.setPluginContext(pluginContext);
        ret.init(policy, serviceDef, options);

        if(LOG.isDebugEnabled()) {
            LOG.debug("<== RangerPolicyRepository.buildPolicyEvaluator(" + policy + "," + serviceDef + "): " + ret);
        }

        return ret;
    }

    private boolean scrubPolicy(RangerPolicy policy) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> RangerPolicyRepository.scrubPolicy(" + policy + ")");
        }
        boolean altered = false;
        Long policyId = policy.getId();
        Map<String, RangerPolicy.RangerPolicyResource> resourceMap = policy.getResources();
        for (Map.Entry<String, RangerPolicy.RangerPolicyResource> entry : resourceMap.entrySet()) {
            String resourceName = entry.getKey();
            RangerPolicy.RangerPolicyResource resource = entry.getValue();
            Iterator<String> iterator = resource.getValues().iterator();
            while (iterator.hasNext()) {
                String value = iterator.next();
                if (value == null) {
                    LOG.warn("RangerPolicyRepository.scrubPolicyResource: found null resource value for " + resourceName + " in policy " + policyId + "!  Removing...");
                    iterator.remove();
                    altered = true;
                }
            }
        }

        scrubPolicyItems(policyId, policy.getPolicyItems());
        scrubPolicyItems(policyId, policy.getAllowExceptions());
        scrubPolicyItems(policyId, policy.getDenyPolicyItems());
        scrubPolicyItems(policyId, policy.getDenyExceptions());
        scrubPolicyItems(policyId, policy.getRowFilterPolicyItems());
        scrubPolicyItems(policyId, policy.getDataMaskPolicyItems());

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== RangerPolicyRepository.scrubPolicy(" + policy + "): " + altered);
        }
        return altered;
    }

    private void scrubPolicyItems(final Long policyId, final List<? extends RangerPolicy.RangerPolicyItem> policyItems) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> RangerPolicyRepository.scrubPolicyItems(" + policyId + "): ");
        }

        for (RangerPolicy.RangerPolicyItem policyItem : policyItems) {
            removeNulls(policyItem.getUsers(), policyId, policyItem);
            removeNulls(policyItem.getGroups(), policyId, policyItem);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== RangerPolicyRepository.scrubPolicyItems(" + policyId + "): ");
        }
    }

    private void removeNulls(Collection<String> strings, final Long policyId, final RangerPolicy.RangerPolicyItem policyItem) {
        Iterator<String> iterator = strings.iterator();

        while (iterator.hasNext()) {
            String value = iterator.next();
            if (value == null) {
                LOG.warn("RangerPolicyRepository.removeNulls: found null user/group in policyItem '" + policyItem + "' in policy " + policyId + "!  Removing...");
                iterator.remove();
            }
        }
    }

    private List<RangerPolicyEvaluator> getReorderedPolicyEvaluators(List<RangerPolicyEvaluator> evaluators) {
        List<RangerPolicyEvaluator> ret = evaluators;

        if (CollectionUtils.isNotEmpty(evaluators)) {
            ret = new ArrayList<>(evaluators);
            Collections.sort(ret, new RangerPolicyEvaluator.PolicyEvalOrderComparator());

            ret = Collections.unmodifiableList(ret);
        }

        return ret;
    }

    private Map<String, RangerResourceTrie> createResourceTrieMap(List<RangerPolicyEvaluator> evaluators, boolean optimizeTrieForRetrieval) {
        final Map<String, RangerResourceTrie> ret;

        if (serviceDef != null && CollectionUtils.isNotEmpty(serviceDef.getResources())) {
            ret = new HashMap<>();

            for (RangerServiceDef.RangerResourceDef resourceDef : serviceDef.getResources()) {
                ret.put(resourceDef.getName(), new RangerResourceTrie(resourceDef, evaluators, optimizeTrieForRetrieval, pluginContext));
            }
        } else {
            ret = null;
        }

        return ret;
    }

    private void updateTrie(Map<String, RangerResourceTrie> trieMap, Integer policyDeltaType, RangerPolicyEvaluator oldEvaluator, RangerPolicyEvaluator newEvaluator) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> RangerPolicyRepository.updateTrie(policyDeltaType=" + policyDeltaType + "): ");
        }
        for (RangerServiceDef.RangerResourceDef resourceDef : serviceDef.getResources()) {

            String resourceDefName = resourceDef.getName();

            RangerResourceTrie<RangerPolicyEvaluator> trie = trieMap.get(resourceDefName);

            if (trie == null) {
                if (RangerPolicyDelta.CHANGE_TYPE_POLICY_DELETE == policyDeltaType || RangerPolicyDelta.CHANGE_TYPE_POLICY_UPDATE == policyDeltaType) {
                    LOG.warn("policyDeltaType is not for POLICY_CREATE and trie for resourceDef:[" + resourceDefName + "] was null! Should not have happened!!");
                }
                trie = new RangerResourceTrie<>(resourceDef, new ArrayList<>(), true, pluginContext);
                trieMap.put(resourceDefName, trie);
            }

            if (policyDeltaType == RangerPolicyDelta.CHANGE_TYPE_POLICY_CREATE) {
                addEvaluatorToTrie(newEvaluator, trie, resourceDefName);
            } else if (policyDeltaType == RangerPolicyDelta.CHANGE_TYPE_POLICY_DELETE) {
                removeEvaluatorFromTrie(oldEvaluator, trie, resourceDefName);
            } else if (policyDeltaType == RangerPolicyDelta.CHANGE_TYPE_POLICY_UPDATE) {
                removeEvaluatorFromTrie(oldEvaluator, trie, resourceDefName);
                addEvaluatorToTrie(newEvaluator, trie, resourceDefName);
            } else {
                LOG.error("policyDeltaType:" + policyDeltaType + " is currently not handled, policy-id:[" + oldEvaluator.getPolicy().getId() +"]");
            }
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("<== RangerPolicyRepository.updateTrie(policyDeltaType=" + policyDeltaType + "): ");
        }
    }

    private void addEvaluatorToTrie(RangerPolicyEvaluator newEvaluator, RangerResourceTrie<RangerPolicyEvaluator> trie, String resourceDefName) {
        if (newEvaluator != null) {
            RangerPolicy.RangerPolicyResource resource = newEvaluator.getPolicyResource().get(resourceDefName);
            trie.add(resource, newEvaluator);
        }
    }

    private void removeEvaluatorFromTrie(RangerPolicyEvaluator oldEvaluator, RangerResourceTrie<RangerPolicyEvaluator> trie, String resourceDefName) {
        if (oldEvaluator != null) {
            RangerPolicy.RangerPolicyResource resource = oldEvaluator.getPolicyResource().get(resourceDefName);
            if (resource != null) {
                trie.delete(resource, oldEvaluator);
            }
        }
    }

    private Map<Long, RangerPolicyEvaluator> createPolicyEvaluatorsMap() {
        Map<Long, RangerPolicyEvaluator> tmpPolicyEvaluatorMap = new HashMap<>();

        for (RangerPolicyEvaluator evaluator : getPolicyEvaluators()) {
            tmpPolicyEvaluatorMap.put(evaluator.getPolicy().getId(), evaluator);
        }
        for (RangerPolicyEvaluator evaluator : getDataMaskPolicyEvaluators()) {
            tmpPolicyEvaluatorMap.put(evaluator.getPolicy().getId(), evaluator);
        }
        for (RangerPolicyEvaluator evaluator : getRowFilterPolicyEvaluators()) {
            tmpPolicyEvaluatorMap.put(evaluator.getPolicy().getId(), evaluator);
        }

        return  Collections.unmodifiableMap(tmpPolicyEvaluatorMap);
    }


    private RangerPolicyEvaluator addPolicy(RangerPolicy policy) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> RangerPolicyRepository.addPolicy(" + policy +")");
        }
        RangerPolicyEvaluator ret = null;

        if (StringUtils.equals(this.serviceDef.getName(), this.componentServiceDef.getName()) || !isPolicyNeedsPruning(policy, this.componentServiceDef.getName())) {
            policies.add(policy);

            if (!skipBuildingPolicyEvaluator(policy, options)) {

                ret = buildPolicyEvaluator(policy, serviceDef, options);

                if (ret != null) {
                    if (policy.getPolicyType() == null || policy.getPolicyType() == RangerPolicy.POLICY_TYPE_ACCESS) {
                        policyEvaluators.add(ret);
                    } else if (policy.getPolicyType() == RangerPolicy.POLICY_TYPE_DATAMASK) {
                        dataMaskPolicyEvaluators.add(ret);
                    } else if (policy.getPolicyType() == RangerPolicy.POLICY_TYPE_ROWFILTER) {
                        rowFilterPolicyEvaluators.add(ret);
                    } else {
                        LOG.warn("RangerPolicyEngine: ignoring policy id=" + policy.getId() + " - invalid policyType '" + policy.getPolicyType() + "'");
                    }

                    policyEvaluatorsMap.put(policy.getId(), ret);
                }
            }
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== RangerPolicyRepository.addPolicy(" + policy +"): " + ret);
        }
        return ret;
    }

    private void removePolicy(Long id) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> RangerPolicyRepository.removePolicy(" + id +")");
        }
        Iterator<RangerPolicy> iterator = policies.iterator();
        while (iterator.hasNext()) {
            if (id.equals(iterator.next().getId())) {
                iterator.remove();
                break;
            }
        }

        policyEvaluatorsMap.remove(id);

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== RangerPolicyRepository.removePolicy(" + id +")");
        }
    }

    private void deletePolicyEvaluator(RangerPolicyEvaluator evaluator) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> RangerPolicyRepository.deletePolicyEvaluator(" + evaluator.getPolicy() + ")");
        }
        int policyType = evaluator.getPolicy().getPolicyType();

        List<RangerPolicyEvaluator> evaluators = null;

        if (policyType == RangerPolicy.POLICY_TYPE_ACCESS) {
            evaluators = this.policyEvaluators;
        } else if (policyType == RangerPolicy.POLICY_TYPE_DATAMASK) {
            evaluators = this.dataMaskPolicyEvaluators;
        } else if (policyType == RangerPolicy.POLICY_TYPE_ROWFILTER) {
            evaluators = this.rowFilterPolicyEvaluators;
        } else {
            LOG.error("Unknown policyType:[" + policyType +"]");
        }
        if (evaluators != null) {
            evaluators.remove(evaluator);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== RangerPolicyRepository.deletePolicyEvaluator(" + evaluator.getPolicy() + ")");
        }
    }

    private RangerPolicyEvaluator update(final RangerPolicyDelta delta, final RangerPolicyEvaluator currentEvaluator) {

        if (LOG.isDebugEnabled()) {
            LOG.debug("==> RangerPolicyRepository.update(delta=" + delta + ", currentEvaluator=" + (currentEvaluator == null ? null : currentEvaluator.getPolicy()) + ")");
        }
        Integer changeType = delta.getChangeType();
        Integer policyType = delta.getPolicyType();
        Long    policyId   = delta.getPolicyId();

        RangerPolicy policy = delta.getPolicy();

        RangerPolicyEvaluator newEvaluator = null;

        switch (changeType) {
            case RangerPolicyDelta.CHANGE_TYPE_POLICY_CREATE:
                if (policy != null) {
                    newEvaluator = addPolicy(policy);
                }
                break;
            case RangerPolicyDelta.CHANGE_TYPE_POLICY_UPDATE: {
                removePolicy(policyId);
                if (policy != null) {
                    newEvaluator = addPolicy(policy);
                }
            }
            break;
            case RangerPolicyDelta.CHANGE_TYPE_POLICY_DELETE: {
                if (currentEvaluator != null) {
                    removePolicy(policyId);
                }
            }
            break;
        }

        Map<String, RangerResourceTrie> trieMap = getTrie(policyType);

        if (trieMap != null) {
            updateTrie(trieMap, changeType, currentEvaluator, newEvaluator);
        }

        if (changeType == RangerPolicyDelta.CHANGE_TYPE_POLICY_UPDATE || changeType == RangerPolicyDelta.CHANGE_TYPE_POLICY_DELETE) {
            if (currentEvaluator != null) {
                deletePolicyEvaluator(currentEvaluator);
            }
        }

        RangerPolicyEvaluator ret =  changeType == RangerPolicyDelta.CHANGE_TYPE_POLICY_DELETE ? currentEvaluator : newEvaluator;

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== RangerPolicyRepository.update(delta=" + delta + ", currentEvaluator=" + (currentEvaluator == null ? null : currentEvaluator.getPolicy()) + ")");
        }

        return ret;
    }

    Map<String, RangerResourceTrie> getTrie(final int policyType) {
        final Map<String, RangerResourceTrie> ret;
        switch (policyType) {
            case RangerPolicy.POLICY_TYPE_ACCESS:
                ret = policyResourceTrie;
                break;
            case RangerPolicy.POLICY_TYPE_DATAMASK:
                ret = dataMaskResourceTrie;
                break;
            case RangerPolicy.POLICY_TYPE_ROWFILTER:
                ret = rowFilterResourceTrie;
                break;
            default:
                ret = null;
        }
        return ret;
    }

    static private final class AuditInfo {
        final boolean isAudited;
        final long    auditPolicyId;

        AuditInfo(boolean isAudited, long auditPolicyId) {
            this.isAudited     = isAudited;
            this.auditPolicyId = auditPolicyId;
        }
        long getAuditPolicyId() {
            return this.auditPolicyId;
        }
        boolean getIsAudited() {
            return isAudited;
        }
    }
}
