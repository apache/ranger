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
import org.apache.ranger.authorization.hadoop.config.RangerConfiguration;
import org.apache.ranger.plugin.contextenricher.RangerContextEnricher;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.policyevaluator.RangerCachedPolicyEvaluator;
import org.apache.ranger.plugin.policyevaluator.RangerOptimizedPolicyEvaluator;
import org.apache.ranger.plugin.policyevaluator.RangerPolicyEvaluator;
import org.apache.ranger.plugin.util.RangerPerfTracer;
import org.apache.ranger.plugin.util.RangerResourceTrie;
import org.apache.ranger.plugin.util.ServicePolicies;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class RangerPolicyRepository {
    private static final Log LOG = LogFactory.getLog(RangerPolicyRepository.class);

    private static final Log PERF_CONTEXTENRICHER_INIT_LOG = RangerPerfTracer.getPerfLogger("contextenricher.init");

    private final String                      serviceName;
    private final RangerServiceDef            serviceDef;
    private final List<RangerPolicy>          policies;
    private final long                        policyVersion;
    private List<RangerContextEnricher>       contextEnrichers;
    private List<RangerPolicyEvaluator>       policyEvaluators;
    private final Map<String, Boolean>        accessAuditCache;
    private final boolean                         disableTrieLookupPrefilter;
    private final Map<String, RangerResourceTrie> policyResourceTrie;


    private static int RANGER_POLICYENGINE_AUDITRESULT_CACHE_SIZE = 64*1024;

    RangerPolicyRepository(ServicePolicies servicePolicies, RangerPolicyEngineOptions options) {
        super();

        serviceName   = servicePolicies.getServiceName();
        serviceDef    = servicePolicies.getServiceDef();
        policies      = Collections.unmodifiableList(servicePolicies.getPolicies());
        policyVersion = servicePolicies.getPolicyVersion() != null ? servicePolicies.getPolicyVersion().longValue() : -1;

        List<RangerContextEnricher> contextEnrichers = new ArrayList<RangerContextEnricher>();
        if (!options.disableContextEnrichers && !CollectionUtils.isEmpty(serviceDef.getContextEnrichers())) {
            for (RangerServiceDef.RangerContextEnricherDef enricherDef : serviceDef.getContextEnrichers()) {
                if (enricherDef == null) {
                    continue;
                }

                RangerContextEnricher contextEnricher = buildContextEnricher(enricherDef);

                if(contextEnricher != null) {
	                contextEnrichers.add(contextEnricher);
                }
            }
        }
        this.contextEnrichers = Collections.unmodifiableList(contextEnrichers);

        List<RangerPolicyEvaluator> policyEvaluators = new ArrayList<RangerPolicyEvaluator>();
        for (RangerPolicy policy : servicePolicies.getPolicies()) {
            if (skipBuildingPolicyEvaluator(policy, options)) {
                continue;
            }

            RangerPolicyEvaluator evaluator = buildPolicyEvaluator(policy, serviceDef, options);


            if (evaluator != null) {
                policyEvaluators.add(evaluator);
            }
        }
        Collections.sort(policyEvaluators);
        this.policyEvaluators = Collections.unmodifiableList(policyEvaluators);

        if(LOG.isDebugEnabled()) {
            LOG.debug("policy evaluation order: " + this.policyEvaluators.size() + " policies");

            int order = 0;
            for(RangerPolicyEvaluator policyEvaluator : this.policyEvaluators) {
                RangerPolicy policy = policyEvaluator.getPolicy();

                LOG.debug("policy evaluation order: #" + (++order) + " - policy id=" + policy.getId() + "; name=" + policy.getName() + "; evalOrder=" + policyEvaluator.getEvalOrder());
            }
        }

        String propertyName = "ranger.plugin." + serviceName + ".policyengine.auditcachesize";

        if(options.cacheAuditResults) {
	        int auditResultCacheSize = RangerConfiguration.getInstance().getInt(propertyName, RANGER_POLICYENGINE_AUDITRESULT_CACHE_SIZE);

	        accessAuditCache = Collections.synchronizedMap(new CacheMap<String, Boolean>(auditResultCacheSize));
        } else {
        	accessAuditCache = null;
        }

        this.disableTrieLookupPrefilter = options.disableTrieLookupPrefilter;

        if(this.disableTrieLookupPrefilter) {
            policyResourceTrie = null;
        } else {
            policyResourceTrie = new HashMap<String, RangerResourceTrie>();
        }

        initResourceTries();
    }

    public String getServiceName() {
        return serviceName;
    }

    public RangerServiceDef getServiceDef() {
        return serviceDef;
    }

    public List<RangerPolicy> getPolicies() {
        return policies;
    }

    public long getPolicyVersion() {
        return policyVersion;
    }

    public List<RangerContextEnricher> getContextEnrichers() {
        return contextEnrichers;
    }

    public List<RangerPolicyEvaluator> getPolicyEvaluators() {
        return policyEvaluators;
    }

    public List<RangerPolicyEvaluator> getPolicyEvaluators(RangerAccessResource resource) {
        String resourceStr = resource == null ? null : resource.getAsString();

        return disableTrieLookupPrefilter || StringUtils.isEmpty(resourceStr) ? getPolicyEvaluators() : getPolicyEvaluators(policyResourceTrie, resource);
    }

    public static boolean isDelegateAdminPolicy(RangerPolicy policy) {
        boolean ret = false;

        ret =      hasDelegateAdminItems(policy.getPolicyItems());

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

    private RangerContextEnricher buildContextEnricher(RangerServiceDef.RangerContextEnricherDef enricherDef) {
        if(LOG.isDebugEnabled()) {
            LOG.debug("==> RangerPolicyRepository.buildContextEnricher(" + enricherDef + ")");
        }

        RangerContextEnricher ret = null;

        RangerPerfTracer perf = null;

        if(RangerPerfTracer.isPerfTraceEnabled(PERF_CONTEXTENRICHER_INIT_LOG)) {
            perf = RangerPerfTracer.getPerfTracer(PERF_CONTEXTENRICHER_INIT_LOG, "RangerContextEnricher.init(name=" + enricherDef.getName() + ")");
        }

        String name    = enricherDef != null ? enricherDef.getName()     : null;
        String clsName = enricherDef != null ? enricherDef.getEnricher() : null;

        if(! StringUtils.isEmpty(clsName)) {
            try {
                @SuppressWarnings("unchecked")
                Class<RangerContextEnricher> enricherClass = (Class<RangerContextEnricher>)Class.forName(clsName);

                ret = enricherClass.newInstance();
            } catch(Exception excp) {
                LOG.error("failed to instantiate context enricher '" + clsName + "' for '" + name + "'", excp);
            }
        }

        if(ret != null) {
        	ret.setContextEnricherDef(enricherDef);
            ret.init();
        }

        RangerPerfTracer.log(perf);

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
        RangerPolicyEvaluator ret = null;

        if(StringUtils.equalsIgnoreCase(options.evaluatorType, RangerPolicyEvaluator.EVALUATOR_TYPE_CACHED)) {
            ret = new RangerCachedPolicyEvaluator();
        } else {
            ret = new RangerOptimizedPolicyEvaluator();
        }

        ret.init(policy, serviceDef, options);

        if(LOG.isDebugEnabled()) {
            LOG.debug("<== RangerPolicyRepository.buildPolicyEvaluator(" + policy + "," + serviceDef + "): " + ret);
        }

        return ret;
    }

    private void initResourceTries() {
        if (!this.disableTrieLookupPrefilter) {
            policyResourceTrie.clear();

            if (serviceDef != null && serviceDef.getResources() != null) {
                for (RangerServiceDef.RangerResourceDef resourceDef : serviceDef.getResources()) {
                    policyResourceTrie.put(resourceDef.getName(), new RangerResourceTrie(resourceDef, policyEvaluators));
                }
            }
        }
    }

    private List<RangerPolicyEvaluator> getPolicyEvaluators(Map<String, RangerResourceTrie> resourceTrie, RangerAccessResource resource) {
        List<RangerPolicyEvaluator> ret          = null;
        Set<String>                 resourceKeys = resource == null ? null : resource.getKeys();

        if(CollectionUtils.isNotEmpty(resourceKeys)) {
            boolean isRetModifiable = false;

            for(String resourceName : resourceKeys) {
                RangerResourceTrie trie = resourceTrie.get(resourceName);

                if(trie == null) { // if no trie exists for this resource level, ignore and continue to next level
                    continue;
                }

                List<RangerPolicyEvaluator> resourceEvaluators = trie.getEvaluatorsForResource(resource.getValue(resourceName));

                if(CollectionUtils.isEmpty(resourceEvaluators)) { // no policies for this resource, bail out
                    ret = null;
                } else if(ret == null) { // initialize ret with policies found for this resource
                    ret = resourceEvaluators;
                } else { // remove policies from ret that are not in resourceEvaluators
                    if(isRetModifiable) {
                        ret.retainAll(resourceEvaluators);
                    } else {
                        final List<RangerPolicyEvaluator> shorterList;
                        final List<RangerPolicyEvaluator> longerList;

                        if (ret.size() < resourceEvaluators.size()) {
                            shorterList = ret;
                            longerList  = resourceEvaluators;
                        } else {
                            shorterList = resourceEvaluators;
                            longerList  = ret;
                        }

                        ret = new ArrayList<>(shorterList);
                        ret.retainAll(longerList);
                        isRetModifiable = true;
                    }
                }

                if(CollectionUtils.isEmpty(ret)) { // if no policy exists, bail out and return empty list
                    ret = null;
                    break;
                }
            }
        }

        if(ret == null) {
            ret = Collections.emptyList();
        }

        if(LOG.isDebugEnabled()) {
            LOG.debug("<== RangerPolicyRepository.getPolicyEvaluators(" + resource.getAsString() + "): evaluatorCount=" + ret.size());
        }

        return ret;
    }

    boolean setAuditEnabledFromCache(RangerAccessRequest request, RangerAccessResult result) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> RangerPolicyRepository.setAuditEnabledFromCache()");
        }

        Boolean value = null;

        if (accessAuditCache != null) {
	        value = accessAuditCache.get(request.getResource().getAsString());
        }

        if ((value != null)) {
            result.setIsAudited(value);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== RangerPolicyRepository.setAuditEnabledFromCache()");
        }

        return value != null;
    }

     void storeAuditEnabledInCache(RangerAccessRequest request, RangerAccessResult ret) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> RangerPolicyRepository.storeAuditEnabledInCache()");
        }

        if ((ret.getIsAuditedDetermined() == true)) {
            String strResource = request.getResource().getAsString();

            Boolean value = ret.getIsAudited() ? Boolean.TRUE : Boolean.FALSE;

            if (accessAuditCache != null) {
	            accessAuditCache.put(strResource, value);
	        }
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== RangerPolicyRepository.storeAuditEnabledInCache()");
        }
    }

    /**
     * Remove nulls from policy resource values
     * @param policy
     */
    boolean scrubPolicy(RangerPolicy policy) {
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

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== RangerPolicyRepository.scrubPolicy(" + policy + "): " + altered);
        }
        return altered;
    }

    @Override
    public String toString( ) {
        StringBuilder sb = new StringBuilder();

        toString(sb);

        return sb.toString();
    }

    public StringBuilder toString(StringBuilder sb) {

        sb.append("RangerPolicyRepository={");

        sb.append("serviceName={").append(serviceName).append("} ");
        sb.append("serviceDef={").append(serviceDef).append("} ");
        sb.append("policyEvaluators={");
        if (policyEvaluators != null) {
            for (RangerPolicyEvaluator policyEvaluator : policyEvaluators) {
                if (policyEvaluator != null) {
                    sb.append(policyEvaluator).append(" ");
                }
            }
        }
        if (contextEnrichers != null) {
            for (RangerContextEnricher contextEnricher : contextEnrichers) {
                if (contextEnricher != null) {
                    sb.append(contextEnricher).append(" ");
                }
            }
        }

        sb.append("} ");

        return sb;
    }

}
