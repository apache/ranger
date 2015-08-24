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
import org.apache.ranger.plugin.store.AbstractServiceStore;
import org.apache.ranger.plugin.util.ServicePolicies;

import java.util.*;

public class RangerPolicyRepository {
    private static final Log LOG = LogFactory.getLog(RangerPolicyRepository.class);

    private final String                      serviceName;
    private final RangerServiceDef            serviceDef;
    private final List<RangerPolicy>          policies;
    private final long                        policyVersion;
    private List<RangerContextEnricher>       contextEnrichers;
    private List<RangerPolicyEvaluator>       policyEvaluators;
    private final Map<String, Boolean>        accessAuditCache;

    private final String                      componentServiceName;
    private final RangerServiceDef            componentServiceDef;

    RangerPolicyRepository(ServicePolicies servicePolicies, RangerPolicyEngineOptions options) {
        super();

        this.componentServiceName = this.serviceName = servicePolicies.getServiceName();
        this.componentServiceDef = this.serviceDef = servicePolicies.getServiceDef();
        this.policies = Collections.unmodifiableList(servicePolicies.getPolicies());
        this.policyVersion = servicePolicies.getPolicyVersion() != null ? servicePolicies.getPolicyVersion() : -1;

        if(LOG.isDebugEnabled()) {
            LOG.debug("RangerPolicyRepository : building resource-policy-repository for service " + serviceName);
        }

        String propertyName = "ranger.plugin." + serviceName + ".policyengine.auditcachesize";

        if (options.cacheAuditResults) {
            final int RANGER_POLICYENGINE_AUDITRESULT_CACHE_SIZE = 64*1024;

            int auditResultCacheSize = RangerConfiguration.getInstance().getInt(propertyName, RANGER_POLICYENGINE_AUDITRESULT_CACHE_SIZE);
            accessAuditCache = Collections.synchronizedMap(new CacheMap<String, Boolean>(auditResultCacheSize));
        } else {
                accessAuditCache = null;
            }

        init(options);

    }

    RangerPolicyRepository(ServicePolicies.TagPolicies tagPolicies, RangerPolicyEngineOptions options,
                           RangerServiceDef componentServiceDef, String componentServiceName) {
        super();

        this.serviceName = tagPolicies.getServiceName();
        this.componentServiceName = componentServiceName;

        this.serviceDef = normalizeAccessTypeDefs(tagPolicies.getServiceDef(), componentServiceDef.getName());
        this.componentServiceDef = componentServiceDef;

        this.policies = Collections.unmodifiableList(normalizePolicyItemAccesses(tagPolicies.getPolicies(), componentServiceDef.getName()));
        this.policyVersion = tagPolicies.getPolicyVersion() != null ? tagPolicies.getPolicyVersion() : -1;
        this.accessAuditCache = null;

        if(LOG.isDebugEnabled()) {
            LOG.debug("RangerPolicyRepository : building tag-policy-repository for tag service " + serviceName);
        }

        init(options);

    }

    public String getServiceName() { return serviceName; }

    public RangerServiceDef getServiceDef() {
        return serviceDef;
    }

    public List<RangerPolicy> getPolicies() {
        return policies;
    }

    public long getPolicyVersion() {
        return policyVersion;
    }

    public List<RangerContextEnricher> getContextEnrichers() { return contextEnrichers; }

    public List<RangerPolicyEvaluator> getPolicyEvaluators() {
        return policyEvaluators;
    }

    private RangerServiceDef normalizeAccessTypeDefs(RangerServiceDef serviceDef, final String componentType) {

        if (serviceDef != null && StringUtils.isNotBlank(componentType)) {

            List<RangerServiceDef.RangerAccessTypeDef> accessTypeDefs = serviceDef.getAccessTypes();

            if (CollectionUtils.isNotEmpty(accessTypeDefs)) {

                String prefix = componentType + AbstractServiceStore.COMPONENT_ACCESSTYPE_SEPARATOR;

                List<RangerServiceDef.RangerAccessTypeDef> unneededAccessTypeDefs = new ArrayList<RangerServiceDef.RangerAccessTypeDef>();

                for (RangerServiceDef.RangerAccessTypeDef accessTypeDef : accessTypeDefs) {

                    String accessType = accessTypeDef.getName();

                    if (StringUtils.startsWith(accessType, prefix)) {

                        String newAccessType = StringUtils.removeStart(accessType, prefix);

                        accessTypeDef.setName(newAccessType);

                        Collection<String> impliedGrants = accessTypeDef.getImpliedGrants();

                        if (CollectionUtils.isNotEmpty(impliedGrants)) {

                            Collection<String> newImpliedGrants = null;

                            for (String impliedGrant : impliedGrants) {

                                if (StringUtils.startsWith(impliedGrant, prefix)) {

                                    String newImpliedGrant = StringUtils.removeStart(impliedGrant, prefix);

                                    if (newImpliedGrants == null) {
                                        newImpliedGrants = new ArrayList<String>();
                                    }

                                    newImpliedGrants.add(newImpliedGrant);
                                }
                            }
                            accessTypeDef.setImpliedGrants(newImpliedGrants);

                        }
                    } else if (StringUtils.contains(accessType, AbstractServiceStore.COMPONENT_ACCESSTYPE_SEPARATOR)) {
                        unneededAccessTypeDefs.add(accessTypeDef);
                    }
                }
                accessTypeDefs.removeAll(unneededAccessTypeDefs);
            }
        }

        return serviceDef;
    }

    private List<RangerPolicy> normalizePolicyItemAccesses(List<RangerPolicy> rangerPolicies, final String componentType) {

        if (CollectionUtils.isNotEmpty(rangerPolicies) && StringUtils.isNotBlank(componentType)) {

            String prefix = componentType + AbstractServiceStore.COMPONENT_ACCESSTYPE_SEPARATOR;

            List<RangerPolicy> unneededPolicies = new ArrayList<>();

            for (RangerPolicy policy : rangerPolicies) {

                List<RangerPolicy.RangerPolicyItem> policyItems = policy.getPolicyItems();

                if (CollectionUtils.isNotEmpty(policyItems)) {

                    List<RangerPolicy.RangerPolicyItem> unneededPolicyItems = new ArrayList< RangerPolicy.RangerPolicyItem>();

                    for (RangerPolicy.RangerPolicyItem policyItem : policyItems) {

                        List<RangerPolicy.RangerPolicyItemAccess> policyItemAccesses = policyItem.getAccesses();

                        if (CollectionUtils.isNotEmpty(policyItemAccesses)) {

                            List<RangerPolicy.RangerPolicyItemAccess> unneededItemAccesses = new ArrayList<RangerPolicy.RangerPolicyItemAccess>();

                            for (RangerPolicy.RangerPolicyItemAccess access : policyItemAccesses) {

                                String accessType = access.getType();

                                if (StringUtils.startsWith(accessType, prefix)) {

                                    String newAccessType = StringUtils.removeStart(accessType, prefix);

                                    access.setType(newAccessType);

                                } else if (accessType.contains(AbstractServiceStore.COMPONENT_ACCESSTYPE_SEPARATOR)) {
                                    unneededItemAccesses.add(access);
                                }
                            }
                            policyItemAccesses.removeAll(unneededItemAccesses);

                            if (policyItemAccesses.isEmpty() && !policyItem.getDelegateAdmin()) {
                                unneededPolicyItems.add(policyItem);
                            }
                        }
                    }

                    policyItems.removeAll(unneededPolicyItems);
                }
                if (CollectionUtils.isEmpty(policyItems) && !policy.getIsAuditEnabled()) {
                    unneededPolicies.add(policy);
                }
            }
            rangerPolicies.removeAll(unneededPolicies);
        }

        return rangerPolicies;
    }

    private void init(RangerPolicyEngineOptions options) {

        List<RangerPolicyEvaluator> policyEvaluators = new ArrayList<RangerPolicyEvaluator>();
        for (RangerPolicy policy : policies) {
            if (!policy.getIsEnabled()) {
                continue;
            }

            RangerPolicyEvaluator evaluator = buildPolicyEvaluator(policy, serviceDef, options);

            if (evaluator != null) {
                policyEvaluators.add(evaluator);
            }
        }
        Collections.sort(policyEvaluators);
        this.policyEvaluators = Collections.unmodifiableList(policyEvaluators);

        List<RangerContextEnricher> contextEnrichers = new ArrayList<RangerContextEnricher>();
        if (CollectionUtils.isNotEmpty(this.policyEvaluators)) {
            if (!options.disableContextEnrichers && !CollectionUtils.isEmpty(serviceDef.getContextEnrichers())) {
                for (RangerServiceDef.RangerContextEnricherDef enricherDef : serviceDef.getContextEnrichers()) {
                    if (enricherDef == null) {
                        continue;
                    }

                    RangerContextEnricher contextEnricher = buildContextEnricher(enricherDef);

                    if (contextEnricher != null) {
                        contextEnrichers.add(contextEnricher);
                    }
                }
            }
        }
        this.contextEnrichers = Collections.unmodifiableList(contextEnrichers);

        if(LOG.isDebugEnabled()) {
            LOG.debug("policy evaluation order: " + this.policyEvaluators.size() + " policies");

            int order = 0;
            for(RangerPolicyEvaluator policyEvaluator : this.policyEvaluators) {
                RangerPolicy policy = policyEvaluator.getPolicy();

                LOG.debug("policy evaluation order: #" + (++order) + " - policy id=" + policy.getId() + "; name=" + policy.getName() + "; evalOrder=" + policyEvaluator.getEvalOrder());
            }
        }
    }

    private RangerContextEnricher buildContextEnricher(RangerServiceDef.RangerContextEnricherDef enricherDef) {
        if(LOG.isDebugEnabled()) {
            LOG.debug("==> RangerPolicyRepository.buildContextEnricher(" + enricherDef + ")");
        }

        RangerContextEnricher ret = null;

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
            ret.setContextServiceName(componentServiceName);
            ret.setContextServiceDef(componentServiceDef);
            ret.init();
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

        RangerPolicyEvaluator ret;

        if(StringUtils.equalsIgnoreCase(options.evaluatorType, RangerPolicyEvaluator.EVALUATOR_TYPE_DEFAULT)) {
            ret = new RangerOptimizedPolicyEvaluator();
        } else if(StringUtils.equalsIgnoreCase(options.evaluatorType, RangerPolicyEvaluator.EVALUATOR_TYPE_OPTIMIZED)) {
            ret = new RangerOptimizedPolicyEvaluator();
        } else {
            ret = new RangerCachedPolicyEvaluator();
        }

        ret.init(policy, serviceDef, options);

        if(LOG.isDebugEnabled()) {
            LOG.debug("<== RangerPolicyRepository.buildPolicyEvaluator(" + policy + "," + serviceDef + "): " + ret);
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

        if ((ret.getIsAuditedDetermined())) {
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
