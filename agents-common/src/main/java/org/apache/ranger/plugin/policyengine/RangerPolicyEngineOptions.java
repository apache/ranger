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

import org.apache.hadoop.conf.Configuration;
import org.apache.ranger.plugin.model.validation.RangerServiceDefHelper;
import org.apache.ranger.plugin.policyevaluator.RangerPolicyEvaluator;

public class RangerPolicyEngineOptions {
    public String evaluatorType = RangerPolicyEvaluator.EVALUATOR_TYPE_AUTO;

    public boolean disableContextEnrichers;
    public boolean disableCustomConditions;
    public boolean disableTagPolicyEvaluation;
    public boolean disableTrieLookupPrefilter;
    public boolean disablePolicyRefresher;
    public boolean disableTagRetriever;
    public boolean disableUserStoreRetriever;
    public boolean disableGdsInfoRetriever;
    public boolean cacheAuditResults                           = true;
    public boolean evaluateDelegateAdminOnly;
    public boolean enableTagEnricherWithLocalRefresher;
    public boolean enableUserStoreEnricherWithLocalRefresher;
    public boolean enableResourceMatcherReuse                  = true;
    @Deprecated
    public boolean disableAccessEvaluationWithPolicyACLSummary = true;
    public boolean optimizeTrieForRetrieval;
    public boolean disableRoleResolution                       = true;
    public boolean optimizeTrieForSpace;
    public boolean optimizeTagTrieForRetrieval;
    public boolean optimizeTagTrieForSpace;

    private RangerServiceDefHelper serviceDefHelper;

    public RangerPolicyEngineOptions() {}

    public RangerPolicyEngineOptions(final RangerPolicyEngineOptions other) {
        this.evaluatorType                             = other.evaluatorType;
        this.disableContextEnrichers                   = other.disableContextEnrichers;
        this.disableCustomConditions                   = other.disableCustomConditions;
        this.disableTagPolicyEvaluation                = other.disableTagPolicyEvaluation;
        this.disableTrieLookupPrefilter                = other.disableTrieLookupPrefilter;
        this.disablePolicyRefresher                    = other.disablePolicyRefresher;
        this.disableTagRetriever                       = other.disableTagRetriever;
        this.disableUserStoreRetriever                 = other.disableUserStoreRetriever;
        this.disableGdsInfoRetriever                   = other.disableGdsInfoRetriever;
        this.cacheAuditResults                         = other.cacheAuditResults;
        this.evaluateDelegateAdminOnly                 = other.evaluateDelegateAdminOnly;
        this.enableTagEnricherWithLocalRefresher       = other.enableTagEnricherWithLocalRefresher;
        this.enableUserStoreEnricherWithLocalRefresher = other.enableUserStoreEnricherWithLocalRefresher;
        this.enableResourceMatcherReuse                = other.enableResourceMatcherReuse;
        this.optimizeTrieForRetrieval                  = other.optimizeTrieForRetrieval;
        this.disableRoleResolution                     = other.disableRoleResolution;
        this.serviceDefHelper                          = null;
        this.optimizeTrieForSpace                      = other.optimizeTrieForSpace;
        this.optimizeTagTrieForRetrieval               = other.optimizeTagTrieForRetrieval;
        this.optimizeTagTrieForSpace                   = other.optimizeTagTrieForSpace;
    }

    public RangerPolicyEngineOptions(final RangerPolicyEngineOptions other, RangerServiceDefHelper serviceDefHelper) {
        this(other);

        this.serviceDefHelper = serviceDefHelper;
    }

    public void configureForPlugin(Configuration conf, String propertyPrefix) {
        disableContextEnrichers    = conf.getBoolean(propertyPrefix + ".policyengine.option.disable.context.enrichers", false);
        disableCustomConditions    = conf.getBoolean(propertyPrefix + ".policyengine.option.disable.custom.conditions", false);
        disableTagPolicyEvaluation = conf.getBoolean(propertyPrefix + ".policyengine.option.disable.tagpolicy.evaluation", false);
        disableTrieLookupPrefilter = conf.getBoolean(propertyPrefix + ".policyengine.option.disable.trie.lookup.prefilter", false);
        disablePolicyRefresher     = conf.getBoolean(propertyPrefix + ".policyengine.option.disable.policy.refresher", false);
        disableTagRetriever        = conf.getBoolean(propertyPrefix + ".policyengine.option.disable.tag.retriever", false);
        disableUserStoreRetriever  = conf.getBoolean(propertyPrefix + ".policyengine.option.disable.userstore.retriever", false);
        disableGdsInfoRetriever    = conf.getBoolean(propertyPrefix + ".policyengine.option.disable.gdsinfo.retriever", false);

        cacheAuditResults          = conf.getBoolean(propertyPrefix + ".policyengine.option.cache.audit.results", true);
        enableResourceMatcherReuse = conf.getBoolean(propertyPrefix + ".policyengine.option.enable.resourcematcher.reuse", true);

        if (!disableTrieLookupPrefilter) {
            cacheAuditResults = false;
        }
        evaluateDelegateAdminOnly                 = false;
        enableTagEnricherWithLocalRefresher       = false;
        enableUserStoreEnricherWithLocalRefresher = false;
        optimizeTrieForRetrieval                  = conf.getBoolean(propertyPrefix + ".policyengine.option.optimize.trie.for.retrieval", false);
        disableRoleResolution                     = conf.getBoolean(propertyPrefix + ".policyengine.option.disable.role.resolution", true);
        optimizeTrieForSpace                      = conf.getBoolean(propertyPrefix + ".policyengine.option.optimize.trie.for.space", false);
        optimizeTagTrieForRetrieval               = conf.getBoolean(propertyPrefix + ".policyengine.option.optimize.tag.trie.for.retrieval", false);
        optimizeTagTrieForSpace                   = conf.getBoolean(propertyPrefix + ".policyengine.option.optimize.tag.trie.for.space", false);
    }

    public void configureDefaultRangerAdmin(Configuration conf, String propertyPrefix) {
        disableContextEnrichers    = conf.getBoolean(propertyPrefix + ".policyengine.option.disable.context.enrichers", true);
        disableCustomConditions    = conf.getBoolean(propertyPrefix + ".policyengine.option.disable.custom.conditions", true);
        disableTagPolicyEvaluation = conf.getBoolean(propertyPrefix + ".policyengine.option.disable.tagpolicy.evaluation", true);
        disableTrieLookupPrefilter = conf.getBoolean(propertyPrefix + ".policyengine.option.disable.trie.lookup.prefilter", false);
        disablePolicyRefresher     = conf.getBoolean(propertyPrefix + ".policyengine.option.disable.policy.refresher", true);
        disableTagRetriever        = conf.getBoolean(propertyPrefix + ".policyengine.option.disable.tag.retriever", true);
        disableUserStoreRetriever  = conf.getBoolean(propertyPrefix + ".policyengine.option.disable.userstore.retriever", true);
        disableGdsInfoRetriever    = conf.getBoolean(propertyPrefix + ".policyengine.option.disable.gdsinfo.retriever", true);

        cacheAuditResults                         = false;
        evaluateDelegateAdminOnly                 = false;
        enableTagEnricherWithLocalRefresher       = false;
        enableUserStoreEnricherWithLocalRefresher = false;
        optimizeTrieForRetrieval                  = conf.getBoolean(propertyPrefix + ".policyengine.option.optimize.trie.for.retrieval", false);
        disableRoleResolution                     = conf.getBoolean(propertyPrefix + ".policyengine.option.disable.role.resolution", true);
        enableResourceMatcherReuse                = conf.getBoolean(propertyPrefix + ".policyengine.option.enable.resourcematcher.reuse", true);
    }

    public void configureDelegateAdmin(Configuration conf, String propertyPrefix) {
        disableContextEnrichers    = conf.getBoolean(propertyPrefix + ".policyengine.option.disable.context.enrichers", true);
        disableCustomConditions    = conf.getBoolean(propertyPrefix + ".policyengine.option.disable.custom.conditions", true);
        disableTagPolicyEvaluation = conf.getBoolean(propertyPrefix + ".policyengine.option.disable.tagpolicy.evaluation", true);
        disableTrieLookupPrefilter = conf.getBoolean(propertyPrefix + ".policyengine.option.disable.trie.lookup.prefilter", false);
        disablePolicyRefresher     = conf.getBoolean(propertyPrefix + ".policyengine.option.disable.policy.refresher", true);
        disableTagRetriever        = conf.getBoolean(propertyPrefix + ".policyengine.option.disable.tag.retriever", true);
        disableUserStoreRetriever  = conf.getBoolean(propertyPrefix + ".policyengine.option.disable.userstore.retriever", true);
        disableGdsInfoRetriever    = conf.getBoolean(propertyPrefix + ".policyengine.option.disable.gdsinfo.retriever", true);
        optimizeTrieForRetrieval   = conf.getBoolean(propertyPrefix + ".policyengine.option.optimize.trie.for.retrieval", false);
        enableResourceMatcherReuse = conf.getBoolean(propertyPrefix + ".policyengine.option.enable.resourcematcher.reuse", true);

        cacheAuditResults                         = false;
        evaluateDelegateAdminOnly                 = true;
        enableTagEnricherWithLocalRefresher       = false;
        enableUserStoreEnricherWithLocalRefresher = false;
    }

    public void configureRangerAdminForPolicySearch(Configuration conf, String propertyPrefix) {
        disableContextEnrichers    = conf.getBoolean(propertyPrefix + ".policyengine.option.disable.context.enrichers", true);
        disableCustomConditions    = conf.getBoolean(propertyPrefix + ".policyengine.option.disable.custom.conditions", true);
        disableTagPolicyEvaluation = conf.getBoolean(propertyPrefix + ".policyengine.option.disable.tagpolicy.evaluation", false);
        disableTrieLookupPrefilter = conf.getBoolean(propertyPrefix + ".policyengine.option.disable.trie.lookup.prefilter", false);
        disablePolicyRefresher     = conf.getBoolean(propertyPrefix + ".policyengine.option.disable.policy.refresher", true);
        disableTagRetriever        = conf.getBoolean(propertyPrefix + ".policyengine.option.disable.tag.retriever", false);
        disableUserStoreRetriever  = conf.getBoolean(propertyPrefix + ".policyengine.option.disable.userstore.retriever", false);
        disableGdsInfoRetriever    = conf.getBoolean(propertyPrefix + ".policyengine.option.disable.gdsinfo.retriever", false);
        optimizeTrieForRetrieval   = conf.getBoolean(propertyPrefix + ".policyengine.option.optimize.trie.for.retrieval", false);

        cacheAuditResults                         = false;
        evaluateDelegateAdminOnly                 = false;
        enableTagEnricherWithLocalRefresher       = true;
        enableUserStoreEnricherWithLocalRefresher = true;

        optimizeTrieForSpace        = conf.getBoolean(propertyPrefix + ".policyengine.option.optimize.trie.for.space", false);
        optimizeTagTrieForRetrieval = conf.getBoolean(propertyPrefix + ".policyengine.option.optimize.tag.trie.for.retrieval", false);
        optimizeTagTrieForSpace     = conf.getBoolean(propertyPrefix + ".policyengine.option.optimize.tag.trie.for.space", true);
        enableResourceMatcherReuse  = conf.getBoolean(propertyPrefix + ".policyengine.option.enable.resourcematcher.reuse", true);
    }

    public RangerServiceDefHelper getServiceDefHelper() {
        return serviceDefHelper;
    }

    void setServiceDefHelper(RangerServiceDefHelper serviceDefHelper) {
        this.serviceDefHelper = serviceDefHelper;
    }

    /*
     * There is no need to implement these, as the options are predefined in a component ServiceREST and hence
     * guaranteed to be unique objects. That implies that the default equals and hashCode should suffice.
     */

    @Override
    public int hashCode() {
        int ret = 0;
        ret += disableContextEnrichers ? 1 : 0;
        ret *= 2;
        ret += disableCustomConditions ? 1 : 0;
        ret *= 2;
        ret += disableTagPolicyEvaluation ? 1 : 0;
        ret *= 2;
        ret += disableTrieLookupPrefilter ? 1 : 0;
        ret *= 2;
        ret += disablePolicyRefresher ? 1 : 0;
        ret *= 2;
        ret += disableTagRetriever ? 1 : 0;
        ret *= 2;
        ret += disableUserStoreRetriever ? 1 : 0;
        ret *= 2;
        ret += disableGdsInfoRetriever ? 1 : 0;
        ret *= 2;
        ret += cacheAuditResults ? 1 : 0;
        ret *= 2;
        ret += evaluateDelegateAdminOnly ? 1 : 0;
        ret *= 2;
        ret += enableTagEnricherWithLocalRefresher ? 1 : 0;
        ret *= 2;
        ret += enableUserStoreEnricherWithLocalRefresher ? 1 : 0;
        ret *= 2;
        ret += optimizeTrieForRetrieval ? 1 : 0;
        ret *= 2;
        ret += disableRoleResolution ? 1 : 0;
        ret *= 2;
        ret += optimizeTrieForSpace ? 1 : 0;
        ret *= 2;
        ret += optimizeTagTrieForRetrieval ? 1 : 0;
        ret *= 2;
        ret += optimizeTagTrieForSpace ? 1 : 0;
        ret *= 2;
        ret += enableResourceMatcherReuse ? 1 : 0;
        ret *= 2;
        return ret;
    }

    @Override
    public boolean equals(Object other) {
        boolean ret = false;
        if (other instanceof RangerPolicyEngineOptions) {
            RangerPolicyEngineOptions that = (RangerPolicyEngineOptions) other;
            ret = this.disableContextEnrichers == that.disableContextEnrichers
                    && this.disableCustomConditions == that.disableCustomConditions
                    && this.disableTagPolicyEvaluation == that.disableTagPolicyEvaluation
                    && this.disableTrieLookupPrefilter == that.disableTrieLookupPrefilter
                    && this.disablePolicyRefresher == that.disablePolicyRefresher
                    && this.disableTagRetriever == that.disableTagRetriever
                    && this.disableUserStoreRetriever == that.disableUserStoreRetriever
                    && this.disableGdsInfoRetriever == that.disableGdsInfoRetriever
                    && this.cacheAuditResults == that.cacheAuditResults
                    && this.evaluateDelegateAdminOnly == that.evaluateDelegateAdminOnly
                    && this.enableTagEnricherWithLocalRefresher == that.enableTagEnricherWithLocalRefresher
                    && this.enableUserStoreEnricherWithLocalRefresher == that.enableUserStoreEnricherWithLocalRefresher
                    && this.optimizeTrieForRetrieval == that.optimizeTrieForRetrieval
                    && this.disableRoleResolution == that.disableRoleResolution
                    && this.optimizeTrieForSpace == that.optimizeTrieForSpace
                    && this.optimizeTagTrieForRetrieval == that.optimizeTagTrieForRetrieval
                    && this.optimizeTagTrieForSpace == that.optimizeTagTrieForSpace
                    && this.enableResourceMatcherReuse == that.enableResourceMatcherReuse;
        }
        return ret;
    }

    @Override
    public String toString() {
        return "PolicyEngineOptions: {" +
                " evaluatorType: " + evaluatorType +
                ", evaluateDelegateAdminOnly: " + evaluateDelegateAdminOnly +
                ", disableContextEnrichers: " + disableContextEnrichers +
                ", disableCustomConditions: " + disableContextEnrichers +
                ", disableTagPolicyEvaluation: " + disableTagPolicyEvaluation +
                ", disablePolicyRefresher: " + disablePolicyRefresher +
                ", disableTagRetriever: " + disableTagRetriever +
                ", disableUserStoreRetriever: " + disableUserStoreRetriever +
                ", disableGdsInfoRetriever: " + disableGdsInfoRetriever +
                ", enableTagEnricherWithLocalRefresher: " + enableTagEnricherWithLocalRefresher +
                ", enableUserStoreEnricherWithLocalRefresher: " + enableUserStoreEnricherWithLocalRefresher +
                ", disableTrieLookupPrefilter: " + disableTrieLookupPrefilter +
                ", optimizeTrieForRetrieval: " + optimizeTrieForRetrieval +
                ", cacheAuditResult: " + cacheAuditResults +
                ", disableRoleResolution: " + disableRoleResolution +
                ", optimizeTrieForSpace: " + optimizeTrieForSpace +
                ", optimizeTagTrieForRetrieval: " + optimizeTagTrieForRetrieval +
                ", optimizeTagTrieForSpace: " + optimizeTagTrieForSpace +
                ", enableResourceMatcherReuse: " + enableResourceMatcherReuse +
                " }";
    }
}
