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
import org.apache.ranger.plugin.policyevaluator.RangerPolicyEvaluator;

public class RangerPolicyEngineOptions {
	public String  evaluatorType           = RangerPolicyEvaluator.EVALUATOR_TYPE_AUTO;
	public boolean cacheAuditResults       = true;
	public boolean disableContextEnrichers;
	public boolean disableCustomConditions;
	public boolean disableTagPolicyEvaluation = true;
	public boolean evaluateDelegateAdminOnly;
	public boolean disableTrieLookupPrefilter;

	public void configureForPlugin(Configuration conf, String propertyPrefix) {
		evaluatorType           = conf.get(propertyPrefix + ".policyengine.option.evaluator.type", RangerPolicyEvaluator.EVALUATOR_TYPE_AUTO);
		cacheAuditResults       = conf.getBoolean(propertyPrefix + ".policyengine.option.cache.audit.results", true);
		disableContextEnrichers = conf.getBoolean(propertyPrefix + ".policyengine.option.disable.context.enrichers", false);
		disableCustomConditions = conf.getBoolean(propertyPrefix + ".policyengine.option.disable.custom.conditions", false);
		disableTagPolicyEvaluation = conf.getBoolean(propertyPrefix + ".policyengine.option.disable.tagpolicy.evaluation", false);
		disableTrieLookupPrefilter = conf.getBoolean(propertyPrefix + ".policyengine.option.disable.trie.lookup.prefilter", false);
	}

	public void configureDefaultRangerAdmin(Configuration conf, String propertyPrefix) {
		evaluatorType             = RangerPolicyEvaluator.EVALUATOR_TYPE_OPTIMIZED;
		cacheAuditResults         = conf.getBoolean(propertyPrefix + ".policyengine.option.cache.audit.results", false);
		disableContextEnrichers   = conf.getBoolean(propertyPrefix + ".policyengine.option.disable.context.enrichers", true);
		disableCustomConditions   = conf.getBoolean(propertyPrefix + ".policyengine.option.disable.custom.conditions", true);
		evaluateDelegateAdminOnly = false;
		disableTrieLookupPrefilter = conf.getBoolean(propertyPrefix + ".policyengine.option.disable.trie.lookup.prefilter", false);
	}

	public void configureDelegateAdmin(Configuration conf, String propertyPrefix) {
		evaluatorType           = RangerPolicyEvaluator.EVALUATOR_TYPE_OPTIMIZED;
		cacheAuditResults       = conf.getBoolean(propertyPrefix + ".policyengine.option.cache.audit.results", false);
		disableContextEnrichers = conf.getBoolean(propertyPrefix + ".policyengine.option.disable.context.enrichers", true);
		disableCustomConditions = conf.getBoolean(propertyPrefix + ".policyengine.option.disable.custom.conditions", true);
		evaluateDelegateAdminOnly = conf.getBoolean(propertyPrefix + ".policyengine.option.evaluate.delegateadmin.only", true);
	}
}
