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

package org.apache.ranger.plugin.policyevaluator;


import java.io.Serializable;
import java.util.Comparator;
import java.util.Map;
import java.util.Set;

import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyResource;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.policyengine.RangerAccessRequest;
import org.apache.ranger.plugin.policyengine.RangerAccessResult;
import org.apache.ranger.plugin.policyengine.RangerAccessResource;
import org.apache.ranger.plugin.policyengine.RangerPolicyEngineOptions;
import org.apache.ranger.plugin.policyengine.RangerResourceAccessInfo;
import org.apache.ranger.plugin.policyresourcematcher.RangerPolicyResourceEvaluator;


public interface RangerPolicyEvaluator extends RangerPolicyResourceEvaluator {
	Comparator<RangerPolicyEvaluator> EVAL_ORDER_COMPARATOR = new RangerPolicyEvaluator.PolicyEvalOrderComparator();

	String EVALUATOR_TYPE_AUTO   = "auto";
	String EVALUATOR_TYPE_OPTIMIZED = "optimized";
	String EVALUATOR_TYPE_CACHED    = "cached";

	void init(RangerPolicy policy, RangerServiceDef serviceDef, RangerPolicyEngineOptions options);

	RangerPolicy getPolicy();

	RangerServiceDef getServiceDef();

	boolean hasAllow();

	boolean hasDeny();

	int getEvalOrder();

	long getUsageCount();

	void incrementUsageCount(int number);

	void setUsageCountImmutable();

	void resetUsageCount();

	int getCustomConditionsCount();

	boolean isAuditEnabled();

	void evaluate(RangerAccessRequest request, RangerAccessResult result);

	boolean isMatch(RangerAccessResource resource, Map<String, Object> evalContext);

	boolean isCompleteMatch(RangerAccessResource resource, Map<String, Object> evalContext);

	boolean isCompleteMatch(Map<String, RangerPolicyResource> resources, Map<String, Object> evalContext);

	boolean isAccessAllowed(RangerAccessResource resource, String user, Set<String> userGroups, String accessType);

	boolean isAccessAllowed(Map<String, RangerPolicyResource> resources, String user, Set<String> userGroups, String accessType);

	void getResourceAccessInfo(RangerAccessRequest request, RangerResourceAccessInfo result);

	class PolicyEvalOrderComparator implements Comparator<RangerPolicyEvaluator>, Serializable {
		@Override
		public int compare(RangerPolicyEvaluator me, RangerPolicyEvaluator other) {
			int result;

			if (me.hasDeny() && !other.hasDeny()) {
				result = -1;
			} else if (!me.hasDeny() && other.hasDeny()) {
				result = 1;
			} else {
				result = Long.compare(other.getUsageCount(), me.getUsageCount());

				if (result == 0) {
					result = Integer.compare(me.getEvalOrder(), other.getEvalOrder());
				}
			}

			return result;
		}
	}

}
