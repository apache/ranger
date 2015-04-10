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
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyResource;
import org.apache.ranger.plugin.policyevaluator.RangerOptimizedPolicyEvaluator;
import org.apache.ranger.plugin.policyevaluator.RangerPolicyEvaluator;
import org.apache.ranger.plugin.util.ServicePolicies;


public class RangerPolicyDb {
	private static final Log LOG = LogFactory.getLog(RangerPolicyDb.class);

	private final ServicePolicies             servicePolicies;
	private final List<RangerPolicyEvaluator> policyEvaluators;

	public RangerPolicyDb(ServicePolicies servicePolicies) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerPolicyDb(" + servicePolicies + ")");
		}

		this.servicePolicies  = servicePolicies;
		this.policyEvaluators = new ArrayList<RangerPolicyEvaluator>();

		RangerServiceDef   serviceDef = servicePolicies.getServiceDef();
		List<RangerPolicy> policies   = servicePolicies.getPolicies();

		if(serviceDef != null && policies != null) {
			for (RangerPolicy policy : policies) {
				if (!policy.getIsEnabled()) {
					continue;
				}

				RangerPolicyEvaluator evaluator = new RangerOptimizedPolicyEvaluator();

				if (evaluator != null) {
					evaluator.init(policy, serviceDef);

					policyEvaluators.add(evaluator);
				}
			}
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerPolicyDb(" + servicePolicies + ")");
		}
	}

	public String getServiceName() {
		return servicePolicies.getServiceName();
	}

	public long getPolicyVersion() {
		Long policyVersion = servicePolicies.getPolicyVersion();

		return policyVersion != null ? policyVersion.longValue() : -1;
	}

	public boolean isAccessAllowed(Map<String, RangerPolicyResource> resources, String user, Set<String> userGroups, String accessType) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerPolicyDb.isAccessAllowed(" + resources + ", " + user + ", " + userGroups + ", " + accessType + ")");
		}

		boolean ret = false;

		for(RangerPolicyEvaluator evaluator : policyEvaluators) {
			ret = evaluator.isAccessAllowed(resources, user, userGroups, accessType);

			if(ret) {
				break;
			}
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerPolicyDb.isAccessAllowed(" + resources + ", " + user + ", " + userGroups + ", " + accessType + "): " + ret);
		}

		return ret;
	}

	public List<RangerPolicy> getAllowedPolicies(String user, Set<String> userGroups, String accessType) {
		List<RangerPolicy> ret = new ArrayList<RangerPolicy>();

		for(RangerPolicyEvaluator evaluator : policyEvaluators) {
			RangerPolicy policy = evaluator.getPolicy();

			boolean isAccessAllowed = isAccessAllowed(policy.getResources(), user, userGroups, accessType);

			if(isAccessAllowed) {
				ret.add(policy);
			}
		}

		return ret;
	}
}
