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

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ranger.plugin.store.ServiceStore;
import org.apache.ranger.plugin.util.ServicePolicies;

class RangerPolicyEngineCache {
	private static final Log LOG = LogFactory.getLog(RangerPolicyEngineCache.class);

	private final Map<String, RangerPolicyEngine> policyEngineCache = new HashMap<String, RangerPolicyEngine>();

	synchronized final RangerPolicyEngine getPolicyEngine(String serviceName, ServiceStore svcStore, RangerPolicyEngineOptions options) {
		RangerPolicyEngine ret = null;

		if(serviceName != null) {
			ret = policyEngineCache.get(serviceName);

			long policyVersion = ret != null ? ret.getPolicyVersion() : -1;

			if(svcStore != null) {
				try {
					ServicePolicies policies = svcStore.getServicePoliciesIfUpdated(serviceName, policyVersion);

					if(policies != null) {
						if(ret == null) {
							ret = addPolicyEngine(policies, options);
						} else if(policies.getPolicyVersion() != null && !policies.getPolicyVersion().equals(policyVersion)) {
							ret = addPolicyEngine(policies, options);
						}
					}
				} catch(Exception excp) {
					LOG.error("getPolicyEngine(" + serviceName + "): failed to get latest policies from service-store", excp);
				}
			}
		}

		return ret;
	}


	private RangerPolicyEngine addPolicyEngine(ServicePolicies policies, RangerPolicyEngineOptions options) {
		RangerPolicyEngine ret = new RangerPolicyEngineImpl("ranger-admin", policies, options);

		policyEngineCache.put(policies.getServiceName(), ret);

		return ret;
	}
}
