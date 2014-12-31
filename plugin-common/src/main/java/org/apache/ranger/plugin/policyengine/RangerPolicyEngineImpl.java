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
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ranger.plugin.manager.ServiceDefManager;
import org.apache.ranger.plugin.manager.ServiceManager;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItem;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyResource;
import org.apache.ranger.plugin.model.RangerService;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.model.RangerServiceDef.RangerResourceDef;
import org.apache.ranger.plugin.policyevaluator.RangerPolicyEvaluator;


public class RangerPolicyEngineImpl implements RangerPolicyEngine {
	private static final Log LOG = LogFactory.getLog(RangerPolicyEngineImpl.class);

	private String                      svcName          = null;
	private List<RangerPolicyEvaluator> policyEvaluators = null;


	public RangerPolicyEngineImpl() {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerPolicyEngine()");
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerPolicyEngine()");
		}
	}
	
	public void init(String serviceName) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerPolicyEngine.init(" + serviceName + ")");
		}

		svcName          = serviceName;
		policyEvaluators = new ArrayList<RangerPolicyEvaluator>();

		ServiceManager svcMgr  = new ServiceManager();
		RangerService  service = svcMgr.getByName(svcName);

		if(service == null) {
			LOG.error(svcName + ": service not found");
		} else {
			ServiceDefManager sdMgr = new ServiceDefManager();

			RangerServiceDef serviceDef = sdMgr.getByName(service.getType());

			if(serviceDef == null) {
				String msg = service.getType() + ": service-def not found";

				LOG.error(msg);

				throw new Exception(msg);
			}

			List<RangerPolicy> policies = svcMgr.getPolicies(service.getId());
			
			if(policies != null) {
				for(RangerPolicy policy : policies) {
					RangerPolicyEvaluator evaluator = getPolicyEvaluator(policy, serviceDef);

					if(evaluator != null) {
						policyEvaluators.add(evaluator);
					}
				}
			}

			if(LOG.isDebugEnabled()) {
				LOG.debug("found " + (policyEvaluators == null ? 0 : policyEvaluators.size()) + " policies in service '" + svcName + "'");
			}
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerPolicyEngine.init(" + serviceName + ")");
		}
	}

	private RangerPolicyEvaluator getPolicyEvaluator(RangerPolicy policy, RangerServiceDef serviceDef) {
		RangerPolicyEvaluator ret = null;

		// TODO: instantiate policy-matcher

		return ret;
	}

	@Override
	public RangerAccessResult isAccessAllowed(RangerAccessRequest request) {
		RangerAccessResult ret = null;

		for(RangerPolicyEvaluator evaluator : policyEvaluators) {
			ret = evaluator.evaluate(request);
			
			if(ret != null) {
				break;
			}
		}

		if(ret == null) {
			ret = new RangerAccessResult(request);

			ret.setAllowed(Boolean.FALSE);
			ret.setAudited(Boolean.FALSE);
		}

		return ret;
	}

	@Override
	public void isAccessAllowed(List<RangerAccessRequest> requests, List<RangerAccessResult> results) {
		if(requests != null && results != null) {
			results.clear();

			for(int i = 0; i < requests.size(); i++) {
				RangerAccessRequest request = requests.get(i);
				RangerAccessResult  result  = isAccessAllowed(request);
				
				results.add(result);
			}
		}
	}

	@Override
	public void auditAccess(RangerAccessResult result) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void auditAccess(List<RangerAccessResult> results) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public String toString( ) {
		StringBuilder sb = new StringBuilder();

		toString(sb);

		return sb.toString();
	}

	public StringBuilder toString(StringBuilder sb) {
		sb.append("RangerPolicyEngineImpl={");

		sb.append("svcName={").append(svcName).append("} ");

		sb.append("policyEvaluators={");
		if(policyEvaluators != null) {
			for(RangerPolicyEvaluator policyEvaluator : policyEvaluators) {
				if(policyEvaluator != null) {
					sb.append(policyEvaluator).append(" ");
				}
			}
		}
		sb.append("} ");

		sb.append("}");

		return sb;
	}
}
