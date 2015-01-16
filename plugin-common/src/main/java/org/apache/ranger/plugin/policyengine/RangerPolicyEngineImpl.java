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

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ranger.plugin.audit.RangerAuditHandler;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.policyevaluator.RangerDefaultPolicyEvaluator;
import org.apache.ranger.plugin.policyevaluator.RangerPolicyEvaluator;


public class RangerPolicyEngineImpl implements RangerPolicyEngine {
	private static final Log LOG = LogFactory.getLog(RangerPolicyEngineImpl.class);

	private String                      serviceName      = null;
	private RangerServiceDef            serviceDef       = null;
	private List<RangerPolicyEvaluator> policyEvaluators = null;


	public RangerPolicyEngineImpl() {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerPolicyEngineImpl()");
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerPolicyEngineImpl()");
		}
	}

	@Override
	public void setPolicies(String serviceName, RangerServiceDef serviceDef, List<RangerPolicy> policies) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerPolicyEngineImpl.setPolicies(" + serviceName + ", " + serviceDef + ", " + policies + ")");
		}

		if(serviceName != null && serviceDef != null && policies != null) {
			List<RangerPolicyEvaluator> evaluators = new ArrayList<RangerPolicyEvaluator>();

			for(RangerPolicy policy : policies) {
				if(! policy.getIsEnabled()) {
					continue;
				}

				RangerPolicyEvaluator evaluator = getPolicyEvaluator(policy, serviceDef);

				if(evaluator != null) {
					evaluators.add(evaluator);
				}
			}

			this.serviceName      = serviceName;
			this.serviceDef       = serviceDef;
			this.policyEvaluators = evaluators;
		} else {
			LOG.error("RangerPolicyEngineImpl.setPolicies(): invalid arguments - null serviceDef/policies");
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerPolicyEngineImpl.setPolicies(" + serviceName + ", " + serviceDef + ", " + policies + ")");
		}
	}

	@Override
	public RangerAccessResult isAccessAllowed(RangerAccessRequest request, RangerAuditHandler auditHandler) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerPolicyEngineImpl.isAccessAllowed(" + request + ")");
		}

		RangerAccessResult ret = isAccessAllowedNoAudit(request);

		if(auditHandler != null) {
			auditHandler.logAudit(request, ret);
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerPolicyEngineImpl.isAccessAllowed(" + request + "): " + ret);
		}

		return ret;
	}

	@Override
	public List<RangerAccessResult> isAccessAllowed(List<RangerAccessRequest> requests, RangerAuditHandler auditHandler) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerPolicyEngineImpl.isAccessAllowed(" + requests + ")");
		}
		
		List<RangerAccessResult> ret = new ArrayList<RangerAccessResult>();

		if(requests != null) {
			for(RangerAccessRequest request : requests) {
				RangerAccessResult result = isAccessAllowedNoAudit(request);

				ret.add(result);
			}
		}

		if(auditHandler != null) {
			auditHandler.logAudit(requests, ret);
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerPolicyEngineImpl.isAccessAllowed(" + requests + "): " + ret);
		}

		return ret;
	}

	protected RangerAccessResult isAccessAllowedNoAudit(RangerAccessRequest request) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerPolicyEngineImpl.isAccessAllowedNoAudit(" + request + ")");
		}

		RangerAccessResult ret = new RangerAccessResult(serviceName, serviceDef);

		if(request != null) {
			if(CollectionUtils.isEmpty(request.getAccessTypes())) {
				request.getAccessTypes().add(ANY_ACCESS);
			}

			for(String accessType : request.getAccessTypes()) {
				ret.setAccessTypeResult(accessType, new RangerAccessResult.ResultDetail());
			}

			List<RangerPolicyEvaluator> evaluators = policyEvaluators;

			if(evaluators != null) {
				for(RangerPolicyEvaluator evaluator : evaluators) {
					evaluator.evaluate(request, ret);

					if(ret.isAllAllowedAndAudited()) {
						break;
					}
				}
			}
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerPolicyEngineImpl.isAccessAllowedNoAudit(" + request + "): " + ret);
		}

		return ret;
	}

	private RangerPolicyEvaluator getPolicyEvaluator(RangerPolicy policy, RangerServiceDef serviceDef) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerPolicyEngineImpl.getPolicyEvaluator(" + policy + "," + serviceDef + ")");
		}

		RangerPolicyEvaluator ret = null;

		ret = new RangerDefaultPolicyEvaluator(); // TODO: configurable evaluator class?

		ret.init(policy, serviceDef);

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerPolicyEngineImpl.getPolicyEvaluator(" + policy + "," + serviceDef + "): " + ret);
		}

		return ret;
	}

	@Override
	public String toString( ) {
		StringBuilder sb = new StringBuilder();

		toString(sb);

		return sb.toString();
	}

	public StringBuilder toString(StringBuilder sb) {
		sb.append("RangerPolicyEngineImpl={");

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


	/*
	public void init(String svcName) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerPolicyEngineImpl.init(" + svcName + ")");
		}

		ServiceManager    svcMgr = new ServiceManager();
		ServiceDefManager sdMgr  = new ServiceDefManager();

		RangerServiceDef   serviceDef = null;
		List<RangerPolicy> policies   = null;

		RangerService  service = svcMgr.getByName(svcName);

		if(service == null) {
			String msg = svcName + ": service not found";

			LOG.error(msg);

			throw new Exception(msg);
		} else {
			serviceDef = sdMgr.getByName(service.getType());

			if(serviceDef == null) {
				String msg = service.getType() + ": service-def not found";

				LOG.error(msg);

				throw new Exception(msg);
			}

			policies = svcMgr.getPolicies(service.getId());

			if(LOG.isDebugEnabled()) {
				LOG.debug("RangerPolicyEngineImpl.init(): found " + (policyEvaluators == null ? 0 : policyEvaluators.size()) + " policies in service '" + svcName + "'");
			}
		}

		setPolicies(serviceDef, policies);

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerPolicyEngineImpl.init(" + svcName + ")");
		}
	}
	*/
}
