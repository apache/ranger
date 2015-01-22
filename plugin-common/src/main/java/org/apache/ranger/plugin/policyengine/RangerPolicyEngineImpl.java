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
import java.util.Collection;
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

	private String                      serviceName         = null;
	private RangerServiceDef            serviceDef          = null;
	private List<RangerPolicyEvaluator> policyEvaluators    = null;
	private RangerAuditHandler          defaultAuditHandler = null;


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

			/* TODO:
			 *  sort evaluators list for faster completion of isAccessAllowed() method
			 *   1. Global policies: the policies that cover for any resource (for example: database=*; table=*; column=*)
			 *   2. Policies that cover all resources under level-1 (for example: every thing in one or more databases)
			 *   3. Policies that cover all resources under level-2 (for example: every thing in one or more tables)
			 *   ...
			 *   4. Policies that cover all resources under level-n (for example: one or more columns)
			 * 
			 */

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
	public void setDefaultAuditHandler(RangerAuditHandler auditHandler) {
		this.defaultAuditHandler = auditHandler;
	}

	@Override
	public RangerAuditHandler getDefaultAuditHandler() {
		return defaultAuditHandler;
	}

	@Override
	public RangerAccessResult createAccessResult(RangerAccessRequest request) {
		return new RangerAccessResult(serviceName, serviceDef, request);	
	}

	@Override
	public RangerAccessResult isAccessAllowed(RangerAccessRequest request) {
		return isAccessAllowed(request, defaultAuditHandler);
	}

	@Override
	public Collection<RangerAccessResult> isAccessAllowed(Collection<RangerAccessRequest> requests) {
		return isAccessAllowed(requests, defaultAuditHandler);
	}

	@Override
	public RangerAccessResult isAccessAllowed(RangerAccessRequest request, RangerAuditHandler auditHandler) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerPolicyEngineImpl.isAccessAllowed(" + request + ")");
		}

		RangerAccessResult ret = isAccessAllowedNoAudit(request);

		if(auditHandler != null) {
			auditHandler.logAudit(ret);
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerPolicyEngineImpl.isAccessAllowed(" + request + "): " + ret);
		}

		return ret;
	}

	@Override
	public Collection<RangerAccessResult> isAccessAllowed(Collection<RangerAccessRequest> requests, RangerAuditHandler auditHandler) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerPolicyEngineImpl.isAccessAllowed(" + requests + ")");
		}
		
		Collection<RangerAccessResult> ret = new ArrayList<RangerAccessResult>();

		if(requests != null) {
			for(RangerAccessRequest request : requests) {
				RangerAccessResult result = isAccessAllowedNoAudit(request);

				ret.add(result);
			}
		}

		if(auditHandler != null) {
			auditHandler.logAudit(ret);
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

		RangerAccessResult ret = createAccessResult(request);

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

		sb.append("serviceName={").append(serviceName).append("} ");
		sb.append("serviceDef={").append(serviceDef).append("} ");

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
