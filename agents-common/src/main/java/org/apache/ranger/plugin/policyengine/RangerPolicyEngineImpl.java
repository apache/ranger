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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ranger.plugin.audit.RangerAuditHandler;
import org.apache.ranger.plugin.contextenricher.RangerContextEnricher;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.policyevaluator.RangerPolicyEvaluator;
import org.apache.ranger.plugin.util.ServicePolicies;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;


public class RangerPolicyEngineImpl implements RangerPolicyEngine {
	private static final Log LOG = LogFactory.getLog(RangerPolicyEngineImpl.class);

	private ServicePolicies        servicePolicies     = null;
	private RangerPolicyRepository policyRepository    = null;
	private RangerAuditHandler     defaultAuditHandler = null;

	public RangerPolicyEngineImpl() {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerPolicyEngineImpl()");
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerPolicyEngineImpl()");
		}
	}

	@Override
	public String getServiceName() {
		RangerPolicyRepository policyRepository = this.policyRepository;

		return policyRepository == null ? null : policyRepository.getServiceName();
	}

	@Override
	public RangerServiceDef getServiceDef() {
		RangerPolicyRepository policyRepository = this.policyRepository;

		return policyRepository == null ? null : policyRepository.getServiceDef();
	}

	@Override
	public List<RangerContextEnricher> getContextEnrichers() {
		RangerPolicyRepository policyRepository = this.policyRepository;

		return policyRepository == null ? null : policyRepository.getContextEnrichers();
	}

	@Override
	public void setPolicies(ServicePolicies servicePolicies) {
		String             serviceName = servicePolicies != null ? servicePolicies.getServiceName() : null;
		RangerServiceDef   serviceDef  = servicePolicies != null ? servicePolicies.getServiceDef() : null;
		List<RangerPolicy> policies    = servicePolicies != null ? servicePolicies.getPolicies() : null;
		
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerPolicyEngineImpl.setPolicies(" + serviceName + ", " + serviceDef + ", policies.count=" + (policies == null ? 0 : policies.size()) + ")");
		}

		if (serviceName != null && serviceDef != null && policies != null) {
			RangerPolicyRepository policyRepository = new RangerPolicyRepository(serviceName);
			policyRepository.init(serviceDef, policies);

			this.servicePolicies  = servicePolicies;
			this.policyRepository = policyRepository;
		} else {
			LOG.error("RangerPolicyEngineImpl.setPolicies ->Invalid arguments: serviceName, serviceDef, or policies is null");
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerPolicyEngineImpl.setPolicies(" + serviceName + ", " + serviceDef + ", policies.count=" + (policies == null ? 0 : policies.size()) + ")");
		}
	}

	@Override
	public ServicePolicies getPolicies() {
		return servicePolicies;
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
		RangerPolicyRepository policyRepository = this.policyRepository;

		return new RangerAccessResult(this.getServiceName(), policyRepository == null ? null : policyRepository.getServiceDef(), request);
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

		RangerPolicyRepository policyRepository = this.policyRepository;

		RangerAccessResult ret = createAccessResult(request);

		if(policyRepository != null && ret != null && request != null) {
			List<RangerPolicyEvaluatorFacade> evaluators = policyRepository.getPolicyEvaluators();

			if(evaluators != null) {
				boolean foundInCache = policyRepository.setAuditEnabledFromCache(request, ret);

				for(RangerPolicyEvaluator evaluator : evaluators) {
					evaluator.evaluate(request, ret);

					// stop once allowed==true && auditedDetermined==true
					if(ret.getIsAccessDetermined() && ret.getIsAuditedDetermined()) {
						break;
					}
				}

				if(! foundInCache) {
					policyRepository.storeAuditEnabledInCache(request, ret);
				}

			}
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerPolicyEngineImpl.isAccessAllowedNoAudit(" + request + "): " + ret);
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
		RangerPolicyRepository policyRepository = this.policyRepository;

		sb.append("RangerPolicyEngineImpl={");

		sb.append("serviceName={").append(this.getServiceName()).append("} ");
		sb.append(policyRepository);

		sb.append("}");

		return sb;
	}
}
