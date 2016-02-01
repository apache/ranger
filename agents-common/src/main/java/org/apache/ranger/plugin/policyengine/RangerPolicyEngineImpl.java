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
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyResource;
import org.apache.ranger.plugin.policyevaluator.RangerPolicyEvaluator;
import org.apache.ranger.plugin.util.RangerPerfTracer;
import org.apache.ranger.plugin.util.ServicePolicies;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;


public class RangerPolicyEngineImpl implements RangerPolicyEngine {
	private static final Log LOG = LogFactory.getLog(RangerPolicyEngineImpl.class);

	private static final Log PERF_POLICYENGINE_INIT_LOG = RangerPerfTracer.getPerfLogger("policyengine.init");
	private static final Log PERF_POLICYENGINE_REQUEST_LOG = RangerPerfTracer.getPerfLogger("policyengine.request");
	private static final Log PERF_POLICYENGINE_AUDIT_LOG = RangerPerfTracer.getPerfLogger("policyengine.audit");
	private static final Log PERF_CONTEXTENRICHER_REQUEST_LOG = RangerPerfTracer.getPerfLogger("contextenricher.request");

	private static final int MAX_POLICIES_FOR_CACHE_TYPE_EVALUATOR = 100;

	private final RangerPolicyRepository policyRepository;


	public RangerPolicyEngineImpl(ServicePolicies servicePolicies) {
		this(servicePolicies, null);
	}

	public RangerPolicyEngineImpl(ServicePolicies servicePolicies, RangerPolicyEngineOptions options) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerPolicyEngineImpl(" + servicePolicies + ", " + options + ")");
		}

		RangerPerfTracer perf = null;

		if(RangerPerfTracer.isPerfTraceEnabled(PERF_POLICYENGINE_INIT_LOG)) {
			perf = RangerPerfTracer.getPerfTracer(PERF_POLICYENGINE_INIT_LOG, "RangerPolicyEngine.init(hashCode=" + Integer.toHexString(System.identityHashCode(this)) + ")");
		}

		if(options == null) {
			options = new RangerPolicyEngineOptions();
		}

		if(StringUtils.isBlank(options.evaluatorType) || StringUtils.equalsIgnoreCase(options.evaluatorType, RangerPolicyEvaluator.EVALUATOR_TYPE_AUTO)) {

			String serviceType  = servicePolicies.getServiceDef().getName();
			String propertyName = "ranger.plugin." + serviceType + ".policyengine.evaluator.auto.maximum.policycount.for.cache.type";

			int thresholdForUsingOptimizedEvaluator = RangerConfiguration.getInstance().getInt(propertyName, MAX_POLICIES_FOR_CACHE_TYPE_EVALUATOR);

			int servicePoliciesCount = servicePolicies.getPolicies().size();

			if (servicePoliciesCount > thresholdForUsingOptimizedEvaluator) {
				options.evaluatorType = RangerPolicyEvaluator.EVALUATOR_TYPE_OPTIMIZED;
			} else {
				options.evaluatorType = RangerPolicyEvaluator.EVALUATOR_TYPE_CACHED;
			}
		} else if (StringUtils.equalsIgnoreCase(options.evaluatorType, RangerPolicyEvaluator.EVALUATOR_TYPE_CACHED)) {
			options.evaluatorType = RangerPolicyEvaluator.EVALUATOR_TYPE_CACHED;
		} else {
			// All other cases
			options.evaluatorType = RangerPolicyEvaluator.EVALUATOR_TYPE_OPTIMIZED;
		}

		policyRepository = new RangerPolicyRepository(servicePolicies, options);

		RangerPerfTracer.log(perf);

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerPolicyEngineImpl()");
		}
	}

	@Override
	public String getServiceName() {
		return policyRepository.getServiceName();
	}

	@Override
	public RangerServiceDef getServiceDef() {
		return policyRepository.getServiceDef();
	}

	@Override
	public long getPolicyVersion() {
		return policyRepository.getPolicyVersion();
	}

	@Override
	public RangerAccessResult createAccessResult(RangerAccessRequest request) {
		return new RangerAccessResult(this.getServiceName(), policyRepository.getServiceDef(), request);
	}

	@Override
	public void preProcess(RangerAccessRequest request) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerPolicyEngineImpl.preProcess(" + request + ")");
		}

		setResourceServiceDef(request);

		List<RangerContextEnricher> enrichers = policyRepository.getContextEnrichers();

		if(!CollectionUtils.isEmpty(enrichers)) {

			for(RangerContextEnricher enricher : enrichers) {

				RangerPerfTracer perf = null;

				if(RangerPerfTracer.isPerfTraceEnabled(PERF_CONTEXTENRICHER_REQUEST_LOG)) {
					perf = RangerPerfTracer.getPerfTracer(PERF_CONTEXTENRICHER_REQUEST_LOG, "RangerContextEnricher.enrich(requestHashCode=" + Integer.toHexString(System.identityHashCode(request)) + ")");
				}

				enricher.enrich(request);

				RangerPerfTracer.log(perf);
			}

		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerPolicyEngineImpl.preProcess(" + request + ")");
		}
	}

	@Override
	public void preProcess(Collection<RangerAccessRequest> requests) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerPolicyEngineImpl.preProcess(" + requests + ")");
		}

		if(CollectionUtils.isNotEmpty(requests)) {
			for(RangerAccessRequest request : requests) {
				setResourceServiceDef(request);
			}

			List<RangerContextEnricher> enrichers = policyRepository.getContextEnrichers();

			if(CollectionUtils.isNotEmpty(enrichers)) {
				for(RangerContextEnricher enricher : enrichers) {
					for(RangerAccessRequest request : requests) {
						enricher.enrich(request);
					}
				}
			}
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerPolicyEngineImpl.preProcess(" + requests + ")");
		}
	}

	@Override
	public RangerAccessResult isAccessAllowed(RangerAccessRequest request, RangerAccessResultProcessor resultProcessor) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerPolicyEngineImpl.isAccessAllowed(" + request + ")");
		}

		RangerPerfTracer perf = null;

		if(RangerPerfTracer.isPerfTraceEnabled(PERF_POLICYENGINE_REQUEST_LOG)) {
			perf = RangerPerfTracer.getPerfTracer(PERF_POLICYENGINE_REQUEST_LOG, "RangerPolicyEngine.isAccessAllowed(requestHashCode=" + Integer.toHexString(System.identityHashCode(request)) + ")");
		}

		RangerAccessResult ret = isAccessAllowedNoAudit(request);

		if (resultProcessor != null) {

			RangerPerfTracer perfAuditTracer = null;
			if(RangerPerfTracer.isPerfTraceEnabled(PERF_POLICYENGINE_AUDIT_LOG)) {
				perfAuditTracer = RangerPerfTracer.getPerfTracer(PERF_POLICYENGINE_AUDIT_LOG, "RangerPolicyEngine.processAudit(requestHashCode=" + Integer.toHexString(System.identityHashCode(request)) + ")");
			}

			resultProcessor.processResult(ret);

			RangerPerfTracer.log(perfAuditTracer);
		}

		RangerPerfTracer.log(perf);

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerPolicyEngineImpl.isAccessAllowed(" + request + "): " + ret);
		}

		return ret;
	}

	@Override
	public Collection<RangerAccessResult> isAccessAllowed(Collection<RangerAccessRequest> requests, RangerAccessResultProcessor resultProcessor) {
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

		if(resultProcessor != null) {
			resultProcessor.processResults(ret);
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerPolicyEngineImpl.isAccessAllowed(" + requests + "): " + ret);
		}

		return ret;
	}

	@Override
	public boolean isAccessAllowed(RangerAccessResource resource, String user, Set<String> userGroups, String accessType) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerPolicyEngineImpl.isAccessAllowed(" + resource + ", " + user + ", " + userGroups + ", " + accessType + ")");
		}

		RangerPerfTracer perf = null;

		if(RangerPerfTracer.isPerfTraceEnabled(PERF_POLICYENGINE_REQUEST_LOG)) {
			perf = RangerPerfTracer.getPerfTracer(PERF_POLICYENGINE_REQUEST_LOG, "RangerPolicyEngine.isAccessAllowed(user=" + user + ",accessType=" + accessType + "resource=" + resource.getAsString() + ")");
		}
		boolean ret = false;

		for(RangerPolicyEvaluator evaluator : policyRepository.getPolicyEvaluators()) {
			ret = evaluator.isAccessAllowed(resource, user, userGroups, accessType);

			if(ret) {
				break;
			}
		}

		RangerPerfTracer.log(perf);

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerPolicyEngineImpl.isAccessAllowed(" + resource + ", " + user + ", " + userGroups + ", " + accessType + "): " + ret);
		}

		return ret;
	}


	@Override
	public boolean isAccessAllowed(Map<String, RangerPolicyResource> resources, String user, Set<String> userGroups, String accessType) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerPolicyEngineImpl.isAccessAllowed(" + resources + ", " + user + ", " + userGroups + ", " + accessType + ")");
		}

		boolean ret = false;

		RangerPerfTracer perf = null;

		if(RangerPerfTracer.isPerfTraceEnabled(PERF_POLICYENGINE_REQUEST_LOG)) {
			perf = RangerPerfTracer.getPerfTracer(PERF_POLICYENGINE_REQUEST_LOG, "RangerPolicyEngine.isAccessAllowed(user=" + user + "," + userGroups + ",accessType=" + accessType + ")");
		}

		for(RangerPolicyEvaluator evaluator : policyRepository.getPolicyEvaluators()) {
			ret = evaluator.isAccessAllowed(resources, user, userGroups, accessType);

			if(ret) {
				break;
			}
		}

		RangerPerfTracer.log(perf);

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== RangerPolicyEngineImpl.isAccessAllowed(" + resources + ", " + user + ", " + userGroups + ", " + accessType + "): " + ret);
		}

		return ret;
	}

	@Override
	public RangerPolicy getExactMatchPolicy(RangerAccessResource resource) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerPolicyEngineImpl.getExactMatchPolicy(" + resource + ")");
		}

		RangerPolicy ret = null;

		for(RangerPolicyEvaluator evaluator : policyRepository.getPolicyEvaluators()) {
			if(evaluator.isSingleAndExactMatch(resource)) {
				ret = evaluator.getPolicy();

				break;
			}
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerPolicyEngineImpl.getExactMatchPolicy(" + resource + "): " + ret);
		}

		return ret;
	}

	@Override
	public List<RangerPolicy> getAllowedPolicies(String user, Set<String> userGroups, String accessType) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerPolicyEngineImpl.getAllowedPolicies(" + user + ", " + userGroups + ", " + accessType + ")");
		}

		List<RangerPolicy> ret = new ArrayList<RangerPolicy>();

		for(RangerPolicyEvaluator evaluator : policyRepository.getPolicyEvaluators()) {
			RangerPolicy policy = evaluator.getPolicy();

			boolean isAccessAllowed = isAccessAllowed(policy.getResources(), user, userGroups, accessType);

			if(isAccessAllowed) {
				ret.add(policy);
			}
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerPolicyEngineImpl.getAllowedPolicies(" + user + ", " + userGroups + ", " + accessType + "): policyCount=" + ret.size());
		}

		return ret;
	}

	protected RangerAccessResult isAccessAllowedNoAudit(RangerAccessRequest request) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerPolicyEngineImpl.isAccessAllowedNoAudit(" + request + ")");
		}

		RangerAccessResult ret = createAccessResult(request);

		if(ret != null && request != null) {
			List<RangerPolicyEvaluator> evaluators = policyRepository.getPolicyEvaluators();

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

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== RangerPolicyEngineImpl.isAccessAllowedNoAudit(" + request + "): " + ret);
		}

		return ret;
	}

	private void setResourceServiceDef(RangerAccessRequest request) {
		RangerAccessResource resource = request.getResource();

		if (resource.getServiceDef() == null) {
			if (resource instanceof RangerMutableResource) {
				RangerMutableResource mutable = (RangerMutableResource) resource;
				mutable.setServiceDef(getServiceDef());
			} else {
				LOG.debug("RangerPolicyEngineImpl.setResourceServiceDef(): Cannot set ServiceDef in RangerResource.");
			}
		}
	}

	@Override
	public String toString( ) {
		StringBuilder sb = new StringBuilder();

		toString(sb);

		return sb.toString();
	}

	public StringBuilder toString(StringBuilder sb) {
		sb.append("RangerPolicyEngineImpl={");

		sb.append("serviceName={").append(this.getServiceName()).append("} ");
		sb.append(policyRepository);

		sb.append("}");

		return sb;
	}
}
