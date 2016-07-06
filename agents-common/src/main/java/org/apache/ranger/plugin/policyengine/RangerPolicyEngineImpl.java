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
import org.apache.ranger.plugin.model.RangerTag;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyResource;
import org.apache.ranger.plugin.policyevaluator.RangerPolicyEvaluator;
import org.apache.ranger.plugin.util.RangerAccessRequestUtil;
import org.apache.ranger.plugin.util.RangerPerfTracer;
import org.apache.ranger.plugin.util.ServicePolicies;

import java.util.*;

public class RangerPolicyEngineImpl implements RangerPolicyEngine {
	private static final Log LOG = LogFactory.getLog(RangerPolicyEngineImpl.class);

	private static final Log PERF_POLICYENGINE_INIT_LOG = RangerPerfTracer.getPerfLogger("policyengine.init");
	private static final Log PERF_POLICYENGINE_REQUEST_LOG = RangerPerfTracer.getPerfLogger("policyengine.request");
	private static final Log PERF_POLICYENGINE_AUDIT_LOG = RangerPerfTracer.getPerfLogger("policyengine.audit");
	private static final Log PERF_CONTEXTENRICHER_REQUEST_LOG = RangerPerfTracer.getPerfLogger("contextenricher.request");

	private static final int MAX_POLICIES_FOR_CACHE_TYPE_EVALUATOR = 100;

	private final RangerPolicyRepository policyRepository;
	private final RangerPolicyRepository tagPolicyRepository;
	
	private List<RangerContextEnricher> allContextEnrichers;

	private boolean  useForwardedIPAddress = false;
	private String[] trustedProxyAddresses = null;

	public RangerPolicyEngineImpl(String appId, ServicePolicies servicePolicies, RangerPolicyEngineOptions options) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> RangerPolicyEngineImpl(" + appId + ", " + servicePolicies + ", " + options + ")");
		}

		RangerPerfTracer perf = null;

		if(RangerPerfTracer.isPerfTraceEnabled(PERF_POLICYENGINE_INIT_LOG)) {
			perf = RangerPerfTracer.getPerfTracer(PERF_POLICYENGINE_INIT_LOG, "RangerPolicyEngine.init(appId=" + appId + ",hashCode=" + Integer.toHexString(System.identityHashCode(this)) + ")");
		}

		if (options == null) {
			options = new RangerPolicyEngineOptions();
		}

		if(StringUtils.isBlank(options.evaluatorType) || StringUtils.equalsIgnoreCase(options.evaluatorType, RangerPolicyEvaluator.EVALUATOR_TYPE_AUTO)) {

			String serviceType  = servicePolicies.getServiceDef().getName();
			String propertyName = "ranger.plugin." + serviceType + ".policyengine.evaluator.auto.maximum.policycount.for.cache.type";

			int thresholdForUsingOptimizedEvaluator = RangerConfiguration.getInstance().getInt(propertyName, MAX_POLICIES_FOR_CACHE_TYPE_EVALUATOR);

			int servicePoliciesCount = servicePolicies.getPolicies().size() + (servicePolicies.getTagPolicies() != null ? servicePolicies.getTagPolicies().getPolicies().size() : 0);

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

		policyRepository = new RangerPolicyRepository(appId, servicePolicies, options);

		ServicePolicies.TagPolicies tagPolicies = servicePolicies.getTagPolicies();

		if (!options.disableTagPolicyEvaluation
				&& tagPolicies != null
				&& !StringUtils.isEmpty(tagPolicies.getServiceName())
				&& tagPolicies.getServiceDef() != null
				&& !CollectionUtils.isEmpty(tagPolicies.getPolicies())) {

			if (LOG.isDebugEnabled()) {
				LOG.debug("RangerPolicyEngineImpl : Building tag-policy-repository for tag-service " + tagPolicies.getServiceName());
			}

			tagPolicyRepository = new RangerPolicyRepository(appId, tagPolicies, options, servicePolicies.getServiceDef(), servicePolicies.getServiceName());

		} else {
			if (LOG.isDebugEnabled()) {
				LOG.debug("RangerPolicyEngineImpl : No tag-policy-repository for service " + servicePolicies.getServiceName());
			}
			tagPolicyRepository = null;
		}

		List<RangerContextEnricher> tmpList;

		List<RangerContextEnricher> tagContextEnrichers = tagPolicyRepository == null ? null :tagPolicyRepository.getContextEnrichers();
		List<RangerContextEnricher> resourceContextEnrichers = policyRepository.getContextEnrichers();

		if (CollectionUtils.isEmpty(tagContextEnrichers)) {
			tmpList = resourceContextEnrichers;
		} else if (CollectionUtils.isEmpty(resourceContextEnrichers)) {
			tmpList = tagContextEnrichers;
		} else {
			tmpList = new ArrayList<RangerContextEnricher>(tagContextEnrichers);
			tmpList.addAll(resourceContextEnrichers);
		}

		this.allContextEnrichers = tmpList;

		RangerPerfTracer.log(perf);

		if (LOG.isDebugEnabled()) {
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
		if (request instanceof RangerAccessRequestImpl) {
			((RangerAccessRequestImpl) request).extractAndSetClientIPAddress(useForwardedIPAddress, trustedProxyAddresses);
		}

		List<RangerContextEnricher> enrichers = allContextEnrichers;

		if(!CollectionUtils.isEmpty(enrichers)) {

			for(RangerContextEnricher enricher : enrichers) {

				RangerPerfTracer perf = null;

				if(RangerPerfTracer.isPerfTraceEnabled(PERF_CONTEXTENRICHER_REQUEST_LOG)) {
					perf = RangerPerfTracer.getPerfTracer(PERF_CONTEXTENRICHER_REQUEST_LOG, "RangerContextEnricher.enrich(requestHashCode=" + Integer.toHexString(System.identityHashCode(request)) + ", enricherName=" + enricher.getName() + ")");
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
				preProcess(request);
			}
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerPolicyEngineImpl.preProcess(" + requests + ")");
		}
	}

	@Override
	public RangerAccessResult isAccessAllowed(RangerAccessRequest request, RangerAccessResultProcessor resultProcessor) {
		if (LOG.isDebugEnabled()) {
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

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== RangerPolicyEngineImpl.isAccessAllowed(" + request + "): " + ret);
		}

		return ret;
	}

	@Override
	public Collection<RangerAccessResult> isAccessAllowed(Collection<RangerAccessRequest> requests, RangerAccessResultProcessor resultProcessor) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> RangerPolicyEngineImpl.isAccessAllowed(" + requests + ")");
		}

		Collection<RangerAccessResult> ret = new ArrayList<RangerAccessResult>();

		if (requests != null) {
			for (RangerAccessRequest request : requests) {

				RangerAccessResult result = isAccessAllowedNoAudit(request);

				ret.add(result);
			}
		}

		if (resultProcessor != null) {
			resultProcessor.processResults(ret);
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== RangerPolicyEngineImpl.isAccessAllowed(" + requests + "): " + ret);
		}

		return ret;
	}

	@Override
	public RangerDataMaskResult evalDataMaskPolicies(RangerAccessRequest request, RangerAccessResultProcessor resultProcessor) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> RangerPolicyEngineImpl.evalDataMaskPolicies(" + request + ")");
		}

		RangerDataMaskResult ret = new RangerDataMaskResult(getServiceName(), getServiceDef(), request);

		if(request != null) {
			List<RangerPolicyEvaluator> evaluators = policyRepository.getDataMaskPolicyEvaluators();
			for (RangerPolicyEvaluator evaluator : evaluators) {
				evaluator.evaluate(request, ret);

				if (ret.getIsAccessDetermined() && ret.getIsAuditedDetermined()) {
					break;
				}
			}
		}

		// no need to audit if mask is not enabled
		if(! ret.isMaskEnabled()) {
			ret.setIsAudited(false);
		}

		if (resultProcessor != null) {
			resultProcessor.processResult(ret);
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== RangerPolicyEngineImpl.evalDataMaskPolicies(" + request + "): " + ret);
		}

		return ret;
	}

	@Override
	public RangerRowFilterResult evalRowFilterPolicies(RangerAccessRequest request, RangerAccessResultProcessor resultProcessor) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> RangerPolicyEngineImpl.evalRowFilterPolicies(" + request + ")");
		}

		RangerRowFilterResult ret = new RangerRowFilterResult(getServiceName(), getServiceDef(), request);

		if(request != null) {
			List<RangerPolicyEvaluator> evaluators = policyRepository.getRowFilterPolicyEvaluators();
			for (RangerPolicyEvaluator evaluator : evaluators) {
				evaluator.evaluate(request, ret);

				if (ret.getIsAccessDetermined() && ret.getIsAuditedDetermined()) {
					break;
				}
			}
		}

		// no need to audit if filter is not enabled
		if(! ret.isRowFilterEnabled()) {
			ret.setIsAudited(false);
		}

		if (resultProcessor != null) {
			resultProcessor.processResult(ret);
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== RangerPolicyEngineImpl.evalRowFilterPolicies(" + request + "): " + ret);
		}

		return ret;
	}

	@Override
	public boolean isAccessAllowed(RangerAccessResource resource, String user, Set<String> userGroups, String accessType) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> RangerPolicyEngineImpl.isAccessAllowed(" + resource + ", " + user + ", " + userGroups + ", " + accessType + ")");
		}

		RangerPerfTracer perf = null;

		if(RangerPerfTracer.isPerfTraceEnabled(PERF_POLICYENGINE_REQUEST_LOG)) {
			perf = RangerPerfTracer.getPerfTracer(PERF_POLICYENGINE_REQUEST_LOG, "RangerPolicyEngine.isAccessAllowed(user=" + user + ",accessType=" + accessType + "resource=" + resource.getAsString() + ")");
		}
		boolean ret = false;

		for (RangerPolicyEvaluator evaluator : policyRepository.getPolicyEvaluators()) {
			ret = evaluator.isAccessAllowed(resource, user, userGroups, accessType);

			if (ret) {
				break;
			}
		}

		RangerPerfTracer.log(perf);

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== RangerPolicyEngineImpl.isAccessAllowed(" + resource + ", " + user + ", " + userGroups + ", " + accessType + "): " + ret);
		}

		return ret;
	}


	@Override
	public boolean isAccessAllowed(Map<String, RangerPolicyResource> resources, String user, Set<String> userGroups, String accessType) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> RangerPolicyEngineImpl.isAccessAllowed(" + resources + ", " + user + ", " + userGroups + ", " + accessType + ")");
		}

		boolean ret = false;

		RangerPerfTracer perf = null;

		if(RangerPerfTracer.isPerfTraceEnabled(PERF_POLICYENGINE_REQUEST_LOG)) {
			perf = RangerPerfTracer.getPerfTracer(PERF_POLICYENGINE_REQUEST_LOG, "RangerPolicyEngine.isAccessAllowed(user=" + user + "," + userGroups + ",accessType=" + accessType + ")");
		}

		for (RangerPolicyEvaluator evaluator : policyRepository.getPolicyEvaluators()) {
			ret = evaluator.isAccessAllowed(resources, user, userGroups, accessType);

			if (ret) {
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
	public List<RangerPolicy> getExactMatchPolicies(RangerAccessResource resource) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> RangerPolicyEngineImpl.getExactMatchPolicies(" + resource + ")");
		}

		List<RangerPolicy> ret = null;

		for (RangerPolicyEvaluator evaluator : policyRepository.getPolicyEvaluators()) {
			if (evaluator.isCompleteMatch(resource)) {
				if(ret == null) {
					ret = new ArrayList<RangerPolicy>();
				}

				ret.add(evaluator.getPolicy());
			}
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== RangerPolicyEngineImpl.getExactMatchPolicies(" + resource + "): " + ret);
		}

		return ret;
	}

	@Override
	public List<RangerPolicy> getExactMatchPolicies(Map<String, RangerPolicyResource> resources) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> RangerPolicyEngineImpl.getExactMatchPolicies(" + resources + ")");
		}

		List<RangerPolicy> ret = null;

		for (RangerPolicyEvaluator evaluator : policyRepository.getPolicyEvaluators()) {
			if (evaluator.isCompleteMatch(resources)) {
				if(ret == null) {
					ret = new ArrayList<RangerPolicy>();
				}

				ret.add(evaluator.getPolicy());
			}
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== RangerPolicyEngineImpl.getExactMatchPolicies(" + resources + "): " + ret);
		}

		return ret;
	}

	@Override
	public List<RangerPolicy> getAllowedPolicies(String user, Set<String> userGroups, String accessType) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> RangerPolicyEngineImpl.getAllowedPolicies(" + user + ", " + userGroups + ", " + accessType + ")");
		}

		List<RangerPolicy> ret = new ArrayList<RangerPolicy>();

		// TODO: run through evaluator in tagPolicyRepository as well
		for (RangerPolicyEvaluator evaluator : policyRepository.getPolicyEvaluators()) {
			RangerPolicy policy = evaluator.getPolicy();

			boolean isAccessAllowed = isAccessAllowed(policy.getResources(), user, userGroups, accessType);

			if (isAccessAllowed) {
				ret.add(policy);
			}
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== RangerPolicyEngineImpl.getAllowedPolicies(" + user + ", " + userGroups + ", " + accessType + "): policyCount=" + ret.size());
		}

		return ret;
	}

	@Override
	public RangerResourceAccessInfo getResourceAccessInfo(RangerAccessRequest request) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerPolicyEngineImpl.getResourceAccessInfo(" + request + ")");
		}

		RangerResourceAccessInfo ret = new RangerResourceAccessInfo(request);

		List<RangerPolicyEvaluator> tagPolicyEvaluators = tagPolicyRepository == null ? null : tagPolicyRepository.getPolicyEvaluators();
		List<RangerPolicyEvaluator> resPolicyEvaluators = policyRepository.getPolicyEvaluators();

		if (CollectionUtils.isNotEmpty(tagPolicyEvaluators)) {
			List<RangerTag> tags = RangerAccessRequestUtil.getRequestTagsFromContext(request.getContext());

			if(CollectionUtils.isNotEmpty(tags)) {
				for (RangerTag tag : tags) {
					RangerAccessRequest tagEvalRequest = new RangerTagAccessRequest(tag, tagPolicyRepository.getServiceDef(), request);

					for (RangerPolicyEvaluator evaluator : tagPolicyEvaluators) {
						evaluator.getResourceAccessInfo(tagEvalRequest, ret);
					}
				}
			}
		}

		if(CollectionUtils.isNotEmpty(resPolicyEvaluators)) {
			for (RangerPolicyEvaluator evaluator : resPolicyEvaluators) {
				evaluator.getResourceAccessInfo(request, ret);
			}
		}

		ret.getAllowedUsers().removeAll(ret.getDeniedUsers());
		ret.getAllowedGroups().removeAll(ret.getDeniedGroups());

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerPolicyEngineImpl.getResourceAccessInfo(" + request + "): " + ret);
		}

		return ret;
	}

	protected RangerAccessResult isAccessAllowedNoAudit(RangerAccessRequest request) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> RangerPolicyEngineImpl.isAccessAllowedNoAudit(" + request + ")");
		}

		RangerAccessResult ret = createAccessResult(request);

		if (ret != null && request != null) {
			if (hasTagPolicies()) {
				isAccessAllowedForTagPolicies(request, ret);

				if (LOG.isDebugEnabled()) {
					if (ret.getIsAccessDetermined() && ret.getIsAuditedDetermined()) {
						LOG.debug("RangerPolicyEngineImpl.isAccessAllowedNoAudit() - access and audit determined by tag policy. No resource policies will be evaluated, request=" + request + ", result=" + ret);
					}
				}
			}

			boolean isAllowedByTags          = ret.getIsAccessDetermined() && ret.getIsAllowed();
			boolean isDeniedByTags           = ret.getIsAccessDetermined() && !ret.getIsAllowed();
			boolean evaluateResourcePolicies = hasResourcePolicies() && (!isDeniedByTags || !ret.getIsAuditedDetermined());

			if (evaluateResourcePolicies) {
				boolean findAuditByResource = !ret.getIsAuditedDetermined();
				boolean foundInCache        = findAuditByResource ? policyRepository.setAuditEnabledFromCache(request, ret) : false;

				if(isAllowedByTags) {
					ret.setIsAccessDetermined(false); // discard allowed result by tag-policies, to evaluate resource policies for possible deny
				}

				List<RangerPolicyEvaluator> evaluators = policyRepository.getPolicyEvaluators();
				for (RangerPolicyEvaluator evaluator : evaluators) {
					evaluator.evaluate(request, ret);

					if(ret.getIsAllowed() && !evaluator.hasDeny()) { // all policies having deny have been evaluated
						ret.setIsAccessDetermined(true);
					}

					if(ret.getIsAuditedDetermined() && ret.getIsAccessDetermined()) {
						break;			// Break out of policy-evaluation loop
					}
				}

				if(ret.getIsAllowed()) {
					ret.setIsAccessDetermined(true);
				}

				if (findAuditByResource && !foundInCache) {
					policyRepository.storeAuditEnabledInCache(request, ret);
				}
			}
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== RangerPolicyEngineImpl.isAccessAllowedNoAudit(" + request + "): " + ret);
		}

		return ret;
	}

	protected void isAccessAllowedForTagPolicies(final RangerAccessRequest request, RangerAccessResult result) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> RangerPolicyEngineImpl.isAccessAllowedForTagPolicies(" + request + ", " + result + ")");
		}

		List<RangerPolicyEvaluator> evaluators = tagPolicyRepository == null ? null : tagPolicyRepository.getPolicyEvaluators();

		if (CollectionUtils.isNotEmpty(evaluators)) {
			List<RangerTag> tags = RangerAccessRequestUtil.getRequestTagsFromContext(request.getContext());

			if(CollectionUtils.isNotEmpty(tags)) {
				for (RangerTag tag : tags) {
					if (LOG.isDebugEnabled()) {
						LOG.debug("RangerPolicyEngineImpl.isAccessAllowedForTagPolicies: Evaluating policies for tag (" + tag.getType() + ")");
					}

					RangerAccessRequest tagEvalRequest = new RangerTagAccessRequest(tag, tagPolicyRepository.getServiceDef(), request);
					RangerAccessResult  tagEvalResult  = createAccessResult(tagEvalRequest);

					// carry fwd results from earlier tags, to optimize the current evaluation
					//  - if access was already allowed by a tag, only deny needs to be looked into
					//  - if audit was already determined, evaluation can bail out as soon as access is determined
					if(result.getIsAllowed()) {
						tagEvalResult.setIsAllowed(result.getIsAllowed());
					}
					tagEvalResult.setAuditResultFrom(result);

					for (RangerPolicyEvaluator evaluator : evaluators) {
						if(! evaluator.isMatch(tagEvalRequest.getResource())) 
							continue;

						evaluator.evaluate(tagEvalRequest, tagEvalResult);

						if(tagEvalResult.getIsAllowed() && !evaluator.hasDeny()) { // all policies having deny have been evaluated
							tagEvalResult.setIsAccessDetermined(true);
						}

						if(tagEvalResult.getIsAuditedDetermined() && tagEvalResult.getIsAccessDetermined()) {
							if (LOG.isDebugEnabled()) {
								LOG.debug("RangerPolicyEngineImpl.isAccessAllowedForTagPolicies: concluding eval of tag (" + tag.getType() + ") with authorization=" + tagEvalResult.getIsAllowed());
							}

							break;			// Break out of policy-evaluation loop for this tag
						}
					}

					if(tagEvalResult.getIsAllowed()) {
						tagEvalResult.setIsAccessDetermined(true);
					}

					if (tagEvalResult.getIsAudited()) {
						result.setIsAudited(true);
					}

					if(!result.getIsAccessDetermined() && tagEvalResult.getIsAccessDetermined()) {
						if(! tagEvalResult.getIsAllowed()) { // access is denied for this tag
							result.setAccessResultFrom(tagEvalResult);
						} else { // access is allowed for this tag
							// if a policy evaluated earlier allowed the access, don't update with current tag result
							if(! result.getIsAllowed()) {
								result.setAccessResultFrom(tagEvalResult);
								result.setIsAccessDetermined(false); // so that evaluation will continue for deny
							}
						}
					}

					if(result.getIsAuditedDetermined() && result.getIsAccessDetermined()) {
						break;			// Break out of policy-evaluation loop
					}
				}
			}
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== RangerPolicyEngineImpl.isAccessAllowedForTagPolicies(" + request + ", " + result + ")" );
		}
	}

	private void setResourceServiceDef(RangerAccessRequest request) {
		RangerAccessResource resource = request.getResource();

		if (resource.getServiceDef() == null) {
			if (resource instanceof RangerMutableResource) {
				RangerMutableResource mutable = (RangerMutableResource) resource;
				mutable.setServiceDef(getServiceDef());
			} else {
				LOG.debug("RangerPolicyEngineImpl.setResourceServiceDef(): Cannot set ServiceDef in RangerTagResourceMap.");
			}
		}
	}

	private boolean hasTagPolicies() {
		return tagPolicyRepository != null && CollectionUtils.isNotEmpty(tagPolicyRepository.getPolicies());
	}

	private boolean hasResourcePolicies() {
		return policyRepository != null && CollectionUtils.isNotEmpty(policyRepository.getPolicies());
	}

	@Override
	public boolean preCleanup() {

		boolean ret = true;
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> RangerPolicyEngineImpl.preCleanup()");
		}

		if (CollectionUtils.isNotEmpty(allContextEnrichers)) {
			for (RangerContextEnricher contextEnricher : allContextEnrichers) {
				boolean notReadyForCleanup = contextEnricher.preCleanup();
				if (!notReadyForCleanup) {
					if (LOG.isDebugEnabled()) {
						LOG.debug("contextEnricher.preCleanup() failed for contextEnricher=" + contextEnricher.getName());
					}
					ret = false;
				}
			}
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== RangerPolicyEngineImpl.preCleanup() : result=" + ret);
		}

		return ret;
	}

	@Override
	public void cleanup() {

		if (LOG.isDebugEnabled()) {
			LOG.debug("==> RangerPolicyEngineImpl.cleanup()");
		}

		RangerPerfTracer perf = null;

		if(RangerPerfTracer.isPerfTraceEnabled(PERF_POLICYENGINE_INIT_LOG)) {
			perf = RangerPerfTracer.getPerfTracer(PERF_POLICYENGINE_INIT_LOG, "RangerPolicyEngine.cleanUp(hashCode=" + Integer.toHexString(System.identityHashCode(this)) + ")");
		}
		preCleanup();

		if (CollectionUtils.isNotEmpty(allContextEnrichers)) {
			for (RangerContextEnricher contextEnricher : allContextEnrichers) {
				contextEnricher.cleanup();
			}
		}

		this.allContextEnrichers = null;

		RangerPerfTracer.log(perf);

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== RangerPolicyEngineImpl.cleanup()");
		}
	}

	@Override
	protected void finalize() throws Throwable {
		try {
			cleanup();
		}
		finally {
			super.finalize();
		}
	}

	@Override
	public void setUseForwardedIPAddress(boolean useForwardedIPAddress) {
		this.useForwardedIPAddress = useForwardedIPAddress;
	}

	@Override
	public void setTrustedProxyAddresses(String[] trustedProxyAddresses) {
		this.trustedProxyAddresses = trustedProxyAddresses;
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
class RangerTagResource extends RangerAccessResourceImpl {
	private static final String KEY_TAG = "tag";


	public RangerTagResource(String tagType, RangerServiceDef tagServiceDef) {
		super.setValue(KEY_TAG, tagType);
		super.setServiceDef(tagServiceDef);
	}
}

class RangerTagAccessRequest extends RangerAccessRequestImpl {
	public RangerTagAccessRequest(RangerTag resourceTag, RangerServiceDef tagServiceDef, RangerAccessRequest request) {
		super.setResource(new RangerTagResource(resourceTag.getType(), tagServiceDef));
		super.setUser(request.getUser());
		super.setUserGroups(request.getUserGroups());
		super.setAction(request.getAction());
		super.setAccessType(request.getAccessType());
		super.setAccessTime(request.getAccessTime());
		super.setRequestData(request.getRequestData());

		Map<String, Object> requestContext = request.getContext();

		RangerAccessRequestUtil.setCurrentTagInContext(request.getContext(), resourceTag);
		RangerAccessRequestUtil.setCurrentResourceInContext(request.getContext(), request.getResource());

		super.setContext(requestContext);

		super.setClientType(request.getClientType());
		super.setClientIPAddress(request.getClientIPAddress());
		super.setRemoteIPAddress(request.getRemoteIPAddress());
		super.setForwardedAddresses(request.getForwardedAddresses());
		super.setSessionId(request.getSessionId());
	}
}


class RangerTagAuditEvent {
	private final String tagType;
	private final RangerAccessResult result;

	RangerTagAuditEvent(String tagType, RangerAccessResult result) {
		this.tagType = tagType;
		this.result = result;
	}
	@Override
	public String toString( ) {
		StringBuilder sb = new StringBuilder();

		toString(sb);

		return sb.toString();
	}

	public void toString(StringBuilder sb) {
		sb.append("RangerTagAuditEvent={");

		sb.append("tagType={").append(this.tagType).append("} ");
		sb.append("isAccessDetermined={").append(this.result.getIsAccessDetermined()).append("}");
		sb.append("isAllowed={").append(this.result.getIsAllowed()).append("}");
		sb.append("policyId={").append(this.result.getPolicyId()).append("}");
		sb.append("reason={").append(this.result.getReason()).append("}");

		sb.append("}");

	}

	static void processTagEvents(List<RangerTagAuditEvent> tagAuditEvents, final boolean deniedAccess) {
		// Process tagAuditEvents to delete unwanted events

		if (CollectionUtils.isEmpty(tagAuditEvents)) return;

		List<RangerTagAuditEvent> unwantedEvents = new ArrayList<RangerTagAuditEvent> ();
		if (deniedAccess) {
			for (RangerTagAuditEvent auditEvent : tagAuditEvents) {
				RangerAccessResult result = auditEvent.result;
				if (result.getIsAllowed()) {
					unwantedEvents.add(auditEvent);
				}
			}
			tagAuditEvents.removeAll(unwantedEvents);
		}
	}
}
