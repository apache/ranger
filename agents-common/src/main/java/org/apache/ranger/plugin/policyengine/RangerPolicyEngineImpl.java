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
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ranger.authorization.hadoop.config.RangerConfiguration;
import org.apache.ranger.plugin.contextenricher.RangerContextEnricher;
import org.apache.ranger.plugin.contextenricher.RangerTagForEval;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyResource;
import org.apache.ranger.plugin.policyevaluator.RangerPolicyEvaluator;
import org.apache.ranger.plugin.policyresourcematcher.RangerPolicyResourceMatcher;
import org.apache.ranger.plugin.util.RangerAccessRequestUtil;
import org.apache.ranger.plugin.util.RangerPerfTracer;
import org.apache.ranger.plugin.util.ServicePolicies;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class RangerPolicyEngineImpl implements RangerPolicyEngine {
	private static final Log LOG = LogFactory.getLog(RangerPolicyEngineImpl.class);

	private static final Log PERF_POLICYENGINE_INIT_LOG = RangerPerfTracer.getPerfLogger("policyengine.init");
	private static final Log PERF_POLICYENGINE_REQUEST_LOG = RangerPerfTracer.getPerfLogger("policyengine.request");
	private static final Log PERF_POLICYENGINE_AUDIT_LOG = RangerPerfTracer.getPerfLogger("policyengine.audit");
	private static final Log PERF_CONTEXTENRICHER_REQUEST_LOG = RangerPerfTracer.getPerfLogger("contextenricher.request");
	private static final Log PERF_POLICYENGINE_REBALANCE_LOG = RangerPerfTracer.getPerfLogger("policyengine.rebalance");
	private static final Log PERF_POLICYENGINE_USAGE_LOG = RangerPerfTracer.getPerfLogger("policyengine.usage");

	private static final int MAX_POLICIES_FOR_CACHE_TYPE_EVALUATOR = 100;

	private final RangerPolicyRepository policyRepository;
	private final RangerPolicyRepository tagPolicyRepository;
	
	private List<RangerContextEnricher> allContextEnrichers;

	private final Map<Long, RangerPolicyEvaluator> policyEvaluatorsMap;

	private boolean  useForwardedIPAddress;
	private String[] trustedProxyAddresses;

	public RangerPolicyEngineImpl(String appId, ServicePolicies servicePolicies, RangerPolicyEngineOptions options) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> RangerPolicyEngineImpl(" + appId + ", " + servicePolicies + ", " + options + ")");
		}

		RangerPerfTracer perf = null;

		if(RangerPerfTracer.isPerfTraceEnabled(PERF_POLICYENGINE_INIT_LOG)) {
			perf = RangerPerfTracer.getPerfTracer(PERF_POLICYENGINE_INIT_LOG, "RangerPolicyEngine.init(appId=" + appId + ",hashCode=" + Integer.toHexString(System.identityHashCode(this)) + ")");
			long freeMemory = Runtime.getRuntime().freeMemory();
			long totalMemory = Runtime.getRuntime().totalMemory();
			PERF_POLICYENGINE_INIT_LOG.debug("In-Use memory: " + (totalMemory - freeMemory) + ", Free memory:" + freeMemory);
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
			tmpList = new ArrayList<>(tagContextEnrichers);
			tmpList.addAll(resourceContextEnrichers);
		}

		this.allContextEnrichers = tmpList;

		policyEvaluatorsMap = createPolicyEvaluatorsMap();

		RangerPerfTracer.log(perf);

		if (PERF_POLICYENGINE_INIT_LOG.isDebugEnabled()) {
			long freeMemory = Runtime.getRuntime().freeMemory();
			long totalMemory = Runtime.getRuntime().totalMemory();
			PERF_POLICYENGINE_INIT_LOG.debug("In-Use memory: " + (totalMemory - freeMemory) + ", Free memory:" + freeMemory);
		}

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

	public RangerPolicyEvaluator getPolicyEvaluator(Long id) {
		return policyEvaluatorsMap.get(id);
	}

	public RangerPolicy getPolicy(Long id) {
		RangerPolicyEvaluator evaluator = getPolicyEvaluator(id);
		return evaluator != null ? evaluator.getPolicy() : null;
	}

	@Override
	public RangerAccessResult createAccessResult(RangerAccessRequest request) {
		RangerAccessResult ret = new RangerAccessResult(this.getServiceName(), policyRepository.getServiceDef(), request);
		switch (policyRepository.getAuditModeEnum()) {
			case AUDIT_ALL:
				ret.setIsAudited(true);
				break;
			case AUDIT_NONE:
				ret.setIsAudited(false);
				break;
			default:
				if (CollectionUtils.isEmpty(policyRepository.getPolicies()) && tagPolicyRepository == null) {
					ret.setIsAudited(true);
				}
				break;
		}
		return ret;
	}

	@Override
	public RangerDataMaskResult createDataMaskResult(RangerAccessRequest request) {
		RangerDataMaskResult ret = new RangerDataMaskResult(this.getServiceName(), policyRepository.getServiceDef(), request);
		switch (policyRepository.getAuditModeEnum()) {
			case AUDIT_ALL:
				ret.setIsAudited(true);
				break;
			case AUDIT_NONE:
				ret.setIsAudited(false);
				break;
			default:
				break;
		}
		return ret;
	}

	@Override
	public RangerRowFilterResult createRowFilterResult(RangerAccessRequest request) {
		RangerRowFilterResult ret = new RangerRowFilterResult(this.getServiceName(), policyRepository.getServiceDef(), request);
		switch (policyRepository.getAuditModeEnum()) {
			case AUDIT_ALL:
				ret.setIsAudited(true);
				break;
			case AUDIT_NONE:
				ret.setIsAudited(false);
				break;
			default:
				break;
		}
		return ret;
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

		RangerAccessRequestUtil.setCurrentUserInContext(request.getContext(), request.getUser());

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
			String requestHashCode = Integer.toHexString(System.identityHashCode(request));
			perf = RangerPerfTracer.getPerfTracer(PERF_POLICYENGINE_REQUEST_LOG, "RangerPolicyEngine.isAccessAllowed(requestHashCode=" + requestHashCode + ")");
			LOG.info("RangerPolicyEngineImpl.isAccessAllowed(" + requestHashCode + ", " + request + ")");
		}

		RangerAccessResult ret = isAccessAllowedNoAudit(request);

		updatePolicyUsageCounts(request, ret);

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

		Collection<RangerAccessResult> ret = new ArrayList<>();

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

		RangerDataMaskResult ret = evalDataMaskPoliciesNoAudit(request);

		// no need to audit if mask is not enabled
		if(! ret.isMaskEnabled()) {
			ret.setIsAudited(false);
		}

		updatePolicyUsageCounts(request, ret);

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

		RangerRowFilterResult ret = evalRowFilterPoliciesNoAudit(request);

		// no need to audit if filter is not enabled
		if(! ret.isRowFilterEnabled()) {
			ret.setIsAudited(false);
		}

		updatePolicyUsageCounts(request, ret);

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

		for (RangerPolicyEvaluator evaluator : policyRepository.getLikelyMatchPolicyEvaluators(resource)) {
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
	public List<RangerPolicy> getExactMatchPolicies(RangerAccessResource resource, Map<String, Object> evalContext) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> RangerPolicyEngineImpl.getExactMatchPolicies(" + resource + ", " + evalContext + ")");
		}

		List<RangerPolicy> ret = null;

		for (RangerPolicyEvaluator evaluator : policyRepository.getPolicyEvaluators()) {
			if (evaluator.isCompleteMatch(resource, evalContext)) {
				if(ret == null) {
					ret = new ArrayList<>();
				}

				ret.add(evaluator.getPolicy());
			}
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== RangerPolicyEngineImpl.getExactMatchPolicies(" + resource + ", " + evalContext + "): " + ret);
		}

		return ret;
	}

	@Override
	public List<RangerPolicy> getExactMatchPolicies(Map<String, RangerPolicyResource> resources, Map<String, Object> evalContext) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> RangerPolicyEngineImpl.getExactMatchPolicies(" + resources + ", " + evalContext + ")");
		}

		List<RangerPolicy> ret = null;

		for (RangerPolicyEvaluator evaluator : policyRepository.getPolicyEvaluators()) {
			if (evaluator.isCompleteMatch(resources, evalContext)) {
				if(ret == null) {
					ret = new ArrayList<>();
				}

				ret.add(evaluator.getPolicy());
			}
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== RangerPolicyEngineImpl.getExactMatchPolicies(" + resources + ", " + evalContext + "): " + ret);
		}

		return ret;
	}

	@Override
	public List<RangerPolicy> getAllowedPolicies(String user, Set<String> userGroups, String accessType) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> RangerPolicyEngineImpl.getAllowedPolicies(" + user + ", " + userGroups + ", " + accessType + ")");
		}

		List<RangerPolicy> ret = new ArrayList<>();

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
	public List<RangerPolicy> getMatchingPolicies(RangerAccessResource resource) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> RangerPolicyEngineImpl.getMatchingPolicies(" + resource + ")");
		}

		List<RangerPolicy> ret = new ArrayList<>();

		RangerAccessRequestImpl request = new RangerAccessRequestImpl(resource, RangerPolicyEngine.ANY_ACCESS, null, null);

		preProcess(request);

		if (hasTagPolicies()) {
			Set<RangerTagForEval> tags = RangerAccessRequestUtil.getRequestTagsFromContext(request.getContext());

			if (CollectionUtils.isNotEmpty(tags)) {
				for (RangerTagForEval tag : tags) {
					RangerAccessRequest         tagEvalRequest            = new RangerTagAccessRequest(tag, tagPolicyRepository.getServiceDef(), request);
					RangerAccessResource        tagResource               = tagEvalRequest.getResource();
					List<RangerPolicyEvaluator> accessPolicyEvaluators    = tagPolicyRepository.getLikelyMatchPolicyEvaluators(tagResource);
					List<RangerPolicyEvaluator> dataMaskPolicyEvaluators  = tagPolicyRepository.getLikelyMatchDataMaskPolicyEvaluators(tagResource);
					List<RangerPolicyEvaluator> rowFilterPolicyEvaluators = tagPolicyRepository.getLikelyMatchRowFilterPolicyEvaluators(tagResource);

					List<RangerPolicyEvaluator>[] likelyEvaluators = new List[] { accessPolicyEvaluators, dataMaskPolicyEvaluators, rowFilterPolicyEvaluators };

					for (List<RangerPolicyEvaluator> evaluators : likelyEvaluators) {
						for (RangerPolicyEvaluator evaluator : evaluators) {
							RangerPolicyResourceMatcher matcher = evaluator.getPolicyResourceMatcher();
							if (matcher != null && matcher.isMatch(tagResource, RangerPolicyResourceMatcher.MatchScope.ANY, null)) {
								ret.add(evaluator.getPolicy());
							}
						}
					}
				}
			}
		}

		if (hasResourcePolicies()) {
			List<RangerPolicyEvaluator> accessPolicyEvaluators    = policyRepository.getLikelyMatchPolicyEvaluators(resource);
			List<RangerPolicyEvaluator> dataMaskPolicyEvaluators  = policyRepository.getLikelyMatchDataMaskPolicyEvaluators(resource);
			List<RangerPolicyEvaluator> rowFilterPolicyEvaluators = policyRepository.getLikelyMatchRowFilterPolicyEvaluators(resource);

			List<RangerPolicyEvaluator>[] likelyEvaluators = new List[] { accessPolicyEvaluators, dataMaskPolicyEvaluators, rowFilterPolicyEvaluators };

			for (List<RangerPolicyEvaluator> evaluators : likelyEvaluators) {
				for (RangerPolicyEvaluator evaluator : evaluators) {
					RangerPolicyResourceMatcher matcher = evaluator.getPolicyResourceMatcher();
					if (matcher != null && matcher.isMatch(resource, RangerPolicyResourceMatcher.MatchScope.ANY, null)) {
						ret.add(evaluator.getPolicy());
					}
				}
			}
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== RangerPolicyEngineImpl.getMatchingPolicies(" + resource + ") : " + ret.size());
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

		if (CollectionUtils.isNotEmpty(tagPolicyEvaluators)) {
			Set<RangerTagForEval> tags = RangerAccessRequestUtil.getRequestTagsFromContext(request.getContext());
			if(CollectionUtils.isNotEmpty(tags)) {
				for (RangerTagForEval tag : tags) {
					RangerAccessRequest tagEvalRequest = new RangerTagAccessRequest(tag, tagPolicyRepository.getServiceDef(), request);

					List<RangerPolicyEvaluator> evaluators = tagPolicyRepository.getLikelyMatchPolicyEvaluators(tagEvalRequest.getResource());

					for (RangerPolicyEvaluator evaluator : evaluators) {
						evaluator.getResourceAccessInfo(tagEvalRequest, ret);
					}
				}
			}
		}

		List<RangerPolicyEvaluator> resPolicyEvaluators = policyRepository.getLikelyMatchPolicyEvaluators(request.getResource());

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
				boolean foundInCache        = findAuditByResource && policyRepository.setAuditEnabledFromCache(request, ret);

				if(isAllowedByTags) {
					ret.setIsAccessDetermined(false); // discard allowed result by tag-policies, to evaluate resource policies for possible deny
				}

				List<RangerPolicyEvaluator> evaluators = policyRepository.getLikelyMatchPolicyEvaluators(request.getResource());
				for (RangerPolicyEvaluator evaluator : evaluators) {
					ret.incrementEvaluatedPoliciesCount();
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

		List<RangerPolicyEvaluator> tagEvaluators = tagPolicyRepository == null ? null : tagPolicyRepository.getPolicyEvaluators();

		if (CollectionUtils.isNotEmpty(tagEvaluators)) {
			Set<RangerTagForEval> tags = RangerAccessRequestUtil.getRequestTagsFromContext(request.getContext());

			if (CollectionUtils.isNotEmpty(tags)) {
				for (RangerTagForEval tag : tags) {
					if (LOG.isDebugEnabled()) {
						LOG.debug("RangerPolicyEngineImpl.isAccessAllowedForTagPolicies: Evaluating policies for tag (" + tag.getType() + ")");
					}

					RangerAccessRequest tagEvalRequest = new RangerTagAccessRequest(tag, tagPolicyRepository.getServiceDef(), request);
					RangerAccessResult tagEvalResult = createAccessResult(tagEvalRequest);

					// carry fwd results from earlier tags, to optimize the current evaluation
					//  - if access was already allowed by a tag, only deny needs to be looked into
					//  - if audit was already determined, evaluation can bail out as soon as access is determined
					if (result.getIsAllowed()) {
						tagEvalResult.setIsAllowed(result.getIsAllowed());
					}
					tagEvalResult.setAuditResultFrom(result);

					List<RangerPolicyEvaluator> evaluators = tagPolicyRepository.getLikelyMatchPolicyEvaluators(tagEvalRequest.getResource());

					for (RangerPolicyEvaluator evaluator : evaluators) {
						result.incrementEvaluatedPoliciesCount();

						evaluator.evaluate(tagEvalRequest, tagEvalResult);

						if (tagEvalResult.getIsAllowed() && !evaluator.hasDeny()) { // all policies having deny have been evaluated
							tagEvalResult.setIsAccessDetermined(true);
						}

						if (tagEvalResult.getIsAuditedDetermined() && tagEvalResult.getIsAccessDetermined()) {
							if (LOG.isDebugEnabled()) {
								LOG.debug("RangerPolicyEngineImpl.isAccessAllowedForTagPolicies: concluding eval of tag (" + tag.getType() + ") with authorization=" + tagEvalResult.getIsAllowed());
							}

							break;            // Break out of policy-evaluation loop for this tag
						}
					}

					if (tagEvalResult.getIsAllowed()) {
						tagEvalResult.setIsAccessDetermined(true);
					}

					if (tagEvalResult.getIsAudited()) {
						result.setAuditResultFrom(tagEvalResult);
					}

					if (!result.getIsAccessDetermined() && tagEvalResult.getIsAccessDetermined()) {
						if (!tagEvalResult.getIsAllowed()) { // access is denied for this tag
							result.setAccessResultFrom(tagEvalResult);
						} else { // access is allowed for this tag
							// if a policy evaluated earlier allowed the access, don't update with current tag result
							if (!result.getIsAllowed()) {
								result.setAccessResultFrom(tagEvalResult);
								result.setIsAccessDetermined(false); // so that evaluation will continue for deny
							}
						}
					}

					if (result.getIsAuditedDetermined() && result.getIsAccessDetermined()) {
						break;            // Break out of policy-evaluation loop
					}
				}
				if (result.getIsAllowed()) {
					result.setIsAccessDetermined(true);
				}
			}
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== RangerPolicyEngineImpl.isAccessAllowedForTagPolicies(" + request + ", " + result + ")");
		}
	}

	RangerDataMaskResult evalDataMaskPoliciesNoAudit(RangerAccessRequest request) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> RangerPolicyEngineImpl.evalDataMaskPoliciesNoAudit(" + request + ")");
		}

		RangerDataMaskResult ret = createDataMaskResult(request);

		if (ret != null && request != null) {
			if (hasTagPolicies()) {
				evalDataMaskPoliciesForTagPolicies(request, ret);

				if (LOG.isDebugEnabled()) {
					if (ret.getIsAccessDetermined() && ret.getIsAuditedDetermined()) {
						LOG.debug("RangerPolicyEngineImpl.evalDataMaskPoliciesNoAudit() - access and audit determined by tag policy. No resource policies will be evaluated, request=" + request + ", result=" + ret);
					}
				}
			}
			boolean isEvaluatedByTags        = ret.getIsAccessDetermined() && ret.getIsAllowed();
			boolean evaluateResourcePolicies = hasResourcePolicies() && (!isEvaluatedByTags || !ret.getIsAuditedDetermined());

			if (evaluateResourcePolicies) {
				boolean                     findAuditByResource = !ret.getIsAuditedDetermined();
				boolean                     foundInCache        = findAuditByResource && policyRepository.setAuditEnabledFromCache(request, ret);
				List<RangerPolicyEvaluator> evaluators          = policyRepository.getLikelyMatchDataMaskPolicyEvaluators(request.getResource());

				for (RangerPolicyEvaluator evaluator : evaluators) {
					ret.incrementEvaluatedPoliciesCount();
					evaluator.evaluate(request, ret);

					if(ret.getIsAccessDetermined()) {
						if (StringUtils.equalsIgnoreCase(ret.getMaskType(), RangerPolicy.MASK_TYPE_NONE)) {
							ret.setMaskType(null);
						}

						if (ret.getIsAuditedDetermined()) {
							break;
						}
					}
				}

				if (findAuditByResource && !foundInCache) {
					policyRepository.storeAuditEnabledInCache(request, ret);
				}
			}
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== RangerPolicyEngineImpl.evalDataMaskPoliciesNoAudit(" + request + "): " + ret);
		}
		return ret;
	}

	protected void evalDataMaskPoliciesForTagPolicies(final RangerAccessRequest request, RangerDataMaskResult result) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> RangerPolicyEngineImpl.evalDataMaskPoliciesForTagPolicies(" + request + ", " + result + ")");
		}

		List<RangerPolicyEvaluator> tagEvaluators = tagPolicyRepository == null ? null : tagPolicyRepository.getDataMaskPolicyEvaluators();

		if (CollectionUtils.isNotEmpty(tagEvaluators)) {
			Set<RangerTagForEval>       tags               = RangerAccessRequestUtil.getRequestTagsFromContext(request.getContext());
			List<PolicyEvaluatorForTag> dataMaskEvaluators = tagPolicyRepository.getLikelyMatchDataMaskPolicyEvaluators(tags);

			if (CollectionUtils.isNotEmpty(dataMaskEvaluators)) {
				for (PolicyEvaluatorForTag dataMaskEvaluator : dataMaskEvaluators) {
					RangerPolicyEvaluator evaluator      = dataMaskEvaluator.getEvaluator();
					RangerTagForEval      tag            = dataMaskEvaluator.getTag();
					RangerAccessRequest   tagEvalRequest = new RangerTagAccessRequest(tag, tagPolicyRepository.getServiceDef(), request);
					RangerDataMaskResult  tagEvalResult  = createDataMaskResult(tagEvalRequest);

					tagEvalResult.setAuditResultFrom(result);
					tagEvalResult.setAccessResultFrom(result);

					tagEvalResult.setMaskType(result.getMaskType());
					tagEvalResult.setMaskCondition(result.getMaskCondition());
					tagEvalResult.setMaskedValue(result.getMaskedValue());

					result.incrementEvaluatedPoliciesCount();

					evaluator.evaluate(tagEvalRequest, tagEvalResult);

					if (tagEvalResult.getIsAudited()) {
						result.setAuditResultFrom(tagEvalResult);
					}

					if (tagEvalResult.getIsAccessDetermined()) {
						result.setAccessResultFrom(tagEvalResult);
						result.setMaskType(tagEvalResult.getMaskType());
						result.setMaskCondition(tagEvalResult.getMaskCondition());
						result.setMaskedValue(tagEvalResult.getMaskedValue());

						if (StringUtils.equalsIgnoreCase(result.getMaskType(), RangerPolicy.MASK_TYPE_NONE)) {
							result.setMaskType(null);
						}
					}

					if (result.getIsAuditedDetermined() && result.getIsAccessDetermined()) {
						if (LOG.isDebugEnabled()) {
							LOG.debug("RangerPolicyEngineImpl.evalDataMaskPoliciesForTagPolicies: concluding eval of dataMask policies for tags: tag=" + tag.getType() + " with authorization=" + tagEvalResult.getIsAllowed());
						}
						break;            // Break out of datamask policy-evaluation loop
					}
				}
			}
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== RangerPolicyEngineImpl.evalDataMaskPoliciesForTagPolicies(" + request + ", " + result + ")");
		}
	}

	RangerRowFilterResult evalRowFilterPoliciesNoAudit(RangerAccessRequest request) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> RangerPolicyEngineImpl.evalRowFilterPoliciesNoAudit(" + request + ")");
		}

		RangerRowFilterResult ret = createRowFilterResult(request);

		if (ret != null && request != null) {
			if (hasTagPolicies()) {
				evalRowFilterPoliciesForTagPolicies(request, ret);

				if (LOG.isDebugEnabled()) {
					if (ret.getIsAccessDetermined() && ret.getIsAuditedDetermined()) {
						LOG.debug("RangerPolicyEngineImpl.evalRowFilterPoliciesNoAudit() - access and audit determined by tag policy. No resource policies will be evaluated, request=" + request + ", result=" + ret);
					}
				}
			}
			boolean isEvaluatedByTags        = ret.getIsAccessDetermined() && ret.getIsAllowed();
			boolean evaluateResourcePolicies = hasResourcePolicies() && (!isEvaluatedByTags || !ret.getIsAuditedDetermined());

			if (evaluateResourcePolicies) {
				boolean                     findAuditByResource = !ret.getIsAuditedDetermined();
				boolean                     foundInCache        = findAuditByResource && policyRepository.setAuditEnabledFromCache(request, ret);
				List<RangerPolicyEvaluator> evaluators          = policyRepository.getLikelyMatchRowFilterPolicyEvaluators(request.getResource());

				for (RangerPolicyEvaluator evaluator : evaluators) {
					ret.incrementEvaluatedPoliciesCount();
					evaluator.evaluate(request, ret);

					if(ret.getIsAuditedDetermined() && ret.getIsAccessDetermined()) {
						break;
					}
				}

				if (findAuditByResource && !foundInCache) {
					policyRepository.storeAuditEnabledInCache(request, ret);
				}
			}
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== RangerPolicyEngineImpl.evalRowFilterPoliciesNoAudit(" + request + "): " + ret);
		}
		return ret;
	}

	protected void evalRowFilterPoliciesForTagPolicies(final RangerAccessRequest request, RangerRowFilterResult result) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> RangerPolicyEngineImpl.evalRowFilterPoliciesForTagPolicies(" + request + ", " + result + ")");
		}

		List<RangerPolicyEvaluator> tagEvaluators = tagPolicyRepository == null ? null : tagPolicyRepository.getRowFilterPolicyEvaluators();

		if (CollectionUtils.isNotEmpty(tagEvaluators)) {
			Set<RangerTagForEval>       tags                = RangerAccessRequestUtil.getRequestTagsFromContext(request.getContext());
			List<PolicyEvaluatorForTag> rowFilterEvaluators = tagPolicyRepository.getLikelyMatchRowFilterPolicyEvaluators(tags);

			if (CollectionUtils.isNotEmpty(rowFilterEvaluators)) {
				for (PolicyEvaluatorForTag rowFilterEvaluator : rowFilterEvaluators) {
					RangerPolicyEvaluator evaluator      = rowFilterEvaluator.getEvaluator();
					RangerTagForEval      tag            = rowFilterEvaluator.getTag();
					RangerAccessRequest   tagEvalRequest = new RangerTagAccessRequest(tag, tagPolicyRepository.getServiceDef(), request);
					RangerRowFilterResult tagEvalResult  = createRowFilterResult(tagEvalRequest);

					tagEvalResult.setAuditResultFrom(result);
					tagEvalResult.setAccessResultFrom(result);

					tagEvalResult.setFilterExpr(result.getFilterExpr());

					result.incrementEvaluatedPoliciesCount();

					evaluator.evaluate(tagEvalRequest, tagEvalResult);

					if (tagEvalResult.getIsAudited()) {
						result.setAuditResultFrom(tagEvalResult);
					}

					if (tagEvalResult.getIsAccessDetermined()) {
						result.setAccessResultFrom(tagEvalResult);
						result.setFilterExpr(tagEvalResult.getFilterExpr());
					}

					if (result.getIsAuditedDetermined() && result.getIsAccessDetermined()) {
						if (LOG.isDebugEnabled()) {
							LOG.debug("RangerPolicyEngineImpl.evalRowFilterPoliciesForTagPolicies: concluding eval of rowFilter policies for tags: tag=" + tag.getType() + " with authorization=" + tagEvalResult.getIsAllowed());
						}
						break;            // Break out of rowFilter policy-evaluation loop
					}
				}
			}
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== RangerPolicyEngineImpl.evalRowFilterPoliciesForTagPolicies(" + request + ", " + result + ")");
		}
	}

	@Override
	public void reorderPolicyEvaluators() {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> reorderEvaluators()");
		}
		RangerPerfTracer perf = null;

		if(RangerPerfTracer.isPerfTraceEnabled(PERF_POLICYENGINE_REBALANCE_LOG)) {
			perf = RangerPerfTracer.getPerfTracer(PERF_POLICYENGINE_REBALANCE_LOG, "RangerPolicyEngine.reorderEvaluators()");
		}
		if (MapUtils.isNotEmpty(policyEvaluatorsMap)) {
			for (Map.Entry<Long, RangerPolicyEvaluator> entry : policyEvaluatorsMap.entrySet()) {
				entry.getValue().setUsageCountImmutable();
			}
		}

		if (tagPolicyRepository != null) {
			tagPolicyRepository.reorderPolicyEvaluators();
		}
		if (policyRepository != null) {
			policyRepository.reorderPolicyEvaluators();
		}

		if (MapUtils.isNotEmpty(policyEvaluatorsMap)) {
			for (Map.Entry<Long, RangerPolicyEvaluator> entry : policyEvaluatorsMap.entrySet()) {
				entry.getValue().resetUsageCount();
			}
		}

		RangerPerfTracer.log(perf);

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== reorderEvaluators()");
		}
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


	private void setResourceServiceDef(RangerAccessRequest request) {
		RangerAccessResource resource = request.getResource();

		if (resource.getServiceDef() == null) {
			if (resource instanceof RangerMutableResource) {
				RangerMutableResource mutable = (RangerMutableResource) resource;
				mutable.setServiceDef(getServiceDef());
			} else {
				LOG.debug("RangerPolicyEngineImpl.setResourceServiceDef(): Cannot set ServiceDef in RangerMutableResource.");
			}
		}
	}

	private boolean hasTagPolicies() {
		return tagPolicyRepository != null && CollectionUtils.isNotEmpty(tagPolicyRepository.getPolicies());
	}

	private boolean hasResourcePolicies() {
		return policyRepository != null && CollectionUtils.isNotEmpty(policyRepository.getPolicies());
	}

	private Map<Long, RangerPolicyEvaluator> createPolicyEvaluatorsMap() {
		Map<Long, RangerPolicyEvaluator> tmpPolicyEvaluatorMap = new HashMap<>();

		if (tagPolicyRepository != null) {
			for (RangerPolicyEvaluator evaluator : tagPolicyRepository.getPolicyEvaluators()) {
				tmpPolicyEvaluatorMap.put(evaluator.getPolicy().getId(), evaluator);
			}
			for (RangerPolicyEvaluator evaluator : tagPolicyRepository.getDataMaskPolicyEvaluators()) {
				tmpPolicyEvaluatorMap.put(evaluator.getPolicy().getId(), evaluator);
			}
			for (RangerPolicyEvaluator evaluator : tagPolicyRepository.getRowFilterPolicyEvaluators()) {
				tmpPolicyEvaluatorMap.put(evaluator.getPolicy().getId(), evaluator);
			}
		}
		for (RangerPolicyEvaluator evaluator : policyRepository.getPolicyEvaluators()) {
			tmpPolicyEvaluatorMap.put(evaluator.getPolicy().getId(), evaluator);
		}
		for (RangerPolicyEvaluator evaluator : policyRepository.getDataMaskPolicyEvaluators()) {
			tmpPolicyEvaluatorMap.put(evaluator.getPolicy().getId(), evaluator);
		}
		for (RangerPolicyEvaluator evaluator : policyRepository.getRowFilterPolicyEvaluators()) {
			tmpPolicyEvaluatorMap.put(evaluator.getPolicy().getId(), evaluator);
		}

		return  Collections.unmodifiableMap(tmpPolicyEvaluatorMap);
	}

	private void updatePolicyUsageCounts(RangerAccessRequest accessRequest, RangerAccessResult accessResult) {

		boolean auditCountUpdated = false;

		if (accessResult.getIsAccessDetermined()) {
			RangerPolicyEvaluator accessPolicy = getPolicyEvaluator(accessResult.getPolicyId());

			if (accessPolicy != null) {

				if (accessPolicy.getPolicy().getIsAuditEnabled()) {
					updateUsageCount(accessPolicy, 2);
					accessResult.setAuditPolicyId(accessResult.getPolicyId());

					auditCountUpdated = true;
				} else {
					updateUsageCount(accessPolicy, 1);
				}

			}
		}

		if (!auditCountUpdated && accessResult.getIsAuditedDetermined()) {
			long auditPolicyId = accessResult.getAuditPolicyId();
			RangerPolicyEvaluator auditPolicy = auditPolicyId == -1 ? null : getPolicyEvaluator(auditPolicyId);

			updateUsageCount(auditPolicy, 1);
		}

		if(RangerPerfTracer.isPerfTraceEnabled(PERF_POLICYENGINE_USAGE_LOG)) {
			RangerAccessRequestImpl rangerAccessRequest = (RangerAccessRequestImpl) accessRequest;
			RangerPerfTracer perf = RangerPerfTracer.getPerfTracer(PERF_POLICYENGINE_USAGE_LOG,
					"RangerPolicyEngine.usage(accessingUser=" + rangerAccessRequest.getUser()
							+ ",accessedResource=" + rangerAccessRequest.getResource().getAsString()
							+ ",accessType=" + rangerAccessRequest.getAccessType()
							+ ",evaluatedPoliciesCount=" + accessResult.getEvaluatedPoliciesCount() + ")");
			RangerPerfTracer.logAlways(perf);
		}
	}

	private void updateUsageCount(RangerPolicyEvaluator evaluator, int number) {
		if (evaluator != null) {
			evaluator.incrementUsageCount(number);
		}
	}
}
