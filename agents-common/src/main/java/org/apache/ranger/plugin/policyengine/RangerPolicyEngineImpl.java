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
import org.apache.ranger.plugin.policyevaluator.RangerPolicyEvaluator.PolicyACLSummary;
import org.apache.ranger.plugin.policyresourcematcher.RangerPolicyResourceMatcher;
import org.apache.ranger.plugin.util.RangerAccessRequestUtil;
import org.apache.ranger.plugin.util.RangerPerfTracer;
import org.apache.ranger.plugin.util.ServicePolicies;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.ranger.plugin.policyevaluator.RangerPolicyEvaluator.ACCESS_CONDITIONAL;

public class RangerPolicyEngineImpl implements RangerPolicyEngine {
	private static final Log LOG = LogFactory.getLog(RangerPolicyEngineImpl.class);

	private static final Log PERF_POLICYENGINE_INIT_LOG = RangerPerfTracer.getPerfLogger("policyengine.init");
	private static final Log PERF_POLICYENGINE_REQUEST_LOG = RangerPerfTracer.getPerfLogger("policyengine.request");
	private static final Log PERF_POLICYENGINE_AUDIT_LOG = RangerPerfTracer.getPerfLogger("policyengine.audit");
	private static final Log PERF_CONTEXTENRICHER_REQUEST_LOG = RangerPerfTracer.getPerfLogger("contextenricher.request");
	private static final Log PERF_POLICYENGINE_REBALANCE_LOG = RangerPerfTracer.getPerfLogger("policyengine.rebalance");
	private static final Log PERF_POLICYENGINE_USAGE_LOG = RangerPerfTracer.getPerfLogger("policyengine.usage");
	private static final Log PERF_POLICYENGINE_GET_ACLS_LOG = RangerPerfTracer.getPerfLogger("policyengine.getResourceACLs");

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
	protected void finalize() throws Throwable {
		try {
			cleanup();
		}
		finally {
			super.finalize();
		}
	}

	@Override
	public String toString( ) {
		StringBuilder sb = new StringBuilder();

		sb.append("RangerPolicyEngineImpl={");

		sb.append("serviceName={").append(this.getServiceName()).append("} ");
		sb.append(policyRepository);

		sb.append("}");

		return sb.toString();
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
	public boolean getUseForwardedIPAddress() {
		return useForwardedIPAddress;
	}

	@Override
	public String[] getTrustedProxyAddresses() {
		return trustedProxyAddresses;
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
	public RangerAccessResult evaluatePolicies(RangerAccessRequest request, int policyType, RangerAccessResultProcessor resultProcessor) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> RangerPolicyEngineImpl.evaluatePolicies(" + request + ", policyType=" + policyType + ")");
		}
		RangerPerfTracer perf = null;

		if(RangerPerfTracer.isPerfTraceEnabled(PERF_POLICYENGINE_REQUEST_LOG)) {
			String requestHashCode = Integer.toHexString(System.identityHashCode(request)) + "_" + Integer.toString(policyType);
			perf = RangerPerfTracer.getPerfTracer(PERF_POLICYENGINE_REQUEST_LOG, "RangerPolicyEngine.evaluatePolicies(requestHashCode=" + requestHashCode + ")");
			LOG.info("RangerPolicyEngineImpl.evaluatePolicies(" + requestHashCode + ", " + request + ")");
		}

		RangerAccessResult ret = evaluatePoliciesNoAudit(request, policyType);

		updatePolicyUsageCounts(request, ret);

		if (resultProcessor != null) {

			RangerPerfTracer perfAuditTracer = null;
			if(RangerPerfTracer.isPerfTraceEnabled(PERF_POLICYENGINE_AUDIT_LOG)) {
				String requestHashCode = Integer.toHexString(System.identityHashCode(request)) + "_" + Integer.toString(policyType);
				perfAuditTracer = RangerPerfTracer.getPerfTracer(PERF_POLICYENGINE_AUDIT_LOG, "RangerPolicyEngine.processAudit(requestHashCode=" + requestHashCode + ")");
			}

			resultProcessor.processResult(ret);

			RangerPerfTracer.log(perfAuditTracer);
		}

		RangerPerfTracer.log(perf);

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== RangerPolicyEngineImpl.evaluatePolicies(" + request + ", policyType=" + policyType + "): " + ret);
		}

		return ret;
	}

	@Override
	public Collection<RangerAccessResult> evaluatePolicies(Collection<RangerAccessRequest> requests, int policyType, RangerAccessResultProcessor resultProcessor) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> RangerPolicyEngineImpl.evaluatePolicies(" + requests + ", policyType=" + policyType + ")");
		}

		Collection<RangerAccessResult> ret = new ArrayList<>();

		if (requests != null) {
			for (RangerAccessRequest request : requests) {
				RangerAccessResult result = evaluatePoliciesNoAudit(request, policyType);

				ret.add(result);
			}
		}

		if (resultProcessor != null) {
			resultProcessor.processResults(ret);
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== RangerPolicyEngineImpl.evaluatePolicies(" + requests + ", policyType=" + policyType + "): " + ret);
		}

		return ret;
	}

	@Override
	public RangerResourceACLs getResourceACLs(RangerAccessRequest request) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> RangerPolicyEngineImpl.getResourceACLs(request=" + request + ")");
		}

		RangerPerfTracer perf = null;

		if(RangerPerfTracer.isPerfTraceEnabled(PERF_POLICYENGINE_GET_ACLS_LOG)) {
			perf = RangerPerfTracer.getPerfTracer(PERF_POLICYENGINE_GET_ACLS_LOG, "RangerPolicyEngine.getResourceACLs(requestHashCode=" + request.getResource().getAsString() + ")");
		}

		RangerResourceACLs          ret                      = new RangerResourceACLs();
		Set<RangerTagForEval>       tags                     = RangerAccessRequestUtil.getRequestTagsFromContext(request.getContext());
		List<PolicyEvaluatorForTag> tagPolicyEvaluators      = tagPolicyRepository == null ? null : tagPolicyRepository.getLikelyMatchPolicyEvaluators(tags, RangerPolicy.POLICY_TYPE_ACCESS, null);
		List<RangerPolicyEvaluator> resourcePolicyEvaluators = policyRepository.getLikelyMatchPolicyEvaluators(request.getResource(), RangerPolicy.POLICY_TYPE_ACCESS);
		List<RangerPolicyEvaluator> allEvaluators;
		Map<Long, RangerPolicyResourceMatcher.MatchType> tagMatchTypeMap            = null;
		Set<Long>                                        policyIdForTemporalTags    = null;

		if (CollectionUtils.isNotEmpty(tagPolicyEvaluators)) {
			allEvaluators   = new ArrayList<>();
			tagMatchTypeMap = new HashMap<>();

			for (PolicyEvaluatorForTag tagEvaluator : tagPolicyEvaluators) {
				RangerPolicyEvaluator evaluator = tagEvaluator.getEvaluator();
				RangerTagForEval      tag       = tagEvaluator.getTag();

				allEvaluators.add(evaluator);
				tagMatchTypeMap.put(evaluator.getId(), tag.getMatchType());

				if (CollectionUtils.isNotEmpty(tag.getValidityPeriods())) {
					if (policyIdForTemporalTags == null) {
						policyIdForTemporalTags = new HashSet<>();
					}

					policyIdForTemporalTags.add(evaluator.getId());
				}
			}

			allEvaluators.addAll(resourcePolicyEvaluators);
			allEvaluators.sort(RangerPolicyEvaluator.EVAL_ORDER_COMPARATOR);
		} else {
			allEvaluators = resourcePolicyEvaluators;
		}

		if (CollectionUtils.isNotEmpty(allEvaluators)) {
			Integer policyPriority = null;

			for (RangerPolicyEvaluator evaluator : allEvaluators) {
				if (policyPriority == null) {
					policyPriority = evaluator.getPolicyPriority();
				}

				if (policyPriority != evaluator.getPolicyPriority()) {
					ret.finalizeAcls();

					policyPriority = evaluator.getPolicyPriority();
				}

				RangerPolicyResourceMatcher.MatchType matchType = tagMatchTypeMap != null ? tagMatchTypeMap.get(evaluator.getId()) : null;

				if (matchType == null) {
					matchType = evaluator.getPolicyResourceMatcher().getMatchType(request.getResource(), request.getContext());
				}

				final boolean isMatched;

				if (request.getResourceMatchingScope() == RangerAccessRequest.ResourceMatchingScope.SELF_OR_DESCENDANTS) {
					isMatched = matchType != RangerPolicyResourceMatcher.MatchType.NONE;
				} else {
					isMatched = matchType == RangerPolicyResourceMatcher.MatchType.SELF || matchType == RangerPolicyResourceMatcher.MatchType.ANCESTOR;
				}

				if (!isMatched) {
					continue;
				}

				PolicyACLSummary aclSummary = evaluator.getPolicyACLSummary();

				if (aclSummary != null) {

					boolean isConditional = (policyIdForTemporalTags != null && policyIdForTemporalTags.contains(evaluator.getId())) || evaluator.getValidityScheduleEvaluatorsCount() != 0;

					Integer accessResult;
					for (Map.Entry<String, Map<String, PolicyACLSummary.AccessResult>> userAccessInfo : aclSummary.getUsersAccessInfo().entrySet()) {
						final String userName = userAccessInfo.getKey();

						for (Map.Entry<String, PolicyACLSummary.AccessResult> accessInfo : userAccessInfo.getValue().entrySet()) {
							if (isConditional) {
								accessResult = ACCESS_CONDITIONAL;
							} else {
								accessResult = accessInfo.getValue().getResult();
								if (accessResult.equals(RangerPolicyEvaluator.ACCESS_UNDETERMINED)) {
									accessResult = RangerPolicyEvaluator.ACCESS_DENIED;
								}
							}
							ret.setUserAccessInfo(userName, accessInfo.getKey(), accessResult);
						}
					}

					for (Map.Entry<String, Map<String, PolicyACLSummary.AccessResult>> groupAccessInfo : aclSummary.getGroupsAccessInfo().entrySet()) {
						final String groupName = groupAccessInfo.getKey();

						for (Map.Entry<String, PolicyACLSummary.AccessResult> accessInfo : groupAccessInfo.getValue().entrySet()) {
							if (isConditional) {
								accessResult = ACCESS_CONDITIONAL;
							} else {
								accessResult = accessInfo.getValue().getResult();
								if (accessResult.equals(RangerPolicyEvaluator.ACCESS_UNDETERMINED)) {
									accessResult = RangerPolicyEvaluator.ACCESS_DENIED;
								}
							}
							ret.setGroupAccessInfo(groupName, accessInfo.getKey(), accessResult);
						}
					}
				}
			}
			ret.finalizeAcls();
		}

		RangerPerfTracer.logAlways(perf);

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== RangerPolicyEngineImpl.getResourceACLs(request=" + request + ") : ret=" + ret);
		}

		return ret;
	}

	@Override
	public boolean preCleanup() {

		boolean ret = true;
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> RangerPolicyEngineImpl.preCleanup()");
		}

		if (CollectionUtils.isNotEmpty(allContextEnrichers)) {
			for (RangerContextEnricher contextEnricher : allContextEnrichers) {
				boolean readyForCleanup = contextEnricher.preCleanup();
				if (!readyForCleanup) {
				    LOG.warn("contextEnricher.preCleanup() failed for contextEnricher=" + contextEnricher.getName());
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

	/*
	* This API is used by ranger-admin
	*/

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

		for (RangerPolicyEvaluator evaluator : policyRepository.getLikelyMatchPolicyEvaluators(resource, RangerPolicy.POLICY_TYPE_ACCESS)) {
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

	/*
	 * This API is used by ranger-admin
	 */

	@Override
	public boolean isAccessAllowed(RangerPolicy policy, String user, Set<String> userGroups, String accessType) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> RangerPolicyEngineImpl.isAccessAllowed(" + policy.getId() + ", " + user + ", " + userGroups + ", " + accessType + ")");
		}

		boolean ret = false;

		RangerPerfTracer perf = null;

		if(RangerPerfTracer.isPerfTraceEnabled(PERF_POLICYENGINE_REQUEST_LOG)) {
			perf = RangerPerfTracer.getPerfTracer(PERF_POLICYENGINE_REQUEST_LOG, "RangerPolicyEngine.isAccessAllowed(user=" + user + "," + userGroups + ",accessType=" + accessType + ")");
		}

		for (RangerPolicyEvaluator evaluator : policyRepository.getPolicyEvaluators()) {
			ret = evaluator.isAccessAllowed(policy, user, userGroups, accessType);

			if (ret) {
				break;
			}
		}

		RangerPerfTracer.log(perf);

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== RangerPolicyEngineImpl.isAccessAllowed(" + policy.getId() + ", " + user + ", " + userGroups + ", " + accessType + "): " + ret);
		}

		return ret;
	}
	/*
	* This API is used by ranger-admin
	*/

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

	/*
	* This API is used by ranger-admin
	*/

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

	/*
	* This API is used by ranger-admin
	*/

	@Override
	public List<RangerPolicy> getMatchingPolicies(RangerAccessResource resource) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> RangerPolicyEngineImpl.getMatchingPolicies(" + resource + ")");
		}

		RangerAccessRequestImpl request = new RangerAccessRequestImpl(resource, RangerPolicyEngine.ANY_ACCESS, null, null);

		preProcess(request);

		List<RangerPolicy> ret = getMatchingPolicies(request);

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== RangerPolicyEngineImpl.getMatchingPolicies(" + resource + ") : " + ret.size());
		}

		return ret;
	}

	@Override
	public List<RangerPolicy> getMatchingPolicies(RangerAccessRequest request) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> RangerPolicyEngineImpl.getMatchingPolicies(" + request + ")");
		}

		List<RangerPolicy> ret = new ArrayList<>();

		if (hasTagPolicies()) {
			Set<RangerTagForEval> tags = RangerAccessRequestUtil.getRequestTagsFromContext(request.getContext());

			if (CollectionUtils.isNotEmpty(tags)) {
				for (RangerTagForEval tag : tags) {
					RangerAccessRequest         tagEvalRequest            = new RangerTagAccessRequest(tag, tagPolicyRepository.getServiceDef(), request);
					RangerAccessResource        tagResource               = tagEvalRequest.getResource();
					List<RangerPolicyEvaluator> likelyEvaluators          = tagPolicyRepository.getLikelyMatchPolicyEvaluators(tagResource);

					for (RangerPolicyEvaluator evaluator : likelyEvaluators) {
						RangerPolicyResourceMatcher matcher = evaluator.getPolicyResourceMatcher();
						if (matcher != null &&
								(request.isAccessTypeAny() ? matcher.isMatch(tagResource, RangerPolicyResourceMatcher.MatchScope.ANY, null) : matcher.isMatch(tagResource, null))) {
							ret.add(evaluator.getPolicy());
						}
					}

				}
			}
		}

		if (hasResourcePolicies()) {
			List<RangerPolicyEvaluator> likelyEvaluators = policyRepository.getLikelyMatchPolicyEvaluators(request.getResource());

			for (RangerPolicyEvaluator evaluator : likelyEvaluators) {
				RangerPolicyResourceMatcher matcher = evaluator.getPolicyResourceMatcher();
				if (matcher != null &&
						(request.isAccessTypeAny() ? matcher.isMatch(request.getResource(), RangerPolicyResourceMatcher.MatchScope.ANY, null) : matcher.isMatch(request.getResource(), null))) {
					ret.add(evaluator.getPolicy());
				}
			}

		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== RangerPolicyEngineImpl.getMatchingPolicies(" + request + ") : " + ret.size());
		}
		return ret;
	}

	/*
	* This API is used by ranger-admin
	*/

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

					List<RangerPolicyEvaluator> evaluators = tagPolicyRepository.getLikelyMatchPolicyEvaluators(tagEvalRequest.getResource(), RangerPolicy.POLICY_TYPE_ACCESS);

					for (RangerPolicyEvaluator evaluator : evaluators) {
						evaluator.getResourceAccessInfo(tagEvalRequest, ret);
					}
				}
			}
		}

		List<RangerPolicyEvaluator> resPolicyEvaluators = policyRepository.getLikelyMatchPolicyEvaluators(request.getResource(), RangerPolicy.POLICY_TYPE_ACCESS);

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

	/*
	 * This API is used by test-code
	 */

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

	/*
	* This API is used only by test-code
	*/

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

	private RangerAccessResult evaluatePoliciesNoAudit(RangerAccessRequest request, int policyType) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> RangerPolicyEngineImpl.evaluatePoliciesNoAudit(" + request + ", policyType =" + policyType + ")");
		}

		RangerAccessResult ret = createAccessResult(request, policyType);
		Date accessTime = request.getAccessTime();

        if (ret != null && request != null) {

			evaluateTagPolicies(request, policyType, ret);

			if (LOG.isDebugEnabled()) {
				if (ret.getIsAccessDetermined() && ret.getIsAuditedDetermined()) {
					if (!ret.getIsAllowed()) {
						LOG.debug("RangerPolicyEngineImpl.evaluatePoliciesNoAudit() - audit determined and access denied by a tag policy. Higher priority resource policies will be evaluated to check for allow, request=" + request + ", result=" + ret);
					} else {
						LOG.debug("RangerPolicyEngineImpl.evaluatePoliciesNoAudit() - audit determined and access allowed by a tag policy. Same or higher priority resource policies will be evaluated to check for deny, request=" + request + ", result=" + ret);
					}
				}
			}

			boolean isAllowedByTags          = ret.getIsAccessDetermined() && ret.getIsAllowed();
			boolean isDeniedByTags           = ret.getIsAccessDetermined() && !ret.getIsAllowed();
			boolean evaluateResourcePolicies = hasResourcePolicies();

			if (evaluateResourcePolicies) {
				boolean findAuditByResource = !ret.getIsAuditedDetermined();
				boolean foundInCache        = findAuditByResource && policyRepository.setAuditEnabledFromCache(request, ret);

				ret.setIsAccessDetermined(false); // discard result by tag-policies, to evaluate resource policies for possible override

				List<RangerPolicyEvaluator> evaluators = policyRepository.getLikelyMatchPolicyEvaluators(request.getResource(), policyType);

				for (RangerPolicyEvaluator evaluator : evaluators) {
					if (!evaluator.isApplicable(accessTime)) {
						continue;
					}

					if (isDeniedByTags) {
						if (ret.getPolicyPriority() >= evaluator.getPolicyPriority()) {
							ret.setIsAccessDetermined(true);
						}
					} else if (isAllowedByTags) {
						if (ret.getPolicyPriority() > evaluator.getPolicyPriority()) {
							ret.setIsAccessDetermined(true);
						}
					}

					ret.incrementEvaluatedPoliciesCount();
					evaluator.evaluate(request, ret);

					if (ret.getIsAllowed()) {
						if (!evaluator.hasDeny()) { // No more deny policies left
							ret.setIsAccessDetermined(true);
						}
					}

					if (ret.getIsAuditedDetermined() && ret.getIsAccessDetermined()) {
						break;            // Break out of policy-evaluation loop
					}

				}

				if (!ret.getIsAccessDetermined()) {
					if (isDeniedByTags) {
						ret.setIsAllowed(false);
					} else if (isAllowedByTags) {
						ret.setIsAllowed(true);
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
			LOG.debug("<== RangerPolicyEngineImpl.evaluatePoliciesNoAudit(" + request + ", policyType =" + policyType + "): " + ret);
		}

		return ret;
	}

	private void evaluateTagPolicies(final RangerAccessRequest request, int policyType, RangerAccessResult result) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> RangerPolicyEngineImpl.evaluateTagPolicies(" + request + ", policyType =" + policyType + ", " + result + ")");
		}

		Date accessTime = request.getAccessTime();

		Set<RangerTagForEval> tags = RangerAccessRequestUtil.getRequestTagsFromContext(request.getContext());

		List<PolicyEvaluatorForTag> policyEvaluators = tagPolicyRepository == null ? null : tagPolicyRepository.getLikelyMatchPolicyEvaluators(tags, policyType, accessTime);

		if (CollectionUtils.isNotEmpty(policyEvaluators)) {
			for (PolicyEvaluatorForTag policyEvaluator : policyEvaluators) {
				RangerPolicyEvaluator evaluator = policyEvaluator.getEvaluator();

				RangerTagForEval tag = policyEvaluator.getTag();

				RangerAccessRequest tagEvalRequest = new RangerTagAccessRequest(tag, tagPolicyRepository.getServiceDef(), request);
				RangerAccessResult tagEvalResult = createAccessResult(tagEvalRequest, policyType);

				if (LOG.isDebugEnabled()) {
					LOG.debug("RangerPolicyEngineImpl.evaluateTagPolicies: Evaluating policies for tag (" + tag.getType() + ")");
				}

				tagEvalResult.setAccessResultFrom(result);
				tagEvalResult.setAuditResultFrom(result);

				result.incrementEvaluatedPoliciesCount();

				evaluator.evaluate(tagEvalRequest, tagEvalResult);

				if (tagEvalResult.getIsAllowed()) {
					if (!evaluator.hasDeny()) { // No Deny policies left now
						tagEvalResult.setIsAccessDetermined(true);
					}
				}

				if (tagEvalResult.getIsAudited()) {
					result.setAuditResultFrom(tagEvalResult);
				}

				if (!result.getIsAccessDetermined()) {
					if (tagEvalResult.getIsAccessDetermined()) {
						result.setAccessResultFrom(tagEvalResult);
					} else {
						if (!result.getIsAllowed() && tagEvalResult.getIsAllowed()) {
							result.setAccessResultFrom(tagEvalResult);
						}
					}
				}

				if (result.getIsAuditedDetermined() && result.getIsAccessDetermined()) {
					break;            // Break out of policy-evaluation loop
				}
			}
		}
		if (result.getIsAllowed()) {
			result.setIsAccessDetermined(true);
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== RangerPolicyEngineImpl.evaluateTagPolicies(" + request + ", policyType =" + policyType + ", " + result + ")");
		}
	}

	private String getServiceName() {
		return policyRepository.getServiceName();
	}

	private RangerPolicyEvaluator getPolicyEvaluator(Long id) {
		return policyEvaluatorsMap.get(id);
	}

	private RangerAccessResult createAccessResult(RangerAccessRequest request, int policyType) {
		RangerAccessResult ret = new RangerAccessResult(policyType, this.getServiceName(), policyRepository.getServiceDef(), request);
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
