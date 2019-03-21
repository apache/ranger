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
import org.apache.commons.collections.ListUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ranger.authorization.hadoop.config.RangerConfiguration;
import org.apache.ranger.plugin.contextenricher.RangerContextEnricher;
import org.apache.ranger.plugin.contextenricher.RangerTagForEval;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerPolicyDelta;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyResource;
import org.apache.ranger.plugin.model.validation.RangerZoneResourceMatcher;
import org.apache.ranger.plugin.policyevaluator.RangerPolicyEvaluator;
import org.apache.ranger.plugin.policyevaluator.RangerPolicyEvaluator.PolicyACLSummary;
import org.apache.ranger.plugin.policyresourcematcher.RangerPolicyResourceMatcher;
import org.apache.ranger.plugin.store.EmbeddedServiceDefsUtil;
import org.apache.ranger.plugin.util.GrantRevokeRequest;
import org.apache.ranger.plugin.util.RangerAccessRequestUtil;
import org.apache.ranger.plugin.util.RangerPerfTracer;
import org.apache.ranger.plugin.util.RangerResourceTrie;
import org.apache.ranger.plugin.util.RangerPolicyDeltaUtil;
import org.apache.ranger.plugin.util.ServicePolicies;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
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

	private boolean isPolicyRepositoryShared = false;
	private boolean isTagPolicyRepositoryShared = false;

	private List<RangerContextEnricher> allContextEnrichers;

	private boolean  useForwardedIPAddress;
	private String[] trustedProxyAddresses;

	private Map<String, RangerPolicyRepository> policyRepositories = new HashMap<>();

	private Map<String, RangerResourceTrie>   trieMap;

	public RangerPolicyEngineImpl(final RangerPolicyEngineImpl other, ServicePolicies servicePolicies) {

		List<RangerPolicyDelta> deltas        = servicePolicies.getPolicyDeltas();
		long                    policyVersion = servicePolicies.getPolicyVersion();

		this.useForwardedIPAddress = other.useForwardedIPAddress;
		this.trustedProxyAddresses = other.trustedProxyAddresses;

		List<RangerPolicyDelta> defaultZoneDeltas = new ArrayList<>();
		List<RangerPolicyDelta> defaultZoneDeltasForTagPolicies = new ArrayList<>();

		if (MapUtils.isNotEmpty(servicePolicies.getSecurityZones())) {
			Map<String, List<RangerPolicyDelta>> zoneDeltasMap = new HashMap<>();

			buildZoneTrie(servicePolicies);

			for (Map.Entry<String, ServicePolicies.SecurityZoneInfo> zone : servicePolicies.getSecurityZones().entrySet()) {
				zoneDeltasMap.put(zone.getKey(), new ArrayList<>());
			}
			for (RangerPolicyDelta delta : deltas) {
				String zoneName = delta.getZoneName();

				if (StringUtils.isNotEmpty(zoneName)) {
					List<RangerPolicyDelta> zoneDeltas = zoneDeltasMap.get(zoneName);
					if (zoneDeltas != null) {
						zoneDeltas.add(delta);
					}
				} else {
					if (servicePolicies.getServiceDef().getName().equals(delta.getServiceType())) {
						defaultZoneDeltas.add(delta);
					} else {
						defaultZoneDeltasForTagPolicies.add(delta);
					}
				}
			}
			for (Map.Entry<String, ServicePolicies.SecurityZoneInfo> zone : servicePolicies.getSecurityZones().entrySet()) {
				final String                 zoneName        = zone.getKey();
				List<RangerPolicyDelta>      zoneDeltas      = zoneDeltasMap.get(zoneName);

				RangerPolicyRepository       otherRepository = other.policyRepositories.get(zoneName);
				final RangerPolicyRepository policyRepository;

				if (CollectionUtils.isNotEmpty(zoneDeltas)) {
					if (otherRepository == null) {
						List<RangerPolicy> policies = new ArrayList<>();
						for (RangerPolicyDelta delta : zoneDeltas) {
							if (delta.getChangeType() == RangerPolicyDelta.CHANGE_TYPE_POLICY_CREATE) {
								policies.add(delta.getPolicy());
							} else {
								LOG.warn("Expected changeType:[" + RangerPolicyDelta.CHANGE_TYPE_POLICY_CREATE + "], found policy-change-delta:[" + delta +"]");
							}
						}
						servicePolicies.getSecurityZones().get(zoneName).setPolicies(policies);

						policyRepository = new RangerPolicyRepository(other.policyRepository.getAppId(), servicePolicies, other.policyRepository.getOptions(), zoneName);
					} else {
						policyRepository = new RangerPolicyRepository(otherRepository, zoneDeltas, policyVersion);
					}
				} else {
					policyRepository = otherRepository;
				}

				policyRepositories.put(zoneName, policyRepository);
			}
		} else {
			for (RangerPolicyDelta delta : deltas) {
				if (servicePolicies.getServiceDef().getName().equals(delta.getServiceType())) {
					defaultZoneDeltas.add(delta);
				} else {
					defaultZoneDeltasForTagPolicies.add(delta);
				}
			}
		}

		if (other.policyRepository != null && CollectionUtils.isNotEmpty(defaultZoneDeltas)) {
			this.policyRepository      = new RangerPolicyRepository(other.policyRepository, defaultZoneDeltas, policyVersion);
		} else {
			this.policyRepository = other.policyRepository;
			other.isPolicyRepositoryShared = true;
		}
		if (CollectionUtils.isNotEmpty(defaultZoneDeltasForTagPolicies)) {
			if (other.tagPolicyRepository != null) {
				this.tagPolicyRepository = new RangerPolicyRepository(other.tagPolicyRepository, defaultZoneDeltasForTagPolicies, policyVersion);
			} else {
				// Only creates are expected
				List<RangerPolicy> tagPolicies = new ArrayList<>();
				for (RangerPolicyDelta delta : defaultZoneDeltasForTagPolicies) {
					if (delta.getChangeType() == RangerPolicyDelta.CHANGE_TYPE_POLICY_CREATE) {
						tagPolicies.add(delta.getPolicy());
					} else {
						LOG.warn("Expected changeType:[" + RangerPolicyDelta.CHANGE_TYPE_POLICY_CREATE + "], found policy-change-delta:[" + delta +"]");
					}
				}
				servicePolicies.getTagPolicies().setPolicies(tagPolicies);
				this.tagPolicyRepository = new RangerPolicyRepository(other.policyRepository.getAppId(), servicePolicies.getTagPolicies(), other.policyRepository.getOptions(), servicePolicies.getServiceDef(), servicePolicies.getServiceName());

			}
		} else {
			this.tagPolicyRepository = other.tagPolicyRepository;
			other.isTagPolicyRepositoryShared = true;
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

		reorderPolicyEvaluators();

	}

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

		if (MapUtils.isNotEmpty(servicePolicies.getSecurityZones())) {
			buildZoneTrie(servicePolicies);
			for (Map.Entry<String, ServicePolicies.SecurityZoneInfo> zone : servicePolicies.getSecurityZones().entrySet()) {
				RangerPolicyRepository policyRepository = new RangerPolicyRepository(appId, servicePolicies, options, zone.getKey());
				policyRepositories.put(zone.getKey(), policyRepository);
			}
		}

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
	public RangerPolicyEngine cloneWithDelta(ServicePolicies servicePolicies) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> cloneWithDelta(" + Arrays.toString(servicePolicies.getPolicyDeltas().toArray()) + ", " + servicePolicies.getPolicyVersion() + ")");
		}
		final RangerPolicyEngineImpl ret;

		RangerPerfTracer perf = null;

		if(RangerPerfTracer.isPerfTraceEnabled(PERF_POLICYENGINE_INIT_LOG)) {
			perf = RangerPerfTracer.getPerfTracer(PERF_POLICYENGINE_INIT_LOG, "RangerPolicyEngine.cloneWithDelta()");
		}

		if (CollectionUtils.isNotEmpty(servicePolicies.getPolicyDeltas()) && RangerPolicyDeltaUtil.isValidDeltas(servicePolicies.getPolicyDeltas(), this.getServiceDef().getName())) {
			ret = new RangerPolicyEngineImpl(this, servicePolicies);
		} else {
			ret = null;
		}

		RangerPerfTracer.log(perf);

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== cloneWithDelta(" + Arrays.toString(servicePolicies.getPolicyDeltas().toArray()) + ", " + servicePolicies.getPolicyVersion() + ")");
		}
		return ret;
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

		RangerAccessResult ret = zoneAwareAccessEvaluationWithNoAudit(request, policyType);

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
				RangerAccessResult result = zoneAwareAccessEvaluationWithNoAudit(request, policyType);

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

		RangerResourceACLs ret  = new RangerResourceACLs();

		RangerPerfTracer   perf = null;

		if(RangerPerfTracer.isPerfTraceEnabled(PERF_POLICYENGINE_GET_ACLS_LOG)) {
			perf = RangerPerfTracer.getPerfTracer(PERF_POLICYENGINE_GET_ACLS_LOG, "RangerPolicyEngine.getResourceACLs(requestHashCode=" + request.getResource().getAsString() + ")");
		}

		String zoneName = trieMap == null ? null : getMatchedZoneName(request.getResource());

		if (LOG.isDebugEnabled()) {
			LOG.debug("zoneName:[" + zoneName + "]");
		}

		Collection<RangerPolicyRepository> matchedRepositories = new ArrayList<>();

		if (StringUtils.isNotEmpty(zoneName)) {
			RangerPolicyRepository policyRepository = policyRepositories.get(zoneName);

			if (policyRepository == null) {
				LOG.error("policyRepository for zoneName:[" + zoneName + "] is null!! ERROR!");
			} else {
				matchedRepositories.add(policyRepository);
			}
		} else {
			// Search all security zones
			matchedRepositories.add(this.policyRepository);
			matchedRepositories.addAll(this.policyRepositories.values());
		}

		List<RangerPolicyEvaluator>                      allEvaluators           = new ArrayList<>();
		Map<Long, RangerPolicyResourceMatcher.MatchType> tagMatchTypeMap         = null;
		Set<Long>                                        policyIdForTemporalTags = null;

		Set<RangerTagForEval> tags = RangerAccessRequestUtil.getRequestTagsFromContext(request.getContext());
		List<PolicyEvaluatorForTag> tagPolicyEvaluators = tagPolicyRepository == null ? null : tagPolicyRepository.getLikelyMatchPolicyEvaluators(tags, RangerPolicy.POLICY_TYPE_ACCESS, null);

		if (CollectionUtils.isNotEmpty(tagPolicyEvaluators)) {
			tagMatchTypeMap = new HashMap<>();

			for (PolicyEvaluatorForTag tagEvaluator : tagPolicyEvaluators) {
				RangerPolicyEvaluator evaluator = tagEvaluator.getEvaluator();
				RangerTagForEval tag = tagEvaluator.getTag();

				allEvaluators.add(evaluator);
				tagMatchTypeMap.put(evaluator.getId(), tag.getMatchType());

				if (CollectionUtils.isNotEmpty(tag.getValidityPeriods())) {
					if (policyIdForTemporalTags == null) {
						policyIdForTemporalTags = new HashSet<>();
					}

					policyIdForTemporalTags.add(evaluator.getId());
				}
			}
		}

		for (RangerPolicyRepository policyRepository : matchedRepositories) {
			List<RangerPolicyEvaluator> resourcePolicyEvaluators = policyRepository.getLikelyMatchPolicyEvaluators(request.getResource(), RangerPolicy.POLICY_TYPE_ACCESS);

			allEvaluators.addAll(resourcePolicyEvaluators);
		}

		allEvaluators.sort(RangerPolicyEvaluator.EVAL_ORDER_COMPARATOR);

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
							RangerPolicy policy = evaluator.getPolicy();
							ret.setUserAccessInfo(userName, accessInfo.getKey(), accessResult, policy);
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
							RangerPolicy policy = evaluator.getPolicy();
							ret.setGroupAccessInfo(groupName, accessInfo.getKey(), accessResult, policy);
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

		if (policyRepository != null && !isPolicyRepositoryShared) {
			policyRepository.preCleanup();
		}
		if (tagPolicyRepository != null && !isTagPolicyRepositoryShared) {
			tagPolicyRepository.preCleanup();
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

		if (policyRepository != null && !isPolicyRepositoryShared) {
			policyRepository.cleanup();
		}
		if (tagPolicyRepository != null && !isTagPolicyRepositoryShared) {
			tagPolicyRepository.cleanup();
		}

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
		if (tagPolicyRepository != null && MapUtils.isNotEmpty(tagPolicyRepository.getPolicyEvaluatorsMap())) {
			for (Map.Entry<Long, RangerPolicyEvaluator> entry : tagPolicyRepository.getPolicyEvaluatorsMap().entrySet()) {
				entry.getValue().setUsageCountImmutable();
			}
		}
		if (policyRepository != null && MapUtils.isNotEmpty(policyRepository.getPolicyEvaluatorsMap())) {
			for (Map.Entry<Long, RangerPolicyEvaluator> entry : policyRepository.getPolicyEvaluatorsMap().entrySet()) {
				entry.getValue().setUsageCountImmutable();
			}
		}

		if (tagPolicyRepository != null) {
			tagPolicyRepository.reorderPolicyEvaluators();
		}
		if (policyRepository != null) {
			policyRepository.reorderPolicyEvaluators();
		}

		if (tagPolicyRepository != null && MapUtils.isNotEmpty(tagPolicyRepository.getPolicyEvaluatorsMap())) {
			for (Map.Entry<Long, RangerPolicyEvaluator> entry : tagPolicyRepository.getPolicyEvaluatorsMap().entrySet()) {
				entry.getValue().resetUsageCount();
			}
		}
		if (policyRepository != null && MapUtils.isNotEmpty(policyRepository.getPolicyEvaluatorsMap())) {
			for (Map.Entry<Long, RangerPolicyEvaluator> entry : policyRepository.getPolicyEvaluatorsMap().entrySet()) {
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
		boolean ret = false;

		RangerPerfTracer perf = null;

		if(RangerPerfTracer.isPerfTraceEnabled(PERF_POLICYENGINE_REQUEST_LOG)) {
			perf = RangerPerfTracer.getPerfTracer(PERF_POLICYENGINE_REQUEST_LOG, "RangerPolicyEngine.isAccessAllowed(user=" + user + ",accessType=" + accessType + "resource=" + resource.getAsString() + ")");
		}

		String zoneName = trieMap == null ? null : getMatchedZoneName(resource);

		if (LOG.isDebugEnabled()) {
			LOG.debug("zoneName:[" + zoneName + "]");
		}

		Collection<RangerPolicyRepository> matchedRepositories = new ArrayList<>();

		if (StringUtils.isNotEmpty(zoneName)) {
			RangerPolicyRepository policyRepository = policyRepositories.get(zoneName);

			if (policyRepository == null) {
				LOG.error("policyRepository for zoneName:[" + zoneName + "] is null!! ERROR!");
			} else {
				matchedRepositories.add(policyRepository);
			}
		} else {
			// Search all security zones
			matchedRepositories.add(this.policyRepository);
			matchedRepositories.addAll(this.policyRepositories.values());
		}

		for (RangerPolicyRepository policyRepository : matchedRepositories) {
			for (RangerPolicyEvaluator evaluator : policyRepository.getLikelyMatchPolicyEvaluators(resource, RangerPolicy.POLICY_TYPE_ACCESS)) {
				ret = evaluator.isAccessAllowed(resource, user, userGroups, accessType);

				if (ret) {
					break;
				}
			}
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

		String zoneName = trieMap == null ? null : policy.getZoneName();

		if (LOG.isDebugEnabled()) {
			LOG.debug("zoneName:[" + zoneName + "]");
		}

		Collection<RangerPolicyRepository> matchedRepositories = new ArrayList<>();

		if (StringUtils.isNotEmpty(zoneName)) {
			RangerPolicyRepository policyRepository = policyRepositories.get(zoneName);

			if (policyRepository == null) {
				LOG.error("policyRepository for zoneName:[" + zoneName + "] is null!! ERROR!");
			} else {
				matchedRepositories.add(policyRepository);
			}
		} else {
			// Search all security zones
			matchedRepositories.add(this.policyRepository);
			matchedRepositories.addAll(this.policyRepositories.values());
		}

		for (RangerPolicyRepository policyRepository : matchedRepositories) {
			for (RangerPolicyEvaluator evaluator : policyRepository.getPolicyEvaluators()) {
				ret = evaluator.isAccessAllowed(policy, user, userGroups, accessType);

				if (ret) {
					break;
				}
			}
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

		RangerPolicyRepository policyRepository = this.policyRepository;

		String zoneName = trieMap == null ? null : getMatchedZoneName(resource);

		if (LOG.isDebugEnabled()) {
			LOG.debug("zoneName:[" + zoneName + "]");
		}

		if (StringUtils.isNotEmpty(zoneName)) {
			policyRepository = policyRepositories.get(zoneName);

			if (policyRepository == null) {
				LOG.error("policyRepository for zoneName:[" + zoneName + "] is null!! ERROR!");
			}
		}

		if (policyRepository != null) {
			for (RangerPolicyEvaluator evaluator : policyRepository.getPolicyEvaluators()) {
				if (evaluator.isCompleteMatch(resource, evalContext)) {
					if (ret == null) {
						ret = new ArrayList<>();
					}

					ret.add(evaluator.getPolicy());
				}
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
	public List<RangerPolicy> getExactMatchPolicies(RangerPolicy policy, Map<String, Object> evalContext) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> RangerPolicyEngineImpl.getExactMatchPolicies(" + policy + ", " + evalContext + ")");
		}

		List<RangerPolicy> ret = null;
		RangerPolicyRepository policyRepository = this.policyRepository;

		String zoneName = trieMap == null ? null : policy.getZoneName();

		if (LOG.isDebugEnabled()) {
			LOG.debug("zoneName:[" + zoneName + "]");
		}

		if (StringUtils.isNotEmpty(zoneName)) {
			policyRepository = policyRepositories.get(zoneName);

			if (policyRepository == null) {
				LOG.error("policyRepository for zoneName:[" + zoneName + "] is null!! ERROR!");
			}
		}

		if (policyRepository != null) {
			Map<String, RangerPolicyResource> resources = policy.getResources();

			for (RangerPolicyEvaluator evaluator : policyRepository.getPolicyEvaluators()) {
				if (evaluator.isCompleteMatch(resources, evalContext)) {
					if (ret == null) {
						ret = new ArrayList<>();
					}

					ret.add(evaluator.getPolicy());
				}
			}
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== RangerPolicyEngineImpl.getExactMatchPolicies(" + policy + ", " + evalContext + "): " + ret);
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

		String zoneName = trieMap == null ? null : getMatchedZoneName(request.getResource());

		if (LOG.isDebugEnabled()) {
			LOG.debug("zoneName:[" + zoneName + "]");
		}

		Collection<RangerPolicyRepository> matchedRepositories = new ArrayList<>();

		if (StringUtils.isNotEmpty(zoneName)) {
			RangerPolicyRepository policyRepository = policyRepositories.get(zoneName);

			if (policyRepository == null) {
				LOG.error("policyRepository for zoneName:[" + zoneName + "] is null!! ERROR!");
			} else {
				matchedRepositories.add(policyRepository);
			}
		} else {
			// Search all security zones
			matchedRepositories.add(this.policyRepository);
			matchedRepositories.addAll(this.policyRepositories.values());
		}

		if (hasTagPolicies(tagPolicyRepository)) {
			Set<RangerTagForEval> tags = RangerAccessRequestUtil.getRequestTagsFromContext(request.getContext());

			if (CollectionUtils.isNotEmpty(tags)) {
				for (RangerTagForEval tag : tags) {
					RangerAccessRequest tagEvalRequest = new RangerTagAccessRequest(tag, tagPolicyRepository.getServiceDef(), request);
					RangerAccessResource tagResource = tagEvalRequest.getResource();
					List<RangerPolicyEvaluator> likelyEvaluators = tagPolicyRepository.getLikelyMatchPolicyEvaluators(tagResource);

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

		for (RangerPolicyRepository policyRepository : matchedRepositories) {

			if (hasResourcePolicies(policyRepository)) {
				List<RangerPolicyEvaluator> likelyEvaluators = policyRepository.getLikelyMatchPolicyEvaluators(request.getResource());

				for (RangerPolicyEvaluator evaluator : likelyEvaluators) {
					RangerPolicyResourceMatcher matcher = evaluator.getPolicyResourceMatcher();
					if (matcher != null &&
								(request.isAccessTypeAny() ? matcher.isMatch(request.getResource(), RangerPolicyResourceMatcher.MatchScope.ANY, null) : matcher.isMatch(request.getResource(), null))) {
						ret.add(evaluator.getPolicy());
					}
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

		String zoneName = trieMap == null ? null : getMatchedZoneName(request.getResource());

		if (LOG.isDebugEnabled()) {
			LOG.debug("zoneName:[" + zoneName + "]");
		}

		Collection<RangerPolicyRepository> matchedRepositories = new ArrayList<>();

		if (StringUtils.isNotEmpty(zoneName)) {
			RangerPolicyRepository policyRepository = policyRepositories.get(zoneName);

			if (policyRepository == null) {
				LOG.error("policyRepository for zoneName:[" + zoneName + "] is null!! ERROR!");
			} else {
				matchedRepositories.add(policyRepository);
			}
		} else {
			// Search all security zones
			matchedRepositories.add(this.policyRepository);
			matchedRepositories.addAll(this.policyRepositories.values());
		}

		List<RangerPolicyEvaluator> tagPolicyEvaluators = tagPolicyRepository == null ? null : tagPolicyRepository.getPolicyEvaluators();

		if (CollectionUtils.isNotEmpty(tagPolicyEvaluators)) {

			Set<RangerTagForEval> tags = RangerAccessRequestUtil.getRequestTagsFromContext(request.getContext());

			if (CollectionUtils.isNotEmpty(tags)) {
				for (RangerTagForEval tag : tags) {
					RangerAccessRequest tagEvalRequest = new RangerTagAccessRequest(tag, tagPolicyRepository.getServiceDef(), request);

					List<RangerPolicyEvaluator> evaluators = tagPolicyRepository.getLikelyMatchPolicyEvaluators(tagEvalRequest.getResource(), RangerPolicy.POLICY_TYPE_ACCESS);

					for (RangerPolicyEvaluator evaluator : evaluators) {
						evaluator.getResourceAccessInfo(tagEvalRequest, ret);
					}
				}
			}
		}

		for (RangerPolicyRepository policyRepository : matchedRepositories) {

			List<RangerPolicyEvaluator> resPolicyEvaluators = policyRepository.getLikelyMatchPolicyEvaluators(request.getResource(), RangerPolicy.POLICY_TYPE_ACCESS);

			if (CollectionUtils.isNotEmpty(resPolicyEvaluators)) {
				for (RangerPolicyEvaluator evaluator : resPolicyEvaluators) {
					evaluator.getResourceAccessInfo(request, ret);
				}
			}

			ret.getAllowedUsers().removeAll(ret.getDeniedUsers());
			ret.getAllowedGroups().removeAll(ret.getDeniedGroups());
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerPolicyEngineImpl.getResourceAccessInfo(" + request + "): " + ret);
		}

		return ret;
	}

	/*
	 * This API is used by test-code; checks only policies within default security-zone
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
	* This API is used only by test-code; checks only policies within default security-zone
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

	public List<RangerPolicy> getResourcePolicies(String zoneName) {
		RangerPolicyRepository zoneResourceRepository = policyRepositories.get(zoneName);
		return zoneResourceRepository == null ? ListUtils.EMPTY_LIST : zoneResourceRepository.getPolicies();
	}

	public List<RangerPolicy> getResourcePolicies() { return policyRepository == null ? ListUtils.EMPTY_LIST : policyRepository.getPolicies(); }

	public List<RangerPolicy> getTagPolicies() { return tagPolicyRepository == null ? ListUtils.EMPTY_LIST : tagPolicyRepository.getPolicies(); }

	private RangerAccessResult zoneAwareAccessEvaluationWithNoAudit(RangerAccessRequest request, int policyType) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> RangerPolicyEngineImpl.zoneAwareAccessEvaluationWithNoAudit(" + request + ", policyType =" + policyType + ")");
		}

		RangerAccessResult ret = null;

		RangerPolicyRepository policyRepository = this.policyRepository;
		RangerPolicyRepository tagPolicyRepository = this.tagPolicyRepository;

		// Evaluate zone-name from request
		String zoneName = trieMap == null ? null : getMatchedZoneName(request.getResource());

		if (LOG.isDebugEnabled()) {
			LOG.debug("zoneName:[" + zoneName + "]");
		}

		if (StringUtils.isNotEmpty(zoneName)) {
			policyRepository = policyRepositories.get(zoneName);

			if (policyRepository == null) {
				LOG.error("policyRepository for zoneName:[" + zoneName + "] is null!! ERROR!");
			}
		}
		if (policyRepository != null) {
			ret = evaluatePoliciesNoAudit(request, policyType, zoneName, policyRepository, tagPolicyRepository);
			ret.setZoneName(zoneName);
		}


		if (LOG.isDebugEnabled()) {
			LOG.debug("<== RangerPolicyEngineImpl.zoneAwareAccessEvaluationWithNoAudit(" + request + ", policyType =" + policyType + "): " + ret);
		}
		return ret;
	}

	private RangerAccessResult evaluatePoliciesNoAudit(RangerAccessRequest request, int policyType, String zoneName, RangerPolicyRepository policyRepository, RangerPolicyRepository tagPolicyRepository) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> RangerPolicyEngineImpl.evaluatePoliciesNoAudit(" + request + ", policyType =" + policyType + ", zoneName=" + zoneName + ")");
		}

		RangerAccessResult ret = createAccessResult(request, policyType);
		Date accessTime = request.getAccessTime() != null ? request.getAccessTime() : new Date();

        if (ret != null && request != null) {

			evaluateTagPolicies(request, policyType, zoneName, tagPolicyRepository, ret);

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
			boolean evaluateResourcePolicies = hasResourcePolicies(policyRepository);

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
						ret.setPolicyVersion(evaluator.getPolicy().getVersion());
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
			LOG.debug("<== RangerPolicyEngineImpl.evaluatePoliciesNoAudit(" + request + ", policyType =" + policyType + ", zoneName=" + zoneName + "): " + ret);
		}

		return ret;
	}

	private void evaluateTagPolicies(final RangerAccessRequest request, int policyType, String zoneName, RangerPolicyRepository tagPolicyRepository, RangerAccessResult result) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> RangerPolicyEngineImpl.evaluateTagPolicies(" + request + ", policyType =" + policyType + ", zoneName=" + zoneName + ", " + result + ")");
		}

		Date accessTime = request.getAccessTime() != null ? request.getAccessTime() : new Date();

		Set<RangerTagForEval> tags = RangerAccessRequestUtil.getRequestTagsFromContext(request.getContext());

		List<PolicyEvaluatorForTag> policyEvaluators = tagPolicyRepository == null ? null : tagPolicyRepository.getLikelyMatchPolicyEvaluators(tags, policyType, accessTime);

		if (CollectionUtils.isNotEmpty(policyEvaluators)) {
			for (PolicyEvaluatorForTag policyEvaluator : policyEvaluators) {
				RangerPolicyEvaluator evaluator = policyEvaluator.getEvaluator();

				String policyZoneName = evaluator.getPolicy().getZoneName();
				if (!StringUtils.equals(zoneName, policyZoneName)) {
					if (LOG.isDebugEnabled()) {
						LOG.debug("Tag policy does not belong to the zone:[" + zoneName + "] of the accessed resource. Not evaluating this policy:[" + evaluator.getPolicy() + "]");
					}
					continue;
				}

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
			LOG.debug("<== RangerPolicyEngineImpl.evaluateTagPolicies(" + request + ", policyType =" + policyType + ", zoneName=" + zoneName + ", " + result + ")");
		}
	}

	private String getServiceName() {
		return policyRepository.getServiceName();
	}

	private RangerPolicyEvaluator getPolicyEvaluator(Long id) {
		RangerPolicyEvaluator ret = policyRepository.getPolicyEvaluator(id);
		if (ret == null && tagPolicyRepository != null) {
			ret = tagPolicyRepository.getPolicyEvaluator(id);
		}
		return ret;
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

	private boolean hasTagPolicies(RangerPolicyRepository tagPolicyRepository) {
		return tagPolicyRepository != null && CollectionUtils.isNotEmpty(tagPolicyRepository.getPolicies());
	}

	private boolean hasResourcePolicies(RangerPolicyRepository policyRepository) {
		return policyRepository != null && CollectionUtils.isNotEmpty(policyRepository.getPolicies());
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

	private void buildZoneTrie(ServicePolicies servicePolicies) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> RangerPolicyEngineImpl.buildZoneTrie()");
        }

        Map<String, ServicePolicies.SecurityZoneInfo> securityZones = servicePolicies.getSecurityZones();

        if (MapUtils.isNotEmpty(securityZones)) {
            RangerServiceDef                serviceDef = servicePolicies.getServiceDef();
			List<RangerZoneResourceMatcher> matchers   = new ArrayList<>();

			for (Map.Entry<String, ServicePolicies.SecurityZoneInfo> securityZone : securityZones.entrySet()) {
                String                           zoneName    = securityZone.getKey();
                ServicePolicies.SecurityZoneInfo zoneDetails = securityZone.getValue();

                if (LOG.isDebugEnabled()) {
                    LOG.debug("Building matchers for zone:[" + zoneName +"]");
                }

                for (Map<String, List<String>> resource : zoneDetails.getResources()) {

                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Building matcher for resource:[" + resource + "] in zone:[" + zoneName +"]");
                    }

                    Map<String, RangerPolicy.RangerPolicyResource> policyResources = new HashMap<>();

                    for (Map.Entry<String, List<String>> entry : resource.entrySet()) {
                        String resourceDefName = entry.getKey();
                        List<String> resourceValues = entry.getValue();

                        RangerPolicy.RangerPolicyResource policyResource = new RangerPolicy.RangerPolicyResource();

                        policyResource.setIsExcludes(false);
                        policyResource.setIsRecursive(StringUtils.equals(serviceDef.getName(), EmbeddedServiceDefsUtil.EMBEDDED_SERVICEDEF_HDFS_NAME));
                        policyResource.setValues(resourceValues);
                        policyResources.put(resourceDefName, policyResource);
                    }

                    matchers.add(new RangerZoneResourceMatcher(zoneName, policyResources, serviceDef));

                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Built matcher for resource:[" + resource +"] in zone:[" + zoneName + "]");
                    }
                }

                if (LOG.isDebugEnabled()) {
                    LOG.debug("Built all matchers for zone:[" + zoneName +"]");
                }
            }

            if (LOG.isDebugEnabled()) {
                LOG.debug("Built matchers for all Zones");
            }

			trieMap = new HashMap<>();

			for (RangerServiceDef.RangerResourceDef resourceDef : serviceDef.getResources()) {
            	trieMap.put(resourceDef.getName(), new RangerResourceTrie<>(resourceDef, matchers));
            }

        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== RangerPolicyEngineImpl.buildZoneTrie()");
        }
    }

    @Override
    public String getMatchedZoneName(GrantRevokeRequest grantRevokeRequest) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> RangerPolicyEngineImpl.getMatchedZoneName(" + grantRevokeRequest + ")");
        }

        String ret = null;

        if (this.trieMap != null) {
            Map<String, ? extends Object> resource             = grantRevokeRequest.getResource();
            Map<String, List<String>>     resourceForZoneMatch = convertFromSingleResource(resource);
            RangerAccessResource          accessResource       = convertToAccessResource(resource);

            ret = getMatchedZoneName(resourceForZoneMatch, accessResource);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== RangerPolicyEngineImpl.getMatchedZoneName(" + grantRevokeRequest + ") : " + ret);
        }

        return ret;
    }

    private String getMatchedZoneName(RangerAccessResource accessResource) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> RangerPolicyEngineImpl.getMatchedZoneName(" + accessResource + ")");
        }

        String ret = null;

        if (this.trieMap != null) {
            Map<String, List<String>> resource = convertFromAccessResource(accessResource);

            ret = getMatchedZoneName(resource, accessResource);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== RangerPolicyEngineImpl.getMatchedZoneName(" + accessResource + ") : " + ret);
        }

        return ret;
    }

    private String getMatchedZoneName(Map<String, List<String>> resource, RangerAccessResource accessResource) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> RangerPolicyEngineImpl.getMatchedZoneName(" + resource + ", " + accessResource + ")");
        }

        String ret = null;

        if (this.trieMap != null) {

            List<List<RangerZoneResourceMatcher>> zoneMatchersList = null;
            List<RangerZoneResourceMatcher>       smallestList     = null;

            for (Map.Entry<String, List<String>> entry : resource.entrySet()) {
                String       resourceDefName = entry.getKey();
                List<String> resourceValues  = entry.getValue();

                RangerResourceTrie<RangerZoneResourceMatcher> trie         = trieMap.get(resourceDefName);

                if (trie == null) {
                    continue;
                }

                List<RangerZoneResourceMatcher>               matchedZones = trie.getEvaluatorsForResource(resourceValues);

                if (LOG.isDebugEnabled()) {
                    LOG.debug("ResourceDefName:[" + resourceDefName + "], values:[" + resourceValues + "], matched-zones:[" + matchedZones + "]");
                }
                if (CollectionUtils.isEmpty(matchedZones)) { // no policies for this resource, bail out
                    zoneMatchersList = null;
                    smallestList     = null;
                    break;
                }

                if (smallestList == null) {
                    smallestList = matchedZones;
                } else {
                    if (zoneMatchersList == null) {
                        zoneMatchersList = new ArrayList<>();
                        zoneMatchersList.add(smallestList);
                    }
                    zoneMatchersList.add(matchedZones);

                    if (smallestList.size() > matchedZones.size()) {
                        smallestList = matchedZones;
                    }
                }
            }
            if (smallestList != null) {

                final List<RangerZoneResourceMatcher> intersection;

                if (zoneMatchersList != null) {
                    intersection = new ArrayList<>(smallestList);
                    for (List<RangerZoneResourceMatcher> zoneMatchers : zoneMatchersList) {
                        if (zoneMatchers != smallestList) {
                            // remove zones from intersection that are not in zoneMatchers
                            intersection.retainAll(zoneMatchers);
                            if (CollectionUtils.isEmpty(intersection)) { // if no zoneMatcher exists, bail out and return empty list
                                break;
                            }
                        }
                    }
                } else {
                    intersection = smallestList;
                }

                if (LOG.isDebugEnabled()) {
                    LOG.debug("Resource:[" + resource + "], matched-zones:[" + intersection + "]");
                }

                if (intersection.size() > 0) {
                    Set<String> matchedZoneNames = new HashSet<>();

                    for (RangerZoneResourceMatcher zoneMatcher : intersection) {
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("Trying to match resource:[" + accessResource + "] using zoneMatcher:[" + zoneMatcher + "]");
                        }
                        // These are potential matches. Try to really match them
                        if (zoneMatcher.getPolicyResourceMatcher().isMatch(accessResource, RangerPolicyResourceMatcher.MatchScope.ANY, null)) {
                            if (LOG.isDebugEnabled()) {
                                LOG.debug("Matched resource:[" + accessResource + "] using zoneMatcher:[" + zoneMatcher + "]");
                            }
                            // Actual match happened
                            matchedZoneNames.add(zoneMatcher.getSecurityZoneName());
                        } else {
                            if (LOG.isDebugEnabled()) {
                                LOG.debug("Did not match resource:[" + accessResource + "] using zoneMatcher:[" + zoneMatcher + "]");
                            }
                        }
                    }
                    LOG.info("The following zone-names matched resource:[" + accessResource + "]: " + matchedZoneNames);

                    if (matchedZoneNames.size() == 1) {
                        String[] zones = new String[1];
                        matchedZoneNames.toArray(zones);
                        ret = zones[0];
                    } else {
                        LOG.error("Internal error, multiple zone-names are matched. The following zone-names matched resource:[" + resource + "]: " + matchedZoneNames);
                    }
                }
            }
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("<== RangerPolicyEngineImpl.getMatchedZoneName(" + resource + ", " + accessResource + ") : " + ret);
        }
        return ret;
    }

    private static Map<String, List<String>> convertFromAccessResource(RangerAccessResource accessResource) {
        return convertFromSingleResource(accessResource.getAsMap());
    }

    private static Map<String, List<String>> convertFromSingleResource(Map<String, ? extends Object> resource) {

        Map<String, List<String>> ret = new HashMap<>();

        for (Map.Entry<String, ? extends Object> entry : resource.entrySet()) {
            List<String> value;

            if (entry.getValue() instanceof Collection) {
                value = (List<String>) entry.getValue();
            } else if (entry.getValue() instanceof String) {
                value = new ArrayList<>();
                value.add((String) entry.getValue());
            } else {
                LOG.error("access-resource contains value of unknown type : [" + entry.getValue().getClass().getCanonicalName() + "]");
                value = new ArrayList<>();
            }

            ret.put(entry.getKey(), value);
        }

        return ret;
    }

    private RangerAccessResource convertToAccessResource(Map<String, ? extends Object> resource) {

        RangerAccessResourceImpl ret = new RangerAccessResourceImpl();

        ret.setServiceDef(getServiceDef());

        for (Map.Entry<String, ? extends Object> entry : resource.entrySet()) {
            ret.setValue(entry.getKey(), entry.getValue());
        }

        return ret;
    }
}
