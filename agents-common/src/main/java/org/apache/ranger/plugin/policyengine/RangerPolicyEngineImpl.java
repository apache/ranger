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
import org.apache.ranger.plugin.contextenricher.RangerTagForEval;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.policyevaluator.RangerPolicyEvaluator;
import org.apache.ranger.plugin.policyevaluator.RangerPolicyEvaluator.PolicyACLSummary;
import org.apache.ranger.plugin.policyresourcematcher.RangerPolicyResourceMatcher;
import org.apache.ranger.plugin.service.RangerDefaultRequestProcessor;
import org.apache.ranger.plugin.util.GrantRevokeRequest;
import org.apache.ranger.plugin.util.RangerAccessRequestUtil;
import org.apache.ranger.plugin.util.RangerPerfTracer;
import org.apache.ranger.plugin.util.RangerRoles;
import org.apache.ranger.plugin.util.ServicePolicies;

import java.util.ArrayList;
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

	private static final Log PERF_POLICYENGINE_REQUEST_LOG  = RangerPerfTracer.getPerfLogger("policyengine.request");
	private static final Log PERF_POLICYENGINE_AUDIT_LOG    = RangerPerfTracer.getPerfLogger("policyengine.audit");
	private static final Log PERF_POLICYENGINE_GET_ACLS_LOG = RangerPerfTracer.getPerfLogger("policyengine.getResourceACLs");

	private final PolicyEngine                 policyEngine;
	private final RangerAccessRequestProcessor requestProcessor;

	static public RangerPolicyEngine getPolicyEngine(final RangerPolicyEngineImpl other, final ServicePolicies servicePolicies) {
		RangerPolicyEngine ret = null;

		if (other != null && servicePolicies != null) {
			PolicyEngine policyEngine = other.policyEngine.cloneWithDelta(servicePolicies);
			if (policyEngine != null) {
				ret = new RangerPolicyEngineImpl(policyEngine);
			}
		}
		return ret;
	}

	public RangerPolicyEngineImpl(String appId, ServicePolicies servicePolicies, RangerPolicyEngineOptions options, RangerPluginContext rangerPluginContext, RangerRoles rangerRoles) {
		policyEngine = new PolicyEngine(appId, servicePolicies, options, rangerPluginContext, rangerRoles);
		policyEngine.getPluginContext().getAuthContext().setRangerRoles(rangerRoles);
		this.requestProcessor = new RangerDefaultRequestProcessor(policyEngine);
	}

	@Override
	public String toString() {
		return policyEngine.toString();
	}

	@Override
	public RangerAccessResult evaluatePolicies(RangerAccessRequest request, int policyType, RangerAccessResultProcessor resultProcessor) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> RangerPolicyEngineImpl.evaluatePolicies(" + request + ", policyType=" + policyType + ")");
		}
		RangerPerfTracer perf = null;

		if(RangerPerfTracer.isPerfTraceEnabled(PERF_POLICYENGINE_REQUEST_LOG)) {
			String requestHashCode = Integer.toHexString(System.identityHashCode(request)) + "_" + policyType;
			perf = RangerPerfTracer.getPerfTracer(PERF_POLICYENGINE_REQUEST_LOG, "RangerPolicyEngine.evaluatePolicies(requestHashCode=" + requestHashCode + ")");
			LOG.info("RangerPolicyEngineImpl.evaluatePolicies(" + requestHashCode + ", " + request + ")");
		}
		requestProcessor.preProcess(request);

		RangerAccessResult ret = zoneAwareAccessEvaluationWithNoAudit(request, policyType);

		if (resultProcessor != null) {

			RangerPerfTracer perfAuditTracer = null;
			if(RangerPerfTracer.isPerfTraceEnabled(PERF_POLICYENGINE_AUDIT_LOG)) {
				String requestHashCode = Integer.toHexString(System.identityHashCode(request)) + "_" + policyType;
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
				requestProcessor.preProcess(request);

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

		requestProcessor.preProcess(request);

		String zoneName = policyEngine.getMatchedZoneName(request.getResource());

		if (LOG.isDebugEnabled()) {
			LOG.debug("zoneName:[" + zoneName + "]");
		}

		final RangerPolicyRepository matchedRepository;

		if (StringUtils.isNotEmpty(zoneName)) {
			matchedRepository = policyEngine.getZonePolicyRepositories().get(zoneName);
		} else {
			matchedRepository = policyEngine.getPolicyRepository();
		}

		if (matchedRepository == null) {
			LOG.error("policyRepository for zoneName:[" + zoneName + "],  serviceName:[" + policyEngine.getPolicyRepository().getServiceName() + "], policyVersion:[" + getPolicyVersion() + "] is null!! ERROR!");
		} else {

			List<RangerPolicyEvaluator> allEvaluators = new ArrayList<>();
			Map<Long, RangerPolicyResourceMatcher.MatchType> tagMatchTypeMap = null;
			Set<Long> policyIdForTemporalTags = null;

			Set<RangerTagForEval> tags = RangerAccessRequestUtil.getRequestTagsFromContext(request.getContext());
			List<PolicyEvaluatorForTag> tagPolicyEvaluators = policyEngine.getTagPolicyRepository() == null ? null : policyEngine.getTagPolicyRepository().getLikelyMatchPolicyEvaluators(tags, RangerPolicy.POLICY_TYPE_ACCESS, null);

			if (CollectionUtils.isNotEmpty(tagPolicyEvaluators)) {
				tagMatchTypeMap = new HashMap<>();

				final boolean useTagPoliciesFromDefaultZone = !policyEngine.isResourceZoneAssociatedWithTagService(zoneName);

				for (PolicyEvaluatorForTag tagEvaluator : tagPolicyEvaluators) {
					RangerPolicyEvaluator evaluator = tagEvaluator.getEvaluator();
					String policyZoneName = evaluator.getPolicy().getZoneName();
					if (useTagPoliciesFromDefaultZone) {
						if (StringUtils.isNotEmpty(policyZoneName)) {
							if (LOG.isDebugEnabled()) {
								LOG.debug("Tag policy [zone:" + policyZoneName + "] does not belong to default zone. Not evaluating this policy:[" + evaluator.getPolicy() + "]");
							}
							continue;
						}
					} else {
						if (!StringUtils.equals(zoneName, policyZoneName)) {
							if (LOG.isDebugEnabled()) {
								LOG.debug("Tag policy [zone:" + policyZoneName + "] does not belong to the zone:[" + zoneName + "] of the accessed resource. Not evaluating this policy:[" + evaluator.getPolicy() + "]");
							}
							continue;
						}
					}
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

			List<RangerPolicyEvaluator> resourcePolicyEvaluators = matchedRepository.getLikelyMatchPolicyEvaluators(request.getResource(), RangerPolicy.POLICY_TYPE_ACCESS);

			allEvaluators.addAll(resourcePolicyEvaluators);

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
						isMatched = matchType == RangerPolicyResourceMatcher.MatchType.SELF || matchType == RangerPolicyResourceMatcher.MatchType.ANCESTOR_WITH_WILDCARDS;
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

						for (Map.Entry<String, Map<String, PolicyACLSummary.AccessResult>> roleAccessInfo : aclSummary.getRolesAccessInfo().entrySet()) {
							final String roleName = roleAccessInfo.getKey();

							for (Map.Entry<String, PolicyACLSummary.AccessResult> accessInfo : roleAccessInfo.getValue().entrySet()) {
								if (isConditional) {
									accessResult = ACCESS_CONDITIONAL;
								} else {
									accessResult = accessInfo.getValue().getResult();
									if (accessResult.equals(RangerPolicyEvaluator.ACCESS_UNDETERMINED)) {
										accessResult = RangerPolicyEvaluator.ACCESS_DENIED;
									}
								}
								RangerPolicy policy = evaluator.getPolicy();
								ret.setRoleAccessInfo(roleName, accessInfo.getKey(), accessResult, policy);
							}
						}
					}
				}

				ret.finalizeAcls();
			}
		}

		RangerPerfTracer.logAlways(perf);

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== RangerPolicyEngineImpl.getResourceACLs(request=" + request + ") : ret=" + ret);
		}

		return ret;
	}

	// This API is used only used by test code
	@Override
	public RangerResourceAccessInfo getResourceAccessInfo(RangerAccessRequest request) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerPolicyEngineImpl.getResourceAccessInfo(" + request + ")");
		}

		requestProcessor.preProcess(request);

		RangerResourceAccessInfo ret = new RangerResourceAccessInfo(request);

		String zoneName = policyEngine.getMatchedZoneName(request.getResource());

		if (LOG.isDebugEnabled()) {
			LOG.debug("zoneName:[" + zoneName + "]");
		}

		final RangerPolicyRepository matchedRepository;

		if (StringUtils.isNotEmpty(zoneName)) {
			matchedRepository = policyEngine.getZonePolicyRepositories().get(zoneName);
		} else {
			matchedRepository = policyEngine.getPolicyRepository();
		}

		if (matchedRepository == null) {
			LOG.error("policyRepository for zoneName:[" + zoneName + "],  serviceName:[" + policyEngine.getPolicyRepository().getServiceName() + "], policyVersion:[" + getPolicyVersion() + "] is null!! ERROR!");
		} else {

			List<RangerPolicyEvaluator> tagPolicyEvaluators = policyEngine.getTagPolicyRepository() == null ? null : policyEngine.getTagPolicyRepository().getPolicyEvaluators();

			if (CollectionUtils.isNotEmpty(tagPolicyEvaluators)) {

				Set<RangerTagForEval> tags = RangerAccessRequestUtil.getRequestTagsFromContext(request.getContext());

				if (CollectionUtils.isNotEmpty(tags)) {

					final boolean useTagPoliciesFromDefaultZone = !policyEngine.isResourceZoneAssociatedWithTagService(zoneName);

					for (RangerTagForEval tag : tags) {
						RangerAccessRequest tagEvalRequest = new RangerTagAccessRequest(tag, policyEngine.getTagPolicyRepository().getServiceDef(), request);

						List<RangerPolicyEvaluator> evaluators = policyEngine.getTagPolicyRepository().getLikelyMatchPolicyEvaluators(tagEvalRequest.getResource(), RangerPolicy.POLICY_TYPE_ACCESS);

						for (RangerPolicyEvaluator evaluator : evaluators) {
							String policyZoneName = evaluator.getPolicy().getZoneName();
							if (useTagPoliciesFromDefaultZone) {
								if (StringUtils.isNotEmpty(policyZoneName)) {
									if (LOG.isDebugEnabled()) {
										LOG.debug("Tag policy [zone:" + policyZoneName + "] does not belong to default zone. Not evaluating this policy:[" + evaluator.getPolicy() + "]");
									}
									continue;
								}
							} else {
								if (!StringUtils.equals(zoneName, policyZoneName)) {
									if (LOG.isDebugEnabled()) {
										LOG.debug("Tag policy [zone:" + policyZoneName + "] does not belong to the zone:[" + zoneName + "] of the accessed resource. Not evaluating this policy:[" + evaluator.getPolicy() + "]");
									}
									continue;
								}
							}
							evaluator.getResourceAccessInfo(tagEvalRequest, ret);
						}
					}
				}
			}

			List<RangerPolicyEvaluator> resPolicyEvaluators = matchedRepository.getLikelyMatchPolicyEvaluators(request.getResource(), RangerPolicy.POLICY_TYPE_ACCESS);

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

	@Override
	public String getMatchedZoneName(GrantRevokeRequest grantRevokeRequest) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> RangerPolicyEngineImpl.getMatchedZoneName(" + grantRevokeRequest + ")");
		}

		String ret = policyEngine.getMatchedZoneName(grantRevokeRequest.getResource());

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== RangerPolicyEngineImpl.getMatchedZoneName(" + grantRevokeRequest + ") : " + ret);
		}

		return ret;
	}

	@Override
	public void setUseForwardedIPAddress(boolean useForwardedIPAddress) {
		policyEngine.setUseForwardedIPAddress(useForwardedIPAddress);
	}

	@Override
	public void setTrustedProxyAddresses(String[] trustedProxyAddresses) {
		policyEngine.setTrustedProxyAddresses(trustedProxyAddresses);
	}

	@Override
	public RangerServiceDef getServiceDef() {
		return policyEngine.getServiceDef();
	}

	@Override
	public long getPolicyVersion() {
		return policyEngine.getPolicyVersion();
	}

	@Override
	public long getRoleVersion() { return policyEngine.getRoleVersion(); }

	@Override
	public void setRangerRoles(RangerRoles rangerRoles) {
		policyEngine.setRangerRoles(rangerRoles);
	}

	@Override
	public Set<String> getRolesFromUserAndGroups(String user, Set<String> groups) {
		return policyEngine.getPluginContext().getAuthContext().getRolesForUserAndGroups(user, groups);
	}

	@Override
	public List<RangerPolicy> getResourcePolicies(String zoneName) {
		return policyEngine.getResourcePolicies(zoneName);
	}

	@Override
	public List<RangerPolicy> getResourcePolicies() {
		return policyEngine.getResourcePolicies();
	}

	@Override
	public List<RangerPolicy> getTagPolicies() {
		return policyEngine.getTagPolicies();
	}

	public void releaseResources() {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> RangerPolicyEngineImpl.releaseResources()");
		}
		PolicyEngine policyEngine = this.policyEngine;
		if (policyEngine != null) {
			policyEngine.preCleanup();
		} else {
			if (LOG.isDebugEnabled()) {
				LOG.debug("Cannot preCleanup policy-engine as it is null!");
			}
		}
		if (LOG.isDebugEnabled()) {
			LOG.debug("<== RangerPolicyEngineImpl.releaseResources()");
		}
	}

	public boolean compare(RangerPolicyEngineImpl other) {
		return policyEngine.compare(other.policyEngine);
	}

	private RangerPolicyEngineImpl(final PolicyEngine policyEngine) {
		this.policyEngine = policyEngine;
		this.requestProcessor = new RangerDefaultRequestProcessor(policyEngine);
	}

	private RangerAccessResult zoneAwareAccessEvaluationWithNoAudit(RangerAccessRequest request, int policyType) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> RangerPolicyEngineImpl.zoneAwareAccessEvaluationWithNoAudit(" + request + ", policyType =" + policyType + ")");
		}

		RangerAccessResult ret = null;

		RangerPolicyRepository policyRepository = policyEngine.getPolicyRepository();
		RangerPolicyRepository tagPolicyRepository = policyEngine.getTagPolicyRepository();

		// Evaluate zone-name from request
		String zoneName = policyEngine.getMatchedZoneName(request.getResource());

		if (LOG.isDebugEnabled()) {
			LOG.debug("zoneName:[" + zoneName + "]");
		}

		if (StringUtils.isNotEmpty(zoneName)) {
			policyRepository = policyEngine.getZonePolicyRepositories().get(zoneName);

			if (policyRepository == null) {
				LOG.error("policyRepository for zoneName:[" + zoneName + "],  serviceName:[" + policyEngine.getPolicyRepository().getServiceName() + "], policyVersion:[" + getPolicyVersion() + "] is null!! ERROR!");
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

		Date accessTime = request.getAccessTime() != null ? request.getAccessTime() : new Date();
		RangerAccessResult ret = policyEngine.createAccessResult(request, policyType);


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
		boolean evaluateResourcePolicies = policyEngine.hasResourcePolicies(policyRepository);

		if (evaluateResourcePolicies) {
			boolean findAuditByResource = !ret.getIsAuditedDetermined();
			boolean foundInCache = findAuditByResource && policyRepository.setAuditEnabledFromCache(request, ret);

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

			if (ret.getIsAllowed()) {
				ret.setIsAccessDetermined(true);
			}

			if (findAuditByResource && !foundInCache) {
				policyRepository.storeAuditEnabledInCache(request, ret);
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
			final boolean useTagPoliciesFromDefaultZone = !policyEngine.isResourceZoneAssociatedWithTagService(zoneName);

			for (PolicyEvaluatorForTag policyEvaluator : policyEvaluators) {
				RangerPolicyEvaluator evaluator = policyEvaluator.getEvaluator();

				String policyZoneName = evaluator.getPolicy().getZoneName();
				if (useTagPoliciesFromDefaultZone) {
					if (StringUtils.isNotEmpty(policyZoneName)) {
						if (LOG.isDebugEnabled()) {
							LOG.debug("Tag policy [zone:" + policyZoneName + "] does not belong to default zone. Not evaluating this policy:[" + evaluator.getPolicy() + "]");
						}
						continue;
					}
				} else {
					if (!StringUtils.equals(zoneName, policyZoneName)) {
						if (LOG.isDebugEnabled()) {
							LOG.debug("Tag policy [zone:" + policyZoneName + "] does not belong to the zone:[" + zoneName + "] of the accessed resource. Not evaluating this policy:[" + evaluator.getPolicy() + "]");
						}
						continue;
					}
				}

				RangerTagForEval tag = policyEvaluator.getTag();

				RangerAccessRequest tagEvalRequest = new RangerTagAccessRequest(tag, tagPolicyRepository.getServiceDef(), request);
				RangerAccessResult tagEvalResult = policyEngine.createAccessResult(tagEvalRequest, policyType);

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

}
