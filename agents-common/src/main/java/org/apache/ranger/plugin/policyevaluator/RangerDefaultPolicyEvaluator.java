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

package org.apache.ranger.plugin.policyevaluator;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerPolicy.RangerDataMaskPolicyItem;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItem;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItemAccess;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItemDataMaskInfo;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItemRowFilterInfo;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyResource;
import org.apache.ranger.plugin.model.RangerPolicy.RangerRowFilterPolicyItem;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.model.RangerServiceDef.RangerAccessTypeDef;
import org.apache.ranger.plugin.policyengine.RangerAccessRequest;
import org.apache.ranger.plugin.policyengine.RangerAccessResource;
import org.apache.ranger.plugin.policyengine.RangerAccessResult;
import org.apache.ranger.plugin.policyengine.RangerDataMaskResult;
import org.apache.ranger.plugin.policyengine.RangerPolicyEngineOptions;
import org.apache.ranger.plugin.policyengine.RangerResourceAccessInfo;
import org.apache.ranger.plugin.policyengine.RangerRowFilterResult;
import org.apache.ranger.plugin.policyresourcematcher.RangerDefaultPolicyResourceMatcher;
import org.apache.ranger.plugin.policyresourcematcher.RangerPolicyResourceMatcher;
import org.apache.ranger.plugin.util.RangerPerfTracer;
import org.apache.ranger.plugin.util.ServiceDefUtil;


public class RangerDefaultPolicyEvaluator extends RangerAbstractPolicyEvaluator {
	private static final Log LOG = LogFactory.getLog(RangerDefaultPolicyEvaluator.class);

	private static final Log PERF_POLICY_INIT_LOG = RangerPerfTracer.getPerfLogger("policy.init");
	private static final Log PERF_POLICY_REQUEST_LOG = RangerPerfTracer.getPerfLogger("policy.request");

	private RangerPolicyResourceMatcher     resourceMatcher          = null;
	private List<RangerPolicyItemEvaluator> allowEvaluators          = null;
	private List<RangerPolicyItemEvaluator> denyEvaluators           = null;
	private List<RangerPolicyItemEvaluator> allowExceptionEvaluators = null;
	private List<RangerPolicyItemEvaluator> denyExceptionEvaluators  = null;
	private int                             customConditionsCount    = 0;
	private List<RangerDataMaskPolicyItemEvaluator>  dataMaskEvaluators  = null;
	private List<RangerRowFilterPolicyItemEvaluator> rowFilterEvaluators = null;
	private String perfTag;

	@Override
	public int getCustomConditionsCount() {
		return customConditionsCount;
	}

	@Override
	public void init(RangerPolicy policy, RangerServiceDef serviceDef, RangerPolicyEngineOptions options) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerDefaultPolicyEvaluator.init()");
		}

		StringBuilder perfTagBuffer = new StringBuilder();
		if (policy != null) {
			perfTagBuffer.append("policyId=").append(policy.getId()).append(", policyName=").append(policy.getName());
		}

		perfTag = perfTagBuffer.toString();

		RangerPerfTracer perf = null;

		if(RangerPerfTracer.isPerfTraceEnabled(PERF_POLICY_INIT_LOG)) {
			perf = RangerPerfTracer.getPerfTracer(PERF_POLICY_INIT_LOG, "RangerPolicyEvaluator.init(" + perfTag + ")");
		}

		super.init(policy, serviceDef, options);

		preprocessPolicy(policy, serviceDef);

		resourceMatcher = new RangerDefaultPolicyResourceMatcher();

		resourceMatcher.setServiceDef(serviceDef);
		resourceMatcher.setPolicy(policy);
		resourceMatcher.init();

		if(policy != null) {
			allowEvaluators          = createPolicyItemEvaluators(policy, serviceDef, options, RangerPolicyItemEvaluator.POLICY_ITEM_TYPE_ALLOW);
			denyEvaluators           = createPolicyItemEvaluators(policy, serviceDef, options, RangerPolicyItemEvaluator.POLICY_ITEM_TYPE_DENY);
			allowExceptionEvaluators = createPolicyItemEvaluators(policy, serviceDef, options, RangerPolicyItemEvaluator.POLICY_ITEM_TYPE_ALLOW_EXCEPTIONS);
			denyExceptionEvaluators  = createPolicyItemEvaluators(policy, serviceDef, options, RangerPolicyItemEvaluator.POLICY_ITEM_TYPE_DENY_EXCEPTIONS);
			dataMaskEvaluators       = createDataMaskPolicyItemEvaluators(policy, serviceDef, options, policy.getDataMaskPolicyItems());
			rowFilterEvaluators      = createRowFilterPolicyItemEvaluators(policy, serviceDef, options, policy.getRowFilterPolicyItems());
		} else {
			allowEvaluators          = Collections.<RangerPolicyItemEvaluator>emptyList();
			denyEvaluators           = Collections.<RangerPolicyItemEvaluator>emptyList();
			allowExceptionEvaluators = Collections.<RangerPolicyItemEvaluator>emptyList();
			denyExceptionEvaluators  = Collections.<RangerPolicyItemEvaluator>emptyList();
			dataMaskEvaluators       = Collections.<RangerDataMaskPolicyItemEvaluator>emptyList();
			rowFilterEvaluators      = Collections.<RangerRowFilterPolicyItemEvaluator>emptyList();
		}

		Collections.sort(allowEvaluators);
		Collections.sort(denyEvaluators);
		Collections.sort(allowExceptionEvaluators);
		Collections.sort(denyExceptionEvaluators);

		/* dataMask, rowFilter policyItems must be evaulated in the order given in the policy; hence no sort
		Collections.sort(dataMaskEvaluators);
		Collections.sort(rowFilterEvaluators);
		*/

		RangerPerfTracer.log(perf);

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerDefaultPolicyEvaluator.init()");
		}
	}

    @Override
    public void evaluate(RangerAccessRequest request, RangerAccessResult result) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> RangerDefaultPolicyEvaluator.evaluate(" + request + ", " + result + ")");
        }

		RangerPerfTracer perf = null;

		if(RangerPerfTracer.isPerfTraceEnabled(PERF_POLICY_REQUEST_LOG)) {
			perf = RangerPerfTracer.getPerfTracer(PERF_POLICY_REQUEST_LOG, "RangerPolicyEvaluator.evaluate(requestHashCode=" + Integer.toHexString(System.identityHashCode(request)) + ","
					+ perfTag + ")");
		}

        if (request != null && result != null) {
            boolean isResourceMatch              = false;
            boolean isResourceHeadMatch          = false;
            boolean isResourceMatchAttempted     = false;
            boolean isResourceHeadMatchAttempted = false;
            final boolean attemptResourceHeadMatch = request.isAccessTypeAny() || request.getResourceMatchingScope() == RangerAccessRequest.ResourceMatchingScope.SELF_OR_DESCENDANTS;

            if (!result.getIsAuditedDetermined()) {
                // Need to match request.resource first. If it matches (or head matches), then only more progress can be made
                if (!isResourceMatchAttempted) {
                    isResourceMatch = isMatch(request.getResource());
                    isResourceMatchAttempted = true;
                }

                // Try head match only if match was not found and ANY access was requested
                if (!isResourceMatch) {
                    if (attemptResourceHeadMatch && !isResourceHeadMatchAttempted) {
                        isResourceHeadMatch = matchResourceHead(request.getResource());
                        isResourceHeadMatchAttempted = true;
                    }
                }

                if (isResourceMatch || isResourceHeadMatch) {
                    // We are done for determining if audit is needed for this policy
                    if (isAuditEnabled()) {
                        result.setIsAudited(true);
                    }
                }
            }

            if (!result.getIsAccessDetermined()) {
                // Attempt resource matching only if there may be a matchable policyItem
                if (hasMatchablePolicyItem(request)) {
                    // Try Match only if it was not attempted as part of evaluating Audit requirement
                    if (!isResourceMatchAttempted) {
                        isResourceMatch = isMatch(request.getResource());
                        isResourceMatchAttempted = true;
                    }

                    // Try Head Match only if no match was found so far AND a head match was not attempted as part of evaluating
                    // Audit requirement
                    if (!isResourceMatch) {
                        if (attemptResourceHeadMatch && !isResourceHeadMatchAttempted) {
                            isResourceHeadMatch = matchResourceHead(request.getResource());
                            isResourceHeadMatchAttempted = true;
                        }
                    }

                    // Go further to evaluate access only if match or head match was found at this point
                    if (isResourceMatch || isResourceHeadMatch) {
                        evaluatePolicyItems(request, result, isResourceMatch);
                    }
                }
            }
        }

		RangerPerfTracer.log(perf);

        if(LOG.isDebugEnabled()) {
            LOG.debug("<== RangerDefaultPolicyEvaluator.evaluate(" + request + ", " + result + ")");
        }
    }

	@Override
	public void evaluate(RangerAccessRequest request, RangerDataMaskResult result) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerDefaultPolicyEvaluator.evaluate(" + request + ", " + result + ")");
		}

		RangerPerfTracer perf = null;

		if(RangerPerfTracer.isPerfTraceEnabled(PERF_POLICY_REQUEST_LOG)) {
			perf = RangerPerfTracer.getPerfTracer(PERF_POLICY_REQUEST_LOG, "RangerPolicyEvaluator.evaluate(requestHashCode=" + Integer.toHexString(System.identityHashCode(request)) + "," + perfTag + ")");
		}

		if (request != null && result != null && CollectionUtils.isNotEmpty(dataMaskEvaluators)) {
			boolean isResourceMatchAttempted     = false;
			boolean isResourceMatch              = false;
			boolean isResourceHeadMatch          = false;
			boolean isResourceHeadMatchAttempted = false;
			final boolean attemptResourceHeadMatch = request.isAccessTypeAny() || request.getResourceMatchingScope() == RangerAccessRequest.ResourceMatchingScope.SELF_OR_DESCENDANTS;

			if (!result.getIsAuditedDetermined()) {
				if (!isResourceMatchAttempted) {
					isResourceMatch = isMatch(request.getResource());
					isResourceMatchAttempted = true;
				}

				if (!isResourceMatch) {
					if (attemptResourceHeadMatch && !isResourceHeadMatchAttempted) {
						isResourceHeadMatch = matchResourceHead(request.getResource());
						isResourceHeadMatchAttempted = true;
					}
				}

				if (isResourceMatch || isResourceHeadMatch) {
					if (isAuditEnabled()) {
						result.setIsAudited(true);
					}
				}
			}

			if (!result.getIsAccessDetermined()) {
				if (!isResourceMatchAttempted) {
					isResourceMatch = isMatch(request.getResource());
					isResourceMatchAttempted = true;
				}

				if (!isResourceMatch) {
					if (attemptResourceHeadMatch && !isResourceHeadMatchAttempted) {
						isResourceHeadMatch = matchResourceHead(request.getResource());
						isResourceHeadMatchAttempted = true;
					}
				}

				if (isResourceMatch || isResourceHeadMatch) {
					evaluatePolicyItems(request, result);
				}
			}
		}

		RangerPerfTracer.log(perf);

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerDefaultPolicyEvaluator.evaluate(" + request + ", " + result + ")");
		}
	}

	@Override
	public void evaluate(RangerAccessRequest request, RangerRowFilterResult result) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerDefaultPolicyEvaluator.evaluate(" + request + ", " + result + ")");
		}

		RangerPerfTracer perf = null;

		if(RangerPerfTracer.isPerfTraceEnabled(PERF_POLICY_REQUEST_LOG)) {
			perf = RangerPerfTracer.getPerfTracer(PERF_POLICY_REQUEST_LOG, "RangerPolicyEvaluator.evaluate(requestHashCode=" + Integer.toHexString(System.identityHashCode(request)) + "," + perfTag + ")");
		}

		if (request != null && result != null && CollectionUtils.isNotEmpty(rowFilterEvaluators)) {
			boolean isResourceMatchAttempted     = false;
			boolean isResourceMatch              = false;
			boolean isResourceHeadMatch          = false;
			boolean isResourceHeadMatchAttempted = false;
			final boolean attemptResourceHeadMatch = request.isAccessTypeAny() || request.getResourceMatchingScope() == RangerAccessRequest.ResourceMatchingScope.SELF_OR_DESCENDANTS;

			if (!result.getIsAuditedDetermined()) {
				if (!isResourceMatchAttempted) {
					isResourceMatch = isMatch(request.getResource());
					isResourceMatchAttempted = true;
				}

				if (!isResourceMatch) {
					if (attemptResourceHeadMatch && !isResourceHeadMatchAttempted) {
						isResourceHeadMatch = matchResourceHead(request.getResource());
						isResourceHeadMatchAttempted = true;
					}
				}

				if (isResourceMatch || isResourceHeadMatch) {
					if (isAuditEnabled()) {
						result.setIsAudited(true);
					}
				}
			}

			if (!result.getIsAccessDetermined()) {
				if (!isResourceMatchAttempted) {
					isResourceMatch = isMatch(request.getResource());
					isResourceMatchAttempted = true;
				}

				if (!isResourceMatch) {
					if (attemptResourceHeadMatch && !isResourceHeadMatchAttempted) {
						isResourceHeadMatch = matchResourceHead(request.getResource());
						isResourceHeadMatchAttempted = true;
					}
				}

				if (isResourceMatch || isResourceHeadMatch) {
					evaluatePolicyItems(request, result);
				}
			}
		}

		RangerPerfTracer.log(perf);

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerDefaultPolicyEvaluator.evaluate(" + request + ", " + result + ")");
		}
	}

	@Override
	public boolean isMatch(RangerAccessResource resource) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerDefaultPolicyEvaluator.isMatch(" + resource + ")");
		}

		boolean ret = false;

		RangerPerfTracer perf = null;

		if(RangerPerfTracer.isPerfTraceEnabled(PERF_POLICY_REQUEST_LOG)) {
			perf = RangerPerfTracer.getPerfTracer(PERF_POLICY_REQUEST_LOG, "RangerPolicyEvaluator.isMatch(resource=" + resource.getAsString() + "," + perfTag + ")");
		}

		if(resourceMatcher != null) {
			ret = resourceMatcher.isMatch(resource);
		}

		RangerPerfTracer.log(perf);

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerDefaultPolicyEvaluator.isMatch(" + resource + "): " + ret);
		}

		return ret;
	}

	@Override
	public boolean isCompleteMatch(RangerAccessResource resource) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerDefaultPolicyEvaluator.isCompleteMatch(" + resource + ")");
		}

		boolean ret = false;

		if(resourceMatcher != null) {
			ret = resourceMatcher.isCompleteMatch(resource);
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerDefaultPolicyEvaluator.isCompleteMatch(" + resource + "): " + ret);
		}

		return ret;
	}

	@Override
	public boolean isCompleteMatch(Map<String, RangerPolicyResource> resources) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerDefaultPolicyEvaluator.isCompleteMatch(" + resources + ")");
		}

		boolean ret = false;

		if(resourceMatcher != null) {
			ret = resourceMatcher.isCompleteMatch(resources);
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerDefaultPolicyEvaluator.isCompleteMatch(" + resources + "): " + ret);
		}

		return ret;
	}

	@Override
	public boolean isAccessAllowed(RangerAccessResource resource, String user, Set<String> userGroups, String accessType) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerDefaultPolicyEvaluator.isAccessAllowed(" + resource + ", " + user + ", " + userGroups + ", " + accessType + ")");
		}

		boolean ret = isAccessAllowed(user, userGroups, accessType) && isMatch(resource);
		
		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerDefaultPolicyEvaluator.isAccessAllowed(" + resource + ", " + user + ", " + userGroups + ", " + accessType + "): " + ret);
		}

		return ret;
	}

	@Override
	public boolean isAccessAllowed(Map<String, RangerPolicyResource> resources, String user, Set<String> userGroups, String accessType) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerDefaultPolicyEvaluator.isAccessAllowed(" + resources + ", " + user + ", " + userGroups + ", " + accessType + ")");
		}

		boolean ret = isAccessAllowed(user, userGroups, accessType) && isMatch(resources);
		
		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerDefaultPolicyEvaluator.isAccessAllowed(" + resources + ", " + user + ", " + userGroups + ", " + accessType + "): " + ret);
		}

		return ret;
	}

	@Override
	public void getResourceAccessInfo(RangerAccessRequest request, RangerResourceAccessInfo result) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerDefaultPolicyEvaluator.getResourceAccessInfo(" + request + ", " + result + ")");
		}

		final boolean isResourceMatch          = isMatch(request.getResource());
		final boolean attemptResourceHeadMatch = request.isAccessTypeAny() || request.getResourceMatchingScope() == RangerAccessRequest.ResourceMatchingScope.SELF_OR_DESCENDANTS;
		final boolean isResourceHeadMatch      = (!isResourceMatch && attemptResourceHeadMatch) ? matchResourceHead(request.getResource()) : false;

		if(isResourceMatch || isResourceHeadMatch) {
			if (CollectionUtils.isNotEmpty(allowEvaluators)) {
				Set<String> users = new HashSet<String>();
				Set<String> groups = new HashSet<String>();

				getResourceAccessInfo(request, allowEvaluators, users, groups);

				if (CollectionUtils.isNotEmpty(allowExceptionEvaluators)) {
					Set<String> exceptionUsers = new HashSet<String>();
					Set<String> exceptionGroups = new HashSet<String>();

					getResourceAccessInfo(request, allowExceptionEvaluators, exceptionUsers, exceptionGroups);

					users.removeAll(exceptionUsers);
					groups.removeAll(exceptionGroups);
				}

				result.getAllowedUsers().addAll(users);
				result.getAllowedGroups().addAll(groups);
			}
		}

		if(isResourceMatch) {
			if(CollectionUtils.isNotEmpty(denyEvaluators)) {
				Set<String> users  = new HashSet<String>();
				Set<String> groups = new HashSet<String>();

				getResourceAccessInfo(request, denyEvaluators, users, groups);

				if(CollectionUtils.isNotEmpty(denyExceptionEvaluators)) {
					Set<String> exceptionUsers  = new HashSet<String>();
					Set<String> exceptionGroups = new HashSet<String>();

					getResourceAccessInfo(request, denyExceptionEvaluators, exceptionUsers, exceptionGroups);

					users.removeAll(exceptionUsers);
					groups.removeAll(exceptionGroups);
				}

				result.getDeniedUsers().addAll(users);
				result.getDeniedGroups().addAll(groups);
			}
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerDefaultPolicyEvaluator.getResourceAccessInfo(" + request + ", " + result + ")");
		}
	}


	protected void evaluatePolicyItems(RangerAccessRequest request, RangerAccessResult result, boolean isResourceMatch) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerDefaultPolicyEvaluator.evaluatePolicyItems(" + request + ", " + result + ", " + isResourceMatch + ")");
		}

		RangerPolicyItemEvaluator matchedPolicyItem = getMatchingPolicyItem(request, denyEvaluators, denyExceptionEvaluators);

		if(matchedPolicyItem == null && !result.getIsAllowed()) { // if not denied, evaluate allowItems only if not already allowed
			matchedPolicyItem = getMatchingPolicyItem(request, allowEvaluators, allowExceptionEvaluators);
		}

		if(matchedPolicyItem != null) {
			RangerPolicy policy = getPolicy();

			if(matchedPolicyItem.getPolicyItemType() == RangerPolicyItemEvaluator.POLICY_ITEM_TYPE_DENY) {
				if(isResourceMatch) {
					result.setIsAllowed(false);
					result.setPolicyId(policy.getId());
					result.setReason(matchedPolicyItem.getComments());
				}
			} else {
				if(! result.getIsAllowed()) { // if access is not yet allowed by another policy
					result.setIsAllowed(true);
					result.setPolicyId(policy.getId());
					result.setReason(matchedPolicyItem.getComments());
				}
			}
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerDefaultPolicyEvaluator.evaluatePolicyItems(" + request + ", " + result + ", " + isResourceMatch + ")");
		}
	}

	protected void evaluatePolicyItems(RangerAccessRequest request, RangerDataMaskResult result) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerDefaultPolicyEvaluator.evaluatePolicyItems(" + request + ", " + result + ")");
		}

		RangerDataMaskPolicyItemEvaluator matchedPolicyItem = getMatchingPolicyItem(request, dataMaskEvaluators);
		RangerPolicyItemDataMaskInfo      dataMaskInfo      = matchedPolicyItem != null ? matchedPolicyItem.getDataMaskInfo() : null;

		if(dataMaskInfo != null) {
			RangerPolicy policy = getPolicy();

			result.setIsAllowed(true);
			result.setIsAccessDetermined(true);

			result.setMaskType(dataMaskInfo.getDataMaskType());
			result.setMaskCondition(dataMaskInfo.getConditionExpr());
			result.setMaskedValue(dataMaskInfo.getValueExpr());
			result.setPolicyId(policy.getId());
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerDefaultPolicyEvaluator.evaluatePolicyItems(" + request + ", " + result + ", " + ")");
		}
	}

	protected void evaluatePolicyItems(RangerAccessRequest request, RangerRowFilterResult result) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerDefaultPolicyEvaluator.evaluatePolicyItems(" + request + ", " + result + ")");
		}

		RangerRowFilterPolicyItemEvaluator matchedPolicyItem = getMatchingPolicyItem(request, rowFilterEvaluators);
		RangerPolicyItemRowFilterInfo      rowFilterInfo     = matchedPolicyItem != null ? matchedPolicyItem.getRowFilterInfo() : null;

		if(rowFilterInfo != null) {
			RangerPolicy policy = getPolicy();

			result.setIsAllowed(true);
			result.setIsAccessDetermined(true);

			result.setFilterExpr(rowFilterInfo.getFilterExpr());
			result.setPolicyId(policy.getId());
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerDefaultPolicyEvaluator.evaluatePolicyItems(" + request + ", " + result + ", " + ")");
		}
	}

	protected RangerPolicyItemEvaluator getDeterminingPolicyItem(String user, Set<String> userGroups, String accessType) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerDefaultPolicyEvaluator.getDeterminingPolicyItem(" + user + ", " + userGroups + ", " + accessType + ")");
		}

		RangerPolicyItemEvaluator ret = null;

		/*
		 *  1. if a deny matches without hitting any deny-exception, return that
		 *  2. if an allow matches without hitting any allow-exception, return that
		 */
		ret = getMatchingPolicyItem(user, userGroups, accessType, denyEvaluators, denyExceptionEvaluators);

		if(ret == null) {
			ret = getMatchingPolicyItem(user, userGroups, accessType, allowEvaluators, allowExceptionEvaluators);
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerDefaultPolicyEvaluator.getDeterminingPolicyItem(" + user + ", " + userGroups + ", " + accessType + "): " + ret);
		}

		return ret;
	}

	private void getResourceAccessInfo(RangerAccessRequest request, List<? extends RangerPolicyItemEvaluator> policyItems, Set<String> users, Set<String> groups) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerDefaultPolicyEvaluator.getResourceAccessInfo(" + request + ", " + policyItems + ", " + users + ", " + groups + ")");
		}

		if (CollectionUtils.isNotEmpty(policyItems)) {
			for (RangerPolicyItemEvaluator policyItemEvaluator : policyItems) {
				if (policyItemEvaluator.matchAccessType(request.getAccessType()) && policyItemEvaluator.matchCustomConditions(request)) {
					if (CollectionUtils.isNotEmpty(policyItemEvaluator.getPolicyItem().getUsers())) {
						users.addAll(policyItemEvaluator.getPolicyItem().getUsers());
					}

					if (CollectionUtils.isNotEmpty(policyItemEvaluator.getPolicyItem().getGroups())) {
						groups.addAll(policyItemEvaluator.getPolicyItem().getGroups());
					}
				}
			}
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerDefaultPolicyEvaluator.getResourceAccessInfo(" + request + ", " + policyItems + ", " + users + ", " + groups + ")");
		}
	}


	protected boolean matchResourceHead(RangerAccessResource resource) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerDefaultPolicyEvaluator.matchResourceHead(" + resource + ")");
		}

		boolean ret = false;

		if(resourceMatcher != null) {
			ret = resourceMatcher.isHeadMatch(resource);
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerDefaultPolicyEvaluator.matchResourceHead(" + resource + "): " + ret);
		}

		return ret;
	}

	protected boolean isMatch(Map<String, RangerPolicyResource> resources) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerDefaultPolicyEvaluator.isMatch(" + resources + ")");
		}

		boolean ret = false;

		if(resourceMatcher != null) {
			ret = resourceMatcher.isMatch(resources);
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerDefaultPolicyEvaluator.isMatch(" + resources + "): " + ret);
		}

		return ret;
	}

	protected boolean isAccessAllowed(String user, Set<String> userGroups, String accessType) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerDefaultPolicyEvaluator.isAccessAllowed(" + user + ", " + userGroups + ", " + accessType + ")");
		}

		boolean ret = false;

		RangerPerfTracer perf = null;

		if(RangerPerfTracer.isPerfTraceEnabled(PERF_POLICY_REQUEST_LOG)) {
			perf = RangerPerfTracer.getPerfTracer(PERF_POLICY_REQUEST_LOG, "RangerPolicyEvaluator.isAccessAllowed(hashCode=" + Integer.toHexString(System.identityHashCode(this)) + "," + perfTag + ")");
		}

		RangerPolicyItemEvaluator item = this.getDeterminingPolicyItem(user, userGroups, accessType);

		if(item != null && item.getPolicyItemType() == RangerPolicyItemEvaluator.POLICY_ITEM_TYPE_ALLOW) {
			ret = true;
		}

		RangerPerfTracer.log(perf);

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerDefaultPolicyEvaluator.isAccessAllowed(" + user + ", " + userGroups + ", " + accessType + "): " + ret);
		}

		return ret;
	}

	public StringBuilder toString(StringBuilder sb) {
		sb.append("RangerDefaultPolicyEvaluator={");

		super.toString(sb);

		sb.append("resourceMatcher={");
		if(resourceMatcher != null) {
			resourceMatcher.toString(sb);
		}
		sb.append("} ");

		sb.append("}");

		return sb;
	}

	private void preprocessPolicy(RangerPolicy policy, RangerServiceDef serviceDef) {
		if(policy == null || (!hasAllow() && !hasDeny()) || serviceDef == null) {
			return;
		}

		Map<String, Collection<String>> impliedAccessGrants = getImpliedAccessGrants(serviceDef);

		if(impliedAccessGrants == null || impliedAccessGrants.isEmpty()) {
			return;
		}

		preprocessPolicyItems(policy.getPolicyItems(), impliedAccessGrants);
		preprocessPolicyItems(policy.getDenyPolicyItems(), impliedAccessGrants);
		preprocessPolicyItems(policy.getAllowExceptions(), impliedAccessGrants);
		preprocessPolicyItems(policy.getDenyExceptions(), impliedAccessGrants);
		preprocessPolicyItems(policy.getDataMaskPolicyItems(), impliedAccessGrants);
	}

	private void preprocessPolicyItems(List<? extends RangerPolicyItem> policyItems, Map<String, Collection<String>> impliedAccessGrants) {
		for(RangerPolicyItem policyItem : policyItems) {
			if(CollectionUtils.isEmpty(policyItem.getAccesses())) {
				continue;
			}

			// Only one round of 'expansion' is done; multi-level impliedGrants (like shown below) are not handled for now
			// multi-level impliedGrants: given admin=>write; write=>read: must imply admin=>read,write
			for(Map.Entry<String, Collection<String>> e : impliedAccessGrants.entrySet()) {
				String             accessType    = e.getKey();
				Collection<String> impliedGrants = e.getValue();

				RangerPolicyItemAccess access = getAccess(policyItem, accessType);

				if(access == null) {
					continue;
				}

				for(String impliedGrant : impliedGrants) {
					RangerPolicyItemAccess impliedAccess = getAccess(policyItem, impliedGrant);

					if(impliedAccess == null) {
						impliedAccess = new RangerPolicyItemAccess(impliedGrant, access.getIsAllowed());

						policyItem.getAccesses().add(impliedAccess);
					} else {
						if(! impliedAccess.getIsAllowed()) {
							impliedAccess.setIsAllowed(access.getIsAllowed());
						}
					}
				}
			}
		}
	}

	private Map<String, Collection<String>> getImpliedAccessGrants(RangerServiceDef serviceDef) {
		Map<String, Collection<String>> ret = null;

		if(serviceDef != null && !CollectionUtils.isEmpty(serviceDef.getAccessTypes())) {
			for(RangerAccessTypeDef accessTypeDef : serviceDef.getAccessTypes()) {
				if(!CollectionUtils.isEmpty(accessTypeDef.getImpliedGrants())) {
					if(ret == null) {
						ret = new HashMap<String, Collection<String>>();
					}

					Collection<String> impliedAccessGrants = ret.get(accessTypeDef.getName());

					if(impliedAccessGrants == null) {
						impliedAccessGrants = new HashSet<String>();

						ret.put(accessTypeDef.getName(), impliedAccessGrants);
					}

					for(String impliedAccessGrant : accessTypeDef.getImpliedGrants()) {
						impliedAccessGrants.add(impliedAccessGrant);
					}
				}
			}
		}

		return ret;
	}

	private RangerPolicyItemAccess getAccess(RangerPolicyItem policyItem, String accessType) {
		RangerPolicyItemAccess ret = null;

		if(policyItem != null && CollectionUtils.isNotEmpty(policyItem.getAccesses())) {
			for(RangerPolicyItemAccess itemAccess : policyItem.getAccesses()) {
				if(StringUtils.equalsIgnoreCase(itemAccess.getType(), accessType)) {
					ret = itemAccess;

					break;
				}
			}
		}

		return ret;
	}

	private List<RangerPolicyItemEvaluator> createPolicyItemEvaluators(RangerPolicy policy, RangerServiceDef serviceDef, RangerPolicyEngineOptions options, int policyItemType) {
		List<RangerPolicyItemEvaluator> ret         = null;
		List<RangerPolicyItem>          policyItems = null;

		if(isPolicyItemTypeEnabled(serviceDef, policyItemType)) {
			if (policyItemType == RangerPolicyItemEvaluator.POLICY_ITEM_TYPE_ALLOW) {
				policyItems = policy.getPolicyItems();
			} else if (policyItemType == RangerPolicyItemEvaluator.POLICY_ITEM_TYPE_DENY) {
				policyItems = policy.getDenyPolicyItems();
			} else if (policyItemType == RangerPolicyItemEvaluator.POLICY_ITEM_TYPE_ALLOW_EXCEPTIONS) {
				policyItems = policy.getAllowExceptions();
			} else if (policyItemType == RangerPolicyItemEvaluator.POLICY_ITEM_TYPE_DENY_EXCEPTIONS) {
				policyItems = policy.getDenyExceptions();
			}
		}

		if(CollectionUtils.isNotEmpty(policyItems)) {
			ret = new ArrayList<RangerPolicyItemEvaluator>();

			int policyItemCounter = 1;

			for(RangerPolicyItem policyItem : policyItems) {
				RangerPolicyItemEvaluator itemEvaluator = new RangerDefaultPolicyItemEvaluator(serviceDef, policy, policyItem, policyItemType, policyItemCounter++, options);

				itemEvaluator.init();

				ret.add(itemEvaluator);

				if(CollectionUtils.isNotEmpty(itemEvaluator.getConditionEvaluators())) {
					customConditionsCount += itemEvaluator.getConditionEvaluators().size();
				}
			}
		} else {
			ret = Collections.<RangerPolicyItemEvaluator>emptyList();
		}

		return ret;
	}

	private List<RangerDataMaskPolicyItemEvaluator> createDataMaskPolicyItemEvaluators(RangerPolicy policy, RangerServiceDef serviceDef, RangerPolicyEngineOptions options, List<RangerDataMaskPolicyItem> policyItems) {
		List<RangerDataMaskPolicyItemEvaluator> ret = null;

		if(CollectionUtils.isNotEmpty(policyItems)) {
			ret = new ArrayList<RangerDataMaskPolicyItemEvaluator>();

			int policyItemCounter = 1;

			for(RangerDataMaskPolicyItem policyItem : policyItems) {
				RangerDataMaskPolicyItemEvaluator itemEvaluator = new RangerDefaultDataMaskPolicyItemEvaluator(serviceDef, policy, policyItem, policyItemCounter++, options);

				itemEvaluator.init();

				ret.add(itemEvaluator);

				if(CollectionUtils.isNotEmpty(itemEvaluator.getConditionEvaluators())) {
					customConditionsCount += itemEvaluator.getConditionEvaluators().size();
				}
			}
		} else {
			ret = Collections.<RangerDataMaskPolicyItemEvaluator>emptyList();
		}

		return ret;
	}

	private List<RangerRowFilterPolicyItemEvaluator> createRowFilterPolicyItemEvaluators(RangerPolicy policy, RangerServiceDef serviceDef, RangerPolicyEngineOptions options, List<RangerRowFilterPolicyItem> policyItems) {
		List<RangerRowFilterPolicyItemEvaluator> ret = null;

		if(CollectionUtils.isNotEmpty(policyItems)) {
			ret = new ArrayList<RangerRowFilterPolicyItemEvaluator>();

			int policyItemCounter = 1;

			for(RangerRowFilterPolicyItem policyItem : policyItems) {
				RangerRowFilterPolicyItemEvaluator itemEvaluator = new RangerDefaultRowFilterPolicyItemEvaluator(serviceDef, policy, policyItem, policyItemCounter++, options);

				itemEvaluator.init();

				ret.add(itemEvaluator);

				if(CollectionUtils.isNotEmpty(itemEvaluator.getConditionEvaluators())) {
					customConditionsCount += itemEvaluator.getConditionEvaluators().size();
				}
			}
		} else {
			ret = Collections.<RangerRowFilterPolicyItemEvaluator>emptyList();
		}

		return ret;
	}

	private boolean isPolicyItemTypeEnabled(RangerServiceDef serviceDef, int policyItemType) {
		boolean ret = true;

		if(policyItemType == RangerPolicyItemEvaluator.POLICY_ITEM_TYPE_DENY ||
		   policyItemType == RangerPolicyItemEvaluator.POLICY_ITEM_TYPE_ALLOW_EXCEPTIONS ||
		   policyItemType == RangerPolicyItemEvaluator.POLICY_ITEM_TYPE_DENY_EXCEPTIONS) {
			ret = ServiceDefUtil.getOption_enableDenyAndExceptionsInPolicies(serviceDef);
		}

		return ret;
	}

	protected <T extends RangerPolicyItemEvaluator> T getMatchingPolicyItem(RangerAccessRequest request, List<T> evaluators) {
		T ret = getMatchingPolicyItem(request, evaluators, null);

		return ret;
	}

	private <T extends RangerPolicyItemEvaluator> T getMatchingPolicyItem(RangerAccessRequest request, List<T> evaluators, List<T> exceptionEvaluators) {
        if(LOG.isDebugEnabled()) {
            LOG.debug("==> RangerDefaultPolicyEvaluator.getMatchingPolicyItem(" + request + ")");
        }

        T ret = null;

        if(CollectionUtils.isNotEmpty(evaluators)) {
            for (T evaluator : evaluators) {
                if(evaluator.isMatch(request)) {
                    ret = evaluator;

                    break;
                }
            }
        }

        if(ret != null && CollectionUtils.isNotEmpty(exceptionEvaluators)) {
            for (T exceptionEvaluator : exceptionEvaluators) {
                if(exceptionEvaluator.isMatch(request)) {
                    if(LOG.isDebugEnabled()) {
                        LOG.debug("RangerDefaultPolicyEvaluator.getMatchingPolicyItem(" + request + "): found exception policyItem(" + exceptionEvaluator.getPolicyItem() + "); ignoring the matchedPolicyItem(" + ret.getPolicyItem() + ")");
                    }

                    ret = null;

                    break;
                }
            }
        }

        if(LOG.isDebugEnabled()) {
            LOG.debug("<== RangerDefaultPolicyEvaluator.getMatchingPolicyItem(" + request + "): " + ret);
        }

        return ret;
    }

	private <T extends RangerPolicyItemEvaluator> T getMatchingPolicyItem(String user, Set<String> userGroups, String accessType, List<T> evaluators, List<T> exceptionEvaluators) {
        if(LOG.isDebugEnabled()) {
            LOG.debug("==> RangerDefaultPolicyEvaluator.getMatchingPolicyItem(" + user + ", " + userGroups + ", " + accessType + ")");
        }

        T ret = null;

        if(CollectionUtils.isNotEmpty(evaluators)) {
            for (T evaluator : evaluators) {
                if(evaluator.matchUserGroup(user, userGroups) && evaluator.matchAccessType(accessType)) {
                    ret = evaluator;

                    break;
                }
            }
        }

        if(ret != null && CollectionUtils.isNotEmpty(exceptionEvaluators)) {
            for (T exceptionEvaluator : exceptionEvaluators) {
                if(exceptionEvaluator.matchUserGroup(user, userGroups) && exceptionEvaluator.matchAccessType(accessType)) {
                    if(LOG.isDebugEnabled()) {
                        LOG.debug("RangerDefaultPolicyEvaluator.getMatchingPolicyItem(" + user + ", " + userGroups + ", " + accessType + "): found exception policyItem(" + exceptionEvaluator.getPolicyItem() + "); ignoring the matchedPolicyItem(" + ret.getPolicyItem() + ")");
                    }

                    ret = null;

                    break;
                }
            }
        }

        if(LOG.isDebugEnabled()) {
            LOG.debug("<== RangerDefaultPolicyEvaluator.getMatchingPolicyItem(" + user + ", " + userGroups + ", " + accessType + "): " + ret);
        }

        return ret;
	}
}
