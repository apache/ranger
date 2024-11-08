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

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItemDataMaskInfo;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyResource;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItemRowFilterInfo;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.model.RangerServiceDef.RangerResourceDef;
import org.apache.ranger.plugin.model.validation.RangerServiceDefHelper;
import org.apache.ranger.plugin.policyengine.PolicyEngine;
import org.apache.ranger.plugin.policyengine.RangerAccessRequest;
import org.apache.ranger.plugin.policyengine.RangerAccessRequest.ResourceMatchingScope;
import org.apache.ranger.plugin.policyengine.RangerPluginContext;
import org.apache.ranger.plugin.policyengine.RangerPolicyEngineOptions;
import org.apache.ranger.plugin.policyengine.RangerResourceACLs;
import org.apache.ranger.plugin.policyengine.RangerResourceACLs.DataMaskResult;
import org.apache.ranger.plugin.policyengine.RangerResourceACLs.RowFilterResult;
import org.apache.ranger.plugin.policyresourcematcher.RangerDefaultPolicyResourceMatcher;
import org.apache.ranger.plugin.policyresourcematcher.RangerPolicyResourceMatcher;
import org.apache.ranger.plugin.policyresourcematcher.RangerPolicyResourceMatcher.MatchType;
import org.apache.ranger.plugin.resourcematcher.RangerAbstractResourceMatcher;
import org.apache.ranger.plugin.resourcematcher.RangerResourceMatcher;
import org.apache.ranger.plugin.util.RangerRequestExprResolver;
import org.apache.ranger.plugin.util.ServiceDefUtil;
import org.apache.ranger.plugin.util.StringTokenReplacer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public abstract class RangerAbstractPolicyEvaluator implements RangerPolicyEvaluator {
	private static final Logger LOG = LoggerFactory.getLogger(RangerAbstractPolicyEvaluator.class);

	private static final AtomicLong NEXT_RESOURCE_EVALUATOR_ID = new AtomicLong(1);

	private final static Map<String, Object> WILDCARD_EVAL_CONTEXT = new HashMap<String, Object>() {
		@Override
		public boolean containsKey(Object key) { return true; }

		@Override
		public Object get(Object key) { return RangerAbstractResourceMatcher.WILDCARD_ASTERISK; }
	};

	static {
		WILDCARD_EVAL_CONTEXT.put(RangerAbstractResourceMatcher.WILDCARD_ASTERISK, RangerAbstractResourceMatcher.WILDCARD_ASTERISK);
	}

	private   RangerPolicy                        policy;
	private   RangerServiceDef                    serviceDef;
	private   boolean                             needsDynamicEval = false;
	private   int                                 evalOrder;
	private   List<RangerPolicyResourceEvaluator> resourceEvaluators = Collections.emptyList();
	protected RangerPluginContext                 pluginContext      = null;


	public void setPluginContext(RangerPluginContext pluginContext) { this.pluginContext = pluginContext; }

	public RangerPluginContext getPluginContext() { return pluginContext; }

	@Override
	public void init(RangerPolicy policy, RangerServiceDef serviceDef, RangerPolicyEngineOptions options) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAbstractPolicyEvaluator.init(" + policy + ", " + serviceDef + ")");
		}

		this.policy           = getPrunedPolicy(policy);
		this.serviceDef       = serviceDef;
		this.needsDynamicEval = false;

		List<RangerPolicyResourceEvaluator>  resourceEvaluators = new ArrayList<>();
		RangerDefaultPolicyResourceEvaluator resourceEvaluator  = new RangerDefaultPolicyResourceEvaluator(NEXT_RESOURCE_EVALUATOR_ID.getAndIncrement(), policy.getResources(), getPolicyType(), serviceDef, options.getServiceDefHelper());

		resourceEvaluators.add(resourceEvaluator);

		this.needsDynamicEval = this.needsDynamicEval || resourceEvaluator.getPolicyResourceMatcher().getNeedsDynamicEval();

		if (CollectionUtils.isNotEmpty(policy.getAdditionalResources())) {
			for (Map<String, RangerPolicyResource> additionalResource : policy.getAdditionalResources()) {
				resourceEvaluator = new RangerDefaultPolicyResourceEvaluator(NEXT_RESOURCE_EVALUATOR_ID.getAndIncrement(), additionalResource, getPolicyType(), serviceDef, options.getServiceDefHelper());

				resourceEvaluators.add(resourceEvaluator);

				this.needsDynamicEval = this.needsDynamicEval || resourceEvaluator.getPolicyResourceMatcher().getNeedsDynamicEval();
			}
		}

		this.resourceEvaluators = resourceEvaluators;

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAbstractPolicyEvaluator.init(" + this.policy + ", " + serviceDef + ")");
		}
	}

	public int getPolicyType() {
		Integer ret = policy != null ? policy.getPolicyType() : null;

		return ret != null ? ret.intValue() : RangerPolicy.POLICY_TYPE_ACCESS;
	}

	@Override
	public RangerPolicy getPolicy() {
		return policy;
	}

	@Override
	public long getPolicyId() {
		Long ret = policy != null ? policy.getId() : null;

		return ret != null ? ret.longValue() : -1;
	}

	@Override
	public int getPolicyPriority() {
		return policy != null && policy.getPolicyPriority() != null ? policy.getPolicyPriority() : RangerPolicy.POLICY_PRIORITY_NORMAL;
	}

	@Override
	public List<RangerPolicyResourceEvaluator> getResourceEvaluators() {
		return resourceEvaluators;
	}

	@Override
	public RangerServiceDef getServiceDef() {
		return serviceDef;
	}

	@Override
	public void getResourceACLs(RangerAccessRequest request, RangerResourceACLs acls, boolean isConditional, Set<String> targetAccessTypes, MatchType matchType, PolicyEngine policyEngine) {
		boolean isMatched          = false;
		boolean isConditionalMatch = false;

		if (matchType == null) {
			for (RangerPolicyResourceEvaluator resourceEvaluator : getResourceEvaluators()) {
				RangerPolicyResourceMatcher matcher = resourceEvaluator.getPolicyResourceMatcher();

				matchType = matcher.getMatchType(request.getResource(), request.getResourceElementMatchingScopes(), request.getContext());

				isMatched = isMatch(matchType, request.getResourceMatchingScope());

				if (isMatched) {
					break;
				} else if (matcher.getNeedsDynamicEval() && !isConditionalMatch && policyEngine != null) {
					MatchType dynWildCardMatch = resourceEvaluator.getMacrosReplaceWithWildcardMatcher(policyEngine).getMatchType(request.getResource(), request.getResourceElementMatchingScopes(), request.getContext());

					isConditionalMatch = isMatch(dynWildCardMatch, request.getResourceMatchingScope());
				}
			}
		} else {
			isMatched = isMatch(matchType, request.getResourceMatchingScope());
		}

		if (isMatched || isConditionalMatch) {
			if (!isConditionalMatch) {
				isConditionalMatch = isConditional || getPolicyConditionsCount() > 0 || getValidityScheduleEvaluatorsCount() != 0;
			}

			int policyType = getPolicyType();

			if (policyType == RangerPolicy.POLICY_TYPE_ACCESS) {
				updateFromPolicyACLs(isConditionalMatch, acls, targetAccessTypes);
			} else if (policyType == RangerPolicy.POLICY_TYPE_ROWFILTER) {
				updateRowFiltersFromPolicy(isConditionalMatch, acls, targetAccessTypes);
			} else if (policyType == RangerPolicy.POLICY_TYPE_DATAMASK) {
				updateDataMasksFromPolicy(isConditionalMatch, acls, targetAccessTypes);
			}
		}
	}

	public boolean hasAllow() {
		return policy != null && CollectionUtils.isNotEmpty(policy.getPolicyItems());
	}

	protected boolean hasMatchablePolicyItem(RangerAccessRequest request) {
		return hasAllow() || hasDeny();
	}

	public boolean hasDeny() {
		return policy != null && (policy.getIsDenyAllElse() || CollectionUtils.isNotEmpty(policy.getDenyPolicyItems()));
	}

	protected boolean needsDynamicEval() { return needsDynamicEval; }

	private RangerPolicy getPrunedPolicy(final RangerPolicy policy) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAbstractPolicyEvaluator.getPrunedPolicy(" + policy + ")");
		}

		final RangerPolicy                        ret;

		final boolean                             isPruningNeeded;
		final List<RangerPolicy.RangerPolicyItem> prunedAllowItems;
		final List<RangerPolicy.RangerPolicyItem> prunedDenyItems;
		final List<RangerPolicy.RangerPolicyItem> prunedAllowExceptions;
		final List<RangerPolicy.RangerPolicyItem> prunedDenyExceptions;

		final RangerPluginContext pluginContext = getPluginContext();

		if (pluginContext != null && pluginContext.getConfig().getPolicyEngineOptions().evaluateDelegateAdminOnly) {
			prunedAllowItems      = policy.getPolicyItems().stream().filter(RangerPolicy.RangerPolicyItem::getDelegateAdmin).collect(Collectors.toList());
			prunedDenyItems       = policy.getDenyPolicyItems().stream().filter(RangerPolicy.RangerPolicyItem::getDelegateAdmin).collect(Collectors.toList());
			prunedAllowExceptions = policy.getAllowExceptions().stream().filter(RangerPolicy.RangerPolicyItem::getDelegateAdmin).collect(Collectors.toList());
			prunedDenyExceptions  = policy.getDenyExceptions().stream().filter(RangerPolicy.RangerPolicyItem::getDelegateAdmin).collect(Collectors.toList());

			isPruningNeeded = prunedAllowItems.size() != policy.getPolicyItems().size()
					|| prunedDenyItems.size() != policy.getDenyPolicyItems().size()
					|| prunedAllowExceptions.size() != policy.getAllowExceptions().size()
					|| prunedDenyExceptions.size() != policy.getDenyExceptions().size();
		} else {
			prunedAllowItems      = null;
			prunedDenyItems       = null;
			prunedAllowExceptions = null;
			prunedDenyExceptions  = null;
			isPruningNeeded       = false;
		}

		if (!isPruningNeeded) {
			ret = policy;
		} else {
			ret = new RangerPolicy();
			ret.updateFrom(policy);

			ret.setId(policy.getId());
			ret.setGuid(policy.getGuid());
			ret.setVersion(policy.getVersion());
			ret.setServiceType(policy.getServiceType());

			ret.setPolicyItems(prunedAllowItems);
			ret.setDenyPolicyItems(prunedDenyItems);
			ret.setAllowExceptions(prunedAllowExceptions);
			ret.setDenyExceptions(prunedDenyExceptions);
		}
		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAbstractPolicyEvaluator.getPrunedPolicy(isPruningNeeded=" + isPruningNeeded + ") : " + ret);
		}

		return ret;
	}

	@Override
	public int getEvalOrder() {
		return evalOrder;
	}
	@Override
	public boolean isAuditEnabled() {
		return policy != null && policy.getIsAuditEnabled();
	}

	public void setEvalOrder(int evalOrder) {
		this.evalOrder = evalOrder;
	}

	@Override
	public PolicyACLSummary getPolicyACLSummary() { return null; }

	@Override
	public String toString( ) {
		StringBuilder sb = new StringBuilder();

		toString(sb);

		return sb.toString();
	}

	public StringBuilder toString(StringBuilder sb) {
		sb.append("RangerAbstractPolicyEvaluator={");

		sb.append("policy={");
		if (policy != null) {
			policy.toString(sb);
		}
		sb.append("} ");

		sb.append("}");

		return sb;
	}

	private boolean isMatch(MatchType matchType, ResourceMatchingScope matchingScope) {
		final boolean ret;

		matchingScope = matchingScope != null ? matchingScope : ResourceMatchingScope.SELF;

		if (matchingScope == ResourceMatchingScope.SELF_OR_DESCENDANTS) {
			ret = matchType == MatchType.SELF || matchType == MatchType.SELF_AND_ALL_DESCENDANTS || matchType == MatchType.DESCENDANT;
		} else {
			ret = matchType == MatchType.SELF || matchType == MatchType.SELF_AND_ALL_DESCENDANTS;
		}

		return ret;
	}


	private void updateFromPolicyACLs(boolean isConditional, RangerResourceACLs resourceACLs, Set<String> targetAccessTypes) {
		PolicyACLSummary aclSummary = getPolicyACLSummary();

		if (aclSummary == null) {
			return;
		}

		for (Map.Entry<String, Map<String, PolicyACLSummary.AccessResult>> userAccessInfo : aclSummary.getUsersAccessInfo().entrySet()) {
			final String userName = userAccessInfo.getKey();

			for (Map.Entry<String, PolicyACLSummary.AccessResult> accessInfo : userAccessInfo.getValue().entrySet()) {
				String accessType = accessInfo.getKey();

				if (targetAccessTypes != null && !targetAccessTypes.contains(accessType)) {
					continue;
				}

				Integer accessResult;

				if (isConditional) {
					accessResult = ACCESS_CONDITIONAL;
				} else {
					accessResult = accessInfo.getValue().getResult();

					if (accessResult.equals(RangerPolicyEvaluator.ACCESS_UNDETERMINED)) {
						accessResult = RangerPolicyEvaluator.ACCESS_DENIED;
					}
				}

				RangerPolicy policy = getPolicy();

				resourceACLs.setUserAccessInfo(userName, accessInfo.getKey(), accessResult, policy);
			}
		}

		for (Map.Entry<String, Map<String, PolicyACLSummary.AccessResult>> groupAccessInfo : aclSummary.getGroupsAccessInfo().entrySet()) {
			final String groupName = groupAccessInfo.getKey();

			for (Map.Entry<String, PolicyACLSummary.AccessResult> accessInfo : groupAccessInfo.getValue().entrySet()) {
				String accessType = accessInfo.getKey();

				if (targetAccessTypes != null && !targetAccessTypes.contains(accessType)) {
					continue;
				}

				Integer accessResult;

				if (isConditional) {
					accessResult = ACCESS_CONDITIONAL;
				} else {
					accessResult = accessInfo.getValue().getResult();

					if (accessResult.equals(RangerPolicyEvaluator.ACCESS_UNDETERMINED)) {
						accessResult = RangerPolicyEvaluator.ACCESS_DENIED;
					}
				}

				RangerPolicy policy = getPolicy();

				resourceACLs.setGroupAccessInfo(groupName, accessInfo.getKey(), accessResult, policy);
			}
		}

		for (Map.Entry<String, Map<String, PolicyACLSummary.AccessResult>> roleAccessInfo : aclSummary.getRolesAccessInfo().entrySet()) {
			final String roleName = roleAccessInfo.getKey();

			for (Map.Entry<String, PolicyACLSummary.AccessResult> accessInfo : roleAccessInfo.getValue().entrySet()) {
				String accessType = accessInfo.getKey();

				if (targetAccessTypes != null && !targetAccessTypes.contains(accessType)) {
					continue;
				}

				Integer accessResult;

				if (isConditional) {
					accessResult = ACCESS_CONDITIONAL;
				} else {
					accessResult = accessInfo.getValue().getResult();

					if (accessResult.equals(RangerPolicyEvaluator.ACCESS_UNDETERMINED)) {
						accessResult = RangerPolicyEvaluator.ACCESS_DENIED;
					}
				}

				RangerPolicy policy = getPolicy();

				resourceACLs.setRoleAccessInfo(roleName, accessInfo.getKey(), accessResult, policy);
			}
		}
	}

	private void updateRowFiltersFromPolicy(boolean isConditional, RangerResourceACLs resourceACLs, Set<String> targetAccessTypes) {
		PolicyACLSummary aclSummary = getPolicyACLSummary();

		if (aclSummary != null) {
			for (RowFilterResult rowFilterResult : aclSummary.getRowFilters()) {
				if (targetAccessTypes != null && !CollectionUtils.containsAny(targetAccessTypes, rowFilterResult.getAccessTypes())) {
					continue;
				}

				rowFilterResult = copyRowFilter(rowFilterResult);

				if (isConditional) {
					rowFilterResult.setIsConditional(true);
				}

				resourceACLs.getRowFilters().add(rowFilterResult);
			}
		}
	}

	private void updateDataMasksFromPolicy(boolean isConditional, RangerResourceACLs resourceACLs, Set<String> targetAccessTypes) {
		PolicyACLSummary aclSummary = getPolicyACLSummary();

		if (aclSummary != null) {
			for (DataMaskResult dataMaskResult : aclSummary.getDataMasks()) {
				if (targetAccessTypes != null && !CollectionUtils.containsAny(targetAccessTypes, dataMaskResult.getAccessTypes())) {
					continue;
				}

				dataMaskResult = copyDataMask(dataMaskResult);

				if (isConditional) {
					dataMaskResult.setIsConditional(true);
				}

				resourceACLs.getDataMasks().add(dataMaskResult);
			}
		}
	}

	private DataMaskResult copyDataMask(DataMaskResult dataMask) {
		DataMaskResult ret = new DataMaskResult(copyStrings(dataMask.getUsers()),
		                                        copyStrings(dataMask.getGroups()),
		                                        copyStrings(dataMask.getRoles()),
		                                        copyStrings(dataMask.getAccessTypes()),
		                                        new RangerPolicyItemDataMaskInfo(dataMask.getMaskInfo()));

		ret.setIsConditional(dataMask.getIsConditional());

		return ret;
	}

	private RowFilterResult copyRowFilter(RowFilterResult rowFilter) {
		RowFilterResult ret = new RowFilterResult(copyStrings(rowFilter.getUsers()),
		                                          copyStrings(rowFilter.getGroups()),
		                                          copyStrings(rowFilter.getRoles()),
		                                          copyStrings(rowFilter.getAccessTypes()),
		                                          new RangerPolicyItemRowFilterInfo(rowFilter.getFilterInfo()));

		ret.setIsConditional(rowFilter.getIsConditional());

		return ret;
	}

	private Set<String> copyStrings(Set<String> values) {
		return values != null ? new HashSet<>(values) : null;
	}

	private Map<String, RangerPolicyResource> getPolicyResourcesWithMacrosReplaced(Map<String, RangerPolicyResource> resources, PolicyEngine policyEngine) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAbstractPolicyEvaluator.getPolicyResourcesWithMacrosReplaced(" + resources + ")");
		}

		final Map<String, RangerPolicyResource> ret;
		final Collection<String>                resourceKeys = resources == null ? null : resources.keySet();

		if (CollectionUtils.isNotEmpty(resourceKeys)) {
			ret = new HashMap<>();

			for (String resourceName : resourceKeys) {
				RangerPolicyResource resourceValues = resources.get(resourceName);
				List<String>         values         = resourceValues == null ? null : resourceValues.getValues();

				if (CollectionUtils.isNotEmpty(values)) {
					StringTokenReplacer tokenReplacer = policyEngine.getStringTokenReplacer(resourceName);

					List<String> modifiedValues = new ArrayList<>();

					for (String value : values) {
						RangerRequestExprResolver exprResolver  = new RangerRequestExprResolver(value, serviceDef.getName());
						String                    modifiedValue = exprResolver.resolveExpressions(WILDCARD_EVAL_CONTEXT);

						if (tokenReplacer != null) {
							modifiedValue = tokenReplacer.replaceTokens(modifiedValue, WILDCARD_EVAL_CONTEXT);
						}

						modifiedValues.add(modifiedValue);
					}

					RangerPolicyResource modifiedPolicyResource = new RangerPolicyResource(modifiedValues, resourceValues.getIsExcludes(), resourceValues.getIsRecursive());

					ret.put(resourceName, modifiedPolicyResource);
				} else {
					ret.put(resourceName, resourceValues);
				}
			}
		} else {
			ret = resources;
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAbstractPolicyEvaluator.getPolicyResourcesWithMacrosReplaced(" + resources  + "): " + ret);
		}

		return ret;
	}

	public class RangerDefaultPolicyResourceEvaluator implements RangerPolicyResourceEvaluator {
		private final    long                               id;
		private final    Map<String, RangerPolicyResource>  resource;
		private final    RangerDefaultPolicyResourceMatcher resourceMatcher;
		private final    RangerResourceDef                  leafResourceDef;
		private volatile RangerDefaultPolicyResourceMatcher macrosReplacedWithWildcardMatcher;

		public RangerDefaultPolicyResourceEvaluator(long id, Map<String, RangerPolicyResource> resource, int policyType, RangerServiceDef serviceDef, RangerServiceDefHelper serviceDefHelper) {
			this.id              = id;
			this.resource        = resource;
			this.leafResourceDef = ServiceDefUtil.getLeafResourceDef(serviceDef, resource);
			this.resourceMatcher = new RangerDefaultPolicyResourceMatcher();

			this.resourceMatcher.setPolicyResources(resource, policyType);
			this.resourceMatcher.setServiceDef(serviceDef);
			this.resourceMatcher.setServiceDefHelper(serviceDefHelper);
			this.resourceMatcher.setPluginContext(pluginContext);
			this.resourceMatcher.init();
		}

		@Override
		public RangerPolicyEvaluator getPolicyEvaluator() {
			return RangerAbstractPolicyEvaluator.this;
		}

		@Override
		public long getId() {
			return id;
		}

		@Override
		public RangerPolicyResourceMatcher getPolicyResourceMatcher() {
			return resourceMatcher;
		}

		@Override
		public RangerPolicyResourceMatcher getMacrosReplaceWithWildcardMatcher(PolicyEngine policyEngine) {
			RangerDefaultPolicyResourceMatcher ret = this.macrosReplacedWithWildcardMatcher;

			if (ret == null) {
				synchronized (this) {
					ret = this.macrosReplacedWithWildcardMatcher;

					if (ret == null) {
						if (resourceMatcher.getNeedsDynamicEval()) {
							Map<String, RangerPolicyResource> updatedResource = getPolicyResourcesWithMacrosReplaced(resource, policyEngine);

							ret = new RangerDefaultPolicyResourceMatcher(true);

							ret.setPolicyResources(updatedResource, resourceMatcher.getPolicyType());
							ret.setServiceDef(serviceDef);
							ret.setServiceDefHelper(resourceMatcher.getServiceDefHelper());
							ret.init();
						} else {
							ret = resourceMatcher;
						}

						this.macrosReplacedWithWildcardMatcher = ret;
					}
				}
			}

			return ret;
		}

		@Override
		public Map<String, RangerPolicyResource> getPolicyResource() {
			return resource;
		}

		@Override
		public RangerResourceMatcher getResourceMatcher(String resourceName) {
			return resourceMatcher.getResourceMatcher(resourceName);
		}

		@Override
		public boolean isAncestorOf(RangerResourceDef resourceDef) {
			if (resourceMatcher.getPolicyType() == RangerPolicy.POLICY_TYPE_AUDIT && (resource == null || resource.isEmpty())) {
				return true;
			} else {
				return ServiceDefUtil.isAncestorOf(serviceDef, leafResourceDef, resourceDef);
			}
		}

		@Override
		public boolean isLeaf(String resourceName) { return StringUtils.equals(resourceName, leafResourceDef.getName()); }
	}
}
