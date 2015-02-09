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

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ranger.plugin.conditionevaluator.RangerConditionEvaluator;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItem;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItemAccess;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItemCondition;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyResource;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.model.RangerServiceDef.RangerAccessTypeDef;
import org.apache.ranger.plugin.model.RangerServiceDef.RangerPolicyConditionDef;
import org.apache.ranger.plugin.model.RangerServiceDef.RangerResourceDef;
import org.apache.ranger.plugin.policyengine.RangerAccessRequest;
import org.apache.ranger.plugin.policyengine.RangerAccessResult;
import org.apache.ranger.plugin.policyengine.RangerPolicyEngine;
import org.apache.ranger.plugin.policyengine.RangerResource;
import org.apache.ranger.plugin.resourcematcher.RangerDefaultResourceMatcher;
import org.apache.ranger.plugin.resourcematcher.RangerResourceMatcher;

import com.google.common.base.Strings;


public class RangerDefaultPolicyEvaluator extends RangerAbstractPolicyEvaluator {
	private static final Log LOG = LogFactory.getLog(RangerDefaultPolicyEvaluator.class);

	private Map<String, RangerResourceMatcher> matchers = null;
	private Map<String, RangerConditionEvaluator> conditionEvaluators = null;

	@Override
	public void init(RangerPolicy policy, RangerServiceDef serviceDef) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerDefaultPolicyEvaluator.init()");
		}

		preprocessPolicy(policy, serviceDef);

		super.init(policy, serviceDef);

		this.matchers = new HashMap<String, RangerResourceMatcher>();

		if(policy != null && policy.getResources() != null && serviceDef != null) {
			for(RangerResourceDef resourceDef : serviceDef.getResources()) {
				String               resourceName   = resourceDef.getName();
				RangerPolicyResource policyResource = policy.getResources().get(resourceName);

				if(policyResource != null) {
					RangerResourceMatcher matcher = createResourceMatcher(resourceDef, policyResource);

					if(matcher != null) {
						matchers.put(resourceName, matcher);
					} else {
						LOG.error("failed to find matcher for resource " + resourceName);
					}
				}
			}
		}
		
		conditionEvaluators = initializeConditionEvaluators(policy, serviceDef);

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerDefaultPolicyEvaluator.init()");
		}
	}

	/**
	 * Non-private only for testability.
	 * @param policy
	 * @param serviceDef
	 * @return a Map of condition name to a new evaluator object of the class configured in service definition for that condition name
	 */
	Map<String, RangerConditionEvaluator> initializeConditionEvaluators(RangerPolicy policy, RangerServiceDef serviceDef) {
		if(LOG.isDebugEnabled()) {
			LOG.debug(String.format("==> RangerDefaultPolicyEvaluator.initializeConditionEvaluators(%s, %s)", policy, serviceDef));
		}

		Map<String, RangerConditionEvaluator> result = new HashMap<String, RangerConditionEvaluator>();
		if (policy == null) {
			LOG.debug("initializeConditionEvaluators: Policy is null!");
		} else if (CollectionUtils.isEmpty(policy.getPolicyItems())) {
			LOG.debug("initializeConditionEvaluators: Policyitems collection null or empty!");
		} else {
			for (RangerPolicyItem item : policy.getPolicyItems()) {
				if (CollectionUtils.isEmpty(item.getConditions())) {
					if (LOG.isDebugEnabled()) {
						LOG.debug(String.format("initializeConditionEvaluators: null or empty condition collection on policy item[%s].  Ok, skipping", item));
					}
				} else {
					for (RangerPolicyItemCondition condition : item.getConditions()) {
						String conditionName = condition.getType();
						// skip it if we have already processed this condition earlier
						if (result.containsKey(conditionName)) {
							continue;
						}
						String evaluatorClassName = getEvaluatorName(serviceDef, conditionName);
						if (Strings.isNullOrEmpty(evaluatorClassName)) {
							LOG.error("initializeConditionEvaluators: Serious Configuration error: Couldn't get condition evaluator class name for condition[" + conditionName + "]!  Disabling all checks for this condition.");
						} else {
							RangerConditionEvaluator anEvaluator = newConditionEvauator(evaluatorClassName);
							if (anEvaluator == null) {
								LOG.error("initializeConditionEvaluators: Serious Configuration error: Couldn't instantiate condition evaluator for class[" + evaluatorClassName + "].  All checks for condition[" + conditionName + "] disabled.");
							} else {
								anEvaluator.init(condition);
								result.put(conditionName, anEvaluator);
							}
						}
					}
				}
			}
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug(String.format("<== RangerDefaultPolicyEvaluator.initializeConditionEvaluators(%s)", result.toString()));
		}
		return result;
	}

	// TODO this should be cached in the policyengine to avoid repeated processing for every policy 
	String getEvaluatorName(RangerServiceDef serviceDef, String conditionName) {
		if(LOG.isDebugEnabled()) {
			LOG.debug(String.format("==> RangerDefaultPolicyEvaluator.initializeConditionEvaluators(%s, %s)", serviceDef, conditionName));
		}
		
		String evaluatorName = null;
		if (Strings.isNullOrEmpty(conditionName)) {
			LOG.debug("initializeConditionEvaluators: Condition name was null or empty!");
		}
		else if (serviceDef == null) {
			LOG.debug("initializeConditionEvaluators: Servicedef was null!");
		} else if (CollectionUtils.isEmpty(serviceDef.getPolicyConditions())) {
			LOG.debug("initializeConditionEvaluators: Policy conditions collection of the service def is empty!  Ok, skipping.");
		} else {
			Iterator<RangerPolicyConditionDef> iterator = serviceDef.getPolicyConditions().iterator();
			while (iterator.hasNext() && evaluatorName == null) {
				RangerPolicyConditionDef conditionDef = iterator.next();
				String name = conditionDef.getName();
				if (conditionName.equals(name)) {
					evaluatorName = conditionDef.getEvaluator();
				}
			}
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug(String.format("<== RangerDefaultPolicyEvaluator.initializeConditionEvaluators(%s -> %s)", conditionName, evaluatorName));
		}
		return evaluatorName;
	}
	
	RangerConditionEvaluator newConditionEvauator(String className) {
		if(LOG.isDebugEnabled()) {
			LOG.debug(String.format("==> RangerDefaultPolicyEvaluator.newConditionEvauator(%s)", className));
		}

		RangerConditionEvaluator evaluator = null;
		try {
			@SuppressWarnings("unchecked")
			Class<RangerConditionEvaluator> matcherClass = (Class<RangerConditionEvaluator>)Class.forName(className);

			evaluator = matcherClass.newInstance();
		} catch(Throwable t) {
			LOG.error("Caught Throwable: unexpected error instantiating object of class[" + className + "].  Returning null!", t);
		}
	
		if(LOG.isDebugEnabled()) {
			LOG.debug(String.format("<== RangerDefaultPolicyEvaluator.newConditionEvauator(%s)", evaluator == null ? null : evaluator.toString()));
		}
		return evaluator;
	}

	@Override
	public void evaluate(RangerAccessRequest request, RangerAccessResult result) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerDefaultPolicyEvaluator.evaluate(" + request + ", " + result + ")");
		}

		RangerPolicy policy = getPolicy();

		if(policy != null && request != null && result != null) {
			boolean isResourceMatch     = isMatch(request.getResource());
			boolean isResourceHeadMatch = isResourceMatch || matchResourceHead(request.getResource());
			String  accessType          = request.getAccessType();

			if(StringUtils.isEmpty(accessType)) {
				accessType = RangerPolicyEngine.ANY_ACCESS;
			}

			boolean isAnyAccess   = StringUtils.equals(accessType, RangerPolicyEngine.ANY_ACCESS);
			boolean isAdminAccess = StringUtils.equals(accessType, RangerPolicyEngine.ADMIN_ACCESS);

			if(isResourceMatch || (isResourceHeadMatch && isAnyAccess)) {
				if(policy.getIsAuditEnabled()) {
					result.setIsAudited(true);
				}

				for(RangerPolicyItem policyItem : policy.getPolicyItems()) {
					if(isAdminAccess) {
						if(policyItem.getDelegateAdmin()) {
							result.setIsAllowed(true);
							result.setPolicyId(policy.getId());
							break;
						}

						continue;
					}

					if(CollectionUtils.isEmpty(policyItem.getAccesses())) {
						continue;
					}

					boolean isUserGroupMatch = matchUserGroup(policyItem, request.getUser(), request.getUserGroups());

					if(! isUserGroupMatch) {
						continue;
					}

					boolean isCustomConditionsMatch = matchCustomConditions(policyItem, request, conditionEvaluators);

					if(! isCustomConditionsMatch) {
						continue;
					}

					if(isAnyAccess) {
						for(RangerPolicyItemAccess access : policyItem.getAccesses()) {
							if(access.getIsAllowed()) {
								result.setIsAllowed(true);
								result.setPolicyId(policy.getId());
								break;
							}
						}
					} else {
						RangerPolicyItemAccess access = getAccess(policyItem, accessType);

						if(access != null && access.getIsAllowed()) {
							result.setIsAllowed(true);
							result.setPolicyId(policy.getId());
						}
					}

					if(result.getIsAllowed()) {
						break;
					}
				}
			}
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerDefaultPolicyEvaluator.evaluate(" + request + ", " + result + ")");
		}
	}

	@Override
	public boolean isMatch(RangerResource resource) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerDefaultPolicyEvaluator.isMatch(" + resource + ")");
		}

		boolean ret = false;

		RangerServiceDef serviceDef = getServiceDef();

		if(serviceDef != null && serviceDef.getResources() != null) {
			Collection<String> resourceKeys = resource == null ? null : resource.getKeys();
			Collection<String> policyKeys   = matchers == null ? null : matchers.keySet();

			boolean keysMatch = CollectionUtils.isEmpty(resourceKeys) || (policyKeys != null && policyKeys.containsAll(resourceKeys));

			if(keysMatch) {
				for(RangerResourceDef resourceDef : serviceDef.getResources()) {
					String                resourceName  = resourceDef.getName();
					String                resourceValue = resource == null ? null : resource.getValue(resourceName);
					RangerResourceMatcher matcher       = matchers == null ? null : matchers.get(resourceName);

					// when no value exists for a resourceName, consider it a match only if: policy doesn't have a matcher OR matcher allows no-value resource
					if(StringUtils.isEmpty(resourceValue)) {
						ret = matcher == null || matcher.isMatch(resourceValue);
					} else {
						ret = matcher != null && matcher.isMatch(resourceValue);
					}

					if(! ret) {
						break;
					}
				}
			} else {
				if(LOG.isDebugEnabled()) {
					LOG.debug("isMatch(): keysMatch=false. isMatch=" + resourceKeys + "; policyKeys=" + policyKeys);
				}
			}
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerDefaultPolicyEvaluator.isMatch(" + resource + "): " + ret);
		}

		return ret;
	}

	public boolean isSingleAndExactMatch(RangerResource resource) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerDefaultPolicyEvaluator.isSingleAndExactMatch(" + resource + ")");
		}

		boolean ret = false;

		RangerServiceDef serviceDef = getServiceDef();

		if(serviceDef != null && serviceDef.getResources() != null) {
			Collection<String> resourceKeys = resource == null ? null : resource.getKeys();
			Collection<String> policyKeys   = matchers == null ? null : matchers.keySet();

			boolean keysMatch = CollectionUtils.isEqualCollection(resourceKeys, policyKeys);

			if(keysMatch) {
				for(RangerResourceDef resourceDef : serviceDef.getResources()) {
					String                resourceName  = resourceDef.getName();
					String                resourceValue = resource == null ? null : resource.getValue(resourceName);
					RangerResourceMatcher matcher       = matchers == null ? null : matchers.get(resourceName);

					if(StringUtils.isEmpty(resourceValue)) {
						ret = matcher == null || matcher.isSingleAndExactMatch(resourceValue);
					} else {
						ret = matcher != null && matcher.isSingleAndExactMatch(resourceValue);
					}

					if(! ret) {
						break;
					}
				}
			} else {
				if(LOG.isDebugEnabled()) {
					LOG.debug("isSingleAndExactMatch(): keysMatch=false. resourceKeys=" + resourceKeys + "; policyKeys=" + policyKeys);
				}
			}
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerDefaultPolicyEvaluator.isSingleAndExactMatch(" + resource + "): " + ret);
		}

		return ret;
	}

	protected boolean matchResourceHead(RangerResource resource) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerDefaultPolicyEvaluator.matchResourceHead(" + resource + ")");
		}

		boolean ret = false;

		RangerServiceDef serviceDef = getServiceDef();

		if(serviceDef != null && serviceDef.getResources() != null) {
			int numMatched   = 0;
			int numUnmatched = 0;

			for(RangerResourceDef resourceDef : serviceDef.getResources()) {
				String                resourceName  = resourceDef.getName();
				String                resourceValue = resource == null ? null : resource.getValue(resourceName);
				RangerResourceMatcher matcher       = matchers == null ? null : matchers.get(resourceName);

				if(numUnmatched > 0) { // no further values are expected in the resource
					if(! StringUtils.isEmpty(resourceValue)) {
						break;
					}

					numUnmatched++;
					continue;
				} else {
					boolean isMatch = false;

					// when no value exists for a resourceName, consider it a match only if (policy doesn't have a matcher OR matcher allows no-value resource)
					if(StringUtils.isEmpty(resourceValue)) {
						isMatch = matcher == null || matcher.isMatch(resourceValue);
					} else {
						isMatch = matcher != null && matcher.isMatch(resourceValue);
					}

					if(isMatch) {
						numMatched++;
					} else {
						numUnmatched++;
					}
				}
			}

			ret = (numMatched > 0) && serviceDef.getResources().size() == (numMatched + numUnmatched);
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerDefaultPolicyEvaluator.matchResourceHead(" + resource + "): " + ret);
		}

		return ret;
	}

	protected boolean matchUserGroup(RangerPolicyItem policyItem, String user, Collection<String> groups) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerDefaultPolicyEvaluator.matchUserGroup(" + policyItem + ", " + user + ", " + groups + ")");
		}

		boolean ret = false;

		if(policyItem != null) {
			if(!ret && user != null && policyItem.getUsers() != null) {
				ret = policyItem.getUsers().contains(user);
			}

			if(!ret && groups != null && policyItem.getGroups() != null) {
				ret = policyItem.getGroups().contains(RangerPolicyEngine.GROUP_PUBLIC) ||
						!Collections.disjoint(policyItem.getGroups(), groups);
			}
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerDefaultPolicyEvaluator.matchUserGroup(" + policyItem + ", " + user + ", " + groups + "): " + ret);
		}

		return ret;
	}

	// takes map in as argument for testability
	protected boolean matchCustomConditions(RangerPolicyItem policyItem, RangerAccessRequest request, Map<String, RangerConditionEvaluator> evaluatorMap) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerDefaultPolicyEvaluator.matchCustomConditions(" + request + ")");
		}

		boolean result = true;
		if (policyItem == null) {
			LOG.debug("matchCustomConditions: Unexpected: policyItem was null");
		} else if (CollectionUtils.isEmpty(policyItem.getConditions())) {
			LOG.debug("matchCustomConditions: policy item does not have any conditions! Ok, implicitly passed.");
		} else {
			Iterator<RangerPolicyItemCondition> iterator = policyItem.getConditions().iterator();
			/*
			 * We need to let the request be evaluated by the condition evaluator for each condition on the policy item.
			 * We bail out as soon as we find a mismatch, i.e. ALL conditions must succeed for condition evaluation to return true.
			 */
			boolean matched = true;
			while (iterator.hasNext() && matched) {
				RangerPolicyItemCondition itemCondition = iterator.next();
				if (itemCondition == null) {
					LOG.debug("matchCustomConditions: Unexpected: Item condition on policy item was null!  Ignoring...");
				} else {
					String conditionName = itemCondition.getType();
					if (StringUtils.isBlank(conditionName)) {
						LOG.debug("matchCustomConditions: Unexpected: condition name on item conditon [" + conditionName + "] was null/empty/blank! Ignoring...");
					} else if (!evaluatorMap.containsKey(conditionName)) {
						LOG.warn("matchCustomConditions: Unexpected: Could not find condition evaluator for condition[" + conditionName + "]! Ignoring...");
					} else {
						RangerConditionEvaluator conditionEvaluator = evaluatorMap.get(conditionName);
						matched = conditionEvaluator.isMatched(request);
						if (LOG.isDebugEnabled()) {
							LOG.debug(String.format("matchCustomConditions: evaluator for condition[%s] returned[%s] for request[%s]", conditionName, matched, request));
						}
					}
				}
			}
			result = result && matched;
		}
		
		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerDefaultPolicyEvaluator.matchCustomConditions(" + request + "): " + result);
		}

		return result;
	}

	protected RangerPolicyItemAccess getAccess(RangerPolicyItem policyItem, String accessType) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerDefaultPolicyEvaluator.getAccess(" + policyItem + ", " + accessType + ")");
		}

		RangerPolicyItemAccess ret = null;

		if(policyItem != null && accessType != null && policyItem.getAccesses() != null) {
			for(RangerPolicyItemAccess access : policyItem.getAccesses()) {
				if(StringUtils.equalsIgnoreCase(accessType, access.getType())) {
					ret = access;

					break;
				}
			}
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerDefaultPolicyEvaluator.getAccess(" + policyItem + ", " + accessType + "): " + ret);
		}

		return ret;
	}

	protected static RangerResourceMatcher createResourceMatcher(RangerResourceDef resourceDef, RangerPolicyResource resource) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerDefaultPolicyEvaluator.createResourceMatcher(" + resourceDef + ", " + resource + ")");
		}

		RangerResourceMatcher ret = null;

		String resName = resourceDef != null ? resourceDef.getName() : null;
		String clsName = resourceDef != null ? resourceDef.getMatcher() : null;
		String options = resourceDef != null ? resourceDef.getMatcherOptions() : null;

		if(! StringUtils.isEmpty(clsName)) {
			try {
				@SuppressWarnings("unchecked")
				Class<RangerResourceMatcher> matcherClass = (Class<RangerResourceMatcher>)Class.forName(clsName);

				ret = matcherClass.newInstance();
			} catch(Exception excp) {
				LOG.error("failed to instantiate resource matcher '" + clsName + "' for '" + resName + "'. Default resource matcher will be used", excp);
			}
		}

		if(ret == null) {
			ret = new RangerDefaultResourceMatcher();
		}

		if(ret != null) {
			ret.init(resourceDef, resource,  options);
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerDefaultPolicyEvaluator.createResourceMatcher(" + resourceDef + ", " + resource + "): " + ret);
		}

		return ret;
	}

	public StringBuilder toString(StringBuilder sb) {
		sb.append("RangerDefaultPolicyEvaluator={");

		super.toString(sb);

		sb.append("matchers={");
		if(matchers != null) {
			for(RangerResourceMatcher matcher : matchers.values()) {
				sb.append("{").append(matcher).append("} ");
			}
		}
		sb.append("} ");

		sb.append("}");

		return sb;
	}

	private void preprocessPolicy(RangerPolicy policy, RangerServiceDef serviceDef) {
		if(policy == null || CollectionUtils.isEmpty(policy.getPolicyItems()) || serviceDef == null) {
			return;
		}

		Map<String, Collection<String>> impliedAccessGrants = getImpliedAccessGrants(serviceDef);

		if(impliedAccessGrants == null || impliedAccessGrants.isEmpty()) {
			return;
		}

		for(RangerPolicyItem policyItem : policy.getPolicyItems()) {
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
}
