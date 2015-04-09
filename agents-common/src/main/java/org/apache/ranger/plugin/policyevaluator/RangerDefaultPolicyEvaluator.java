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
import java.util.Set;

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
import org.apache.ranger.plugin.policyengine.RangerAccessResource;
import org.apache.ranger.plugin.policyengine.RangerAccessResult;
import org.apache.ranger.plugin.policyengine.RangerPolicyEngine;
import org.apache.ranger.plugin.resourcematcher.RangerDefaultResourceMatcher;
import org.apache.ranger.plugin.resourcematcher.RangerResourceMatcher;

import com.google.common.base.Strings;
import com.google.common.collect.Sets;


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

    public Map<String, RangerConditionEvaluator> getConditionEvaluators() {
        return conditionEvaluators;
    }
    public int computePolicyEvalOrder() { return 0;}

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
						RangerPolicyConditionDef conditionDef = getConditionDef(serviceDef, conditionName);
						if (conditionDef == null) {
							LOG.error("initializeConditionEvaluators: Serious Configuration error: Couldn't get condition Definition for condition[" + conditionName + "]!  Disabling all checks for this condition.");
						} else {
							String evaluatorClassName = conditionDef.getEvaluator();
							if (Strings.isNullOrEmpty(evaluatorClassName)) {
								LOG.error("initializeConditionEvaluators: Serious Configuration error: Couldn't get condition evaluator class name for condition[" + conditionName + "]!  Disabling all checks for this condition.");
							} else {
								RangerConditionEvaluator anEvaluator = newConditionEvaluator(evaluatorClassName);
								if (anEvaluator == null) {
									LOG.error("initializeConditionEvaluators: Serious Configuration error: Couldn't instantiate condition evaluator for class[" + evaluatorClassName + "].  All checks for condition[" + conditionName + "] disabled.");
								} else {
									anEvaluator.init(conditionDef, condition);
									result.put(conditionName, anEvaluator);
								}
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

	RangerPolicyConditionDef getConditionDef(RangerServiceDef serviceDef, String conditionName) {
		if(LOG.isDebugEnabled()) {
			LOG.debug(String.format("==> RangerDefaultPolicyEvaluator.initializeConditionEvaluators(%s, %s)", serviceDef, conditionName));
		}
		
		RangerPolicyConditionDef result = null;
		if (Strings.isNullOrEmpty(conditionName)) {
			LOG.debug("initializeConditionEvaluators: Condition name was null or empty!");
		}
		else if (serviceDef == null) {
			LOG.debug("initializeConditionEvaluators: Servicedef was null!");
		} else if (CollectionUtils.isEmpty(serviceDef.getPolicyConditions())) {
			LOG.debug("initializeConditionEvaluators: Policy conditions collection of the service def is empty!  Ok, skipping.");
		} else {
			Iterator<RangerPolicyConditionDef> iterator = serviceDef.getPolicyConditions().iterator();
			while (iterator.hasNext() && result == null) {
				RangerPolicyConditionDef conditionDef = iterator.next();
				String name = conditionDef.getName();
				if (conditionName.equals(name)) {
					result = conditionDef;
				}
			}
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug(String.format("<== RangerDefaultPolicyEvaluator.initializeConditionEvaluators(%s -> %s)", conditionName, result));
		}
		return result;
	}

	RangerConditionEvaluator newConditionEvaluator(String className) {
		if(LOG.isDebugEnabled()) {
			LOG.debug(String.format("==> RangerDefaultPolicyEvaluator.newConditionEvaluator(%s)", className));
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
			LOG.debug(String.format("<== RangerDefaultPolicyEvaluator.newConditionEvaluator(%s)", evaluator == null ? null : evaluator.toString()));
		}
		return evaluator;
	}

    @Override
    public void evaluate(RangerAccessRequest request, RangerAccessResult result) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> RangerDefaultPolicyEvaluator.evaluate(" + request + ", " + result + ")");
        }
        RangerPolicy policy = getPolicy();

        if (policy != null && request != null && result != null) {
            boolean isMatchAttempted = false;
            boolean matchResult = false;
            boolean isHeadMatchAttempted = false;
            boolean headMatchResult = false;

            if (!result.getIsAuditedDetermined()) {
                // Need to match request.resource first. If it matches (or head matches), then only more progress can be made
                if (!isMatchAttempted) {
                    matchResult = isMatch(request.getResource());
                    isMatchAttempted = true;
                }

                // Try head match only if match was not found and ANY access was requested
                if (!matchResult) {
                    if (request.isAccessTypeAny() && !isHeadMatchAttempted) {
                        headMatchResult = matchResourceHead(request.getResource());
                        isHeadMatchAttempted = true;
                    }
                }

                if (matchResult || headMatchResult) {
                    // We are done for determining if audit is needed for this policy
                    if (policy.getIsAuditEnabled()) {
                        result.setIsAudited(true);
                    }
                }
            }

            if (!result.getIsAccessDetermined()) {
                // Try Match only if it was not attempted as part of evaluating Audit requirement
                if (!isMatchAttempted) {
                    matchResult = isMatch(request.getResource());
	                isMatchAttempted = true;
                }

                // Try Head Match only if no match was found so far AND a head match was not attempted as part of evaluating
                // Audit requirement
                if (!matchResult) {
                    if (request.isAccessTypeAny() && !isHeadMatchAttempted) {
                        headMatchResult = matchResourceHead(request.getResource());
	                    isHeadMatchAttempted = true;
                    }
                }
                // Go further to evaluate access only if match or head match was found at this point
                if (matchResult || headMatchResult) {
                    evaluatePolicyItemsForAccess(request, result);
                }
            }
        }

        if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerDefaultPolicyEvaluator.evaluate(" + request + ", " + result + ")");
		}
	}

    protected void evaluatePolicyItemsForAccess(RangerAccessRequest request, RangerAccessResult result) {
        if(LOG.isDebugEnabled()) {
            LOG.debug("==> RangerDefaultPolicyEvaluator.evaluatePolicyItemsForAccess(" + request + ", " + result + ")");
        }

        for (RangerPolicy.RangerPolicyItem policyItem : getPolicy().getPolicyItems()) {

            boolean isUserGroupMatch = matchUserGroup(policyItem, request.getUser(), request.getUserGroups());

            if (!isUserGroupMatch) {
                continue;
            }
            // This is only for Grant and Revoke access requests sent by the component. For those cases
            // Our plugin will fill in the accessType as ADMIN_ACCESS.

            if (request.isAccessTypeDelegatedAdmin()) {
                if (policyItem.getDelegateAdmin()) {
                    result.setIsAllowed(true);
                    result.setPolicyId(getPolicy().getId());
                    break;
                }
                continue;
            }

            if (CollectionUtils.isEmpty(policyItem.getAccesses())) {
                continue;
            }

            boolean accessAllowed = false;
            if (request.isAccessTypeAny()) {
                for (RangerPolicy.RangerPolicyItemAccess access : policyItem.getAccesses()) {
                    if (access.getIsAllowed()) {
                        accessAllowed = true;
                        break;
                    }
                }
            } else {
                RangerPolicy.RangerPolicyItemAccess access = getAccess(policyItem, request.getAccessType());

                if (access != null && access.getIsAllowed()) {
                    accessAllowed = true;
                }
            }
            if (accessAllowed == false) {
                continue;
            }

            boolean isCustomConditionsMatch = matchCustomConditions(policyItem, request, getConditionEvaluators());

            if (!isCustomConditionsMatch) {
                continue;
            }

            result.setIsAllowed(true);
            result.setPolicyId(getPolicy().getId());
            break;
        }
        if(LOG.isDebugEnabled()) {
            LOG.debug("<== RangerDefaultPolicyEvaluator.evaluatePolicyItemsForAccess(" + request + ", " + result + ")");
        }
    }

	@Override
	public boolean isMatch(RangerAccessResource resource) {
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

	@Override
	public boolean isSingleAndExactMatch(RangerAccessResource resource) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerDefaultPolicyEvaluator.isSingleAndExactMatch(" + resource + ")");
		}

		boolean ret = false;

		RangerServiceDef serviceDef = getServiceDef();

		if(serviceDef != null && serviceDef.getResources() != null) {
			Collection<String> resourceKeys = resource == null ? null : resource.getKeys();
			Collection<String> policyKeys   = matchers == null ? null : matchers.keySet();

			boolean keysMatch = false;

			if (resourceKeys != null && policyKeys != null) {
				keysMatch = CollectionUtils.isEqualCollection(resourceKeys, policyKeys);
			}

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

	@Override
	public boolean isAccessAllowed(Map<String, RangerPolicyResource> resources, String user, Set<String> userGroups, String accessType) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerDefaultPolicyEvaluator.isAccessAllowed(" + resources + ", " + user + ", " + userGroups + ", " + accessType + ")");
		}

		boolean ret = isAccessAllowedNoCustomConditionEval(user, userGroups, accessType) && isMatch(resources);
		
		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerDefaultPolicyEvaluator.isAccessAllowed(" + resources + ", " + user + ", " + userGroups + ", " + accessType + "): " + ret);
		}

		return ret;
	}


	protected boolean matchResourceHead(RangerAccessResource resource) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerDefaultPolicyEvaluator.matchResourceHead(" + resource + ")");
		}
		boolean ret; 
		
		if (resource == null || CollectionUtils.isEmpty(resource.getKeys())) { // sanity-check, firewalling
			LOG.debug("matchResourceHead: resource was null/empty!");
			ret = true; // null resource matches anything
		} else if (getServiceDef() == null) { // sanity-check, firewalling
			LOG.debug("matchResourceHead: service-def was null!");
			ret = false; // null policy can never match a non-empty resource 
		} else if (getPolicy() == null || getPolicy().getResources() == null) {
			LOG.debug("matchResourceHead: policy or its resources were null!");
			ret = false; // null policy can never match a non-empty resource 
		} else if (matchers == null || matchers.size() != getPolicy().getResources().size()) { // sanity-check, firewalling
			LOG.debug("matchResourceHead: matchers could be found for some of the policy resources");
			ret = false; // empty policy can never match a non-empty resources and can't be evaluated meaningfully in matchers are absent
		} else {
			if (!Sets.difference(resource.getKeys(), matchers.keySet()).isEmpty()) { // e.g. avoid using udf policy for resource that has more than db specified and vice-versa
				LOG.debug("matchResourceHead: resource/policy resource-keys mismatch. policy is incompatible with resource; can't match.");
				ret = false;
			} else {
				Set<String> policyResourceNames = matchers.keySet();
				boolean skipped = false;
				boolean matched = true;
				Iterator<RangerResourceDef> iterator = getServiceDef().getResources().iterator();
				while (iterator.hasNext() && matched) {
					RangerResourceDef resourceDef = iterator.next();
					String resourceName = resourceDef.getName();
					// we only work with resources that are relevant to this policy
					if (policyResourceNames.contains(resourceName)) {
						String resourceValue = resource.getValue(resourceName);
						if (StringUtils.isEmpty(resourceValue)) {
							if (LOG.isDebugEnabled()) {
								LOG.debug("Skipping matching for " + resourceName + " since it is null/empty on resource");
							}
							skipped = true; // once we skip a level all lower levels must be skippable, too
						} else if (skipped == true) {
							LOG.debug("matchResourceHead: found a lower level resource when a higer level resource was absent!");
							matched = false;
						} else if (!matchers.get(resourceName).isMatch(resourceValue)) {
							if (LOG.isDebugEnabled()) {
								LOG.debug("matchResourceHead: matcher for " + resourceName + " failed");
							}
							matched = false;
						}
					}
				}
				ret = matched;
			}
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

		if (resourceDef != null) {
			String resName = resourceDef.getName();
			String clsName = resourceDef.getMatcher();

			if (!StringUtils.isEmpty(clsName)) {
				try {
					@SuppressWarnings("unchecked")
					Class<RangerResourceMatcher> matcherClass = (Class<RangerResourceMatcher>) Class.forName(clsName);

					ret = matcherClass.newInstance();
				} catch (Exception excp) {
					LOG.error("failed to instantiate resource matcher '" + clsName + "' for '" + resName + "'. Default resource matcher will be used", excp);
				}
			}


			if (ret == null) {
				ret = new RangerDefaultResourceMatcher();
			}

			if (ret != null) {
				ret.init(resourceDef, resource);
			}
		} else {
			LOG.error("RangerDefaultPolicyEvaluator: RangerResourceDef is null");
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerDefaultPolicyEvaluator.createResourceMatcher(" + resourceDef + ", " + resource + "): " + ret);
		}

		return ret;
	}

	protected boolean isMatch(Map<String, RangerPolicyResource> resources) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerDefaultPolicyEvaluator.isMatch(" + resources + ")");
		}

		boolean ret = false;

		RangerServiceDef serviceDef = getServiceDef();

		if(serviceDef != null && serviceDef.getResources() != null) {
			Collection<String> resourceKeys = resources == null ? null : resources.keySet();
			Collection<String> policyKeys   = matchers == null ? null : matchers.keySet();

			boolean keysMatch = CollectionUtils.isEmpty(resourceKeys) || (policyKeys != null && policyKeys.containsAll(resourceKeys));

			if(keysMatch) {
				for(RangerResourceDef resourceDef : serviceDef.getResources()) {
					String                resourceName   = resourceDef.getName();
					RangerPolicyResource  resourceValues = resources == null ? null : resources.get(resourceName);
					RangerResourceMatcher matcher        = matchers == null ? null : matchers.get(resourceName);

					// when no value exists for a resourceName, consider it a match only if: policy doesn't have a matcher OR matcher allows no-value resource
					if(resourceValues == null || CollectionUtils.isEmpty(resourceValues.getValues())) {
						ret = matcher == null || matcher.isMatch(null);
					} else if(matcher != null) {
						for(String resourceValue : resourceValues.getValues()) {
							ret = matcher.isMatch(resourceValue);

							if(! ret) {
								break;
							}
						}
					}

					if(! ret) {
						break;
					}
				}
			} else {
				if(LOG.isDebugEnabled()) {
					LOG.debug("isMatch(): keysMatch=false. resourceKeys=" + resourceKeys + "; policyKeys=" + policyKeys);
				}
			}
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerDefaultPolicyEvaluator.isMatch(" + resources + "): " + ret);
		}

		return ret;
	}

	protected boolean isAccessAllowedNoCustomConditionEval(String user, Set<String> userGroups, String accessType) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerDefaultPolicyEvaluator.isAccessAllowedNoCustomConditionEval(" + user + ", " + userGroups + ", " + accessType + ")");
		}

		boolean ret = false;

		if (StringUtils.isEmpty(accessType)) {
			accessType = RangerPolicyEngine.ANY_ACCESS;
		}

		boolean isAnyAccess   = StringUtils.equals(accessType, RangerPolicyEngine.ANY_ACCESS);
		boolean isAdminAccess = StringUtils.equals(accessType, RangerPolicyEngine.ADMIN_ACCESS);

		for (RangerPolicy.RangerPolicyItem policyItem : getPolicy().getPolicyItems()) {
			if (isAdminAccess) {
				if(! policyItem.getDelegateAdmin()) {
					continue;
				}
			} else if (CollectionUtils.isEmpty(policyItem.getAccesses())) {
				continue;
			} else if (isAnyAccess) {
				boolean accessAllowed = false;

				for (RangerPolicy.RangerPolicyItemAccess access : policyItem.getAccesses()) {
					if (access.getIsAllowed()) {
						accessAllowed = true;
						break;
					}
				}

				if(! accessAllowed) {
					continue;
				}
			} else {
				RangerPolicy.RangerPolicyItemAccess access = getAccess(policyItem, accessType);
				if (access == null || !access.getIsAllowed()) {
					continue;
				}
			}

			boolean isUserGroupMatch = matchUserGroup(policyItem, user, userGroups);

			if (!isUserGroupMatch) {
				continue;
			}

			ret = true;
			break;
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerDefaultPolicyEvaluator.isAccessAllowedNoCustomConditionEval(" + user + ", " + userGroups + ", " + accessType + "): " + ret);
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
