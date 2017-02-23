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

package org.apache.ranger.plugin.policyresourcematcher;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.List;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyResource;
import org.apache.ranger.plugin.model.RangerServiceDef.RangerResourceDef;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.model.validation.RangerServiceDefHelper;
import org.apache.ranger.plugin.policyengine.RangerAccessResource;
import org.apache.ranger.plugin.policyengine.RangerAccessResourceImpl;
import org.apache.ranger.plugin.resourcematcher.RangerDefaultResourceMatcher;
import org.apache.ranger.plugin.resourcematcher.RangerResourceMatcher;

import com.google.common.collect.Sets;

public class RangerDefaultPolicyResourceMatcher implements RangerPolicyResourceMatcher {
	private static final Log LOG = LogFactory.getLog(RangerDefaultPolicyResourceMatcher.class);

	protected RangerServiceDef                  serviceDef;
	protected RangerPolicy                      policy;
	protected Map<String, RangerPolicyResource> policyResources;

	private Map<String, RangerResourceMatcher> matchers;
	private boolean                            needsDynamicEval;
	private List<RangerResourceDef> firstValidResourceDefHierarchy;
	/*
	 * For hive resource policy:
	 * 	lastNonAnyMatcherIndex will be set to
	 * 		0 : if all matchers in policy are '*'; such as database=*, table=*, column=*
	 * 		1 : database=hr, table=*, column=*
	 * 		2 : database=<any>, table=employee, column=*
	 * 		3 : database=<any>, table=<any>, column=ssn
	 */
	private int lastNonAnyMatcherIndex;

	@Override
	public void setServiceDef(RangerServiceDef serviceDef) {
		this.serviceDef = serviceDef;
	}

	@Override
	public void setPolicy(RangerPolicy policy) {
		this.policy = policy;

		setPolicyResources(policy == null ? null : policy.getResources());
	}

	@Override
	public void setPolicyResources(Map<String, RangerPolicyResource> policyResources) {
		this.policyResources = policyResources;
	}

	@Override
	public boolean getNeedsDynamicEval() {
		return needsDynamicEval;
	}

	@Override
	public void init() {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerDefaultPolicyResourceMatcher.init()");
		}

		String errorText = "";

		if(policyResources != null && !policyResources.isEmpty() && serviceDef != null) {
			Set<String> policyResourceKeySet = policyResources.keySet();

			RangerServiceDefHelper serviceDefHelper = new RangerServiceDefHelper(serviceDef, false);
			int policyType = policy != null && policy.getPolicyType() != null ? policy.getPolicyType() : RangerPolicy.POLICY_TYPE_ACCESS;
			Set<List<RangerResourceDef>> validResourceHierarchies = serviceDefHelper.getResourceHierarchies(policyType);

			for (List<RangerResourceDef> validResourceHierarchy : validResourceHierarchies) {

				Set<String> resourceDefNameSet = serviceDefHelper.getAllResourceNames(validResourceHierarchy);

				if ((Sets.difference(policyResourceKeySet, resourceDefNameSet)).isEmpty()) {

					firstValidResourceDefHierarchy = validResourceHierarchy;
					break;

				}

			}

			if (firstValidResourceDefHierarchy != null) {
				List<String> resourceDefNameOrderedList = serviceDefHelper.getAllResourceNamesOrdered(firstValidResourceDefHierarchy);

				boolean foundGapsInResourceSpecs = false;
				boolean skipped = false;

				for (String resourceDefName : resourceDefNameOrderedList) {
					RangerPolicyResource policyResource = policyResources.get(resourceDefName);
					if (policyResource == null) {
						skipped = true;
					} else if (skipped) {
						foundGapsInResourceSpecs = true;
						break;
					}
				}

				if (foundGapsInResourceSpecs) {

					errorText = "policyResources does not specify contiguous sequence in any valid resourcedef hiearchy.";
					if (LOG.isDebugEnabled()) {
						LOG.debug("RangerDefaultPolicyResourceMatcher.init() failed: Gaps found in policyResources, internal error, skipping..");
					}
					firstValidResourceDefHierarchy = null;

				} else {
					matchers = new HashMap<>();

					for (RangerResourceDef resourceDef : firstValidResourceDefHierarchy) {

						String resourceName = resourceDef.getName();
						RangerPolicyResource policyResource = policyResources.get(resourceName);

						if (policyResource != null) {
							RangerResourceMatcher matcher = createResourceMatcher(resourceDef, policyResource);

							if (matcher != null) {
								if (!needsDynamicEval && matcher.getNeedsDynamicEval()) {
									needsDynamicEval = true;
								}
								matchers.put(resourceName, matcher);
								if (!matcher.isMatchAny()) {
									lastNonAnyMatcherIndex = matchers.size();
								}
							} else {
								LOG.error("failed to find matcher for resource " + resourceName);
							}
						} else {
							if (LOG.isDebugEnabled()) {
								LOG.debug("RangerDefaultPolicyResourceMatcher.init() - no matcher created for " + resourceName + ". Continuing ...");
							}
						}
					}
				}
			} else {
				errorText = "policyResources elements are not part of any valid resourcedef hierarchy.";
			}
		} else {
			errorText = " policyResources is null or empty, or serviceDef is null.";
		}

		if(matchers == null) {
			Set<String> policyResourceKeys = policyResources == null ? null : policyResources.keySet();
			StringBuilder sb = new StringBuilder();
			if (CollectionUtils.isNotEmpty(policyResourceKeys)) {
				for (String policyResourceKeyName : policyResourceKeys) {
					sb.append(" ").append(policyResourceKeyName).append(" ");
				}
			}
			String keysString = sb.toString();
			String serviceDefName = serviceDef == null ? "" : serviceDef.getName();
			String validHierarchy = "";
			if (serviceDef != null && CollectionUtils.isNotEmpty(firstValidResourceDefHierarchy)) {
				RangerServiceDefHelper serviceDefHelper = new RangerServiceDefHelper(serviceDef, false);
				List<String> resourceDefNameOrderedList = serviceDefHelper.getAllResourceNamesOrdered(firstValidResourceDefHierarchy);

				for (String resourceDefName : resourceDefNameOrderedList) {
					validHierarchy += " " + resourceDefName + " ";
				}
			}
			LOG.warn("RangerDefaultPolicyResourceMatcher.init() failed: " + errorText + " (serviceDef=" + serviceDefName + ", policyResourceKeys=" + keysString
			+ ", validHierarchy=" + validHierarchy + ")");
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerDefaultPolicyResourceMatcher.init()");
		}
	}

	@Override
	public RangerServiceDef getServiceDef() {
		return serviceDef;
	}

	@Override
	public RangerResourceMatcher getResourceMatcher(String resourceName) {
		return matchers != null ? matchers.get(resourceName) : null;
	}

	@Override
	public boolean isMatch(Map<String, RangerPolicyResource> resources, Map<String, Object> evalContext) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerDefaultPolicyResourceMatcher.isMatch(" + resources  + ", " + evalContext + ")");
		}

		boolean ret = false;

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
						ret = matcher == null || matcher.isMatch(null, null);
					} else if(matcher != null) {
						for(String resourceValue : resourceValues.getValues()) {
							ret = matcher.isMatch(resourceValue, evalContext);

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
			LOG.debug("<== RangerDefaultPolicyResourceMatcher.isMatch(" + resources  + ", " + evalContext + "): " + ret);
		}

		return ret;
	}

	@Override
	public boolean isCompleteMatch(RangerAccessResource resource, Map<String, Object> evalContext) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerDefaultPolicyResourceMatcher.isCompleteMatch(" + resource  + ", " + evalContext + ")");
		}

		boolean ret = false;

		if(serviceDef != null && serviceDef.getResources() != null) {
			Collection<String> resourceKeys = resource == null ? null : resource.getKeys();
			Collection<String> policyKeys   = matchers == null ? null : matchers.keySet();

			boolean keysMatch = resourceKeys != null && policyKeys != null && CollectionUtils.isEqualCollection(resourceKeys, policyKeys);

			if(keysMatch) {
				for(RangerResourceDef resourceDef : serviceDef.getResources()) {
					String                resourceName  = resourceDef.getName();
					String                resourceValue = resource == null ? null : resource.getValue(resourceName);
					RangerResourceMatcher matcher       = matchers == null ? null : matchers.get(resourceName);

					if(StringUtils.isEmpty(resourceValue)) {
						ret = matcher == null || matcher.isCompleteMatch(resourceValue, evalContext);
					} else {
						ret = matcher != null && matcher.isCompleteMatch(resourceValue, evalContext);
					}

					if(! ret) {
						break;
					}
				}
			} else {
				if(LOG.isDebugEnabled()) {
					LOG.debug("isCompleteMatch(): keysMatch=false. resourceKeys=" + resourceKeys + "; policyKeys=" + policyKeys);
				}
			}
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerDefaultPolicyResourceMatcher.isCompleteMatch(" + resource  + ", " + evalContext + "): " + ret);
		}

		return ret;
	}

	@Override
	public boolean isCompleteMatch(Map<String, RangerPolicyResource> resources, Map<String, Object> evalContext) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerDefaultPolicyResourceMatcher.isCompleteMatch(" + resources + ", " + evalContext + ")");
		}

		boolean ret = false;

		if(serviceDef != null && serviceDef.getResources() != null) {
			Collection<String> resourceKeys = resources == null ? null : resources.keySet();
			Collection<String> policyKeys   = matchers == null ? null : matchers.keySet();

			boolean keysMatch = resourceKeys != null && policyKeys != null && CollectionUtils.isEqualCollection(resourceKeys, policyKeys);

			if(keysMatch) {
				for(RangerResourceDef resourceDef : serviceDef.getResources()) {
					String                resourceName   = resourceDef.getName();
					RangerPolicyResource  resourceValues = resources == null ? null : resources.get(resourceName);
					RangerPolicyResource  policyValues   = policyResources == null ? null : policyResources.get(resourceName);

					if(resourceValues == null || CollectionUtils.isEmpty(resourceValues.getValues())) {
						ret = (policyValues == null || CollectionUtils.isEmpty(policyValues.getValues()));
					} else if(policyValues != null && CollectionUtils.isNotEmpty(policyValues.getValues())) {
						ret = CollectionUtils.isEqualCollection(resourceValues.getValues(), policyValues.getValues());
					}

					if(! ret) {
						break;
					}
				}
			} else {
				if(LOG.isDebugEnabled()) {
					LOG.debug("isCompleteMatch(): keysMatch=false. resourceKeys=" + resourceKeys + "; policyKeys=" + policyKeys);
				}
			}
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerDefaultPolicyResourceMatcher.isCompleteMatch(" + resources + ", " + evalContext + "): " + ret);
		}

		return ret;
	}

	@Override
	public boolean isMatch(RangerAccessResource resource, Map<String, Object> evalContext) {
		return isMatch(resource, MatchScope.SELF_OR_ANCESTOR, evalContext);
	}

	@Override
	public boolean isMatch(RangerPolicy policy, MatchScope scope, Map<String, Object> evalContext) {

		boolean ret = false;
		MatchType matchType = MatchType.NONE;

		Map<String, RangerPolicyResource> resources = policy.getResources();

		if (MapUtils.isNotEmpty(resources)) {

			RangerAccessResourceImpl accessResource = new RangerAccessResourceImpl();
			accessResource.setServiceDef(serviceDef);

			// Build up accessResource resourceDef by resourceDef.
			// For each resourceDef,
			// 		examine policy-values one by one.
			// 		The first value that is acceptable, that is,
			// 			value matches in any way, is used for that resourceDef, and
			//			next resourceDef is processed.
			// 		If none of the values matches, the policy as a whole definitely will not match,
			//		therefore, the match is failed
			// After all resourceDefs are processed, and some match is achieved at every
			// level, the final matchType (which is for the entire policy) is checked against
			// requested scope to determine the match-result.

			// Unit tests in TestDefaultPolicyResourceForPolicy.java, test_defaultpolicyresourcematcher_for_policy.json,
			// and test_defaultpolicyresourcematcher_for_hdfs_policy.json

			for (RangerResourceDef resourceDef : firstValidResourceDefHierarchy) {

				ret = false;
				matchType = MatchType.NONE;

				String name = resourceDef.getName();
				RangerPolicyResource policyResource = resources.get(name);

				if (policyResource != null) {
					for (String value : policyResource.getValues()) {

						accessResource.setValue(name, value);

						matchType = getMatchType(accessResource, evalContext);

						if (matchType != MatchType.NONE) { // One value for this resourceDef matched
							ret = true;
							break;
						}
					}
				}

				if (!ret) { // None of the values specified for this resourceDef matched, no point in continuing with next resourceDef
					break;
				}
			}
			ret = ret && isMatch(scope, matchType);
		}
		return ret;
	}

	@Override
	public boolean isMatch(RangerAccessResource resource, MatchScope scope, Map<String, Object> evalContext) {

		final boolean ret;

		MatchType matchType = getMatchType(resource, evalContext);
		ret = isMatch(scope, matchType);

		return ret;
	}

	@Override
	public MatchType getMatchType(RangerAccessResource resource, Map<String, Object> evalContext) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> RangerDefaultPolicyResourceMatcher.getMatchType(" + resource + evalContext + ")");
		}

		int matchersSize = matchers == null ? 0 : matchers.size();
		int resourceKeysSize = resource == null || resource.getKeys() == null ? 0 : resource.getKeys().size();

		MatchType ret = MatchType.NONE;

		if (!isValid(resource)) {
			ret = MatchType.NONE;
		} else if (matchersSize == 0 || lastNonAnyMatcherIndex == 0) {
			ret = resourceKeysSize == 0 ? MatchType.SELF : MatchType.ANCESTOR;
		} else if (resourceKeysSize == 0) {
			ret = MatchType.DESCENDANT;
		} else {
			int index = 0;
			for (RangerResourceDef resourceDef : firstValidResourceDefHierarchy) {

				String resourceName = resourceDef.getName();
				RangerResourceMatcher matcher = matchers.get(resourceName);
				String resourceValue = resource.getValue(resourceName);

				if (resourceValue != null) {
					if (matcher != null) {
						index++;
						if (matcher.isMatch(resourceValue, evalContext)) {
							ret = index == resourceKeysSize && matcher.isMatchAny() ? MatchType.ANCESTOR : MatchType.SELF;
						} else {
							ret = MatchType.NONE;
							break;
						}
					} else {
						// More resource-levels than matchers
						ret = MatchType.ANCESTOR;
						break;
					}
				} else {
					if (matcher != null) {
						// More matchers than resource-levels
						if (index >= lastNonAnyMatcherIndex) {
							// All AnyMatch matchers after this
							ret = MatchType.ANCESTOR;
						} else {
							ret = MatchType.DESCENDANT;
						}
					} else {
						// Common part of several possible hierarchies matched
						if (resourceKeysSize > index) {
							ret = MatchType.ANCESTOR;
						}
					}
					break;
				}
			}

		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== RangerDefaultPolicyResourceMatcher.getMatchType(" + resource + evalContext + "): " + ret);
		}

		return ret;
	}

	private boolean isValidResourceDefHierachyForResource(List<RangerResourceDef> resourceHierarchy, RangerAccessResource resource) {
		boolean foundAllResourceKeys = true;

		for (String resourceKey : resource.getKeys()) {
			boolean found = false;
			for (RangerResourceDef resourceDef : resourceHierarchy) {
				if (resourceDef.getName().equals(resourceKey)) {
					found = true;
					break;
				}
			}
			if (!found) {
				foundAllResourceKeys = false;
				break;
			}
		}

		return foundAllResourceKeys;
	}

	private boolean isValid(RangerAccessResource resource) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> RangerDefaultPolicyResourceMatcher.isValid(" + resource + ")");
		}

		boolean ret = true;

		if (matchers != null && resource != null && resource.getKeys() != null) {
			if (matchers.keySet().containsAll(resource.getKeys()) || resource.getKeys().containsAll(matchers.keySet())) {

				List<RangerResourceDef> aValidHierarchy = null;

				if (resource.getKeys().containsAll(matchers.keySet()) && resource.getKeys().size() > matchers.keySet().size()) {
					if (isValidResourceDefHierachyForResource(firstValidResourceDefHierarchy, resource)) {
						aValidHierarchy = firstValidResourceDefHierarchy;
					} else {
						RangerServiceDefHelper serviceDefHelper = new RangerServiceDefHelper(serviceDef, false);
						int policyType = policy != null && policy.getPolicyType() != null ? policy.getPolicyType() : RangerPolicy.POLICY_TYPE_ACCESS;
						Set<List<RangerResourceDef>> validResourceHierarchies = serviceDefHelper.getResourceHierarchies(policyType);

						for (List<RangerResourceDef> resourceHierarchy : validResourceHierarchies) {
							if (resourceHierarchy == firstValidResourceDefHierarchy) { // Pointer comparison
								// firstValidResourceDefHierarchy is already checked before and it does not match
								continue;
							}

							if (isValidResourceDefHierachyForResource(resourceHierarchy, resource)) {
								aValidHierarchy = resourceHierarchy;
								break;
							}
						}
					}
				} else {
					aValidHierarchy = firstValidResourceDefHierarchy;
				}

				if (aValidHierarchy != null) {
					boolean skipped = false;

					for (RangerResourceDef resourceDef : aValidHierarchy) {

						String resourceName = resourceDef.getName();
						String resourceValue = resource.getValue(resourceName);

						if (resourceValue == null) {
							if (!skipped) {
								skipped = true;
							}
						} else {
							if (skipped) {
								ret = false;
								break;
							}
						}

					}
				} else {
					ret = false;
				}
			} else {
				ret = false;
			}
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== RangerDefaultPolicyResourceMatcher.isValid(" + resource + "): " + ret);
		}

		return ret;
	}

	private boolean isMatch(final MatchScope scope, final MatchType matchType) {
		final boolean ret;
		switch (scope) {
			case SELF_OR_ANCESTOR_OR_DESCENDANT: {
				ret = matchType != MatchType.NONE;
				break;
			}
			case SELF: {
				ret = matchType == MatchType.SELF;
				break;
			}
			case SELF_OR_DESCENDANT: {
				ret = matchType == MatchType.SELF || matchType == MatchType.DESCENDANT;
				break;
			}
			case SELF_OR_ANCESTOR: {
				ret = matchType == MatchType.SELF || matchType == MatchType.ANCESTOR;
				break;
			}
			case DESCENDANT: {
				ret = matchType == MatchType.DESCENDANT;
				break;
			}
			case ANCESTOR: {
				ret = matchType == MatchType.ANCESTOR;
				break;
			}
			default:
				ret = matchType != MatchType.NONE;
				break;
		}
		return ret;
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();

		toString(sb);

		return sb.toString();
	}

	@Override
	public StringBuilder toString(StringBuilder sb) {
		sb.append("RangerDefaultPolicyResourceMatcher={");

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

	protected static RangerResourceMatcher createResourceMatcher(RangerResourceDef resourceDef, RangerPolicyResource resource) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerDefaultPolicyResourceMatcher.createResourceMatcher(" + resourceDef + ", " + resource + ")");
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
				ret.setResourceDef(resourceDef);
				ret.setPolicyResource(resource);
				ret.init();
			}
		} else {
			LOG.error("RangerDefaultPolicyResourceMatcher: RangerResourceDef is null");
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerDefaultPolicyResourceMatcher.createResourceMatcher(" + resourceDef + ", " + resource + "): " + ret);
		}

		return ret;
	}

}
