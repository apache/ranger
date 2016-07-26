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
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyResource;
import org.apache.ranger.plugin.model.RangerServiceDef.RangerResourceDef;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.model.validation.RangerServiceDefHelper;
import org.apache.ranger.plugin.policyengine.RangerAccessResource;
import org.apache.ranger.plugin.resourcematcher.RangerDefaultResourceMatcher;
import org.apache.ranger.plugin.resourcematcher.RangerResourceMatcher;

import com.google.common.collect.Sets;

public class RangerDefaultPolicyResourceMatcher implements RangerPolicyResourceMatcher {
	private static final Log LOG = LogFactory.getLog(RangerDefaultPolicyResourceMatcher.class);

	protected RangerServiceDef                  serviceDef      = null;
	protected RangerPolicy                      policy          = null;
	protected Map<String, RangerPolicyResource> policyResources = null;

	private Map<String, RangerResourceMatcher> matchers = null;
	private List<RangerResourceDef> firstValidResourceDefHierarchy;

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
	public void init() {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerDefaultPolicyResourceMatcher.init()");
		}

		String errorText = "";

		if(policyResources != null && policyResources.size() > 0 && serviceDef != null) {

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

					matchers = new HashMap<String, RangerResourceMatcher>();

					for (RangerResourceDef resourceDef : firstValidResourceDefHierarchy) {

						String resourceName = resourceDef.getName();
						RangerPolicyResource policyResource = policyResources.get(resourceName);

						if (policyResource != null) {
							RangerResourceMatcher matcher = createResourceMatcher(resourceDef, policyResource);

							if (matcher != null) {
								matchers.put(resourceName, matcher);
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
	public boolean isMatch(RangerAccessResource resource) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerDefaultPolicyResourceMatcher.isMatch(" + resource + ")");
		}

		boolean ret = false;

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
			LOG.debug("<== RangerDefaultPolicyResourceMatcher.isMatch(" + resource + "): " + ret);
		}

		return ret;
	}


	@Override
	public boolean isMatch(Map<String, RangerPolicyResource> resources) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerDefaultPolicyResourceMatcher.isMatch(" + resources + ")");
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
			LOG.debug("<== RangerDefaultPolicyResourceMatcher.isMatch(" + resources + "): " + ret);
		}

		return ret;
	}

	@Override
	public boolean isCompleteMatch(RangerAccessResource resource) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerDefaultPolicyResourceMatcher.isCompleteMatch(" + resource + ")");
		}

		boolean ret = false;

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
						ret = matcher == null || matcher.isCompleteMatch(resourceValue);
					} else {
						ret = matcher != null && matcher.isCompleteMatch(resourceValue);
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
			LOG.debug("<== RangerDefaultPolicyResourceMatcher.isCompleteMatch(" + resource + "): " + ret);
		}

		return ret;
	}

	@Override
	public boolean isHeadMatch(RangerAccessResource resource) {

		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerDefaultPolicyResourceMatcher.isHeadMatch(" + resource + ")");
		}

		boolean ret = false;
		
		if (matchers == null) {

			LOG.debug("RangerDefaultPolicyResourceMatcher.isHeadMatch(): PolicyResourceMatcher not initialized correctly!!!");
			ret = false;

		} else if (resource == null || CollectionUtils.isEmpty(resource.getKeys())) { // sanity-check, firewalling

			LOG.debug("RangerDefaultPolicyResourceMatcher.isHeadMatch: resource was null/empty!");
			ret = true; // null resource matches anything

		} else {

			ret = newIsHeadMatch(resource);

		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerDefaultPolicyResourceMatcher.matchResourceHead(" + resource + "): " + ret);
		}

		return ret;
	}

	@Override
	public boolean isExactHeadMatch(RangerAccessResource resource) {

		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerDefaultPolicyResourceMatcher.isExactHeadMatch(" + resource + ")");
		}

		boolean ret = false;

		if (matchers == null) {

			LOG.debug("RangerDefaultPolicyResourceMatcher.isExactHeadMatch(): PolicyResourceMatcher not initialized correctly!!!");
			ret = false;

		} else if (resource == null || CollectionUtils.isEmpty(resource.getKeys())) { // sanity-check, firewalling

			LOG.debug("RangerDefaultPolicyResourceMatcher.isExactHeadMatch: resource was null/empty!");
			ret = false;

		} else if (matchers.size() > resource.getKeys().size()) {

			LOG.debug("RangerDefaultPolicyResourceMatcher.isExactHeadMatch: more levels specified in PolicyResourceMatcher than in resource being matched!!");
			ret = false;

		} else {

			ret = newIsHeadMatch(resource);

		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerDefaultPolicyResourceMatcher.isExactHeadMatch(" + resource + ")" + ret);
		}

		return ret;
	}

	private boolean newIsHeadMatch(RangerAccessResource resource) {

		if (LOG.isDebugEnabled()) {
			LOG.debug("==> RangerDefaultPolicyResourceMatcher.newIsHeadMatch(" + resource + ")");
		}

		boolean skipped = false;
		boolean matched = true;

		for (RangerResourceDef resourceDef : firstValidResourceDefHierarchy) {

			String resourceName = resourceDef.getName();
			String resourceValue = resource.getValue(resourceName);
			RangerResourceMatcher matcher = matchers.get(resourceName);

			if (matcher != null) {

				if (StringUtils.isNotBlank(resourceValue)) {

					if (!skipped) {

						matched = matcher.isMatch(resourceValue);

					} else {

						matched = false;

					}
				} else {

					skipped = true;

				}
			}

			if (!matched) {
				break;
			}
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== RangerDefaultPolicyResourceMatcher.newIsHeadMatch(" + resource + "): " + matched);
		}

		return matched;
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

	@Override
	public boolean isCompleteMatch(Map<String, RangerPolicyResource> resources) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerDefaultPolicyResourceMatcher.isCompleteMatch(" + resources + ")");
		}

		boolean ret = false;

		if(serviceDef != null && serviceDef.getResources() != null) {
			Collection<String> resourceKeys = resources == null ? null : resources.keySet();
			Collection<String> policyKeys   = matchers == null ? null : matchers.keySet();

			boolean keysMatch = false;

			if (resourceKeys != null && policyKeys != null) {
				keysMatch = CollectionUtils.isEqualCollection(resourceKeys, policyKeys);
			}

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
			LOG.debug("<== RangerDefaultPolicyResourceMatcher.isCompleteMatch(" + resources + "): " + ret);
		}

		return ret;
	}
}
