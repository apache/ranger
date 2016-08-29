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
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyResource;
import org.apache.ranger.plugin.model.RangerServiceDef.RangerResourceDef;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.policyengine.RangerAccessResource;
import org.apache.ranger.plugin.resourcematcher.RangerDefaultResourceMatcher;
import org.apache.ranger.plugin.resourcematcher.RangerResourceMatcher;

import com.google.common.collect.Sets;

public class RangerDefaultPolicyResourceMatcher implements RangerPolicyResourceMatcher {
	private static final Log LOG = LogFactory.getLog(RangerDefaultPolicyResourceMatcher.class);

	protected RangerServiceDef                  serviceDef     = null;
	protected Map<String, RangerPolicyResource> policyResources = null;

	private Map<String, RangerResourceMatcher> matchers = null;

	@Override
	public void setServiceDef(RangerServiceDef serviceDef) {
		this.serviceDef = serviceDef;
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

		this.matchers = new HashMap<String, RangerResourceMatcher>();

		if(policyResources != null && serviceDef != null) {
			for(RangerResourceDef resourceDef : serviceDef.getResources()) {
				String               resourceName   = resourceDef.getName();
				RangerPolicyResource policyResource = policyResources.get(resourceName);

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
	public boolean isSingleAndExactMatch(RangerAccessResource resource) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerDefaultPolicyResourceMatcher.isSingleAndExactMatch(" + resource + ")");
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
			LOG.debug("<== RangerDefaultPolicyResourceMatcher.isSingleAndExactMatch(" + resource + "): " + ret);
		}

		return ret;
	}

	@Override
	public boolean isHeadMatch(RangerAccessResource resource) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerDefaultPolicyResourceMatcher.matchResourceHead(" + resource + ")");
		}
		boolean ret; 
		
		if (resource == null || CollectionUtils.isEmpty(resource.getKeys())) { // sanity-check, firewalling
			LOG.debug("isHeadMatch: resource was null/empty!");
			ret = true; // null resource matches anything
		} else if (serviceDef == null) { // sanity-check, firewalling
			LOG.debug("isHeadMatch: service-def was null!");
			ret = false; // null policy can never match a non-empty resource 
		} else if (policyResources == null) {
			LOG.debug("isHeadMatch: policyResources were null!");
			ret = false; // null policy can never match a non-empty resource 
		} else if (matchers == null || matchers.size() != policyResources.size()) { // sanity-check, firewalling
			LOG.debug("isHeadMatch: matchers could be found for some of the policy resources");
			ret = false; // empty policy can never match a non-empty resources and can't be evaluated meaningfully in matchers are absent
		} else {
			if (!Sets.difference(resource.getKeys(), matchers.keySet()).isEmpty()) { // e.g. avoid using udf policy for resource that has more than db specified and vice-versa
				LOG.debug("isHeadMatch: resource/policy resource-keys mismatch. policy is incompatible with resource; can't match.");
				ret = false;
			} else {
				Set<String> policyResourceNames = matchers.keySet();
				boolean skipped = false;
				boolean matched = true;
				Iterator<RangerResourceDef> iterator = serviceDef.getResources().iterator();
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
							LOG.debug("isHeadMatch: found a lower level resource when a higer level resource was absent!");
							matched = false;
						} else if (!matchers.get(resourceName).isMatch(resourceValue)) {
							if (LOG.isDebugEnabled()) {
								LOG.debug("isHeadMatch: matcher for " + resourceName + " failed");
							}
							matched = false;
						}
					}
				}
				ret = matched;
			}
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerDefaultPolicyResourceMatcher.matchResourceHead(" + resource + "): " + ret);
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
