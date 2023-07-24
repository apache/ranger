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

package org.apache.ranger.plugin.resourcematcher;


import org.apache.ranger.plugin.policyengine.RangerAccessRequest.ResourceElementMatchingScope;
import org.apache.ranger.plugin.policyengine.RangerAccessRequest.ResourceElementMatchType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;

import static org.apache.ranger.plugin.policyengine.RangerAccessRequest.ResourceElementMatchType.NONE;


public class RangerDefaultResourceMatcher extends RangerAbstractResourceMatcher {
	private static final Logger LOG = LoggerFactory.getLogger(RangerDefaultResourceMatcher.class);

	@Override
	public boolean isMatch(Object resource, ResourceElementMatchingScope matchingScope, Map<String, Object> evalContext) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerDefaultResourceMatcher.isMatch(" + resource + ", " + evalContext + ")");
		}

		ResourceElementMatchType matchType = getMatchType(resource, matchingScope, evalContext);
		boolean                  ret       = ResourceMatcher.isMatch(matchType, matchingScope);

		if (ret == false) {
			if(LOG.isDebugEnabled()) {
				StringBuilder sb = new StringBuilder();
				sb.append("[");
				for (String policyValue: policyValues) {
					sb.append(policyValue);
					sb.append(" ");
				}
				sb.append("]");

				LOG.debug("RangerDefaultResourceMatcher.isMatch returns FALSE, (resource=" + resource + ", policyValues=" + sb.toString() + ")");
			}
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerDefaultResourceMatcher.isMatch(" + resource + ", " + evalContext + "): " + ret);
		}

		return ret;
	}

	@Override
	public ResourceElementMatchType getMatchType(Object resource, ResourceElementMatchingScope matchingScope, Map<String, Object> evalContext) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerDefaultResourceMatcher.getMatchType(" + resource + ", " + evalContext + ")");
		}

		ResourceElementMatchType ret                = NONE;
		boolean                  allValuesRequested = isAllValuesRequested(resource);
		boolean                  isPrefixMatch      = matchingScope == ResourceElementMatchingScope.SELF_OR_PREFIX;

		if (isMatchAny || (allValuesRequested && !isPrefixMatch)) {
			ret = isMatchAny ? ResourceElementMatchType.SELF : NONE;
		} else {
			if (resource instanceof String) {
				String strValue = (String) resource;

				for (ResourceMatcher resourceMatcher : resourceMatchers.getResourceMatchers()) {
					ResourceElementMatchType matchType = resourceMatcher.getMatchType(strValue, matchingScope, evalContext);

					if (matchType != NONE) {
						ret = matchType;
					}

					if (ret == ResourceElementMatchType.SELF) {
						break;
					}
				}
			} else if (resource instanceof Collection) {
				@SuppressWarnings("unchecked")
				Collection<String> resourceValues = (Collection<String>) resource;

				for (ResourceMatcher resourceMatcher : resourceMatchers.getResourceMatchers()) {
					for (String resourceValue : resourceValues) {
						ResourceElementMatchType matchType = resourceMatcher.getMatchType(resourceValue, matchingScope, evalContext);

						if (matchType != NONE) {
							ret = matchType;
						}

						if (ret == ResourceElementMatchType.SELF) {
							break;
						}
					}

					if (ret == ResourceElementMatchType.SELF) {
						break;
					}
				}
			}
		}

		ret = applyExcludes(allValuesRequested, ret);

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerDefaultResourceMatcher.getMatchType(" + resource + ", " + evalContext + "): " + ret);
		}

		return ret;
	}

	public StringBuilder toString(StringBuilder sb) {
		sb.append("RangerDefaultResourceMatcher={");

		super.toString(sb);

		sb.append("}");

		return sb;
	}
}
