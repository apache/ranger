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

import java.util.Comparator;
import java.util.Map;

import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyResource;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.model.validation.RangerServiceDefHelper;
import org.apache.ranger.plugin.policyengine.RangerAccessRequest.ResourceElementMatchingScope;
import org.apache.ranger.plugin.policyengine.RangerAccessResource;
import org.apache.ranger.plugin.policyengine.RangerPluginContext;
import org.apache.ranger.plugin.resourcematcher.RangerResourceMatcher;

public interface RangerPolicyResourceMatcher {
	enum MatchScope { SELF, SELF_OR_DESCENDANT, SELF_OR_ANCESTOR, DESCENDANT, ANCESTOR, ANY, SELF_AND_ALL_DESCENDANTS}
	enum MatchType { NONE, SELF, DESCENDANT, ANCESTOR, SELF_AND_ALL_DESCENDANTS}

	Comparator<MatchType> MATCH_TYPE_COMPARATOR = new MatchTypeComparator();

	void init();

	void setServiceDef(RangerServiceDef serviceDef);

	void setPolicy(RangerPolicy policy);

	void setPolicyResources(Map<String, RangerPolicyResource> policyResources);

	void setPolicyResources(Map<String, RangerPolicyResource> policyResources, int policyType);

	void setServiceDefHelper(RangerServiceDefHelper serviceDefHelper);

	void setPluginContext(RangerPluginContext pluginContext);

	RangerServiceDef getServiceDef();

	RangerResourceMatcher getResourceMatcher(String resourceName);

	boolean isMatch(RangerAccessResource resource, Map<String, Object> evalContext);

	boolean isMatch(RangerAccessResource resource, Map<String, ResourceElementMatchingScope> scopes, Map<String, Object> evalContext);

	boolean isMatch(Map<String, RangerPolicyResource> resources, Map<String, Object> evalContext);

	boolean isMatch(Map<String, RangerPolicyResource> resources, Map<String, ResourceElementMatchingScope> scopes, Map<String, Object> evalContext);

	boolean isMatch(RangerAccessResource resource, MatchScope scope, Map<String, Object> evalContext);

	boolean isMatch(RangerAccessResource resource, Map<String, ResourceElementMatchingScope> scopes, MatchScope scope, Map<String, Object> evalContext);

	boolean isMatch(RangerPolicy policy, MatchScope scope, Map<String, Object> evalContext);

	boolean isMatch(RangerPolicy policy, Map<String, ResourceElementMatchingScope> scopes, MatchScope scope, Map<String, Object> evalContext);

	MatchType getMatchType(RangerAccessResource resource, Map<String, Object> evalContext);

	MatchType getMatchType(RangerAccessResource resource, Map<String, ResourceElementMatchingScope> scopes, Map<String, Object> evalContext);

	boolean isCompleteMatch(RangerAccessResource resource, Map<String, Object> evalContext);

	boolean isCompleteMatch(Map<String, RangerPolicyResource> resources, Map<String, Object> evalContext);

	boolean getNeedsDynamicEval();

	StringBuilder toString(StringBuilder sb);

	// order: SELF, SELF_AND_ALL_DESCENDANTS, ANCESTOR, DESCENDANT, NONE
	class MatchTypeComparator implements Comparator<MatchType> {
		@Override
		public int compare(MatchType o1, MatchType o2) {
			final int ret;

			if (o1 == o2) {
				ret = 0;
			} else if (o1 == null) {
				return 1;
			} else if (o2 == null) {
				return -1;
			} else {
				switch (o1) {
					case SELF:
						ret = -1;
					break;

					case SELF_AND_ALL_DESCENDANTS:
						ret = o2 == MatchType.SELF ? 1 : -1;
					break;

					case ANCESTOR:
						ret = (o2 == MatchType.SELF || o2 == MatchType.SELF_AND_ALL_DESCENDANTS) ? 1 : -1;
					break;

					case DESCENDANT:
						ret = o2 == MatchType.NONE ? -1 : 1;
					break;

					case NONE:
					default:
						ret = 1;
					break;
				}
			}

			return ret;
		}
	}
}
