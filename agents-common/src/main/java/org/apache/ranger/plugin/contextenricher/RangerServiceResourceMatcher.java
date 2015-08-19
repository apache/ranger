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

package org.apache.ranger.plugin.contextenricher;

import org.apache.ranger.plugin.model.RangerServiceResource;
import org.apache.ranger.plugin.policyengine.RangerAccessResource;
import org.apache.ranger.plugin.policyresourcematcher.RangerPolicyResourceMatcher;

public class RangerServiceResourceMatcher {
	private final RangerServiceResource serviceResource;
	private final RangerPolicyResourceMatcher policyResourceMatcher;

	public RangerServiceResourceMatcher(final RangerServiceResource serviceResource, RangerPolicyResourceMatcher policyResourceMatcher) {
		this.serviceResource = serviceResource;
		this.policyResourceMatcher = policyResourceMatcher;
	}

	public RangerServiceResource getServiceResource() { return serviceResource; }

	public RangerPolicyResourceMatcher getPolicyResourceMatcher() { return policyResourceMatcher; }

	public boolean isMatch(RangerAccessResource requestedResource) {
		return this.policyResourceMatcher.isExactHeadMatch(requestedResource);
	}
}
