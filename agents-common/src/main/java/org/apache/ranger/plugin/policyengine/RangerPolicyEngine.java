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

package org.apache.ranger.plugin.policyengine;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyResource;

public interface RangerPolicyEngine {
	String GROUP_PUBLIC   = "public";
	String ANY_ACCESS     = "_any";
	String ADMIN_ACCESS   = "_admin";

	String AUDIT_ALL = "audit-all";
	String AUDIT_NONE = "audit-none";
	String AUDIT_DEFAULT = "audit-default";

	String getServiceName();

	RangerServiceDef getServiceDef();

	long getPolicyVersion();

	RangerAccessResult createAccessResult(RangerAccessRequest request);

	void preProcess(RangerAccessRequest request);

	void preProcess(Collection<RangerAccessRequest> requests);

	RangerAccessResult isAccessAllowed(RangerAccessRequest request, RangerAccessResultProcessor resultProcessor);

	Collection<RangerAccessResult> isAccessAllowed(Collection<RangerAccessRequest> requests, RangerAccessResultProcessor resultProcessor);

	RangerDataMaskResult evalDataMaskPolicies(RangerAccessRequest request, RangerAccessResultProcessor resultProcessor);

	RangerRowFilterResult evalRowFilterPolicies(RangerAccessRequest request, RangerAccessResultProcessor resultProcessor);

	boolean isAccessAllowed(RangerAccessResource resource, String user, Set<String> userGroups, String accessType);

	boolean isAccessAllowed(Map<String, RangerPolicyResource> resources, String user, Set<String> userGroups, String accessType);

	List<RangerPolicy> getExactMatchPolicies(RangerAccessResource resource);

	List<RangerPolicy> getExactMatchPolicies(Map<String, RangerPolicyResource> resources);

	List<RangerPolicy> getAllowedPolicies(String user, Set<String> userGroups, String accessType);

	RangerResourceAccessInfo getResourceAccessInfo(RangerAccessRequest request);

	boolean preCleanup();

	void setUseForwardedIPAddress(boolean useForwardedIPAddress);

	void setTrustedProxyAddresses(String[] trustedProxyAddresses);

	void cleanup();
}
