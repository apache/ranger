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

package org.apache.ranger.plugin.service;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.ranger.plugin.contextenricher.RangerContextEnricher;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.policyengine.RangerAccessRequest;
import org.apache.ranger.plugin.policyengine.RangerAccessRequestImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessResource;
import org.apache.ranger.plugin.policyengine.RangerAccessResult;
import org.apache.ranger.plugin.policyengine.RangerAccessResultProcessor;
import org.apache.ranger.plugin.policyengine.RangerMutableResource;
import org.apache.ranger.plugin.policyengine.RangerPolicyEngine;
import org.apache.ranger.plugin.policyengine.RangerResourceACLs;
import org.apache.ranger.plugin.policyengine.RangerResourceAccessInfo;
import org.apache.ranger.plugin.util.GrantRevokeRequest;
import org.apache.ranger.plugin.util.RangerAccessRequestUtil;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class RangerAuthContext implements RangerPolicyEngine {
    private RangerPolicyEngine policyEngine;
    private Map<RangerContextEnricher, Object> requestContextEnrichers;

    protected RangerAuthContext() {
        this(null, null);
    }

    RangerAuthContext(RangerPolicyEngine policyEngine, Map<RangerContextEnricher, Object> requestContextEnrichers) {
        this.policyEngine = policyEngine;
        this.requestContextEnrichers = requestContextEnrichers;
    }

    RangerAuthContext(RangerAuthContext other) {
    	if (other != null) {
		    this.policyEngine = other.getPolicyEngine();
		    Map<RangerContextEnricher, Object> localReference = other.requestContextEnrichers;
		    if (MapUtils.isNotEmpty(localReference)) {
			    this.requestContextEnrichers = new HashMap<>(localReference);
		    }
	    }
    }
    public RangerPolicyEngine getPolicyEngine() {
        return policyEngine;
    }

    void setPolicyEngine(RangerPolicyEngine policyEngine) { this.policyEngine = policyEngine; }

    public Map<RangerContextEnricher, Object> getRequestContextEnrichers() {
        return requestContextEnrichers;
    }

    public void addOrReplaceRequestContextEnricher(RangerContextEnricher enricher, Object database) {
        if (requestContextEnrichers == null) {
            requestContextEnrichers = new HashMap<>();
        }

        requestContextEnrichers.put(enricher, database);
    }

    public void cleanupRequestContextEnricher(RangerContextEnricher enricher) {
        if (requestContextEnrichers != null) {
            requestContextEnrichers.remove(enricher);
        }
    }

    @Override
    public void setUseForwardedIPAddress(boolean useForwardedIPAddress) {
        policyEngine.setUseForwardedIPAddress(useForwardedIPAddress);
    }

    @Override
    public void setTrustedProxyAddresses(String[] trustedProxyAddresses) {
        policyEngine.setTrustedProxyAddresses(trustedProxyAddresses);
    }

	@Override
	public boolean getUseForwardedIPAddress() {
		return policyEngine.getUseForwardedIPAddress();
	}

	@Override
	public String[] getTrustedProxyAddresses() {
		return policyEngine.getTrustedProxyAddresses();
	}

    @Override
    public RangerServiceDef getServiceDef() {
        return policyEngine.getServiceDef();
    }

    @Override
    public long getPolicyVersion() {
        return policyEngine.getPolicyVersion();
    }

    public Collection<RangerAccessResult> isAccessAllowed(Collection<RangerAccessRequest> requests, RangerAccessResultProcessor resultProcessor) {
        preProcess(requests);
        return policyEngine.evaluatePolicies(requests, RangerPolicy.POLICY_TYPE_ACCESS, resultProcessor);
    }

    public RangerAccessResult isAccessAllowed(RangerAccessRequest request, RangerAccessResultProcessor resultProcessor) {
        preProcess(request);
        return policyEngine.evaluatePolicies(request, RangerPolicy.POLICY_TYPE_ACCESS, resultProcessor);
    }

    public RangerAccessResult evalDataMaskPolicies(RangerAccessRequest request, RangerAccessResultProcessor resultProcessor) {
        preProcess(request);
        return policyEngine.evaluatePolicies(request, RangerPolicy.POLICY_TYPE_DATAMASK, resultProcessor);
    }

    public RangerAccessResult evalRowFilterPolicies(RangerAccessRequest request, RangerAccessResultProcessor resultProcessor) {
        preProcess(request);
        return policyEngine.evaluatePolicies(request, RangerPolicy.POLICY_TYPE_ROWFILTER, resultProcessor);
    }

    @Override
    public void preProcess(RangerAccessRequest request) {

        RangerAccessResource resource = request.getResource();
        if (resource.getServiceDef() == null) {
	        if (resource instanceof RangerMutableResource) {
		        RangerMutableResource mutable = (RangerMutableResource) resource;
		        mutable.setServiceDef(getServiceDef());
	        }
        }
	    if (request instanceof RangerAccessRequestImpl) {
		    ((RangerAccessRequestImpl) request).extractAndSetClientIPAddress(getUseForwardedIPAddress(), getTrustedProxyAddresses());
	    }

	    RangerAccessRequestUtil.setCurrentUserInContext(request.getContext(), request.getUser());
	    if (MapUtils.isNotEmpty(requestContextEnrichers)) {
            for (Map.Entry<RangerContextEnricher, Object> entry : requestContextEnrichers.entrySet()) {
                entry.getKey().enrich(request);
            }
        }
    }

    @Override
    public void preProcess(Collection<RangerAccessRequest> requests) {
        if (CollectionUtils.isNotEmpty(requests)) {
            for (RangerAccessRequest request : requests) {
                preProcess(request);
            }
        }
    }

    @Override
    public RangerAccessResult evaluatePolicies(RangerAccessRequest request, int policyType, RangerAccessResultProcessor resultProcessor) {
        return policyEngine.evaluatePolicies(request, policyType, resultProcessor);
    }

    @Override
    public Collection<RangerAccessResult> evaluatePolicies(Collection<RangerAccessRequest> requests, int policyType, RangerAccessResultProcessor resultProcessor) {
        return policyEngine.evaluatePolicies(requests, policyType, resultProcessor);
    }

	@Override
	public RangerResourceACLs getResourceACLs(RangerAccessRequest request) {
		preProcess(request);
		return policyEngine.getResourceACLs(request);
	}

	@Override
	public String getMatchedZoneName(GrantRevokeRequest grantRevokeRequest) {
		return policyEngine.getMatchedZoneName(grantRevokeRequest);
	}

    @Override
    public boolean preCleanup() {
        return policyEngine.preCleanup();
    }

    @Override
    public void cleanup() {
        policyEngine.cleanup();
    }

    @Override
    public RangerResourceAccessInfo getResourceAccessInfo(RangerAccessRequest request) {
        preProcess(request);
        return policyEngine.getResourceAccessInfo(request);
    }

    @Override
    public List<RangerPolicy> getMatchingPolicies(RangerAccessResource resource) {
        RangerAccessRequestImpl request = new RangerAccessRequestImpl(resource, RangerPolicyEngine.ANY_ACCESS, null, null);
        preProcess(request);
        return getMatchingPolicies(request);
    }

    @Override
    public List<RangerPolicy> getMatchingPolicies(RangerAccessRequest request) {
        return policyEngine.getMatchingPolicies(request);
    }

    /* This API is called for a long running policy-engine. Not needed here */
    @Override
    public void reorderPolicyEvaluators() {
    }

    /* The following APIs are used only by ranger-admin. Providing dummy implementation */
    @Override
    public boolean isAccessAllowed(RangerAccessResource resource, String user, Set<String> userGroups, String accessType) {
        return false;
    }

    @Override
	public boolean isAccessAllowed(RangerPolicy policy, String user, Set<String> userGroups, String accessType) {
    	return false;
    }

	@Override
    public boolean isAccessAllowed(Map<String, RangerPolicy.RangerPolicyResource> resources, String user, Set<String> userGroups, String accessType) {
        return false;
    }

    @Override
    public List<RangerPolicy> getExactMatchPolicies(RangerPolicy policy, Map<String, Object> evalContext) {
        return null;
    }

    @Override
    public List<RangerPolicy> getExactMatchPolicies(RangerAccessResource resource, Map<String, Object> evalContext) {
        return null;
    }

    @Override
    public List<RangerPolicy> getAllowedPolicies(String user, Set<String> userGroups, String accessType) {
        return null;
    }

}
