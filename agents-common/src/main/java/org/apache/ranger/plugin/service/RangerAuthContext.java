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
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ranger.plugin.contextenricher.RangerContextEnricher;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.policyengine.RangerAccessRequest;
import org.apache.ranger.plugin.policyengine.RangerAccessRequestImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessResource;
import org.apache.ranger.plugin.policyengine.RangerAccessResult;
import org.apache.ranger.plugin.policyengine.RangerAccessResultProcessor;
import org.apache.ranger.plugin.policyengine.RangerMutableResource;
import org.apache.ranger.plugin.policyengine.RangerPluginContext;
import org.apache.ranger.plugin.policyengine.RangerPolicyEngine;
import org.apache.ranger.plugin.policyengine.RangerResourceACLs;
import org.apache.ranger.plugin.policyengine.RangerResourceAccessInfo;
import org.apache.ranger.plugin.util.GrantRevokeRequest;
import org.apache.ranger.plugin.util.RangerAccessRequestUtil;
import org.apache.ranger.plugin.util.RangerRoles;
import org.apache.ranger.plugin.util.ServicePolicies;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class RangerAuthContext implements RangerPolicyEngine {
    private static final Log LOG = LogFactory.getLog(RangerAuthContext.class);
    private final RangerPluginContext rangerPluginContext;
    private final RangerPolicyEngine policyEngine;
    private final Map<RangerContextEnricher, Object> requestContextEnrichers;

    public RangerAuthContext(RangerPolicyEngine policyEngine, Map<RangerContextEnricher, Object> requestContextEnrichers, RangerPluginContext rangerPluginContext) {
        this.policyEngine = policyEngine;
        this.requestContextEnrichers = requestContextEnrichers != null ? requestContextEnrichers : new ConcurrentHashMap<>();
        this.rangerPluginContext = rangerPluginContext;
    }

    RangerAuthContext(RangerAuthContext other) {
        if (other != null) {
            this.policyEngine = other.getPolicyEngine();
            this.rangerPluginContext = other.rangerPluginContext;
            Map<RangerContextEnricher, Object> localReference = other.requestContextEnrichers;
            if (MapUtils.isNotEmpty(localReference)) {
                this.requestContextEnrichers = new ConcurrentHashMap<>(localReference);
            } else {
                this.requestContextEnrichers = new ConcurrentHashMap<>();
            }
        } else {
            this.policyEngine            = null;
            this.requestContextEnrichers = new ConcurrentHashMap<>();
            this.rangerPluginContext     = null;
        }
    }

    public RangerAuthContext(RangerPolicyEngine policyEngine, RangerAuthContext other) {
        this.policyEngine = policyEngine;

        if (other != null) {
            Map<RangerContextEnricher, Object> localReference = other.requestContextEnrichers;

            this.rangerPluginContext     = other.rangerPluginContext;
            this.requestContextEnrichers = MapUtils.isNotEmpty(localReference) ? new ConcurrentHashMap<>(localReference) : new ConcurrentHashMap<>();
        } else {
            this.rangerPluginContext     = null;
            this.requestContextEnrichers = new ConcurrentHashMap<>();
        }
    }

    public RangerPolicyEngine getPolicyEngine() {
        return policyEngine;
    }

    public Map<RangerContextEnricher, Object> getRequestContextEnrichers() {
        return requestContextEnrichers;
    }

    public void addOrReplaceRequestContextEnricher(RangerContextEnricher enricher, Object database) {
        // concurrentHashMap does not allow null to be inserted into it, so insert a dummy which is checked
        // when enrich() is called
        requestContextEnrichers.put(enricher, database != null ? database : enricher);
    }

    public void cleanupRequestContextEnricher(RangerContextEnricher enricher) {
        requestContextEnrichers.remove(enricher);

    }

    @Override
    public void setUseForwardedIPAddress(boolean useForwardedIPAddress) {
        if (policyEngine != null) {
            policyEngine.setUseForwardedIPAddress(useForwardedIPAddress);
        }
    }

    @Override
    public void setTrustedProxyAddresses(String[] trustedProxyAddresses) {
        if (policyEngine != null) {
            policyEngine.setTrustedProxyAddresses(trustedProxyAddresses);
        }
    }

	@Override
	public boolean getUseForwardedIPAddress() {
        if (policyEngine != null) {
            return policyEngine.getUseForwardedIPAddress();
        }
        return false;
	}

	@Override
	public String[] getTrustedProxyAddresses() {
        if (policyEngine != null) {
            return policyEngine.getTrustedProxyAddresses();
        }
        return null;
	}

    @Override
    public RangerServiceDef getServiceDef() {
        if (policyEngine != null) {
            return policyEngine.getServiceDef();
        }
        return null;
    }

    @Override
    public long getPolicyVersion() {
        if (policyEngine != null) {
            return policyEngine.getPolicyVersion();
        }
        return 0L;
    }

    public Collection<RangerAccessResult> isAccessAllowed(Collection<RangerAccessRequest> requests, RangerAccessResultProcessor resultProcessor) {
        if (policyEngine != null) {
            preProcess(requests);
            return policyEngine.evaluatePolicies(requests, RangerPolicy.POLICY_TYPE_ACCESS, resultProcessor);
        }
        return null;
    }

    public RangerAccessResult isAccessAllowed(RangerAccessRequest request, RangerAccessResultProcessor resultProcessor) {
        if (policyEngine != null) {
            preProcess(request);
            return policyEngine.evaluatePolicies(request, RangerPolicy.POLICY_TYPE_ACCESS, resultProcessor);
        }
        return null;
    }

    public RangerAccessResult evalDataMaskPolicies(RangerAccessRequest request, RangerAccessResultProcessor resultProcessor) {
        if (policyEngine != null) {
            preProcess(request);
            return policyEngine.evaluatePolicies(request, RangerPolicy.POLICY_TYPE_DATAMASK, resultProcessor);
        }
        return null;
    }

    public RangerAccessResult evalRowFilterPolicies(RangerAccessRequest request, RangerAccessResultProcessor resultProcessor) {
        if (policyEngine != null) {
            preProcess(request);
            return policyEngine.evaluatePolicies(request, RangerPolicy.POLICY_TYPE_ROWFILTER, resultProcessor);
        }
        return null;
    }

    @Override
    public void preProcess(RangerAccessRequest request) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> RangerAuthContext.preProcess");
        }
        RangerAccessResource resource = request.getResource();
        if (resource.getServiceDef() == null) {
            if (resource instanceof RangerMutableResource) {
                RangerMutableResource mutable = (RangerMutableResource) resource;
                mutable.setServiceDef(getServiceDef());
            }
        }
        if (request instanceof RangerAccessRequestImpl) {
            RangerAccessRequestImpl reqImpl = (RangerAccessRequestImpl) request;
            reqImpl.extractAndSetClientIPAddress(getUseForwardedIPAddress(), getTrustedProxyAddresses());
            if (rangerPluginContext != null) {
                reqImpl.setClusterName(rangerPluginContext.getClusterName());
                reqImpl.setClusterType(rangerPluginContext.getClusterType());
            }
        }

        RangerAccessRequestUtil.setCurrentUserInContext(request.getContext(), request.getUser());

        Set<String> roles = getRolesFromUserAndGroups(request.getUser(), request.getUserGroups());

        if (CollectionUtils.isNotEmpty(roles)) {
            RangerAccessRequestUtil.setCurrentUserRolesInContext(request.getContext(), roles);
        }

        String owner = request.getResource() != null ? request.getResource().getOwnerUser() : null;

        if (StringUtils.isNotEmpty(owner)) {
            RangerAccessRequestUtil.setOwnerInContext(request.getContext(), owner);
        }

        if (MapUtils.isNotEmpty(requestContextEnrichers)) {
            for (Map.Entry<RangerContextEnricher, Object> entry : requestContextEnrichers.entrySet()) {
                if (entry.getValue() instanceof RangerContextEnricher && entry.getKey().equals(entry.getValue())) {
                    // This entry was a result of addOrReplaceRequestContextEnricher() API called with null database value
                    entry.getKey().enrich(request, null);
                } else {
                    entry.getKey().enrich(request, entry.getValue());
                }
            }
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("<== RangerAuthContext.preProcess");
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
        if (policyEngine != null) {
            return policyEngine.evaluatePolicies(request, policyType, resultProcessor);
        }
        return null;
    }

    @Override
    public Collection<RangerAccessResult> evaluatePolicies(Collection<RangerAccessRequest> requests, int policyType, RangerAccessResultProcessor resultProcessor) {
        if (policyEngine != null) {
            return policyEngine.evaluatePolicies(requests, policyType, resultProcessor);
        }
        return null;
    }

	@Override
	public RangerResourceACLs getResourceACLs(RangerAccessRequest request) {
        if (policyEngine != null) {
            preProcess(request);
            return policyEngine.getResourceACLs(request);
        }
        return null;
	}

	@Override
	public String getMatchedZoneName(GrantRevokeRequest grantRevokeRequest) {
        if (policyEngine != null) {
            return policyEngine.getMatchedZoneName(grantRevokeRequest);
        }
        return null;
	}

    @Override
    public boolean preCleanup() {
        if (policyEngine != null) {
            return policyEngine.preCleanup();
        }
        return false;
    }

    @Override
    public void cleanup() {
        if (policyEngine != null) {
            policyEngine.cleanup();
        }
    }

    @Override
    public RangerResourceAccessInfo getResourceAccessInfo(RangerAccessRequest request) {
        if (policyEngine != null) {
            preProcess(request);
            return policyEngine.getResourceAccessInfo(request);
        }
        return null;
    }

    @Override
    public List<RangerPolicy> getMatchingPolicies(RangerAccessResource resource) {
        if (policyEngine != null) {
            RangerAccessRequestImpl request = new RangerAccessRequestImpl(resource, RangerPolicyEngine.ANY_ACCESS, null, null);
            preProcess(request);
            return getMatchingPolicies(request);
        }
        return null;
    }

    @Override
    public List<RangerPolicy> getMatchingPolicies(RangerAccessRequest request) {
        if (policyEngine != null) {
            return policyEngine.getMatchingPolicies(request);
        }
        return null;
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
    public boolean isAccessAllowed(RangerPolicy policy, String user, Set<String> userGroups, Set<String> roles, String accessType) {
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

    @Override
    public RangerPolicyEngine cloneWithDelta(ServicePolicies servicePolicies, RangerRoles rangerRoles) {
        if (policyEngine != null) {
            return policyEngine.cloneWithDelta(servicePolicies, rangerRoles);
        }
        return null;
    }

    @Override
    public Set<String> getRolesFromUserAndGroups(String user, Set<String> groups) {
        if (policyEngine != null) {
            return policyEngine.getRolesFromUserAndGroups(user, groups);
        }
        return null;
    }

    public RangerRoles getRangerRoles() {
        return  policyEngine.getRangerRoles();
    }

    @Override
    public void setRangerRoles(RangerRoles rangerRoles) {
        if (policyEngine != null) {
            policyEngine.setRangerRoles(rangerRoles);
        }
    }
}
