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

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyResource;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.model.RangerServiceDef.RangerResourceDef;
import org.apache.ranger.plugin.model.validation.RangerZoneResourceMatcher;
import org.apache.ranger.plugin.policyresourcematcher.RangerPolicyResourceMatcher;
import org.apache.ranger.plugin.policyresourcematcher.RangerPolicyResourceMatcher.MatchType;
import org.apache.ranger.plugin.store.EmbeddedServiceDefsUtil;
import org.apache.ranger.plugin.util.RangerResourceEvaluatorsRetriever;
import org.apache.ranger.plugin.util.ServicePolicies.SecurityZoneInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class RangerSecurityZoneMatcher {
    private static final Logger LOG = LoggerFactory.getLogger(RangerSecurityZoneMatcher.class);

    private final Map<String, RangerResourceTrie<RangerZoneResourceMatcher>> resourceZoneTrie;
    private final Set<String>                                                zonesWithTagService;
    private final RangerServiceDef                                           serviceDef;

    public RangerSecurityZoneMatcher(Map<String, SecurityZoneInfo> securityZones, RangerServiceDef serviceDef, RangerPluginContext pluginContext) {
        this.resourceZoneTrie    = new HashMap<>();
        this.zonesWithTagService = new HashSet<>();
        this.serviceDef          = serviceDef;

        buildZoneTrie(securityZones, serviceDef, pluginContext);
    }

    public boolean hasTagService(String zoneName) {
        return zonesWithTagService.contains(zoneName);
    }

    public Set<String> getZonesForResourceAndChildren(Map<String, ?> resource) {
        return getZonesForResourceAndChildren(resource, convertToAccessResource(resource));
    }

    public Set<String> getZonesForResourceAndChildren(RangerAccessResource resource) {
        return getZonesForResourceAndChildren(resource.getAsMap(), resource);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        } else if (obj == null || !getClass().equals(obj.getClass())) {
            return false;
        }

        RangerSecurityZoneMatcher other = (RangerSecurityZoneMatcher) obj;

        return Objects.equals(resourceZoneTrie, other.resourceZoneTrie) &&
               Objects.equals(zonesWithTagService, other.zonesWithTagService);
    }

    @Override
    public int hashCode() {
        return Objects.hash(resourceZoneTrie, zonesWithTagService);
    }

    private Set<String> getZonesForResourceAndChildren(Map<String, ?> resource, RangerAccessResource accessResource) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> RangerSecurityZoneMatcher.getZonesForResourceAndChildren({})", accessResource);
        }

        Set<String> ret = null;

        if (MapUtils.isNotEmpty(this.resourceZoneTrie)) {
            Collection<RangerZoneResourceMatcher> matchers = RangerResourceEvaluatorsRetriever.getEvaluators(resourceZoneTrie, resource);

            if (CollectionUtils.isNotEmpty(matchers)) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Resource:[{}], matchers:[{}]", resource, matchers);
                }

                ret = new HashSet<>(matchers.size());

                // These are potential matches. Try to really match them
                for (RangerZoneResourceMatcher matcher : matchers) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Trying to match resource:[{}] using matcher:[{}]", accessResource, matcher);
                    }

                    RangerPolicyResourceMatcher policyResourceMatcher = matcher.getPolicyResourceMatcher();
                    MatchType                   matchType             = policyResourceMatcher.getMatchType(accessResource, null);

                    if (matchType == MatchType.DESCENDANT) { // add unzoned name
                        ret.add("");
                    }

                    if (matchType != MatchType.NONE) {
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("Matched resource:[{}] using matcher:[{}]", accessResource, matcher);
                        }

                        // Actual match happened
                        ret.add(matcher.getSecurityZoneName());
                    } else {
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("Did not match resource:[{}] using matcher:[{}]", accessResource, matcher);
                        }
                    }
                }

                if (LOG.isDebugEnabled()) {
                    LOG.debug("zone-names matched resource:[{}]: {}", accessResource, ret);
                }
            }
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== RangerSecurityZoneMatcher.getZonesForResourceAndChildren({}): ret={}", accessResource, ret);
        }

        return ret;
    }

    private void buildZoneTrie(Map<String, SecurityZoneInfo> securityZones, RangerServiceDef serviceDef, RangerPluginContext pluginContext) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> RangerSecurityZoneMatcher.buildZoneTrie()");
        }

        Map<String, Boolean> resourceIsRecursive = new HashMap<>();

        if (MapUtils.isNotEmpty(securityZones)) {
            List<RangerZoneResourceMatcher> matchers = new ArrayList<>();

            for (Map.Entry<String, SecurityZoneInfo> securityZone : securityZones.entrySet()) {
                String           zoneName    = securityZone.getKey();
                SecurityZoneInfo zoneDetails = securityZone.getValue();

                if (LOG.isDebugEnabled()) {
                    LOG.debug("Building matchers for zone:[{}]", zoneName);
                }

                for (Map<String, List<String>> resource : zoneDetails.getResources()) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Building matcher for resource:[{}] in zone:[{}]", resource, zoneName);
                    }

                    Map<String, RangerPolicyResource> policyResources = new HashMap<>();

                    for (Map.Entry<String, List<String>> entry : resource.entrySet()) {
                        String       resourceDefName = entry.getKey();
                        List<String> resourceValues  = entry.getValue();
                        Boolean      isRecursive     = resourceIsRecursive.computeIfAbsent(resourceDefName, f -> EmbeddedServiceDefsUtil.isRecursiveEnabled(serviceDef, resourceDefName));

                        policyResources.put(resourceDefName, new RangerPolicyResource(resourceValues, false, isRecursive));
                    }

                    matchers.add(new RangerZoneResourceMatcher(zoneName, policyResources, serviceDef, pluginContext));

                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Built matcher for resource:[{}] in zone:[{}]", resource, zoneName);
                    }
                }

                if (LOG.isDebugEnabled()) {
                    LOG.debug("Built all matchers for zone:[{}]", zoneName);
                }

                if (Boolean.TRUE.equals(zoneDetails.getContainsAssociatedTagService())) {
                    zonesWithTagService.add(zoneName);
                }
            }

            if (LOG.isDebugEnabled()) {
                LOG.debug("Built matchers for all Zones");
            }

            RangerPolicyEngineOptions options = pluginContext.getConfig().getPolicyEngineOptions();

            for (RangerResourceDef resourceDef : serviceDef.getResources()) {
                resourceZoneTrie.put(resourceDef.getName(), new RangerResourceTrie<>(resourceDef, matchers, options.optimizeTrieForSpace, options.optimizeTrieForRetrieval, pluginContext));
            }
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== RangerSecurityZoneMatcher.buildZoneTrie()");
        }
    }

    private RangerAccessResource convertToAccessResource(Map<String, ?> resource) {
        RangerAccessResourceImpl ret = new RangerAccessResourceImpl();

        ret.setServiceDef(serviceDef);

        for (Map.Entry<String, ?> entry : resource.entrySet()) {
            ret.setValue(entry.getKey(), entry.getValue());
        }

        return ret;
    }
}
