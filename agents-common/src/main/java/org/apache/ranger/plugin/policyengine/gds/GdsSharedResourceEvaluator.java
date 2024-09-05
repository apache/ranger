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

package org.apache.ranger.plugin.policyengine.gds;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.ranger.plugin.conditionevaluator.RangerConditionEvaluator;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItemDataMaskInfo;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItemRowFilterInfo;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyResource;
import org.apache.ranger.plugin.model.RangerServiceDef.RangerResourceDef;
import org.apache.ranger.plugin.model.validation.RangerServiceDefHelper;
import org.apache.ranger.plugin.policyengine.RangerAccessRequest;
import org.apache.ranger.plugin.policyengine.RangerPluginContext;
import org.apache.ranger.plugin.policyengine.RangerResourceACLs;
import org.apache.ranger.plugin.policyevaluator.RangerCustomConditionEvaluator;
import org.apache.ranger.plugin.policyresourcematcher.RangerDefaultPolicyResourceMatcher;
import org.apache.ranger.plugin.policyresourcematcher.RangerPolicyResourceMatcher;
import org.apache.ranger.plugin.policyresourcematcher.RangerPolicyResourceMatcher.MatchType;
import org.apache.ranger.plugin.policyresourcematcher.RangerResourceEvaluator;
import org.apache.ranger.plugin.resourcematcher.RangerResourceMatcher;
import org.apache.ranger.plugin.util.ServiceDefUtil;
import org.apache.ranger.plugin.util.ServiceGdsInfo.SharedResourceInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class GdsSharedResourceEvaluator implements RangerResourceEvaluator {
    private static final Logger LOG = LoggerFactory.getLogger(GdsSharedResourceEvaluator.class);

    public static final GdsSharedResourceEvalOrderComparator EVAL_ORDER_COMPARATOR = new GdsSharedResourceEvalOrderComparator();

    private final SharedResourceInfo                resource;
    private final RangerConditionEvaluator          conditionEvaluator;
    private final Map<String, RangerPolicyResource> policyResource;
    private final RangerPolicyResourceMatcher       policyResourceMatcher;
    private final RangerResourceDef                 leafResourceDef;
    private final Set<String>                       allowedAccessTypes;

    public GdsSharedResourceEvaluator(SharedResourceInfo resource, Set<String> defaultAccessTypes, RangerServiceDefHelper serviceDefHelper, RangerPluginContext pluginContext) {
        this.resource           = resource;
        this.conditionEvaluator = RangerCustomConditionEvaluator.getInstance().getExpressionEvaluator(resource.getConditionExpr(), serviceDefHelper.getServiceDef());

        if (this.resource.getResource() == null) {
            this.resource.setResource(Collections.emptyMap());
        }

        if (StringUtils.isNotBlank(resource.getSubResourceType()) && resource.getSubResource() != null && CollectionUtils.isNotEmpty(resource.getSubResource().getValues())) {
            this.policyResource = new HashMap<>(resource.getResource());

            this.policyResource.put(resource.getSubResourceType(), resource.getSubResource());
        } else {
            this.policyResource = resource.getResource();
        }

        this.policyResourceMatcher = initPolicyResourceMatcher(policyResource, serviceDefHelper, pluginContext);
        this.leafResourceDef       = ServiceDefUtil.getLeafResourceDef(serviceDefHelper.getServiceDef(), policyResource);
        this.allowedAccessTypes    = serviceDefHelper.expandImpliedAccessGrants(resource.getAccessTypes() != null ? resource.getAccessTypes() : defaultAccessTypes);

        LOG.debug("GdsSharedResourceEvaluator: resource={}, conditionEvaluator={}, policyResource={}, leafResourceDef={}, allowedAccessTypes={}",
                  resource, conditionEvaluator, policyResource, leafResourceDef, allowedAccessTypes);
    }

    @Override
    public long getId() {
        return resource.getId();
    }

    @Override
    public RangerPolicyResourceMatcher getPolicyResourceMatcher() {
        return policyResourceMatcher;
    }

    @Override
    public Map<String, RangerPolicyResource> getPolicyResource() {
        return policyResource;
    }

    @Override
    public RangerResourceMatcher getResourceMatcher(String resourceName) {
        return policyResourceMatcher.getResourceMatcher(resourceName);
    }

    @Override
    public boolean isAncestorOf(RangerResourceDef resourceDef) {
        return ServiceDefUtil.isAncestorOf(policyResourceMatcher.getServiceDef(), leafResourceDef, resourceDef);
    }

    @Override
    public boolean isLeaf(String resourceName) { return StringUtils.equals(leafResourceDef.getName(), resourceName); }

    public Collection<String> getResourceKeys() {
        return resource != null && resource.getResource() != null ? resource.getResource().keySet() : Collections.emptySet();
    }

    public boolean isConditional() { return conditionEvaluator != null; }

    public Set<String> getAllowedAccessTypes() { return allowedAccessTypes; }

    public boolean isAllowed(RangerAccessRequest request) {
        LOG.debug("==> GdsSharedResourceEvaluator.evaluate({})", request);

        boolean ret = conditionEvaluator == null || conditionEvaluator.isMatched(request);

        if (ret) {
            ret = request.isAccessTypeAny() ? !allowedAccessTypes.isEmpty() : allowedAccessTypes.contains(request.getAccessType());

            if (ret) {
                MatchType matchType = policyResourceMatcher.getMatchType(request.getResource(), request.getResourceElementMatchingScopes(), request.getContext());

                if (request.isAccessTypeAny()) {
                    ret = matchType != RangerPolicyResourceMatcher.MatchType.NONE;
                } else if (request.getResourceMatchingScope() == RangerAccessRequest.ResourceMatchingScope.SELF_OR_DESCENDANTS) {
                    ret = matchType != RangerPolicyResourceMatcher.MatchType.NONE;
                } else {
                    ret = matchType == RangerPolicyResourceMatcher.MatchType.SELF || matchType == RangerPolicyResourceMatcher.MatchType.SELF_AND_ALL_DESCENDANTS;
                }

                if (!ret) {
                    LOG.debug("GdsSharedResourceEvaluator.evaluate({}): not matched for resource {}", request, request.getResource());
                }
            } else {
                LOG.debug("GdsSharedResourceEvaluator.evaluate({}): not matched for accessType {}", request, request.getAccessType());
            }
        } else {
            LOG.debug("GdsSharedResourceEvaluator.evaluate({}): not matched for condition {}", request, resource.getConditionExpr());
        }

        LOG.debug("<== GdsSharedResourceEvaluator.evaluate({})", request);

        return ret;
    }

    public void getResourceACLs(RangerAccessRequest request, RangerResourceACLs acls, boolean isConditional, List<GdsDshidEvaluator> dshidEvaluators) {
        LOG.debug("==> GdsSharedResourceEvaluator.getResourceACLs({}, {})", request, acls);

        boolean isResourceMatch = policyResourceMatcher.isMatch(request.getResource(), request.getResourceElementMatchingScopes(), request.getContext());

        if (isResourceMatch) {
            isConditional = isConditional || conditionEvaluator != null;

            for (GdsDshidEvaluator dshidEvaluator : dshidEvaluators) {
                dshidEvaluator.getResourceACLs(request, acls, isConditional, getAllowedAccessTypes());
            }
        }

        LOG.debug("<== GdsSharedResourceEvaluator.getResourceACLs({}, {})", request, acls);
    }

    public RangerPolicyItemRowFilterInfo getRowFilter() {
        return resource.getRowFilter();
    }

    public RangerPolicyItemDataMaskInfo getDataMask(String subResourceName) {
        return resource.getSubResourceMasks() != null ? resource.getSubResourceMasks().get(subResourceName) : null;
    }

    private static RangerPolicyResourceMatcher initPolicyResourceMatcher(Map<String, RangerPolicyResource> policyResource, RangerServiceDefHelper serviceDefHelper, RangerPluginContext pluginContext) {
        RangerDefaultPolicyResourceMatcher matcher = new RangerDefaultPolicyResourceMatcher();

        matcher.setServiceDefHelper(serviceDefHelper);
        matcher.setServiceDef(serviceDefHelper.getServiceDef());
        matcher.setPolicyResources(policyResource, RangerPolicy.POLICY_TYPE_ACCESS);
        matcher.setPluginContext(pluginContext);

        matcher.init();

        return matcher;
    }

    public static class GdsSharedResourceEvalOrderComparator implements Comparator<GdsSharedResourceEvaluator> {
        @Override
        public int compare(GdsSharedResourceEvaluator me, GdsSharedResourceEvaluator other) {
            int ret = 0;

            if (me != null && other != null) {
                ret = compareStrings(me.resource.getName(), other.resource.getName());

                if (ret == 0) {
                    ret = Integer.compare(me.resource.getResource().size(), other.resource.getResource().size());

                    if (ret == 0) {
                        ret = Long.compare(me.getId(), other.getId());
                    }
                }
            } else if (me != null) {
                ret = -1;
            } else if (other != null) {
                ret = 1;
            }

            return ret;
        }

        static int compareStrings(String str1, String str2) {
            if (str1 == null) {
                return str2 == null ? 0 : -1;
            } else {
                return str2 == null ? 1 : str1.compareTo(str2);
            }
        }
    }
}
