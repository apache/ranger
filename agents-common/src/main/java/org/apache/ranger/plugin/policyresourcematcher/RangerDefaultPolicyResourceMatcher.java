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
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.List;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ranger.authorization.hadoop.config.RangerConfiguration;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyResource;
import org.apache.ranger.plugin.model.RangerServiceDef.RangerResourceDef;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.model.validation.RangerServiceDefHelper;
import org.apache.ranger.plugin.policyengine.RangerAccessResource;
import org.apache.ranger.plugin.policyengine.RangerAccessResourceImpl;
import org.apache.ranger.plugin.resourcematcher.RangerDefaultResourceMatcher;
import org.apache.ranger.plugin.resourcematcher.RangerResourceMatcher;

public class RangerDefaultPolicyResourceMatcher implements RangerPolicyResourceMatcher {
    private static final Log LOG = LogFactory.getLog(RangerDefaultPolicyResourceMatcher.class);

    protected RangerServiceDef                  serviceDef;
    protected int                               policyType;
    protected Map<String, RangerPolicyResource> policyResources;

    private Map<String, RangerResourceMatcher>  allMatchers;
    private boolean                             needsDynamicEval = false;
    private List<RangerResourceDef>             validResourceHierarchy;
    private boolean                             isInitialized = false;
    private RangerServiceDefHelper              serviceDefHelper;

    @Override
    public void setServiceDef(RangerServiceDef serviceDef) {
        if (isInitialized) {
            LOG.warn("RangerDefaultPolicyResourceMatcher is already initialized. init() must be done again after updating serviceDef");
        }

        this.serviceDef = serviceDef;
    }

    @Override
    public void setPolicy(RangerPolicy policy) {
        if (isInitialized) {
            LOG.warn("RangerDefaultPolicyResourceMatcher is already initialized. init() must be done again after updating policy");
        }

        if (policy == null) {
            setPolicyResources(null, RangerPolicy.POLICY_TYPE_ACCESS);
        } else {
            setPolicyResources(policy.getResources(), policy.getPolicyType() == null ? RangerPolicy.POLICY_TYPE_ACCESS : policy.getPolicyType());
        }

    }

    @Override
    public void setPolicyResources(Map<String, RangerPolicyResource> policyResources) {
        if (isInitialized) {
            LOG.warn("RangerDefaultPolicyResourceMatcher is already initialized. init() must be done again after updating policy-resources");
        }

        setPolicyResources(policyResources, RangerPolicy.POLICY_TYPE_ACCESS);
    }

    @Override
    public void setPolicyResources(Map<String, RangerPolicyResource> policyResources, int policyType) {
        this.policyResources = policyResources;
        this.policyType = policyType;
    }

    @Override
    public boolean getNeedsDynamicEval() { return needsDynamicEval; }

    @Override
    public void init() {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> RangerDefaultPolicyResourceMatcher.init()");
        }

        allMatchers            = null;
        needsDynamicEval       = false;
        validResourceHierarchy = null;
        isInitialized          = false;
        serviceDefHelper       = null;

        String errorText = "";

        boolean useCache = RangerConfiguration.getInstance().getBoolean("ranger.plugin.use-cache-for-service-def-helper", false);

        if (policyResources != null && !policyResources.isEmpty() && serviceDef != null) {
            serviceDefHelper                                    = new RangerServiceDefHelper(serviceDef, useCache);

            Set<List<RangerResourceDef>> resourceHierarchies   = serviceDefHelper.getResourceHierarchies(policyType, policyResources.keySet());
            int                          validHierarchiesCount = 0;

            for (List<RangerResourceDef> resourceHierarchy : resourceHierarchies) {
                boolean foundGapsInResourceSpecs = false;
                boolean skipped                  = false;

                for (RangerResourceDef resourceDef : resourceHierarchy) {
                    RangerPolicyResource policyResource = policyResources.get(resourceDef.getName());

                    if (policyResource == null) {
                        skipped = true;
                    } else if (skipped) {
                        foundGapsInResourceSpecs = true;
                        break;
                    }
                }

                if (foundGapsInResourceSpecs) {
                    LOG.warn("RangerDefaultPolicyResourceMatcher.init(): gaps found in policyResources, skipping hierarchy:[" + resourceHierarchies + "]");
                } else {
                    validHierarchiesCount++;

                    if (validHierarchiesCount == 1) {
                        validResourceHierarchy = resourceHierarchy;
                    } else {
                        validResourceHierarchy = null;
                    }
                }
            }

            if (validHierarchiesCount > 0) {
                allMatchers = new HashMap<>();

                for (List<RangerResourceDef> resourceHierarchy : resourceHierarchies) {
                    for (RangerResourceDef resourceDef : resourceHierarchy) {
                        String resourceName = resourceDef.getName();

                        if (allMatchers.get(resourceName) != null) {
                            continue;
                        }

                        RangerPolicyResource policyResource = policyResources.get(resourceName);

                        if (policyResource == null) {
                            if (LOG.isDebugEnabled()) {
                                LOG.debug("RangerDefaultPolicyResourceMatcher.init(): no matcher created for " + resourceName + ". Continuing ...");
                            }

                            continue;
                        }

                        RangerResourceMatcher matcher = createResourceMatcher(resourceDef, policyResource);

                        if (matcher != null) {
                            if (!needsDynamicEval && matcher.getNeedsDynamicEval()) {
                                needsDynamicEval = true;
                            }

                            allMatchers.put(resourceName, matcher);
                        } else {
                            LOG.error("RangerDefaultPolicyResourceMatcher.init(): failed to find matcher for resource " + resourceName);

                            allMatchers = null;
                            errorText   = "no matcher found for resource " + resourceName;

                            break;
                        }
                    }

                    if (allMatchers == null) {
                        break;
                    }
                }
            } else {
                errorText = "policyResources elements are not part of any valid resourcedef hierarchy.";
            }
        } else {
            errorText = "policyResources is null or empty, or serviceDef is null.";
        }

        if (allMatchers == null) {
            serviceDefHelper       = null;
            validResourceHierarchy = null;

            Set<String>   policyResourceKeys = policyResources == null ? null : policyResources.keySet();
            String        serviceDefName     = serviceDef == null ? "" : serviceDef.getName();
            StringBuilder keysString         = new StringBuilder();

            if (CollectionUtils.isNotEmpty(policyResourceKeys)) {
                for (String policyResourceKeyName : policyResourceKeys) {
                    keysString.append(policyResourceKeyName).append(" ");
                }
            }

            LOG.error("RangerDefaultPolicyResourceMatcher.init() failed: " + errorText + " (serviceDef=" + serviceDefName + ", policyResourceKeys=" + keysString.toString());
        } else {
            isInitialized = true;
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== RangerDefaultPolicyResourceMatcher.init(): ret=" + isInitialized);
        }
    }

    @Override
    public RangerServiceDef getServiceDef() {
        return serviceDef;
    }

    @Override
    public RangerResourceMatcher getResourceMatcher(String resourceName) {
        return allMatchers != null ? allMatchers.get(resourceName) : null;
    }

    @Override
    public boolean isMatch(Map<String, RangerPolicyResource> resources, Map<String, Object> evalContext) {
        if(LOG.isDebugEnabled()) {
            LOG.debug("==> RangerDefaultPolicyResourceMatcher.isMatch(" + resources  + ", " + evalContext + ")");
        }

        boolean ret = isMatch(resources, MatchScope.SELF_OR_ANCESTOR, true, evalContext);


        if(LOG.isDebugEnabled()) {
            LOG.debug("<== RangerDefaultPolicyResourceMatcher.isMatch(" + resources  + ", " + evalContext + "): " + ret);
        }

        return ret;
    }

    @Override
    public boolean isCompleteMatch(RangerAccessResource resource, Map<String, Object> evalContext) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> RangerDefaultPolicyResourceMatcher.isCompleteMatch(" + resource + ", " + evalContext + ")");
        }

        boolean            ret          = false;
        Collection<String> resourceKeys = resource == null ? null : resource.getKeys();
        Collection<String> policyKeys   = policyResources == null ? null : policyResources.keySet();
        boolean            keysMatch    = resourceKeys != null && policyKeys != null && CollectionUtils.isEqualCollection(resourceKeys, policyKeys);

        if (keysMatch) {
            for (RangerResourceDef resourceDef : serviceDef.getResources()) {
                String                resourceName  = resourceDef.getName();
                String                resourceValue = resource == null ? null : resource.getValue(resourceName);
                RangerResourceMatcher matcher       = allMatchers == null ? null : allMatchers.get(resourceName);

                if (StringUtils.isEmpty(resourceValue)) {
                    ret = matcher == null || matcher.isCompleteMatch(resourceValue, evalContext);
                } else {
                    ret = matcher != null && matcher.isCompleteMatch(resourceValue, evalContext);
                }

                if (!ret) {
                    break;
                }
            }
        } else {
            if (LOG.isDebugEnabled()) {
                LOG.debug("isCompleteMatch(): keysMatch=false. resourceKeys=" + resourceKeys + "; policyKeys=" + policyKeys);
            }
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== RangerDefaultPolicyResourceMatcher.isCompleteMatch(" + resource + ", " + evalContext + "): " + ret);
        }

        return ret;
    }

    @Override
    public boolean isCompleteMatch(Map<String, RangerPolicyResource> resources, Map<String, Object> evalContext) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> RangerDefaultPolicyResourceMatcher.isCompleteMatch(" + resources + ", " + evalContext + ")");
        }

        boolean            ret          = false;
        Collection<String> resourceKeys = resources == null ? null : resources.keySet();
        Collection<String> policyKeys   = policyResources == null ? null : policyResources.keySet();
        boolean            keysMatch    = resourceKeys != null && policyKeys != null && CollectionUtils.isEqualCollection(resourceKeys, policyKeys);

        if (keysMatch) {
            for (RangerResourceDef resourceDef : serviceDef.getResources()) {
                String               resourceName   = resourceDef.getName();
                RangerPolicyResource resourceValues = resources == null ? null : resources.get(resourceName);
                RangerPolicyResource policyValues   = policyResources == null ? null : policyResources.get(resourceName);

                if (resourceValues == null || CollectionUtils.isEmpty(resourceValues.getValues())) {
                    ret = (policyValues == null || CollectionUtils.isEmpty(policyValues.getValues()));
                } else if (policyValues != null && CollectionUtils.isNotEmpty(policyValues.getValues())) {
                    ret = CollectionUtils.isEqualCollection(resourceValues.getValues(), policyValues.getValues());
                }

                if (!ret) {
                    break;
                }
            }
        } else {
            if (LOG.isDebugEnabled()) {
                LOG.debug("isCompleteMatch(): keysMatch=false. resourceKeys=" + resourceKeys + "; policyKeys=" + policyKeys);
            }
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== RangerDefaultPolicyResourceMatcher.isCompleteMatch(" + resources + ", " + evalContext + "): " + ret);
        }

        return ret;
    }

    @Override
    public boolean isMatch(RangerAccessResource resource, Map<String, Object> evalContext) {
        return isMatch(resource, MatchScope.SELF_OR_ANCESTOR, evalContext);
    }

    @Override
    public boolean isMatch(RangerPolicy policy, MatchScope scope, Map<String, Object> evalContext) {
        if (policy.getPolicyType() == policyType) {
            return isMatch(policy.getResources(), scope, false, evalContext);
        } else {
            return false;
        }
    }

    boolean isMatch(Map<String, RangerPolicyResource> resources, MatchScope scope, boolean mustMatchAllPolicyValues, Map<String, Object> evalContext) {
        boolean ret = false;

        if (MapUtils.isNotEmpty(resources)) {
            List<RangerResourceDef> hierarchy = getMatchingHierarchy(resources.keySet());

            if (CollectionUtils.isNotEmpty(hierarchy)) {
                MatchType                matchType      = MatchType.NONE;
                RangerAccessResourceImpl accessResource = new RangerAccessResourceImpl();

                accessResource.setServiceDef(serviceDef);

                // Build up accessResource resourceDef by resourceDef.
                // For each resourceDef,
                //         examine policy-values one by one.
                //         The first value that is acceptable, that is,
                //             value matches in any way, is used for that resourceDef, and
                //            next resourceDef is processed.
                //         If none of the values matches, the policy as a whole definitely will not match,
                //        therefore, the match is failed
                // After all resourceDefs are processed, and some match is achieved at every
                // level, the final matchType (which is for the entire policy) is checked against
                // requested scope to determine the match-result.

                // Unit tests in TestDefaultPolicyResourceForPolicy.java, test_defaultpolicyresourcematcher_for_policy.json,
                // test_defaultpolicyresourcematcher_for_hdfs_policy.json, and
                // test_defaultpolicyresourcematcher_for_resource_specific_policy.json

                boolean skipped = false;

                for (RangerResourceDef resourceDef : hierarchy) {
                    String               name           = resourceDef.getName();
                    RangerPolicyResource policyResource = resources.get(name);

                    if (policyResource != null && CollectionUtils.isNotEmpty(policyResource.getValues())) {
                        ret       = false;
                        matchType = MatchType.NONE;

                        if (!skipped) {
                            for (String value : policyResource.getValues()) {
                                accessResource.setValue(name, value);

                                matchType = getMatchType(accessResource, evalContext);

                                if (matchType != MatchType.NONE) { // One value for this resourceDef matched
                                    ret = true;

                                    if (!mustMatchAllPolicyValues) {
                                        break;
                                    }
                                }
                            }
                        } else {
                            break;
                        }
                    } else {
                        skipped = true;
                    }

                    if (!ret) { // None of the values specified for this resourceDef matched, no point in continuing with next resourceDef
                        break;
                    }
                }
                ret = ret && isMatch(scope, matchType);
            }
        }

        return ret;
    }

    @Override
    public boolean isMatch(RangerAccessResource resource, MatchScope scope, Map<String, Object> evalContext) {
        MatchType matchType = getMatchType(resource, evalContext);
        boolean   ret       = isMatch(scope, matchType);

        return ret;
    }

    @Override
    public MatchType getMatchType(RangerAccessResource resource, Map<String, Object> evalContext) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> RangerDefaultPolicyResourceMatcher.getMatchType(" + resource + evalContext + ")");
        }

        MatchType ret              = MatchType.NONE;
        int       policyKeysSize   = policyResources == null ? 0 : policyResources.size();
        int       resourceKeysSize = resource == null || resource.getKeys() == null ? 0 : resource.getKeys().size();

        if (policyKeysSize == 0 && resourceKeysSize == 0) {
            ret = MatchType.SELF;
        } else {
            List<RangerResourceDef> hierarchy = getMatchingHierarchy(resource);
            if (CollectionUtils.isNotEmpty(hierarchy)) {
                int lastNonAnyMatcherIndex = 0;
                /*
                 * For hive resource policy:
                 *     lastNonAnyMatcherIndex will be set to
                 *         0 : if all matchers in policy are '*'; such as database=*, table=*, column=*
                 *         1 : database=hr, table=*, column=*
                 *         2 : database=<any>, table=employee, column=*
                 *         3 : database=<any>, table=<any>, column=ssn
                */
                int matchersSize = 0;

                for (RangerResourceDef resourceDef : hierarchy) {
                    RangerResourceMatcher matcher = allMatchers.get(resourceDef.getName());
                    if (matcher != null) {
                        matchersSize++;
                        if (!matcher.isMatchAny()) {
                            lastNonAnyMatcherIndex = matchersSize;
                        }
                    }
                }

                if (resourceKeysSize == 0 && lastNonAnyMatcherIndex == 0) {
                    ret = MatchType.SELF;
                } else if (lastNonAnyMatcherIndex == 0) {
                    ret = MatchType.ANCESTOR;
                } else {
                    int index = 0;
                    for (RangerResourceDef resourceDef : hierarchy) {

                        String resourceName = resourceDef.getName();
                        RangerResourceMatcher matcher = allMatchers.get(resourceName);
                        String resourceValue = resource.getValue(resourceName);

                        if (resourceValue != null) {
                            if (matcher != null) {
                                index++;
                                if (matcher.isMatch(resourceValue, evalContext)) {
                                    ret = index == resourceKeysSize && matcher.isMatchAny() ? MatchType.ANCESTOR : MatchType.SELF;
                                } else {
                                    ret = MatchType.NONE;
                                    break;
                                }
                            } else {
                                // More resource-levels than matchers
                                ret = MatchType.ANCESTOR;
                                break;
                            }
                        } else {
                            if (matcher != null) {
                                // More matchers than resource-levels
                                if (index >= lastNonAnyMatcherIndex) {
                                    // All AnyMatch matchers after this
                                    ret = MatchType.ANCESTOR;
                                } else {
                                    ret = MatchType.DESCENDANT;
                                }
                            } else {
                                // Common part of several possible hierarchies matched
                                if (resourceKeysSize > index) {
                                    ret = MatchType.ANCESTOR;
                                }
                            }
                            break;
                        }
                    }
                    if (ret == MatchType.SELF && resourceKeysSize > matchersSize) {
                        ret = MatchType.ANCESTOR;
                    }
                }
            }

        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== RangerDefaultPolicyResourceMatcher.getMatchType(" + resource + evalContext + "): " + ret);
        }

        return ret;
    }

    private List<RangerResourceDef> getMatchingHierarchy(Set<String> resourceKeys) {
        List<RangerResourceDef> ret = null;

        if (CollectionUtils.isNotEmpty(resourceKeys)) {
            Set<List<RangerResourceDef>> resourceHierarchies = serviceDefHelper == null ? Collections.EMPTY_SET : serviceDefHelper.getResourceHierarchies(policyType, resourceKeys);

            // pick the shortest hierarchy
            for (List<RangerResourceDef> resourceHierarchy : resourceHierarchies) {
                if (ret == null) {
                    ret = resourceHierarchy;
                } else {
                    if (resourceHierarchy.size() < ret.size()) {
                        ret = resourceHierarchy;
                        if (ret.size() == resourceKeys.size()) {
                            break;
                        }
                    }
                }
            }
        }

        return ret;
    }

    private List<RangerResourceDef> getMatchingHierarchy(RangerAccessResource resource) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> RangerDefaultPolicyResourceMatcher.getMatchingHierarchy(" + resource + ")");
        }

        final List<RangerResourceDef> ret;

        Set<String> policyResourcesKeySet = policyResources == null ? Collections.EMPTY_SET : policyResources.keySet();

        if (resource != null && resource.getKeys() != null) {
            List<RangerResourceDef> aValidHierarchy = null;

            if (validResourceHierarchy != null && serviceDefHelper != null) {
                if (serviceDefHelper.hierarchyHasAllResources(validResourceHierarchy, resource.getKeys())) {
                    aValidHierarchy = validResourceHierarchy;
                }
            } else {
                if (policyResourcesKeySet.containsAll(resource.getKeys())) {
                    aValidHierarchy = getMatchingHierarchy(policyResourcesKeySet);
                } else if (resource.getKeys().containsAll(policyResourcesKeySet)) {
                    aValidHierarchy = getMatchingHierarchy(resource.getKeys());
                }
            }

            if (aValidHierarchy != null) {
                boolean isValid = true;
                boolean skipped = false;

                for (RangerResourceDef resourceDef : aValidHierarchy) {
                    String resourceName = resourceDef.getName();
                    String resourceValue = resource.getValue(resourceName);

                    if (resourceValue == null) {
                        if (!skipped) {
                            skipped = true;
                        }
                    } else {
                        if (skipped) {
                            isValid = false;
                            break;
                        }
                    }
                }

                if (!isValid) {
                    aValidHierarchy = null;
                }
            }

            ret = aValidHierarchy;
        } else {
            ret = getMatchingHierarchy(policyResourcesKeySet);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== RangerDefaultPolicyResourceMatcher.getMatchingHierarchy(" + resource + "): " + ret);
        }

        return ret;
    }

    private boolean isMatch(final MatchScope scope, final MatchType matchType) {
        final boolean ret;
        switch (scope) {
            case SELF_OR_ANCESTOR_OR_DESCENDANT: {
                ret = matchType != MatchType.NONE;
                break;
            }
            case SELF: {
                ret = matchType == MatchType.SELF;
                break;
            }
            case SELF_OR_DESCENDANT: {
                ret = matchType == MatchType.SELF || matchType == MatchType.DESCENDANT;
                break;
            }
            case SELF_OR_ANCESTOR: {
                ret = matchType == MatchType.SELF || matchType == MatchType.ANCESTOR;
                break;
            }
            case DESCENDANT: {
                ret = matchType == MatchType.DESCENDANT;
                break;
            }
            case ANCESTOR: {
                ret = matchType == MatchType.ANCESTOR;
                break;
            }
            default:
                ret = matchType != MatchType.NONE;
                break;
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

        sb.append("isInitialized=").append(isInitialized).append(", ");

        sb.append("matchers={");
        if(allMatchers != null) {
            for(RangerResourceMatcher matcher : allMatchers.values()) {
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

            ret.setResourceDef(resourceDef);
            ret.setPolicyResource(resource);
            ret.init();
        } else {
            LOG.error("RangerDefaultPolicyResourceMatcher: RangerResourceDef is null");
        }

        if(LOG.isDebugEnabled()) {
            LOG.debug("<== RangerDefaultPolicyResourceMatcher.createResourceMatcher(" + resourceDef + ", " + resource + "): " + ret);
        }

        return ret;
    }

}
