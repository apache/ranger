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

package org.apache.ranger.plugin.util;

import org.apache.commons.collections.Predicate;
import org.apache.commons.lang.StringUtils;
import org.apache.ranger.plugin.contextenricher.RangerServiceResourceMatcher;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.model.validation.RangerServiceDefHelper;
import org.apache.ranger.plugin.policyengine.RangerAccessRequest;
import org.apache.ranger.plugin.policyengine.RangerAccessResource;
import org.apache.ranger.plugin.policyengine.RangerResourceTrie;
import org.apache.ranger.plugin.policyresourcematcher.RangerResourceEvaluator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class CachedResourceEvaluators {
    private final Map<String, Map<Map<String, RangerAccessRequest.ResourceElementMatchingScope>, Collection<RangerServiceResourceMatcher>>> cache     = new HashMap<>();
    private final RangerReadWriteLock                                                                                                       cacheLock = new RangerReadWriteLock(true);

    private static final Logger LOG = LoggerFactory.getLogger(CachedResourceEvaluators.class);
    private static final Logger PERF_EVALUATORS_RETRIEVAL_LOG = RangerPerfTracer.getPerfLogger("CachedResourceEvaluators.retrieval");

    public CachedResourceEvaluators() {}

    public Collection<RangerServiceResourceMatcher> getEvaluators(String resourceKey, Map<String, RangerAccessRequest.ResourceElementMatchingScope> scopes) {
        Collection<RangerServiceResourceMatcher> ret;

        try (RangerReadWriteLock.RangerLock ignored = cacheLock.getReadLock()) {
            ret = cache.getOrDefault(resourceKey, Collections.emptyMap()).get(scopes);
        }

        return ret;
    }

    public void cacheEvaluators(String resource, Map<String, RangerAccessRequest.ResourceElementMatchingScope> scopes, Collection<RangerServiceResourceMatcher> evaluators) {
        try (RangerReadWriteLock.RangerLock ignored = cacheLock.getWriteLock()) {
            cache.computeIfAbsent(resource, k -> new HashMap<>()).put(scopes, evaluators);
        }
    }

    public void removeCacheEvaluators(Set<String> resources) {
        try (RangerReadWriteLock.RangerLock ignored = cacheLock.getWriteLock()) {
            resources.forEach(cache::remove);
        }
    }

    public void clearCache() {
        try (RangerReadWriteLock.RangerLock ignored = cacheLock.getWriteLock()) {
            cache.clear();
        }
    }

    public static Collection<RangerServiceResourceMatcher> getEvaluators(RangerAccessRequest request, Map<String, RangerResourceTrie<RangerServiceResourceMatcher>> serviceResourceTrie, CachedResourceEvaluators cache) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> CachedResourceEvaluators.getEvaluators(request=" + request + ")");
        }

        Collection<RangerServiceResourceMatcher> ret      = null;
        final RangerAccessResource               resource = request.getResource();
        RangerServiceDefHelper                   helper   = new RangerServiceDefHelper(resource.getServiceDef());

        RangerPerfTracer perf = null;

        if (RangerPerfTracer.isPerfTraceEnabled(PERF_EVALUATORS_RETRIEVAL_LOG)) {
            perf = RangerPerfTracer.getPerfTracer(PERF_EVALUATORS_RETRIEVAL_LOG, "CachedResourceEvaluators.getEvaluators(resource=" + resource.getAsString() + ")");
        }

        final RangerAccessRequest.ResourceMatchingScope resourceMatchingScope = request.getResourceMatchingScope() != null ? request.getResourceMatchingScope() : RangerAccessRequest.ResourceMatchingScope.SELF;
        final Predicate                                 predicate             = !(request.isAccessTypeAny() || resourceMatchingScope == RangerAccessRequest.ResourceMatchingScope.SELF_OR_DESCENDANTS) && excludeDescendantMatches(resource) ? new SelfOrAncestorPredicate(helper.getResourceDef(resource.getLeafName())) : null;

        if (predicate != null) {
            ret = cache.getEvaluators(resource.getCacheKey(), request.getResourceElementMatchingScopes());
        }

        if (ret == null) {
            ret = RangerResourceEvaluatorsRetriever.getEvaluators(serviceResourceTrie, resource.getAsMap(), request.getResourceElementMatchingScopes(), predicate);

            if (LOG.isDebugEnabled()) {
                LOG.debug("Found [{}] service-resource-matchers for service-resource [{}]", (ret == null ? null : ret.size()), resource.getAsString());
            }

            if (predicate != null) {
                cache.cacheEvaluators(resource.getCacheKey(), request.getResourceElementMatchingScopes(), ret);
            }
        } else {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Found [" + ret.size() + "] service-resource-matchers for service-resource [" + resource.getAsString() + "] in the cache");
            }
        }

        RangerPerfTracer.logAlways(perf);

        if (ret == null) {
            ret = new ArrayList<>();
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== CachedResourceEvaluators.getEvaluators(request=" + request + "): evaluators=" + ret);
        }

        return ret;
    }

    public static boolean excludeDescendantMatches(RangerAccessResource resource) {
        final boolean ret;

        String               leafName = resource.getLeafName();

        if (StringUtils.isNotEmpty(leafName)) {
            RangerServiceDefHelper                        helper      = new RangerServiceDefHelper(resource.getServiceDef());
            Set<List<RangerServiceDef.RangerResourceDef>> hierarchies = helper.getResourceHierarchies(RangerPolicy.POLICY_TYPE_ACCESS, resource.getKeys());

            // skip caching if the leaf of accessed resource is the deepest in the only applicable hierarchy
            if (hierarchies.size() == 1) {
                List<RangerServiceDef.RangerResourceDef> theHierarchy    = hierarchies.iterator().next();
                RangerServiceDef.RangerResourceDef       leafOfHierarchy = theHierarchy.get(theHierarchy.size() - 1);

                ret = !StringUtils.equals(leafOfHierarchy.getName(), leafName);
            } else {
                ret = true;
            }
        } else {
            ret = false;
        }
        return ret;
    }

    private static class SelfOrAncestorPredicate implements Predicate {
        private final RangerServiceDef.RangerResourceDef leafResourceDef;

        public SelfOrAncestorPredicate(RangerServiceDef.RangerResourceDef leafResourceDef) {
            this.leafResourceDef = leafResourceDef;
        }

        @Override
        public boolean evaluate(Object o) {
            if (o instanceof RangerResourceEvaluator) {
                RangerResourceEvaluator evaluator = (RangerResourceEvaluator) o;

                return evaluator.isLeaf(leafResourceDef.getName()) || evaluator.isAncestorOf(leafResourceDef);
            }

            return false;
        }
    }
}
