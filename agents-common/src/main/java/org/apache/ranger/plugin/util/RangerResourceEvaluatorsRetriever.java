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

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.collections.Predicate;
import org.apache.ranger.plugin.policyengine.RangerAccessRequest.ResourceElementMatchingScope;
import org.apache.ranger.plugin.policyengine.RangerResourceTrie;
import org.apache.ranger.plugin.policyresourcematcher.RangerResourceEvaluator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

public class RangerResourceEvaluatorsRetriever {
    private static final Logger LOG = LoggerFactory.getLogger(RangerResourceEvaluatorsRetriever.class);

    public static <T  extends RangerResourceEvaluator> Collection<T> getEvaluators(Map<String, RangerResourceTrie<T>> resourceTrie, Map<String, ?> resource) {
        return getEvaluators(resourceTrie, resource, null);
    }

    public static <T  extends RangerResourceEvaluator> Collection<T> getEvaluators(Map<String, RangerResourceTrie<T>> resourceTrie, Map<String, ?> resource, Map<String, ResourceElementMatchingScope> scopes) {
        return getEvaluators(resourceTrie, resource, scopes, null);
    }

    public static <T  extends RangerResourceEvaluator> Collection<T> getEvaluators(Map<String, RangerResourceTrie<T>> resourceTrie, Map<String, ?> resource, Map<String, ResourceElementMatchingScope> scopes, Predicate predicate) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> RangerPolicyResourceEvaluatorsRetriever.getEvaluators(" + resource + ")");
        }
        Set<T> ret = null;

        if (scopes == null) {
            scopes = Collections.emptyMap();
        }

        if (MapUtils.isNotEmpty(resourceTrie) && MapUtils.isNotEmpty(resource)) {
            Set<String> resourceKeys         = resource.keySet();
            String      resourceWithMinEvals = null;

            if (resourceKeys.size() > 1) {
                int minEvalCount = 0; // initial value doesn't matter, as the count for the first resource will be assigned later, in line #70

                for (String resourceDefName : resourceKeys) {
                    RangerResourceTrie<T> trie = resourceTrie.get(resourceDefName);

                    if (trie == null) {
                        continue;
                    }

                    Object resourceValues = resource.get(resourceDefName);

                    int evalCount = trie.getEvaluatorsCountForResource(resourceValues, scopes.get(resourceDefName), predicate);

                    if (resourceWithMinEvals == null || (evalCount < minEvalCount)) {
                        resourceWithMinEvals = resourceDefName;
                        minEvalCount         = evalCount;
                    }
                }

                if (minEvalCount == 0) {
                    resourceWithMinEvals = null;
                    ret = Collections.emptySet();
                }
            } else if (resourceKeys.size() == 1) { // skip getEvaluatorsCountForResource() when there is only one resource
                String                resourceKey = resourceKeys.iterator().next();
                RangerResourceTrie<T> trie        = resourceTrie.get(resourceKey);

                if (trie != null) {
                    resourceWithMinEvals = resourceKey;
                }
            }

            if (resourceWithMinEvals != null) {
                RangerResourceTrie<T> trie = resourceTrie.get(resourceWithMinEvals);

                ret = trie.getEvaluatorsForResource(resource.get(resourceWithMinEvals), scopes.get(resourceWithMinEvals), predicate);

                for (String resourceDefName : resourceKeys) {
                    if (resourceWithMinEvals.equals(resourceDefName)) {
                        continue;
                    }

                    trie = resourceTrie.get(resourceDefName);

                    if (trie == null) {
                        continue;
                    }

                    Set<T> evaluators = trie.getEvaluatorsForResource(resource.get(resourceDefName), scopes.get(resourceDefName), ret, predicate);

                    if (CollectionUtils.isEmpty(evaluators)) {
                        ret = Collections.emptySet();

                        break;
                    } else {
                        if (evaluators.size() < ret.size()) {
                            ret = evaluators;
                        }
                    }
                }
            }
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== RangerResourceEvaluatorsRetriever.getEvaluators(" + resource + ") : evaluator:[" + ret + "]");
        }

        return ret;
    }
}
