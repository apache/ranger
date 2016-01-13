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

package org.apache.ranger.plugin.policyevaluator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.policyengine.RangerAccessResource;
import org.apache.ranger.plugin.policyengine.RangerPolicyEngineOptions;

public class RangerCachedPolicyEvaluator extends RangerOptimizedPolicyEvaluator {
    private static final Log LOG = LogFactory.getLog(RangerCachedPolicyEvaluator.class);

    private RangerResourceAccessCache cache = null;

    @Override
    public void init(RangerPolicy policy, RangerServiceDef serviceDef, RangerPolicyEngineOptions options) {
        if(LOG.isDebugEnabled()) {
            LOG.debug("==> RangerCachedPolicyEvaluator.init()");
        }

        super.init(policy, serviceDef, options);

        cache = RangerResourceAccessCacheImpl.getInstance(serviceDef, policy);

        if(LOG.isDebugEnabled()) {
            LOG.debug("<== RangerCachedPolicyEvaluator.init()");
        }
    }

    @Override
    public boolean isMatch(RangerAccessResource resource) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> RangerCachedPolicyEvaluator.isMatch(" + resource + ")");
        }

        boolean result = false;

        // Check in the evaluator-owned cache for the match, if found return. else call super.isMatch(), add result to cache
        RangerResourceAccessCache.LookupResult lookup = cache.lookup(resource);

        if (lookup != RangerResourceAccessCache.LookupResult.IN_NOTMATCHED_CACHE) {
            // We dont know definitely that this previously not matched
            if (lookup != RangerResourceAccessCache.LookupResult.IN_MATCHED_CACHE) {
                result = super.isMatch(resource);

                // update the cache with the result of the match
                if(result) {
	                cache.add(resource, RangerResourceAccessCache.CacheType.MATCHED_CACHE);
                } else {
	                cache.add(resource, RangerResourceAccessCache.CacheType.NOTMATCHED_CACHE);
                }
            } else {
                result = true;
            }
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== RangerCachedPolicyEvaluator.isMatch(" + resource + "): " + result);
        }

        return result;
    }
}
