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


import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ranger.authorization.hadoop.config.RangerConfiguration;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.policyengine.CacheMap;
import org.apache.ranger.plugin.policyengine.RangerAccessResource;


public class RangerResourceAccessCacheImpl implements RangerResourceAccessCache {
    private static final Log LOG = LogFactory.getLog(RangerResourceAccessCacheImpl.class);

    public synchronized static RangerResourceAccessCache getInstance(RangerServiceDef serviceDef, RangerPolicy policy) {
        return new RangerResourceAccessCacheImpl(policy);
    }

    private Map<String, String> matchedResourceCache;
    private Map<String, String> notMatchedResourceCache;

    private RangerResourceAccessCacheImpl(RangerPolicy policy) {
        if(LOG.isDebugEnabled()) {
            LOG.debug("==> RangerResourceAccessCacheImpl.constructor(), policyName:" + policy.getName());
        }

        int matchedCacheSize    = RangerConfiguration.getInstance().getInt("ranger.policyengine.matched.cached.count", 1000);
        int notMatchedCacheSize = RangerConfiguration.getInstance().getInt("ranger.policyengine.not.matched.cached.count", matchedCacheSize * 10);

        matchedResourceCache    = new CacheMap<String, String>(matchedCacheSize);
        notMatchedResourceCache = new CacheMap<String, String>(notMatchedCacheSize);

        if(LOG.isDebugEnabled()) {
            LOG.debug("<== RangerResourceAccessCacheImpl.constructor(), policyName:" + policy.getName());
        }
    }

    @Override
    public LookupResult lookup(RangerAccessResource resource) {
        String strResource = resource.getCacheKey();

        if(LOG.isDebugEnabled()) {
            LOG.debug("==> RangerResourceAccessCacheImpl.lookup(" + strResource + ")");
        }

        LookupResult result = LookupResult.NOT_FOUND;

        try {
        	synchronized(this) {
	            if (matchedResourceCache.containsKey(strResource)) {
	                result = LookupResult.IN_MATCHED_CACHE;
	            } else if(notMatchedResourceCache.containsKey(strResource)) {
	                result = LookupResult.IN_NOTMATCHED_CACHE;
	            }
        	}
        } catch (Exception exception) {
            result = LookupResult.ERROR;
        }

        if(LOG.isDebugEnabled()) {
            LOG.debug("<== RangerResourceAccessCacheImpl.lookup(" + strResource + "): " + result);
        }

        return result;
    }

    @Override
    public void add(RangerAccessResource resource, CacheType cacheType) {
        String strResource = resource.getCacheKey();

        if(LOG.isDebugEnabled()) {
            LOG.debug("==> RangerResourceAccessCacheImpl.add(" + strResource + ", " + cacheType + ")");
        }

        synchronized(this) {
	        switch (cacheType) {
	            case MATCHED_CACHE:
	                matchedResourceCache.put(strResource, strResource);
	                break;
	
	            case NOTMATCHED_CACHE:
	                notMatchedResourceCache.put(strResource, strResource);
	                break;
	            default:
	                break;
	        }
        }

        if(LOG.isDebugEnabled()) {
            LOG.debug("<== RangerResourceAccessCacheImpl.add(" + strResource + ", " + cacheType + ")");
        }
    }
}
