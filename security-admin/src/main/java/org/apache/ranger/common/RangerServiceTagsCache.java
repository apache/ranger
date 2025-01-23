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

package org.apache.ranger.common;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.ranger.authorization.hadoop.config.RangerAdminConfig;
import org.apache.ranger.biz.TagDBStore;
import org.apache.ranger.plugin.store.TagStore;
import org.apache.ranger.plugin.util.RangerServiceTagsDeltaUtil;
import org.apache.ranger.plugin.util.ServiceTags;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

public class RangerServiceTagsCache {
    private static final Logger LOG = LoggerFactory.getLogger(RangerServiceTagsCache.class);

    private static final int MAX_WAIT_TIME_FOR_UPDATE = 10;

    private static volatile RangerServiceTagsCache sInstance;

    private final boolean useServiceTagsCache;
    private final int     waitTimeInSeconds;
    private final boolean dedupStrings;

    private final Map<String, ServiceTagsWrapper> serviceTagsMap = new HashMap<>();

    private RangerServiceTagsCache() {
        RangerAdminConfig config = RangerAdminConfig.getInstance();

        useServiceTagsCache = config.getBoolean("ranger.admin.tag.download.usecache", true);
        waitTimeInSeconds   = config.getInt("ranger.admin.tag.download.cache.max.waittime.for.update", MAX_WAIT_TIME_FOR_UPDATE);
        dedupStrings        = config.getBoolean("ranger.admin.tag.dedup.strings", Boolean.TRUE);
    }

    public static RangerServiceTagsCache getInstance() {
        RangerServiceTagsCache me = sInstance;

        if (me == null) {
            synchronized (RangerServiceTagsCache.class) {
                me = sInstance;

                if (me == null) {
                    me        = new RangerServiceTagsCache();
                    sInstance = me;
                }
            }
        }

        return me;
    }

    public void dump() {
        if (useServiceTagsCache) {
            final Set<String> serviceNames;

            synchronized (this) {
                serviceNames = serviceTagsMap.keySet();
            }

            if (CollectionUtils.isNotEmpty(serviceNames)) {
                ServiceTagsWrapper cachedServiceTagsWrapper;

                for (String serviceName : serviceNames) {
                    synchronized (this) {
                        cachedServiceTagsWrapper = serviceTagsMap.get(serviceName);
                    }

                    LOG.debug("serviceName:{}, Cached-MetaData:{}", serviceName, cachedServiceTagsWrapper);
                }
            }
        }
    }

    public ServiceTags getServiceTags(String serviceName, Long serviceId, Long lastKnownVersion, boolean needsBackwardCompatibility, TagStore tagStore) throws Exception {
        LOG.debug("==> RangerServiceTagsCache.getServiceTags({}, {}, {}, {})", serviceName, serviceId, lastKnownVersion, needsBackwardCompatibility);

        ServiceTags ret = null;

        if (StringUtils.isNotBlank(serviceName) && serviceId != null) {
            LOG.debug("useServiceTagsCache={}", useServiceTagsCache);

            if (!useServiceTagsCache) {
                if (tagStore != null) {
                    try {
                        ret = tagStore.getServiceTags(serviceName, -1L);

                        if (ret != null && dedupStrings) {
                            ret.dedupStrings();
                        }
                    } catch (Exception exception) {
                        LOG.error("getServiceTags({}): failed to get latest tags from tag-store", serviceName, exception);
                    }
                } else {
                    LOG.error("getServiceTags({}): failed to get latest tags as tag-store is null!", serviceName);
                }
            } else {
                ServiceTagsWrapper serviceTagsWrapper;

                synchronized (this) {
                    serviceTagsWrapper = serviceTagsMap.get(serviceName);

                    if (serviceTagsWrapper != null) {
                        if (!serviceId.equals(serviceTagsWrapper.getServiceId())) {
                            LOG.debug("Service [{}] changed service-id from {} to {}", serviceName, serviceTagsWrapper.getServiceId(), serviceId);
                            LOG.debug("Recreating serviceTagsWrapper for serviceName [{}]", serviceName);

                            serviceTagsMap.remove(serviceName);
                            serviceTagsWrapper = null;
                        }
                    }
                    if (serviceTagsWrapper == null) {
                        serviceTagsWrapper = new ServiceTagsWrapper(serviceId);

                        serviceTagsMap.put(serviceName, serviceTagsWrapper);
                    }
                }

                if (tagStore != null) {
                    ret = serviceTagsWrapper.getLatestOrCached(serviceName, tagStore, lastKnownVersion, needsBackwardCompatibility);
                } else {
                    LOG.error("getServiceTags({}): failed to get latest tags as tag-store is null!", serviceName);

                    ret = serviceTagsWrapper.getServiceTags();
                }
            }
        } else {
            LOG.error("getServiceTags() failed to get tags as serviceName is null or blank and/or serviceId is null!");
        }

        LOG.debug("<== RangerServiceTagsCache.getServiceTags({}, {}, {}, {}): count={}", serviceName, serviceId, lastKnownVersion, needsBackwardCompatibility, (ret == null || ret.getTags() == null) ? 0 : ret.getTags().size());

        return ret;
    }

    /**
     * Reset service tag cache using serviceName if provided.
     * If serviceName is empty, reset everything.
     *
     * @param serviceName
     * @return true if was able to reset service tag cache, false otherwise
     */
    public boolean resetCache(final String serviceName) {
        LOG.debug("==> RangerServiceTagsCache.resetCache({})", serviceName);

        boolean ret = false;

        synchronized (this) {
            if (!serviceTagsMap.isEmpty()) {
                if (StringUtils.isBlank(serviceName)) {
                    serviceTagsMap.clear();

                    LOG.debug("RangerServiceTagsCache.resetCache(): Removed policy caching for all services.");

                    ret = true;
                } else {
                    ServiceTagsWrapper removedServicePoliciesWrapper = serviceTagsMap.remove(serviceName.trim()); // returns null if key not found

                    ret = removedServicePoliciesWrapper != null;

                    if (ret) {
                        LOG.debug("RangerServiceTagsCache.resetCache(): Removed policy caching for [{}] service.", serviceName);
                    } else {
                        LOG.warn("RangerServiceTagsCache.resetCache(): Caching for [{}] service not found, hence reset is skipped.", serviceName);
                    }
                }
            } else {
                LOG.warn("RangerServiceTagsCache.resetCache(): Policy cache is already empty.");
            }
        }

        LOG.debug("<== RangerServiceTagsCache.resetCache(): ret={}", ret);

        return ret;
    }

    private class ServiceTagsWrapper {
        final Long             serviceId;
        ServiceTags            serviceTags;
        Date                   updateTime;
        long                   longestDbLoadTimeInMs = -1;
        ServiceTagsDeltasCache deltaCache;
        ReentrantLock          lock = new ReentrantLock();

        ServiceTagsWrapper(Long serviceId) {
            this.serviceId = serviceId;
            serviceTags    = null;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();

            toString(sb);

            return sb.toString();
        }

        Long getServiceId() {
            return serviceId;
        }

        ServiceTags getServiceTags() {
            return serviceTags;
        }

        Date getUpdateTime() {
            return updateTime;
        }

        ServiceTags getLatestOrCached(String serviceName, TagStore tagStore, Long lastKnownVersion, boolean needsBackwardCompatibility) throws Exception {
            LOG.debug("==> RangerServiceTagsCache.getLatestOrCached(lastKnownVersion={}, {})", lastKnownVersion, needsBackwardCompatibility);

            ServiceTags ret        = null;
            boolean     lockResult = false;

            try {
                final boolean isCacheCompletelyLoaded;

                lockResult = lock.tryLock(waitTimeInSeconds, TimeUnit.SECONDS);

                if (lockResult) {
                    isCacheCompletelyLoaded = getLatest(serviceName, tagStore);

                    if (isCacheCompletelyLoaded) {
                        LOG.debug("ServiceTags cache was completely loaded from database ");
                    }

                    if (needsBackwardCompatibility || isCacheCompletelyLoaded || lastKnownVersion == -1L || lastKnownVersion.equals(serviceTags.getTagVersion())) {
                        // Looking for all tags, or Some disqualifying change encountered
                        LOG.debug("Need to return all cached ServiceTags: [needsBackwardCompatibility:{}, isCacheCompletelyLoaded:{}, lastKnownVersion:{}, serviceTagsVersion:{}]", needsBackwardCompatibility, isCacheCompletelyLoaded, lastKnownVersion, serviceTags.getTagVersion());

                        ret = this.serviceTags;
                    } else {
                        boolean     isDeltaCacheReinitialized = false;
                        ServiceTags serviceTagsDelta          = this.deltaCache != null ? this.deltaCache.getServiceTagsDeltaFromVersion(lastKnownVersion) : null;

                        if (serviceTagsDelta == null) {
                            serviceTagsDelta          = tagStore.getServiceTagsDelta(serviceName, lastKnownVersion);
                            isDeltaCacheReinitialized = true;
                        }
                        if (serviceTagsDelta != null) {
                            LOG.debug("Deltas were requested. Returning deltas from lastKnownVersion:[{}]", lastKnownVersion);

                            if (isDeltaCacheReinitialized) {
                                this.deltaCache = new ServiceTagsDeltasCache(lastKnownVersion, serviceTagsDelta);
                            }

                            ret = serviceTagsDelta;
                        } else {
                            LOG.warn("Deltas were requested, but could not get them!! lastKnownVersion:[{}]; Returning cached ServiceTags:[{}]", lastKnownVersion, serviceTags != null ? serviceTags.getTagVersion() : -1L);

                            this.deltaCache = null;
                            ret             = this.serviceTags;
                        }
                    }
                } else {
                    LOG.debug("Could not get lock in [{}] seconds, returning cached ServiceTags", waitTimeInSeconds);

                    ret = this.serviceTags;
                }
            } catch (InterruptedException exception) {
                LOG.error("getLatestOrCached:lock got interrupted..", exception);
            } finally {
                if (lockResult) {
                    lock.unlock();
                }
            }

            LOG.debug("<== RangerServiceTagsCache.getLatestOrCached(lastKnownVersion={}, {}): {}", lastKnownVersion, needsBackwardCompatibility, ret);

            return ret;
        }

        boolean getLatest(String serviceName, TagStore tagStore) throws Exception {
            LOG.debug("==> ServiceTagsWrapper.getLatest({})", serviceName);

            boolean    isCacheCompletelyLoaded  = false;
            final Long cachedServiceTagsVersion = serviceTags != null ? serviceTags.getTagVersion() : -1L;

            LOG.debug("Found ServiceTags in-cache : {}", serviceTags != null);

            Long tagVersionInDb = tagStore.getTagVersion(serviceName);

            if (serviceTags == null || tagVersionInDb == null || !tagVersionInDb.equals(cachedServiceTagsVersion)) {
                LOG.debug("loading serviceTags from db ... cachedServiceTagsVersion={}, tagVersionInDb={}", cachedServiceTagsVersion, tagVersionInDb);

                long        startTimeMs       = System.currentTimeMillis();
                ServiceTags serviceTagsFromDb = tagStore.getServiceTags(serviceName, cachedServiceTagsVersion);
                long        dbLoadTime        = System.currentTimeMillis() - startTimeMs;

                if (dbLoadTime > longestDbLoadTimeInMs) {
                    longestDbLoadTimeInMs = dbLoadTime;
                }

                updateTime = new Date();

                if (serviceTagsFromDb != null) {
                    LOG.debug("loading serviceTags from database and it took:{} seconds", TimeUnit.MILLISECONDS.toSeconds(dbLoadTime));

                    if (dedupStrings) {
                        serviceTagsFromDb.dedupStrings();
                    }

                    if (serviceTags == null) {
                        LOG.debug("Initializing ServiceTags cache for the first time");

                        this.serviceTags = serviceTagsFromDb;
                        this.deltaCache  = null;

                        pruneUnusedAttributes();

                        isCacheCompletelyLoaded = true;
                    } else if (!serviceTagsFromDb.getIsDelta()) {
                        // service-tags are loaded because of some disqualifying event
                        LOG.debug("Complete set of tag are loaded from database, because of some disqualifying event or because tag-delta is not supported");

                        this.serviceTags = serviceTagsFromDb;
                        this.deltaCache  = null;

                        pruneUnusedAttributes();

                        isCacheCompletelyLoaded = true;
                    } else { // Previously cached service tags are still valid - no disqualifying change
                        // Rebuild tags cache from original tags and deltas
                        LOG.debug("Retrieved tag-deltas from database. These will be applied on top of ServiceTags version:[{}], tag-deltas:[{}]", cachedServiceTagsVersion, serviceTagsFromDb.getTagVersion());

                        boolean supportsTagsDedeup = TagDBStore.isSupportsTagsDedup();

                        this.serviceTags = RangerServiceTagsDeltaUtil.applyDelta(serviceTags, serviceTagsFromDb, supportsTagsDedeup);
                        this.deltaCache  = new ServiceTagsDeltasCache(cachedServiceTagsVersion, serviceTagsFromDb);
                    }
                } else {
                    LOG.error("Could not get tags from database, from-version:[{})", cachedServiceTagsVersion);
                }

                LOG.debug("ServiceTags old-version:[{}], new-version:[{}]", cachedServiceTagsVersion, serviceTags.getTagVersion());
            } else {
                LOG.debug("ServiceTags Cache already has the latest version, version:[{}]", cachedServiceTagsVersion);
            }

            LOG.debug("<== ServiceTagsWrapper.getLatest({}): {}", serviceName, isCacheCompletelyLoaded);

            return isCacheCompletelyLoaded;
        }

        StringBuilder toString(StringBuilder sb) {
            sb.append("RangerServiceTagsWrapper={");

            sb.append("updateTime=").append(updateTime)
                    .append(", longestDbLoadTimeInMs=").append(longestDbLoadTimeInMs)
                    .append(", Service-Version:").append(serviceTags != null ? serviceTags.getTagVersion() : "null")
                    .append(", Number-Of-Tags:").append(serviceTags != null ? serviceTags.getTags().size() : 0);

            sb.append("} ");

            return sb;
        }

        private void pruneUnusedAttributes() {
            RangerServiceTagsDeltaUtil.pruneUnusedAttributes(this.serviceTags);
        }

        class ServiceTagsDeltasCache {
            final long        fromVersion;
            final ServiceTags serviceTagsDelta;

            ServiceTagsDeltasCache(final long fromVersion, ServiceTags serviceTagsDelta) {
                this.fromVersion      = fromVersion;
                this.serviceTagsDelta = serviceTagsDelta;
            }

            ServiceTags getServiceTagsDeltaFromVersion(long fromVersion) {
                return this.fromVersion == fromVersion ? this.serviceTagsDelta : null;
            }
        }
    }
}
