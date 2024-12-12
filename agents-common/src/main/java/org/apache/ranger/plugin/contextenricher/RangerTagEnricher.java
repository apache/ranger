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

package org.apache.ranger.plugin.contextenricher;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.ranger.authorization.hadoop.config.RangerPluginConfig;
import org.apache.ranger.authorization.utils.JsonUtils;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyResource;
import org.apache.ranger.plugin.model.RangerServiceDef.RangerResourceDef;
import org.apache.ranger.plugin.model.RangerServiceResource;
import org.apache.ranger.plugin.model.RangerTag;
import org.apache.ranger.plugin.model.validation.RangerServiceDefHelper;
import org.apache.ranger.plugin.policyengine.RangerAccessRequest;
import org.apache.ranger.plugin.policyengine.RangerAccessRequest.ResourceMatchingScope;
import org.apache.ranger.plugin.policyengine.RangerAccessRequestImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessResource;
import org.apache.ranger.plugin.policyengine.RangerAccessResourceImpl;
import org.apache.ranger.plugin.policyengine.RangerPluginContext;
import org.apache.ranger.plugin.policyengine.RangerResourceTrie;
import org.apache.ranger.plugin.policyresourcematcher.RangerDefaultPolicyResourceMatcher;
import org.apache.ranger.plugin.policyresourcematcher.RangerPolicyResourceMatcher;
import org.apache.ranger.plugin.service.RangerAuthContext;
import org.apache.ranger.plugin.util.CachedResourceEvaluators;
import org.apache.ranger.plugin.util.DownloadTrigger;
import org.apache.ranger.plugin.util.DownloaderTask;
import org.apache.ranger.plugin.util.RangerAccessRequestUtil;
import org.apache.ranger.plugin.util.RangerCommonConstants;
import org.apache.ranger.plugin.util.RangerPerfTracer;
import org.apache.ranger.plugin.util.RangerReadWriteLock;
import org.apache.ranger.plugin.util.RangerServiceNotFoundException;
import org.apache.ranger.plugin.util.RangerServiceTagsDeltaUtil;
import org.apache.ranger.plugin.util.ServiceTags;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.Reader;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class RangerTagEnricher extends RangerAbstractContextEnricher {
    private static final Logger LOG                            = LoggerFactory.getLogger(RangerTagEnricher.class);
    private static final Logger PERF_CONTEXTENRICHER_INIT_LOG  = RangerPerfTracer.getPerfLogger("contextenricher.init");
    private static final Logger PERF_SET_SERVICETAGS_LOG       = RangerPerfTracer.getPerfLogger("tagenricher.setservicetags");
    private static final Logger PERF_SERVICETAGS_RETRIEVAL_LOG = RangerPerfTracer.getPerfLogger("tagenricher.tags.retrieval");

    public  static final String TAG_RETRIEVER_CLASSNAME_OPTION       = "tagRetrieverClassName";
    private static final String TAG_REFRESHER_POLLINGINTERVAL_OPTION = "tagRefresherPollingInterval";
    private static final String TAG_DISABLE_TRIE_PREFILTER_OPTION    = "disableTrieLookupPrefilter";

    private final BlockingQueue<DownloadTrigger> tagDownloadQueue = new LinkedBlockingQueue<>();
    private final RangerReadWriteLock            lock             = new RangerReadWriteLock(false);
    private final CachedResourceEvaluators       cache            = new CachedResourceEvaluators();
    private       RangerTagRefresher             tagRefresher;
    private       RangerTagRetriever             tagRetriever;
    private       boolean                        disableTrieLookupPrefilter;
    private       EnrichedServiceTags            enrichedServiceTags;
    private       boolean                        disableCacheIfServiceNotFound = true;
    private       boolean                        dedupStrings                  = true;
    private       Timer                          tagDownloadTimer;
    private       RangerServiceDefHelper         serviceDefHelper;

    public static RangerServiceResourceMatcher createRangerServiceResourceMatcher(RangerServiceResource serviceResource, RangerServiceDefHelper serviceDefHelper, ResourceHierarchies hierarchies, RangerPluginContext pluginContext) {
        LOG.debug("==> createRangerServiceResourceMatcher(serviceResource={})", serviceResource);

        RangerServiceResourceMatcher ret = null;

        final Collection<String> resourceKeys = serviceResource.getResourceElements().keySet();

        for (int policyType : RangerPolicy.POLICY_TYPES) {
            Boolean isValidHierarchy = hierarchies.isValidHierarchy(policyType, resourceKeys);

            if (isValidHierarchy == null) { // hierarchy not yet validated
                isValidHierarchy = Boolean.FALSE;

                for (List<RangerResourceDef> hierarchy : serviceDefHelper.getResourceHierarchies(policyType)) {
                    if (serviceDefHelper.hierarchyHasAllResources(hierarchy, resourceKeys)) {
                        isValidHierarchy = Boolean.TRUE;

                        break;
                    }
                }

                hierarchies.addHierarchy(policyType, resourceKeys, isValidHierarchy);
            }

            if (isValidHierarchy) {
                RangerDefaultPolicyResourceMatcher matcher = new RangerDefaultPolicyResourceMatcher();

                matcher.setServiceDef(serviceDefHelper.getServiceDef());
                matcher.setPolicyResources(serviceResource.getResourceElements(), policyType);
                matcher.setPluginContext(pluginContext);

                LOG.debug("RangerTagEnricher.setServiceTags() - Initializing matcher with (resource={}, serviceDef={})", serviceResource, serviceDefHelper.getServiceDef());

                matcher.setServiceDefHelper(serviceDefHelper);
                matcher.init();

                ret = new RangerServiceResourceMatcher(serviceResource, matcher);

                break;
            }
        }

        LOG.debug("<== createRangerServiceResourceMatcher(serviceResource={}) : [{}]", serviceResource, ret);

        return ret;
    }

    @Override
    public void init() {
        LOG.debug("==> RangerTagEnricher.init()");

        super.init();

        String propertyPrefix        = getPropertyPrefix();
        String tagRetrieverClassName = getOption(TAG_RETRIEVER_CLASSNAME_OPTION);
        long   pollingIntervalMs     = getLongOption(TAG_REFRESHER_POLLINGINTERVAL_OPTION, 60 * 1000L);

        dedupStrings               = getBooleanConfig(propertyPrefix + ".dedup.strings", true);
        disableTrieLookupPrefilter = getBooleanOption(TAG_DISABLE_TRIE_PREFILTER_OPTION, false);
        serviceDefHelper           = new RangerServiceDefHelper(serviceDef, false);

        if (StringUtils.isNotBlank(tagRetrieverClassName)) {
            try {
                @SuppressWarnings("unchecked")
                Class<RangerTagRetriever> tagRetriverClass = (Class<RangerTagRetriever>) Class.forName(tagRetrieverClassName);

                tagRetriever = tagRetriverClass.newInstance();
            } catch (ClassNotFoundException exception) {
                LOG.error("Class {} not found, exception={}", tagRetrieverClassName, exception);
            } catch (ClassCastException exception) {
                LOG.error("Class {} is not a type of RangerTagRetriever, exception={}", tagRetrieverClassName, exception);
            } catch (IllegalAccessException exception) {
                LOG.error("Class {} illegally accessed, exception={}", tagRetrieverClassName, exception);
            } catch (InstantiationException exception) {
                LOG.error("Class {} could not be instantiated, exception={}", tagRetrieverClassName, exception);
            }

            if (tagRetriever != null) {
                disableCacheIfServiceNotFound = getBooleanConfig(propertyPrefix + ".disable.cache.if.servicenotfound", true);

                String cacheDir      = getConfig(propertyPrefix + ".policy.cache.dir", null);
                String cacheFilename = String.format("%s_%s_tag.json", appId, serviceName);

                cacheFilename = cacheFilename.replace(File.separatorChar, '_');
                cacheFilename = cacheFilename.replace(File.pathSeparatorChar, '_');

                String cacheFile = cacheDir == null ? null : (cacheDir + File.separator + cacheFilename);

                createLock();

                tagRetriever.setServiceName(serviceName);
                tagRetriever.setServiceDef(serviceDef);
                tagRetriever.setAppId(appId);
                tagRetriever.setPluginConfig(getPluginConfig());
                tagRetriever.setPluginContext(getPluginContext());
                tagRetriever.init(enricherDef.getEnricherOptions());

                tagRefresher = new RangerTagRefresher(tagRetriever, this, -1L, tagDownloadQueue, cacheFile);

                LOG.info("Created RangerTagRefresher Thread({})", tagRefresher.getName());

                try {
                    tagRefresher.populateTags();
                } catch (Throwable exception) {
                    LOG.error("Exception when retrieving tag for the first time for this enricher", exception);
                }

                tagRefresher.setDaemon(true);
                tagRefresher.startRefresher();

                tagDownloadTimer = new Timer("policyDownloadTimer", true);

                try {
                    tagDownloadTimer.schedule(new DownloaderTask(tagDownloadQueue), pollingIntervalMs, pollingIntervalMs);

                    LOG.debug("Scheduled tagDownloadRefresher to download tags every {} milliseconds", pollingIntervalMs);
                } catch (IllegalStateException exception) {
                    LOG.error("Error scheduling tagDownloadTimer:", exception);
                    LOG.error("*** Tags will NOT be downloaded every {} milliseconds ***", pollingIntervalMs);

                    tagDownloadTimer = null;
                }
            }
        } else {
            LOG.error("No value specified for {} in the RangerTagEnricher options", TAG_RETRIEVER_CLASSNAME_OPTION);
        }

        LOG.debug("<== RangerTagEnricher.init()");
    }

    @Override
    public void enrich(RangerAccessRequest request, Object dataStore) {
        LOG.debug("==> RangerTagEnricher.enrich({}) with dataStore:[{}]", request, dataStore);

        final Set<RangerTagForEval> matchedTags;

        try (RangerReadWriteLock.RangerLock readLock = this.lock.getReadLock()) {
            if (readLock.isLockingEnabled()) {
                LOG.debug("Acquired lock - {}", readLock);
            }

            final EnrichedServiceTags enrichedServiceTags;

            if (dataStore instanceof EnrichedServiceTags) {
                enrichedServiceTags = (EnrichedServiceTags) dataStore;
            } else {
                enrichedServiceTags = this.enrichedServiceTags;

                if (dataStore != null) {
                    LOG.warn("Incorrect type of dataStore :[{}], falling back to original enrich", dataStore.getClass().getName());
                }
            }

            matchedTags = enrichedServiceTags == null ? null : findMatchingTags(request, enrichedServiceTags);

            RangerAccessRequestUtil.setRequestTagsInContext(request.getContext(), matchedTags);
        }

        LOG.debug("<== RangerTagEnricher.enrich({}) with dataStore:[{}]): tags count={}", request, dataStore, (matchedTags == null ? 0 : matchedTags.size()));
    }

    /*
     * This class implements a cache of result of look-up of keyset of policy-resources for each of the collections of hierarchies
     * for policy types: access, datamask and rowfilter. If a keyset is examined for validity in a hierarchy of a policy-type,
     * then that record is maintained in this cache for later look-up.
     *
     * The basic idea is that with a large number of tagged service-resources, this cache will speed up performance as well as put
     * a cap on the upper bound because it is expected that the cardinality of set of all possible keysets for all resource-def
     * combinations in a service-def will be much smaller than the number of service-resources.
     */
    @Override
    public boolean preCleanup() {
        LOG.debug("==> RangerTagEnricher.preCleanup()");

        super.preCleanup();

        Timer tagDownloadTimer = this.tagDownloadTimer;

        this.tagDownloadTimer = null;

        if (tagDownloadTimer != null) {
            tagDownloadTimer.cancel();
        }

        RangerTagRefresher tagRefresher = this.tagRefresher;

        this.tagRefresher = null;

        if (tagRefresher != null) {
            LOG.debug("Trying to clean up RangerTagRefresher({})", tagRefresher.getName());

            tagRefresher.cleanup();
        }

        LOG.debug("<== RangerTagEnricher.preCleanup() : result={}", true);

        return true;
    }

    @Override
    public void enrich(RangerAccessRequest request) {
        LOG.debug("==> RangerTagEnricher.enrich({})", request);

        enrich(request, null);

        LOG.debug("<== RangerTagEnricher.enrich({})", request);
    }

    public void setServiceTags(final ServiceTags serviceTags) {
        boolean rebuildOnlyIndex = false;

        setServiceTags(serviceTags, rebuildOnlyIndex);
    }

    public Long getServiceTagsVersion() {
        EnrichedServiceTags localEnrichedServiceTags = enrichedServiceTags;

        return localEnrichedServiceTags != null ? localEnrichedServiceTags.getServiceTags().getTagVersion() : -1L;
    }

    public void syncTagsWithAdmin(final DownloadTrigger token) throws InterruptedException {
        tagDownloadQueue.put(token);

        token.waitForCompletion();
    }

    public EnrichedServiceTags getEnrichedServiceTags() {
        return enrichedServiceTags;
    }

    protected void setServiceTags(final ServiceTags serviceTags, final boolean rebuildOnlyIndex) {
        LOG.debug("==> RangerTagEnricher.setServiceTags(serviceTags={}, rebuildOnlyIndex={})", serviceTags, rebuildOnlyIndex);

        final EnrichedServiceTags localEnrichedServiceTags;
        final Set<String>         keysToRemoveFromCache = new HashSet<>();

        try (RangerReadWriteLock.RangerLock writeLock = this.lock.getWriteLock()) {
            if (writeLock.isLockingEnabled()) {
                LOG.debug("Acquired lock - {}", writeLock);
            }

            RangerPerfTracer perf = null;

            if (RangerPerfTracer.isPerfTraceEnabled(PERF_SET_SERVICETAGS_LOG)) {
                perf = RangerPerfTracer.getPerfTracer(PERF_SET_SERVICETAGS_LOG, "RangerTagEnricher.setServiceTags(newTagVersion=" + serviceTags.getTagVersion() + ",isDelta=" + serviceTags.getIsDelta() + ")");
            }

            if (serviceTags == null) {
                LOG.info("ServiceTags is null for service {}", serviceName);

                localEnrichedServiceTags = null;
            } else {
                if (dedupStrings) {
                    serviceTags.dedupStrings();
                }

                if (!serviceTags.getIsDelta()) {
                    if (serviceTags.getIsTagsDeduped()) {
                        final int countOfDuplicateTags = serviceTags.dedupTags();

                        LOG.info("Number of duplicate tags removed from the received serviceTags:[{}]. Number of tags in the de-duplicated serviceTags :[{}].", countOfDuplicateTags, serviceTags.getTags().size());
                    }

                    localEnrichedServiceTags = processServiceTags(serviceTags);
                } else {
                    LOG.debug("Received service-tag deltas:{}", serviceTags);

                    ServiceTags oldServiceTags = enrichedServiceTags != null ? enrichedServiceTags.getServiceTags() : new ServiceTags();
                    ServiceTags allServiceTags = rebuildOnlyIndex ? oldServiceTags : RangerServiceTagsDeltaUtil.applyDelta(oldServiceTags, serviceTags, serviceTags.getIsTagsDeduped());

                    if (serviceTags.getTagsChangeExtent() == ServiceTags.TagsChangeExtent.NONE) {
                        LOG.debug("No change to service-tags other than version change");

                        localEnrichedServiceTags = enrichedServiceTags;
                    } else {
                        if (serviceTags.getTagsChangeExtent() != ServiceTags.TagsChangeExtent.TAGS) {
                            Map<String, RangerResourceTrie<RangerServiceResourceMatcher>> trieMap;

                            if (enrichedServiceTags == null) {
                                trieMap = new HashMap<>();
                            } else {
                                trieMap = writeLock.isLockingEnabled() ? enrichedServiceTags.getServiceResourceTrie() : copyServiceResourceTrie();
                            }

                            localEnrichedServiceTags = processServiceTagDeltas(serviceTags, allServiceTags, trieMap, keysToRemoveFromCache);
                        } else {
                            LOG.debug("Delta contains only tag attribute changes");

                            List<RangerServiceResourceMatcher>                            resourceMatchers    = enrichedServiceTags != null ? enrichedServiceTags.getServiceResourceMatchers() : new ArrayList<>();
                            Map<String, RangerResourceTrie<RangerServiceResourceMatcher>> serviceResourceTrie = enrichedServiceTags != null ? enrichedServiceTags.getServiceResourceTrie() : new HashMap<>();

                            localEnrichedServiceTags = new EnrichedServiceTags(allServiceTags, resourceMatchers, serviceResourceTrie);
                        }
                    }
                }
            }

            synchronized (RangerTagEnricher.class) {
                enrichedServiceTags = localEnrichedServiceTags;

                if (serviceTags != null) {
                    if (serviceTags.getIsDelta()) {
                        cache.removeCacheEvaluators(keysToRemoveFromCache);

                        keysToRemoveFromCache.clear();
                    } else {
                        cache.clearCache();
                    }
                }

                setEnrichedServiceTagsInPlugin();
            }

            RangerPerfTracer.logAlways(perf);
        }

        LOG.debug("<== RangerTagEnricher.setServiceTags(serviceTags={}, rebuildOnlyIndex={})", serviceTags, rebuildOnlyIndex);
    }

    protected Long getResourceTrieVersion() {
        EnrichedServiceTags localEnrichedServiceTags = enrichedServiceTags;

        return localEnrichedServiceTags != null ? localEnrichedServiceTags.getResourceTrieVersion() : -1L;
    }

    protected RangerReadWriteLock createLock() {
        String             propertyPrefix        = getPropertyPrefix();
        RangerPluginConfig config                = getPluginConfig();
        boolean            deltasEnabled         = config != null && config.getBoolean(propertyPrefix + RangerCommonConstants.PLUGIN_CONFIG_SUFFIX_TAG_DELTA, RangerCommonConstants.PLUGIN_CONFIG_SUFFIX_TAG_DELTA_DEFAULT);
        boolean            inPlaceUpdatesEnabled = config != null && config.getBoolean(propertyPrefix + RangerCommonConstants.PLUGIN_CONFIG_SUFFIX_IN_PLACE_TAG_UPDATES, RangerCommonConstants.PLUGIN_CONFIG_SUFFIX_IN_PLACE_TAG_UPDATES_DEFAULT);
        boolean            useReadWriteLock      = deltasEnabled && inPlaceUpdatesEnabled;

        LOG.info("Policy-Engine will{}use read-write locking to update tags in place when tag-deltas are provided", (useReadWriteLock ? " " : " not "));

        return new RangerReadWriteLock(useReadWriteLock);
    }

    private EnrichedServiceTags processServiceTags(ServiceTags serviceTags) {
        LOG.debug("Processing all service-tags");

        final EnrichedServiceTags ret;

        if (CollectionUtils.isEmpty(serviceTags.getServiceResources())) {
            LOG.info("There are no tagged resources for service {}", serviceName);
            ret = null;
        } else {
            ResourceHierarchies                hierarchies      = new ResourceHierarchies();
            List<RangerServiceResourceMatcher> resourceMatchers = new ArrayList<>();
            List<RangerServiceResource>        serviceResources = serviceTags.getServiceResources();

            for (ListIterator<RangerServiceResource> iter = serviceResources.listIterator(); iter.hasNext(); ) {
                RangerServiceResource        serviceResource        = iter.next();
                RangerServiceResourceMatcher serviceResourceMatcher = createRangerServiceResourceMatcher(serviceResource, serviceDefHelper, hierarchies, getPluginContext());

                if (serviceResourceMatcher != null) {
                    resourceMatchers.add(serviceResourceMatcher);
                } else {
                    iter.remove();

                    List<Long> tags = serviceTags.getResourceToTagIds().remove(serviceResource.getId());

                    LOG.warn("Invalid resource [{}]: failed to create resource-matcher. Ignoring {} tags associated with the resource", serviceResource, (tags != null ? tags.size() : 0));
                }
            }

            Map<String, RangerResourceTrie<RangerServiceResourceMatcher>> serviceResourceTrie = null;

            if (!disableTrieLookupPrefilter) {
                serviceResourceTrie = new HashMap<>();

                for (RangerResourceDef resourceDef : serviceDef.getResources()) {
                    serviceResourceTrie.put(resourceDef.getName(), new RangerResourceTrie(resourceDef, resourceMatchers, getPolicyEngineOptions().optimizeTagTrieForRetrieval, getPolicyEngineOptions().optimizeTagTrieForSpace, null));
                }
            }

            ret = new EnrichedServiceTags(serviceTags, resourceMatchers, serviceResourceTrie);
        }
        return ret;
    }

    private EnrichedServiceTags processServiceTagDeltas(ServiceTags deltas, ServiceTags allServiceTags, Map<String, RangerResourceTrie<RangerServiceResourceMatcher>> serviceResourceTrie, Set<String> keysToRemoveFromCache) {
        LOG.debug("Delta contains changes other than tag attribute changes, [{}]", deltas.getTagsChangeExtent());

        boolean                            isInError        = false;
        ResourceHierarchies                hierarchies      = new ResourceHierarchies();
        List<RangerServiceResourceMatcher> resourceMatchers = new ArrayList<>();

        if (enrichedServiceTags != null) {
            resourceMatchers.addAll(enrichedServiceTags.getServiceResourceMatchers());
        }

        List<RangerServiceResource> changedServiceResources = deltas.getServiceResources();

        for (RangerServiceResource serviceResource : changedServiceResources) {
            final RangerAccessResource removedAccessResource = MapUtils.isEmpty(serviceResource.getResourceElements()) ? null : removeOldServiceResource(serviceResource, resourceMatchers, serviceResourceTrie);

            if (removedAccessResource != null) {
                if (!StringUtils.isEmpty(serviceResource.getResourceSignature())) {
                    RangerServiceResourceMatcher resourceMatcher = createRangerServiceResourceMatcher(serviceResource, serviceDefHelper, hierarchies, getPluginContext());

                    if (resourceMatcher != null) {
                        for (RangerResourceDef resourceDef : serviceDef.getResources()) {
                            RangerPolicyResource                             policyResource = serviceResource.getResourceElements().get(resourceDef.getName());
                            RangerResourceTrie<RangerServiceResourceMatcher> trie           = serviceResourceTrie.get(resourceDef.getName());

                            if (trie != null) {
                                LOG.debug("Trying to add resource-matcher to existing trie for {}", resourceDef.getName());

                                trie.add(policyResource, resourceMatcher);
                                trie.wrapUpUpdate();

                                LOG.debug("Added resource-matcher for policy-resource:[{}]", policyResource);
                            } else {
                                LOG.debug("Trying to add resource-matcher to new trie for {}", resourceDef.getName());

                                trie = new RangerResourceTrie<>(resourceDef, Collections.singletonList(resourceMatcher), getPolicyEngineOptions().optimizeTagTrieForRetrieval, getPolicyEngineOptions().optimizeTagTrieForSpace, null);

                                serviceResourceTrie.put(resourceDef.getName(), trie);
                            }
                        }

                        resourceMatchers.add(resourceMatcher);
                    } else {
                        LOG.error("Could not create resource-matcher for resource: [{}]. Should NOT happen!!", serviceResource);
                        LOG.error("Setting tagVersion to -1 to ensure that in the next download all tags are downloaded");

                        isInError = true;
                    }
                } else {
                    LOG.debug("Service-resource:[id={}] is deleted as its resource-signature is empty. No need to create it!", serviceResource.getId());
                }

                keysToRemoveFromCache.add(removedAccessResource.getCacheKey());
            } else {
                isInError = true;
            }

            if (isInError) {
                break;
            }
        }

        final EnrichedServiceTags ret;

        if (isInError) {
            LOG.error("Error in processing tag-deltas. Will continue to use old tags");

            deltas.setTagVersion(-1L);
            keysToRemoveFromCache.clear();

            ret = enrichedServiceTags;
        } else {
            for (Map.Entry<String, RangerResourceTrie<RangerServiceResourceMatcher>> entry : serviceResourceTrie.entrySet()) {
                entry.getValue().wrapUpUpdate();
            }

            ret = new EnrichedServiceTags(allServiceTags, resourceMatchers, serviceResourceTrie);
        }

        return ret;
    }

    private RangerAccessResource removeOldServiceResource(RangerServiceResource serviceResource, List<RangerServiceResourceMatcher> resourceMatchers, Map<String, RangerResourceTrie<RangerServiceResourceMatcher>> resourceTries) {
        final RangerAccessResource ret;
        boolean                    result = true;

        if (enrichedServiceTags != null) {
            LOG.debug("Removing service-resource:[{}] from trie-map", serviceResource);

            RangerAccessResourceImpl accessResource = new RangerAccessResourceImpl();

            for (Map.Entry<String, RangerPolicyResource> entry : serviceResource.getResourceElements().entrySet()) {
                accessResource.setValue(entry.getKey(), entry.getValue().getValues());
            }

            accessResource.setServiceDef(serviceDef);

            LOG.debug("RangerAccessResource:[{}] created to represent service-resource[{}] to find evaluators from trie-map", accessResource, serviceResource);

            RangerAccessRequestImpl request = new RangerAccessRequestImpl();

            request.setResource(accessResource);

            Collection<RangerServiceResourceMatcher> oldMatchers = CachedResourceEvaluators.getEvaluators(request, enrichedServiceTags.getServiceResourceTrie(), cache);

            LOG.debug("Found [{}] matchers for service-resource[{}]", oldMatchers, serviceResource);

            if (CollectionUtils.isNotEmpty(oldMatchers)) {
                List<RangerServiceResourceMatcher> notMatched = new ArrayList<>();

                for (RangerServiceResourceMatcher resourceMatcher : oldMatchers) {
                    final RangerPolicyResourceMatcher.MatchType matchType = resourceMatcher.getMatchType(accessResource, request.getResourceElementMatchingScopes(), request.getContext());

                    LOG.debug("resource:[{}, MatchType:[{}]", accessResource, matchType);

                    if (matchType != RangerPolicyResourceMatcher.MatchType.SELF) {
                        notMatched.add(resourceMatcher);
                    }
                }

                LOG.debug("oldMatchers : [{}] do not match resource:[{}] exactly and will be discarded", notMatched, accessResource);

                oldMatchers.removeAll(notMatched);
            }

            for (RangerServiceResourceMatcher matcher : oldMatchers) {
                for (RangerResourceDef resourceDef : serviceDef.getResources()) {
                    String                                           resourceDefName = resourceDef.getName();
                    RangerResourceTrie<RangerServiceResourceMatcher> trie            = resourceTries.get(resourceDefName);

                    if (trie != null) {
                        trie.delete(serviceResource.getResourceElements().get(resourceDefName), matcher);
                    } else {
                        LOG.error("Cannot find resourceDef with name:[{}]. Should NOT happen!!", resourceDefName);
                        LOG.error("Setting tagVersion to -1 to ensure that in the next download all tags are downloaded");

                        result = false;

                        break;
                    }
                }
            }

            if (result) {
                resourceMatchers.removeAll(oldMatchers);

                LOG.debug("Found and removed [{}] matchers for service-resource[{}] from trie-map", oldMatchers, serviceResource);

                ret = accessResource;
            } else {
                ret = null;
            }
        } else {
            ret = null;
        }

        return ret;
    }

    private void setEnrichedServiceTagsInPlugin() {
        LOG.debug("==> setEnrichedServiceTagsInPlugin()");

        RangerAuthContext authContext = getAuthContext();

        if (authContext != null) {
            authContext.addOrReplaceRequestContextEnricher(this, enrichedServiceTags);

            notifyAuthContextChanged();
        }

        LOG.debug("<== setEnrichedServiceTagsInPlugin()");
    }

    private Set<RangerTagForEval> findMatchingTags(final RangerAccessRequest request, EnrichedServiceTags dataStore) {
        LOG.debug("==> RangerTagEnricher.findMatchingTags({})", request);

        // To minimize chance for race condition between Tag-Refresher thread and access-evaluation thread
        final EnrichedServiceTags enrichedServiceTags = dataStore != null ? dataStore : this.enrichedServiceTags;

        Set<RangerTagForEval> ret      = null;
        RangerAccessResource  resource = request.getResource();
        RangerPerfTracer      perf     = null;

        if (RangerPerfTracer.isPerfTraceEnabled(PERF_SERVICETAGS_RETRIEVAL_LOG)) {
            perf = RangerPerfTracer.getPerfTracer(PERF_SERVICETAGS_RETRIEVAL_LOG, "RangerTagEnricher.findMatchingTags=" + resource.getAsString() + ")");
        }

        if ((resource == null || resource.getKeys() == null || resource.getKeys().isEmpty()) && request.isAccessTypeAny()) {
            ret = enrichedServiceTags.getTagsForEmptyResourceAndAnyAccess();
        } else {
            final Collection<RangerServiceResourceMatcher> serviceResourceMatchers = CachedResourceEvaluators.getEvaluators(request, enrichedServiceTags.getServiceResourceTrie(), cache);

            if (CollectionUtils.isNotEmpty(serviceResourceMatchers)) {
                for (RangerServiceResourceMatcher resourceMatcher : serviceResourceMatchers) {
                    final RangerPolicyResourceMatcher.MatchType matchType = resourceMatcher.getMatchType(resource, request.getResourceElementMatchingScopes(), request.getContext());

                    LOG.debug("resource:[{}, MatchType:[{}]", resource, matchType);

                    final ResourceMatchingScope resourceMatchingScope = request.getResourceMatchingScope() != null ? request.getResourceMatchingScope() : ResourceMatchingScope.SELF;
                    final boolean               isMatched;

                    if (request.isAccessTypeAny() || resourceMatchingScope == ResourceMatchingScope.SELF_OR_DESCENDANTS) {
                        isMatched = matchType == RangerPolicyResourceMatcher.MatchType.ANCESTOR || matchType == RangerPolicyResourceMatcher.MatchType.SELF || matchType == RangerPolicyResourceMatcher.MatchType.SELF_AND_ALL_DESCENDANTS || matchType == RangerPolicyResourceMatcher.MatchType.DESCENDANT;
                    } else {
                        isMatched = matchType == RangerPolicyResourceMatcher.MatchType.ANCESTOR || matchType == RangerPolicyResourceMatcher.MatchType.SELF || matchType == RangerPolicyResourceMatcher.MatchType.SELF_AND_ALL_DESCENDANTS;
                    }

                    if (isMatched) {
                        if (ret == null) {
                            ret = new HashSet<>();
                        }
                        ret.addAll(getTagsForServiceResource(request.getAccessTime(), enrichedServiceTags.getServiceTags(), resourceMatcher.getServiceResource(), matchType));
                    }
                }
            }
        }

        RangerPerfTracer.logAlways(perf);

        if (CollectionUtils.isEmpty(ret)) {
            LOG.debug("RangerTagEnricher.findMatchingTags({}) - No tags Found ", resource);
        } else {
            LOG.debug("RangerTagEnricher.findMatchingTags({}) - {} tags Found ", resource, ret.size());
        }

        LOG.debug("<== RangerTagEnricher.findMatchingTags({})", request);

        return ret;
    }

    private static Set<RangerTagForEval> getTagsForServiceResource(Date accessTime, final ServiceTags serviceTags, final RangerServiceResource serviceResource, final RangerPolicyResourceMatcher.MatchType matchType) {
        Set<RangerTagForEval> ret = new HashSet<>();

        final Long                  resourceId       = serviceResource.getId();
        final Map<Long, List<Long>> resourceToTagIds = serviceTags.getResourceToTagIds();
        final Map<Long, RangerTag>  tags             = serviceTags.getTags();

        LOG.debug("Looking for tags for resource-id:[{}] in serviceTags:[{}]", resourceId, serviceTags);

        if (resourceId != null && MapUtils.isNotEmpty(resourceToTagIds) && MapUtils.isNotEmpty(tags)) {
            List<Long> tagIds = resourceToTagIds.get(resourceId);

            if (CollectionUtils.isNotEmpty(tagIds)) {
                accessTime = accessTime == null ? new Date() : accessTime;

                for (Long tagId : tagIds) {
                    RangerTag tag = tags.get(tagId);

                    if (tag != null) {
                        RangerTagForEval tagForEval = new RangerTagForEval(tag, matchType);

                        if (tagForEval.isApplicable(accessTime)) {
                            ret.add(tagForEval);
                        }
                    }
                }
            } else {
                LOG.debug("No tags mapping found for resource:[{}]", resourceId);
            }
        } else {
            LOG.debug("resourceId is null or resourceToTagTds mapping is null or tags mapping is null!");
        }

        return ret;
    }

    private Map<String, RangerResourceTrie<RangerServiceResourceMatcher>> copyServiceResourceTrie() {
        Map<String, RangerResourceTrie<RangerServiceResourceMatcher>> ret = new HashMap<>();

        if (enrichedServiceTags != null) {
            for (Map.Entry<String, RangerResourceTrie<RangerServiceResourceMatcher>> entry : enrichedServiceTags.getServiceResourceTrie().entrySet()) {
                RangerResourceTrie<RangerServiceResourceMatcher> resourceTrie = new RangerResourceTrie<>(entry.getValue());

                ret.put(entry.getKey(), resourceTrie);
            }
        }
        return ret;
    }

    public static class ResourceHierarchies {
        private final Map<Collection<String>, Boolean> accessHierarchies    = new HashMap<>();
        private final Map<Collection<String>, Boolean> dataMaskHierarchies  = new HashMap<>();
        private final Map<Collection<String>, Boolean> rowFilterHierarchies = new HashMap<>();

        Boolean isValidHierarchy(int policyType, Collection<String> resourceKeys) {
            switch (policyType) {
                case RangerPolicy.POLICY_TYPE_ACCESS:
                    return accessHierarchies.get(resourceKeys);
                case RangerPolicy.POLICY_TYPE_DATAMASK:
                    return dataMaskHierarchies.get(resourceKeys);
                case RangerPolicy.POLICY_TYPE_ROWFILTER:
                    return rowFilterHierarchies.get(resourceKeys);
                default:
                    return null;
            }
        }

        void addHierarchy(int policyType, Collection<String> resourceKeys, Boolean isValid) {
            switch (policyType) {
                case RangerPolicy.POLICY_TYPE_ACCESS:
                    accessHierarchies.put(resourceKeys, isValid);
                    break;
                case RangerPolicy.POLICY_TYPE_DATAMASK:
                    dataMaskHierarchies.put(resourceKeys, isValid);
                    break;
                case RangerPolicy.POLICY_TYPE_ROWFILTER:
                    rowFilterHierarchies.put(resourceKeys, isValid);
                    break;
                default:
                    LOG.error("unknown policy-type {}", policyType);
                    break;
            }
        }
    }

    public static final class EnrichedServiceTags {
        private final ServiceTags                                                   serviceTags;
        private final List<RangerServiceResourceMatcher>                            serviceResourceMatchers;
        private final Map<String, RangerResourceTrie<RangerServiceResourceMatcher>> serviceResourceTrie;
        private final Set<RangerTagForEval>                                         tagsForEmptyResourceAndAnyAccess; // Used only when accessed resource is empty and access type is 'any'
        private final Long                                                          resourceTrieVersion;

        EnrichedServiceTags(ServiceTags serviceTags, List<RangerServiceResourceMatcher> serviceResourceMatchers, Map<String, RangerResourceTrie<RangerServiceResourceMatcher>> serviceResourceTrie) {
            this.serviceTags                      = serviceTags;
            this.serviceResourceMatchers          = serviceResourceMatchers;
            this.serviceResourceTrie              = serviceResourceTrie;
            this.tagsForEmptyResourceAndAnyAccess = createTagsForEmptyResourceAndAnyAccess();
            this.resourceTrieVersion              = serviceTags.getTagVersion();
        }

        public ServiceTags getServiceTags() {
            return serviceTags;
        }

        public List<RangerServiceResourceMatcher> getServiceResourceMatchers() {
            return serviceResourceMatchers;
        }

        public Map<String, RangerResourceTrie<RangerServiceResourceMatcher>> getServiceResourceTrie() {
            return serviceResourceTrie;
        }

        public Long getResourceTrieVersion() {
            return resourceTrieVersion;
        }

        public Set<RangerTagForEval> getTagsForEmptyResourceAndAnyAccess() {
            return tagsForEmptyResourceAndAnyAccess;
        }

        private Set<RangerTagForEval> createTagsForEmptyResourceAndAnyAccess() {
            Set<RangerTagForEval> tagsForEmptyResourceAndAnyAccess = new HashSet<>();

            for (Map.Entry<Long, RangerTag> entry : serviceTags.getTags().entrySet()) {
                tagsForEmptyResourceAndAnyAccess.add(new RangerTagForEval(entry.getValue(), RangerPolicyResourceMatcher.MatchType.DESCENDANT));
            }

            return tagsForEmptyResourceAndAnyAccess;
        }
    }

    static class RangerTagRefresher extends Thread {
        private static final Logger LOG = LoggerFactory.getLogger(RangerTagRefresher.class);

        private final RangerTagRetriever             tagRetriever;
        private final RangerTagEnricher              tagEnricher;
        private final BlockingQueue<DownloadTrigger> tagDownloadQueue;
        private final String                         cacheFile;
        private       long                           lastKnownVersion;
        private       long                           lastActivationTimeInMillis;
        private       boolean                        hasProvidedTagsToReceiver;

        RangerTagRefresher(RangerTagRetriever tagRetriever, RangerTagEnricher tagEnricher, long lastKnownVersion, BlockingQueue<DownloadTrigger> tagDownloadQueue, String cacheFile) {
            this.tagRetriever     = tagRetriever;
            this.tagEnricher      = tagEnricher;
            this.lastKnownVersion = lastKnownVersion;
            this.tagDownloadQueue = tagDownloadQueue;
            this.cacheFile        = cacheFile;

            setName("RangerTagRefresher(serviceName=" + tagRetriever.getServiceName() + ")-" + getId());
        }

        public long getLastActivationTimeInMillis() {
            return lastActivationTimeInMillis;
        }

        public void setLastActivationTimeInMillis(long lastActivationTimeInMillis) {
            this.lastActivationTimeInMillis = lastActivationTimeInMillis;
        }

        @Override
        public void run() {
            LOG.debug("==> RangerTagRefresher().run()");

            while (true) {
                DownloadTrigger trigger = null;

                try {
                    RangerPerfTracer perf = null;

                    if (RangerPerfTracer.isPerfTraceEnabled(PERF_CONTEXTENRICHER_INIT_LOG)) {
                        perf = RangerPerfTracer.getPerfTracer(PERF_CONTEXTENRICHER_INIT_LOG, "RangerTagRefresher(" + getName() + ").populateTags(lastKnownVersion=" + lastKnownVersion + ")");
                    }

                    trigger = tagDownloadQueue.take();

                    populateTags();

                    RangerPerfTracer.log(perf);
                } catch (InterruptedException excp) {
                    LOG.info("RangerTagRefresher({}).run(): Interrupted! Exiting thread", getName(), excp);
                    break;
                } finally {
                    if (trigger != null) {
                        trigger.signalCompletion();
                    }
                }
            }

            LOG.debug("<== RangerTagRefresher().run()");
        }

        void cleanup() {
            LOG.debug("==> RangerTagRefresher.cleanup()");

            stopRefresher();

            LOG.debug("<== RangerTagRefresher.cleanup()");
        }

        final void startRefresher() {
            try {
                super.start();
            } catch (Exception excp) {
                LOG.error("RangerTagRefresher({}).startRetriever(): Failed to start, exception={}", getName(), excp);
            }
        }

        final ServiceTags loadFromCache() {
            ServiceTags serviceTags = null;

            LOG.debug("==> RangerTagRetriever(serviceName={}).loadFromCache()", tagEnricher.getServiceName());

            File cacheFile = StringUtils.isEmpty(this.cacheFile) ? null : new File(this.cacheFile);

            if (cacheFile != null && cacheFile.isFile() && cacheFile.canRead()) {
                Reader reader = null;

                try {
                    reader = new FileReader(cacheFile);

                    serviceTags = JsonUtils.jsonToObject(reader, ServiceTags.class);

                    if (serviceTags != null && !StringUtils.equals(tagEnricher.getServiceName(), serviceTags.getServiceName())) {
                        LOG.warn("ignoring unexpected serviceName '{}' in cache file '{}'", serviceTags.getServiceName(), cacheFile.getAbsolutePath());

                        serviceTags.setServiceName(tagEnricher.getServiceName());
                    }
                } catch (Exception excp) {
                    LOG.error("failed to load service-tags from cache file {}", cacheFile.getAbsolutePath(), excp);
                } finally {
                    if (reader != null) {
                        try {
                            reader.close();
                        } catch (Exception excp) {
                            LOG.error("error while closing opened cache file {}", cacheFile.getAbsolutePath(), excp);
                        }
                    }
                }
            } else {
                LOG.warn("cache file does not exist or not readable '{}'", (cacheFile == null ? null : cacheFile.getAbsolutePath()));
            }

            LOG.debug("<== RangerTagRetriever(serviceName={}).loadFromCache()", tagEnricher.getServiceName());

            return serviceTags;
        }

        final void saveToCache(ServiceTags serviceTags) {
            LOG.debug("==> RangerTagRetriever(serviceName={}).saveToCache()", tagEnricher.getServiceName());

            if (serviceTags != null) {
                File cacheFile = StringUtils.isEmpty(this.cacheFile) ? null : new File(this.cacheFile);

                if (cacheFile != null) {
                    Writer writer = null;

                    try {
                        writer = new FileWriter(cacheFile);

                        JsonUtils.objectToWriter(writer, serviceTags);
                    } catch (Exception excp) {
                        LOG.error("failed to save service-tags to cache file '{}'", cacheFile.getAbsolutePath(), excp);
                    } finally {
                        if (writer != null) {
                            try {
                                writer.close();
                            } catch (Exception excp) {
                                LOG.error("error while closing opened cache file '{}'", cacheFile.getAbsolutePath(), excp);
                            }
                        }
                    }
                }
            } else {
                LOG.info("service-tags is null for service={}. Nothing to save in cache", tagRetriever.getServiceName());
            }

            LOG.debug("<== RangerTagRetriever(serviceName={}).saveToCache()", tagEnricher.getServiceName());
        }

        final void disableCache() {
            LOG.debug("==> RangerTagRetriever.disableCache(serviceName={})", tagEnricher.getServiceName());

            File cacheFile = StringUtils.isEmpty(this.cacheFile) ? null : new File(this.cacheFile);

            if (cacheFile != null && cacheFile.isFile() && cacheFile.canRead()) {
                LOG.warn("Cleaning up local tags cache");

                String renamedCacheFile = cacheFile.getAbsolutePath() + "_" + System.currentTimeMillis();

                if (!cacheFile.renameTo(new File(renamedCacheFile))) {
                    LOG.error("Failed to move {} to {}", cacheFile.getAbsolutePath(), renamedCacheFile);
                } else {
                    LOG.warn("moved {} to {}", cacheFile.getAbsolutePath(), renamedCacheFile);
                }
            } else {
                LOG.debug("No local TAGS cache found. No need to disable it!");
            }

            LOG.debug("<== RangerTagRetriever.disableCache(serviceName={})", tagEnricher.getServiceName());
        }

        private void populateTags() throws InterruptedException {
            if (tagEnricher != null) {
                ServiceTags serviceTags;

                try {
                    serviceTags = tagRetriever.retrieveTags(lastKnownVersion, lastActivationTimeInMillis);

                    if (serviceTags == null) {
                        if (!hasProvidedTagsToReceiver) {
                            serviceTags = loadFromCache();
                        }
                    } else if (!serviceTags.getIsDelta()) {
                        saveToCache(serviceTags);
                    }

                    if (serviceTags != null) {
                        tagEnricher.setServiceTags(serviceTags);

                        if (serviceTags.getIsDelta() && serviceTags.getTagVersion() != -1L) {
                            saveToCache(tagEnricher.enrichedServiceTags.serviceTags);
                        }

                        LOG.info("RangerTagRefresher(serviceName={}).populateTags() - Updated tags-cache to new version of tags, lastKnownVersion={}; newVersion={}",
                                tagRetriever.getServiceName(), lastKnownVersion, (serviceTags.getTagVersion() == null ? -1L : serviceTags.getTagVersion()));

                        hasProvidedTagsToReceiver = true;
                        lastKnownVersion          = serviceTags.getTagVersion() == null ? -1L : serviceTags.getTagVersion();

                        setLastActivationTimeInMillis(System.currentTimeMillis());
                    } else {
                        LOG.debug("RangerTagRefresher(serviceName={}).populateTags() - No need to update tags-cache. lastKnownVersion={}", tagRetriever.getServiceName(), lastKnownVersion);
                    }
                } catch (RangerServiceNotFoundException snfe) {
                    LOG.error("Caught ServiceNotFound exception :", snfe);

                    // Need to clean up local tag cache
                    if (tagEnricher.disableCacheIfServiceNotFound) {
                        disableCache();
                        tagEnricher.setServiceTags(null);
                        setLastActivationTimeInMillis(System.currentTimeMillis());

                        lastKnownVersion = -1L;
                    }
                } catch (InterruptedException interruptedException) {
                    throw interruptedException;
                } catch (Exception e) {
                    LOG.error("RangerTagRefresher(serviceName={}).populateTags(): Encountered unexpected exception. Ignoring", tagRetriever.getServiceName(), e);
                }
            } else {
                LOG.error("RangerTagRefresher(serviceName={}).populateTags() - no tag receiver to update tag-cache", tagRetriever.getServiceName());
            }
        }

        private void stopRefresher() {
            if (super.isAlive()) {
                super.interrupt();

                boolean setInterrupted = false;
                boolean isJoined       = false;

                while (!isJoined) {
                    try {
                        super.join();

                        isJoined = true;

                        LOG.debug("RangerTagRefresher({}) is stopped",  getName());
                    } catch (InterruptedException excp) {
                        LOG.warn("RangerTagRefresher({}).stopRefresher(): Error while waiting for thread to exit", getName(), excp);
                        LOG.warn("Retrying Thread.join(). Current thread will be marked as 'interrupted' after Thread.join() returns");

                        setInterrupted = true;
                    }
                }
                if (setInterrupted) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }
}
