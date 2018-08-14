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

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ranger.authorization.hadoop.config.RangerConfiguration;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.model.RangerServiceResource;
import org.apache.ranger.plugin.model.RangerTag;
import org.apache.ranger.plugin.model.validation.RangerServiceDefHelper;
import org.apache.ranger.plugin.policyengine.RangerAccessRequest;
import org.apache.ranger.plugin.policyengine.RangerAccessResource;
import org.apache.ranger.plugin.policyresourcematcher.RangerDefaultPolicyResourceMatcher;
import org.apache.ranger.plugin.policyresourcematcher.RangerPolicyResourceMatcher;
import org.apache.ranger.plugin.util.RangerAccessRequestUtil;
import org.apache.ranger.plugin.util.RangerPerfTracer;
import org.apache.ranger.plugin.util.RangerResourceTrie;
import org.apache.ranger.plugin.util.RangerServiceNotFoundException;
import org.apache.ranger.plugin.util.ServiceTags;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.Reader;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class RangerTagEnricher extends RangerAbstractContextEnricher {
	private static final Log LOG = LogFactory.getLog(RangerTagEnricher.class);

	private static final Log PERF_CONTEXTENRICHER_INIT_LOG = RangerPerfTracer.getPerfLogger("contextenricher.init");
	private static final Log PERF_TRIE_OP_LOG = RangerPerfTracer.getPerfLogger("resourcetrie.retrieval");


	public static final String TAG_REFRESHER_POLLINGINTERVAL_OPTION = "tagRefresherPollingInterval";
	public static final String TAG_RETRIEVER_CLASSNAME_OPTION       = "tagRetrieverClassName";
	public static final String TAG_DISABLE_TRIE_PREFILTER_OPTION    = "disableTrieLookupPrefilter";
	public static final int[] allPolicyTypes                        = new int[] {RangerPolicy.POLICY_TYPE_ACCESS, RangerPolicy.POLICY_TYPE_DATAMASK, RangerPolicy.POLICY_TYPE_ROWFILTER};

	private RangerTagRefresher                 tagRefresher               = null;
	private RangerTagRetriever                 tagRetriever               = null;
	private boolean                            disableTrieLookupPrefilter = false;
	private EnrichedServiceTags                enrichedServiceTags;
	private boolean                            disableCacheIfServiceNotFound = true;

	@Override
	public void init() {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> RangerTagEnricher.init()");
		}

		super.init();

		String tagRetrieverClassName = getOption(TAG_RETRIEVER_CLASSNAME_OPTION);

		long pollingIntervalMs = getLongOption(TAG_REFRESHER_POLLINGINTERVAL_OPTION, 60 * 1000);

		disableTrieLookupPrefilter = getBooleanOption(TAG_DISABLE_TRIE_PREFILTER_OPTION, false);

		if (StringUtils.isNotBlank(tagRetrieverClassName)) {

			try {
				@SuppressWarnings("unchecked")
				Class<RangerTagRetriever> tagRetriverClass = (Class<RangerTagRetriever>) Class.forName(tagRetrieverClassName);

				tagRetriever = tagRetriverClass.newInstance();

			} catch (ClassNotFoundException exception) {
				LOG.error("Class " + tagRetrieverClassName + " not found, exception=" + exception);
			} catch (ClassCastException exception) {
				LOG.error("Class " + tagRetrieverClassName + " is not a type of RangerTagRetriever, exception=" + exception);
			} catch (IllegalAccessException exception) {
				LOG.error("Class " + tagRetrieverClassName + " illegally accessed, exception=" + exception);
			} catch (InstantiationException exception) {
				LOG.error("Class " + tagRetrieverClassName + " could not be instantiated, exception=" + exception);
			}

			if (tagRetriever != null) {
				String propertyPrefix    = "ranger.plugin." + serviceDef.getName();
				disableCacheIfServiceNotFound = RangerConfiguration.getInstance().getBoolean(propertyPrefix + ".disable.cache.if.servicenotfound", true);
				String cacheDir          = RangerConfiguration.getInstance().get(propertyPrefix + ".policy.cache.dir");
				String cacheFilename = String.format("%s_%s_tag.json", appId, serviceName);
				cacheFilename = cacheFilename.replace(File.separatorChar,  '_');
				cacheFilename = cacheFilename.replace(File.pathSeparatorChar,  '_');

				String cacheFile = cacheDir == null ? null : (cacheDir + File.separator + cacheFilename);
				tagRetriever.setServiceName(serviceName);
				tagRetriever.setServiceDef(serviceDef);
				tagRetriever.setAppId(appId);
				tagRetriever.init(enricherDef.getEnricherOptions());

				tagRefresher = new RangerTagRefresher(tagRetriever, this, -1L, cacheFile, pollingIntervalMs);

				try {
					tagRefresher.populateTags();
				} catch (Throwable exception) {
					LOG.error("Exception when retrieving tag for the first time for this enricher", exception);
				}
				tagRefresher.setDaemon(true);
				tagRefresher.startRefresher();
			}
		} else {
			LOG.error("No value specified for " + TAG_RETRIEVER_CLASSNAME_OPTION + " in the RangerTagEnricher options");
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== RangerTagEnricher.init()");
		}
	}

	@Override
	public void enrich(RangerAccessRequest request) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> RangerTagEnricher.enrich(" + request + ")");
		}

		final Set<RangerTagForEval> matchedTags = enrichedServiceTags == null ? null : findMatchingTags(request);

		RangerAccessRequestUtil.setRequestTagsInContext(request.getContext(), matchedTags);

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== RangerTagEnricher.enrich(" + request + "): tags count=" + (matchedTags == null ? 0 : matchedTags.size()));
		}
	}

	/*
	 * This class implements a cache of result of look-up of keyset of policy-resources for each of the collections of hierarchies
	 * for policy types: access, datamask and rowfilter. If a keyset is examined for validity in a hierarchy of a policy-type,
	 * then that record is maintained in this cache for later look-up.
	 *
	 * The basic idea is that with a large number of tagged service-resources, this cache will speed up performance as well as put
	 * a cap on the upper bound because it is expected	that the cardinality of set of all possible keysets for all resource-def
	 * combinations in a service-def will be much smaller than the number of service-resources.
	 */

	static private class ResourceHierarchies {
		private final Map<Collection<String>, Boolean> accessHierarchies    = new HashMap<>();
		private final Map<Collection<String>, Boolean> dataMaskHierarchies  = new HashMap<>();
		private final Map<Collection<String>, Boolean> rowFilterHierarchies = new HashMap<>();

		public Boolean isValidHierarchy(int policyType, Collection<String> resourceKeys) {
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

		public void addHierarchy(int policyType, Collection<String> resourceKeys, Boolean isValid) {
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
					LOG.error("unknown policy-type " + policyType);
					break;
			}
		}
	}

	public void setServiceTags(final ServiceTags serviceTags) {

		if (serviceTags == null || CollectionUtils.isEmpty(serviceTags.getServiceResources())) {
			LOG.info("ServiceTags is null or there are no tagged resources for service " + serviceName);
			enrichedServiceTags = null;
		} else {

			List<RangerServiceResourceMatcher> resourceMatchers = new ArrayList<RangerServiceResourceMatcher>();

			RangerServiceDefHelper serviceDefHelper = new RangerServiceDefHelper(serviceDef, false);

			List<RangerServiceResource> serviceResources = serviceTags.getServiceResources();

			ResourceHierarchies hierarchies = new ResourceHierarchies();

			for (RangerServiceResource serviceResource : serviceResources) {
				final Collection<String> resourceKeys = serviceResource.getResourceElements().keySet();

				for (int policyType : allPolicyTypes) {
					Boolean isValidHierarchy = hierarchies.isValidHierarchy(policyType, resourceKeys);

					if (isValidHierarchy == null) { // hierarchy not yet validated
						isValidHierarchy = Boolean.FALSE;

						for (List<RangerServiceDef.RangerResourceDef> hierarchy : serviceDefHelper.getResourceHierarchies(policyType)) {
							if (serviceDefHelper.hierarchyHasAllResources(hierarchy, resourceKeys)) {
								isValidHierarchy = Boolean.TRUE;

								break;
							}
						}

						hierarchies.addHierarchy(policyType, resourceKeys, isValidHierarchy);
					}

					if (isValidHierarchy) {
						RangerDefaultPolicyResourceMatcher matcher = new RangerDefaultPolicyResourceMatcher();

						matcher.setServiceDef(this.serviceDef);
						matcher.setPolicyResources(serviceResource.getResourceElements(), policyType);

						if (LOG.isDebugEnabled()) {
							LOG.debug("RangerTagEnricher.setServiceTags() - Initializing matcher with (resource=" + serviceResource
									+ ", serviceDef=" + this.serviceDef.getName() + ")");

						}
						matcher.init();

						RangerServiceResourceMatcher serviceResourceMatcher = new RangerServiceResourceMatcher(serviceResource, matcher);
						resourceMatchers.add(serviceResourceMatcher);
					}
				}
			}


			Map<String, RangerResourceTrie<RangerServiceResourceMatcher>> serviceResourceTrie = null;

			if (!disableTrieLookupPrefilter) {
				serviceResourceTrie = new HashMap<String, RangerResourceTrie<RangerServiceResourceMatcher>>();

				for (RangerServiceDef.RangerResourceDef resourceDef : serviceDef.getResources()) {
					serviceResourceTrie.put(resourceDef.getName(), new RangerResourceTrie<RangerServiceResourceMatcher>(resourceDef, resourceMatchers));
				}
			}

			Set<RangerTagForEval> tagsForEmptyResourceAndAnyAccess = new HashSet<RangerTagForEval>();
			for (Map.Entry<Long, RangerTag> entry : serviceTags.getTags().entrySet()) {
				tagsForEmptyResourceAndAnyAccess.add(new RangerTagForEval(entry.getValue(), RangerPolicyResourceMatcher.MatchType.DESCENDANT));
			}

			enrichedServiceTags = new EnrichedServiceTags(serviceTags, resourceMatchers, serviceResourceTrie, tagsForEmptyResourceAndAnyAccess);
		}
	}

	protected Long getServiceTagsVersion() {
		return enrichedServiceTags != null ? enrichedServiceTags.getServiceTags().getTagVersion() : null;
	}

	@Override
	public boolean preCleanup() {
		boolean ret = true;

		if (LOG.isDebugEnabled()) {
			LOG.debug("==> RangerTagEnricher.preCleanup()");
		}

		if (tagRefresher != null) {
			tagRefresher.cleanup();
			tagRefresher = null;
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== RangerTagEnricher.preCleanup() : result=" + ret);
		}
		return ret;
	}

	private Set<RangerTagForEval> findMatchingTags(final RangerAccessRequest request) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> RangerTagEnricher.findMatchingTags(" + request + ")");
		}

		// To minimize chance for race condition between Tag-Refresher thread and access-evaluation thread
		final EnrichedServiceTags enrichedServiceTags = this.enrichedServiceTags;

		Set<RangerTagForEval> ret = null;

		RangerAccessResource resource = request.getResource();

		if ((resource == null || resource.getKeys() == null || resource.getKeys().size() == 0) && request.isAccessTypeAny()) {
			ret = enrichedServiceTags.getTagsForEmptyResourceAndAnyAccess();
		} else {

			final List<RangerServiceResourceMatcher> serviceResourceMatchers = getEvaluators(resource, enrichedServiceTags);

			if (CollectionUtils.isNotEmpty(serviceResourceMatchers)) {

				for (RangerServiceResourceMatcher resourceMatcher : serviceResourceMatchers) {

					final RangerPolicyResourceMatcher.MatchType matchType = resourceMatcher.getMatchType(resource, request.getContext());

					final boolean isMatched;

					if (request.isAccessTypeAny()) {
						isMatched = matchType != RangerPolicyResourceMatcher.MatchType.NONE;
					} else if (request.getResourceMatchingScope() == RangerAccessRequest.ResourceMatchingScope.SELF_OR_DESCENDANTS) {
						isMatched = matchType != RangerPolicyResourceMatcher.MatchType.NONE;
					} else {
						isMatched = matchType == RangerPolicyResourceMatcher.MatchType.SELF || matchType == RangerPolicyResourceMatcher.MatchType.ANCESTOR;
					}

					if (isMatched) {
						if (ret == null) {
							ret = new HashSet<RangerTagForEval>();
						}
						ret.addAll(getTagsForServiceResource(enrichedServiceTags.getServiceTags(), resourceMatcher.getServiceResource(), matchType));
					}
				}
			}
		}

		if (LOG.isDebugEnabled()) {
			if (CollectionUtils.isEmpty(ret)) {
				LOG.debug("RangerTagEnricher.findMatchingTags(" + resource + ") - No tags Found ");
			} else {
				LOG.debug("RangerTagEnricher.findMatchingTags(" + resource + ") - " + ret.size() + " tags Found ");
			}
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== RangerTagEnricher.findMatchingTags(" + request + ")");
		}

		return ret;
	}

	private List<RangerServiceResourceMatcher> getEvaluators(RangerAccessResource resource, EnrichedServiceTags enrichedServiceTags) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerTagEnricher.getEvaluators(" + (resource != null ? resource.getAsString() : null) + ")");
		}

		List<RangerServiceResourceMatcher> ret = null;

		final Map<String, RangerResourceTrie<RangerServiceResourceMatcher>> serviceResourceTrie = enrichedServiceTags.getServiceResourceTrie();

		if (resource == null || resource.getKeys() == null || resource.getKeys().isEmpty() || serviceResourceTrie == null) {
			ret = enrichedServiceTags.getServiceResourceMatchers();
		} else {
			RangerPerfTracer perf = null;

			if(RangerPerfTracer.isPerfTraceEnabled(PERF_TRIE_OP_LOG)) {
				perf = RangerPerfTracer.getPerfTracer(PERF_TRIE_OP_LOG, "RangerTagEnricher.getEvaluators(resource=" + resource.getAsString() + ")");
			}

			Set<String> resourceKeys = resource.getKeys();
			List<List<RangerServiceResourceMatcher>> serviceResourceMatchersList = null;
			List<RangerServiceResourceMatcher> smallestList = null;

			if (CollectionUtils.isNotEmpty(resourceKeys)) {

				for (String resourceName : resourceKeys) {
					RangerResourceTrie<RangerServiceResourceMatcher> trie = serviceResourceTrie.get(resourceName);

					if (trie == null) { // if no trie exists for this resource level, ignore and continue to next level
						continue;
					}

					List<RangerServiceResourceMatcher> serviceResourceMatchers = trie.getEvaluatorsForResource(resource.getValue(resourceName));

					if (CollectionUtils.isEmpty(serviceResourceMatchers)) { // no policies for this resource, bail out
						serviceResourceMatchersList = null;
						smallestList = null;
						break;
					}

					if (smallestList == null) {
						smallestList = serviceResourceMatchers;
					} else {
						if (serviceResourceMatchersList == null) {
							serviceResourceMatchersList = new ArrayList<>();
							serviceResourceMatchersList.add(smallestList);
						}
						serviceResourceMatchersList.add(serviceResourceMatchers);

						if (smallestList.size() > serviceResourceMatchers.size()) {
							smallestList = serviceResourceMatchers;
						}
					}
				}
				if (serviceResourceMatchersList != null) {
					ret = new ArrayList<>(smallestList);
					for (List<RangerServiceResourceMatcher> serviceResourceMatchers : serviceResourceMatchersList) {
						if (serviceResourceMatchers != smallestList) {
							// remove policies from ret that are not in serviceResourceMatchers
							ret.retainAll(serviceResourceMatchers);
							if (CollectionUtils.isEmpty(ret)) { // if no policy exists, bail out and return empty list
								ret = null;
								break;
							}
						}
					}
				} else {
					ret = smallestList;
				}
			}
			RangerPerfTracer.logAlways(perf);
		}

		if(ret == null) {
			ret = Collections.emptyList();
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerTagEnricher.getEvaluators(" + (resource != null ? resource.getAsString() : null) + "): evaluatorCount=" + ret.size());
		}

		return ret;
	}

	static private Set<RangerTagForEval> getTagsForServiceResource(final ServiceTags serviceTags, final RangerServiceResource serviceResource, final RangerPolicyResourceMatcher.MatchType matchType) {

		Set<RangerTagForEval> ret = new HashSet<RangerTagForEval>();

		final Long resourceId = serviceResource.getId();

		final Map<Long, List<Long>> resourceToTagIds = serviceTags.getResourceToTagIds();
		final Map<Long, RangerTag> tags = serviceTags.getTags();

		if (resourceId != null && MapUtils.isNotEmpty(resourceToTagIds) && MapUtils.isNotEmpty(tags)) {

			List<Long> tagIds = resourceToTagIds.get(resourceId);

			if (CollectionUtils.isNotEmpty(tagIds)) {

				for (Long tagId : tagIds) {

					RangerTag tag = tags.get(tagId);

					if (tag != null) {
						ret.add(new RangerTagForEval(tag, matchType));
					}
				}
			}
		}

		return ret;
	}

	static private final class EnrichedServiceTags {
		final private ServiceTags                        serviceTags;
		final private List<RangerServiceResourceMatcher> serviceResourceMatchers;
		final private Map<String, RangerResourceTrie<RangerServiceResourceMatcher>>    serviceResourceTrie;
		final private Set<RangerTagForEval>              tagsForEmptyResourceAndAnyAccess; // Used only when accessed resource is empty and access type is 'any'

		EnrichedServiceTags(ServiceTags serviceTags, List<RangerServiceResourceMatcher> serviceResourceMatchers,
							Map<String, RangerResourceTrie<RangerServiceResourceMatcher>> serviceResourceTrie, Set<RangerTagForEval> tagsForEmptyResourceAndAnyAccess) {
			this.serviceTags             = serviceTags;
			this.serviceResourceMatchers = serviceResourceMatchers;
			this.serviceResourceTrie     = serviceResourceTrie;
			this.tagsForEmptyResourceAndAnyAccess          = tagsForEmptyResourceAndAnyAccess;
		}
		ServiceTags getServiceTags() {return serviceTags;}
		List<RangerServiceResourceMatcher> getServiceResourceMatchers() { return serviceResourceMatchers;}
		Map<String, RangerResourceTrie<RangerServiceResourceMatcher>> getServiceResourceTrie() { return serviceResourceTrie;}
		Set<RangerTagForEval> getTagsForEmptyResourceAndAnyAccess() { return tagsForEmptyResourceAndAnyAccess;}
	}

	static class RangerTagRefresher extends Thread {
		private static final Log LOG = LogFactory.getLog(RangerTagRefresher.class);

		private final RangerTagRetriever tagRetriever;
		private final RangerTagEnricher tagEnricher;
		private long lastKnownVersion = -1L;
		private long lastActivationTimeInMillis = 0L;

		private final long pollingIntervalMs;
		private final String cacheFile;
		private boolean hasProvidedTagsToReceiver = false;
		private Gson gson;


		final long getPollingIntervalMs() {
			return pollingIntervalMs;
		}

		RangerTagRefresher(RangerTagRetriever tagRetriever, RangerTagEnricher tagEnricher, long lastKnownVersion, String cacheFile, long pollingIntervalMs) {
			this.tagRetriever = tagRetriever;
			this.tagEnricher = tagEnricher;
			this.lastKnownVersion = lastKnownVersion;
			this.cacheFile = cacheFile;
			this.pollingIntervalMs = pollingIntervalMs;
			try {
				gson = new GsonBuilder().setDateFormat("yyyyMMdd-HH:mm:ss.SSS-Z").setPrettyPrinting().create();
			} catch(Throwable excp) {
				LOG.fatal("failed to create GsonBuilder object", excp);
			}
		}

		public long getLastActivationTimeInMillis() {
			return lastActivationTimeInMillis;
		}

		public void setLastActivationTimeInMillis(long lastActivationTimeInMillis) {
			this.lastActivationTimeInMillis = lastActivationTimeInMillis;
		}

		@Override
		public void run() {

			if (LOG.isDebugEnabled()) {
				LOG.debug("==> RangerTagRefresher(pollingIntervalMs=" + pollingIntervalMs + ").run()");
			}

			while (true) {

				try {

					// Sleep first and then fetch tags
					if (pollingIntervalMs > 0) {
						Thread.sleep(pollingIntervalMs);
					} else {
						break;
					}
					RangerPerfTracer perf = null;

					if(RangerPerfTracer.isPerfTraceEnabled(PERF_CONTEXTENRICHER_INIT_LOG)) {
						perf = RangerPerfTracer.getPerfTracer(PERF_CONTEXTENRICHER_INIT_LOG, "RangerTagRefresher.populateTags(serviceName=" + tagRetriever.getServiceName() + ",lastKnownVersion=" + lastKnownVersion + ")");
					}
					populateTags();

					RangerPerfTracer.log(perf);

				} catch (InterruptedException excp) {
					LOG.debug("RangerTagRefresher(pollingIntervalMs=" + pollingIntervalMs + ").run() : interrupted! Exiting thread", excp);
					break;
				}
			}

			if (LOG.isDebugEnabled()) {
				LOG.debug("<== RangerTagRefresher().run()");
			}
		}

		private void populateTags() throws InterruptedException {

			if (tagEnricher != null) {
				ServiceTags serviceTags = null;

				try {
					serviceTags = tagRetriever.retrieveTags(lastKnownVersion, lastActivationTimeInMillis);

					if (serviceTags == null) {
						if (!hasProvidedTagsToReceiver) {
							serviceTags = loadFromCache();
						}
					} else {
						saveToCache(serviceTags);
					}

					if (serviceTags != null) {
						tagEnricher.setServiceTags(serviceTags);
						LOG.info("RangerTagRefresher.populateTags() - Updated tags-cache to new version of tags, lastKnownVersion=" + lastKnownVersion + "; newVersion="
								+ (serviceTags.getTagVersion() == null ? -1L : serviceTags.getTagVersion()));
						hasProvidedTagsToReceiver = true;
						lastKnownVersion = serviceTags.getTagVersion() == null ? -1L : serviceTags.getTagVersion();
						setLastActivationTimeInMillis(System.currentTimeMillis());
					} else {
						if (LOG.isDebugEnabled()) {
							LOG.debug("RangerTagRefresher.populateTags() - No need to update tags-cache. lastKnownVersion=" + lastKnownVersion);
						}
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
					LOG.error("Encountered unexpected exception. Ignoring", e);
				}

			} else {
				LOG.error("RangerTagRefresher.populateTags() - no tag receiver to update tag-cache");
			}
		}

		void cleanup() {
			if (LOG.isDebugEnabled()) {
				LOG.debug("==> RangerTagRefresher.cleanup()");
			}

			stopRefresher();

			if (LOG.isDebugEnabled()) {
				LOG.debug("<== RangerTagRefresher.cleanup()");
			}
		}

		final void startRefresher() {
			try {
				super.start();
			} catch (Exception excp) {
				LOG.error("RangerTagRefresher.startRetriever() - failed to start, exception=" + excp);
			}
		}

		private void stopRefresher() {

			if (super.isAlive()) {
				super.interrupt();

				try {
					super.join();
				} catch (InterruptedException excp) {
					LOG.error("RangerTagRefresher(): error while waiting for thread to exit", excp);
				}
			}
		}


		final ServiceTags loadFromCache() {
			ServiceTags serviceTags = null;

			if (LOG.isDebugEnabled()) {
				LOG.debug("==> RangerTagRetriever(serviceName=" + tagEnricher.getServiceName() + ").loadFromCache()");
			}

			File cacheFile = StringUtils.isEmpty(this.cacheFile) ? null : new File(this.cacheFile);

			if (cacheFile != null && cacheFile.isFile() && cacheFile.canRead()) {
				Reader reader = null;

				try {
					reader = new FileReader(cacheFile);

					serviceTags = gson.fromJson(reader, ServiceTags.class);

					if (serviceTags != null) {
						if (!StringUtils.equals(tagEnricher.getServiceName(), serviceTags.getServiceName())) {
							LOG.warn("ignoring unexpected serviceName '" + serviceTags.getServiceName() + "' in cache file '" + cacheFile.getAbsolutePath() + "'");

							serviceTags.setServiceName(tagEnricher.getServiceName());
						}
					}
				} catch (Exception excp) {
					LOG.error("failed to load service-tags from cache file " + cacheFile.getAbsolutePath(), excp);
				} finally {
					if (reader != null) {
						try {
							reader.close();
						} catch (Exception excp) {
							LOG.error("error while closing opened cache file " + cacheFile.getAbsolutePath(), excp);
						}
					}
				}
			} else {
				LOG.warn("cache file does not exist or not readable '" + (cacheFile == null ? null : cacheFile.getAbsolutePath()) + "'");
			}

			if (LOG.isDebugEnabled()) {
				LOG.debug("<== RangerTagRetriever(serviceName=" + tagEnricher.getServiceName() + ").loadFromCache()");
			}

			return serviceTags;
		}

		final void saveToCache(ServiceTags serviceTags) {
			if (LOG.isDebugEnabled()) {
				LOG.debug("==> RangerTagRetriever(serviceName=" + tagEnricher.getServiceName() + ").saveToCache()");
			}

			if (serviceTags != null) {
				File cacheFile = StringUtils.isEmpty(this.cacheFile) ? null : new File(this.cacheFile);

				if (cacheFile != null) {
					Writer writer = null;

					try {
						writer = new FileWriter(cacheFile);

						gson.toJson(serviceTags, writer);
					} catch (Exception excp) {
						LOG.error("failed to save service-tags to cache file '" + cacheFile.getAbsolutePath() + "'", excp);
					} finally {
						if (writer != null) {
							try {
								writer.close();
							} catch (Exception excp) {
								LOG.error("error while closing opened cache file '" + cacheFile.getAbsolutePath() + "'", excp);
							}
						}
					}
				}
			} else {
				LOG.info("service-tags is null. Nothing to save in cache");
			}

			if (LOG.isDebugEnabled()) {
				LOG.debug("<== RangerTagRetriever(serviceName=" + tagEnricher.getServiceName() + ").saveToCache()");
			}
		}

		final void disableCache() {
			if (LOG.isDebugEnabled()) {
				LOG.debug("==> RangerTagRetriever.disableCache(serviceName=" + tagEnricher.getServiceName() + ")");
			}

			File cacheFile = StringUtils.isEmpty(this.cacheFile) ? null : new File(this.cacheFile);
			if (cacheFile != null && cacheFile.isFile() && cacheFile.canRead()) {
				LOG.warn("Cleaning up local tags cache");
				String renamedCacheFile = cacheFile.getAbsolutePath() + "_" + System.currentTimeMillis();
				if (!cacheFile.renameTo(new File(renamedCacheFile))) {
					LOG.error("Failed to move " + cacheFile.getAbsolutePath() + " to " + renamedCacheFile);
				} else {
					LOG.warn("moved " + cacheFile.getAbsolutePath() + " to " + renamedCacheFile);
				}
			} else {
				if (LOG.isDebugEnabled()) {
					LOG.debug("No local TAGS cache found. No need to disable it!");
				}
			}

			if (LOG.isDebugEnabled()) {
				LOG.debug("<== RangerTagRetriever.disableCache(serviceName=" + tagEnricher.getServiceName() + ")");
			}
		}
	}
}
