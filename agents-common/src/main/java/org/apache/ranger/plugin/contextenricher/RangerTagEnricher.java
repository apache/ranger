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
import org.apache.ranger.plugin.policyengine.RangerAccessResourceImpl;
import org.apache.ranger.plugin.policyresourcematcher.RangerDefaultPolicyResourceMatcher;
import org.apache.ranger.plugin.policyresourcematcher.RangerPolicyResourceMatcher;
import org.apache.ranger.plugin.util.DownloadTrigger;
import org.apache.ranger.plugin.util.DownloaderTask;
import org.apache.ranger.plugin.service.RangerAuthContext;
import org.apache.ranger.plugin.service.RangerBasePlugin;
import org.apache.ranger.plugin.util.RangerAccessRequestUtil;
import org.apache.ranger.plugin.util.RangerPerfTracer;
import org.apache.ranger.plugin.util.RangerResourceTrie;
import org.apache.ranger.plugin.util.RangerServiceNotFoundException;
import org.apache.ranger.plugin.util.RangerServiceTagsDeltaUtil;
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
import java.util.Timer;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class RangerTagEnricher extends RangerAbstractContextEnricher {
	private static final Log LOG = LogFactory.getLog(RangerTagEnricher.class);

	private static final Log PERF_CONTEXTENRICHER_INIT_LOG = RangerPerfTracer.getPerfLogger("contextenricher.init");
	private static final Log PERF_TRIE_OP_LOG              = RangerPerfTracer.getPerfLogger("resourcetrie.retrieval");
	private static final Log PERF_SET_SERVICETAGS_LOG      = RangerPerfTracer.getPerfLogger("tagenricher.setservicetags");


	private static final String TAG_REFRESHER_POLLINGINTERVAL_OPTION = "tagRefresherPollingInterval";
	private static final String TAG_RETRIEVER_CLASSNAME_OPTION       = "tagRetrieverClassName";
	private static final String TAG_DISABLE_TRIE_PREFILTER_OPTION    = "disableTrieLookupPrefilter";

	private RangerTagRefresher                 tagRefresher;
	private RangerTagRetriever                 tagRetriever;
	private boolean                            disableTrieLookupPrefilter;
	private EnrichedServiceTags                enrichedServiceTags;
	private boolean                            disableCacheIfServiceNotFound = true;

	private final BlockingQueue<DownloadTrigger> tagDownloadQueue = new LinkedBlockingQueue<>();
	private Timer                              tagDownloadTimer;

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

				tagRefresher = new RangerTagRefresher(tagRetriever, this, -1L, tagDownloadQueue, cacheFile);

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
					if (LOG.isDebugEnabled()) {
						LOG.debug("Scheduled tagDownloadRefresher to download tags every " + pollingIntervalMs + " milliseconds");
					}
				} catch (IllegalStateException exception) {
					LOG.error("Error scheduling tagDownloadTimer:", exception);
					LOG.error("*** Tags will NOT be downloaded every " + pollingIntervalMs + " milliseconds ***");
					tagDownloadTimer = null;
				}
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

		enrich(request, null);

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== RangerTagEnricher.enrich(" + request + ")");
		}
	}

	@Override
	public void enrich(RangerAccessRequest request, Object dataStore) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> RangerTagEnricher.enrich(" + request + ") with dataStore:[" + dataStore + "]");
		}
		final EnrichedServiceTags enrichedServiceTags;

		if (dataStore instanceof EnrichedServiceTags) {
			enrichedServiceTags = (EnrichedServiceTags) dataStore;
		} else {
			enrichedServiceTags = this.enrichedServiceTags;

			if (dataStore != null) {
				LOG.warn("Incorrect type of dataStore :[" + dataStore.getClass().getName() + "], falling back to original enrich");
			}
		}

		final Set<RangerTagForEval> matchedTags = enrichedServiceTags == null ? null : findMatchingTags(request, enrichedServiceTags);

		RangerAccessRequestUtil.setRequestTagsInContext(request.getContext(), matchedTags);

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== RangerTagEnricher.enrich(" + request + ") with dataStore:[" + dataStore + "]): tags count=" + (matchedTags == null ? 0 : matchedTags.size()));
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
					LOG.error("unknown policy-type " + policyType);
					break;
			}
		}
	}

	public void setServiceTags(final ServiceTags serviceTags) {
		boolean rebuildOnlyIndex = false;
		setServiceTags(serviceTags, rebuildOnlyIndex);
	}

	protected void setServiceTags(final ServiceTags serviceTags, final boolean rebuildOnlyIndex) {

		if (LOG.isDebugEnabled()) {
			LOG.debug("==> RangerTagEnricher.setServiceTags(serviceTags=" + serviceTags + ", rebuildOnlyIndex=" + rebuildOnlyIndex + ")");
		}

		if (serviceTags == null) {
			LOG.info("ServiceTags is null for service " + serviceName);
			enrichedServiceTags = null;
		} else  {
			RangerServiceDefHelper serviceDefHelper = new RangerServiceDefHelper(serviceDef, false);
			ResourceHierarchies    hierarchies      = new ResourceHierarchies();

			RangerPerfTracer perf = null;

			if(RangerPerfTracer.isPerfTraceEnabled(PERF_SET_SERVICETAGS_LOG)) {
				perf = RangerPerfTracer.getPerfTracer(PERF_SET_SERVICETAGS_LOG, "RangerTagEnricher.setServiceTags(newTagVersion=" + serviceTags.getTagVersion() + ",isDelta=" + serviceTags.getIsDelta() + ")");
			}

			if (!serviceTags.getIsDelta()) {
				if (LOG.isDebugEnabled()) {
					LOG.debug("Received all service-tags");
				}
				if (CollectionUtils.isEmpty(serviceTags.getServiceResources())) {
					LOG.info("There are no tagged resources for service " + serviceName);
					enrichedServiceTags = null;
				} else {

					List<RangerServiceResourceMatcher> resourceMatchers = new ArrayList<>();
					List<RangerServiceResource> serviceResources = serviceTags.getServiceResources();

					for (RangerServiceResource serviceResource : serviceResources) {
						RangerServiceResourceMatcher serviceResourceMatcher = createRangerServiceResourceMatcher(serviceResource, serviceDefHelper, hierarchies);
						if (serviceResourceMatcher != null) {
							resourceMatchers.add(serviceResourceMatcher);
						} else {
							LOG.error("Could not create service-resource-matcher for service-resource:[" + serviceResource + "]");
						}
					}

					Map<String, RangerResourceTrie<RangerServiceResourceMatcher>> serviceResourceTrie = null;

					if (!disableTrieLookupPrefilter) {
						serviceResourceTrie = new HashMap<>();

						for (RangerServiceDef.RangerResourceDef resourceDef : serviceDef.getResources()) {
							serviceResourceTrie.put(resourceDef.getName(), new RangerResourceTrie<>(resourceDef, resourceMatchers));
						}
					}

					enrichedServiceTags = new EnrichedServiceTags(serviceTags, resourceMatchers, serviceResourceTrie);
				}

			} else {
				if (LOG.isDebugEnabled()) {
					LOG.debug("Received service-tag deltas:" + serviceTags);
				}

				ServiceTags delta          = serviceTags;
				ServiceTags oldServiceTags = enrichedServiceTags != null ? enrichedServiceTags.getServiceTags() : new ServiceTags();

				ServiceTags                                                   newServiceTags         = rebuildOnlyIndex ? oldServiceTags : RangerServiceTagsDeltaUtil.applyDelta(oldServiceTags, delta);
				List<RangerServiceResourceMatcher>                            newResourceMatchers    = enrichedServiceTags != null ? enrichedServiceTags.getServiceResourceMatchers() : new ArrayList<>();
				Map<String, RangerResourceTrie<RangerServiceResourceMatcher>> newServiceResourceTrie = enrichedServiceTags != null ? enrichedServiceTags.getServiceResourceTrie() : new HashMap<>();

				if (serviceTags.getTagsChangeExtent() == ServiceTags.TagsChangeExtent.NONE) {
					if (LOG.isDebugEnabled()) {
						LOG.debug("No change to service-tags other than version change");
					}
				} else {
					if (serviceTags.getTagsChangeExtent() != ServiceTags.TagsChangeExtent.TAGS) {
						if (LOG.isDebugEnabled()) {
							LOG.debug("Delta contains changes other than tag attribute changes, [" + serviceTags.getTagsChangeExtent() + "]");
						}

						newServiceResourceTrie = new HashMap<>();
						newResourceMatchers    = new ArrayList<>();

						if (enrichedServiceTags != null) {
							newResourceMatchers.addAll(enrichedServiceTags.getServiceResourceMatchers());
						}

						Map<String, RangerResourceTrie<RangerServiceResourceMatcher>> serviceResourceTrie = enrichedServiceTags != null ? enrichedServiceTags.getServiceResourceTrie() : new HashMap<>();

						for (Map.Entry<String, RangerResourceTrie<RangerServiceResourceMatcher>> entry : serviceResourceTrie.entrySet()) {
							RangerResourceTrie<RangerServiceResourceMatcher> resourceTrie = new RangerResourceTrie<>(entry.getValue());
							newServiceResourceTrie.put(entry.getKey(), resourceTrie);
						}

						List<RangerServiceResource> changedServiceResources = delta.getServiceResources();

						for (RangerServiceResource serviceResource : changedServiceResources) {

							if (enrichedServiceTags != null) {

								if (LOG.isDebugEnabled()) {
									LOG.debug("Removing service-resource:[" + serviceResource + "] from trie-map");
								}

								// Remove existing serviceResource from the copy

								RangerAccessResourceImpl accessResource = new RangerAccessResourceImpl();

								for (Map.Entry<String, RangerPolicy.RangerPolicyResource> entry : serviceResource.getResourceElements().entrySet()) {
									accessResource.setValue(entry.getKey(), entry.getValue());
								}
								if (LOG.isDebugEnabled()) {
									LOG.debug("RangerAccessResource:[" + accessResource + "] created to represent service-resource[" + serviceResource + "] to find evaluators from trie-map");
								}

								List<RangerServiceResourceMatcher> oldMatchers = getEvaluators(accessResource, enrichedServiceTags);

								if (LOG.isDebugEnabled()) {
									LOG.debug("Found [" + oldMatchers.size() + "] matchers for service-resource[" + serviceResource + "]");
								}

								for (RangerServiceResourceMatcher matcher : oldMatchers) {

									for (String resourceDefName : serviceResource.getResourceElements().keySet()) {
										RangerResourceTrie<RangerServiceResourceMatcher> trie = newServiceResourceTrie.get(resourceDefName);
										if (trie != null) {
											trie.delete(serviceResource.getResourceElements().get(resourceDefName), matcher);
										} else {
											LOG.error("Cannot find resourceDef with name:[" + resourceDefName + "]. Should NOT happen!!");
											LOG.error("Setting tagVersion to -1 to ensure that in the next download all tags are downloaded");
											newServiceTags.setTagVersion(-1L);
										}
									}
								}

								// Remove old resource matchers
								newResourceMatchers.removeAll(oldMatchers);

								if (LOG.isDebugEnabled()) {
									LOG.debug("Found and removed [" + oldMatchers.size() + "] matchers for service-resource[" + serviceResource + "] from trie-map");
								}
							}

							if (!StringUtils.isEmpty(serviceResource.getResourceSignature())) {

								RangerServiceResourceMatcher resourceMatcher = createRangerServiceResourceMatcher(serviceResource, serviceDefHelper, hierarchies);

								if (resourceMatcher != null) {
									for (String resourceDefName : serviceResource.getResourceElements().keySet()) {

										RangerResourceTrie<RangerServiceResourceMatcher> trie = newServiceResourceTrie.get(resourceDefName);

										if (trie == null) {
											List<RangerServiceDef.RangerResourceDef> resourceDefs = serviceDef.getResources();
											RangerServiceDef.RangerResourceDef found = null;
											for (RangerServiceDef.RangerResourceDef resourceDef : resourceDefs) {
												if (StringUtils.equals(resourceDef.getName(), resourceDefName)) {
													found = resourceDef;
													break;
												}
											}
											if (found != null) {
												List<RangerServiceResourceMatcher> resourceMatchers = new ArrayList<>();
												trie = new RangerResourceTrie<>(found, resourceMatchers);
												newServiceResourceTrie.put(resourceDefName, trie);
											}
										}

										if (trie != null) {
											trie.add(serviceResource.getResourceElements().get(resourceDefName), resourceMatcher);
											if (LOG.isDebugEnabled()) {
												LOG.debug("Added resource-matcher for service-resource:[" + serviceResource + "]");
											}
										} else {
											LOG.error("Could not create resource-matcher for resource: [" + serviceResource + "]. Should NOT happen!!");
											LOG.error("Setting tagVersion to -1 to ensure that in the next download all tags are downloaded");
											newServiceTags.setTagVersion(-1L);
										}
									}
									newResourceMatchers.add(resourceMatcher);
								} else {
									LOG.error("Could not create resource-matcher for resource: [" + serviceResource + "]. Should NOT happen!!");
									LOG.error("Setting tagVersion to -1 to ensure that in the next download all tags are downloaded");
									newServiceTags.setTagVersion(-1L);
								}
							} else {
								if (LOG.isDebugEnabled()) {
									LOG.debug("Service-resource:[id=" + serviceResource.getId() + "] is deleted as its resource-signature is empty. No need to create it!");
								}
							}
						}
					} else {
						if (LOG.isDebugEnabled()) {
							LOG.debug("Delta contains only tag attribute changes");
						}
					}

					enrichedServiceTags = new EnrichedServiceTags(newServiceTags, newResourceMatchers, newServiceResourceTrie);
				}
			}
			RangerPerfTracer.logAlways(perf);
		}

		setEnrichedServiceTagsInPlugin();

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== RangerTagEnricher.setServiceTags(serviceTags=" + serviceTags + ", rebuildOnlyIndex=" + rebuildOnlyIndex + ")");
		}

	}

	protected Long getServiceTagsVersion() {
		return enrichedServiceTags != null ? enrichedServiceTags.getServiceTags().getTagVersion() : -1L;
	}

	protected Long getResourceTrieVersion() {
		return enrichedServiceTags != null ? enrichedServiceTags.getResourceTrieVersion() : -1L;
	}

	@Override
	public boolean preCleanup() {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> RangerTagEnricher.preCleanup()");
		}

		super.preCleanup();

		if (tagDownloadTimer != null) {
			tagDownloadTimer.cancel();
			tagDownloadTimer = null;
		}

		if (tagRefresher != null) {
			tagRefresher.cleanup();
			tagRefresher = null;
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== RangerTagEnricher.preCleanup() : result=" + true);
		}
		return true;
	}

	public void syncTagsWithAdmin(final DownloadTrigger token) throws InterruptedException {
		tagDownloadQueue.put(token);
		token.waitForCompletion();
	}

	public boolean compare(RangerTagEnricher other) {
		boolean ret;

		if (enrichedServiceTags.getServiceResourceTrie() != null && other.enrichedServiceTags.getServiceResourceTrie() != null) {
			ret = enrichedServiceTags.getServiceResourceTrie().size() == other.enrichedServiceTags.getServiceResourceTrie().size();

			if (ret && enrichedServiceTags.getServiceResourceTrie().size() > 0) {
				for (Map.Entry<String, RangerResourceTrie<RangerServiceResourceMatcher>> entry : enrichedServiceTags.getServiceResourceTrie().entrySet()) {
					ret = entry.getValue().compareSubtree(other.enrichedServiceTags.getServiceResourceTrie().get(entry.getKey()));
					if (!ret) {
						break;
					}
				}
			}
		} else {
			ret = enrichedServiceTags.getServiceResourceTrie() == other.enrichedServiceTags.getServiceResourceTrie();
		}

		if (ret) {
			// Compare mappings
			ServiceTags myServiceTags = enrichedServiceTags.getServiceTags();
			ServiceTags otherServiceTags = other.enrichedServiceTags.getServiceTags();

			ret = StringUtils.equals(myServiceTags.getServiceName(), otherServiceTags.getServiceName()) &&
					//myServiceTags.getTagVersion().equals(otherServiceTags.getTagVersion()) &&
					myServiceTags.getTags().size() == otherServiceTags.getTags().size() &&
					myServiceTags.getServiceResources().size() == otherServiceTags.getServiceResources().size() &&
					myServiceTags.getResourceToTagIds().size() == otherServiceTags.getResourceToTagIds().size();
			if (ret) {
				for (RangerServiceResource serviceResource : myServiceTags.getServiceResources()) {
					Long serviceResourceId = serviceResource.getId();

					List<Long> myTagsForResource = myServiceTags.getResourceToTagIds().get(serviceResourceId);
					List<Long> otherTagsForResource = otherServiceTags.getResourceToTagIds().get(serviceResourceId);

					ret = CollectionUtils.size(myTagsForResource) == CollectionUtils.size(otherTagsForResource);

					if (ret && CollectionUtils.size(myTagsForResource) > 0) {
						ret = myTagsForResource.size() == CollectionUtils.intersection(myTagsForResource, otherTagsForResource).size();
					}
				}
			}
		}

		return ret;
	}

	private Set<RangerTagForEval> findMatchingTags(final RangerAccessRequest request, EnrichedServiceTags dataStore) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> RangerTagEnricher.findMatchingTags(" + request + ")");
		}

		// To minimize chance for race condition between Tag-Refresher thread and access-evaluation thread
		final EnrichedServiceTags enrichedServiceTags = dataStore != null ? dataStore : this.enrichedServiceTags;

		Set<RangerTagForEval> ret = null;

		RangerAccessResource resource = request.getResource();

		if ((resource == null || resource.getKeys() == null || resource.getKeys().isEmpty()) && request.isAccessTypeAny()) {
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
							ret = new HashSet<>();
						}
						ret.addAll(getTagsForServiceResource(enrichedServiceTags.getServiceTags(), resourceMatcher.getServiceResource(), matchType));
					}

				}
			}
		}

		if (CollectionUtils.isEmpty(ret)) {
			if (LOG.isDebugEnabled()) {
				LOG.debug("RangerTagEnricher.findMatchingTags(" + resource + ") - No tags Found ");
			}
		} else {
			if (LOG.isDebugEnabled()) {
				LOG.debug("RangerTagEnricher.findMatchingTags(" + resource + ") - " + ret.size() + " tags Found ");
			}
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== RangerTagEnricher.findMatchingTags(" + request + ")");
		}

		return ret;
	}

	private RangerServiceResourceMatcher createRangerServiceResourceMatcher(RangerServiceResource serviceResource, RangerServiceDefHelper serviceDefHelper, ResourceHierarchies hierarchies) {

		if (LOG.isDebugEnabled()) {
			LOG.debug("==> createRangerServiceResourceMatcher(serviceResource=" + serviceResource + ")");
		}

		RangerServiceResourceMatcher ret = null;

		final Collection<String> resourceKeys = serviceResource.getResourceElements().keySet();

		for (int policyType : RangerPolicy.POLICY_TYPES) {
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
				matcher.setServiceDefHelper(serviceDefHelper);
				matcher.init();

				ret = new RangerServiceResourceMatcher(serviceResource, matcher);
				break;
			}
		}
		if (LOG.isDebugEnabled()) {
			LOG.debug("<== createRangerServiceResourceMatcher(serviceResource=" + serviceResource + ") : [" + ret + "]");
		}
		return ret;

	}

	private void setEnrichedServiceTagsInPlugin() {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> setEnrichedServiceTagsInPlugin()");
		}

		RangerAuthContext authContext = getAuthContext();
		if (authContext != null) {
			authContext.addOrReplaceRequestContextEnricher(this, enrichedServiceTags);

			Map<String, RangerBasePlugin> servicePluginMap = RangerBasePlugin.getServicePluginMap();
			RangerBasePlugin plugin = servicePluginMap != null ? servicePluginMap.get(getServiceName()) : null;
			if (plugin != null) {
				plugin.contextChanged();
			}
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== setEnrichedServiceTagsInPlugin()");
		}
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

					if (CollectionUtils.isEmpty(serviceResourceMatchers)) { // no tags for this resource, bail out
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
							// remove other serviceResourceMatchers from ret that are not in serviceResourceMatchers
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

	private static Set<RangerTagForEval> getTagsForServiceResource(final ServiceTags serviceTags, final RangerServiceResource serviceResource, final RangerPolicyResourceMatcher.MatchType matchType) {
		Set<RangerTagForEval> ret = new HashSet<>();

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
		final private ServiceTags                                                      serviceTags;
		final private List<RangerServiceResourceMatcher>                               serviceResourceMatchers;
		final private Map<String, RangerResourceTrie<RangerServiceResourceMatcher>>    serviceResourceTrie;
		final private Set<RangerTagForEval>                                            tagsForEmptyResourceAndAnyAccess; // Used only when accessed resource is empty and access type is 'any'
		final private Long                                                             resourceTrieVersion;

		EnrichedServiceTags(ServiceTags serviceTags, List<RangerServiceResourceMatcher> serviceResourceMatchers, Map<String, RangerResourceTrie<RangerServiceResourceMatcher>> serviceResourceTrie) {
			this.serviceTags                      = serviceTags;
			this.serviceResourceMatchers          = serviceResourceMatchers;
			this.serviceResourceTrie              = serviceResourceTrie;
			this.tagsForEmptyResourceAndAnyAccess = createTagsForEmptyResourceAndAnyAccess();
			this.resourceTrieVersion              = serviceTags.getTagVersion();
		}
		ServiceTags                                                   getServiceTags() {return serviceTags;}
		List<RangerServiceResourceMatcher>                            getServiceResourceMatchers() { return serviceResourceMatchers;}
		Map<String, RangerResourceTrie<RangerServiceResourceMatcher>> getServiceResourceTrie() { return serviceResourceTrie;}
		Long                                                          getResourceTrieVersion() { return resourceTrieVersion;}
		Set<RangerTagForEval>                                         getTagsForEmptyResourceAndAnyAccess() { return tagsForEmptyResourceAndAnyAccess;}

		private Set<RangerTagForEval> createTagsForEmptyResourceAndAnyAccess() {
			Set<RangerTagForEval> tagsForEmptyResourceAndAnyAccess = new HashSet<>();
			for (Map.Entry<Long, RangerTag> entry : serviceTags.getTags().entrySet()) {
				tagsForEmptyResourceAndAnyAccess.add(new RangerTagForEval(entry.getValue(), RangerPolicyResourceMatcher.MatchType.DESCENDANT));
			}
			return tagsForEmptyResourceAndAnyAccess;
		}
	}

	static class RangerTagRefresher extends Thread {
		private static final Log LOG = LogFactory.getLog(RangerTagRefresher.class);

		private final RangerTagRetriever tagRetriever;
		private final RangerTagEnricher tagEnricher;
		private long lastKnownVersion;
		private final BlockingQueue<DownloadTrigger> tagDownloadQueue;
		private long lastActivationTimeInMillis;

		private final String cacheFile;
		private boolean hasProvidedTagsToReceiver;
		private Gson gson;

		RangerTagRefresher(RangerTagRetriever tagRetriever, RangerTagEnricher tagEnricher, long lastKnownVersion, BlockingQueue<DownloadTrigger> tagDownloadQueue, String cacheFile) {
			this.tagRetriever = tagRetriever;
			this.tagEnricher = tagEnricher;
			this.lastKnownVersion = lastKnownVersion;
			this.tagDownloadQueue = tagDownloadQueue;
			this.cacheFile = cacheFile;
			try {
				gson = new GsonBuilder().setDateFormat("yyyyMMdd-HH:mm:ss.SSS-Z").create();
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
				LOG.debug("==> RangerTagRefresher().run()");
			}

			while (true) {

				try {
					RangerPerfTracer perf = null;

					if(RangerPerfTracer.isPerfTraceEnabled(PERF_CONTEXTENRICHER_INIT_LOG)) {
						perf = RangerPerfTracer.getPerfTracer(PERF_CONTEXTENRICHER_INIT_LOG, "RangerTagRefresher.populateTags(serviceName=" + tagRetriever.getServiceName() + ",lastKnownVersion=" + lastKnownVersion + ")");
					}
					DownloadTrigger trigger = tagDownloadQueue.take();
					populateTags();
					trigger.signalCompletion();

					RangerPerfTracer.log(perf);

				} catch (InterruptedException excp) {
					LOG.debug("RangerTagRefresher().run() : interrupted! Exiting thread", excp);
					break;
				}
			}

			if (LOG.isDebugEnabled()) {
				LOG.debug("<== RangerTagRefresher().run()");
			}
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
						if (serviceTags.getIsDelta()) {
							saveToCache(tagEnricher.enrichedServiceTags.serviceTags);
						}
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

					if (serviceTags != null && !StringUtils.equals(tagEnricher.getServiceName(), serviceTags.getServiceName())) {
						LOG.warn("ignoring unexpected serviceName '" + serviceTags.getServiceName() + "' in cache file '" + cacheFile.getAbsolutePath() + "'");

						serviceTags.setServiceName(tagEnricher.getServiceName());
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
