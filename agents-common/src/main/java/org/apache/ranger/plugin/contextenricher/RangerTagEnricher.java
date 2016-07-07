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
import org.apache.ranger.plugin.model.RangerServiceResource;
import org.apache.ranger.plugin.model.RangerTag;
import org.apache.ranger.plugin.policyengine.RangerAccessRequest;
import org.apache.ranger.plugin.policyengine.RangerAccessResource;
import org.apache.ranger.plugin.policyresourcematcher.RangerDefaultPolicyResourceMatcher;
import org.apache.ranger.plugin.util.RangerAccessRequestUtil;
import org.apache.ranger.plugin.util.RangerPerfTracer;
import org.apache.ranger.plugin.util.ServiceTags;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class RangerTagEnricher extends RangerAbstractContextEnricher {
	private static final Log LOG = LogFactory.getLog(RangerTagEnricher.class);

	private static final Log PERF_CONTEXTENRICHER_INIT_LOG = RangerPerfTracer.getPerfLogger("contextenricher.init");

	public static final String TAG_REFRESHER_POLLINGINTERVAL_OPTION = "tagRefresherPollingInterval";

	public static final String TAG_RETRIEVER_CLASSNAME_OPTION = "tagRetrieverClassName";

	private RangerTagRefresher tagRefresher = null;

	private RangerTagRetriever tagRetriever = null;

	ServiceTags serviceTags = null;

	List<RangerServiceResourceMatcher> serviceResourceMatchers;

	@Override
	public void init() {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> RangerTagEnricher.init()");
		}

		super.init();

		String tagRetrieverClassName = getOption(TAG_RETRIEVER_CLASSNAME_OPTION);

		long pollingIntervalMs = getLongOption(TAG_REFRESHER_POLLINGINTERVAL_OPTION, 60 * 1000);

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
				LOG.error("Class " + tagRetrieverClassName + " could not be instantiated, exception=" + exception);
			} catch (InstantiationException exception) {
				LOG.error("Class " + tagRetrieverClassName + " could not be instantiated, exception=" + exception);
			}

			if (tagRetriever != null) {
				String propertyPrefix    = "ranger.plugin." + serviceDef.getName();
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

		List<RangerTag> matchedTags = findMatchingTags(request.getResource());

		RangerAccessRequestUtil.setRequestTagsInContext(request.getContext(), matchedTags);

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== RangerTagEnricher.enrich(" + request + "): tags count=" + (matchedTags == null ? 0 : matchedTags.size()));
		}
	}

	public void setServiceTags(final ServiceTags serviceTags) {

		List<RangerServiceResourceMatcher> resourceMatchers = new ArrayList<RangerServiceResourceMatcher>();

		List<RangerServiceResource> serviceResources = serviceTags.getServiceResources();

		if (CollectionUtils.isNotEmpty(serviceResources)) {

			for (RangerServiceResource serviceResource : serviceResources) {
				RangerDefaultPolicyResourceMatcher matcher = new RangerDefaultPolicyResourceMatcher();

				matcher.setServiceDef(this.serviceDef);
				matcher.setPolicyResources(serviceResource.getResourceElements());

				if (LOG.isDebugEnabled()) {
					LOG.debug("RangerTagEnricher.setServiceTags() - Initializing matcher with (resource=" + serviceResource
							+ ", serviceDef=" + this.serviceDef.getName() + ")");

				}
				matcher.init();

				RangerServiceResourceMatcher serviceResourceMatcher = new RangerServiceResourceMatcher(serviceResource, matcher);
				resourceMatchers.add(serviceResourceMatcher);
			}

		}

		this.serviceResourceMatchers = resourceMatchers;
		this.serviceTags = serviceTags;
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

	private List<RangerTag> findMatchingTags(final RangerAccessResource resource) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> RangerTagEnricher.findMatchingTags(" + resource + ")");
		}

		List<RangerTag> ret = null;
		final List<RangerServiceResourceMatcher> serviceResourceMatchers = this.serviceResourceMatchers;

		if (CollectionUtils.isNotEmpty(serviceResourceMatchers)) {

			final ServiceTags serviceTags = this.serviceTags;

			for (RangerServiceResourceMatcher resourceMatcher : serviceResourceMatchers) {

				boolean matchResult = resourceMatcher.isMatch(resource);

				if (matchResult) {
					if (ret == null) {
						ret = new ArrayList<RangerTag>();
					}
					// Find tags from serviceResource
					ret.addAll(getTagsForServiceResource(serviceTags, resourceMatcher.getServiceResource()));
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
			LOG.debug("<== RangerTagEnricher.findMatchingTags(" + resource + ")");
		}

		return ret;
	}

	static private List<RangerTag> getTagsForServiceResource(final ServiceTags serviceTags, final RangerServiceResource serviceResource) {

		List<RangerTag> ret = new ArrayList<RangerTag>();

		final Long resourceId = serviceResource.getId();

		final Map<Long, List<Long>> resourceToTagIds = serviceTags.getResourceToTagIds();
		final Map<Long, RangerTag> tags = serviceTags.getTags();

		if (resourceId != null && MapUtils.isNotEmpty(resourceToTagIds) && MapUtils.isNotEmpty(tags)) {

			List<Long> tagIds = resourceToTagIds.get(resourceId);

			if (CollectionUtils.isNotEmpty(tagIds)) {

				for (Long tagId : tagIds) {

					RangerTag tag = tags.get(tagId);

					if (tag != null) {
						ret.add(tag);
					}
				}
			}
		}

		return ret;
	}

	static class RangerTagRefresher extends Thread {
		private static final Log LOG = LogFactory.getLog(RangerTagRefresher.class);

		private final RangerTagRetriever tagRetriever;
		private final RangerTagEnricher tagEnricher;
		private long lastKnownVersion = -1L;

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

				serviceTags = tagRetriever.retrieveTags(lastKnownVersion);

				if (serviceTags == null) {
					if (!hasProvidedTagsToReceiver) {
						serviceTags = loadFromCache();
					}
				} else {
					saveToCache(serviceTags);
				}

				if (serviceTags != null) {
					tagEnricher.setServiceTags(serviceTags);
					lastKnownVersion = serviceTags.getTagVersion() == null ? -1L : serviceTags.getTagVersion();
					LOG.info("RangerTagRefresher.populateTags() - Updated tags-cache to new version of tags, lastKnownVersion=" + lastKnownVersion + "; newVersion=" + serviceTags.getTagVersion());
					hasProvidedTagsToReceiver = true;
				} else {
					if (LOG.isDebugEnabled()) {
						LOG.debug("RangerTagRefresher.populateTags() - No need to update tags-cache. lastKnownVersion=" + lastKnownVersion);
					}
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
	}
}
