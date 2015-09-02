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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ranger.plugin.model.RangerServiceResource;
import org.apache.ranger.plugin.model.RangerTag;
import org.apache.ranger.plugin.policyengine.RangerAccessRequest;
import org.apache.ranger.plugin.policyengine.RangerAccessResource;
import org.apache.ranger.plugin.policyresourcematcher.RangerDefaultPolicyResourceMatcher;
import org.apache.ranger.plugin.util.RangerAccessRequestUtil;
import org.apache.ranger.plugin.util.ServiceTags;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class RangerTagEnricher extends RangerAbstractContextEnricher implements RangerTagReceiver {
	private static final Log LOG = LogFactory.getLog(RangerTagEnricher.class);

	public static final String TAG_REFRESHER_POLLINGINTERVAL_OPTION = "tagRefresherPollingInterval";

	public static final String TAG_RETRIEVER_CLASSNAME_OPTION = "tagRetrieverClassName";

	private RangerTagRefresher tagRefresher = null;

	private RangerTagRetriever tagRetriever = null;

	private long lastKnownVersion = -1L;

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

			cleanup();

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
				tagRetriever.setServiceName(serviceName);
				tagRetriever.setServiceDef(serviceDef);
				tagRetriever.setAppId(appId);
				tagRetriever.setLastKnownVersion(lastKnownVersion);
				tagRetriever.setTagReceiver(this);
				tagRetriever.init(enricherDef.getEnricherOptions());

				try {
					tagRetriever.retrieveTags();
				} catch (Exception exception) {
					// Ignore
				}

				tagRefresher = new RangerTagRefresher(tagRetriever, pollingIntervalMs);

				tagRefresher.startRefresher();
			}
		} else {
			LOG.error("No value specified for " + TAG_RETRIEVER_CLASSNAME_OPTION + " in the RangerTagEnricher options");
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== RangerTagEnricher.init()");
		}
	}

	public void cleanup() {

		if (tagRefresher != null) {
			tagRefresher.cleanup();
			tagRefresher = null;
		}
	}

	@Override
	public void enrich(RangerAccessRequest request) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> RangerTagEnricher.enrich(" + request + ")");
		}

		List<RangerServiceResourceMatcher> serviceResourceMatchersCopy = serviceResourceMatchers;

		List<RangerTag> matchedTags = findMatchingTags(request.getResource(), serviceResourceMatchersCopy);

		if (CollectionUtils.isNotEmpty(matchedTags)) {
			RangerAccessRequestUtil.setRequestTagsInContext(request.getContext(), matchedTags);

			if (LOG.isDebugEnabled()) {
				LOG.debug("RangerTagEnricher.enrich(" + request + ") - " + matchedTags.size() + " tags found by enricher.");
			}
		} else {
			if (LOG.isDebugEnabled()) {
				LOG.debug("RangerTagEnricher.enrich(" + request + ") - no tags found by enricher.");
			}
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== RangerTagEnricher.enrich(" + request + ")");
		}
	}

	@Override
	public void setServiceTags(final ServiceTags serviceTags) {
		this.serviceTags = serviceTags;
		this.lastKnownVersion = serviceTags.getTagVersion();

		List<RangerServiceResourceMatcher> resourceMatchers = new ArrayList<RangerServiceResourceMatcher>();

		List<RangerServiceResource> serviceResources = this.serviceTags.getServiceResources();

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

		serviceResourceMatchers = resourceMatchers;

	}

	private List<RangerTag> findMatchingTags(final RangerAccessResource resource, final List<RangerServiceResourceMatcher> resourceMatchers) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> RangerTagEnricher.findMatchingTags(" + resource + ")");
		}

		List<RangerTag> ret = null;

		if (CollectionUtils.isNotEmpty(resourceMatchers)) {

			for (RangerServiceResourceMatcher resourceMatcher : resourceMatchers) {

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

	static private List<RangerTag> getTagsForServiceResource(ServiceTags serviceTags, RangerServiceResource serviceResource) {

		List<RangerTag> ret = new ArrayList<RangerTag>();

		Long resourceId = serviceResource.getId();

		Map<Long, List<Long>> resourceToTagIds = serviceTags.getResourceToTagIds();
		Map<Long, RangerTag> tags = serviceTags.getTags();

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

		private final long pollingIntervalMs;

		final long getPollingIntervalMs() {
			return pollingIntervalMs;
		}

		RangerTagRefresher(RangerTagRetriever tagRetriever, long pollingIntervalMs) {
			this.tagRetriever = tagRetriever;
			this.pollingIntervalMs = pollingIntervalMs;
		}

		@Override
		public void run() {

			if (LOG.isDebugEnabled()) {
				LOG.debug("==> RangerTagRefresher(pollingIntervalMs=" + pollingIntervalMs + ").run()");
			}

			while (true) {

				try {

					tagRetriever.retrieveTags();

					if (pollingIntervalMs > 0) {
						Thread.sleep(pollingIntervalMs);
					} else {
						break;
					}
				} catch (InterruptedException excp) {
					LOG.info("RangerTagRefresher(pollingIntervalMs=" + pollingIntervalMs + ").run() : interrupted! Exiting thread", excp);
					break;
				}
			}

			if (LOG.isDebugEnabled()) {
				LOG.debug("<== RangerTagRefresher().run()");
			}
		}

		void cleanup() {
			stopRefresher();
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
	}

}
