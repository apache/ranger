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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ranger.plugin.model.RangerServiceResource;
import org.apache.ranger.plugin.model.RangerTag;
import org.apache.ranger.plugin.policyengine.RangerAccessRequest;
import org.apache.ranger.plugin.policyengine.RangerAccessResource;
import org.apache.ranger.plugin.policyengine.RangerPolicyEngine;
import org.apache.ranger.plugin.policyresourcematcher.RangerDefaultPolicyResourceMatcher;
import org.apache.ranger.plugin.policyresourcematcher.RangerPolicyResourceMatcher;
import org.apache.ranger.plugin.util.ServiceTags;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class RangerTagProvider extends RangerAbstractContextEnricher implements RangerTagReceiver {
	private static final Log LOG = LogFactory.getLog(RangerTagProvider.class);

	public enum TagProviderTypeEnum {
		INVALID_TAG_PROVIDER,
		FILESTORE_BASED_TAG_PROVIDER,
		RANGER_ADMIN_TAG_PROVIDER,
		EXTERNAL_SYSTEM_TAG_PROVIDER
	}

	protected TagProviderTypeEnum tagProviderType = TagProviderTypeEnum.INVALID_TAG_PROVIDER;
	protected RangerTagRefresher tagRefresher;
	ServiceTags serviceTags;
	List<RangerServiceResourceMatcher> serviceResourceMatchers;

	@Override
	public void init() {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> RangerTagProvider.init()");
		}

		super.init();

		Map<String, String> options = enricherDef != null ? enricherDef.getEnricherOptions() : null;

		String tagProviderTypeString = getOption("tagProviderType", "RANGER_ADMIN_TAG_PROVIDER");
		long pollingIntervalMs = getLongOption("pollingInterval", 60 * 1000);

		if (tagProviderTypeString.equals(TagProviderTypeEnum.FILESTORE_BASED_TAG_PROVIDER.toString())) {
			tagRefresher = new RangerTagFileStoreRetriever(serviceName, pollingIntervalMs, this);
			tagProviderType = TagProviderTypeEnum.FILESTORE_BASED_TAG_PROVIDER;
		} else if (tagProviderTypeString.equals(TagProviderTypeEnum.RANGER_ADMIN_TAG_PROVIDER.toString())) {
			tagRefresher = new RangerAdminTagRetriever(serviceName, serviceDef, pollingIntervalMs, this);
			tagProviderType = TagProviderTypeEnum.RANGER_ADMIN_TAG_PROVIDER;
		} else if (tagProviderTypeString.equals(TagProviderTypeEnum.EXTERNAL_SYSTEM_TAG_PROVIDER.toString())) {
			// TODO
			tagProviderType = TagProviderTypeEnum.EXTERNAL_SYSTEM_TAG_PROVIDER;
		} else {
			LOG.error("RangerTagProvider.init() - Invalid Tag Provider.. ");
		}

		// Provide additional options
		if (tagRefresher != null) {
			tagRefresher.init(options);
			tagRefresher.retrieveTags();
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== RangerTagProvider.init() - Tag Provider Type:" + tagProviderType);
		}
	}

	@Override
	public void enrich(RangerAccessRequest request) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> RangerTagProvider.enrich(" + request + ")");
		}

		List<RangerServiceResourceMatcher> serviceResourceMatchersCopy = serviceResourceMatchers;

		List<RangerTag> matchedTags = findMatchingTags(request.getResource(), serviceResourceMatchersCopy);

		if (CollectionUtils.isNotEmpty(matchedTags)) {
			request.getContext().put(RangerPolicyEngine.KEY_CONTEXT_TAGS, matchedTags);
			if (LOG.isDebugEnabled()) {
				LOG.debug("RangerTagProvider.enrich(" + request + ") - " + matchedTags.size() + " tags found by enricher.");
			}
		} else {
			if (LOG.isDebugEnabled()) {
				LOG.debug("RangerTagProvider.enrich(" + request + ") - no tags found by enricher.");
			}
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== RangerTagProvider.enrich(" + request + ")");
		}
	}

	@Override
	public void setServiceTags(final ServiceTags serviceTags) {
		this.serviceTags = serviceTags;

		List<RangerServiceResourceMatcher> resourceMatchers = new ArrayList<RangerServiceResourceMatcher>();

		List<RangerServiceResource> serviceResources = this.serviceTags.getServiceResources();

		if (CollectionUtils.isNotEmpty(serviceResources)) {

			for (RangerServiceResource serviceResource : serviceResources) {
				RangerDefaultPolicyResourceMatcher matcher = new RangerDefaultPolicyResourceMatcher();

				matcher.setServiceDef(this.serviceDef);
				matcher.setPolicyResources(serviceResource.getResourceElements());

				if (LOG.isDebugEnabled()) {
					LOG.debug("RangerTagProvider.setServiceTags() - Initializing matcher with (resource=" + serviceResource
							+ ", serviceDef=" + this.serviceDef.getName() + ")" );

				}
				matcher.init();

				RangerServiceResourceMatcher serviceResourceMatcher = new RangerServiceResourceMatcher(serviceResource, matcher);
				resourceMatchers.add(serviceResourceMatcher);

			}
		}

		serviceResourceMatchers = resourceMatchers;

		if (tagRefresher != null && !tagRefresher.getIsStarted()) {
			tagRefresher.startRetriever();
		}
	}

	private List<RangerTag> findMatchingTags(final RangerAccessResource resource, final List<RangerServiceResourceMatcher> resourceMatchers) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> RangerTagProvider.findMatchingTags(" + resource + ")");
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
				LOG.debug("RangerTagProvider.findMatchingTags(" + resource + ") - No tags Found ");
			} else {
				LOG.debug("RangerTagProvider.findMatchingTags(" + resource + ") - " + ret.size() + " tags Found ");
			}
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== RangerTagProvider.findMatchingTags(" + resource + ")");
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
}
