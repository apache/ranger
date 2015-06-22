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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerResource;
import org.apache.ranger.plugin.policyengine.RangerAccessRequest;
import org.apache.ranger.plugin.policyengine.RangerAccessResource;
import org.apache.ranger.plugin.policyengine.RangerPolicyEngine;
import org.apache.ranger.plugin.policyresourcematcher.RangerDefaultPolicyResourceMatcher;
import org.apache.ranger.plugin.policyresourcematcher.RangerPolicyResourceMatcher;

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
	List<RangerTaggedResourceMatcher> taggedResourceMatchers;

	@Override
	public void init() {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> RangerTagProvider.init()");
		}

		super.init();

		String tagProviderTypeString = getOption("TagProviderType", "FILE_BASED_TAG_PROVIDER");
		long pollingIntervalMs = getLongOption("pollingInterval", 60 * 1000);

		if (tagProviderTypeString.equals(TagProviderTypeEnum.FILESTORE_BASED_TAG_PROVIDER.toString())) {
			tagRefresher = new RangerTagFileStoreRetriever(serviceDef.getName(), serviceName, pollingIntervalMs, this);
			tagProviderType = TagProviderTypeEnum.FILESTORE_BASED_TAG_PROVIDER;
		} else if (tagProviderTypeString.equals(TagProviderTypeEnum.RANGER_ADMIN_TAG_PROVIDER.toString())) {
			tagRefresher = new RangerAdminTagRetriever(serviceDef.getName(), serviceName, pollingIntervalMs, this);
			tagProviderType = TagProviderTypeEnum.RANGER_ADMIN_TAG_PROVIDER;
		} else if (tagProviderTypeString.equals(TagProviderTypeEnum.EXTERNAL_SYSTEM_TAG_PROVIDER.toString())) {
			// TODO
			tagProviderType = TagProviderTypeEnum.EXTERNAL_SYSTEM_TAG_PROVIDER;
		} else {
			LOG.error("RangerTagProvider.init() - Invalid Tag Provider.. ");
		}

		// Provide additional options
		if (tagRefresher != null) {
			tagRefresher.init(null);
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

		List<RangerTaggedResourceMatcher> taggedResourceMatchersCopy = taggedResourceMatchers;

		List<RangerResource.RangerResourceTag> matchedTags = findMatchingTags(request.getResource(), taggedResourceMatchersCopy);

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
	public void setRangerResources(final List<RangerResource> resources) {

		List<RangerTaggedResourceMatcher> resourceMatchers = new ArrayList<RangerTaggedResourceMatcher>();

		if (CollectionUtils.isNotEmpty(resources)) {

			for (RangerResource taggedResource : resources) {
				RangerDefaultPolicyResourceMatcher matcher = new RangerDefaultPolicyResourceMatcher();

				matcher.setServiceDef(this.serviceDef);
				matcher.setPolicyResources(taggedResource.getResourceSpec());

				if (LOG.isDebugEnabled()) {
					LOG.debug("RangerTagProvider.setRangerResources() - Initializing matcher with (resource=" + taggedResource
							+ ", serviceDef=" + this.serviceDef.getName() + ")" );

				}
				matcher.init();

				RangerTaggedResourceMatcher taggedResourceMatcher = new RangerTaggedResourceMatcher(taggedResource, matcher);
				resourceMatchers.add(taggedResourceMatcher);

			}
		}

		taggedResourceMatchers = resourceMatchers;

		if (tagRefresher != null && !tagRefresher.getIsStarted()) {
			tagRefresher.startRetriever();
		}
	}

	static private List<RangerResource.RangerResourceTag> findMatchingTags(final RangerAccessResource resource, final List<RangerTaggedResourceMatcher> resourceMatchers) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> RangerTagProvider.findMatchingTags(" + resource + ")");
		}

		List<RangerResource.RangerResourceTag> ret = null;

		if (CollectionUtils.isNotEmpty(resourceMatchers)) {

			for (RangerTaggedResourceMatcher resourceMatcher : resourceMatchers) {

				RangerResource taggedResource = resourceMatcher.getRangerResource();
				RangerPolicyResourceMatcher matcher = resourceMatcher.getPolicyResourceMatcher();

				boolean matchResult = matcher.isExactHeadMatch(resource);

				if (matchResult) {
					if (ret == null) {
						ret = new ArrayList<RangerResource.RangerResourceTag>();
					}
					ret.addAll(taggedResource.getTags());
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
}
