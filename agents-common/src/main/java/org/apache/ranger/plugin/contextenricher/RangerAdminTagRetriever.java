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
import org.apache.ranger.admin.client.RangerAdminClient;
import org.apache.ranger.plugin.model.RangerTaggedResource;
import org.apache.ranger.plugin.service.RangerBasePlugin;
import org.apache.ranger.plugin.util.TagServiceResources;
import org.apache.ranger.services.tag.RangerServiceTag;

import java.util.Date;
import java.util.List;
import java.util.Map;

public class RangerAdminTagRetriever extends RangerTagRefresher {
	private static final Log LOG = LogFactory.getLog(RangerAdminTagRetriever.class);
	private static String propertyPrefixPreamble = "ranger.plugin.";
	private static String appId = "tag-retriever";

	private final String componentType;
	private final String tagServiceName;
	private final String propertyPrefix;

	private RangerTagReceiver receiver;
	private RangerAdminClient adminClient;
	private Long lastTimestamp;

	public RangerAdminTagRetriever(final String componentType, final String tagServiceName, final long pollingIntervalMs, final RangerTagReceiver enricher) {
		super(pollingIntervalMs);
		this.componentType = componentType;
		this.tagServiceName = tagServiceName;
		setReceiver(enricher);
		propertyPrefix = propertyPrefixPreamble + componentType;
		this.lastTimestamp = 0L;
	}

	@Override
	public void init(Map<String, String> options) {

		if (MapUtils.isNotEmpty(options)) {
			String useTestTagProvider = options.get("useTestTagProvider");

			if (useTestTagProvider != null && useTestTagProvider.equals("true")) {
				adminClient = RangerServiceTag.createAdminClient(tagServiceName);
			}
		}
		if (adminClient == null) {
			adminClient = RangerBasePlugin.createAdminClient(tagServiceName, appId, propertyPrefix);
		}

	}

	@Override
	public void setReceiver(RangerTagReceiver receiver) {
		this.receiver = receiver;
	}

	@Override
	public void retrieveTags() {
		if (adminClient != null) {
			List<RangerTaggedResource> resources = null;

			try {
				long before = new Date().getTime();
				TagServiceResources taggedResources = adminClient.getTaggedResources(tagServiceName, componentType, lastTimestamp);
				resources = taggedResources.getTaggedResources();
				lastTimestamp = before;
			} catch (Exception exp) {
				LOG.error("RangerAdminTagRetriever.retrieveTags() - Error retrieving resources");
			}

			if (receiver != null && CollectionUtils.isNotEmpty(resources)) {
				receiver.setRangerTaggedResources(resources);
			} else {
				LOG.error("RangerAdminTagRetriever.retrieveTags() - No receiver to send resources to .. OR .. no updates to tagged resources!!");
			}
		} else {
			LOG.error("RangerAdminTagRetriever.retrieveTags() - No Tag Provider ...");
		}
	}

}

