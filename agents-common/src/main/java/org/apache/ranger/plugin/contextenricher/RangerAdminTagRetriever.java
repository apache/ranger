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
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.service.RangerBasePlugin;
import org.apache.ranger.plugin.util.ServiceTags;
import org.apache.ranger.services.tag.RangerServiceTag;

import java.util.Date;
import java.util.List;
import java.util.Map;

public class RangerAdminTagRetriever extends RangerTagRefresher {
	private static final Log LOG = LogFactory.getLog(RangerAdminTagRetriever.class);
	private static String propertyPrefixPreamble = "ranger.plugin.";
	private static String appId = "tag-retriever";

	private final String serviceName;
	private final String propertyPrefix;

	private RangerTagReceiver receiver;
	private RangerAdminClient adminClient;
	private long lastKnownVersion;

	public RangerAdminTagRetriever(final String serviceName, final RangerServiceDef serviceDef, final long pollingIntervalMs, final RangerTagReceiver enricher) {
		super(pollingIntervalMs);
		this.serviceName = serviceName;
		setReceiver(enricher);
		propertyPrefix = propertyPrefixPreamble + serviceDef.getName();
		this.lastKnownVersion = -1L;
	}

	@Override
	public void init(Map<String, String> options) {

		if (adminClient == null) {
			adminClient = RangerBasePlugin.createAdminClient(serviceName, appId, propertyPrefix);
		}

	}

	@Override
	public void setReceiver(RangerTagReceiver receiver) {
		this.receiver = receiver;
	}

	@Override
	public void retrieveTags() {
		if (adminClient != null && receiver != null) {
			ServiceTags serviceTags = null;
			try {
				serviceTags = adminClient.getServiceTagsIfUpdated(lastKnownVersion);
			} catch (Exception exp) {
				LOG.error("RangerAdminTagRetriever.retrieveTags() - Error retrieving resources, exception=", exp);
			}

			if (serviceTags != null) {
				LOG.info("RangerAdminTagRetriever.retrieveTags() - Updating tags-cache to new version of tags, lastKnownVersion=" + lastKnownVersion + "; newVersion=" + serviceTags.getTagVersion());
				lastKnownVersion = serviceTags.getTagVersion();
				receiver.setServiceTags(serviceTags);
			} else {
				if (LOG.isDebugEnabled()) {
					LOG.debug("RangerAdminTagRetriever.retrieveTags() - No need to update tags-cache. lastKnownVersion=" + lastKnownVersion);
				}
			}
		} else {
			LOG.error("RangerAdminTagRetriever.retrieveTags() - No admin client to get tags from or no tag receiver to update tag-cache");
		}
	}

}

