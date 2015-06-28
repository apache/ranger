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
import org.apache.ranger.plugin.model.RangerTaggedResource;
import org.apache.ranger.plugin.store.TagStore;
import org.apache.ranger.plugin.store.file.TagFileStore;
import org.apache.ranger.plugin.util.TagServiceResources;

import java.util.Date;
import java.util.List;
import java.util.Map;

public class RangerTagFileStoreRetriever extends RangerTagRefresher {
	private static final Log LOG = LogFactory.getLog(RangerTagFileStoreRetriever.class);

	private final String componentType;
	private final String tagServiceName;
	private RangerTagReceiver receiver;

	private TagStore tagStore;
	private Long lastTimestamp;

	public RangerTagFileStoreRetriever(final String componentType, final String tagServiceName, final long pollingIntervalMs, final RangerTagReceiver enricher) {
		super(pollingIntervalMs);
		this.componentType = componentType;
		this.tagServiceName = tagServiceName;
		this.lastTimestamp = 0L;
		setReceiver(enricher);
	}

	@Override
	public void init(Map<String, String> options) {
		tagStore = TagFileStore.getInstance();
	}

	@Override
	public void setReceiver(RangerTagReceiver receiver) {
		this.receiver = receiver;
	}

	@Override
	public void retrieveTags() {
		if (tagStore != null) {
			List<RangerTaggedResource> resources = null;

			try {
				long before = new Date().getTime();
				TagServiceResources tagServiceResources = tagStore.getResources(tagServiceName, componentType, lastTimestamp);
				resources = tagServiceResources.getTaggedResources();
				lastTimestamp = before;
			} catch (Exception exp) {
				LOG.error("RangerTagFileStoreRetriever.retrieveTags() - Error retrieving resources");
			}

			if (receiver != null && CollectionUtils.isNotEmpty(resources)) {
				receiver.setRangerTaggedResources(resources);
			} else {
				LOG.error("RangerAdminTagRetriever.retrieveTags() - No receiver to send resources to .. OR .. no updates to tagged resources!!");
			}
		} else {
			LOG.error("RangerTagFileStoreRetriever.retrieveTags() - No TagFileStore ...");
		}
	}

}

