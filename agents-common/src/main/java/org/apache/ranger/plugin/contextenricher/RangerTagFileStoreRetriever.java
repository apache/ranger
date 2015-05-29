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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ranger.plugin.model.RangerResource;
import org.apache.ranger.plugin.store.file.TagFileStore;

import java.util.List;
import java.util.Map;

public class RangerTagFileStoreRetriever extends RangerTagRefresher {
	private static final Log LOG = LogFactory.getLog(RangerTagFileStoreRetriever.class);

	private final String componentType;
	private final String tagServiceName;
	private RangerTagReceiver receiver;
	private TagFileStore tagFileStore;

	public RangerTagFileStoreRetriever(final String componentType, final String tagServiceName, final long pollingIntervalMs, final RangerTagReceiver enricher) {
		super(pollingIntervalMs);
		this.componentType = componentType;
		this.tagServiceName = tagServiceName;
		setReceiver(enricher);
	}

	@Override
	public void init(Map<String, Object> options) {
		tagFileStore = TagFileStore.getInstance();
	}

	@Override
	public void setReceiver(RangerTagReceiver receiver) {
		this.receiver = receiver;
	}

	@Override
	public void retrieveTags() {
		if (tagFileStore != null) {
			List<RangerResource> resources = null;

			try {
				resources = tagFileStore.getResources(tagServiceName, componentType);
			} catch (Exception exp) {
				LOG.error("RangerTagFileStoreRetriever.retrieveTags() - Error retrieving resources");
			}

			if (receiver != null) {
				receiver.setRangerResources(resources);
			} else {
				LOG.error("RangerTagFileStoreRetriever.retrieveTags() - No receiver to send resources to !!");
			}
		} else {
			LOG.error("RangerTagFileStoreRetriever.retrieveTags() - No TagFileStore ...");
		}
	}

}

