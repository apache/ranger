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

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ranger.plugin.store.TagStore;
import org.apache.ranger.plugin.store.file.TagFileStore;
import org.apache.ranger.plugin.util.ServiceTags;

import java.nio.channels.ClosedByInterruptException;
import java.util.Map;

public class RangerTagFileStoreRetriever extends RangerTagRetriever {
	private static final Log LOG = LogFactory.getLog(RangerTagFileStoreRetriever.class);

	private TagStore tagStore;

	public RangerTagFileStoreRetriever() {
	}

	@Override
	public void init(Map<String, String> options) {
		if (StringUtils.isNotBlank(serviceName) && serviceDef != null && StringUtils.isNotBlank(appId) && tagReceiver != null) {

			tagStore = TagFileStore.getInstance();

		} else {
			LOG.error("FATAL: Cannot find service-name to use for retrieving tags. Will NOT be able to retrieve tags.");
		}
	}

	@Override
	public void retrieveTags() throws InterruptedException {

		if (tagStore != null && tagReceiver != null) {
			ServiceTags serviceTags = null;
			try {
				serviceTags = tagStore.getServiceTagsIfUpdated(serviceName, lastKnownVersion);
			}
			catch (InterruptedException interruptedException) {
				LOG.error("Tag-retriever thread was interrupted");
				throw interruptedException;
			}
			catch (ClosedByInterruptException closedByInterruptException) {
				LOG.error("Tag-retriever thread was interrupted while blocked on I/O");
				throw new InterruptedException();
			}
			catch (Exception exception) {
				LOG.error("RangerTagFileStoreRetriever.retrieveTags() - Error retrieving resources, exception=", exception);
			}

			if (serviceTags != null) {
				tagReceiver.setServiceTags(serviceTags);
				LOG.info("RangerTagFileStoreRetriever.retrieveTags() - Updated tags-cache to new version of tags, lastKnownVersion=" + lastKnownVersion + "; newVersion=" + serviceTags.getTagVersion());
				setLastKnownVersion(serviceTags.getTagVersion());
			} else {
				if (LOG.isDebugEnabled()) {
					LOG.debug("RangerTagFileStoreRetriever.retrieveTags() - No need to update tags-cache. lastKnownVersion=" + lastKnownVersion);
				}
			}
		} else {
			LOG.error("RangerTagFileStoreRetriever.retrieveTags() - No tag-store to get tags from or no tag receiver to update tag-cache...");
		}
	}
}

