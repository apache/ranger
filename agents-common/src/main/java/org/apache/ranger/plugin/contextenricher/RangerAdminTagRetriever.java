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
import org.apache.ranger.admin.client.RangerAdminClient;
import org.apache.ranger.authorization.hadoop.config.RangerPluginConfig;
import org.apache.ranger.plugin.policyengine.RangerPluginContext;
import org.apache.ranger.plugin.util.ServiceTags;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.channels.ClosedByInterruptException;
import java.util.Map;

public class RangerAdminTagRetriever extends RangerTagRetriever {
	private static final Logger LOG = LoggerFactory.getLogger(RangerAdminTagRetriever.class);

	private static final String  OPTION_DEDUP_TAGS         = "deDupTags";
	private static final Boolean OPTION_DEDUP_TAGS_DEFAULT = true;


	private RangerAdminClient adminClient;
	private boolean           deDupTags;

	@Override
	public void init(Map<String, String> options) {

		if (StringUtils.isNotBlank(serviceName) && serviceDef != null && StringUtils.isNotBlank(appId)) {
			RangerPluginConfig pluginConfig = super.pluginConfig;

			if (pluginConfig == null) {
				pluginConfig = new RangerPluginConfig(serviceDef.getName(), serviceName, appId, null, null, null);
			}

			String              deDupTagsVal  = options != null? options.get(OPTION_DEDUP_TAGS) : null;
			RangerPluginContext pluginContext = getPluginContext();
			RangerAdminClient	rangerAdmin   = pluginContext.getAdminClient();

			this.deDupTags   = StringUtils.isNotBlank(deDupTagsVal) ? Boolean.parseBoolean(deDupTagsVal) : OPTION_DEDUP_TAGS_DEFAULT;
			this.adminClient = (rangerAdmin != null) ? rangerAdmin : pluginContext.createAdminClient(pluginConfig);
		} else {
			LOG.error("FATAL: Cannot find service/serviceDef to use for retrieving tags. Will NOT be able to retrieve tags.");
		}
	}

	@Override
	public ServiceTags retrieveTags(long lastKnownVersion, long lastActivationTimeInMillis) throws Exception {

		ServiceTags serviceTags = null;

		if (adminClient != null) {
			try {
				serviceTags = adminClient.getServiceTagsIfUpdated(lastKnownVersion, lastActivationTimeInMillis);
			} catch (ClosedByInterruptException closedByInterruptException) {
				LOG.error("Tag-retriever thread was interrupted while blocked on I/O");
				throw new InterruptedException();
			} catch (Exception e) {
				LOG.error("Tag-retriever encounterd exception, exception=", e);
				LOG.error("Returning null service tags");
			}
		}

		if (serviceTags != null && !serviceTags.getIsDelta() && deDupTags) {
			final int countOfDuplicateTags = serviceTags.dedupTags();

			LOG.info("Number of duplicate tags removed from the received serviceTags:[" + countOfDuplicateTags + "]. Number of tags in the de-duplicated serviceTags :[" + serviceTags.getTags().size() + "].");
		}

		return serviceTags;
	}

}

