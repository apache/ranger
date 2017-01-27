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
import org.apache.ranger.admin.client.RangerAdminClient;
import org.apache.ranger.plugin.service.RangerBasePlugin;
import org.apache.ranger.plugin.util.ServiceTags;

import java.nio.channels.ClosedByInterruptException;
import java.util.Map;

public class RangerAdminTagRetriever extends RangerTagRetriever {
	private static final Log LOG = LogFactory.getLog(RangerAdminTagRetriever.class);

	private RangerAdminClient adminClient;

	@Override
	public void init(Map<String, String> options) {

		if (StringUtils.isNotBlank(serviceName) && serviceDef != null && StringUtils.isNotBlank(appId)) {
			String propertyPrefix    = "ranger.plugin." + serviceDef.getName();

			adminClient = RangerBasePlugin.createAdminClient(serviceName, appId, propertyPrefix);

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
			}
		}
		return serviceTags;
	}

}

