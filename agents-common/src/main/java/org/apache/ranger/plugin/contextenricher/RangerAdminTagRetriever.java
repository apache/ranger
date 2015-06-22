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
import org.apache.ranger.admin.client.RangerAdminRESTClient;
import org.apache.ranger.authorization.hadoop.config.RangerConfiguration;
import org.apache.ranger.plugin.model.RangerResource;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RangerAdminTagRetriever extends RangerTagRefresher {
	private static final Log LOG = LogFactory.getLog(RangerAdminTagRetriever.class);

	private final String componentType;
	private final String tagServiceName;
	private RangerTagReceiver receiver;
	private RangerAdminRESTClient rangerAdminRESTClient;

	public RangerAdminTagRetriever(final String componentType, final String tagServiceName, final long pollingIntervalMs, final RangerTagReceiver enricher) {
		super(pollingIntervalMs);
		this.componentType = componentType;
		this.tagServiceName = tagServiceName;
		setReceiver(enricher);
	}

	@Override
	public void init(Map<String, Object> options) {

		String propertyPrefix    = "ranger.plugin.tag";
		String url               = RangerConfiguration.getInstance().get(propertyPrefix + ".provider.rest.url", "http://node-1.example.com:6080");
		String sslConfigFileName = RangerConfiguration.getInstance().get(propertyPrefix + ".provider.rest.ssl.config.file", "abcd");
		String userName               = RangerConfiguration.getInstance().get(propertyPrefix + ".provider.login.username", "admin");
		String password = RangerConfiguration.getInstance().get(propertyPrefix + ".provider.login.password", "admin");

		Map<String, String> configs = new HashMap<String, String>();

		configs.put("URL", url);
		configs.put("SSL_CONFIG_FILE_NAME", sslConfigFileName);
		configs.put("username", userName);
		configs.put("password", password);

		rangerAdminRESTClient = new RangerAdminRESTClient();
		rangerAdminRESTClient.init(tagServiceName, configs);

	}

	@Override
	public void setReceiver(RangerTagReceiver receiver) {
		this.receiver = receiver;
	}

	@Override
	public void retrieveTags() {
		if (rangerAdminRESTClient != null) {
			List<RangerResource> resources = null;

			try {
				resources = rangerAdminRESTClient.getTaggedResources(componentType);
			} catch (Exception exp) {
				LOG.error("RangerAdminTagRetriever.retrieveTags() - Error retrieving resources");
			}

			if (receiver != null && resources != null) {
				receiver.setRangerResources(resources);
			} else {
				LOG.error("RangerAdminTagRetriever.retrieveTags() - No receiver to send resources to !!");
			}
		} else {
			LOG.error("RangerAdminTagRetriever.retrieveTags() - No TagFileStore ...");
		}
	}

}

