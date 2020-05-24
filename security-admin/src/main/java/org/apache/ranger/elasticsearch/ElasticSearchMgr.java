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

package org.apache.ranger.elasticsearch;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.log4j.Logger;
import org.apache.ranger.audit.destination.ElasticSearchAuditDestination;
import org.apache.ranger.audit.provider.MiscUtil;
import org.apache.ranger.common.PropertiesUtil;
import org.apache.ranger.common.StringUtil;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.stereotype.Component;

import static org.apache.ranger.audit.destination.ElasticSearchAuditDestination.*;

/**
 * This class initializes the ElasticSearch client
 *
 */
@Component
public class ElasticSearchMgr {

	private static final Logger logger = Logger.getLogger(ElasticSearchMgr.class);
	public String index;

	synchronized void connect() {
		if (client == null) {
			synchronized (ElasticSearchAuditDestination.class) {
				if (client == null) {

					String urls = PropertiesUtil.getProperty(CONFIG_PREFIX + "." + CONFIG_URLS);
					String protocol = PropertiesUtil.getProperty(CONFIG_PREFIX + "." + CONFIG_PROTOCOL, "http");
					String user = PropertiesUtil.getProperty(CONFIG_PREFIX + "." + CONFIG_USER, "");
					String password = PropertiesUtil.getProperty(CONFIG_PREFIX + "." + CONFIG_PWRD, "");
					int port = Integer.parseInt(PropertiesUtil.getProperty(CONFIG_PREFIX + "." + CONFIG_PORT));
					this.index = PropertiesUtil.getProperty(CONFIG_PREFIX + "." + CONFIG_INDEX, "audit");
					String parameterString = String.format("(URLs: %s; Protocol: %s; Port: %s; User: %s)",
							urls, protocol, port, user);
					logger.info("Initializing ElasticSearch " + parameterString);
					if (urls != null) {
						urls = urls.trim();
					}
					if (!new StringUtil().isEmpty(urls) && urls.equalsIgnoreCase("NONE")) {
						logger.info(String.format("Clearing URI config value: %s", urls));
						urls = null;
					}

					try {

						final CredentialsProvider credentialsProvider;
						if(!user.isEmpty()) {
							credentialsProvider = new BasicCredentialsProvider();
							logger.info(String.format("Using %s to login to ElasticSearch", user));
							credentialsProvider.setCredentials(AuthScope.ANY,
									new UsernamePasswordCredentials(user, password));
						} else {
							credentialsProvider = null;
						}
						client = new RestHighLevelClient(
								RestClient.builder(
										MiscUtil.toArray(urls, ",").stream()
												.map(x -> new HttpHost(x, port, protocol))
												.<HttpHost>toArray(i -> new HttpHost[i])
								).setHttpClientConfigCallback(clientBuilder ->
										(credentialsProvider != null) ? clientBuilder.setDefaultCredentialsProvider(credentialsProvider) : clientBuilder));
					} catch (Throwable t) {
						logger.fatal("Can't connect to ElasticSearch: " + parameterString, t);
					}
				}
			}
		}
	}

	RestHighLevelClient client = null;
	public RestHighLevelClient getClient() {
		if(client !=null) {
			return client;
		} else {
			connect();
		}
		return client;
	}

}
