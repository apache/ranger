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
import org.apache.log4j.Logger;
import org.apache.ranger.audit.destination.ElasticSearchAuditDestination;
import org.apache.ranger.audit.provider.MiscUtil;
import org.apache.ranger.common.PropertiesUtil;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.stereotype.Component;

/**
 * This class initializes Solr
 *
 */
@Component
public class ElasticSearchMgr {

	private static final Logger logger = Logger.getLogger(ElasticSearchMgr.class);

	private String getProperty(String propName, String defaultValue) {
		String value = PropertiesUtil.getProperty(propName);
		if (null == value) return defaultValue;
		return value;
	}

	synchronized void connect() {
		RestHighLevelClient me = client;
		if (me == null) {
			synchronized (ElasticSearchAuditDestination.class) {
				me = client;
				if (client == null) {
					String urls = PropertiesUtil.getProperty("ranger.audit.solr.url");
					String protocol = getProperty("ranger.audit.solr.url", "http");
					int port = Integer.parseInt(getProperty("ranger.audit.solr.port", "8080"));
					if (urls != null) {
						urls = urls.trim();
					}
					if (urls != null && urls.equalsIgnoreCase("NONE")) {
						urls = null;
					}

					try {
						me = client = new RestHighLevelClient(
								RestClient.builder(
										MiscUtil.toArray(urls, ",").stream()
												.map(x -> new HttpHost(x, port, protocol))
												.<HttpHost>toArray(i -> new HttpHost[i])
								));
						;
					} catch (Throwable t) {
						logger.fatal("Can't connect to Solr server. urls=" + urls, t);
					}
				}
			}
		}
	}

	RestHighLevelClient client = null;
	public RestHighLevelClient getClient() {
		if(client !=null){
			return client;
		}else{
			synchronized(this){
				connect();
			}
		}
		return client;
	}

}
