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

package org.apache.ranger.solr;

import java.util.*;

import org.apache.log4j.Logger;
import org.apache.ranger.audit.utils.InMemoryJAASConfiguration;
import org.apache.ranger.biz.RangerBizUtil;
import org.apache.ranger.common.PropertiesUtil;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.BinaryRequestWriter;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.HttpClientUtil;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.impl.Krb5HttpClientBuilder;
import org.apache.solr.client.solrj.impl.SolrHttpClientBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * This class initializes Solr
 *
 */
@Component
public class SolrMgr {

	private static final Logger logger = Logger.getLogger(SolrMgr.class);

	@Autowired
	RangerBizUtil rangerBizUtil;

	static final Object lock = new Object();

	SolrClient solrClient = null;
	Date lastConnectTime = null;
	volatile boolean initDone = false;

	final static String SOLR_URLS_PROP = "ranger.audit.solr.urls";
	final static String SOLR_ZK_HOSTS = "ranger.audit.solr.zookeepers";
	final static String SOLR_COLLECTION_NAME = "ranger.audit.solr.collection.name";
	final static String PROP_JAVA_SECURITY_AUTH_LOGIN_CONFIG   = "java.security.auth.login.config";

	public static final String DEFAULT_COLLECTION_NAME = "ranger_audits";

	public SolrMgr() {
		init();
	}

	void connect() {
		if (!initDone) {
			synchronized (lock) {
				if (!initDone) {
					if ("solr".equalsIgnoreCase(rangerBizUtil.getAuditDBType())) {
						String zkHosts = PropertiesUtil
								.getProperty(SOLR_ZK_HOSTS);
						if (zkHosts == null) {
							zkHosts = PropertiesUtil
									.getProperty("ranger.audit.solr.zookeeper");
						}
						if (zkHosts == null) {
							zkHosts = PropertiesUtil
									.getProperty("ranger.solr.zookeeper");
						}

						String solrURL = PropertiesUtil
								.getProperty(SOLR_URLS_PROP);

						if (solrURL == null) {
							// Try with url
							solrURL = PropertiesUtil
									.getProperty("ranger.audit.solr.url");
						}
						if (solrURL == null) {
							// Let's try older property name
							solrURL = PropertiesUtil
									.getProperty("ranger.solr.url");
						}

						if (zkHosts != null && !"".equals(zkHosts.trim())
								&& !"none".equalsIgnoreCase(zkHosts.trim())) {
							zkHosts = zkHosts.trim();
							String collectionName = PropertiesUtil
									.getProperty(SOLR_COLLECTION_NAME);
							if (collectionName == null
									|| "none".equalsIgnoreCase(collectionName)) {
								collectionName = DEFAULT_COLLECTION_NAME;
							}

							logger.info("Solr zkHosts=" + zkHosts
									+ ", collectionName=" + collectionName);

							try {
								// Instantiate
								Krb5HttpClientBuilder krbBuild = new Krb5HttpClientBuilder();
								SolrHttpClientBuilder kb = krbBuild.getBuilder();
								HttpClientUtil.setHttpClientBuilder(kb);
								final List<String> zkhosts = new ArrayList<String>(Arrays.asList(zkHosts.split(",")));
								CloudSolrClient solrCloudClient = new CloudSolrClient.Builder(zkhosts, Optional.empty()).build();
								solrCloudClient
										.setDefaultCollection(collectionName);
								solrClient = solrCloudClient;
							} catch (Throwable t) {
								logger.fatal(
										"Can't connect to Solr server. ZooKeepers="
												+ zkHosts + ", collection="
												+ collectionName, t);
							}

						} else {
							if (solrURL == null || solrURL.isEmpty()
									|| "none".equalsIgnoreCase(solrURL)) {
								logger.fatal("Solr ZKHosts and URL for Audit are empty. Please set property "
										+ SOLR_ZK_HOSTS
										+ " or "
										+ SOLR_URLS_PROP);
							} else {
								try {
									Krb5HttpClientBuilder krbBuild = new Krb5HttpClientBuilder();
									SolrHttpClientBuilder kb = krbBuild.getBuilder();
									HttpClientUtil.setHttpClientBuilder(kb);
									HttpSolrClient.Builder builder = new HttpSolrClient.Builder();
									builder.withBaseSolrUrl(solrURL);
									builder.allowCompression(true);
									builder.withConnectionTimeout(1000);
									HttpSolrClient httpSolrClient = builder.build();
									httpSolrClient
											.setRequestWriter(new BinaryRequestWriter());
									solrClient = httpSolrClient;
									initDone = true;

								} catch (Throwable t) {
									logger.fatal(
											"Can't connect to Solr server. URL="
													+ solrURL, t);
								}
							}
						}
					}

				}
			}
		}
	}

	private void init() {
		logger.info("==>SolrMgr.init()" );
		Properties  props = PropertiesUtil.getProps();
		try {
			 // SolrJ requires "java.security.auth.login.config"  property to be set to identify itself that it is kerberized. So using a dummy property for it
			 // Acutal solrclient JAAS configs are read from the ranger-admin-site.xml in ranger admin config folder and set by InMemoryJAASConfiguration
			 // Refer InMemoryJAASConfiguration doc for JAAS Configuration
			 if ( System.getProperty(PROP_JAVA_SECURITY_AUTH_LOGIN_CONFIG) == null ) {
				 System.setProperty(PROP_JAVA_SECURITY_AUTH_LOGIN_CONFIG, "/dev/null");
			 }
			 logger.info("Loading SolrClient JAAS config from Ranger audit config if present...");
			 InMemoryJAASConfiguration.init(props);
			} catch (Exception e) {
				logger.error("ERROR: Unable to load SolrClient JAAS config from ranger admin config file. Audit to Kerberized Solr will fail...", e);
			}
		logger.info("<==SolrMgr.init()" );
	}

	public SolrClient getSolrClient() {
		if(solrClient!=null){
			return solrClient;
		}else{
			synchronized(this){
				connect();
			}
		}
		return solrClient;
	}

}
