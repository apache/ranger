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

package org.apache.ranger.audit.destination;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ranger.audit.model.AuditEventBase;
import org.apache.ranger.audit.model.AuthzAuditEvent;
import org.apache.ranger.audit.provider.MiscUtil;
import org.apache.ranger.audit.utils.InMemoryJAASConfiguration;
import org.apache.ranger.audit.utils.SolrAppUtil;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.HttpClientUtil;
import org.apache.solr.client.solrj.impl.Krb5HttpClientConfigurer;
import org.apache.solr.client.solrj.impl.LBHttpSolrClient;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;

import java.lang.reflect.Field;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;


public class SolrAuditDestination extends AuditDestination {
	private static final Log LOG = LogFactory
			.getLog(SolrAuditDestination.class);

	public static final String PROP_SOLR_URLS = "urls";
	public static final String PROP_SOLR_ZK = "zookeepers";
	public static final String PROP_SOLR_COLLECTION = "collection";
	public static final String PROP_SOLR_FORCE_USE_INMEMORY_JAAS_CONFIG = "force.use.inmemory.jaas.config";

	public static final String DEFAULT_COLLECTION_NAME = "ranger_audits";
	public static final String PROP_JAVA_SECURITY_AUTH_LOGIN_CONFIG = "java.security.auth.login.config";

	private volatile SolrClient solrClient = null;

	public SolrAuditDestination() {
	}

	@Override
	public void init(Properties props, String propPrefix) {
		LOG.info("init() called");
		super.init(props, propPrefix);
		init();
		connect();
	}

	@Override
	public void stop() {
		super.stop();
		logStatus();
	}

	synchronized void connect() {
		SolrClient me = solrClient;
		if (me == null) {
			synchronized(SolrAuditDestination.class) {
				me = solrClient;
				if (solrClient == null) {
					String urls = MiscUtil.getStringProperty(props, propPrefix
							+ "." + PROP_SOLR_URLS);
					if (urls != null) {
						urls = urls.trim();
					}
					if (urls != null && urls.equalsIgnoreCase("NONE")) {
						urls = null;
					}
					List<String> solrURLs = new ArrayList<String>();
					String zkHosts = null;
					solrURLs = MiscUtil.toArray(urls, ",");
					zkHosts = MiscUtil.getStringProperty(props, propPrefix + "."
							+ PROP_SOLR_ZK);
					if (zkHosts != null && zkHosts.equalsIgnoreCase("NONE")) {
						zkHosts = null;
					}
					String collectionName = MiscUtil.getStringProperty(props,
							propPrefix + "." + PROP_SOLR_COLLECTION);
					if (collectionName == null
							|| collectionName.equalsIgnoreCase("none")) {
						collectionName = DEFAULT_COLLECTION_NAME;
					}

					LOG.info("Solr zkHosts=" + zkHosts + ", solrURLs=" + urls
							+ ", collectionName=" + collectionName);

					if (zkHosts != null && !zkHosts.isEmpty()) {
						LOG.info("Connecting to solr cloud using zkHosts="
								+ zkHosts);
						try {
							// Instantiate
							HttpClientUtil.setConfigurer(new Krb5HttpClientConfigurer());
							final String zkhosts =zkHosts;
							final CloudSolrClient solrCloudClient = MiscUtil.executePrivilegedAction(new PrivilegedExceptionAction<CloudSolrClient>() {
								@Override
								public CloudSolrClient run()  throws Exception {
									CloudSolrClient solrCloudClient = new CloudSolrClient(
											zkhosts);
									return solrCloudClient;
								};
							});

							solrCloudClient.setDefaultCollection(collectionName);
							me = solrClient = solrCloudClient;
						} catch (Throwable t) {
							LOG.fatal("Can't connect to Solr server. ZooKeepers="
									+ zkHosts, t);
						}
						finally {
							resetInitializerInSOLR();
						}
					} else if (solrURLs != null && !solrURLs.isEmpty()) {
						try {
							LOG.info("Connecting to Solr using URLs=" + solrURLs);
							HttpClientUtil.setConfigurer(new Krb5HttpClientConfigurer());
							final List<String> solrUrls = solrURLs;
							final LBHttpSolrClient lbSolrClient = MiscUtil.executePrivilegedAction(new PrivilegedExceptionAction<LBHttpSolrClient>() {
								@Override
								public LBHttpSolrClient run()  throws Exception {
									LBHttpSolrClient lbSolrClient = new LBHttpSolrClient(
											solrUrls.get(0));
									return lbSolrClient;
								};
							});

							lbSolrClient.setConnectionTimeout(1000);

							for (int i = 1; i < solrURLs.size(); i++) {
								lbSolrClient.addSolrServer(solrURLs.get(i));
							}
							me = solrClient = lbSolrClient;
						} catch (Throwable t) {
							LOG.fatal("Can't connect to Solr server. URL="
									+ solrURLs, t);
						}
						finally {
							resetInitializerInSOLR();
						}
					}
				}
			}
		}
	}


    private void resetInitializerInSOLR() {
		javax.security.auth.login.Configuration solrConfig = javax.security.auth.login.Configuration.getConfiguration();
		String solrConfigClassName = solrConfig.getClass().getName();
		String solrJassConfigEnd = "SolrJaasConfiguration";
		if (solrConfigClassName.endsWith(solrJassConfigEnd)) {
			try {
				Field f = solrConfig.getClass().getDeclaredField("initiateAppNames");
				if (f != null) {
					f.setAccessible(true);
					HashSet<String> val = new HashSet<String>();
					f.set(solrConfig, val);
					if ( LOG.isDebugEnabled() ) {
						LOG.debug("resetInitializerInSOLR: successfully reset the initiateAppNames");
					}

				} else {
					if ( LOG.isDebugEnabled() ) {
						LOG.debug("resetInitializerInSOLR: not applying on class [" + solrConfigClassName + "] as it does not have initiateAppNames variable name.");
					}
				}
			} catch (Throwable t) {
				logError("resetInitializerInSOLR: Unable to reset SOLRCONFIG.initiateAppNames to be empty", t);
			}
		}
		else {
			if ( LOG.isDebugEnabled() ) {
				LOG.debug("resetInitializerInSOLR: not applying on class [" + solrConfigClassName + "] as it does not endwith [" + solrJassConfigEnd + "]");
			}
		}

    }

	@Override
	public boolean log(Collection<AuditEventBase> events) {
		boolean ret = false;
		try {
			logStatusIfRequired();
			addTotalCount(events.size());

			if (solrClient == null) {
				connect();
				if (solrClient == null) {
					// Solr is still not initialized. So need return error
					addDeferredCount(events.size());
					return ret;
				}
			}

			final Collection<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();
			for (AuditEventBase event : events) {
				AuthzAuditEvent authzEvent = (AuthzAuditEvent) event;
				// Convert AuditEventBase to Solr document
				SolrInputDocument document = toSolrDoc(authzEvent);
				docs.add(document);
			}
			try {
				final UpdateResponse response = SolrAppUtil.addDocsToSolr(solrClient, docs);

				if (response.getStatus() != 0) {
					addFailedCount(events.size());
					logFailedEvent(events, response.toString());
				} else {
					addSuccessCount(events.size());
					ret = true;
				}
			} catch (SolrException ex) {
				addFailedCount(events.size());
				logFailedEvent(events, ex);
			}
		} catch (Throwable t) {
			addDeferredCount(events.size());
			logError("Error sending message to Solr", t);
		}
		return ret;
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see org.apache.ranger.audit.provider.AuditProvider#flush()
	 */
	@Override
	public void flush() {

	}

	SolrInputDocument toSolrDoc(AuthzAuditEvent auditEvent) {
		SolrInputDocument doc = new SolrInputDocument();
		doc.addField("id", auditEvent.getEventId());
		doc.addField("access", auditEvent.getAccessType());
		doc.addField("enforcer", auditEvent.getAclEnforcer());
		doc.addField("agent", auditEvent.getAgentId());
		doc.addField("repo", auditEvent.getRepositoryName());
		doc.addField("sess", auditEvent.getSessionId());
		doc.addField("reqUser", auditEvent.getUser());
		doc.addField("reqData", auditEvent.getRequestData());
		doc.addField("resource", auditEvent.getResourcePath());
		doc.addField("cliIP", auditEvent.getClientIP());
		doc.addField("logType", auditEvent.getLogType());
		doc.addField("result", auditEvent.getAccessResult());
		doc.addField("policy", auditEvent.getPolicyId());
		doc.addField("repoType", auditEvent.getRepositoryType());
		doc.addField("resType", auditEvent.getResourceType());
		doc.addField("reason", auditEvent.getResultReason());
		doc.addField("action", auditEvent.getAction());
		doc.addField("evtTime", auditEvent.getEventTime());
		doc.addField("seq_num", auditEvent.getSeqNum());
		doc.setField("event_count", auditEvent.getEventCount());
		doc.setField("event_dur_ms", auditEvent.getEventDurationMS());
		doc.setField("tags", auditEvent.getTags());
		doc.setField("cluster", auditEvent.getClusterName());
		doc.setField("zoneName", auditEvent.getZoneName());

		return doc;
	}

	public boolean isAsync() {
		return true;
	}

	private void init() {
		LOG.info("==>SolrAuditDestination.init()" );
		try {
			 // SolrJ requires "java.security.auth.login.config"  property to be set to identify itself that it is kerberized. So using a dummy property for it
			 // Acutal solrclient JAAS configs are read from the ranger-<component>-audit.xml present in  components conf folder and set by InMemoryJAASConfiguration
			 // Refer InMemoryJAASConfiguration doc for JAAS Configuration
			 String confFileName = System.getProperty(PROP_JAVA_SECURITY_AUTH_LOGIN_CONFIG);
			 LOG.info("In solrAuditDestination.init() : JAAS Configuration set as [" + confFileName + "]");
			 if ( System.getProperty(PROP_JAVA_SECURITY_AUTH_LOGIN_CONFIG) == null ) {
				 if ( MiscUtil.getBooleanProperty(props, propPrefix + "." + PROP_SOLR_FORCE_USE_INMEMORY_JAAS_CONFIG,false) ) {
					 System.setProperty(PROP_JAVA_SECURITY_AUTH_LOGIN_CONFIG, "/dev/null");
				 } else {
					LOG.warn("No Client JAAS config present in solr audit config. Ranger Audit to Kerberized Solr will fail...");
				}
			 }
			 LOG.info("Loading SolrClient JAAS config from Ranger audit config if present...");
			 InMemoryJAASConfiguration.init(props);
			} catch (Exception e) {
				LOG.error("ERROR: Unable to load SolrClient JAAS config from Audit config file. Audit to Kerberized Solr will fail...", e);
		}
        finally {
			 String confFileName = System.getProperty(PROP_JAVA_SECURITY_AUTH_LOGIN_CONFIG);
			 LOG.info("In solrAuditDestination.init() (finally) : JAAS Configuration set as [" + confFileName + "]");
		}
		LOG.info("<==SolrAuditDestination.init()" );
	}
}
