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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ranger.audit.destination.AuditDestination;
import org.apache.ranger.audit.model.AuditEventBase;
import org.apache.ranger.audit.model.AuthzAuditEvent;
import org.apache.ranger.audit.provider.MiscUtil;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.LBHttpSolrClient;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;

public class SolrAuditDestination extends AuditDestination {
	private static final Log LOG = LogFactory
			.getLog(SolrAuditDestination.class);

	public static final String PROP_SOLR_URLS = "urls";
	public static final String PROP_SOLR_ZK = "zookeepers";

	SolrClient solrClient = null;

	public SolrAuditDestination() {
	}

	@Override
	public void init(Properties props, String propPrefix) {
		LOG.info("init() called");
		super.init(props, propPrefix);
		connect();
	}

	synchronized void connect() {
		if (solrClient == null) {
			if (solrClient == null) {
				String urls = MiscUtil.getStringProperty(props, propPrefix
						+ "." + PROP_SOLR_URLS);
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

				try {
					if (zkHosts != null && !zkHosts.isEmpty()) {
						// Instantiate
						solrClient = new CloudSolrClient(zkHosts);
					} else if (solrURLs != null && !solrURLs.isEmpty()) {
						LBHttpSolrClient lbSolrClient = new LBHttpSolrClient(
								solrURLs.get(0));
						lbSolrClient.setConnectionTimeout(1000);

						for (int i = 1; i < solrURLs.size(); i++) {
							lbSolrClient.addSolrServer(solrURLs.get(i));
						}
						solrClient = lbSolrClient;
					}
				} catch (Throwable t) {
					LOG.fatal("Can't connect to Solr server. URL=" + solrURLs,
							t);
				}
			}
		}
	}

	@Override
	public boolean log(Collection<AuditEventBase> events) {
		try {
			if (solrClient == null) {
				connect();
				if (solrClient == null) {
					// Solr is still not initialized. So need to throw error
					return false;
				}
			}

			Collection<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();
			for (AuditEventBase event : events) {
				AuthzAuditEvent authzEvent = (AuthzAuditEvent) event;
				// Convert AuditEventBase to Solr document
				SolrInputDocument document = toSolrDoc(authzEvent);
				docs.add(document);
			}
			try {
				UpdateResponse response = solrClient.add(docs);
				if (response.getStatus() != 0) {
					logFailedEvent(events, response.toString());
				}
			} catch (SolrException ex) {
				logFailedEvent(events, ex);
			}
		} catch (Throwable t) {
			logError("Error sending message to Solr", t);
			return false;
		}
		return true;
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

		return doc;
	}

	public boolean isAsync() {
		return true;
	}

}
