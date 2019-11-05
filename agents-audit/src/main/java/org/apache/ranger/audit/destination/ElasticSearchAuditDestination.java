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
import org.apache.http.HttpHost;
import org.apache.ranger.audit.model.AuditEventBase;
import org.apache.ranger.audit.model.AuthzAuditEvent;
import org.apache.ranger.audit.provider.MiscUtil;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;

import java.util.*;


public class ElasticSearchAuditDestination extends AuditDestination {
    private static final Log LOG = LogFactory.getLog(ElasticSearchAuditDestination.class);

    public static final String ELASTICSEARCH_URLS = "urls";
    public static final String ELASTICSEARCH_PORT = "port";
    public static final String ELASTICSEARCH_PROTOCOL = "protocol";

    public ElasticSearchAuditDestination() {
    }

    private volatile RestHighLevelClient client = null;

    @Override
    public void init(Properties props, String propPrefix) {
        LOG.info("init() called");
        super.init(props, propPrefix);
        connect();
    }

    @Override
    public void stop() {
        super.stop();
        logStatus();
    }

    @Override
    public boolean log(Collection<AuditEventBase> events) {
        boolean ret = false;
		try {
			logStatusIfRequired();
			addTotalCount(events.size());

            if (client == null) {
				connect();
				if (client == null) {
					// Solr is still not initialized. So need return error
					addDeferredCount(events.size());
					return ret;
				}
			}

			final Collection<Map<String, Object>> docs = new ArrayList<Map<String, Object>>();
			for (AuditEventBase event : events) {
				AuthzAuditEvent authzEvent = (AuthzAuditEvent) event;
				docs.add(toDoc(authzEvent));
			}
			try {
                IndexRequest indexRequest = new IndexRequest("posts").id("1").source(docs.toArray());
                IndexResponse response = client.index(indexRequest, RequestOptions.DEFAULT);

				if (response.status().getStatus() >= 400) {
					addFailedCount(events.size());
					logFailedEvent(events, response.toString());
				} else {
					addSuccessCount(events.size());
					ret = true;
				}
			} catch (Exception ex) {
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

    public boolean isAsync() {
        return true;
    }

    synchronized void connect() {
        RestHighLevelClient me = client;
        if (me == null) {
            synchronized (ElasticSearchAuditDestination.class) {
                me = client;
                if (client == null) {
                    String urls = MiscUtil.getStringProperty(props, propPrefix + "." + ELASTICSEARCH_URLS);
                    String protocol = getStringProperty(props, propPrefix + "." + ELASTICSEARCH_PROTOCOL, "http");
                    int port = MiscUtil.getIntProperty(props, propPrefix + "." + ELASTICSEARCH_PORT, 8080);
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
                        LOG.fatal("Can't connect to Solr server. urls=" + urls, t);
                    }
                }
            }
        }
    }

    private String getStringProperty(Properties props, String propName, String defaultValue) {
        String value = MiscUtil.getStringProperty(props, propName);
        if (null == value) return defaultValue;
        return value;
    }

    Map<String, Object> toDoc(AuthzAuditEvent auditEvent) {
        Map<String, Object> doc = new HashMap<String, Object>();
		doc.put("id", auditEvent.getEventId());
		doc.put("access", auditEvent.getAccessType());
		doc.put("enforcer", auditEvent.getAclEnforcer());
		doc.put("agent", auditEvent.getAgentId());
		doc.put("repo", auditEvent.getRepositoryName());
		doc.put("sess", auditEvent.getSessionId());
		doc.put("reqUser", auditEvent.getUser());
		doc.put("reqData", auditEvent.getRequestData());
		doc.put("resource", auditEvent.getResourcePath());
		doc.put("cliIP", auditEvent.getClientIP());
		doc.put("logType", auditEvent.getLogType());
		doc.put("result", auditEvent.getAccessResult());
		doc.put("policy", auditEvent.getPolicyId());
		doc.put("repoType", auditEvent.getRepositoryType());
		doc.put("resType", auditEvent.getResourceType());
		doc.put("reason", auditEvent.getResultReason());
		doc.put("action", auditEvent.getAction());
		doc.put("evtTime", auditEvent.getEventTime());
		doc.put("seq_num", auditEvent.getSeqNum());
		doc.put("event_count", auditEvent.getEventCount());
		doc.put("event_dur_ms", auditEvent.getEventDurationMS());
		doc.put("tags", auditEvent.getTags());
		doc.put("cluster", auditEvent.getClusterName());
		doc.put("zoneName", auditEvent.getZoneName());
		doc.put("agentHost", auditEvent.getAgentHostname());
		doc.put("policyVersion", auditEvent.getPolicyVersion());
		return doc;
	}
	
}
