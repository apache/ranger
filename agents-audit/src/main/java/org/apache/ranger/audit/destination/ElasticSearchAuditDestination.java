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
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.ranger.audit.model.AuditEventBase;
import org.apache.ranger.audit.model.AuthzAuditEvent;
import org.apache.ranger.audit.provider.MiscUtil;
import org.elasticsearch.action.admin.indices.open.OpenIndexRequest;
import org.elasticsearch.action.admin.indices.open.OpenIndexResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;


public class ElasticSearchAuditDestination extends AuditDestination {
    private static final Log LOG = LogFactory.getLog(ElasticSearchAuditDestination.class);

    public static final String CONFIG_URLS = "urls";
    public static final String CONFIG_PORT = "port";
    public static final String CONFIG_USER = "user";
    public static final String CONFIG_PWRD = "password";
    public static final String CONFIG_PROTOCOL = "protocol";
    public static final String CONFIG_INDEX = "index";
    public static final String CONFIG_PREFIX = "ranger.audit.elasticsearch";
    public static final String DEFAULT_INDEX = "audit";

    private String index = "index";
    private volatile RestHighLevelClient client = null;

    public ElasticSearchAuditDestination() {
        propPrefix = CONFIG_PREFIX;
    }


    @Override
    public void init(Properties props, String propPrefix) {
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
                    // ElasticSearch is still not initialized. So need return error
                    addDeferredCount(events.size());
                    return ret;
                }
            }

            ArrayList<AuditEventBase> eventList = new ArrayList<>(events);
            BulkRequest bulkRequest = new BulkRequest();
            try {
                for (AuditEventBase event : eventList) {
                    AuthzAuditEvent authzEvent = (AuthzAuditEvent) event;
                    String id = authzEvent.getEventId();
                    Map<String, Object> doc = toDoc(authzEvent);
                    bulkRequest.add(new IndexRequest(index).id(id).source(doc).type(""));
                }
            } catch (Exception ex) {
                addFailedCount(eventList.size());
                logFailedEvent(eventList, ex);
            }
            BulkResponse response = client.bulk(bulkRequest);
            if (response.status().getStatus() >= 400) {
                addFailedCount(eventList.size());
                logFailedEvent(eventList, "HTTP " + response.status().getStatus());
            } else {
                BulkItemResponse[] items = response.getItems();
                for (int i = 0; i < items.length; i++) {
                    AuditEventBase itemRequest = eventList.get(i);
                    BulkItemResponse itemResponse = items[i];
                    if (itemResponse.isFailed()) {
                        addFailedCount(1);
                        logFailedEvent(Arrays.asList(itemRequest), itemResponse.getFailureMessage());
                    } else {
                        LOG.debug(String.format("Indexed %s", itemRequest.getEventKey()));
                        addSuccessCount(1);
                        ret = true;
                    }
                }
            }
        } catch (Throwable t) {
            addDeferredCount(events.size());
            logError("Error sending message to ElasticSearch", t);
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
        if (client == null) {
            synchronized (ElasticSearchAuditDestination.class) {
                if (client == null) {
                    client = newClient();
                }
            }
        }
    }

    private final AtomicLong lastLoggedAt = new AtomicLong(0);

    private RestHighLevelClient newClient() {
        String protocol = getStringProperty(props, propPrefix + "." + CONFIG_PROTOCOL, "http");
        String user = getStringProperty(props, propPrefix + "." + CONFIG_USER, "");
        String password = getStringProperty(props, propPrefix + "." + CONFIG_PWRD, "");
        int port = MiscUtil.getIntProperty(props, propPrefix + "." + CONFIG_PORT, 9200);
        this.index = getStringProperty(props, propPrefix + "." + CONFIG_INDEX, DEFAULT_INDEX);
        String urls = getUrls();
        try {
            final CredentialsProvider credentialsProvider;
            if(!user.isEmpty()) {
                credentialsProvider = new BasicCredentialsProvider();
                credentialsProvider.setCredentials(AuthScope.ANY,
                        new UsernamePasswordCredentials(user, password));
            } else {
                credentialsProvider = null;
            }
            RestHighLevelClient restHighLevelClient = new RestHighLevelClient(
                    RestClient.builder(
                            MiscUtil.toArray(urls, ",").stream()
                                    .map(x -> new HttpHost(x, port, protocol))
                                    .<HttpHost>toArray(i -> new HttpHost[i])
                    ).setHttpClientConfigCallback(clientBuilder ->
                            (credentialsProvider != null) ? clientBuilder.setDefaultCredentialsProvider(credentialsProvider) : clientBuilder));
            LOG.debug("Initialized client");
            boolean exits = false;
            try {
                exits = restHighLevelClient.indices().open(new OpenIndexRequest(this.index)).isShardsAcknowledged();
            } catch (Exception e) {
                LOG.warn("Error validating index " + this.index);
            }
            if(exits) {
                LOG.debug("Index exists");
            } else {
                LOG.info("Index does not exist");
            }
            return restHighLevelClient;
        } catch (Throwable t) {
            lastLoggedAt.updateAndGet(lastLoggedAt -> {
                long now = System.currentTimeMillis();
                long elapsed = now - lastLoggedAt;
                if (elapsed > TimeUnit.MINUTES.toMillis(1)) {
                    LOG.fatal("Can't connect to ElasticSearch server. urls=" + urls, t);
                    return now;
                } else {
                    return lastLoggedAt;
                }
            });
            return null;
        }
    }

    private String getUrls() {
        String urls = MiscUtil.getStringProperty(props, propPrefix + "." + CONFIG_URLS);
        if (urls != null) {
            urls = urls.trim();
        }
        if (urls != null && urls.equalsIgnoreCase("NONE")) {
            urls = null;
        }
        return urls;
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
