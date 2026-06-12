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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.nio.entity.NStringEntity;
import org.apache.http.util.EntityUtils;
import org.apache.ranger.audit.model.AuditEventBase;
import org.apache.ranger.audit.model.AuthzAuditEvent;
import org.apache.ranger.audit.provider.MiscUtil;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.TimeZone;
import java.util.UUID;

public class OpenSearchAuditDestination extends AuditDestination {
    private static final Logger LOG = LoggerFactory.getLogger(OpenSearchAuditDestination.class);

    public static final String CONFIG_PREFIX   = "ranger.audit.opensearch";
    public static final String CONFIG_URLS     = "urls";
    public static final String CONFIG_PORT     = "port";
    public static final String CONFIG_USER     = "user";
    public static final String CONFIG_PASSWORD = "password";
    public static final String CONFIG_PROTOCOL = "protocol";
    public static final String CONFIG_INDEX    = "index";
    public static final String DEFAULT_INDEX   = "ranger_audits";

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final ThreadLocal<SimpleDateFormat> DATE_FORMAT = ThreadLocal.withInitial(() -> {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
        sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
        return sdf;
    });

    private volatile RestClient client;
    private String index;
    private String user;
    private String password;
    private String protocol;
    private String urls;
    private int    port;

    public OpenSearchAuditDestination() {
        propPrefix = CONFIG_PREFIX;
    }

    @Override
    public void init(Properties props, String propPrefix) {
        super.init(props, propPrefix);

        this.urls     = MiscUtil.getStringProperty(props, propPrefix + "." + CONFIG_URLS, "localhost");
        this.port     = MiscUtil.getIntProperty(props, propPrefix + "." + CONFIG_PORT, 9200);
        this.protocol = MiscUtil.getStringProperty(props, propPrefix + "." + CONFIG_PROTOCOL, "http");
        this.user     = MiscUtil.getStringProperty(props, propPrefix + "." + CONFIG_USER, "");
        this.password = MiscUtil.getStringProperty(props, propPrefix + "." + CONFIG_PASSWORD, "");
        this.index    = MiscUtil.getStringProperty(props, propPrefix + "." + CONFIG_INDEX, DEFAULT_INDEX);

        LOG.info("OpenSearchAuditDestination.init(): urls={}, port={}, index={}", urls, port, index);

        getClient();
    }

    @Override
    public void stop() {
        logStatus();

        if (client != null) {
            try {
                client.close();
            } catch (Exception e) {
                LOG.error("Error closing OpenSearch client", e);
            }
        }
    }

    @Override
    public void flush() {
    }

    @Override
    public boolean log(Collection<AuditEventBase> events) {
        if (events == null || events.isEmpty()) {
            return true;
        }

        RestClient currentClient = getClient();

        if (currentClient == null) {
            LOG.error("OpenSearch client is null. Cannot write audit events.");
            return false;
        }

        try {
            StringBuilder bulk = new StringBuilder();

            for (AuditEventBase event : events) {
                AuthzAuditEvent auditEvent = (AuthzAuditEvent) event;
                Map<String, Object> doc = toDoc(auditEvent);
                String id = (String) doc.get("id");

                if (StringUtils.isBlank(id)) {
                    id = UUID.randomUUID().toString();
                    doc.put("id", id);
                }

                Map<String, Object> indexProps = new HashMap<>();
                indexProps.put("_index", index);
                indexProps.put("_id", id);

                bulk.append(MAPPER.writeValueAsString(Map.of("index", indexProps))).append('\n');
                bulk.append(MAPPER.writeValueAsString(doc)).append('\n');
            }

            Request request = new Request("POST", "/_bulk");
            request.setEntity(new NStringEntity(bulk.toString(), ContentType.create("application/x-ndjson", StandardCharsets.UTF_8)));

            Response response = currentClient.performRequest(request);

            if (response.getStatusLine().getStatusCode() >= 400) {
                LOG.error("OpenSearch bulk request failed: HTTP {}", response.getStatusLine().getStatusCode());
                return false;
            }

            String responseBody = EntityUtils.toString(response.getEntity());
            @SuppressWarnings("unchecked")
            Map<String, Object> responseMap = MAPPER.readValue(responseBody, Map.class);

            if (Boolean.TRUE.equals(responseMap.get("errors"))) {
                LOG.error("OpenSearch bulk response contains item-level errors");
                return false;
            }

            addSuccessCount(events.size());
            return true;
        } catch (Exception e) {
            addFailedCount(events.size());
            LOG.error("Failed to write audit events to OpenSearch", e);
            return false;
        }
    }

    public boolean isAsync() {
        return true;
    }

    synchronized RestClient getClient() {
        if (client == null) {
            if (StringUtils.isBlank(urls) || "NONE".equalsIgnoreCase(urls)) {
                LOG.error("OpenSearch URLs not configured");
                return null;
            }

            HttpHost[] hosts = Arrays.stream(urls.split(",")).map(String::trim).filter(h -> !h.isEmpty()).map(h -> new HttpHost(h, port, protocol)).toArray(HttpHost[]::new);
            RestClientBuilder builder = RestClient.builder(hosts);

            if (StringUtils.isNotBlank(user) && StringUtils.isNotBlank(password) && !"NONE".equalsIgnoreCase(user) && !"NONE".equalsIgnoreCase(password)) {
                CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
                credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(user, password));
                builder.setHttpClientConfigCallback(httpClientBuilder -> httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider));
            }

            client = builder.build();
        }

        return client;
    }

    Map<String, Object> toDoc(AuthzAuditEvent event) {
        Map<String, Object> doc = new HashMap<>();

        doc.put("id", event.getEventId());
        doc.put("access", event.getAccessType());
        doc.put("enforcer", event.getAclEnforcer());
        doc.put("agent", event.getAgentId());
        doc.put("repo", event.getRepositoryName());
        doc.put("sess", event.getSessionId());
        doc.put("reqUser", event.getUser());
        doc.put("reqData", event.getRequestData());
        doc.put("resource", event.getResourcePath());
        doc.put("cliIP", event.getClientIP());
        doc.put("logType", event.getLogType());
        doc.put("result", event.getAccessResult());
        doc.put("policy", event.getPolicyId());
        doc.put("repoType", event.getRepositoryType());
        doc.put("resType", event.getResourceType());
        doc.put("reason", event.getResultReason());
        doc.put("action", event.getAction());
        doc.put("evtTime", formatDate(event.getEventTime()));
        doc.put("seq_num", event.getSeqNum());
        doc.put("event_count", event.getEventCount());
        doc.put("event_dur_ms", event.getEventDurationMS());
        doc.put("tags", event.getTags());
        doc.put("datasets", event.getDatasets());
        doc.put("projects", event.getProjects());
        doc.put("cluster", event.getClusterName());
        doc.put("zoneName", event.getZoneName());
        doc.put("agentHost", event.getAgentHostname());
        doc.put("policyVersion", event.getPolicyVersion());

        return doc;
    }

    private static String formatDate(Date date) {
        return date != null ? DATE_FORMAT.get().format(date) : null;
    }
}
