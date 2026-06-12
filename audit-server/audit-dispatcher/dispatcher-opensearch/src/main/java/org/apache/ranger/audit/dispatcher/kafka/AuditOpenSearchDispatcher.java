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

package org.apache.ranger.audit.dispatcher.kafka;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpHost;
import org.apache.http.HttpStatus;
import org.apache.http.auth.AuthSchemeProvider;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.config.AuthSchemes;
import org.apache.http.config.Lookup;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.auth.SPNegoSchemeFactory;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.nio.entity.NStringEntity;
import org.apache.http.util.EntityUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.ranger.audit.dispatcher.AuditEventOpenSearchDocMapper;
import org.apache.ranger.audit.model.AuthzAuditEvent;
import org.apache.ranger.audit.provider.MiscUtil;
import org.apache.ranger.audit.server.AuditServerConstants;
import org.apache.ranger.audit.utils.AuditServerLogFormatter;
import org.apache.ranger.authorization.credutils.CredentialsProviderUtil;
import org.apache.ranger.authorization.credutils.kerberos.KerberosCredentialsProvider;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.stream.Collectors;

public class AuditOpenSearchDispatcher extends AuditDispatcherBase {
    private static final Logger LOG = LoggerFactory.getLogger(AuditOpenSearchDispatcher.class);
    private static final String DEFAULT_GROUP = "ranger_audit_opensearch_dispatcher_group";
    private static final String DEFAULT_INDEX = "ranger_audits";
    private static final long   RETRY_SLEEP_MS = 5000L;
    private static final int    DEFAULT_PORT   = 9200;
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final TypeReference<Map<String, Object>> MAP_TYPE = new TypeReference<Map<String, Object>>() { };

    private RestClient openSearchClient;
    private String     openSearchIndex;

    public AuditOpenSearchDispatcher(final Properties props, final String propPrefix) throws Exception {
        super(props, propPrefix, DEFAULT_GROUP);
        init(props, propPrefix);
    }

    @Override
    protected final String getDispatcherName() {
        return "OPENSEARCH";
    }

    @Override
    protected final DispatcherWorker createDispatcherWorker(final String workerId, final List<Integer> assignedPartitions) {
        return new OpenSearchDispatcherWorker(workerId, assignedPartitions);
    }

    @Override
    protected final void shutdownDestination() {
        if (openSearchClient != null) {
            try {
                openSearchClient.close();
            } catch (Exception e) {
                LOG.error("Error shutting down OpenSearch REST client", e);
            }
        }
    }

    private void init(final Properties props, final String propPrefix) throws Exception {
        LOG.info("==> AuditOpenSearchDispatcher.init()");

        String pfx = propPrefix + ".";

        this.openSearchIndex = MiscUtil.getStringProperty(props, pfx + "index", DEFAULT_INDEX);
        this.openSearchClient = createOpenSearchClient(props, pfx);

        this.dispatcherThreadCount = MiscUtil.getIntProperty(props, pfx + AuditServerConstants.PROP_DISPATCHER_THREAD_COUNT, 1);
        this.offsetCommitStrategy = MiscUtil.getStringProperty(props, pfx + AuditServerConstants.PROP_DISPATCHER_OFFSET_COMMIT_STRATEGY, AuditServerConstants.DEFAULT_OFFSET_COMMIT_STRATEGY);
        this.offsetCommitInterval = MiscUtil.getLongProperty(props, pfx + AuditServerConstants.PROP_DISPATCHER_OFFSET_COMMIT_INTERVAL, AuditServerConstants.DEFAULT_OFFSET_COMMIT_INTERVAL_MS);

        AuditServerLogFormatter.builder("AuditOpenSearchDispatcher Configuration")
                .add("Index", openSearchIndex)
                .add("Thread Count", dispatcherThreadCount)
                .add("Commit Strategy", offsetCommitStrategy)
                .add("Commit Interval (ms)", offsetCommitInterval + " (manual mode only)")
                .logInfo(LOG);

        LOG.info("<== AuditOpenSearchDispatcher.init()");
    }

    private void processMessageBatch(final Collection<String> audits) throws Exception {
        if (audits == null || audits.isEmpty()) {
            throw new Exception("Failure in sending audits into OpenSearch");
        }

        StringBuilder bulkBody = new StringBuilder();

        for (String audit : audits) {
            AuthzAuditEvent auditEvent = MiscUtil.fromJson(audit, AuthzAuditEvent.class);
            String id = auditEvent.getEventId();
            Map<String, Object> doc = AuditEventOpenSearchDocMapper.toDoc(auditEvent);

            if (id == null || id.trim().isEmpty()) {
                id = UUID.randomUUID().toString();
                doc.put("id", id);
            }

            Map<String, Object> indexProperties = new HashMap<>();
            indexProperties.put("_index", openSearchIndex);
            indexProperties.put("_id", id);

            Map<String, Object> indexMeta = Collections.singletonMap("index", indexProperties);
            bulkBody.append(OBJECT_MAPPER.writeValueAsString(indexMeta)).append('\n')
                .append(OBJECT_MAPPER.writeValueAsString(doc)).append('\n');
        }

        Request request = new Request("POST", "/_bulk");
        request.setEntity(new NStringEntity(bulkBody.toString(), ContentType.create("application/x-ndjson", StandardCharsets.UTF_8)));

        Response response = openSearchClient.performRequest(request);
        int status = response.getStatusLine().getStatusCode();

        if (status >= HttpStatus.SC_BAD_REQUEST) {
            throw new Exception("OpenSearch bulk request failed with HTTP " + status);
        }

        String responseBody = response.getEntity() != null ? EntityUtils.toString(response.getEntity()) : "{}";
        Map<String, Object> responseMap = OBJECT_MAPPER.readValue(responseBody, MAP_TYPE);
        Object hasErrors = responseMap.get("errors");

        if (Boolean.TRUE.equals(hasErrors)) {
            throw new Exception("OpenSearch bulk request returned item errors: " + responseBody);
        }
    }

    private RestClient createOpenSearchClient(final Properties props, final String pfx) {
        String    protocol = MiscUtil.getStringProperty(props, pfx + "protocol", "http");
        String    urls     = MiscUtil.getStringProperty(props, pfx + "urls", "localhost");
        int       port     = MiscUtil.getIntProperty(props, pfx + "port", DEFAULT_PORT);
        String    user     = MiscUtil.getStringProperty(props, pfx + "user", "");
        String    password = MiscUtil.getStringProperty(props, pfx + "password", "");
        HttpHost[] hosts   = MiscUtil.toArray(urls, ",").stream().map(h -> new HttpHost(h, port, protocol)).toArray(HttpHost[]::new);

        LOG.info("Connecting to OpenSearch: {}://{}:{}/{}", protocol, urls, port, openSearchIndex);

        RestClientBuilder builder = RestClient.builder(hosts);

        if (isCredentialConfigured(user) && isCredentialConfigured(password)) {
            if (password.contains("keytab") && new File(password).exists()) {
                KerberosCredentialsProvider creds = CredentialsProviderUtil.getKerberosCredentials(user, password);
                Lookup<AuthSchemeProvider> authRegistry = RegistryBuilder.<AuthSchemeProvider>create()
                        .register(AuthSchemes.SPNEGO, new SPNegoSchemeFactory())
                        .build();
                builder.setHttpClientConfigCallback(httpClientBuilder -> httpClientBuilder
                        .setDefaultCredentialsProvider(creds)
                        .setDefaultAuthSchemeRegistry(authRegistry));
                LOG.info("OpenSearch client configured with Kerberos credentials for user: {}", user);
            } else {
                CredentialsProvider creds = new BasicCredentialsProvider();
                creds.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(user, password));
                builder.setHttpClientConfigCallback(httpClientBuilder -> httpClientBuilder.setDefaultCredentialsProvider(creds));
                LOG.info("OpenSearch client configured with basic auth for user: {}", user);
            }
        }

        return builder.build();
    }

    private boolean isCredentialConfigured(final String value) {
        return StringUtils.isNotBlank(value) && !"NONE".equalsIgnoreCase(value.trim());
    }

    private class OpenSearchDispatcherWorker extends DispatcherWorker {
        OpenSearchDispatcherWorker(final String workerId, final List<Integer> assignedPartitions) {
            super(workerId, assignedPartitions);
        }

        @Override
        protected void processRecordBatch(final ConsumerRecords<String, String> records) {
            for (TopicPartition tp : records.partitions()) {
                List<ConsumerRecord<String, String>> tpRecords = records.records(tp);

                if (tpRecords.isEmpty()) {
                    continue;
                }

                try {
                    List<String> auditBatch = tpRecords.stream().map(ConsumerRecord::value).collect(Collectors.toList());
                    processMessageBatch(auditBatch);

                    ConsumerRecord<String, String> last = tpRecords.get(tpRecords.size() - 1);
                    pendingOffsets.put(tp, new OffsetAndMetadata(last.offset() + 1));
                    messagesProcessedSinceLastCommit.addAndGet(tpRecords.size());
                } catch (Exception e) {
                    LOG.error("Error processing batch in worker '{}', partition={}, batch size: {}", workerId, tp, tpRecords.size(), e);

                    ConsumerRecord<String, String> first = tpRecords.get(0);
                    pendingOffsets.put(tp, new OffsetAndMetadata(first.offset()));

                    try {
                        workerDispatcher.seek(tp, first.offset());
                    } catch (Exception seekEx) {
                        LOG.error("Failed to seek to offset {} for partition {} after OpenSearch batch error", first.offset(), tp, seekEx);
                    }

                    try {
                        Thread.sleep(RETRY_SLEEP_MS);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                    }
                }
            }
        }
    }
}
