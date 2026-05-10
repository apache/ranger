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

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.ranger.audit.destination.SolrAuditDestination;
import org.apache.ranger.audit.provider.AuditProviderFactory;
import org.apache.ranger.audit.provider.MiscUtil;
import org.apache.ranger.audit.server.AuditServerConstants;
import org.apache.ranger.audit.utils.AuditServerLogFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Solr dispatcher that writes audits into Solr index using rangerauditserver user
 */
public class AuditSolrDispatcher extends AuditDispatcherBase {
    private static final Logger LOG = LoggerFactory.getLogger(AuditSolrDispatcher.class);

    private static final String DEFAULT_RANGER_AUDIT_SOLR_DISPATCHER_GROUP = "ranger_audit_solr_dispatcher_group";
    private static final String PROP_SOLR_DEST_PREFIX                      = "solr";

    private final SolrAuditDestination          solrAuditDestination;

    public AuditSolrDispatcher(Properties props, String propPrefix) throws Exception {
        super(props, propPrefix, DEFAULT_RANGER_AUDIT_SOLR_DISPATCHER_GROUP);

        this.solrAuditDestination = new SolrAuditDestination();

        init(props, propPrefix);
    }

    @Override
    protected String getDispatcherName() {
        return "SOLR";
    }

    @Override
    protected DispatcherWorker createDispatcherWorker(String workerId, List<Integer> assignedPartitions) {
        return new SolrDispatcherWorker(workerId, assignedPartitions);
    }

    @Override
    protected void shutdownDestination() {
        if (solrAuditDestination != null) {
            try {
                solrAuditDestination.stop();
            } catch (Exception e) {
                LOG.error("Error shutting down Solr destination handler", e);
            }
        }
    }

    private void init(Properties props, String propPrefix) throws Exception {
        LOG.info("==> AuditSolrDispatcher.init()");

        solrAuditDestination.init(props, AuditProviderFactory.AUDIT_DEST_BASE + "." + PROP_SOLR_DEST_PREFIX);

        this.dispatcherThreadCount = MiscUtil.getIntProperty(props, propPrefix + "." + AuditServerConstants.PROP_DISPATCHER_THREAD_COUNT, 1);

        LOG.info("Dispatcher thread count: {}", dispatcherThreadCount);

        this.offsetCommitStrategy = MiscUtil.getStringProperty(props, propPrefix + "." + AuditServerConstants.PROP_DISPATCHER_OFFSET_COMMIT_STRATEGY, AuditServerConstants.DEFAULT_OFFSET_COMMIT_STRATEGY);

        // Get offset commit interval (only used for manual strategy)
        this.offsetCommitInterval = MiscUtil.getLongProperty(props, propPrefix + "." + AuditServerConstants.PROP_DISPATCHER_OFFSET_COMMIT_INTERVAL, AuditServerConstants.DEFAULT_OFFSET_COMMIT_INTERVAL_MS);

        AuditServerLogFormatter.builder("AuditSolrDispatcher Offset Management Configuration")
                .add("Commit Strategy", offsetCommitStrategy)
                .add("Commit Interval (ms)", offsetCommitInterval + " (used in manual mode only)")
                .logInfo(LOG);

        LOG.info("<== AuditSolrDispatcher.init()");
    }

    /**
     * Process a batch of audit messages.
     * This method leverages SolrAuditDestination's batch processing capability
     * to send multiple audits to Solr in a single request, improving performance.
     *
     * @param audits Collection of audit messages in JSON format
     * @throws Exception if batch processing fails
     */
    public void processMessageBatch(Collection<String> audits) throws Exception {
        boolean processed = audits != null && !audits.isEmpty() && solrAuditDestination.logJSON(audits);

        if (!processed) {
            throw new Exception("Failure in sending audits into Solr");
        }
    }

    private class SolrDispatcherWorker extends DispatcherWorker {
        public SolrDispatcherWorker(String workerId, List<Integer> assignedPartitions) {
            super(workerId, assignedPartitions);
        }

        /**
         * Process a batch of records.
         * In generic worker mode, processes messages from ANY appId.
         * Uses batch processing to send all records to Solr in one request for efficiency.
         */
        @Override
        protected void processRecordBatch(ConsumerRecords<String, String> records) {
            // Collect all audit messages for batch processing
            List<String> auditBatch = new ArrayList<>();
            List<ConsumerRecord<String, String>> recordList = new ArrayList<>();

            for (ConsumerRecord<String, String> record : records) {
                LOG.debug("Worker '{}' consumed: partition={}, key={}, offset={}",
                        workerId, record.partition(), record.key(), record.offset());

                auditBatch.add(record.value());
                recordList.add(record);
            }

            // Process entire batch at once
            try {
                if (!auditBatch.isEmpty()) {
                    processMessageBatch(auditBatch);

                    // Track offsets for all successfully processed messages
                    for (ConsumerRecord<String, String> record : recordList) {
                        TopicPartition partition = new TopicPartition(record.topic(), record.partition());
                        pendingOffsets.put(partition, new OffsetAndMetadata(record.offset() + 1));
                        messagesProcessedSinceLastCommit.incrementAndGet();
                    }
                }
            } catch (Exception e) {
                LOG.error("Error processing batch in worker '{}', batch size: {}",
                        workerId, auditBatch.size(), e);

                // On batch error, track offsets up to the first message to avoid reprocessing
                // We use seek to ensure Kafka re-delivers this exact batch
                if (!recordList.isEmpty()) {
                    // Because records could span multiple partitions, we must track the first
                    // offset and issue a seek for EVERY partition present in this batch.
                    Map<TopicPartition, Long> firstOffsets = new HashMap<>();

                    for (ConsumerRecord<String, String> record : recordList) {
                        TopicPartition partition = new TopicPartition(record.topic(), record.partition());
                        firstOffsets.putIfAbsent(partition, record.offset());
                    }

                    for (Map.Entry<TopicPartition, Long> entry : firstOffsets.entrySet()) {
                        TopicPartition partition = entry.getKey();
                        Long firstOffset = entry.getValue();

                        pendingOffsets.put(partition, new OffsetAndMetadata(firstOffset));

                        try {
                            workerDispatcher.seek(partition, firstOffset);
                        } catch (Exception seekEx) {
                            LOG.error("Failed to seek to offset {} for partition {} after Solr batch error", firstOffset, partition, seekEx);
                        }
                    }

                    // Add sleep to prevent frequent polling when Solr is completely down
                    try {
                        Thread.sleep(5000);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                    }
                }
            }
        }
    }
}
