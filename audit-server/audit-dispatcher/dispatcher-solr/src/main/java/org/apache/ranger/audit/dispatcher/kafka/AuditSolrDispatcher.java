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
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.ranger.audit.destination.SolrAuditDestination;
import org.apache.ranger.audit.provider.AuditProviderFactory;
import org.apache.ranger.audit.provider.MiscUtil;
import org.apache.ranger.audit.server.AuditServerConstants;
import org.apache.ranger.audit.utils.AuditServerLogFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Solr dispatcher that writes audits into Solr index using rangerauditserver user
 */
public class AuditSolrDispatcher extends AuditDispatcherBase {
    private static final Logger LOG = LoggerFactory.getLogger(AuditSolrDispatcher.class);

    private static final String RANGER_AUDIT_SOLR_DISPATCHER_GROUP = AuditServerConstants.DEFAULT_RANGER_AUDIT_SOLR_DISPATCHER_GROUP;

    private final AtomicBoolean                 running              = new AtomicBoolean(false);
    private final Map<String, DispatcherWorker> dispatcherWorkers    = new ConcurrentHashMap<>();
    private final SolrAuditDestination          solrAuditDestination;

    private ExecutorService dispatcherThreadPool;
    private int             dispatcherThreadCount = 1;

    // Offset management configuration (batch or manual only supported)
    private String offsetCommitStrategy = AuditServerConstants.DEFAULT_OFFSET_COMMIT_STRATEGY;
    private long   offsetCommitInterval = AuditServerConstants.DEFAULT_OFFSET_COMMIT_INTERVAL_MS;

    public AuditSolrDispatcher(Properties props, String propPrefix) throws Exception {
        super(props, propPrefix, RANGER_AUDIT_SOLR_DISPATCHER_GROUP);

        this.solrAuditDestination = new SolrAuditDestination();

        init(props, propPrefix);
    }

    @Override
    public void run() {
        try {
            if (running.compareAndSet(false, true)) {
                startDispatcherWorkers();
            }

            // Keep main thread alive while dispatcher threads are running
            while (running.get()) {
                Thread.sleep(1000);
            }
        } catch (Throwable e) {
            LOG.error("Error in AuditSolrDispatcher", e);
        } finally {
            shutdown();
        }
    }

    @Override
    public void shutdown() {
        LOG.info("Shutting down AuditSolrDispatcher...");

        running.set(false);

        // Shutdown dispatcher workers
        if (dispatcherThreadPool != null) {
            dispatcherThreadPool.shutdownNow();

            try {
                if (!dispatcherThreadPool.awaitTermination(30, TimeUnit.SECONDS)) {
                    LOG.warn("Dispatcher thread pool did not terminate within 30 seconds");
                }
            } catch (InterruptedException e) {
                LOG.warn("Interrupted while waiting for dispatcher thread pool to terminate", e);
                Thread.currentThread().interrupt();
            }
        }

        // Close main dispatcher
        if (dispatcher != null) {
            try {
                dispatcher.close();
            } catch (Exception e) {
                LOG.error("Error closing main dispatcher", e);
            }
        }

        dispatcherWorkers.clear();

        LOG.info("AuditSolrDispatcher shutdown complete");
    }

    private void init(Properties props, String propPrefix) throws Exception {
        LOG.info("==> AuditSolrDispatcher.init()");

        solrAuditDestination.init(props, AuditProviderFactory.AUDIT_DEST_BASE + "." + AuditServerConstants.PROP_SOLR_DEST_PREFIX);

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

    /**
     * Start dispatcher workers for horizontal scaling.
     * Each worker subscribes to the topic and Kafka automatically assigns partitions.
     * Workers can process messages from ANY appId.
     */
    private void startDispatcherWorkers() {
        int workerCount = dispatcherThreadCount;

        LOG.info("==> AuditSolrDispatcher.startDispatcherWorkers(): Creating {} generic workers for horizontal scaling", workerCount);
        LOG.info("Each worker will subscribe to topic '{}' and process partitions assigned by Kafka", topicName);

        // Create thread pool sized for generic workers
        dispatcherThreadPool = Executors.newFixedThreadPool(workerCount);
        LOG.info("Created thread pool with {} threads for scalable SOLR consumption", workerCount);

        // Create generic workers (no appId pre-assignment)
        for (int i = 0; i < workerCount; i++) {
            String workerId = "solr-worker-" + i;
            // Pass empty list for partitions (Kafka will assign dynamically)
            DispatcherWorker worker = new DispatcherWorker(workerId, new ArrayList<>());
            dispatcherWorkers.put(workerId, worker);
            dispatcherThreadPool.submit(worker);

            LOG.info("Started SOLR dispatcher worker '{}' - will process ANY appId assigned by Kafka", workerId);
        }

        LOG.info("<== AuditSolrDispatcher.startDispatcherWorkers(): All {} workers started in SUBSCRIBE mode", workerCount);
    }

    private class DispatcherWorker implements Runnable {
        private final String workerId;
        private final List<Integer> assignedPartitions;
        private KafkaConsumer<String, String> workerDispatcher;

        // Offset management
        private final Map<TopicPartition, OffsetAndMetadata> pendingOffsets = new HashMap<>();
        private final AtomicLong lastCommitTime = new AtomicLong(System.currentTimeMillis());
        private final AtomicInteger messagesProcessedSinceLastCommit = new AtomicInteger(0);

        public DispatcherWorker(String workerId, List<Integer> assignedPartitions) {
            this.workerId = workerId;
            this.assignedPartitions = assignedPartitions;
        }

        @Override
        public void run() {
            try {
                // Create dispatcher for this worker with offset management configuration
                Properties workerDispatcherProps = new Properties();
                workerDispatcherProps.putAll(dispatcherProps);

                // Configure offset management based on strategy
                configureOffsetManagement(workerDispatcherProps);

                workerDispatcher = new KafkaConsumer<>(workerDispatcherProps);

                // Create re-balance listener
                AuditDispatcherRebalanceListener reBalanceListener = new AuditDispatcherRebalanceListener(
                        workerId,
                        AuditServerConstants.DESTINATION_SOLR,
                        topicName,
                        offsetCommitStrategy,
                        dispatcherGroupId,
                        workerDispatcher,
                        pendingOffsets,
                        messagesProcessedSinceLastCommit,
                        lastCommitTime,
                        assignedPartitions);

                // Subscribe to topic with re-balance listener and let kafka automatically assign partitions
                workerDispatcher.subscribe(Collections.singletonList(topicName), reBalanceListener);

                LOG.info("[SOLR-DISPATCHER] Worker '{}' subscribed successfully, waiting for partition assignment from Kafka", workerId);
                long threadId = Thread.currentThread().getId();
                String threadName = Thread.currentThread().getName();
                LOG.info("[SOLR-DISPATCHER-STARTUP] Worker '{}' [Thread-ID: {}, Thread-Name: '{}'] started | Topic: '{}' | Dispatcher-Group: {} | Mode: SUBSCRIBE",
                        workerId, threadId, threadName, topicName, dispatcherGroupId);

                // Consume messages
                while (running.get()) {
                    ConsumerRecords<String, String> records = workerDispatcher.poll(Duration.ofMillis(100));

                    if (!records.isEmpty()) {
                        processRecordBatch(records);

                        // Handle offset committing based on strategy
                        handleOffsetCommitting();
                    }
                }
            } catch (Throwable e) {
                LOG.error("Error in dispatcher worker '{}'", workerId, e);
            } finally {
                // Final offset commit before shutdown
                commitPendingOffsets(true);

                if (workerDispatcher != null) {
                    try {
                        // Unsubscribe before closing
                        LOG.info("Worker '{}': Unsubscribing from topic", workerId);
                        workerDispatcher.unsubscribe();
                    } catch (Exception e) {
                        LOG.warn("Worker '{}': Error during unsubscribe", workerId, e);
                    }

                    try {
                        LOG.info("Worker '{}': Closing dispatcher", workerId);
                        workerDispatcher.close();
                    } catch (Exception e) {
                        LOG.error("Error closing dispatcher for worker '{}'", workerId, e);
                    }
                }
                LOG.info("Dispatcher worker '{}' stopped", workerId);
            }
        }

        /**
         * Configure offset management for this worker's dispatcher (batch or manual)
         */
        private void configureOffsetManagement(Properties dispatcherProps) {
            // Always disable auto commit - only batch or manual strategies supported
            dispatcherProps.put("enable.auto.commit", "false");
            LOG.debug("Worker '{}' configured for manual offset commit with strategy: {}", workerId, offsetCommitStrategy);
        }

        /**
         * Process a batch of records.
         * In generic worker mode, processes messages from ANY appId.
         * Uses batch processing to send all records to Solr in one request for efficiency.
         */
        private void processRecordBatch(ConsumerRecords<String, String> records) {
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
                // (Note: This is a simplistic approach - could be enhanced to track last successful offset)
                if (!recordList.isEmpty()) {
                    ConsumerRecord<String, String> firstRecord = recordList.get(0);
                    TopicPartition partition = new TopicPartition(firstRecord.topic(), firstRecord.partition());
                    pendingOffsets.put(partition, new OffsetAndMetadata(firstRecord.offset()));
                }
            }
        }

        /**
         * Handle offset committing based on the configured strategy (batch or manual only)
         */
        private void handleOffsetCommitting() {
            boolean shouldCommit = false;
            long currentTime = System.currentTimeMillis();

            if (AuditServerConstants.PROP_OFFSET_COMMIT_STRATEGY_BATCH.equals(offsetCommitStrategy)) {
                // Commit after processing each batch
                shouldCommit = !pendingOffsets.isEmpty();
            } else if (AuditServerConstants.PROP_OFFSET_COMMIT_STRATEGY_MANUAL.equals(offsetCommitStrategy)) {
                // Commit based on time interval
                shouldCommit = (currentTime - lastCommitTime.get()) >= offsetCommitInterval && !pendingOffsets.isEmpty();
            }

            if (shouldCommit) {
                commitPendingOffsets(false);
            }
        }

        /**
         * Commit pending offsets
         */
        private void commitPendingOffsets(boolean isShutdown) {
            if (pendingOffsets.isEmpty()) {
                return;
            }

            try {
                workerDispatcher.commitSync(pendingOffsets);

                LOG.debug("Worker '{}' committed {} offsets, processed {} messages",
                        workerId, pendingOffsets.size(), messagesProcessedSinceLastCommit.get());

                // Clear committed offsets
                pendingOffsets.clear();
                lastCommitTime.set(System.currentTimeMillis());
                messagesProcessedSinceLastCommit.set(0);
            } catch (Exception e) {
                LOG.error("Error committing offsets in worker '{}': {}", workerId, pendingOffsets, e);

                if (isShutdown) {
                    // During shutdown, retry to avoid loss of any offsets
                    try {
                        Thread.sleep(1000);
                        workerDispatcher.commitSync(pendingOffsets);
                        LOG.info("Successfully committed offsets on retry during shutdown for worker '{}'", workerId);
                    } catch (Exception retryException) {
                        LOG.error("Failed to commit offsets even on retry during shutdown for worker '{}'", workerId, retryException);
                    }
                }
            }
        }
    }
}
