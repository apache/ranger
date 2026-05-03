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

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.ranger.audit.provider.MiscUtil;
import org.apache.ranger.audit.server.AuditServerConstants;
import org.apache.ranger.audit.utils.AuditMessageQueueUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public abstract class AuditDispatcherBase implements AuditDispatcher {
    private static final Logger LOG = LoggerFactory.getLogger(AuditDispatcherBase.class);

    public final Properties                    dispatcherProps = new Properties();
    public final KafkaConsumer<String, String> dispatcher;
    public final String                        topicName;
    public final String                        dispatcherGroupId;

    protected final AtomicBoolean                 running               = new AtomicBoolean(false);
    protected final Map<String, DispatcherWorker> dispatcherWorkers     = new ConcurrentHashMap<>();
    protected ExecutorService                     dispatcherThreadPool;
    protected int                                 dispatcherThreadCount = 1;
    protected String                              offsetCommitStrategy  = AuditServerConstants.DEFAULT_OFFSET_COMMIT_STRATEGY;
    protected long                                offsetCommitInterval  = AuditServerConstants.DEFAULT_OFFSET_COMMIT_INTERVAL_MS;

    public AuditDispatcherBase(Properties props, String propPrefix, String dispatcherGroupId) throws Exception {
        this.dispatcherGroupId = getDispatcherGroupId(props, propPrefix, dispatcherGroupId);

        dispatcherProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, MiscUtil.getStringProperty(props, propPrefix + "." + AuditServerConstants.PROP_BOOTSTRAP_SERVERS));
        dispatcherProps.put(ConsumerConfig.GROUP_ID_CONFIG, this.dispatcherGroupId);
        dispatcherProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, Class.forName("org.apache.kafka.common.serialization.StringDeserializer"));
        dispatcherProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, Class.forName("org.apache.kafka.common.serialization.StringDeserializer"));

        String securityProtocol = MiscUtil.getStringProperty(props, propPrefix + "." + AuditServerConstants.PROP_SECURITY_PROTOCOL, AuditServerConstants.DEFAULT_SECURITY_PROTOCOL);
        dispatcherProps.put(AdminClientConfig.SECURITY_PROTOCOL_CONFIG, securityProtocol);

        dispatcherProps.put("sasl.mechanism", MiscUtil.getStringProperty(props, propPrefix + "." + AuditServerConstants.PROP_SASL_MECHANISM, AuditServerConstants.DEFAULT_SASL_MECHANISM));
        dispatcherProps.put(AuditServerConstants.PROP_SASL_KERBEROS_SERVICE_NAME, AuditServerConstants.DEFAULT_SERVICE_NAME);

        if (securityProtocol.toUpperCase().contains(AuditServerConstants.PROP_SECURITY_PROTOCOL_VALUE)) {
            dispatcherProps.put(AuditServerConstants.PROP_SASL_JAAS_CONFIG, AuditMessageQueueUtils.getJAASConfig(props, propPrefix));
        }

        dispatcherProps.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, MiscUtil.getIntProperty(props, propPrefix + "." + AuditServerConstants.PROP_DISPATCHER_MAX_POLL_RECORDS, AuditServerConstants.DEFAULT_MAX_POLL_RECORDS));
        dispatcherProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // Configure re-balancing parameters for subscribe mode
        // These ensure stable dispatcher group behavior during horizontal scaling
        int sessionTimeoutMs    = MiscUtil.getIntProperty(props, propPrefix + "." + AuditServerConstants.PROP_DISPATCHER_SESSION_TIMEOUT_MS, AuditServerConstants.DEFAULT_SESSION_TIMEOUT_MS);
        int maxPollIntervalMs   = MiscUtil.getIntProperty(props, propPrefix + "." + AuditServerConstants.PROP_DISPATCHER_MAX_POLL_INTERVAL_MS, AuditServerConstants.DEFAULT_MAX_POLL_INTERVAL_MS);
        int heartbeatIntervalMs = MiscUtil.getIntProperty(props, propPrefix + "." + AuditServerConstants.PROP_DISPATCHER_HEARTBEAT_INTERVAL_MS, AuditServerConstants.DEFAULT_HEARTBEAT_INTERVAL_MS);

        dispatcherProps.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, sessionTimeoutMs);
        dispatcherProps.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, maxPollIntervalMs);
        dispatcherProps.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, heartbeatIntervalMs);

        // Configure partition assignment strategy
        String partitionAssignmentStrategy = MiscUtil.getStringProperty(props, propPrefix + "." + AuditServerConstants.PROP_DISPATCHER_PARTITION_ASSIGNMENT_STRATEGY, AuditServerConstants.DEFAULT_PARTITION_ASSIGNMENT_STRATEGY);
        dispatcherProps.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, partitionAssignmentStrategy);

        LOG.info("Dispatcher '{}' configured for subscription-based partition assignment with re-balancing support", this.dispatcherGroupId);
        LOG.info("Re-balancing config - session.timeout.ms: {}, max.poll.interval.ms: {}, heartbeat.interval.ms: {}", sessionTimeoutMs, maxPollIntervalMs, heartbeatIntervalMs);
        LOG.info("Partition assignment strategy: {}", partitionAssignmentStrategy);

        dispatcher = new KafkaConsumer<>(dispatcherProps);
        topicName  = MiscUtil.getStringProperty(props, propPrefix + "." + AuditServerConstants.PROP_TOPIC_NAME, AuditServerConstants.DEFAULT_TOPIC);
    }

    @Override
    public KafkaConsumer<String, String> getDispatcher() {
        return dispatcher;
    }

    @Override
    public String getTopicName() {
        return topicName;
    }

    public String getDispatcherGroupId() {
        return dispatcherGroupId;
    }

    private String getDispatcherGroupId(Properties props, String propPrefix, String defaultDispatcherGroupId) {
        String configuredGroupId = MiscUtil.getStringProperty(props, propPrefix + ".kafka.group.id");
        if (configuredGroupId != null && !configuredGroupId.trim().isEmpty()) {
            return configuredGroupId.trim();
        }
        return defaultDispatcherGroupId;
    }

    protected abstract String getDispatcherName();

    protected abstract DispatcherWorker createDispatcherWorker(String workerId, List<Integer> assignedPartitions);

    protected void shutdownDestination() {
        // To be overridden by subclasses if needed
    }

    @Override
    public void run() {
        try {
            LOG.info("Starting {} dispatcher", getClass().getSimpleName());
            if (running.compareAndSet(false, true)) {
                startDispatcherWorkers();
            }

            // Keep main thread alive while dispatcher threads are running
            while (running.get()) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    LOG.info("{} dispatcher main thread interrupted", getDispatcherName());
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        } catch (Throwable e) {
            LOG.error("Error in {}", getClass().getSimpleName(), e);
        } finally {
            shutdown();
        }
    }

    @Override
    public void shutdown() {
        LOG.info("==> {} shutdown()", getClass().getSimpleName());

        running.set(false);

        if (dispatcherThreadPool != null) {
            dispatcherThreadPool.shutdownNow();
            try {
                if (!dispatcherThreadPool.awaitTermination(30, java.util.concurrent.TimeUnit.SECONDS)) {
                    LOG.warn("{} dispatcher thread pool did not terminate within 30 seconds", getDispatcherName());
                }
            } catch (InterruptedException e) {
                LOG.warn("Interrupted while waiting for {} dispatcher thread pool to terminate", getDispatcherName(), e);
                Thread.currentThread().interrupt();
            }
        }

        dispatcherWorkers.clear();

        if (dispatcher != null) {
            try {
                dispatcher.close();
            } catch (Exception e) {
                LOG.error("Error closing main dispatcher", e);
            }
        }

        shutdownDestination();

        LOG.info("<== {} shutdown() complete", getClass().getSimpleName());
    }

    protected void startDispatcherWorkers() {
        LOG.info("==> AuditDispatcherBase.startDispatcherWorkers(): Creating {} workers for horizontal scaling", dispatcherThreadCount);
        LOG.info("Each worker will subscribe to topic '{}' and process partitions assigned by Kafka", topicName);

        dispatcherThreadPool = Executors.newFixedThreadPool(dispatcherThreadCount);
        LOG.info("Created thread pool with {} threads for scalable {} consumption", dispatcherThreadCount, getDispatcherName());

        for (int i = 0; i < dispatcherThreadCount; i++) {
            String workerId = getDispatcherName().toLowerCase() + "-worker-" + i;
            DispatcherWorker worker = createDispatcherWorker(workerId, new ArrayList<>());
            dispatcherWorkers.put(workerId, worker);
            dispatcherThreadPool.submit(worker);

            LOG.info("Started {} dispatcher worker '{}' - will process ANY appId assigned by Kafka", getDispatcherName(), workerId);
        }

        LOG.info("<== AuditDispatcherBase.startDispatcherWorkers(): All {} workers started in SUBSCRIBE mode", dispatcherThreadCount);
    }

    protected abstract class DispatcherWorker implements Runnable {
        protected final String workerId;
        protected final List<Integer> assignedPartitions;
        protected KafkaConsumer<String, String> workerDispatcher;

        // Offset management
        protected final Map<TopicPartition, OffsetAndMetadata> pendingOffsets = new HashMap<>();
        protected final AtomicLong lastCommitTime = new AtomicLong(System.currentTimeMillis());
        protected final AtomicInteger messagesProcessedSinceLastCommit = new AtomicInteger(0);

        public DispatcherWorker(String workerId, List<Integer> assignedPartitions) {
            this.workerId = workerId;
            this.assignedPartitions = assignedPartitions;
        }

        protected abstract void processRecordBatch(ConsumerRecords<String, String> records);

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
                AuditDispatcherRebalanceListener rebalanceListener = new AuditDispatcherRebalanceListener(
                        workerId,
                        getDispatcherName(),
                        topicName,
                        offsetCommitStrategy,
                        dispatcherGroupId,
                        workerDispatcher,
                        pendingOffsets,
                        messagesProcessedSinceLastCommit,
                        lastCommitTime,
                        assignedPartitions);

                // Subscribe to topic with re-balance listener
                workerDispatcher.subscribe(Collections.singletonList(topicName), rebalanceListener);

                LOG.info("[{}-DISPATCHER] Worker '{}' subscribed successfully, waiting for partition assignment from Kafka", getDispatcherName(), workerId);
                long threadId = Thread.currentThread().getId();
                String threadName = Thread.currentThread().getName();
                LOG.info("[{}-DISPATCHER-STARTUP] Worker '{}' [Thread-ID: {}, Thread-Name: '{}'] started | Topic: '{}' | Dispatcher-Group: {} | Mode: SUBSCRIBE",
                        getDispatcherName(), workerId, threadId, threadName, topicName, dispatcherGroupId);

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
                LOG.error("Error in {} dispatcher worker '{}'", getDispatcherName(), workerId, e);
            } finally {
                // Final offset commit before shutdown
                commitPendingOffsets(true);

                if (workerDispatcher != null) {
                    try {
                        LOG.info("{} Worker '{}': Unsubscribing from topic", getDispatcherName(), workerId);
                        workerDispatcher.unsubscribe();
                    } catch (Exception e) {
                        LOG.warn("{} Worker '{}': Error during unsubscribe", getDispatcherName(), workerId, e);
                    }

                    try {
                        LOG.info("{} Worker '{}': Closing dispatcher", getDispatcherName(), workerId);
                        workerDispatcher.close();
                    } catch (Exception e) {
                        LOG.error("Error closing dispatcher for {} worker '{}'", getDispatcherName(), workerId, e);
                    }
                }
                LOG.info("{} dispatcher worker '{}' stopped", getDispatcherName(), workerId);
            }
        }

        private void configureOffsetManagement(Properties dispatcherProps) {
            // Always disable auto commit - only batch or manual strategies supported
            dispatcherProps.put("enable.auto.commit", "false");
            LOG.debug("{} worker '{}' configured for manual offset commit with strategy: {}", getDispatcherName(), workerId, offsetCommitStrategy);
        }

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

        private void commitPendingOffsets(boolean isShutdown) {
            if (pendingOffsets.isEmpty()) {
                return;
            }

            try {
                workerDispatcher.commitSync(pendingOffsets);

                LOG.debug("{} worker '{}' committed {} offsets, processed {} messages",
                        getDispatcherName(), workerId, pendingOffsets.size(), messagesProcessedSinceLastCommit.get());

                // Clear committed offsets
                pendingOffsets.clear();
                lastCommitTime.set(System.currentTimeMillis());
                messagesProcessedSinceLastCommit.set(0);
            } catch (Exception e) {
                LOG.error("Error committing offsets in {} worker '{}': {}", getDispatcherName(), workerId, pendingOffsets, e);

                if (isShutdown) {
                    // During shutdown, retry to avoid loss of any offsets
                    try {
                        Thread.sleep(1000);
                        workerDispatcher.commitSync(pendingOffsets);
                        LOG.info("Successfully committed offsets on retry during shutdown for {} worker '{}'", getDispatcherName(), workerId);
                    } catch (Exception retryException) {
                        LOG.error("Failed to commit offsets even on retry during shutdown for {} worker '{}'", getDispatcherName(), workerId, retryException);
                    }
                }
            }
        }
    }
}
