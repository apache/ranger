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
package org.apache.ranger.audit.consumer.kafka;

import org.apache.hadoop.security.SecureClientLogin;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.ranger.audit.provider.AuditProviderFactory;
import org.apache.ranger.audit.provider.MiscUtil;
import org.apache.ranger.audit.server.AuditServerConstants;
import org.apache.ranger.audit.server.HdfsConsumerConfig;
import org.apache.ranger.audit.utils.AuditServerLogFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
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

public class AuditHDFSConsumer extends AuditConsumerBase implements AuditConsumer {
    private static final Logger LOG                                = LoggerFactory.getLogger(AuditHDFSConsumer.class);
    private static final String RANGER_AUDIT_HDFS_CONSUMER_GROUP   = AuditServerConstants.DEFAULT_RANGER_AUDIT_HDFS_CONSUMER_GROUP;

    // Register AuditHDFSConsumer factory in the audit consumer registry
    static {
        try {
            AuditConsumerRegistry.getInstance().registerFactory(AuditServerConstants.PROP_HDFS_DEST_PREFIX, (props, propPrefix) -> new AuditHDFSConsumer(props, propPrefix));
            LOG.info("Registered HDFS consumer with AuditConsumerRegistry");
        } catch (Exception e) {
            LOG.error("Failed to register HDFS consumer factory", e);
        }
    }

    private final AtomicBoolean               running              = new AtomicBoolean(false);
    private       ExecutorService             consumerThreadPool;
    private final Map<String, ConsumerWorker> consumerWorkers      = new ConcurrentHashMap<>();
    private       int                         consumerThreadCount  = 1;
    // Offset management configuration (batch or manual only supported)
    private       String                      offsetCommitStrategy = AuditServerConstants.DEFAULT_OFFSET_COMMIT_STRATEGY;
    private       long                        offsetCommitInterval = AuditServerConstants.DEFAULT_OFFSET_COMMIT_INTERVAL_MS;
    // Message routing handler for HDFS destination
    private       AuditRouterHDFS             auditRouterHDFS;
    private       Properties                  props;
    private       String                      propPrefix;

    public AuditHDFSConsumer(Properties props, String propPrefix) throws Exception {
        super(props, propPrefix, RANGER_AUDIT_HDFS_CONSUMER_GROUP);
        this.props = new Properties();
        this.props.putAll(props);
        this.propPrefix = propPrefix;
        init(props, propPrefix);
    }

    @Override
    public void init(Properties props, String propPrefix) throws Exception {
        LOG.info("==> AuditHDFSConsumer.init():  AuditHDFSConsumer initializing with appId-based threading and offset management");

        consumer = getKafkaConsumer();

        // Initialize Ranger UGI for HDFS operations if Kerberos is enabled
        initializeRangerUGI();

        // Add Hadoop configuration properties from HdfsConsumerConfig to props
        addHadoopConfigToProps(props);

        // Initialize consumer configuration
        initConsumerConfig(props, propPrefix);

        // Initialize destination handler for message processing
        auditRouterHDFS = new AuditRouterHDFS();
        auditRouterHDFS.init(props, AuditProviderFactory.AUDIT_DEST_BASE + "." + AuditServerConstants.PROP_HDFS_DEST_PREFIX);

        LOG.info("<== AuditHDFSConsumer.init(): AuditHDFSConsumer initialized successfully");
    }

    private void initializeRangerUGI() throws Exception {
        LOG.info("==> AuditHDFSConsumer.initializeRangerUGI()");

        try {
            HdfsConsumerConfig auditConfig = HdfsConsumerConfig.getInstance();
            String             authType    = auditConfig.get(AuditServerConstants.PROP_HADOOP_AUTHENTICATION_TYPE, "simple");

            if (!AuditServerConstants.PROP_HADOOP_AUTH_TYPE_KERBEROS.equalsIgnoreCase(authType)) {
                LOG.info("Hadoop authentication is not Kerberos ({}), skipping Ranger UGI initialization", authType);
                return;
            }

            String principal = auditConfig.get(AuditServerConstants.AUDIT_SERVER_PROP_PREFIX + AuditServerConstants.PROP_AUDIT_SERVICE_PRINCIPAL);
            String keytab    = auditConfig.get(AuditServerConstants.AUDIT_SERVER_PROP_PREFIX + AuditServerConstants.PROP_AUDIT_SERVICE_KEYTAB);
            String hostName  = auditConfig.get(AuditServerConstants.AUDIT_SERVER_PROP_PREFIX + "host");

            if (principal == null || keytab == null) {
                LOG.warn("Kerberos is enabled but principal or keytab is null! principal={}, keytab={}", principal, keytab);
                String msg = String.format("Kerberos is enabled but principal or keytab is null! principal={}, keytab={}", principal, keytab);
                throw new Exception(msg);
            }

            if (principal.contains("_HOST")) {
                try {
                    principal = SecureClientLogin.getPrincipal(principal, hostName);
                    LOG.info("Resolved principal from [{}] using hostname [{}]", principal, hostName);
                } catch (IOException e) {
                    LOG.error("Failed to resolve principal pattern [{}] with hostname [{}]", principal, hostName, e);
                    throw e;
                }
            }

            // Set Hadoop security configuration from core-site.xml
            org.apache.hadoop.conf.Configuration coreSite = auditConfig.getCoreSiteConfiguration();
            UserGroupInformation.setConfiguration(coreSite);

            LOG.info("Initializing Ranger UGI for HDFS writes: principal={}, keytab={}", principal, keytab);

            UserGroupInformation rangerUGI = UserGroupInformation.loginUserFromKeytabAndReturnUGI(principal, keytab);

            MiscUtil.setUGILoginUser(rangerUGI, null);

            LOG.info("<== AuditHDFSConsumer.initializeRangerUGI(): Ranger UGI initialized successfully: user={}, auth={}, hasKerberos={}",
                    rangerUGI.getUserName(), rangerUGI.getAuthenticationMethod(), rangerUGI.hasKerberosCredentials());
        } catch (IOException e) {
            LOG.error("Failed to initialize Ranger UGI for HDFS writes", e);
            throw e;
        }
    }

    /**
     * Add Hadoop configuration properties from core-site.xml and hdfs-site.xml to props.
     */
    private void addHadoopConfigToProps(Properties props) {
        LOG.info("==> AuditHDFSConsumer.addHadoopConfigToProps()");

        try {
            HdfsConsumerConfig hdfsConfig   = HdfsConsumerConfig.getInstance();
            String             configPrefix = "xasecure.audit.destination.hdfs.config.";

            Properties hadoopProps = hdfsConfig.getHadoopPropertiesWithPrefix(configPrefix);
            props.putAll(hadoopProps);

            LOG.info("<== AuditHDFSConsumer.addHadoopConfigToProps(): Added {} Hadoop configuration properties from HdfsConsumerConfig", hadoopProps.size());
        } catch (Exception e) {
            LOG.error("Failed to add Hadoop configuration properties to props", e);
        }
    }

    private void initConsumerConfig(Properties props, String propPrefix) {
        LOG.info("==> AuditHDFSConsumer.initConsumerConfig()");

        // Get consumer thread count
        this.consumerThreadCount = MiscUtil.getIntProperty(props, propPrefix + "." + AuditServerConstants.PROP_CONSUMER_THREAD_COUNT, 1);
        LOG.info("HDFS consumer thread count: {}", consumerThreadCount);

        // Initialize offset management configuration
        initializeOffsetManagement(props, propPrefix);

        LOG.info("<== AuditHDFSConsumer.initConsumerConfig()");
    }

    private void initializeOffsetManagement(Properties props, String propPrefix) {
        LOG.info("==> AuditHDFSConsumer.initializeOffsetManagement()");

        // Get offset commit strategy
        this.offsetCommitStrategy = MiscUtil.getStringProperty(props,
                propPrefix + "." + AuditServerConstants.PROP_CONSUMER_OFFSET_COMMIT_STRATEGY,
                AuditServerConstants.DEFAULT_OFFSET_COMMIT_STRATEGY);

        // Get offset commit interval (only used for manual strategy)
        this.offsetCommitInterval = MiscUtil.getLongProperty(props,
                propPrefix + "." + AuditServerConstants.PROP_CONSUMER_OFFSET_COMMIT_INTERVAL,
                AuditServerConstants.DEFAULT_OFFSET_COMMIT_INTERVAL_MS);

        AuditServerLogFormatter.builder("HDFS Consumer Offset Management Configuration")
                .add("Commit Strategy", offsetCommitStrategy)
                .add("Commit Interval (ms)", offsetCommitInterval + " (used in manual mode only)")
                .logInfo(LOG);

        LOG.info("<== AuditHDFSConsumer.initializeOffsetManagement()");
    }

    @Override
    public void run() {
        try {
            if (auditRouterHDFS == null) {
                init(this.props, this.propPrefix);
            }

            LOG.info("Starting AuditHDFSConsumer with appId-based thread");
            startMultithreadedConsumption();

            // Keep main thread alive while consumer threads are running
            while (running.get()) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    LOG.info("HDFS consumer main thread interrupted");
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        } catch (Throwable e) {
            LOG.error("Error in AuditHDFSConsumer", e);
        } finally {
            shutdown();
        }
    }

    @Override
    public KafkaConsumer<String, String> getKafkaConsumer() {
        return consumer;
    }

    @Override
    public void processMessage(String audit) throws Exception {
        processMessage(audit, null);
    }

    public void processMessage(String message, String partitionKey) throws Exception {
        auditRouterHDFS.routeAuditMessage(message, partitionKey);
    }

    @Override
    public String getTopicName() {
        return topicName;
    }

    public String getConsumerGroupId() {
        return consumerGroupId;
    }

    private void startMultithreadedConsumption() {
        LOG.debug("==> AuditHDFSConsumer.startMultithreadedConsumption()");

        if (running.compareAndSet(false, true)) {
            startConsumerWorkers();
        }

        LOG.debug("<== AuditHDFSConsumer.startMultithreadedConsumption()");
    }

    private void startConsumerWorkers() {
        LOG.info("==> AuditHDFSConsumer.startConsumerWorkers(): Creating {} consumer workers for horizontal scaling", consumerThreadCount);
        LOG.info("Each worker will subscribe to topic '{}' and process partitions assigned by Kafka", topicName);

        // Create thread pool sized for consumer workers
        consumerThreadPool = Executors.newFixedThreadPool(consumerThreadCount);
        LOG.info("Created thread pool with {} threads for scalable HDFS consumption", consumerThreadCount);

        // Create HDFS consumer workers
        for (int i = 0; i < consumerThreadCount; i++) {
            String workerId = "hdfs-worker-" + i;
            ConsumerWorker worker = new ConsumerWorker(workerId, new ArrayList<>());
            consumerWorkers.put(workerId, worker);
            consumerThreadPool.submit(worker);

            LOG.info("Started HDFS consumer worker '{}' - will process ANY appId assigned by Kafka", workerId);
        }

        LOG.info("<== AuditHDFSConsumer.startConsumerWorkers(): All {} workers started in SUBSCRIBE mode", consumerThreadCount);
    }

    private class ConsumerWorker implements Runnable {
        private final String workerId;
        private final List<Integer> assignedPartitions;
        private KafkaConsumer<String, String> workerConsumer;

        // Offset management
        private final Map<TopicPartition, OffsetAndMetadata> pendingOffsets = new HashMap<>();
        private final AtomicLong lastCommitTime = new AtomicLong(System.currentTimeMillis());
        private final AtomicInteger messagesProcessedSinceLastCommit = new AtomicInteger(0);

        public ConsumerWorker(String workerId, List<Integer> assignedPartitions) {
            this.workerId = workerId;
            this.assignedPartitions = assignedPartitions;
        }

        @Override
        public void run() {
            try {
                // Create consumer for this worker with offset management configuration
                Properties workerConsumerProps = new Properties();
                workerConsumerProps.putAll(consumerProps);

                // Configure offset management based on strategy
                configureOffsetManagement(workerConsumerProps);

                workerConsumer = new KafkaConsumer<>(workerConsumerProps);

                // Create re-balance listener
                AuditConsumerRebalanceListener rebalanceListener = new AuditConsumerRebalanceListener(
                        workerId,
                        AuditServerConstants.DESTINATION_HDFS,
                        topicName,
                        offsetCommitStrategy,
                        consumerGroupId,
                        workerConsumer,
                        pendingOffsets,
                        messagesProcessedSinceLastCommit,
                        lastCommitTime,
                        assignedPartitions);

                // Subscribe to topic with re-balance listener
                workerConsumer.subscribe(Collections.singletonList(topicName), rebalanceListener);

                LOG.info("[HDFS-CONSUMER] Worker '{}' subscribed successfully, waiting for partition assignment from Kafka", workerId);
                long threadId = Thread.currentThread().getId();
                String threadName = Thread.currentThread().getName();
                LOG.info("[HDFS-CONSUMER-STARTUP] Worker '{}' [Thread-ID: {}, Thread-Name: '{}'] started | Topic: '{}' | Consumer-Group: {} | Mode: SUBSCRIBE",
                        workerId, threadId, threadName, topicName, consumerGroupId);

                // Consume messages
                while (running.get()) {
                    ConsumerRecords<String, String> records = workerConsumer.poll(Duration.ofMillis(100));

                    if (!records.isEmpty()) {
                        processRecordBatch(records);
                        // Handle offset committing based on strategy
                        handleOffsetCommitting();
                    }
                }
            } catch (Exception e) {
                LOG.error("Error in HDFS consumer worker '{}'", workerId, e);
            } finally {
                // Final offset commit before shutdown
                commitPendingOffsets(true);

                if (workerConsumer != null) {
                    try {
                        LOG.info("HDFS Worker '{}': Unsubscribing from topic", workerId);
                        workerConsumer.unsubscribe();
                    } catch (Exception e) {
                        LOG.warn("HDFS Worker '{}': Error during unsubscribe", workerId, e);
                    }

                    try {
                        LOG.info("HDFS Worker '{}': Closing consumer", workerId);
                        workerConsumer.close();
                    } catch (Exception e) {
                        LOG.error("Error closing consumer for HDFS worker '{}'", workerId, e);
                    }
                }
                LOG.info("HDFS consumer worker '{}' stopped", workerId);
            }
        }

        private void configureOffsetManagement(Properties consumerProps) {
            // Always disable auto commit - only batch or manual strategies supported
            consumerProps.put("enable.auto.commit", "false");
            LOG.debug("HDFS worker '{}' configured for manual offset commit with strategy: {}", workerId, offsetCommitStrategy);
        }

        private void processRecordBatch(ConsumerRecords<String, String> records) {
            for (ConsumerRecord<String, String> record : records) {
                try {
                    LOG.debug("HDFS worker '{}' consumed: partition={}, key={}, offset={}",
                            workerId, record.partition(), record.key(), record.offset());

                    // Process the message using the destination handler
                    // The partition key (record.key()) contains the appId for HDFS path routing
                    processMessage(record.value(), record.key());

                    // Track offset for manual commit strategies
                    TopicPartition partition = new TopicPartition(record.topic(), record.partition());
                    pendingOffsets.put(partition, new OffsetAndMetadata(record.offset() + 1));
                    messagesProcessedSinceLastCommit.incrementAndGet();
                } catch (Exception e) {
                    LOG.error("Error processing message in HDFS worker '{}': partition={}, key={}, offset={}",
                            workerId, record.partition(), record.key(), record.offset(), e);

                    // On error, track offset to prevent reprocessing
                    TopicPartition partition = new TopicPartition(record.topic(), record.partition());
                    pendingOffsets.put(partition, new OffsetAndMetadata(record.offset()));
                }
            }
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
                workerConsumer.commitSync(pendingOffsets);

                LOG.debug("HDFS worker '{}' committed {} offsets, processed {} messages",
                        workerId, pendingOffsets.size(), messagesProcessedSinceLastCommit.get());

                // Clear committed offsets
                pendingOffsets.clear();
                lastCommitTime.set(System.currentTimeMillis());
                messagesProcessedSinceLastCommit.set(0);
            } catch (Exception e) {
                LOG.error("Error committing offsets in HDFS worker '{}': {}", workerId, pendingOffsets, e);

                if (isShutdown) {
                    // During shutdown, retry to avoid loss of any offsets
                    try {
                        Thread.sleep(1000);
                        workerConsumer.commitSync(pendingOffsets);
                        LOG.info("Successfully committed offsets on retry during shutdown for HDFS worker '{}'", workerId);
                    } catch (Exception retryException) {
                        LOG.error("Failed to commit offsets even on retry during shutdown for HDFS worker '{}'", workerId, retryException);
                    }
                }
            }
        }
    }

    @Override
    public void shutdown() {
        LOG.info("==> AuditHDFSConsumer.shutdown()");

        // Stop consumer threads
        running.set(false);

        // Shutdown consumer workers
        if (consumerThreadPool != null) {
            consumerThreadPool.shutdownNow();
            try {
                if (!consumerThreadPool.awaitTermination(30, java.util.concurrent.TimeUnit.SECONDS)) {
                    LOG.warn("HDFS consumer thread pool did not terminate within 30 seconds");
                }
            } catch (InterruptedException e) {
                LOG.warn("Interrupted while waiting for HDFS consumer thread pool to terminate", e);
                Thread.currentThread().interrupt();
            }
        }

        consumerWorkers.clear();

        // Shutdown destination handler
        if (auditRouterHDFS != null) {
            try {
                auditRouterHDFS.shutdown();
            } catch (Exception e) {
                LOG.error("Error shutting down HDFS destination handler", e);
            }
        }

        // Close main Kafka consumer
        if (consumer != null) {
            try {
                consumer.close();
            } catch (Exception e) {
                LOG.error("Error closing main Kafka consumer", e);
            }
        }

        LOG.info("<== AuditHDFSConsumer.shutdown() complete");
    }
}
