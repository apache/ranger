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
import org.apache.ranger.audit.server.HdfsDispatcherConfig;
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

public class AuditHDFSDispatcher extends AuditDispatcherBase {
    private static final Logger LOG = LoggerFactory.getLogger(AuditHDFSDispatcher.class);

    private static final String RANGER_AUDIT_HDFS_DISPATCHER_GROUP = AuditServerConstants.DEFAULT_RANGER_AUDIT_HDFS_DISPATCHER_GROUP;

    // Register AuditHDFSDispatcher factory in the audit dispatcher registry
    static {
        try {
            AuditDispatcherRegistry.getInstance().registerFactory(AuditServerConstants.PROP_HDFS_DEST_PREFIX, AuditHDFSDispatcher::new);
            LOG.info("Registered HDFS dispatcher with AuditDispatcherRegistry");
        } catch (Exception e) {
            LOG.error("Failed to register HDFS dispatcher factory", e);
        }
    }

    private final AtomicBoolean                 running           = new AtomicBoolean(false);
    private final Map<String, DispatcherWorker> dispatcherWorkers = new ConcurrentHashMap<>();
    private final AuditRouterHDFS               auditRouterHDFS;

    private ExecutorService dispatcherThreadPool;
    private int             dispatcherThreadCount  = 1;

    // Offset management configuration (batch or manual only supported)
    private String offsetCommitStrategy = AuditServerConstants.DEFAULT_OFFSET_COMMIT_STRATEGY;
    private long   offsetCommitInterval = AuditServerConstants.DEFAULT_OFFSET_COMMIT_INTERVAL_MS;

    public AuditHDFSDispatcher(Properties props, String propPrefix) throws Exception {
        super(props, propPrefix, RANGER_AUDIT_HDFS_DISPATCHER_GROUP);

        auditRouterHDFS = new AuditRouterHDFS();

        init(props, propPrefix);
    }

    @Override
    public void run() {
        try {
            LOG.info("Starting AuditHDFSDispatcher with appId-based thread");
            startMultithreadedConsumption();

            // Keep main thread alive while dispatcher threads are running
            while (running.get()) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    LOG.info("HDFS dispatcher main thread interrupted");
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        } catch (Throwable e) {
            LOG.error("Error in AuditHDFSDispatcher", e);
        } finally {
            shutdown();
        }
    }

    @Override
    public void shutdown() {
        LOG.info("==> AuditHDFSDispatcher.shutdown()");

        // Stop dispatcher threads
        running.set(false);

        // Shutdown dispatcher workers
        if (dispatcherThreadPool != null) {
            dispatcherThreadPool.shutdownNow();
            try {
                if (!dispatcherThreadPool.awaitTermination(30, java.util.concurrent.TimeUnit.SECONDS)) {
                    LOG.warn("HDFS dispatcher thread pool did not terminate within 30 seconds");
                }
            } catch (InterruptedException e) {
                LOG.warn("Interrupted while waiting for HDFS dispatcher thread pool to terminate", e);
                Thread.currentThread().interrupt();
            }
        }

        dispatcherWorkers.clear();

        // Shutdown destination handler
        if (auditRouterHDFS != null) {
            try {
                auditRouterHDFS.shutdown();
            } catch (Exception e) {
                LOG.error("Error shutting down HDFS destination handler", e);
            }
        }

        // Close main Kafka dispatcher
        if (dispatcher != null) {
            try {
                dispatcher.close();
            } catch (Exception e) {
                LOG.error("Error closing main Kafka dispatcher", e);
            }
        }

        LOG.info("<== AuditHDFSDispatcher.shutdown() complete");
    }

    private void init(Properties props, String propPrefix) throws Exception {
        LOG.info("==> AuditHDFSDispatcher.init():  AuditHDFSDispatcher initializing with appId-based threading and offset management");

        // Initialize Ranger UGI for HDFS operations if Kerberos is enabled
        initializeRangerUGI(props, propPrefix);

        // Add Hadoop configuration properties from HdfsDispatcherConfig to props
        addHadoopConfigToProps(props);

        // Initialize dispatcher configuration
        initDispatcherConfig(props, propPrefix);

        // Initialize destination handler for message processing
        auditRouterHDFS.init(props, AuditProviderFactory.AUDIT_DEST_BASE + "." + AuditServerConstants.PROP_HDFS_DEST_PREFIX);

        LOG.info("<== AuditHDFSDispatcher.init(): AuditHDFSDispatcher initialized successfully");
    }

    private void processMessage(String message, String partitionKey) throws Exception {
        auditRouterHDFS.routeAuditMessage(message, partitionKey);
    }

    private void initializeRangerUGI(Properties props, String propPrefix) throws Exception {
        LOG.info("==> AuditHDFSDispatcher.initializeRangerUGI()");

        try {
            HdfsDispatcherConfig auditConfig = HdfsDispatcherConfig.getInstance();
            String             authType    = auditConfig.get(AuditServerConstants.PROP_HADOOP_AUTHENTICATION_TYPE, "simple");

            if (!AuditServerConstants.PROP_HADOOP_AUTH_TYPE_KERBEROS.equalsIgnoreCase(authType)) {
                LOG.info("Hadoop authentication is not Kerberos ({}), skipping Ranger UGI initialization", authType);
                return;
            }

            String principal = MiscUtil.getStringProperty(props, propPrefix + "." + AuditServerConstants.PROP_AUDIT_SERVICE_PRINCIPAL);
            String keytab    = MiscUtil.getStringProperty(props, propPrefix + "." + AuditServerConstants.PROP_AUDIT_SERVICE_KEYTAB);
            String hostName  = MiscUtil.getStringProperty(props, propPrefix + "." + "host");

            if (principal == null || keytab == null) {
                LOG.warn("Kerberos is enabled but principal or keytab is null! principal={}, keytab={}", principal, keytab);
                String msg = String.format("Kerberos is enabled but principal or keytab is null! principal=%s, keytab=%s", principal, keytab);
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

            LOG.info("<== AuditHDFSDispatcher.initializeRangerUGI(): Ranger UGI initialized successfully: user={}, auth={}, hasKerberos={}",
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
        LOG.info("==> AuditHDFSDispatcher.addHadoopConfigToProps()");

        try {
            HdfsDispatcherConfig hdfsConfig   = HdfsDispatcherConfig.getInstance();
            String             configPrefix = "xasecure.audit.destination.hdfs.config.";

            Properties hadoopProps = hdfsConfig.getHadoopPropertiesWithPrefix(configPrefix);
            props.putAll(hadoopProps);

            LOG.info("<== AuditHDFSDispatcher.addHadoopConfigToProps(): Added {} Hadoop configuration properties from HdfsDispatcherConfig", hadoopProps.size());
        } catch (Exception e) {
            LOG.error("Failed to add Hadoop configuration properties to props", e);
        }
    }

    private void initDispatcherConfig(Properties props, String propPrefix) {
        LOG.info("==> AuditHDFSDispatcher.initDispatcherConfig()");

        // Get dispatcher thread count
        this.dispatcherThreadCount = MiscUtil.getIntProperty(props, propPrefix + "." + AuditServerConstants.PROP_DISPATCHER_THREAD_COUNT, 1);
        LOG.info("HDFS dispatcher thread count: {}", dispatcherThreadCount);

        // Initialize offset management configuration
        initializeOffsetManagement(props, propPrefix);

        LOG.info("<== AuditHDFSDispatcher.initDispatcherConfig()");
    }

    private void initializeOffsetManagement(Properties props, String propPrefix) {
        LOG.info("==> AuditHDFSDispatcher.initializeOffsetManagement()");

        // Get offset commit strategy
        this.offsetCommitStrategy = MiscUtil.getStringProperty(props,
                propPrefix + "." + AuditServerConstants.PROP_DISPATCHER_OFFSET_COMMIT_STRATEGY,
                AuditServerConstants.DEFAULT_OFFSET_COMMIT_STRATEGY);

        // Get offset commit interval (only used for manual strategy)
        this.offsetCommitInterval = MiscUtil.getLongProperty(props,
                propPrefix + "." + AuditServerConstants.PROP_DISPATCHER_OFFSET_COMMIT_INTERVAL,
                AuditServerConstants.DEFAULT_OFFSET_COMMIT_INTERVAL_MS);

        AuditServerLogFormatter.builder("HDFS Dispatcher Offset Management Configuration")
                .add("Commit Strategy", offsetCommitStrategy)
                .add("Commit Interval (ms)", offsetCommitInterval + " (used in manual mode only)")
                .logInfo(LOG);

        LOG.info("<== AuditHDFSDispatcher.initializeOffsetManagement()");
    }

    private void startMultithreadedConsumption() {
        LOG.debug("==> AuditHDFSDispatcher.startMultithreadedConsumption()");

        if (running.compareAndSet(false, true)) {
            startDispatcherWorkers();
        }

        LOG.debug("<== AuditHDFSDispatcher.startMultithreadedConsumption()");
    }

    private void startDispatcherWorkers() {
        LOG.info("==> AuditHDFSDispatcher.startDispatcherWorkers(): Creating {} dispatcher workers for horizontal scaling", dispatcherThreadCount);
        LOG.info("Each worker will subscribe to topic '{}' and process partitions assigned by Kafka", topicName);

        // Create thread pool sized for dispatcher workers
        dispatcherThreadPool = Executors.newFixedThreadPool(dispatcherThreadCount);
        LOG.info("Created thread pool with {} threads for scalable HDFS consumption", dispatcherThreadCount);

        // Create HDFS dispatcher workers
        for (int i = 0; i < dispatcherThreadCount; i++) {
            String workerId = "hdfs-worker-" + i;
            DispatcherWorker worker = new DispatcherWorker(workerId, new ArrayList<>());
            dispatcherWorkers.put(workerId, worker);
            dispatcherThreadPool.submit(worker);

            LOG.info("Started HDFS dispatcher worker '{}' - will process ANY appId assigned by Kafka", workerId);
        }

        LOG.info("<== AuditHDFSDispatcher.startDispatcherWorkers(): All {} workers started in SUBSCRIBE mode", dispatcherThreadCount);
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
                AuditDispatcherRebalanceListener rebalanceListener = new AuditDispatcherRebalanceListener(
                        workerId,
                        AuditServerConstants.DESTINATION_HDFS,
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

                LOG.info("[HDFS-DISPATCHER] Worker '{}' subscribed successfully, waiting for partition assignment from Kafka", workerId);
                long threadId = Thread.currentThread().getId();
                String threadName = Thread.currentThread().getName();
                LOG.info("[HDFS-DISPATCHER-STARTUP] Worker '{}' [Thread-ID: {}, Thread-Name: '{}'] started | Topic: '{}' | Dispatcher-Group: {} | Mode: SUBSCRIBE",
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
                LOG.error("Error in HDFS dispatcher worker '{}'", workerId, e);
            } finally {
                // Final offset commit before shutdown
                commitPendingOffsets(true);

                if (workerDispatcher != null) {
                    try {
                        LOG.info("HDFS Worker '{}': Unsubscribing from topic", workerId);
                        workerDispatcher.unsubscribe();
                    } catch (Exception e) {
                        LOG.warn("HDFS Worker '{}': Error during unsubscribe", workerId, e);
                    }

                    try {
                        LOG.info("HDFS Worker '{}': Closing dispatcher", workerId);
                        workerDispatcher.close();
                    } catch (Exception e) {
                        LOG.error("Error closing dispatcher for HDFS worker '{}'", workerId, e);
                    }
                }
                LOG.info("HDFS dispatcher worker '{}' stopped", workerId);
            }
        }

        private void configureOffsetManagement(Properties dispatcherProps) {
            // Always disable auto commit - only batch or manual strategies supported
            dispatcherProps.put("enable.auto.commit", "false");
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
                workerDispatcher.commitSync(pendingOffsets);

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
                        workerDispatcher.commitSync(pendingOffsets);
                        LOG.info("Successfully committed offsets on retry during shutdown for HDFS worker '{}'", workerId);
                    } catch (Exception retryException) {
                        LOG.error("Failed to commit offsets even on retry during shutdown for HDFS worker '{}'", workerId, retryException);
                    }
                }
            }
        }
    }
}
