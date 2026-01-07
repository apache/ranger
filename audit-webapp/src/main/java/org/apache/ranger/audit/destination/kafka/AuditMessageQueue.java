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

package org.apache.ranger.audit.destination.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.ranger.audit.destination.AuditDestination;
import org.apache.ranger.audit.model.AuditEventBase;
import org.apache.ranger.audit.model.AuthzAuditEvent;
import org.apache.ranger.audit.provider.MiscUtil;
import org.apache.ranger.audit.utils.AuditMessageQueueUtils;
import org.apache.ranger.audit.utils.AuditServerLogFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.PrivilegedExceptionAction;
import java.util.Collection;
import java.util.Map;
import java.util.Properties;

/**
 * AuditMessageQueue creates the necessary kafka queue for http post to relay the audit events into kafka.
 * It creates the necessary audit topics, producers threads consumer threads and recovery threads for
 * the destinations configured.
 */
public class AuditMessageQueue extends AuditDestination {
    public KafkaProducer<String, String> kafkaProducer;
    public AuditProducer                 auditProducerRunnable;
    public AuditSolrConsumer             auditSolrConsumerRunnable;
    public AuditHDFSConsumer             auditHDFSConsumerRunnable;
    public AuditMessageQueueUtils        auditMessageQueueUtils;
    public String                        topicName;
    private Thread                       producerThread;
    private AuditRecoveryManager         recoveryManager;

    private static final Logger LOG = LoggerFactory.getLogger(AuditMessageQueue.class);

    @Override
    public void init(Properties props, String propPrefix) {
        LOG.info("==> AuditMessageQueue.init()");

        LOG.debug("AuditMessageQueue.init() propPrefix: {}", propPrefix);
        for (Map.Entry<Object, Object> entry : props.entrySet()) {
            LOG.debug(entry.getKey() + " : " + entry.getValue());
        }

        super.init(props, propPrefix);

        auditMessageQueueUtils = new AuditMessageQueueUtils(props);

        // create kafka topic for ranger audits queue, Kafka producer, Kafka consumers and audit recovery manager
        createAuditsTopic(props, propPrefix);
        createKafkaProducer(props, propPrefix);
        createKafkaConsumers(props, propPrefix);
        createRecoveryManager(props, propPrefix);

        LOG.info("<== AuditMessageQueue.init() : created topic: {} , producer: {}", topicName, (kafkaProducer != null) ? kafkaProducer.getClass() : "");
    }

    @Override
    public void start() {
        LOG.debug("==> AuditMessageQueue.start() called to start Audit Producer and Audit Consumers...");
        startRangerAuditRecoveryThread();
        startRangerAuditProducer();
        startRangerAuditConsumers();
        LOG.debug("<== AuditMessageQueue.start()");
    }

    @Override
    public void stop() {
        LOG.info("==> AuditMessageQueue.stop()");

        // Shutdown recovery manager first to process any remaining messages
        if (recoveryManager != null) {
            try {
                LOG.info("Shutting down Audit recovery manager...");
                recoveryManager.stop();
                LOG.info("Audit recovery manager shutdown completed");
            } catch (Exception e) {
                LOG.error("Error shutting down Audit recovery manager", e);
            }
        }

        // Shutdown producer thread
        if (auditProducerRunnable != null) {
            try {
                LOG.info("Shutting down Audit producer...");
                auditProducerRunnable.shutdown();

                // Interrupt and wait for producer thread to finish
                if (producerThread != null && producerThread.isAlive()) {
                    producerThread.interrupt();
                    try {
                        producerThread.join(5000); // Wait up to 5 seconds
                        if (producerThread.isAlive()) {
                            LOG.warn("Audit Producer thread did not terminate within 5 seconds");
                        } else {
                            LOG.info("Audit Producer thread terminated successfully");
                        }
                    } catch (InterruptedException e) {
                        LOG.warn("Interrupted while waiting for producer thread to terminate", e);
                        Thread.currentThread().interrupt();
                    }
                }
                LOG.info("Audit producer shutdown completed");
            } catch (Exception e) {
                LOG.error("Error shutting down Kafka audit producer", e);
            }
        }

        // Shutdown Solr consumer
        if (auditSolrConsumerRunnable != null) {
            try {
                LOG.info("Shutting down Solr consumer...");
                auditSolrConsumerRunnable.shutdown();
                LOG.info("Solr consumer shutdown completed");
            } catch (Exception e) {
                LOG.error("Error shutting down Solr consumer", e);
            }
        }

        // Shutdown HDFS consumer
        if (auditHDFSConsumerRunnable != null) {
            try {
                LOG.info("Shutting down HDFS consumer...");
                auditHDFSConsumerRunnable.shutdown();
                LOG.info("HDFS consumer shutdown completed");
            } catch (Exception e) {
                LOG.error("Error shutting down HDFS consumer", e);
            }
        }

        // Close producer (redundant but safe)
        if (kafkaProducer != null) {
            try {
                LOG.info("Closing Kafka producer...");
                kafkaProducer.close();
                LOG.info("Kafka producer shutdown completed");
            } catch (Exception e) {
                LOG.error("Error shutting down Kafka producer", e);
            }
        }

        LOG.info("<== AuditMessageQueue.stop()");
    }

    @Override
    public synchronized boolean log(final AuditEventBase event) {
        boolean ret = false;
        if (event instanceof AuthzAuditEvent) {
            AuthzAuditEvent authzEvent = (AuthzAuditEvent) event;

            if (authzEvent.getAgentHostname() == null) {
                authzEvent.setAgentHostname(MiscUtil.getHostname());
            }

            if (authzEvent.getLogType() == null) {
                authzEvent.setLogType("RangerAudit");
            }

            if (authzEvent.getEventId() == null) {
                authzEvent.setEventId(MiscUtil.generateUniqueId());
            }

            // Partition key is agentId (aka.plugin ID) used to send the message to the respective partition
            // AuditSourceBasedPartitioner is used to determine the partition to which the message will be sent
            // e.g. key = hiveServer2 -> message will be sent to partition allocated to hiveServer2

            final String key     = authzEvent.getAgentId();
            final String message = MiscUtil.stringify(event);

            try {
                if (topicName == null || kafkaProducer == null) {
                    init(props, propPrefix);
                }
                if (kafkaProducer != null) {
                    MiscUtil.executePrivilegedAction((PrivilegedExceptionAction<Void>) () -> {
                        AuditProducer.send(kafkaProducer, topicName, key, message);
                        return null;
                    });
                    ret = true;
                } else {
                    // Kafka producer not available - spool to file for recovery
                    LOG.warn("Kafka producer not available, spooling message to recovery");
                    spoolToRecovery(key, message);
                }
            } catch (Throwable t) {
                LOG.error("Error sending message to Kafka topic. topic={}, key={}, message={}", topicName, key, t.getMessage());
                // Spool to file for recovery
                spoolToRecovery(key, message);
            }
        }
        return ret;
    }

    @Override
    public boolean log(final Collection<AuditEventBase> events) {
        return false;
    }

    private void startRangerAuditRecoveryThread() {
        LOG.info("==> AuditMessageQueue.startRangerAuditRecoveryThread()");

        try {
            if (recoveryManager != null) {
                recoveryManager.start();
                LOG.info("Audit Recovery Manager started with writer and retry threads");
            } else {
                LOG.warn("==== Recovery manager is null; recovery threads not started");
            }
        } catch (Exception e) {
            LOG.error("Error starting Audit Recovery Manager", e);
        }

        LOG.info("<== AuditMessageQueue.startRangerAuditRecoveryThread()");
    }

    private void startRangerAuditProducer() {
        LOG.info("==> AuditMessageQueue.startRangerAuditProducer()");

        try {
            if (auditProducerRunnable != null) {
                producerThread = new Thread(auditProducerRunnable, "AuditProducer");
                producerThread.setDaemon(true);
                producerThread.start();
                LOG.info("==== AuditProducer Thread started: {}", producerThread.getName());
            } else {
                LOG.warn("AuditProducer runnable is null; producer thread not started");
            }
        } catch (Exception e) {
            LOG.error("Error Starting Ranger Audit Producer", e);
        }

        LOG.info("<== AuditMessageQueue.startRangerAuditProducer()");
    }

    private void startRangerAuditConsumers() {
        LOG.info("==> AuditMessageQueue.startRangerAuditConsumers()");

        // Log overall consumer startup summary
        logAuditConsumersStartupBanner();

        // Start Solr Consumer
        if (auditSolrConsumerRunnable != null) {
            try {
                Thread solrConsumerThread = new Thread(auditSolrConsumerRunnable, "AuditSolrConsumer");
                solrConsumerThread.setDaemon(true);
                solrConsumerThread.start();
                LOG.info("==== Solr Consumer Main Thread started [Thread-ID: {}, Thread-Name: '{}'] with Consumer group ID: {}",
                        solrConsumerThread.getId(), solrConsumerThread.getName(), auditSolrConsumerRunnable.getConsumerGroupId());
            } catch (Exception e) {
                LOG.error("Error starting Kafka Solr Consumer", e);
            }
        } else {
            LOG.info("==== Solr Consumer is DISABLED (not configured)");
        }

        // Start HDFS Consumer
        if (auditHDFSConsumerRunnable != null) {
            try {
                Thread hdfsConsumerThread = new Thread(auditHDFSConsumerRunnable, "AuditHDFSConsumer");
                hdfsConsumerThread.setDaemon(true);
                hdfsConsumerThread.start();
                LOG.info("==== HDFS Consumer Main Thread started [Thread-ID: {}, Thread-Name: '{}'] with Consumer group ID: {}",
                        hdfsConsumerThread.getId(), hdfsConsumerThread.getName(), auditHDFSConsumerRunnable.getConsumerGroupId());
            } catch (Exception e) {
                LOG.error("Error starting Kafka HDFS Consumer", e);
            }
        } else {
            LOG.info("==== HDFS Consumer is DISABLED (not configured)");
        }

        LOG.info("<== AuditMessageQueue.startRangerAuditConsumers()");
    }

    /**
     * Log an informative banner showing which audit consumers are being started
     */
    private void logAuditConsumersStartupBanner() {
        LOG.info("################## RANGER AUDIT CONSUMERS INITIALIZATION #######################");

        boolean solrEnabled = (auditSolrConsumerRunnable != null);
        boolean hdfsEnabled = (auditHDFSConsumerRunnable != null);

        AuditServerLogFormatter.builder("Audit Destination Status: ")
                .add("  - SOLR Consumer: {}", solrEnabled ? "ENABLED" : "DISABLED")
                .add("  - HDFS Consumer: {}", hdfsEnabled ? "ENABLED" : "DISABLED")
                .logInfo(LOG);

        if (solrEnabled || hdfsEnabled) {
            LOG.info("...... Starting audit consumer threads ......");
        } else {
            LOG.warn("WARNING: No audit consumers are enabled!");
        }
        LOG.info("################################################################################");
    }

    private void createKafkaProducer(final Properties props, final String propPrefix) {
        if (auditProducerRunnable == null) {
            auditProducerRunnable = auditMessageQueueUtils.createKafkaProducer(props, propPrefix);
            if (auditProducerRunnable != null) {
                kafkaProducer = auditProducerRunnable.getKafkaProducer();
            }
        }
    }

    private void createKafkaConsumers(final Properties props, final String propPrefix) {
        boolean isSolrConsumerEnabled = auditMessageQueueUtils.isSolrConsumerEnabled();
        if (isSolrConsumerEnabled && auditSolrConsumerRunnable == null) {
            auditSolrConsumerRunnable = auditMessageQueueUtils.createKafkaSolrConsumer(props, propPrefix);
        }

        boolean isHdfsConsumerEnabled = auditMessageQueueUtils.isHDFSConsumerEnabled();
        if (isHdfsConsumerEnabled && auditHDFSConsumerRunnable == null) {
            auditHDFSConsumerRunnable = auditMessageQueueUtils.createKafkaHDFSConsumer(props, propPrefix);
        }
    }

    private void createAuditsTopic(final Properties props, final String propPrefix) {
        if (topicName == null) {
            topicName = auditMessageQueueUtils.createAuditsTopicIfNotExists(props, propPrefix);
        }
    }

    private void createRecoveryManager(final Properties props, final String propPrefix) {
        if (recoveryManager == null && kafkaProducer != null && topicName != null) {
            try {
                recoveryManager = new AuditRecoveryManager(props, propPrefix, kafkaProducer, topicName);
                LOG.info("Created Audit Recovery Manager");
            } catch (Exception e) {
                LOG.error("Error creating Audit Recovery Manager", e);
            }
        }
    }

    /**
     * Spool failed audit message to recovery system
     */
    private void spoolToRecovery(String key, String message) {
        if (recoveryManager != null) {
            boolean queued = recoveryManager.addFailedMessage(key, message);
            if (queued) {
                LOG.debug("Spooled failed message to recovery system");
            } else {
                LOG.warn("Failed to spool message to recovery system - queue may be full or recovery disabled");
            }
        } else {
            LOG.warn("Recovery manager not initialized - cannot spool failed message");
        }
    }

    /**
     * Get recovery statistics for monitoring
     */
    public AuditRecoveryManager.RecoveryStats getRecoveryStats() {
        if (recoveryManager != null) {
            return recoveryManager.getStats();
        }
        return null;
    }
}
