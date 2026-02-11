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

package org.apache.ranger.audit.producer.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.ranger.audit.destination.AuditDestination;
import org.apache.ranger.audit.model.AuditEventBase;
import org.apache.ranger.audit.model.AuthzAuditEvent;
import org.apache.ranger.audit.provider.MiscUtil;
import org.apache.ranger.audit.utils.AuditMessageQueueUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.PrivilegedExceptionAction;
import java.util.Collection;
import java.util.Properties;

/**
 * AuditMessageQueue creates the necessary kafka queue for http post to relay the audit events into kafka.
 * It creates the necessary audit topics, producer threads and recovery threads.
 */
public class AuditMessageQueue extends AuditDestination {
    public KafkaProducer<String, String> kafkaProducer;
    public AuditProducer                 auditProducerRunnable;
    public AuditMessageQueueUtils        auditMessageQueueUtils;
    public String                        topicName;

    private Thread                       producerThread;
    private AuditRecoveryManager         recoveryManager;

    private static final Logger LOG = LoggerFactory.getLogger(AuditMessageQueue.class);

    @Override
    public void init(Properties props, String propPrefix) {
        LOG.info("==> AuditMessageQueue.init() [CORE AUDIT SERVER]");

        super.init(props, propPrefix);

        auditMessageQueueUtils = new AuditMessageQueueUtils(props);

        createAuditsTopic(props, propPrefix);
        createKafkaProducer(props, propPrefix);
        createRecoveryManager(props, propPrefix);

        LOG.info("<== AuditMessageQueue.init() [CORE AUDIT SERVER]: created topic: {}, producer: {}",
                topicName, (kafkaProducer != null) ? kafkaProducer.getClass() : "");
    }

    @Override
    public void start() {
        LOG.debug("==> AuditMessageQueue.start() - Starting Audit Producer and Recovery threads");
        startRangerAuditRecoveryThread();
        startRangerAuditProducer();
        LOG.debug("<== AuditMessageQueue.start()");
    }

    @Override
    public void stop() {
        LOG.info("==> AuditMessageQueue.stop() [CORE AUDIT SERVER]");

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

        // Close producer
        if (kafkaProducer != null) {
            try {
                LOG.info("Closing Kafka producer...");
                kafkaProducer.close();
                LOG.info("Kafka producer shutdown completed");
            } catch (Exception e) {
                LOG.error("Error shutting down Kafka producer", e);
            }
        }

        LOG.info("<== AuditMessageQueue.stop() [CORE AUDIT SERVER]");
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

            // Partition key is agentId (aka plugin ID) used by Kafka's default partitioner
            // for load balancing across partitions. Messages with the same key go to the same partition,
            // providing ordering guarantees per appId while enabling full horizontal scaling

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

    private void createKafkaProducer(final Properties props, final String propPrefix) {
        if (auditProducerRunnable == null) {
            try {
                auditProducerRunnable = new AuditProducer(props, propPrefix);
                if (auditProducerRunnable != null) {
                    kafkaProducer = auditProducerRunnable.getKafkaProducer();
                }
            } catch (Exception e) {
                LOG.error("Error creating Kafka producer", e);
            }
        }
    }

    private void createAuditsTopic(final Properties props, final String propPrefix) {
        if (topicName == null) {
            topicName = auditMessageQueueUtils.createAuditsTopicIfNotExists(props, propPrefix);
        }
    }

    private void createRecoveryManager(final Properties props, final String propPrefix) {
        // Create recovery manager even if kafkaProducer is null - it will handle null producer gracefully
        // This ensures audits are spooled when Kafka is unavailable during startup
        if (recoveryManager == null && topicName != null) {
            try {
                recoveryManager = new AuditRecoveryManager(props, propPrefix, this, topicName);
                LOG.info("Created Audit Recovery Manager (Kafka producer available: {})", (kafkaProducer != null));
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
