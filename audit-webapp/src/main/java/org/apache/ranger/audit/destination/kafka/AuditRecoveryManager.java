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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * AuditRecoveryManager - Manages the audit recovery system with two threads:
 * 1. AuditRecoveryWriter - Writes failed messages to spool files with .failed file extension
 * 2. AuditRecoveryRetry - Reads .failed files and retries to Kafka and if successful move to archive folder with .processed file extension
 */
public class AuditRecoveryManager {
    private static final Logger LOG = LoggerFactory.getLogger(AuditRecoveryManager.class);

    private final AuditRecoveryWriter recoveryWriter;
    private final AuditRecoveryRetry  recoveryRetry;
    private       Thread              writerThread;
    private       Thread              retryThread;
    private volatile boolean          started;

    public AuditRecoveryManager(Properties props, String propPrefix, KafkaProducer<String, String> kafkaProducer, String topicName) {
        LOG.info("==> AuditRecoveryManager()");

        // Initialize writer and retry components
        this.recoveryWriter = new AuditRecoveryWriter(props, propPrefix);
        this.recoveryRetry  = new AuditRecoveryRetry(props, propPrefix, kafkaProducer, topicName);

        LOG.info("<== AuditRecoveryManager() initialized");
    }

    /**
     * Start the recovery threads
     */
    public void start() {
        if (started) {
            LOG.info("AuditRecoveryManager already started");
            return;
        }

        LOG.info("==> AuditRecoveryManager.start()");

        try {
            // Start recovery writer thread
            if (recoveryWriter.isEnabled()) {
                writerThread = new Thread(recoveryWriter, "AuditRecoveryWriter");
                writerThread.setDaemon(true);
                writerThread.start();
                LOG.info("Started AuditRecoveryWriter thread");
            } else {
                LOG.info("AuditRecoveryWriter is disabled");
            }

            // Start recovery retry thread
            if (recoveryRetry.isEnabled()) {
                retryThread = new Thread(recoveryRetry, "AuditRecoveryRetry");
                retryThread.setDaemon(true);
                retryThread.start();
                LOG.info("Started AuditRecoveryRetry thread");
            } else {
                LOG.info("AuditRecoveryRetry is disabled");
            }

            started = true;
            LOG.info("<== AuditRecoveryManager.start() completed successfully");
        } catch (Exception e) {
            LOG.error("Error starting AuditRecoveryManager", e);
            throw new RuntimeException("Failed to start AuditRecoveryManager", e);
        }
    }

    /**
     * Stop the recovery threads gracefully
     */
    public void stop() {
        if (!started) {
            LOG.warn("AuditRecoveryManager not started");
            return;
        }

        LOG.info("==> AuditRecoveryManager.stop()");

        try {
            // Shutdown recovery writer
            if (recoveryWriter != null) {
                LOG.info("Shutting down AuditRecoveryWriter...");
                recoveryWriter.shutdown();

                if (writerThread != null && writerThread.isAlive()) {
                    writerThread.interrupt();
                    try {
                        writerThread.join(10000); // Wait up to 10 seconds
                        if (writerThread.isAlive()) {
                            LOG.warn("AuditRecoveryWriter thread did not terminate within 10 seconds");
                        } else {
                            LOG.info("AuditRecoveryWriter thread terminated successfully");
                        }
                    } catch (InterruptedException e) {
                        LOG.warn("Interrupted while waiting for writer thread to terminate", e);
                        Thread.currentThread().interrupt();
                    }
                }
            }

            // Shutdown recovery retry
            if (recoveryRetry != null) {
                LOG.info("Shutting down AuditRecoveryRetry...");
                recoveryRetry.shutdown();

                if (retryThread != null && retryThread.isAlive()) {
                    retryThread.interrupt();
                    try {
                        //Wait up to 10 seconds
                        retryThread.join(10000);
                        if (retryThread.isAlive()) {
                            LOG.warn("AuditRecoveryRetry thread did not terminate within 10 seconds");
                        } else {
                            LOG.info("AuditRecoveryRetry thread terminated successfully");
                        }
                    } catch (InterruptedException e) {
                        LOG.warn("Interrupted while waiting for retry thread to terminate", e);
                        Thread.currentThread().interrupt();
                    }
                }
            }

            started = false;

            LOG.info("<== AuditRecoveryManager.stop() completed");
        } catch (Exception e) {
            LOG.error("Error stopping AuditRecoveryManager", e);
        }
    }

    /**
     * Add a failed audit message to the recovery queue
     * @param key The partition key (agentId)
     * @param message The serialized audit message
     * @return true if message was queued successfully
     */
    public boolean addFailedMessage(String key, String message) {
        boolean ret;
        if (recoveryWriter == null || !recoveryWriter.isEnabled()) {
            ret = false;
        } else  {
            ret = recoveryWriter.addFailedMessage(key, message);
        }
        return ret;
    }

    /**
     * Get recovery statistics for monitoring
     */
    public RecoveryStats getStats() {
        RecoveryStats ret = new RecoveryStats();

        if (recoveryWriter != null) {
            ret.writerQueueSize    = recoveryWriter.getQueueSize();
            ret.writerCurrentFile  = recoveryWriter.getCurrentFileName();
            ret.writerMessageCount = recoveryWriter.getCurrentFileMessageCount();
        }

        if (recoveryRetry != null) {
            ret.retryTotalRetried   = recoveryRetry.getTotalMessagesRetried();
            ret.retryTotalSucceeded = recoveryRetry.getTotalMessagesSucceeded();
            ret.retryTotalFailed    = recoveryRetry.getTotalMessagesFailed();
        }

        return ret;
    }

    /**
     * Check if recovery system is enabled and running
     */
    public boolean isRunning() {
        return started;
    }

    /**
     * Statistics class for monitoring
     */
    public static class RecoveryStats {
        public int    writerQueueSize;
        public int    writerMessageCount;
        public String writerCurrentFile;
        public long   retryTotalRetried;
        public long   retryTotalSucceeded;
        public long   retryTotalFailed;

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder("RecoveryStats{")
                    .append(" writerQueue= ").append(writerQueueSize)
                    .append(" writerFile= ").append(writerCurrentFile)
                    .append(" writerMsgCount= ").append(writerMessageCount)
                    .append(" retryRetried= ").append(retryTotalRetried)
                    .append(" retrySucceeded= ").append(retryTotalSucceeded)
                    .append(" retryFailed= ").append(retryTotalFailed);
            return sb.toString();
        }
    }
}
