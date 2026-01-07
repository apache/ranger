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

import org.apache.ranger.audit.provider.MiscUtil;
import org.apache.ranger.audit.utils.AuditServerLogFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * AuditRecoveryWriter - Thread that receives failed audit messages and writes them to local spool files with .failed file extension
 * Processed files are rotated based on time configuration and moved to archive with .processed file externsion after writing
 */
public class AuditRecoveryWriter implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(AuditRecoveryWriter.class);

    private static final String  PROP_RECOVERY_ENABLED              = "recovery.enabled";
    private static final String  PROP_SPOOL_DIR                     = "recovery.spool.dir";
    private static final String  PROP_FILE_ROTATION_INTERVAL_SEC    = "recovery.file.rotation.interval.sec";
    private static final String  PROP_MAX_MESSAGES_PER_FILE         = "recovery.max.messages.per.file";
    private static final boolean DEFAULT_RECOVERY_ENABLED           = true;
    private static final String  DEFAULT_SPOOL_DIR                  = "/var/log/audit-server/audit/spool";
    private static final long    DEFAULT_FILE_ROTATION_INTERVAL_SEC = 300;
    private static final int     DEFAULT_MAX_MESSAGES_PER_FILE      = 10000;

    private final BlockingQueue<FailedAuditMessage> messageQueue = new LinkedBlockingQueue<>(100000);
    private final Properties props;
    private final String propPrefix;

    private volatile boolean running = true;
    private          boolean enabled;
    private          String  spoolDir;
    private          long    fileRotationIntervalSec;
    private          int     maxMessagesPerFile;

    private BufferedWriter currentWriter;
    private String         currentFileName;
    private long           currentFileStartTime;
    private int            currentFileMessageCount;

    public AuditRecoveryWriter(Properties props, String propPrefix) {
        this.props      = new Properties();
        this.props.putAll(props);
        this.propPrefix = propPrefix;
        init();
    }

    private void init() {
        enabled                 = MiscUtil.getBooleanProperty(props, propPrefix + "." + PROP_RECOVERY_ENABLED, DEFAULT_RECOVERY_ENABLED);
        spoolDir                = MiscUtil.getStringProperty(props, propPrefix + "." + PROP_SPOOL_DIR, DEFAULT_SPOOL_DIR);
        fileRotationIntervalSec = MiscUtil.getLongProperty(props, propPrefix + "." + PROP_FILE_ROTATION_INTERVAL_SEC, DEFAULT_FILE_ROTATION_INTERVAL_SEC);
        maxMessagesPerFile      = MiscUtil.getIntProperty(props, propPrefix + "." + PROP_MAX_MESSAGES_PER_FILE, DEFAULT_MAX_MESSAGES_PER_FILE);

        AuditServerLogFormatter.builder("AuditRecoveryWriter Configuration:")
                .add("enabled: ", enabled)
                .add("spoolDir: ", spoolDir)
                .add("fileRotationIntervalSec: ", fileRotationIntervalSec)
                .add("enabled: ", enabled)
                .add("maxMessagesPerFile: ", maxMessagesPerFile);
    }

    public boolean isEnabled() {
        return enabled;
    }

    /**
     * Add a failed audit message to the queue for writing to spool file
     */
    public boolean addFailedMessage(String key, String message) {
        boolean ret;

        if (!enabled || !running) {
            ret = false;
        } else {
            try {
                FailedAuditMessage failedMsg = new FailedAuditMessage(key, message, System.currentTimeMillis());
                boolean added = messageQueue.offer(failedMsg, 1, TimeUnit.SECONDS);
                if (!added) {
                    LOG.warn("Failed to add message to recovery queue - queue is full");
                }
                ret = added;
            } catch (InterruptedException e) {
                LOG.warn("Interrupted while adding message to recovery queue", e);
                Thread.currentThread().interrupt();
                ret = false;
            }
        }

        return ret;
    }

    @Override
    public void run() {
        if (!enabled) {
            LOG.info("AuditRecoveryWriter is disabled. Thread will not start.");
            return;
        }

        LOG.info("==> AuditRecoveryWriter thread started");

        try {
            // Create spool directory
            try {
                createDirectories();
            } catch (IOException e) {
                LOG.error("Failed to create spool directory", e);
                return;
            }

            while (running) {
                try {
                    // Poll for messages with timeout
                    FailedAuditMessage failedMsg = messageQueue.poll(1, TimeUnit.SECONDS);

                    if (failedMsg != null) {
                        writeMessageToFile(failedMsg);
                    }

                    // Check if file needs rotation
                    checkAndRotateFile();
                } catch (InterruptedException e) {
                    if (running) {
                        LOG.warn("AuditRecoveryWriter interrupted", e);
                    }
                    Thread.currentThread().interrupt();
                    break;
                } catch (Exception e) {
                    LOG.error("Error in AuditRecoveryWriter loop", e);
                }
            }
        } finally {
            // Cleanup
            closeCurrentFile();
            LOG.info("<== AuditRecoveryWriter thread stopped");
        }
    }

    public void shutdown() {
        LOG.info("==> AuditRecoveryWriter.shutdown()");
        running = false;

        // Process remaining messages in queue
        int remainingMessages = messageQueue.size();
        if (remainingMessages > 0) {
            LOG.info("Processing {} remaining messages before shutdown", remainingMessages);

            long shutdownStartTime = System.currentTimeMillis();
            long shutdownTimeout = 30000; // 30 seconds

            while (!messageQueue.isEmpty() && (System.currentTimeMillis() - shutdownStartTime < shutdownTimeout)) {
                try {
                    FailedAuditMessage failedMsg = messageQueue.poll(100, TimeUnit.MILLISECONDS);
                    if (failedMsg != null) {
                        writeMessageToFile(failedMsg);
                    }
                } catch (Exception e) {
                    LOG.error("Error processing remaining messages during shutdown", e);
                    break;
                }
            }
        }

        // Close current file
        closeCurrentFile();

        LOG.info("<== AuditRecoveryWriter.shutdown() completed");
    }

    private void createDirectories() throws IOException {
        Path spoolPath = Paths.get(spoolDir);

        if (!Files.exists(spoolPath)) {
            Files.createDirectories(spoolPath);
            LOG.info("Created spool directory: {}", spoolDir);
        }
    }

    private void writeMessageToFile(FailedAuditMessage failedMsg) {
        try {
            // Open new file if needed
            if (currentWriter == null) {
                openNewFile();
            }

            // Write message: timestamp|key|message
            String line = String.format("%d|%s|%s%n", failedMsg.timestamp, failedMsg.key != null ? failedMsg.key : "", failedMsg.message);

            currentWriter.write(line);
            currentFileMessageCount++;

            // Flush periodically for safety
            if (currentFileMessageCount % 100 == 0) {
                currentWriter.flush();
            }
        } catch (IOException e) {
            LOG.error("Error writing message to spool file: {}", currentFileName, e);
            // Try to close and reopen file
            closeCurrentFile();
        }
    }

    private void openNewFile() throws IOException {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd_HHmmss_SSS");
        String timestamp = sdf.format(new Date());
        // Use .tmp extension while writing to prevent retry thread from picking it up
        currentFileName         = String.format("spool_audit_%s.tmp", timestamp);
        String filePath         = Paths.get(spoolDir, currentFileName).toString();
        currentWriter           = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(filePath, true), "UTF-8"));
        currentFileStartTime    = System.currentTimeMillis();
        currentFileMessageCount = 0;

        LOG.info("Opened new spool file (temp): {}", currentFileName);
    }

    private void checkAndRotateFile() {
        if (currentWriter == null) {
            return;
        }

        long currentTime = System.currentTimeMillis();
        long elapsedSec  = (currentTime - currentFileStartTime) / 1000;

        // Rotate if time elapsed or message count exceeded
        boolean rotateByTime = elapsedSec >= fileRotationIntervalSec;
        boolean rotateByCount = currentFileMessageCount >= maxMessagesPerFile;

        if (rotateByTime || rotateByCount) {
            if (rotateByTime) {
                LOG.info("Rotating spool file due to time: {} seconds elapsed", elapsedSec);
            }
            if (rotateByCount) {
                LOG.info("Rotating spool file due to message count: {} messages written", currentFileMessageCount);
            }

            closeCurrentFile();
        }
    }

    private void closeCurrentFile() {
        if (currentWriter != null) {
            String tempFileName = currentFileName;
            try {
                currentWriter.flush();
                currentWriter.close();

                // Rename from .tmp to .failed to signal it's ready for retry
                String failedFileName = tempFileName.replace(".tmp", ".failed");
                Path   tempPath       = Paths.get(spoolDir, tempFileName);
                Path   failedPath     = Paths.get(spoolDir, failedFileName);

                Files.move(tempPath, failedPath);

                LOG.info("Closed and finalized spool file: {} ({} messages) - Ready for retry processing", failedFileName, currentFileMessageCount);
            } catch (IOException e) {
                LOG.error("Error closing/renaming spool file: {}", tempFileName, e);
            } finally {
                currentWriter           = null;
                currentFileName         = null;
                currentFileStartTime    = 0;
                currentFileMessageCount = 0;
            }
        }
    }

    private static class FailedAuditMessage {
        final String key;
        final String message;
        final long   timestamp;

        FailedAuditMessage(String key, String message, long timestamp) {
            this.key       = key;
            this.message   = message;
            this.timestamp = timestamp;
        }
    }

    public int getQueueSize() {
        return messageQueue.size();
    }

    public String getCurrentFileName() {
        return currentFileName;
    }

    public int getCurrentFileMessageCount() {
        return currentFileMessageCount;
    }
}
