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
import org.apache.ranger.audit.provider.MiscUtil;
import org.apache.ranger.audit.utils.AuditServerLogFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.PrivilegedExceptionAction;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Properties;

/**
 * AuditRecoveryRetry - Thread that reads .failed files from spool directory and retries sending to Kafka
 * Successfully processed files are moved to archive directory with .processed extension
 * Manages archive retention by deleting old .processed files
 */
public class AuditRecoveryRetry implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(AuditRecoveryRetry.class);

    // Configuration keys
    private static final String  PROP_RECOVERY_ENABLED      = "recovery.enabled";
    private static final String  PROP_SPOOL_DIR             = "recovery.spool.dir";
    private static final String  PROP_ARCHIVE_DIR           = "recovery.archive.dir";
    private static final String  PROP_RETRY_INTERVAL_SEC    = "recovery.retry.interval.sec";
    private static final String  PROP_MAX_ARCHIVE_FILES     = "recovery.archive.max.processed.files";
    private static final String  PROP_RETRY_MAX_ATTEMPTS    = "recovery.retry.max.attempts";
    private static final boolean DEFAULT_RECOVERY_ENABLED   = true;
    private static final String  DEFAULT_SPOOL_DIR          = "/var/log/audit-server/audit/spool";
    private static final String  DEFAULT_ARCHIVE_DIR        = "/var/log/audit-server/audit/archive";
    private static final long    DEFAULT_RETRY_INTERVAL_SEC = 60; // 1 minute
    private static final int     DEFAULT_MAX_ARCHIVE_FILES  = 100;
    private static final int     DEFAULT_RETRY_MAX_ATTEMPTS = 3;

    private final Properties                    props;
    private final String                        propPrefix;
    private final String                        topicName;
    private final KafkaProducer<String, String> kafkaProducer;

    private volatile boolean running = true;
    private          boolean enabled;
    private          String  spoolDir;
    private          String  archiveDir;
    private          long    retryIntervalSec;
    private          long    totalMessagesRetried;
    private          long    totalMessagesSucceeded;
    private          long    totalMessagesFailed;
    private          int     maxArchiveFiles;
    private          int     retryMaxAttempts;

    public AuditRecoveryRetry(Properties props, String propPrefix, KafkaProducer<String, String> kafkaProducer, String topicName) {
        this.props         = new Properties();
        this.props.putAll(props);
        this.propPrefix    = propPrefix;
        this.kafkaProducer = kafkaProducer;
        this.topicName     = topicName;

        init();
    }

    private void init() {
        enabled          = MiscUtil.getBooleanProperty(props, propPrefix + "." + PROP_RECOVERY_ENABLED, DEFAULT_RECOVERY_ENABLED);
        spoolDir         = MiscUtil.getStringProperty(props, propPrefix + "." + PROP_SPOOL_DIR, DEFAULT_SPOOL_DIR);
        archiveDir       = MiscUtil.getStringProperty(props, propPrefix + "." + PROP_ARCHIVE_DIR, DEFAULT_ARCHIVE_DIR);
        retryIntervalSec = MiscUtil.getLongProperty(props, propPrefix + "." + PROP_RETRY_INTERVAL_SEC, DEFAULT_RETRY_INTERVAL_SEC);
        maxArchiveFiles  = MiscUtil.getIntProperty(props, propPrefix + "." + PROP_MAX_ARCHIVE_FILES, DEFAULT_MAX_ARCHIVE_FILES);
        retryMaxAttempts = MiscUtil.getIntProperty(props, propPrefix + "." + PROP_RETRY_MAX_ATTEMPTS, DEFAULT_RETRY_MAX_ATTEMPTS);

        AuditServerLogFormatter.builder("AuditRecoveryRetry Configuration:")
                .add("enabled: ", enabled)
                .add("spoolDir: ", spoolDir)
                .add("archiveDir: ", archiveDir)
                .add("retryIntervalSec: ", retryIntervalSec)
                .add("maxArchiveFiles: ", maxArchiveFiles)
                .add("retryMaxAttempts: ", retryMaxAttempts)
                .logInfo(LOG);
    }

    public boolean isEnabled() {
        return enabled;
    }

    @Override
    public void run() {
        if (!enabled) {
            LOG.info("AuditRecoveryRetry is disabled. Thread will not start.");
            return;
        }

        LOG.info("==> AuditRecoveryRetry thread started");

        try {
            // Create archive directory if needed
            try {
                createDirectories();
            } catch (IOException e) {
                LOG.error("Failed to create archive directory", e);
                return;
            }

            while (running) {
                try {
                    // Check and clean up archive
                    cleanupArchive();

                    // Process failed files from spool directory
                    processFailedFiles();

                    // Sleep before next retry cycle
                    Thread.sleep(retryIntervalSec * 1000);
                } catch (InterruptedException e) {
                    if (running) {
                        LOG.warn("AuditRecoveryRetry interrupted", e);
                    }
                    Thread.currentThread().interrupt();
                    break;
                } catch (Exception e) {
                    LOG.error("Error in AuditRecoveryRetry loop", e);
                }
            }
        } finally {
            LOG.info("<== AuditRecoveryRetry thread stopped");
            LOG.info("Recovery Statistics - Retried: {}, Succeeded: {}, Failed: {}", totalMessagesRetried, totalMessagesSucceeded, totalMessagesFailed);
        }
    }

    private void createDirectories() throws IOException {
        Path archivePath = Paths.get(archiveDir);
        if (!Files.exists(archivePath)) {
            Files.createDirectories(archivePath);
            LOG.info("Created archive directory: {}", archiveDir);
        }
    }

    private void processFailedFiles() {
        File spoolDirectory = new File(spoolDir);
        File[] files = spoolDirectory.listFiles((dir, name) -> name.startsWith("spool_audit_") && name.endsWith(".failed"));

        if (files == null || files.length == 0) {
            LOG.debug("No failed files to process in spool directory");
            return;
        }

        // Sort files by modification time (oldest first)
        Arrays.sort(files, Comparator.comparingLong(File::lastModified));

        LOG.info("Processing {} failed files from spool directory", files.length);

        for (File file : files) {
            if (!running) {
                break;
            }

            try {
                processFailedFile(file);
            } catch (Exception e) {
                LOG.error("Error processing failed file: {}", file.getName(), e);
            }
        }
    }

    private void processFailedFile(File file) {
        LOG.info("Processing failed file: {}", file.getName());

        int messagesProcessed = 0;
        int messagesSucceeded = 0;
        int messagesFailed = 0;

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(file), "UTF-8"))) {
            String line;

            while ((line = reader.readLine()) != null && running) {
                if (line.trim().isEmpty()) {
                    continue;
                }

                try {
                    // Parse line: timestamp|key|message
                    String[] parts = line.split("\\|", 3);
                    if (parts.length < 3) {
                        LOG.warn("Invalid line format in file {}: {}", file.getName(), line);
                        continue;
                    }

                    // parts[0] is timestamp - not used but kept for future metrics
                    String key = parts[1].isEmpty() ? null : parts[1];
                    String message = parts[2];

                    messagesProcessed++;
                    totalMessagesRetried++;

                    // Retry sending to Kafka
                    boolean success = retrySendToKafka(key, message);

                    if (success) {
                        messagesSucceeded++;
                        totalMessagesSucceeded++;
                    } else {
                        messagesFailed++;
                        totalMessagesFailed++;
                    }
                } catch (Exception e) {
                    LOG.error("Error processing line from file {}: {}", file.getName(), line, e);
                    messagesFailed++;
                    totalMessagesFailed++;
                }
            }

            LOG.info("Completed processing file: {} - Processed: {}, Succeeded: {}, Failed: {}",
                    file.getName(), messagesProcessed, messagesSucceeded, messagesFailed);

            // Move file to archive if all messages succeeded
            if (messagesFailed == 0 && messagesSucceeded > 0) {
                moveToArchive(file);
            } else if (messagesFailed > 0) {
                // Keep file in spool directory as .failed for retry in next cycle
                LOG.warn("File {} has {} failed messages - keeping in spool for retry",
                        file.getName(), messagesFailed);
            }
        } catch (IOException e) {
            LOG.error("Error reading failed file: {}", file.getName(), e);
        }
    }

    private boolean retrySendToKafka(String key, String message) {
        if (kafkaProducer == null) {
            LOG.warn("Kafka producer is null, cannot retry message");
            return false;
        }

        for (int attempt = 1; attempt <= retryMaxAttempts; attempt++) {
            try {
                MiscUtil.executePrivilegedAction((PrivilegedExceptionAction<Void>) () -> {
                    AuditProducer.send(kafkaProducer, topicName, key, message);
                    return null;
                });

                LOG.debug("Successfully retried message to Kafka (attempt {})", attempt);
                return true;
            } catch (Exception e) {
                LOG.warn("Failed to retry message to Kafka (attempt {}): {}", attempt, e.getMessage());
                if (attempt < retryMaxAttempts) {
                    try {
                        // Wait before retry with exponential backoff
                        long waitTime = (long) Math.min(1000 * Math.pow(2, attempt - 1), 10000);
                        Thread.sleep(waitTime);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        return false;
                    }
                }
            }
        }

        LOG.error("Failed to retry message after {} attempts", retryMaxAttempts);
        return false;
    }

    private void moveToArchive(File file) {
        try {
            // Change extension from .failed to .processed
            String processedFileName = file.getName().replace(".failed", ".processed");
            Path targetPath = Paths.get(archiveDir, processedFileName);

            if (Files.move(file.toPath(), targetPath) != null) {
                LOG.info("Moved successfully processed file to archive: {} -> {}",
                        file.getName(), processedFileName);
            } else {
                LOG.warn("Failed to move file to archive: {}", file.getName());
            }
        } catch (Exception e) {
            LOG.error("Error moving file to archive: {}", file.getName(), e);
        }
    }

    private void cleanupArchive() {
        try {
            File archiveDirectory = new File(archiveDir);
            File[] files = archiveDirectory.listFiles((dir, name) ->
                    (name.startsWith("spool_audit_") && name.endsWith(".processed")));

            if (files == null || files.length <= maxArchiveFiles) {
                LOG.debug("Archive cleanup: {} .processed files, max: {} - no cleanup needed",
                        files != null ? files.length : 0, maxArchiveFiles);
                return;
            }

            // Sort by modification time (oldest first)
            Arrays.sort(files, Comparator.comparingLong(File::lastModified));

            // Delete oldest .processed files beyond retention limit
            int filesToDelete = files.length - maxArchiveFiles;
            LOG.info("Archive cleanup: Deleting {} oldest .processed files (current: {}, max: {})",
                    filesToDelete, files.length, maxArchiveFiles);

            for (int i = 0; i < filesToDelete; i++) {
                File file = files[i];
                try {
                    if (file.delete()) {
                        LOG.info("Deleted old processed archive file: {}", file.getName());
                    } else {
                        LOG.warn("Failed to delete old processed archive file: {}", file.getName());
                    }
                } catch (Exception e) {
                    LOG.error("Error deleting old processed archive file: {}", file.getName(), e);
                }
            }
        } catch (Exception e) {
            LOG.error("Error during archive cleanup", e);
        }
    }

    public void shutdown() {
        LOG.info("==> AuditRecoveryRetry.shutdown()");
        running = false;
        LOG.info("<== AuditRecoveryRetry.shutdown() completed");
    }

    // Getters for metrics/monitoring
    public long getTotalMessagesRetried() {
        return totalMessagesRetried;
    }

    public long getTotalMessagesSucceeded() {
        return totalMessagesSucceeded;
    }

    public long getTotalMessagesFailed() {
        return totalMessagesFailed;
    }
}
