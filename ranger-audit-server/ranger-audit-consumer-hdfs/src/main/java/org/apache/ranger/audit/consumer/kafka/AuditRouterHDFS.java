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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.ranger.audit.destination.HDFSAuditDestination;
import org.apache.ranger.audit.provider.MiscUtil;
import org.apache.ranger.audit.utils.AuditServerLogFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Router class that routes audit messages to different HDFSAuditDestination threads
 * based on the app_id.
 * Each app_id gets its own HDFSAuditDestination instance running in a separate thread.
 *
 * This router writes audits to HDFS as the rangerauditserver user. The audit folder in HDFS
 * should be configured with appropriate permissions to allow rangerauditserver to write audits
 * either by Ranger policy or by HDFS acl.
 */
public class AuditRouterHDFS {
    private static final Logger LOG = LoggerFactory.getLogger(AuditRouterHDFS.class);

    private final Map<String, HDFSAuditDestination> destinationMap = new ConcurrentHashMap<>();
    private final Map<String, ExecutorService>      executorMap    = new ConcurrentHashMap<>();
    private final ObjectMapper                      jsonMapper     = new ObjectMapper();
    private       Properties                        props;
    private       String                            hdfsPropPrefix;

    public AuditRouterHDFS() {
    }

    public void init(Properties props, String hdfsPropPrefix) {
        LOG.info("==> AuditRouterHDFS.init()");

        this.props          = new Properties();
        this.props.putAll(props);
        this.hdfsPropPrefix = hdfsPropPrefix;

        LOG.info("<== AuditRouterHDFS.init()");
    }

    /**
     * Routes audit message to appropriate HDFSAuditDestination based on app_id
     * @param message JSON audit message
     * @param partitionKey The partition key from Kafka (used as app_id)
     */
    public void routeAuditMessage(String message, String partitionKey) {
        LOG.debug("==> AuditRouterHDFS:routeAuditMessage(): Message => {}, partitionKey => {}", message, partitionKey);

        try {
            String appId         = extractAppId(message, partitionKey);
            String serviceType   = extractServiceType(message);
            String agentHostname = extractAgentHostname(message);

            LOG.debug("Routing audit message for app_id: {}, serviceType: {}, agentHostname: {}", appId, serviceType, agentHostname);

            // Get or create HDFSAuditDestination for the appId
            if (appId != null) {
                HDFSAuditDestination hdfsAuditDestination = getHdfsAuditDestination(appId, serviceType, agentHostname);

                // Submit message to destination's thread for processing
                final String finalAppId = appId;
                ExecutorService executor = executorMap.get(appId);

                if (executor != null && !executor.isShutdown()) {
                    executor.submit(() -> {
                        try {
                            hdfsAuditDestination.logJSON(Collections.singletonList(message));
                            LOG.debug("Successfully wrote audit for app_id: {}", finalAppId);
                        } catch (Exception e) {
                            LOG.error("Error processing audit message for app_id: {}", finalAppId, e);
                        }
                    });
                } else {
                    LOG.warn("Executor is null or shutdown for app_id: {}", appId);
                }
            }
        } catch (Exception e) {
            LOG.error("AuditRouterHDFS:routeAuditMessage(): Error routing audit message: {}", message, e);
        }

        LOG.debug("<== AuditRouterHDFS:routeAuditMessage()");
    }

    /**
     * Extract app_id from audit message. First tries partition key, then falls back to parsing JSON.
     */
    private String extractAppId(String message, String partitionKey) {
        // First try to use partition key as app_id if available
        if (partitionKey != null && !partitionKey.trim().isEmpty()) {
            return partitionKey;
        }
        // Fall back to extracting from JSON message
        try {
            JsonNode rootNode    = jsonMapper.readTree(message);
            JsonNode agentIdNode = rootNode.get("agent");
            if (agentIdNode != null) {
                return agentIdNode.asText();
            }
        } catch (Exception e) {
            LOG.debug("AuditRouterHDFS:extractAppId(): Failed to parse JSON message for app_id extraction: {}", e.getMessage());
        }
        return null;
    }

    /**
     * Extract serviceType from audit message additional_info
     */
    private String extractServiceType(String message) {
        try {
            JsonNode rootNode           = jsonMapper.readTree(message);
            JsonNode additionalInfoNode = rootNode.get("additional_info");
            JsonNode additionalInfoJson = jsonMapper.readTree(additionalInfoNode.asText());
            JsonNode serviceTypeNode = additionalInfoJson.get("serviceType");
            if (serviceTypeNode != null) {
                return serviceTypeNode.asText();
            }
        } catch (Exception e) {
            LOG.debug("AuditRouterHDFS:extractServiceType(): Failed to parse JSON message for serviceType extraction: {}", e.getMessage());
        }
        return null;
    }

    /**
     * Extract agentHostname from audit message
     */
    private String extractAgentHostname(String message) {
        try {
            JsonNode rootNode      = jsonMapper.readTree(message);
            JsonNode agentHostNode = rootNode.get("agentHost");
            if (agentHostNode != null) {
                return agentHostNode.asText();
            }
        } catch (Exception e) {
            LOG.debug("AuditRouterHDFS:extractAgentHostname(): Failed to parse JSON message for agentHostname extraction: {}", e.getMessage());
        }
        return null;
    }

    /**
     * Get or create HDFSAuditDestination for the given app_id
     */
    private synchronized HDFSAuditDestination getHdfsAuditDestination(String appId, String serviceType, String agentHostname) {
        LOG.debug("==> AuditRouterHDFS:getHdfsAuditDestination() for app_id: {}, serviceType: {} and agentHostname: {}", appId, serviceType, agentHostname);
        HDFSAuditDestination destination = destinationMap.get(appId);
        if (destination == null) {
            try {
                destination = createHDFSDestination(appId, serviceType, agentHostname);
                destinationMap.put(appId, destination);

                // Create dedicated executor for this destination
                ExecutorService executor = Executors.newSingleThreadExecutor(r -> {
                    Thread t = new Thread(r, "HDFSAuditDestination-" + appId);
                    t.setDaemon(true);
                    return t;
                });
                executorMap.put(appId, executor);
            } catch (Exception e) {
                LOG.error("Failed to create HDFSAuditDestination for app_id: {}", appId, e);
                throw new RuntimeException("AuditRouterHDFS:getOrCreateDestination() : Failed to create destination for app_id: " + appId, e);
            }
        }

        LOG.debug("<== AuditRouterHDFS:getHdfsAuditDestination()..got HDFSAuditDestination for app_id: {}", appId);

        return destination;
    }

    /**
     * Create and initialize HDFSAuditDestination with app_id specific configuration
     */
    private HDFSAuditDestination createHDFSDestination(String appId, String serviceType, String agentHostname) throws Exception {
        LOG.debug("==> AuditRouterHDFS:createHDFSDestination(): Creating new HDFSAuditDestination for app_id: {}, serviceType: {}, agentHostname: {}", appId, serviceType, agentHostname);

        HDFSAuditDestination destination = new HDFSAuditDestination();

        // Create app_id specific properties
        Properties appSpecificProps = new Properties();
        appSpecificProps.putAll(props);

        // Determine file type and writer implementation
        String fileType   = MiscUtil.getStringProperty(props, hdfsPropPrefix + ".batch.filequeue.filetype", "json");
        String writerImpl = getWriterImplementation(fileType);

        // Configure directory properties
        String baseDir    = MiscUtil.getStringProperty(props, hdfsPropPrefix + ".dir", "/ranger/audit/" + serviceType);
        String subDir     = MiscUtil.getStringProperty(props, hdfsPropPrefix + ".subdir", appId + "/%time:yyyyMMdd%/");

        // Set file extension based on file type
        String fileExtension  = getFileExtension(fileType);
        // Get unique instance identifier for filename uniqueness across scaled audit services
        String instanceId = getUniqueInstanceIdentifier();

        // Use agentHostname from audit message if available, otherwise fall back to local hostname token
        String hostnameValue = (agentHostname != null && !agentHostname.isEmpty()) ? agentHostname : "%hostname%";

        // Build default filename with agent hostname to properly identify the source system
        String defaultFileName = appId + "_ranger_audit_" + hostnameValue + "_" + instanceId + fileExtension;
        String fileNameFormat = MiscUtil.getStringProperty(props, hdfsPropPrefix + ".filename.format", defaultFileName);

        // If filename format contains %hostname% and we have agentHostname, replace it
        if (agentHostname != null && !agentHostname.isEmpty() && fileNameFormat.contains("%hostname%")) {
            fileNameFormat = fileNameFormat.replace("%hostname%", agentHostname);
        }

        // Set the enhanced properties
        appSpecificProps.setProperty(hdfsPropPrefix + ".dir", baseDir);
        appSpecificProps.setProperty(hdfsPropPrefix + ".subdir", subDir);
        appSpecificProps.setProperty(hdfsPropPrefix + ".filename.format", fileNameFormat);
        appSpecificProps.setProperty(hdfsPropPrefix + ".filewriter.impl", writerImpl);
        appSpecificProps.setProperty(hdfsPropPrefix + ".batch.filequeue.filetype", fileType);
        // Preserve other properties
        preserveFileSystemProperties(appSpecificProps, hdfsPropPrefix);

        // Log configuration
        AuditServerLogFormatter.builder("Initializing HDFSAuditDestination for app_id: " + appId)
                .add("Base directory", baseDir)
                .add("Subdirectory pattern", subDir)
                .add("Filename format", fileNameFormat)
                .add("Agent hostname", agentHostname)
                .add("Instance identifier", instanceId)
                .add("File type", fileType)
                .add("Writer implementation", writerImpl)
                .logInfo(LOG);

        destination.init(appSpecificProps, hdfsPropPrefix);
        destination.start();

        LOG.debug("<== AuditRouterHDFS:createHDFSDestination(): Created new HDFSDestination {}", destination.getName());

        return destination;
    }

    private String getWriterImplementation(String fileType) {
        switch (fileType.toLowerCase()) {
            case "orc":
                return "org.apache.ranger.audit.utils.RangerORCAuditWriter";
            case "json":
            default:
                return "org.apache.ranger.audit.utils.RangerJSONAuditWriter";
        }
    }

    private String getFileExtension(String fileType) {
        switch (fileType.toLowerCase()) {
            case "orc":
                return ".orc";
            case "json":
            default:
                return ".log";
        }
    }

    private void preserveFileSystemProperties(Properties appSpecificProps, String baseDestPrefix) {
        String fileRolloverSec = MiscUtil.getStringProperty(props, baseDestPrefix + ".file.rollover.sec");
        if (fileRolloverSec != null) {
            appSpecificProps.setProperty(baseDestPrefix + ".file.rollover.sec", fileRolloverSec);
        }

        String rolloverPeriod = MiscUtil.getStringProperty(props, baseDestPrefix + ".file.rollover.period");
        if (rolloverPeriod != null) {
            appSpecificProps.setProperty(baseDestPrefix + ".file.rollover.period", rolloverPeriod);
        }

        String appendEnabled = MiscUtil.getStringProperty(props, baseDestPrefix + ".file.append.enabled");
        if (appendEnabled != null) {
            appSpecificProps.setProperty(baseDestPrefix + ".file.append.enabled", appendEnabled);
        }

        String periodicRolloverEnabled = MiscUtil.getStringProperty(props, baseDestPrefix + ".file.rollover.enable.periodic.rollover");
        if (periodicRolloverEnabled != null) {
            appSpecificProps.setProperty(baseDestPrefix + ".file.rollover.enable.periodic.rollover", periodicRolloverEnabled);
        }

        String periodicRolloverCheckTime = MiscUtil.getStringProperty(props, baseDestPrefix + ".file.rollover.periodic.rollover.check.sec");
        if (periodicRolloverCheckTime != null) {
            appSpecificProps.setProperty(baseDestPrefix + ".file.rollover.periodic.rollover.check.sec", periodicRolloverCheckTime);
        }
    }

    private String getUniqueInstanceIdentifier() {
        String jvmInstanceId = MiscUtil.getJvmInstanceId();
        LOG.info("Using JVM instance ID as unique identifier: {}", jvmInstanceId);
        return jvmInstanceId;
    }

    public void shutdown() {
        LOG.info("==> AuditRouterHDFS.shutdown()");

        // Shutdown all executors
        for (Map.Entry<String, ExecutorService> entry : executorMap.entrySet()) {
            String appId = entry.getKey();
            ExecutorService executor = entry.getValue();

            LOG.info("Shutting down executor for app_id: {}", appId);
            executor.shutdown();

            try {
                if (!executor.awaitTermination(30, java.util.concurrent.TimeUnit.SECONDS)) {
                    LOG.warn("Executor for app_id {} did not terminate gracefully, forcing shutdown", appId);
                    executor.shutdownNow();
                }
            } catch (InterruptedException e) {
                LOG.warn("Interrupted while waiting for executor shutdown for app_id: {}", appId);
                executor.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }

        // Stop all destinations
        for (Map.Entry<String, HDFSAuditDestination> entry : destinationMap.entrySet()) {
            String appId = entry.getKey();
            HDFSAuditDestination destination = entry.getValue();

            LOG.info("Stopping HDFSAuditDestination for app_id: {}", appId);
            try {
                destination.stop();
            } catch (Exception e) {
                LOG.error("Error stopping HDFSAuditDestination for app_id: {}", appId, e);
            }
        }

        destinationMap.clear();
        executorMap.clear();

        LOG.info("<== AuditRouterHDFS.shutdown()");
    }
}
