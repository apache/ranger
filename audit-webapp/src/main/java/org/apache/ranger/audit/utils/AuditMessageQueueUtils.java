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

package org.apache.ranger.audit.utils;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.security.SecureClientLogin;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreatePartitionsResult;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.ranger.audit.destination.kafka.AuditHDFSConsumer;
import org.apache.ranger.audit.destination.kafka.AuditProducer;
import org.apache.ranger.audit.destination.kafka.AuditSolrConsumer;
import org.apache.ranger.audit.provider.AuditProviderFactory;
import org.apache.ranger.audit.provider.MiscUtil;
import org.apache.ranger.audit.server.AuditServerConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

public class AuditMessageQueueUtils {
    private static final Logger LOG = LoggerFactory.getLogger(AuditMessageQueueUtils.class);

    AuditServerUtils auditServerUtils = new AuditServerUtils();
    boolean          isTopicReady;
    boolean          isSolrConsumerEnabled;
    boolean          isHDFSConsumerEnabled;

    public AuditMessageQueueUtils(Properties props) {
        isHDFSConsumerEnabled = MiscUtil.getBooleanProperty(props, AuditProviderFactory.AUDIT_DEST_BASE + "." + AuditServerConstants.PROP_HDFS_DEST_PREFIX, false);
        isSolrConsumerEnabled = MiscUtil.getBooleanProperty(props, AuditProviderFactory.AUDIT_DEST_BASE + "." + AuditServerConstants.PROP_SOLR_DEST_PREFIX, false);
    }

    public String createAuditsTopicIfNotExists(Properties props, String propPrefix) {
        LOG.info("==> AuditMessageQueueUtils:createAuditsTopicIfNotExists()");

        String ret                   = null;
        String topicName             = MiscUtil.getStringProperty(props, propPrefix + "." + AuditServerConstants.PROP_TOPIC_NAME, AuditServerConstants.DEFAULT_TOPIC);
        String bootstrapServers      = MiscUtil.getStringProperty(props, propPrefix + "." + AuditServerConstants.PROP_BOOTSTRAP_SERVERS);
        String securityProtocol      = MiscUtil.getStringProperty(props, propPrefix + "." + AuditServerConstants.PROP_SECURITY_PROTOCOL, AuditServerConstants.DEFAULT_SECURITY_PROTOCOL);
        String saslMechanism         = MiscUtil.getStringProperty(props, propPrefix + "." + AuditServerConstants.PROP_SASL_MECHANISM, AuditServerConstants.DEFAULT_SASL_MECHANISM);
        int    connMaxIdleTimeoutMS  = MiscUtil.getIntProperty(props, propPrefix + "." + AuditServerConstants.PROP_CONN_MAX_IDEAL_MS, 10000);
        int    partitions            = getPartitions(props, propPrefix);
        short  replicationFactor     = (short) MiscUtil.getIntProperty(props, propPrefix + "." + AuditServerConstants.PROP_REPLICATION_FACTOR, AuditServerConstants.DEFAULT_REPLICATION_FACTOR);
        int    reqTimeoutMS          = MiscUtil.getIntProperty(props, propPrefix + "." + AuditServerConstants.PROP_REQ_TIMEOUT_MS, 5000);

        // Retry configuration for Kafka connection during startup
        int maxRetries     = MiscUtil.getIntProperty(props, propPrefix + "." + AuditServerConstants.PROP_KAFKA_STARTUP_MAX_RETRIES, 10);
        int retryDelayMs   = MiscUtil.getIntProperty(props, propPrefix + "."+ AuditServerConstants.PROP_KAFKA_STARTUP_RETRY_DELAY_MS, 3000);
        int currentAttempt = 0;

        Map<String, Object> kafkaProp = new HashMap<>();
        kafkaProp.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        kafkaProp.put(AuditServerConstants.PROP_SASL_MECHANISM, saslMechanism);
        kafkaProp.put(AdminClientConfig.SECURITY_PROTOCOL_CONFIG, securityProtocol);

        if (securityProtocol != null && securityProtocol.toUpperCase().contains("SASL")) {
            kafkaProp.put(AuditServerConstants.PROP_SASL_JAAS_CONFIG, getJAASConfig(props, propPrefix));
        }

        kafkaProp.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, reqTimeoutMS);
        kafkaProp.put(AdminClientConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG, connMaxIdleTimeoutMS);

        while (currentAttempt <= maxRetries && ret == null) {
            currentAttempt++;

            try (AdminClient admin = AdminClient.create(kafkaProp)) {
                if (currentAttempt > 1) {
                    LOG.info("Attempting to connect to Kafka (attempt {}/{})", currentAttempt, maxRetries + 1);
                }

                Set<String> names = admin.listTopics().names().get();
                if (!names.contains(topicName)) {
                    NewTopic topic = new NewTopic(topicName, partitions, replicationFactor);
                    admin.createTopics(Collections.singletonList(topic)).all().get();
                    ret = topic.name();
                    LOG.info("Creating topic '{}' with {} partitions and replication factor {}", topicName, partitions, replicationFactor);
                    // Wait for metadata to propagate across the cluster
                    boolean isTopicReady = auditServerUtils.waitUntilTopicReady(admin, topicName, Duration.ofSeconds(60));
                    if (isTopicReady) {
                        try {
                            DescribeTopicsResult result           = admin.describeTopics(Collections.singletonList(topicName));
                            TopicDescription     topicDescription = result.values().get(topicName).get();
                            ret = topicDescription.name();

                            int partitionCount = topicDescription.partitions().size();
                            setTopicReady(isTopicReady);

                            LOG.info("Topic: {} successfully created with {} partitions", ret, partitionCount);
                        } catch (Exception e) {
                            throw new RuntimeException("Failed to fetch metadata for topic:" + topicName, e);
                        }
                    }
                } else {
                    /***
                     * Topic already existing. Check and update number of partitions for audit server. This is for upgrade
                     * from existing audit mechanism to audit server
                     */
                    ret = updateExistingTopicPartitions(admin, topicName, partitions, replicationFactor);
                }
            } catch (Exception ex) {
                if (currentAttempt <= maxRetries) {
                    LOG.warn("AuditMessageQueueUtils:createAuditsTopicIfNotExists(): Failed to connect to Kafka on attempt {}/{}. Retrying in {} ms. Error: {}", 
                             currentAttempt, maxRetries + 1, retryDelayMs, ex.getMessage());
                    try {
                        Thread.sleep(retryDelayMs);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        LOG.error("Interrupted while waiting to retry Kafka connection", ie);
                        break;
                    }
                } else {
                    LOG.error("AuditMessageQueueUtils:createAuditsTopicIfNotExists(): Error creating topic after {} attempts", currentAttempt, ex);
                }
            }
        }

        LOG.info("<== AuditMessageQueueUtils:createAuditsTopicIfNotExists() ret: {}", ret);

        return ret;
    }

    public AuditProducer createKafkaProducer(Properties props, String propPrefix) {
        LOG.info("==> AuditMessageQueueUtils:createKafkaProducer()");

        AuditProducer ret = null;
        try {
            ret = new AuditProducer(props, propPrefix);
        } catch (Exception ex) {
            LOG.error("AuditMessageQueueUtils:createKafkaProducer(): Error creating KafkaProducer", ex);
        }

        LOG.info("<== AuditMessageQueueUtils:createKafkaProducer() ret: {}", ret);

        return ret;
    }

    public AuditSolrConsumer createKafkaSolrConsumer(Properties props, String propPrefix) {
        LOG.info("==> AuditMessageQueueUtils:createKafkaSolrConsumer()");

        AuditSolrConsumer ret = null;
        try {
            ret = new AuditSolrConsumer(props, propPrefix);
        } catch (Exception ex) {
            LOG.error("AuditMessageQueueUtils:createKafkaSolrConsumer(): Error creating AuditSolrConsumer", ex);
        }

        LOG.info("<== AuditMessageQueueUtils:createKafkaSolrConsumer() ret: {}", ret);

        return ret;
    }

    public AuditHDFSConsumer createKafkaHDFSConsumer(Properties props, String propPrefix) {
        LOG.info("==> AuditMessageQueueUtils:createKafkaHDFSConsumer()");

        AuditHDFSConsumer ret = null;
        try {
            ret = new AuditHDFSConsumer(props, propPrefix);
        } catch (Exception ex) {
            LOG.error("AuditMessageQueueUtils:createKafkaHDFSConsumer(): Error creating AuditHDFSConsumer", ex);
        }

        LOG.info("<== AuditMessageQueueUtils:createKafkaHDFSConsumer() ret: {}", ret);

        return ret;
    }

    public boolean isSolrConsumerEnabled() {
        return isSolrConsumerEnabled;
    }

    public boolean isHDFSConsumerEnabled() {
        return isHDFSConsumerEnabled;
    }

    public boolean isTopicReady() {
        return isTopicReady;
    }

    public void setTopicReady(boolean topicReady) {
        isTopicReady = topicReady;
    }

    public String getJAASConfig(Properties props, String propPrefix) {
        // Use ranger service principal and keytab for Kafka authentication
        // This ensures consistent identity across all Ranger services and destination writes
        String hostName  = props.getProperty(AuditServerConstants.AUDIT_SERVER_PROP_PREFIX + "host");
        String principal = props.getProperty(AuditServerConstants.AUDIT_SERVER_PROP_PREFIX + AuditServerConstants.PROP_AUDIT_SERVICE_PRINCIPAL);
        String keytab    = props.getProperty(AuditServerConstants.AUDIT_SERVER_PROP_PREFIX + AuditServerConstants.PROP_AUDIT_SERVICE_KEYTAB);

        AuditServerLogFormatter.builder("Kerberos Configuration")
                .add("Principal (raw)", principal)
                .add("Hostname", hostName)
                .add("Keytab path", keytab)
                .logInfo(LOG);

        // Validate keytab file exists and is readable
        if (keytab != null) {
            java.io.File keytabFile = new java.io.File(keytab);
            if (!keytabFile.exists()) {
                LOG.error("ERROR: Keytab file does not exist: {}", keytab);
                throw new IllegalStateException("Keytab file not found: " + keytab);
            }
            if (!keytabFile.canRead()) {
                LOG.error("ERROR: Keytab file is not readable: {}", keytab);
                throw new IllegalStateException("Keytab file not readable: " + keytab);
            }

            AuditServerLogFormatter.builder("Keytab File Validation")
                    .add("Exists", keytabFile.exists())
                    .add("Readable", keytabFile.canRead())
                    .add("Size (bytes)", keytabFile.length())
                    .logInfo(LOG);
        }

        try {
            principal = SecureClientLogin.getPrincipal(props.getProperty(AuditServerConstants.AUDIT_SERVER_PROP_PREFIX + AuditServerConstants.PROP_AUDIT_SERVICE_PRINCIPAL), hostName);
            LOG.info("Principal (resolved): {}", principal);
        } catch (Exception e) {
            principal = null;
            LOG.error("ERROR: Failed to resolve principal from _HOST pattern!", e);
        }

        if (keytab == null || principal == null) {
            AuditServerLogFormatter.builder("Please configure the following properties in audit-server-site.xml:")
                    .add(AuditServerConstants.AUDIT_SERVER_PROP_PREFIX + AuditServerConstants.PROP_AUDIT_SERVICE_PRINCIPAL, "ranger/_HOST@YOUR-REALM")
                    .add(AuditServerConstants.AUDIT_SERVER_PROP_PREFIX + AuditServerConstants.PROP_AUDIT_SERVICE_KEYTAB, "/path/to/ranger.keytab")
                    .logError(LOG);
            throw new IllegalStateException("Ranger service principal and keytab must be configured for Kafka authentication. ");
        }

        String jaasConfig = new StringBuilder()
                .append(AuditServerConstants.JAAS_KRB5_MODULE).append(" ")
                .append(AuditServerConstants.JAAS_USE_KEYTAB).append(" ")
                .append(AuditServerConstants.JAAS_KEYTAB).append(keytab).append("\"").append(" ")
                .append(AuditServerConstants.JAAS_STOKE_KEY).append(" ")
                .append(AuditServerConstants.JAAS_USER_TICKET_CACHE).append(" ")
                .append(AuditServerConstants.JAAS_SERVICE_NAME).append(" ")
                .append(AuditServerConstants.JAAS_PRINCIPAL).append(principal).append("\";")
                .toString();

        AuditServerLogFormatter.builder("JAAS Configuration Generated")
                .add("Principal", principal)
                .add("Keytab", keytab)
                .add("Full JAAS Config", jaasConfig)
                .logInfo(LOG);

        return jaasConfig;
    }

    public String updateExistingTopicPartitions(AdminClient admin, String topicName, int partitions, short replicationFactor) {
        LOG.info("==> AuditMessageQueueUtils:updateExistingTopicPartitions() topic: {}, desired partitions: {}", topicName, partitions);

        String ret = null;
        int maxRetries = 3;
        int retryDelayMs = 1000; // Start with 1 second

        try {
            // Describe the existing topic to get current partition count
            DescribeTopicsResult describeTopicsResult = admin.describeTopics(Collections.singletonList(topicName));
            TopicDescription     topicDescription     = describeTopicsResult.values().get(topicName).get();
            int                  currentPartitions    = topicDescription.partitions().size();

            ret = topicDescription.name();
            LOG.info("Topic '{}' already exists with {} partitions", ret, currentPartitions);

            // Check if we need to increase partitions
            if (partitions > currentPartitions) {
                LOG.info("Upgrading topic '{}' from {} to {} partitions for audit server mechanism", topicName, currentPartitions, partitions);

                boolean   updateSuccess = false;
                Exception lastException = null;

                // Retry logic while updating partitions
                for (int attempt = 1; attempt <= maxRetries; attempt++) {
                    try {
                        LOG.info("Partition update attempt {}/{} for topic '{}'", attempt, maxRetries, topicName);

                        // Create partition increase request
                        Map<String, NewPartitions> newPartitionsMap = new HashMap<>();
                        newPartitionsMap.put(topicName, NewPartitions.increaseTo(partitions));

                        // Execute partition increase
                        CreatePartitionsResult createPartitionsResult = admin.createPartitions(newPartitionsMap);
                        createPartitionsResult.all().get(); // Wait for operation to complete

                        LOG.info("Successfully initiated partition increase for topic '{}' on attempt {}", topicName, attempt);

                        // Wait for metadata to propagate across the cluster
                        boolean isTopicReady = auditServerUtils.waitUntilTopicReady(admin, topicName, Duration.ofSeconds(60));

                        if (isTopicReady) {
                            // Verify the partition count after update
                            DescribeTopicsResult verifyResult       = admin.describeTopics(Collections.singletonList(topicName));
                            TopicDescription     updatedDescription = verifyResult.values().get(topicName).get();
                            int                  updatedPartitions  = updatedDescription.partitions().size();

                            if (updatedPartitions >= partitions) {
                                setTopicReady(true);
                                LOG.info("Topic '{}' successfully upgraded from {} to {} partitions on attempt {}", topicName, currentPartitions, updatedPartitions, attempt);
                                updateSuccess = true;
                                break;
                            } else {
                                LOG.warn("Topic '{}' partition update completed but verification shows only {} partitions (expected {})", topicName, updatedPartitions, partitions);
                                throw new IllegalStateException("Partition count verification failed");
                            }
                        } else {
                            LOG.warn("Topic '{}' partition update completed but topic not ready within timeout on attempt {}", topicName, attempt);
                            throw new IllegalStateException("Topic not ready after partition update");
                        }
                    } catch (Exception e) {
                        lastException = e;
                        LOG.warn("Partition update attempt {}/{} failed for topic '{}': {}", attempt, maxRetries, topicName, e.getMessage());
                        if (attempt < maxRetries) {
                            int currentDelay = retryDelayMs * (1 << (attempt - 1));
                            LOG.info("Retrying partition update in {} ms...", currentDelay);
                            Thread.sleep(currentDelay);
                        }
                    }
                }
                if (!updateSuccess) {
                    String errorMsg = String.format(
                            "FATAL: Failed to update partitions for topic '%s' after %d attempts. " +
                                    "Required: %d partitions, Current: %d partitions. " +
                                    "Cannot proceed without sufficient partitions.", topicName, maxRetries, partitions, currentPartitions);
                    LOG.error(errorMsg, lastException);
                    throw new RuntimeException(errorMsg, lastException);
                }
            } else if (partitions < currentPartitions) {
                LOG.warn("Topic '{}' has {} partitions, which is more than the configured {} partitions. " +
                        "Kafka does not support reducing partition count. Using existing partition count.", topicName, currentPartitions, partitions);
                setTopicReady(true);
            } else {
                LOG.info("Topic '{}' already has the correct number of partitions: {}", topicName, currentPartitions);
                setTopicReady(true);
            }
        } catch (Exception e) {
            String errorMsg = String.format("FATAL: Error updating partitions for topic '%s'", topicName);
            LOG.error(errorMsg, e);
            throw new RuntimeException(errorMsg, e);
        }

        LOG.info("<== AuditMessageQueueUtils:updateExistingTopicPartitions() ret: {}", ret);

        return ret;
    }

    /**
     * Calculate the number of partitions needed for the Kafka topic
     *
     * Always auto-calculated from partition overrides (sum of all partition counts):
     *   - Each appId gets 2 partitions by default
     *   - Can be overridden via xasecure.audit.destination.kafka.partitioner.override.<appId>
     *   - Total = sum of all partition counts (defaults + overrides)
     *   - Enforces minimum threshold to reserve partitions for unconfigured appIds
     *
     * Examples with 8 appIds:
     *   - Default (no overrides): 8 appIds × 2 (default) = 16 partitions (threshold: 20)
     *   - With overrides: hdfs=5, hive=3, others=2 each → 5+3+(6×2) = 20 partitions
     *   - Custom allocation: hdfs=10, hive=8, hbase=5, knox=3, others=2 each → 10+8+5+3+(4×2) = 34 partitions
     *   - Small config: hdfs=2, hive=2 → 4 partitions calculated, but threshold enforces 20 partitions
     *     (16 reserved partitions available for unconfigured appIds)
     *
     * @return Total number of partitions required
     */
    private int getPartitions(Properties prop, String propPrefix) {
        int ret = 0;
        // Auto-calculate partitions based on configured appIds and their partition overrides
        String configuredAppIds = MiscUtil.getStringProperty(prop,
                propPrefix + "." + AuditServerConstants.PROP_CONFIGURED_APP_IDS,
                AuditServerConstants.DEFAULT_CONFIGURED_APP_IDS);

        Map<String, Integer> partitionOverrides = new HashMap<>();

        if (StringUtils.isNotEmpty(configuredAppIds)) {
            String[] appIds = configuredAppIds.split(",");
            if (!ArrayUtils.isEmpty(appIds)) {
                // Build partition overrides map
                for (String appId : appIds) {
                    String overrideKey = propPrefix + "." + AuditServerConstants.PROP_PARTITION_OVERRIDE_PREFIX + "." + appId.trim();
                    int partitionCount = MiscUtil.getIntProperty(prop, overrideKey, AuditServerConstants.DEFAULT_PARTITIONS_PER_APP_ID);
                    partitionOverrides.put(appId.trim(), partitionCount);
                }

                // Calculate as sum of partition overrides
                ret = partitionOverrides.values().stream().mapToInt(Integer::intValue).sum();

                // Get minimum partition threshold (configurable, default: 20)
                int minPartitionsThreshold = MiscUtil.getIntProperty(prop,
                        propPrefix + "." + AuditServerConstants.PROP_MIN_PARTITIONS_THRESHOLD,
                        AuditServerConstants.DEFAULT_MIN_PARTITIONS_THRESHOLD);

                // Enforce minimum threshold to reserve partitions for unconfigured appIds
                if (ret < minPartitionsThreshold) {
                    int reservedPartitions = minPartitionsThreshold - ret;
                    AuditServerLogFormatter.builder("Enforcing minimum partition threshold:")
                            .add(" Calculated partitions: ", ret)
                            .add(" Minimum threshold: ", minPartitionsThreshold)
                            .add(" Reserved partitions for unconfigured appIds: ", reservedPartitions)
                            .add(" Partition counts per configured appId: ", partitionOverrides)
                            .logInfo(LOG);
                    ret = minPartitionsThreshold;
                } else {
                    AuditServerLogFormatter.builder("Auto-calculated partitions from appIds and partition overrides:")
                            .add(" Partition counts per appId: ", partitionOverrides)
                            .add(" Total partitions: ", ret)
                            .logInfo(LOG);
                }
            }
        }

        return ret;
    }
}
