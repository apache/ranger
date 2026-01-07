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

import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.ranger.audit.model.AuthzAuditEvent;
import org.apache.ranger.audit.provider.AuditHandler;
import org.apache.ranger.audit.provider.AuditProviderFactory;
import org.apache.ranger.audit.provider.MiscUtil;
import org.apache.ranger.audit.server.AuditServerConstants;
import org.apache.ranger.authorization.utils.JsonUtils;
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
import java.util.concurrent.ThreadLocalRandom;

import static org.apache.ranger.audit.provider.MiscUtil.TOKEN_APP_TYPE;
import static org.apache.ranger.audit.provider.MiscUtil.TOKEN_END;
import static org.apache.ranger.audit.provider.MiscUtil.TOKEN_START;
import static org.apache.ranger.audit.provider.MiscUtil.toArray;

public class AuditServerUtils {
    private static final Logger LOG = LoggerFactory.getLogger(AuditServerUtils.class);

    private Properties                                                   auditConfig;
    private ConcurrentHashMap<String, Map<String, AuditProviderFactory>> auditProviderMap = new ConcurrentHashMap<>();

    public Properties getAuditConfig() {
        if (auditConfig == null) {
            return null;
        }
        Properties copy = new Properties();
        copy.putAll(auditConfig);
        return copy;
    }

    public void setAuditConfig(Properties auditConfig) {
        if (auditConfig != null) {
            this.auditConfig = new Properties();
            this.auditConfig.putAll(auditConfig);
        } else {
            this.auditConfig = null;
        }
    }

    public AuditHandler getAuditProvider(AuthzAuditEvent auditEvent) throws Exception {
        AuditHandler ret = null;

        AuditProviderFactory auditProviderFactory = getAuditProvideFactory(auditEvent);
        if (auditProviderFactory != null) {
            ret = auditProviderFactory.getAuditProvider();
        }

        return ret;
    }

    private AuditProviderFactory getAuditProvideFactory(AuthzAuditEvent auditEvent) throws Exception {
        LOG.debug("==> AuditServerUtils.getAuditProviderFactory()");

        AuditProviderFactory ret         = null;
        String               hostName    = auditEvent.getAgentHostname();
        String               appId       = auditEvent.getAgentId();
        String               serviceType = getServiceType(auditEvent);

        Map<String, AuditProviderFactory> auditProviderFactoryMap = auditProviderMap.get(hostName);
        if (MapUtils.isNotEmpty(auditProviderFactoryMap)) {
            ret = auditProviderFactoryMap.get(appId);
        }

        if (ret == null) {
            ret = createAndCacheAuditProvider(hostName, serviceType, appId);
            if (MapUtils.isEmpty(auditProviderFactoryMap)) {
                auditProviderFactoryMap = new HashMap<>();
            }
            auditProviderFactoryMap.put(appId, ret);
            auditProviderMap.put(hostName, auditProviderFactoryMap);
        }

        if (LOG.isDebugEnabled()) {
            logAuditProviderCreated();
            LOG.debug("<== AuditServerUtils.getAuditProviderFactory(): {}", ret);
        }

        return ret;
    }

    private AuditProviderFactory createAndCacheAuditProvider(String hostName, String serviceType, String appId) throws Exception {
        LOG.debug("==> AuditServerUtils.createAndCacheAuditProvider(hostname: {}, serviceType: {}, appId: {})", hostName, serviceType, appId);

        AuditProviderFactory ret = initAuditProvider(serviceType, appId);
        if (!ret.isInitDone()) {
            ret = initAuditProvider(serviceType, appId);
        }
        if (!ret.isInitDone()) {
            String msg = String.format("AuditHandler for appId={%s}, hostname={%s} not initialized correctly. Please check audit configuration...", appId, hostName);
            LOG.error(msg);
            throw new Exception(msg);
        }

        LOG.debug("<== AuditServerUtils.createAndCacheAuditProvider(hostname: {}, serviceType: {}, appId: {})", hostName, serviceType, appId);
        return ret;
    }

    private AuditProviderFactory initAuditProvider(String serviceType, String appId) {
        Properties           properties = getConfigurationForAuditService(serviceType);
        AuditProviderFactory ret        = AuditProviderFactory.getInstance();
        ret.init(properties, appId);
        return ret;
    }

    private Properties getConfigurationForAuditService(String serviceType) {
        Properties ret = getAuditConfig();
        setCloudStorageLocationProperty(ret, serviceType);
        setSpoolFolderForDestination(ret, serviceType);
        return ret;
    }

    private String getServiceType(AuthzAuditEvent auditEvent) {
        String              ret            = null;
        String              additionalInfo = auditEvent.getAdditionalInfo();
        Map<String, String> addInfoMap     = JsonUtils.jsonToMapStringString(additionalInfo);
        if (MapUtils.isNotEmpty(addInfoMap)) {
            ret = addInfoMap.get("serviceType");
        }
        return ret;
    }

    private void setCloudStorageLocationProperty(Properties prop, String serviceType) {
        String hdfsDir = prop.getProperty("xasecure.audit.destination.hdfs.dir");
        if (StringUtils.isNotBlank(hdfsDir)) {
            StringBuilder sb = new StringBuilder(hdfsDir).append("/").append(serviceType);
            setProperty(prop, "xasecure.audit.destination.hdfs.dir", sb.toString());
        }
    }

    private void setSpoolFolderForDestination(Properties ret, String serviceType) {
        ArrayList<String> enabledDestinations = getAuditDestinationList(ret);
        for (String dest : enabledDestinations) {
            String spoolDirProp = new StringBuilder("xasecure.audit.destination.").append(dest).append(".batch.filespool.dir").toString();
            String spoolDir = ret.getProperty(spoolDirProp);
            if (StringUtils.isNotBlank(spoolDir)) {
                spoolDir = replaceToken(spoolDir, serviceType);
                setProperty(ret, spoolDirProp, spoolDir);
            }
        }
    }

    private void setProperty(Properties properties, String key, String val) {
        properties.setProperty(key, val);
    }

    private ArrayList<String> getAuditDestinationList(Properties props) {
        ArrayList<String> ret = new ArrayList<>();

        for (Object propNameObj : props.keySet()) {
            String propName = propNameObj.toString();
            if (!propName.startsWith(AuditProviderFactory.AUDIT_DEST_BASE)) {
                continue;
            }
            String       destName = propName.substring(AuditProviderFactory.AUDIT_DEST_BASE.length() + 1);
            List<String> splits   = toArray(destName, ".");
            if (splits.size() > 1) {
                continue;
            }
            String value = props.getProperty(propName);
            if ("enable".equalsIgnoreCase(value) || "enabled".equalsIgnoreCase(value) || "true".equalsIgnoreCase(value)) {
                ret.add(destName);
                LOG.info("Audit destination {} is set to {}", propName, value);
            }
        }
        return ret;
    }

    private String replaceToken(String str, String appType) {
        String ret = str;
        for (int startPos = 0; startPos < str.length(); ) {
            int tagStartPos = str.indexOf(TOKEN_START, startPos);

            if (tagStartPos == -1) {
                break;
            }

            int tagEndPos = str.indexOf(TOKEN_END, tagStartPos + TOKEN_START.length());

            if (tagEndPos == -1) {
                break;
            }

            String tag   = str.substring(tagStartPos, tagEndPos + TOKEN_END.length());
            String token = tag.substring(TOKEN_START.length(), tag.lastIndexOf(TOKEN_END));
            String val   = "";
            if (token != null) {
                if (token.equals(TOKEN_APP_TYPE)) {
                    val = appType;
                }
            }
            ret      = str.substring(0, tagStartPos) + val + str.substring(tagEndPos + TOKEN_END.length());
            startPos = tagStartPos + val.length();
        }

        return ret;
    }

    private void logAuditProviderCreated() {
        if (MapUtils.isNotEmpty(auditProviderMap)) {
            for (Map.Entry<String, Map<String, AuditProviderFactory>> entry : auditProviderMap.entrySet()) {
                String                            hostname           = entry.getKey();
                Map<String, AuditProviderFactory> providerFactoryMap = entry.getValue();
                if (MapUtils.isNotEmpty(providerFactoryMap)) {
                    for (Map.Entry<String, AuditProviderFactory> ap : providerFactoryMap.entrySet()) {
                        String               serviceAppId            = ap.getKey();
                        AuditProviderFactory auditProviderFactoryVal = ap.getValue();
                        String               apVal                   = auditProviderFactoryVal.getAuditProvider().getName();
                        LOG.debug("AuditProvider created for HostName: {} ServiceAppId: {} AuditProvider: {}", hostname, serviceAppId, apVal);
                    }
                }
            }
        }
    }

    // AppId-based partition consumption methods
    /**
     * Get topic partition count
     */
    public int getTopicPartitionCount(Properties props, String propPrefix, String topicName) {
        LOG.debug("==> AuditHDFSConsumer.getTopicPartitionCount()");

        int ret = 0;
        AuditMessageQueueUtils auditMessageQueueUtils = new AuditMessageQueueUtils(props);

        Map<String, Object> adminProps = new HashMap<>();
        adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,
                MiscUtil.getStringProperty(props, propPrefix + "." + AuditServerConstants.PROP_BOOTSTRAP_SERVERS));
        String securityProtocol = MiscUtil.getStringProperty(props, propPrefix + "." + AuditServerConstants.PROP_SECURITY_PROTOCOL,
                AuditServerConstants.DEFAULT_SECURITY_PROTOCOL);
        adminProps.put(AdminClientConfig.SECURITY_PROTOCOL_CONFIG, securityProtocol);
        adminProps.put(AuditServerConstants.PROP_SASL_MECHANISM,
                MiscUtil.getStringProperty(props, propPrefix + "." + AuditServerConstants.PROP_SASL_MECHANISM,
                        AuditServerConstants.DEFAULT_SASL_MECHANISM));
        adminProps.put(AuditServerConstants.PROP_SASL_KERBEROS_SERVICE_NAME, AuditServerConstants.DEFAULT_SERVICE_NAME);

        if (securityProtocol != null && securityProtocol.toUpperCase().contains("SASL")) {
            adminProps.put(AuditServerConstants.PROP_SASL_JAAS_CONFIG, auditMessageQueueUtils.getJAASConfig(props, propPrefix));
        }

        boolean isTopicReady = auditMessageQueueUtils.isTopicReady();
        if (!isTopicReady) {
            ret = checkAndFetchCreatedPartitionCount(adminProps, topicName);
            auditMessageQueueUtils.setTopicReady(true);
        } else {
            try {
                Admin kafkaAdmin  = Admin.create(adminProps);
                DescribeTopicsResult result = kafkaAdmin.describeTopics(Collections.singletonList(topicName));
                TopicDescription topicDescription = result.values().get(topicName).get();
                ret = topicDescription.partitions().size();
            } catch (Exception e) {
                throw new RuntimeException("Failed to fetch metadata for topic:" + topicName, e);
            }
        }

        LOG.debug("<== AuditHDFSConsumer.getTopicPartitionCount(): {}", ret);
        return ret;
    }

    public int checkAndFetchCreatedPartitionCount(Map<String, Object> adminProps, String topicName) {
        LOG.debug("==> AuditHDFSConsumer.checkAndFetchCreatedPartitionCount()");
        int         ret         = 0;
        boolean     topicReady  = false;
        Duration waitTime    = Duration.ofSeconds(60);
        Admin kafkaAdmin  = Admin.create(adminProps);

        try {
            topicReady = waitUntilTopicReady(kafkaAdmin, topicName, waitTime);
        } catch (Exception e) {
            throw new RuntimeException("Failed to fetch metadata for topic:" + topicName, e);
        }

        if (topicReady) {
            try {
                DescribeTopicsResult result = kafkaAdmin.describeTopics(Collections.singletonList(topicName));
                TopicDescription topicDescription = result.values().get(topicName).get();
                ret = topicDescription.partitions().size();
            } catch (Exception e) {
                throw new RuntimeException("Failed to fetch metadata for topic:" + topicName, e);
            }
        }

        LOG.debug("<== AuditHDFSConsumer.checkAndFetchCreatedPartitionCount(): Number of partitions: {}", ret);

        return ret;
    }

    public boolean waitUntilTopicReady(Admin admin, String topic, Duration totalWait) throws Exception {
        long endTime     = System.nanoTime() + totalWait.toNanos();
        long baseSleepMs = 100L;
        long maxSleepMs  = 2000L;

        while (System.nanoTime() < endTime) {
            try {
                DescribeTopicsResult describeTopicsResult = admin.describeTopics(Collections.singleton(topic));
                TopicDescription     topicDescription     = describeTopicsResult.values().get(topic).get();
                boolean              allHaveLeader        = topicDescription.partitions().stream().allMatch(partitionInfo -> partitionInfo.leader() != null);
                boolean              allHaveISR           = topicDescription.partitions().stream().allMatch(partitionInfo -> !partitionInfo.isr().isEmpty());
                if (allHaveLeader && allHaveISR) {
                    return true;
                }
            } catch (Exception e) {
                // If topic hasn’t propagated yet, you’ll see UnknownTopicOrPartitionException
                // continue to wait for topic availability
                if (!(rootCause(e) instanceof UnknownTopicOrPartitionException)) {
                    throw e;
                }
            }

            // Sleep until the created topic is available for metadata fetch
            baseSleepMs = Math.min(maxSleepMs, baseSleepMs * 2);
            long sleep = baseSleepMs + ThreadLocalRandom.current().nextLong(0, baseSleepMs / 2 + 1);
            Thread.sleep(sleep);
        }
        return false;
    }

    private static Throwable rootCause(Throwable t) {
        Throwable throwable = t;
        while (throwable.getCause() != null) {
            throwable = throwable.getCause();
        }
        return throwable;
    }

    /**
     * Log consolidated startup summary for consumer threads and their partition assignments
     *
     * @param logger                Logger instance to use for logging
     * @param appIdPartitions       Map of appId to partition assignments
     * @param destination           Destination type (e.g., "SOLR", "HDFS")
     * @param consumerGroupId       Kafka consumer group ID
     * @param topicName             Kafka topic name
     * @param offsetCommitStrategy  Offset commit strategy being used
     * @param workerIdPrefix        Prefix for worker IDs (e.g., "consumer-worker-", "hdfs-consumer-worker-")
     */
    public static void logConsumerStartupSummary(Logger logger, Map<String, List<Integer>> appIdPartitions,
                                                  String destination, String consumerGroupId, String topicName,
                                                  String offsetCommitStrategy, String workerIdPrefix) {
        logger.info("================================================================================");
        logger.info("                    {} CONSUMER STARTUP SUMMARY", destination);
        logger.info("================================================================================");
        logger.info("Consumer Group ID    : {}", consumerGroupId);
        logger.info("Topic Name           : {}", topicName);
        logger.info("Total AppIds         : {}", appIdPartitions.size());
        logger.info("Total Threads        : {} (one per appId)", appIdPartitions.size());
        logger.info("Offset Commit Strategy: {}", offsetCommitStrategy);
        logger.info("--------------------------------------------------------------------------------");
        logger.info("Thread-to-Partition Assignments:");
        logger.info("--------------------------------------------------------------------------------");

        int threadNum = 1;
        for (Map.Entry<String, List<Integer>> entry : appIdPartitions.entrySet()) {
            String sourceAppId = entry.getKey();
            List<Integer> partitions = entry.getValue();
            String workerId = workerIdPrefix + sourceAppId;

            logger.info("  [{}] Worker-ID: '{}' | AppId: '{}' | Partitions: {}",
                    threadNum++, workerId, sourceAppId, partitions);
        }

        logger.info("================================================================================");
        logger.info("All {} consumer threads initialized and ready to process audit messages", destination);
        logger.info("================================================================================");
    }

}
