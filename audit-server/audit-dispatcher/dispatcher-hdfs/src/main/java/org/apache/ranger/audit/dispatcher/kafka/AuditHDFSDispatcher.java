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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.SecureClientLogin;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.ranger.audit.provider.AuditProviderFactory;
import org.apache.ranger.audit.provider.MiscUtil;
import org.apache.ranger.audit.server.AuditServerConstants;
import org.apache.ranger.audit.utils.AuditServerLogFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

public class AuditHDFSDispatcher extends AuditDispatcherBase {
    private static final Logger LOG = LoggerFactory.getLogger(AuditHDFSDispatcher.class);

    private static final String CONFIG_CORE_SITE                           = "core-site.xml";
    private static final String CONFIG_HDFS_SITE                           = "hdfs-site.xml";
    private static final String DEFAULT_RANGER_AUDIT_HDFS_DISPATCHER_GROUP = "ranger_audit_hdfs_dispatcher_group";
    private static final String PROP_HDFS_DEST_PREFIX                      = "hdfs";

    private final AuditRouterHDFS               auditRouterHDFS;

    public AuditHDFSDispatcher(Properties props, String propPrefix) throws Exception {
        super(props, propPrefix, DEFAULT_RANGER_AUDIT_HDFS_DISPATCHER_GROUP);

        auditRouterHDFS = new AuditRouterHDFS();

        init(props, propPrefix);
    }

    @Override
    protected String getDispatcherName() {
        return PROP_HDFS_DEST_PREFIX.toUpperCase();
    }

    @Override
    protected DispatcherWorker createDispatcherWorker(String workerId, List<Integer> assignedPartitions) {
        return new HDFSDispatcherWorker(workerId, assignedPartitions);
    }

    @Override
    protected void shutdownDestination() {
        if (auditRouterHDFS != null) {
            try {
                auditRouterHDFS.shutdown();
            } catch (Exception e) {
                LOG.error("Error shutting down HDFS destination handler", e);
            }
        }
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
        auditRouterHDFS.init(props, AuditProviderFactory.AUDIT_DEST_BASE + "." + PROP_HDFS_DEST_PREFIX);

        LOG.info("<== AuditHDFSDispatcher.init(): AuditHDFSDispatcher initialized successfully");
    }

    private void processMessage(String message, String partitionKey) throws Exception {
        auditRouterHDFS.routeAuditMessage(message, partitionKey);
    }

    private void initializeRangerUGI(Properties props, String propPrefix) throws Exception {
        LOG.info("==> AuditHDFSDispatcher.initializeRangerUGI()");

        try {
            Configuration coreSite = getCoreSiteConfiguration();
            String        authType = coreSite.get(AuditServerConstants.PROP_HADOOP_AUTHENTICATION_TYPE, "simple");

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
            UserGroupInformation.setConfiguration(coreSite);

            LOG.info("Initializing Ranger AuditServer UGI for HDFS writes: principal={}, keytab={}", principal, keytab);

            UserGroupInformation rangerAuditServerUGI = UserGroupInformation.loginUserFromKeytabAndReturnUGI(principal, keytab);

            MiscUtil.setUGILoginUser(rangerAuditServerUGI, null);

            LOG.info("<== AuditHDFSDispatcher.initializeRangerUGI(): Ranger UGI initialized successfully: user={}, auth={}, hasKerberos={}",
                            rangerAuditServerUGI.getUserName(), rangerAuditServerUGI.getAuthenticationMethod(), rangerAuditServerUGI.hasKerberosCredentials());
        } catch (IOException e) {
            LOG.error("Failed to initialize Ranger AuditServer UGI for HDFS writes", e);
            throw e;
        }
    }

    /**
     * Add Hadoop configuration properties from core-site.xml and hdfs-site.xml to props.
     */
    private void addHadoopConfigToProps(Properties props) {
        LOG.info("==> AuditHDFSDispatcher.addHadoopConfigToProps()");

        try {
            String     configPrefix = "xasecure.audit.destination.hdfs.config.";
            Properties hadoopProps  = getHadoopPropertiesWithPrefix(configPrefix);
            props.putAll(hadoopProps);

            LOG.info("<== AuditHDFSDispatcher.addHadoopConfigToProps(): Added {} Hadoop configuration properties", hadoopProps.size());
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

    /**
    * Get Hadoop configuration properties (from core-site.xml and hdfs-site.xml) with a specific prefix.
    * This is need for the HdfsAuditDestination and its parent classes  for routing the audits to hdfs location.
    * @param prefix The prefix to add to each property name ("xasecure.audit.destination.hdfs.config.")
    * @return Properties from core-site.xml and hdfs-site.xml with the specified prefix
    */
    private Properties getHadoopPropertiesWithPrefix(String prefix) {
        LOG.debug("==> AuditHDFSDispatcher.getHadoopPropertiesWithPrefix(prefix={})", prefix);

        Properties prefixedProps = new java.util.Properties();
        int propsAdded = 0;

        try {
            // Load core-site.xml separately to get pure Hadoop security properties
            Configuration coreSite = new Configuration(false);
            coreSite.addResource(CONFIG_CORE_SITE);

            for (java.util.Map.Entry<String, String> entry : coreSite) {
                String propName  = entry.getKey();
                String propValue = entry.getValue();

                if (propValue != null && !propValue.trim().isEmpty()) {
                    prefixedProps.setProperty(prefix + propName, propValue);
                    LOG.trace("Added from core-site.xml: {} = {}", propName, propValue);
                    propsAdded++;
                }
            }

            // Load hdfs-site.xml separately to get pure HDFS client properties
            Configuration hdfsSite = new Configuration(false);
            hdfsSite.addResource(CONFIG_HDFS_SITE);

            for (java.util.Map.Entry<String, String> entry : hdfsSite) {
                String propName  = entry.getKey();
                String propValue = entry.getValue();

                if (propValue != null && !propValue.trim().isEmpty()) {
                    prefixedProps.setProperty(prefix + propName, propValue);
                    LOG.trace("Added from hdfs-site.xml: {} = {}", propName, propValue);
                    propsAdded++;
                }
            }

            LOG.debug("<== AuditHDFSDispatcher.getHadoopPropertiesWithPrefix(): Added {} Hadoop properties with prefix '{}'", propsAdded, prefix);
        } catch (Exception e) {
            LOG.error("Failed to load Hadoop properties from core-site.xml and hdfs-site.xml", e);
        }

        return prefixedProps;
    }

    /**
    * Get core-site.xml Configuration for UGI initialization.
    * @return Configuration loaded from core-site.xml
    */
    private Configuration getCoreSiteConfiguration() {
        LOG.debug("==> AuditHDFSDispatcher.getCoreSiteConfiguration()");

        Configuration coreSite = new Configuration(false);
        coreSite.addResource(CONFIG_CORE_SITE);

        LOG.debug("<== AuditHDFSDispatcher.getCoreSiteConfiguration(): authentication={}", coreSite.get("hadoop.security.authentication"));

        return coreSite;
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

    private class HDFSDispatcherWorker extends DispatcherWorker {
        public HDFSDispatcherWorker(String workerId, List<Integer> assignedPartitions) {
            super(workerId, assignedPartitions);
        }

        @Override
        protected void processRecordBatch(ConsumerRecords<String, String> records) {
            Set<TopicPartition> failedPartitions = new HashSet<>();

            for (ConsumerRecord<String, String> record : records) {
                TopicPartition partition = new TopicPartition(record.topic(), record.partition());

                if (failedPartitions.contains(partition)) {
                    continue; // Skip remaining records for this partition in the current batch
                }

                try {
                    LOG.debug("HDFS worker '{}' consumed: partition={}, key={}, offset={}",
                            workerId, record.partition(), record.key(), record.offset());

                    // Process the message using the destination handler
                    // The partition key (record.key()) contains the appId for HDFS path routing
                    processMessage(record.value(), record.key());

                    // Track offset for manual commit strategies
                    pendingOffsets.put(partition, new OffsetAndMetadata(record.offset() + 1));
                    messagesProcessedSinceLastCommit.incrementAndGet();
                } catch (Exception e) {
                    LOG.error("Error processing message in HDFS worker '{}': partition={}, key={}, offset={}",
                            workerId, record.partition(), record.key(), record.offset(), e);

                    // On error, track the offset of the failed message so we retry it
                    pendingOffsets.put(partition, new OffsetAndMetadata(record.offset()));

                    // Seek the consumer back to the failed offset so it is re-fetched in the next poll
                    try {
                        workerDispatcher.seek(partition, record.offset());
                    } catch (Exception seekEx) {
                        LOG.error("Failed to seek to offset {} for partition {} after processing error", record.offset(), partition, seekEx);
                    }

                    // Add sleep to prevent frequent polling when HDFS is completely down
                    try {
                        Thread.sleep(5000);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                    }

                    // Mark this partition as failed so we don't process subsequent records for it in this batch
                    failedPartitions.add(partition);
                }
            }
        }
    }
}
