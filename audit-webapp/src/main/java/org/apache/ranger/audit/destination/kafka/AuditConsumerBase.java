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

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.ranger.audit.provider.MiscUtil;
import org.apache.ranger.audit.server.AuditServerConstants;
import org.apache.ranger.audit.utils.AuditMessageQueueUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class AuditConsumerBase {
    private static final Logger LOG = LoggerFactory.getLogger(AuditConsumerBase.class);

    public Properties                    consumerProps = new Properties();
    public KafkaConsumer<String, String> consumer;
    public String                        topicName;
    public String                        consumerGroupId;

    public AuditConsumerBase(Properties props, String propPrefix, String consumerGroupId) throws Exception {
        AuditMessageQueueUtils auditMessageQueueUtils = new AuditMessageQueueUtils(props);

        this.consumerGroupId = getConsumerGroupId(props, propPrefix, consumerGroupId);

        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, MiscUtil.getStringProperty(props, propPrefix + "." + AuditServerConstants.PROP_BOOTSTRAP_SERVERS));
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, this.consumerGroupId);
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, Class.forName("org.apache.kafka.common.serialization.StringDeserializer"));
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, Class.forName("org.apache.kafka.common.serialization.StringDeserializer"));

        String securityProtocol = MiscUtil.getStringProperty(props, propPrefix + "." + AuditServerConstants.PROP_SECURITY_PROTOCOL, AuditServerConstants.DEFAULT_SECURITY_PROTOCOL);
        consumerProps.put(AdminClientConfig.SECURITY_PROTOCOL_CONFIG, securityProtocol);

        consumerProps.put(AuditServerConstants.PROP_SASL_MECHANISM, MiscUtil.getStringProperty(props, propPrefix + "." + AuditServerConstants.PROP_SASL_MECHANISM, AuditServerConstants.DEFAULT_SASL_MECHANISM));
        consumerProps.put(AuditServerConstants.PROP_SASL_KERBEROS_SERVICE_NAME, AuditServerConstants.DEFAULT_SERVICE_NAME);

        if (securityProtocol.toUpperCase().contains(AuditServerConstants.PROP_SECURITY_PROTOCOL_VALUE)) {
            consumerProps.put(AuditServerConstants.PROP_SASL_JAAS_CONFIG, auditMessageQueueUtils.getJAASConfig(props, propPrefix));
        }

        consumerProps.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, MiscUtil.getIntProperty(props, propPrefix + "." + AuditServerConstants.PROP_CONSUMER_MAX_POLL_RECORDS, AuditServerConstants.DEFAULT_MAX_POLL_RECORDS));
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // Configure re-balancing parameters for subscribe mode
        // These ensure stable consumer group behavior during horizontal scaling
        int sessionTimeoutMs    = MiscUtil.getIntProperty(props, propPrefix + "." + AuditServerConstants.PROP_CONSUMER_SESSION_TIMEOUT_MS, AuditServerConstants.DEFAULT_SESSION_TIMEOUT_MS);
        int maxPollIntervalMs   = MiscUtil.getIntProperty(props, propPrefix + "." + AuditServerConstants.PROP_CONSUMER_MAX_POLL_INTERVAL_MS, AuditServerConstants.DEFAULT_MAX_POLL_INTERVAL_MS);
        int heartbeatIntervalMs = MiscUtil.getIntProperty(props, propPrefix + "." + AuditServerConstants.PROP_CONSUMER_HEARTBEAT_INTERVAL_MS, AuditServerConstants.DEFAULT_HEARTBEAT_INTERVAL_MS);

        consumerProps.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, sessionTimeoutMs);
        consumerProps.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, maxPollIntervalMs);
        consumerProps.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, heartbeatIntervalMs);

        // Configure partition assignment strategy
        String partitionAssignmentStrategy = MiscUtil.getStringProperty(props, propPrefix + "." + AuditServerConstants.PROP_CONSUMER_PARTITION_ASSIGNMENT_STRATEGY, AuditServerConstants.DEFAULT_PARTITION_ASSIGNMENT_STRATEGY);
        consumerProps.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, partitionAssignmentStrategy);

        LOG.info("Consumer '{}' configured for subscription-based partition assignment with re-balancing support", this.consumerGroupId);
        LOG.info("Re-balancing config - session.timeout.ms: {}, max.poll.interval.ms: {}, heartbeat.interval.ms: {}", sessionTimeoutMs, maxPollIntervalMs, heartbeatIntervalMs);
        LOG.info("Partition assignment strategy: {}", partitionAssignmentStrategy);

        consumer = new KafkaConsumer<>(consumerProps);

        topicName =  MiscUtil.getStringProperty(props, propPrefix + "." + AuditServerConstants.PROP_TOPIC_NAME, AuditServerConstants.DEFAULT_TOPIC);
    }

    private String getConsumerGroupId(Properties props, String propPrefix, String defaultConsumerGroupId) {
        String configuredGroupId = MiscUtil.getStringProperty(props, propPrefix + ".consumer.group.id");
        if (configuredGroupId != null && !configuredGroupId.trim().isEmpty()) {
            return configuredGroupId.trim();
        }
        return defaultConsumerGroupId;
    }

    public KafkaConsumer<String, String> getConsumer() {
        return consumer;
    }

    public String getConsumerGroupId() {
        return consumerGroupId;
    }
}
