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

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.ranger.audit.provider.MiscUtil;
import org.apache.ranger.audit.server.AuditServerConstants;
import org.apache.ranger.audit.utils.AuditMessageQueueUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public abstract class AuditDispatcherBase implements AuditDispatcher {
    private static final Logger LOG = LoggerFactory.getLogger(AuditDispatcherBase.class);

    public final Properties                    dispatcherProps = new Properties();
    public final KafkaConsumer<String, String> dispatcher;
    public final String                        topicName;
    public final String                        dispatcherGroupId;

    public AuditDispatcherBase(Properties props, String propPrefix, String dispatcherGroupId) throws Exception {
        AuditMessageQueueUtils auditMessageQueueUtils = new AuditMessageQueueUtils();

        this.dispatcherGroupId = getDispatcherGroupId(props, propPrefix, dispatcherGroupId);

        dispatcherProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, MiscUtil.getStringProperty(props, propPrefix + "." + AuditServerConstants.PROP_BOOTSTRAP_SERVERS));
        dispatcherProps.put(ConsumerConfig.GROUP_ID_CONFIG, this.dispatcherGroupId);
        dispatcherProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, Class.forName("org.apache.kafka.common.serialization.StringDeserializer"));
        dispatcherProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, Class.forName("org.apache.kafka.common.serialization.StringDeserializer"));

        String securityProtocol = MiscUtil.getStringProperty(props, propPrefix + "." + AuditServerConstants.PROP_SECURITY_PROTOCOL, AuditServerConstants.DEFAULT_SECURITY_PROTOCOL);
        dispatcherProps.put(AdminClientConfig.SECURITY_PROTOCOL_CONFIG, securityProtocol);

        dispatcherProps.put(AuditServerConstants.PROP_SASL_MECHANISM, MiscUtil.getStringProperty(props, propPrefix + "." + AuditServerConstants.PROP_SASL_MECHANISM, AuditServerConstants.DEFAULT_SASL_MECHANISM));
        dispatcherProps.put(AuditServerConstants.PROP_SASL_KERBEROS_SERVICE_NAME, AuditServerConstants.DEFAULT_SERVICE_NAME);

        if (securityProtocol.toUpperCase().contains(AuditServerConstants.PROP_SECURITY_PROTOCOL_VALUE)) {
            dispatcherProps.put(AuditServerConstants.PROP_SASL_JAAS_CONFIG, auditMessageQueueUtils.getJAASConfig(props, propPrefix));
        }

        dispatcherProps.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, MiscUtil.getIntProperty(props, propPrefix + "." + AuditServerConstants.PROP_DISPATCHER_MAX_POLL_RECORDS, AuditServerConstants.DEFAULT_MAX_POLL_RECORDS));
        dispatcherProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // Configure re-balancing parameters for subscribe mode
        // These ensure stable dispatcher group behavior during horizontal scaling
        int sessionTimeoutMs    = MiscUtil.getIntProperty(props, propPrefix + "." + AuditServerConstants.PROP_DISPATCHER_SESSION_TIMEOUT_MS, AuditServerConstants.DEFAULT_SESSION_TIMEOUT_MS);
        int maxPollIntervalMs   = MiscUtil.getIntProperty(props, propPrefix + "." + AuditServerConstants.PROP_DISPATCHER_MAX_POLL_INTERVAL_MS, AuditServerConstants.DEFAULT_MAX_POLL_INTERVAL_MS);
        int heartbeatIntervalMs = MiscUtil.getIntProperty(props, propPrefix + "." + AuditServerConstants.PROP_DISPATCHER_HEARTBEAT_INTERVAL_MS, AuditServerConstants.DEFAULT_HEARTBEAT_INTERVAL_MS);

        dispatcherProps.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, sessionTimeoutMs);
        dispatcherProps.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, maxPollIntervalMs);
        dispatcherProps.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, heartbeatIntervalMs);

        // Configure partition assignment strategy
        String partitionAssignmentStrategy = MiscUtil.getStringProperty(props, propPrefix + "." + AuditServerConstants.PROP_DISPATCHER_PARTITION_ASSIGNMENT_STRATEGY, AuditServerConstants.DEFAULT_PARTITION_ASSIGNMENT_STRATEGY);
        dispatcherProps.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, partitionAssignmentStrategy);

        LOG.info("Dispatcher '{}' configured for subscription-based partition assignment with re-balancing support", this.dispatcherGroupId);
        LOG.info("Re-balancing config - session.timeout.ms: {}, max.poll.interval.ms: {}, heartbeat.interval.ms: {}", sessionTimeoutMs, maxPollIntervalMs, heartbeatIntervalMs);
        LOG.info("Partition assignment strategy: {}", partitionAssignmentStrategy);

        dispatcher = new KafkaConsumer<>(dispatcherProps);

        topicName =  MiscUtil.getStringProperty(props, propPrefix + "." + AuditServerConstants.PROP_TOPIC_NAME, AuditServerConstants.DEFAULT_TOPIC);
    }

    @Override
    public KafkaConsumer<String, String> getDispatcher() {
        return dispatcher;
    }

    @Override
    public String getTopicName() {
        return topicName;
    }

    public String getDispatcherGroupId() {
        return dispatcherGroupId;
    }

    private String getDispatcherGroupId(Properties props, String propPrefix, String defaultDispatcherGroupId) {
        String configuredGroupId = MiscUtil.getStringProperty(props, propPrefix + ".dispatcher.group.id");
        if (configuredGroupId != null && !configuredGroupId.trim().isEmpty()) {
            return configuredGroupId.trim();
        }
        return defaultDispatcherGroupId;
    }
}
