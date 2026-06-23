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

package org.apache.ranger.audit.producer.kafka.partition;

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.ranger.audit.provider.MiscUtil;
import org.apache.ranger.audit.server.AuditServerConstants;
import org.apache.ranger.audit.utils.AuditMessageQueueUtils;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/** Kafka client settings for the partition-plan registry topic. */
public class PartitionPlanKafkaConfig {
    private PartitionPlanKafkaConfig() {
    }

    /** Resolves the compacted Kafka topic that stores the partition plan registry. */
    public static String resolvePlanTopicName(Properties props, String propPrefix) {
        return MiscUtil.getStringProperty(props, propPrefix + "." + AuditServerConstants.PROP_PARTITION_PLAN_TOPIC, AuditServerConstants.DEFAULT_PARTITION_PLAN_TOPIC);
    }

    /** Returns whether ingestor should load routing from the Kafka plan registry. */
    public static boolean isDynamicPartitionPlanEnabled(Properties props, String propPrefix) {
        return MiscUtil.getBooleanProperty(props, propPrefix + "." + AuditServerConstants.PROP_PARTITION_PLAN_DYNAMIC_ENABLED, false);
    }

    /** Resolves short usernames allowed to call partition-plan admin REST (empty = any authenticated principal). */
    public static Set<String> resolvePartitionPlanAdminUsers(Properties props, String propPrefix) {
        String configured = MiscUtil.getStringProperty(props, propPrefix + "." + AuditServerConstants.PROP_PARTITION_PLAN_ALLOWED_USERS, "");
        if (configured == null || configured.isBlank()) {
            return Collections.emptySet();
        }
        Set<String> users = new LinkedHashSet<>();
        for (String user : configured.split(",")) {
            if (user != null && !user.isBlank()) {
                users.add(user.trim());
            }
        }
        return users;
    }

    /** Returns whether the Kafka producer partitioner should use the in-memory dynamic plan. */
    public static boolean isDynamicPartitionPlanEnabled(Map<String, ?> configs, String ingestorPropPrefix) {
        String key = ingestorPropPrefix + "." + AuditServerConstants.PROP_PARTITION_PLAN_DYNAMIC_ENABLED;
        Object val = configs.get(key);
        if (val == null) {
            return false;
        }
        if (val instanceof Boolean) {
            return (Boolean) val;
        }
        return Boolean.parseBoolean(val.toString());
    }

    /** Resolves how often each ingestor pod reloads the plan from Kafka. */
    public static int resolveRefreshIntervalMs(Properties props, String propPrefix) {
        return MiscUtil.getIntProperty(props, propPrefix + "." + AuditServerConstants.PROP_PARTITION_PLAN_REFRESH_INTERVAL_MS, AuditServerConstants.DEFAULT_PARTITION_PLAN_REFRESH_INTERVAL_MS);
    }

    /** Resolves the Kafka consumer poll timeout when draining the compacted plan topic. */
    public static int resolveConsumerPollTimeoutMs(Properties props, String propPrefix) {
        return MiscUtil.getIntProperty(props, propPrefix + "." + AuditServerConstants.PROP_PARTITION_PLAN_CONSUMER_POLL_TIMEOUT_MS, AuditServerConstants.DEFAULT_PARTITION_PLAN_CONSUMER_POLL_TIMEOUT_MS);
    }

    /** Builds Kafka producer properties for writing to the plan registry topic. */
    public static Properties producerConfig(Properties props, String propPrefix) throws Exception {
        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, MiscUtil.getStringProperty(props, propPrefix + "." + AuditServerConstants.PROP_BOOTSTRAP_SERVERS));
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        producerProps.put(ProducerConfig.ACKS_CONFIG, "all");
        producerProps.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, MiscUtil.getIntProperty(props, propPrefix + "." + AuditServerConstants.PROP_REQ_TIMEOUT_MS, AuditServerConstants.DEFAULT_PRODUCER_REQUEST_TIMEOUT_MS));
        applySecurity(producerProps, props, propPrefix);
        return producerProps;
    }

    /** Builds Kafka consumer properties for reading the plan registry topic. */
    public static Properties consumerConfig(Properties props, String propPrefix, String groupId) throws Exception {
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, MiscUtil.getStringProperty(props, propPrefix + "." + AuditServerConstants.PROP_BOOTSTRAP_SERVERS));
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, MiscUtil.getIntProperty(props, propPrefix + "." + AuditServerConstants.PROP_REQ_TIMEOUT_MS, AuditServerConstants.DEFAULT_PRODUCER_REQUEST_TIMEOUT_MS));
        applySecurity(consumerProps, props, propPrefix);
        return consumerProps;
    }

    /** Applies Kerberos/SASL settings shared by plan-registry Kafka clients. */
    private static void applySecurity(Properties clientProps, Properties props, String propPrefix) throws Exception {
        String securityProtocol = MiscUtil.getStringProperty(props, propPrefix + "." + AuditServerConstants.PROP_SECURITY_PROTOCOL, AuditServerConstants.DEFAULT_SECURITY_PROTOCOL);
        clientProps.put(AdminClientConfig.SECURITY_PROTOCOL_CONFIG, securityProtocol);
        clientProps.put(AuditServerConstants.PROP_SASL_MECHANISM, MiscUtil.getStringProperty(props, propPrefix + "." + AuditServerConstants.PROP_SASL_MECHANISM, AuditServerConstants.DEFAULT_SASL_MECHANISM));
        clientProps.put(AuditServerConstants.PROP_SASL_KERBEROS_SERVICE_NAME, AuditServerConstants.DEFAULT_SERVICE_NAME);
        if (securityProtocol.toUpperCase().contains(AuditServerConstants.PROP_SECURITY_PROTOCOL_VALUE)) {
            clientProps.put(AuditServerConstants.PROP_SASL_JAAS_CONFIG, AuditMessageQueueUtils.getJAASConfig(props, propPrefix));
        }
    }
}
