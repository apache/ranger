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
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.ranger.audit.provider.MiscUtil;
import org.apache.ranger.audit.server.AuditServerConstants;
import org.apache.ranger.audit.utils.AuditMessageQueueUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.PrivilegedExceptionAction;
import java.util.Properties;

public class AuditProducer implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(AuditProducer.class);

    public Properties                    producerProps = new Properties();
    public KafkaProducer<String, String> kafkaProducer;
    private volatile boolean             running = true;

    public AuditProducer(Properties props, String propPrefix) throws Exception {
        LOG.debug("==> AuditProducer()");

        AuditMessageQueueUtils auditMessageQueueUtils = new AuditMessageQueueUtils(props);
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, MiscUtil.getStringProperty(props, propPrefix + "." + AuditServerConstants.PROP_BOOTSTRAP_SERVERS));
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, Class.forName("org.apache.kafka.common.serialization.StringSerializer"));
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, Class.forName("org.apache.kafka.common.serialization.StringSerializer"));
        producerProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        producerProps.put(ProducerConfig.ACKS_CONFIG, "all");

        String securityProtocol = MiscUtil.getStringProperty(props, propPrefix + "." + AuditServerConstants.PROP_SECURITY_PROTOCOL, AuditServerConstants.DEFAULT_SECURITY_PROTOCOL);
        producerProps.put(AdminClientConfig.SECURITY_PROTOCOL_CONFIG, securityProtocol);

        producerProps.put(AuditServerConstants.PROP_SASL_MECHANISM, MiscUtil.getStringProperty(props, propPrefix + "." + AuditServerConstants.PROP_SASL_MECHANISM, AuditServerConstants.DEFAULT_SASL_MECHANISM));
        producerProps.put(AuditServerConstants.PROP_SASL_KERBEROS_SERVICE_NAME, AuditServerConstants.DEFAULT_SERVICE_NAME);

        if (securityProtocol.toUpperCase().contains(AuditServerConstants.PROP_SECURITY_PROTOCOL_VALUE)) {
            producerProps.put(AuditServerConstants.PROP_SASL_JAAS_CONFIG, auditMessageQueueUtils.getJAASConfig(props, propPrefix));
        }

        producerProps.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, "org.apache.ranger.audit.destination.kafka.AuditSourceBasedPartitioner");
        producerProps.put(ProducerConfig.LINGER_MS_CONFIG, 5);
        producerProps.put(ProducerConfig.BATCH_SIZE_CONFIG, 32 * 1024);

        // Pass partitioner configuration (appIds and partition overrides) to partitioner
        copyPartitionerConfiguration(props, propPrefix);

        try {
            kafkaProducer = MiscUtil.executePrivilegedAction((PrivilegedExceptionAction<KafkaProducer<String, String>>) () -> new KafkaProducer<>(producerProps));
            LOG.info("AuditProducer(): KafkaProducer Created successfully !");
        } catch (Exception ex) {
            LOG.error("AuditProducer(): Error create KafkaProducer...!", ex);
        }

        LOG.debug("<== AuditProducer()");
    }

    /**
     * Copy partitioner configuration from audit properties to producer properties.
     * This includes:
     * 1. Configured appIds - defines which sources are active
     * 2. Partition overrides - defines partition count per appId
     */
    private void copyPartitionerConfiguration(Properties props, String propPrefix) {
        // Copy configured plugin app IDs - needed for partitioner initialization
        String configuredAppIdsKey = propPrefix + "." + AuditServerConstants.PROP_CONFIGURED_APP_IDS;
        String configuredPlugins   = MiscUtil.getStringProperty(props, configuredAppIdsKey, null);
        if (configuredPlugins != null) {
            producerProps.put(configuredAppIdsKey, configuredPlugins);
            LOG.info("Configured plugin app IDs for partitioner: {}", configuredPlugins);
        }

        String partitionOverridePrefix = propPrefix + "." + AuditServerConstants.PROP_PARTITION_OVERRIDE_PREFIX + ".";
        for (String key : props.stringPropertyNames()) {
            if (key.startsWith(partitionOverridePrefix)) {
                String value = props.getProperty(key);
                producerProps.put(key, value);
                LOG.info("Configured partition override: {} = {}", key, value);
            }
        }
    }

    @Override
    public void run() {
        LOG.info("AuditProducer thread started");
        while (running) {
            try {
                Thread.sleep(100);  // keep thread alive
            } catch (InterruptedException e) {
                LOG.info("AuditProducer: Thread interrupted. Exiting...");
                Thread.currentThread().interrupt();
                break;
            }
        }
        LOG.info("AuditProducer thread stopped");
    }

    public void shutdown() {
        LOG.info("==> AuditProducer.shutdown()");
        running = false;

        if (kafkaProducer != null) {
            try {
                LOG.info("Closing Kafka producer...");
                kafkaProducer.close();
                LOG.info("Kafka producer closed successfully");
            } catch (Exception e) {
                LOG.error("Error closing Kafka producer", e);
            }
        }

        LOG.info("<== AuditProducer.shutdown()");
    }

    public KafkaProducer<String, String> getKafkaProducer() {
        return kafkaProducer;
    }

    public static void send(KafkaProducer<String, String> producer, String topic, String key, String value) throws Exception {
        ProducerRecord<String, String> auditEvent = new ProducerRecord<>(topic, key, value);
        try {
            producer.send(auditEvent, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception e) {
                    if (e != null) {
                        LOG.error("Error sending Ranger Audit logs to Kafka....", e);
                    } else {
                        LOG.debug("Ranger Audit sent to Topic: {} Partition: {} Offset: {}", metadata.topic(), metadata.partition(), metadata.offset());
                    }
                }
            });
        } catch (Exception e) {
            throw  new Exception(e);
        }
    }
}
