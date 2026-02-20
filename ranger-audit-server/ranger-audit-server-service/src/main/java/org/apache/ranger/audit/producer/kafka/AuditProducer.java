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

package org.apache.ranger.audit.producer.kafka;

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

        producerProps.put(ProducerConfig.LINGER_MS_CONFIG, 5);
        producerProps.put(ProducerConfig.BATCH_SIZE_CONFIG, 32 * 1024);

        // Check if configured.plugins is set to determine partitioning strategy
        // 1) configured.plugins is set then Custom AuditPartitioner is used, it allocates predefined set of partitions to each appId.
        // 2) if configured.plugins is not set then default kafka hash based partitioner is used with initial quota of 10 partition.
        String configuredPlugins = MiscUtil.getStringProperty(props, propPrefix + "." + AuditServerConstants.PROP_CONFIGURED_PLUGINS, "");
        if (configuredPlugins != null && !configuredPlugins.trim().isEmpty()) {
            // Plugin-based partitioning: use AuditPartitioner
            String partitionerClass = MiscUtil.getStringProperty(props, propPrefix + "." + AuditServerConstants.PROP_PARTITIONER_CLASS, AuditServerConstants.DEFAULT_PARTITIONER_CLASS);
            producerProps.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, partitionerClass);
            LOG.info("Configured plugins detected - using plugin-based partitioner: {}", partitionerClass);

            // Pass all xasecure.audit.destination.kafka.* properties to partitioner (no namespace translation)
            for (String propName : props.stringPropertyNames()) {
                if (propName.startsWith(propPrefix + ".")) {
                    producerProps.put(propName, props.getProperty(propName));
                }
            }
        } else {
            // No configured plugins: use Kafka default hash-based partitioner
            LOG.info("No configured plugins - using Kafka default hash-based partitioner");
        }

        try {
            kafkaProducer = MiscUtil.executePrivilegedAction((PrivilegedExceptionAction<KafkaProducer<String, String>>) () -> new KafkaProducer<>(producerProps));
            LOG.info("AuditProducer(): KafkaProducer created successfully!");
        } catch (Exception ex) {
            LOG.warn("AuditProducer(): Unable to create KafkaProducer - Kafka may not be available. " +
                     "Audit messages will be spooled to recovery system for retry. Error: {}", ex.getMessage());
            LOG.debug("Full exception details:", ex);
        }

        LOG.debug("<== AuditProducer()");
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
