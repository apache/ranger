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
import org.apache.ranger.audit.utils.AuditServerLogFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class AuditProducer implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(AuditProducer.class);

    /** Max wait for REST batch send callbacks; overridable via site XML. */
    private static volatile int batchSendTimeoutMs = AuditServerConstants.DEFAULT_PRODUCER_BATCH_SEND_TIMEOUT_MS;

    public Properties                    producerProps = new Properties();
    public KafkaProducer<String, String> kafkaProducer;
    private volatile boolean             running = true;

    public AuditProducer(Properties props, String propPrefix) throws Exception {
        LOG.debug("==> AuditProducer()");

        producerProps = createProducerConfig(props, propPrefix);

        try {
            PrivilegedExceptionAction<KafkaProducer<String, String>> createProducer = () -> new KafkaProducer<>(producerProps);
            kafkaProducer = MiscUtil.executePrivilegedAction(createProducer);
            logProducerProperties(producerProps);
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
                kafkaProducer.flush();
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

    private static void logProducerProperties(final Properties props) {
        AuditServerLogFormatter.LogBuilder logBuilder = AuditServerLogFormatter.builder("AuditProducer(): KafkaProducer properties");
        ArrayList<String> names = new ArrayList<>(props.stringPropertyNames());
        Collections.sort(names);

        for (String name : names) {
            String value = props.getProperty(name);

            if (AuditServerConstants.PROP_SASL_JAAS_CONFIG.equals(name)) {
                value = "***";
            }

            logBuilder.add(name, value);
        }

        logBuilder.logInfo(LOG);
    }

    /**
     * Build Kafka producer properties from ingestor site XML (testable without connecting to Kafka).
     */
    static Properties createProducerConfig(Properties props, String propPrefix) throws Exception {
        Properties producerProps = new Properties();

        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, MiscUtil.getStringProperty(props, propPrefix + "." + AuditServerConstants.PROP_BOOTSTRAP_SERVERS));
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, Class.forName("org.apache.kafka.common.serialization.StringSerializer"));
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, Class.forName("org.apache.kafka.common.serialization.StringSerializer"));
        producerProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        producerProps.put(ProducerConfig.ACKS_CONFIG, "all");

        String producerPrefix = propPrefix + "." + AuditServerConstants.PROP_KAFKA_PRODUCER_PREFIX;

        producerProps.put(ProducerConfig.BATCH_SIZE_CONFIG,
                MiscUtil.getIntProperty(props, producerPrefix + AuditServerConstants.PROP_PRODUCER_BATCH_SIZE,
                        AuditServerConstants.DEFAULT_PRODUCER_BATCH_SIZE));
        producerProps.put(ProducerConfig.LINGER_MS_CONFIG,
                MiscUtil.getIntProperty(props, producerPrefix + AuditServerConstants.PROP_PRODUCER_LINGER_MS,
                        AuditServerConstants.DEFAULT_PRODUCER_LINGER_MS));
        producerProps.put(ProducerConfig.BUFFER_MEMORY_CONFIG,
                MiscUtil.getLongProperty(props, producerPrefix + AuditServerConstants.PROP_PRODUCER_BUFFER_MEMORY,
                        AuditServerConstants.DEFAULT_PRODUCER_BUFFER_MEMORY));
        producerProps.put(ProducerConfig.COMPRESSION_TYPE_CONFIG,
                MiscUtil.getStringProperty(props, producerPrefix + AuditServerConstants.PROP_PRODUCER_COMPRESSION_TYPE,
                        AuditServerConstants.DEFAULT_PRODUCER_COMPRESSION_TYPE));
        producerProps.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG,
                MiscUtil.getIntProperty(props, producerPrefix + AuditServerConstants.PROP_PRODUCER_DELIVERY_TIMEOUT_MS,
                        AuditServerConstants.DEFAULT_PRODUCER_DELIVERY_TIMEOUT_MS));
        producerProps.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG,
                MiscUtil.getIntProperty(props, producerPrefix + AuditServerConstants.PROP_PRODUCER_MAX_REQUEST_SIZE,
                        AuditServerConstants.DEFAULT_PRODUCER_MAX_REQUEST_SIZE));
        producerProps.put(ProducerConfig.MAX_BLOCK_MS_CONFIG,
                MiscUtil.getIntProperty(props, producerPrefix + AuditServerConstants.PROP_PRODUCER_MAX_BLOCK_MS,
                        AuditServerConstants.DEFAULT_PRODUCER_MAX_BLOCK_MS));
        producerProps.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG,
                MiscUtil.getIntProperty(props, propPrefix + "." + AuditServerConstants.PROP_REQ_TIMEOUT_MS,
                        AuditServerConstants.DEFAULT_PRODUCER_REQUEST_TIMEOUT_MS));

        batchSendTimeoutMs = MiscUtil.getIntProperty(props,
                producerPrefix + AuditServerConstants.PROP_PRODUCER_BATCH_SEND_TIMEOUT_MS,
                AuditServerConstants.DEFAULT_PRODUCER_BATCH_SEND_TIMEOUT_MS);

        String securityProtocol = MiscUtil.getStringProperty(props, propPrefix + "." + AuditServerConstants.PROP_SECURITY_PROTOCOL, AuditServerConstants.DEFAULT_SECURITY_PROTOCOL);
        producerProps.put(AdminClientConfig.SECURITY_PROTOCOL_CONFIG, securityProtocol);

        producerProps.put(AuditServerConstants.PROP_SASL_MECHANISM, MiscUtil.getStringProperty(props, propPrefix + "." + AuditServerConstants.PROP_SASL_MECHANISM, AuditServerConstants.DEFAULT_SASL_MECHANISM));
        producerProps.put(AuditServerConstants.PROP_SASL_KERBEROS_SERVICE_NAME, AuditServerConstants.DEFAULT_SERVICE_NAME);

        if (securityProtocol.toUpperCase().contains(AuditServerConstants.PROP_SECURITY_PROTOCOL_VALUE)) {
            producerProps.put(AuditServerConstants.PROP_SASL_JAAS_CONFIG, AuditMessageQueueUtils.getJAASConfig(props, propPrefix));
        }

        String configuredPlugins = MiscUtil.getStringProperty(props, propPrefix + "." + AuditServerConstants.PROP_CONFIGURED_PLUGINS, "");
        if (configuredPlugins != null && !configuredPlugins.trim().isEmpty()) {
            String partitionerClass = MiscUtil.getStringProperty(props, propPrefix + "." + AuditServerConstants.PROP_PARTITIONER_CLASS, AuditServerConstants.DEFAULT_PARTITIONER_CLASS);
            producerProps.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, partitionerClass);
            LOG.info("Configured plugins detected - using plugin-based partitioner: {}", partitionerClass);

            for (String propName : props.stringPropertyNames()) {
                if (propName.startsWith(propPrefix + ".")) {
                    producerProps.put(propName, props.getProperty(propName));
                }
            }
        } else {
            LOG.info("No configured plugins - using Kafka default hash-based partitioner");
        }

        return producerProps;
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

    /**
     * Send a batch of audit events to Kafka efficiently using a batch key for all events.
     *
     * @param producer Kafka producer instance
     * @param topic Topic to send to
     * @param batchKey Single key (appId) to use for ALL events in the batch
     * @param values List of serialized event messages
     * @throws Exception if batch send fails
     */
    public static void sendBatch(KafkaProducer<String, String> producer, String topic, String batchKey, List<String> values) throws Exception {
        int batchSize = values.size();
        LOG.debug("==> AuditProducer.sendBatch(): Sending batch of {} events to topic: {} with single key: {}", batchSize, topic, batchKey);

        final CountDownLatch   latch         = new CountDownLatch(batchSize);
        final AtomicInteger    errorCount    = new AtomicInteger(0);
        final List<Exception>  errors        = new ArrayList<>();
        final List<Integer>    failedIndices = new ArrayList<>();
        final List<String>     failedValues  = new ArrayList<>();

        // Send all records asynchronously with the same batch key
        // With custom partitioning: events distributed round-robin across partition range
        // Without custom partitioning: all events go to same partition (hash-based)
        for (int i = 0; i < batchSize; i++) {
            final String value = values.get(i);
            final int index = i;

            ProducerRecord<String, String> record = new ProducerRecord<>(topic, batchKey, value);

            try {
                producer.send(record, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception e) {
                        if (e != null) {
                            errorCount.incrementAndGet();
                            synchronized (errors) {
                                errors.add(e);
                                failedIndices.add(index);
                                failedValues.add(value);
                            }
                            LOG.error("Error sending audit event {} in batch to Kafka: {}", index, e.getMessage());
                        } else {
                            LOG.debug("Batch event {} sent to Topic: {} Partition: {} Offset: {}", index, metadata.topic(), metadata.partition(), metadata.offset());
                        }
                        latch.countDown();
                    }
                });
            } catch (Exception e) {
                errorCount.incrementAndGet();
                synchronized (errors) {
                    errors.add(e);
                    failedIndices.add(i);
                    failedValues.add(value);
                }
                LOG.error("Failed to queue audit event {} for sending: {}", i, e.getMessage());
                latch.countDown();
            }
        }

        // max wait time before timeout (configurable via ranger.audit.ingestor.kafka.producer.batch.send.timeout.ms)
        boolean completed = latch.await(batchSendTimeoutMs, TimeUnit.MILLISECONDS);

        if (!completed) {
            String errorMsg = String.format("Batch send timed out after %d ms. %d/%d events still pending",
                    batchSendTimeoutMs, latch.getCount(), batchSize);
            LOG.error(errorMsg);
            throw new Exception(errorMsg);
        }

        if (errorCount.get() > 0) {
            int successCount = batchSize - errorCount.get();
            String errorMsg = String.format("Batch send had %d/%d failures, %d succeeded", errorCount.get(), batchSize, successCount);
            LOG.error(errorMsg);
            LOG.error("Failed event indices: {}", failedIndices);

            // Create exception with failed values for selective retry
            BatchSendException batchException = new BatchSendException(errorMsg, failedValues);
            if (!errors.isEmpty()) {
                batchException.initCause(errors.get(0));
            }
            throw batchException;
        }

        LOG.debug("<== AuditProducer.sendBatch(): Successfully sent batch of {} events to Kafka topic: {}, key: {}", batchSize, topic, batchKey);
    }

    /**
     * Custom exception that carries the list of failed messages for selective retry.
     * This prevents duplicates by only retrying the messages that actually failed.
     */
    public static class BatchSendException extends Exception {
        private final List<String> failedMessages;

        public BatchSendException(String message, List<String> failedMessages) {
            super(message);
            this.failedMessages = new ArrayList<>(failedMessages);
        }

        public List<String> getFailedMessages() {
            return failedMessages;
        }
    }
}
