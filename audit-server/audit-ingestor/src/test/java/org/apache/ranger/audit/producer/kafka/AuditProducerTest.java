/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ranger.audit.producer.kafka;

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.ranger.audit.server.AuditServerConstants;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

public class AuditProducerTest {
    private static final String PROP_PREFIX = AuditServerConstants.PROP_PREFIX_AUDIT_SERVER + "kafka";

    private static void assertProducerProp(Properties props, String key, Object expected) {
        assertEquals(String.valueOf(expected), String.valueOf(props.get(key)));
    }

    @Test
    public void testCreateProducerConfigDefaults() throws Exception {
        Properties props = new Properties();
        props.setProperty(PROP_PREFIX + "." + AuditServerConstants.PROP_BOOTSTRAP_SERVERS, "kafka:9092");
        props.setProperty(PROP_PREFIX + "." + AuditServerConstants.PROP_SECURITY_PROTOCOL, "PLAINTEXT");

        Properties producerProps = AuditProducer.createProducerConfig(props, PROP_PREFIX);

        assertEquals("kafka:9092", producerProps.get(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));
        assertEquals("true", producerProps.get(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG));
        assertEquals("all", producerProps.get(ProducerConfig.ACKS_CONFIG));
        assertProducerProp(producerProps, ProducerConfig.BATCH_SIZE_CONFIG, AuditServerConstants.DEFAULT_PRODUCER_BATCH_SIZE);
        assertProducerProp(producerProps, ProducerConfig.LINGER_MS_CONFIG, AuditServerConstants.DEFAULT_PRODUCER_LINGER_MS);
        assertProducerProp(producerProps, ProducerConfig.BUFFER_MEMORY_CONFIG, AuditServerConstants.DEFAULT_PRODUCER_BUFFER_MEMORY);
        assertEquals(AuditServerConstants.DEFAULT_PRODUCER_COMPRESSION_TYPE,
                producerProps.get(ProducerConfig.COMPRESSION_TYPE_CONFIG));
        assertProducerProp(producerProps, ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, AuditServerConstants.DEFAULT_PRODUCER_DELIVERY_TIMEOUT_MS);
        assertProducerProp(producerProps, ProducerConfig.MAX_REQUEST_SIZE_CONFIG, AuditServerConstants.DEFAULT_PRODUCER_MAX_REQUEST_SIZE);
        assertProducerProp(producerProps, ProducerConfig.MAX_BLOCK_MS_CONFIG, AuditServerConstants.DEFAULT_PRODUCER_MAX_BLOCK_MS);
        assertProducerProp(producerProps, ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, AuditServerConstants.DEFAULT_PRODUCER_REQUEST_TIMEOUT_MS);
        assertEquals("PLAINTEXT", producerProps.get(AdminClientConfig.SECURITY_PROTOCOL_CONFIG));
        assertFalse(producerProps.containsKey(ProducerConfig.PARTITIONER_CLASS_CONFIG));
    }

    @Test
    public void testCreateProducerConfigCustomTuning() throws Exception {
        Properties props = new Properties();
        props.setProperty(PROP_PREFIX + "." + AuditServerConstants.PROP_BOOTSTRAP_SERVERS, "broker1:9092,broker2:9092");
        props.setProperty(PROP_PREFIX + "." + AuditServerConstants.PROP_SECURITY_PROTOCOL, "PLAINTEXT");
        props.setProperty(PROP_PREFIX + "." + AuditServerConstants.PROP_REQ_TIMEOUT_MS, "45000");

        String producerPrefix = PROP_PREFIX + "." + AuditServerConstants.PROP_KAFKA_PRODUCER_PREFIX;
        props.setProperty(producerPrefix + AuditServerConstants.PROP_PRODUCER_BATCH_SIZE, "262144");
        props.setProperty(producerPrefix + AuditServerConstants.PROP_PRODUCER_LINGER_MS, "50");
        props.setProperty(producerPrefix + AuditServerConstants.PROP_PRODUCER_BUFFER_MEMORY, "268435456");
        props.setProperty(producerPrefix + AuditServerConstants.PROP_PRODUCER_COMPRESSION_TYPE, "snappy");
        props.setProperty(producerPrefix + AuditServerConstants.PROP_PRODUCER_DELIVERY_TIMEOUT_MS, "180000");
        props.setProperty(producerPrefix + AuditServerConstants.PROP_PRODUCER_MAX_REQUEST_SIZE, "2097152");
        props.setProperty(producerPrefix + AuditServerConstants.PROP_PRODUCER_MAX_BLOCK_MS, "120000");

        Properties producerProps = AuditProducer.createProducerConfig(props, PROP_PREFIX);

        assertProducerProp(producerProps, ProducerConfig.BATCH_SIZE_CONFIG, 262144);
        assertProducerProp(producerProps, ProducerConfig.LINGER_MS_CONFIG, 50);
        assertProducerProp(producerProps, ProducerConfig.BUFFER_MEMORY_CONFIG, 268435456L);
        assertEquals("snappy", producerProps.get(ProducerConfig.COMPRESSION_TYPE_CONFIG));
        assertProducerProp(producerProps, ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 180000);
        assertProducerProp(producerProps, ProducerConfig.MAX_REQUEST_SIZE_CONFIG, 2097152);
        assertProducerProp(producerProps, ProducerConfig.MAX_BLOCK_MS_CONFIG, 120000);
        assertProducerProp(producerProps, ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 45000);
    }

    @Test
    public void testCreateProducerConfigPluginPartitioner() throws Exception {
        Properties props = new Properties();
        props.setProperty(PROP_PREFIX + "." + AuditServerConstants.PROP_BOOTSTRAP_SERVERS, "kafka:9092");
        props.setProperty(PROP_PREFIX + "." + AuditServerConstants.PROP_SECURITY_PROTOCOL, "PLAINTEXT");
        props.setProperty(PROP_PREFIX + "." + AuditServerConstants.PROP_CONFIGURED_PLUGINS, "hdfs,hive");
        props.setProperty(PROP_PREFIX + "." + AuditServerConstants.PROP_PARTITIONER_CLASS,
                AuditServerConstants.DEFAULT_PARTITIONER_CLASS);
        props.setProperty(PROP_PREFIX + "." + AuditServerConstants.PROP_TOPIC_PARTITIONS_PER_CONFIGURED_PLUGIN, "3");

        Properties producerProps = AuditProducer.createProducerConfig(props, PROP_PREFIX);

        assertEquals(AuditServerConstants.DEFAULT_PARTITIONER_CLASS,
                producerProps.get(ProducerConfig.PARTITIONER_CLASS_CONFIG));
        assertEquals("hdfs,hive", producerProps.get(PROP_PREFIX + "." + AuditServerConstants.PROP_CONFIGURED_PLUGINS));
        assertEquals("3", producerProps.get(PROP_PREFIX + "." + AuditServerConstants.PROP_TOPIC_PARTITIONS_PER_CONFIGURED_PLUGIN));
    }
}
