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

package org.apache.ranger.audit.utils;

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.ranger.audit.server.AuditServerConstants;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class AuditMessageQueueUtilsTest {
    private static final String PROP_PREFIX = AuditServerConstants.PROP_PREFIX_AUDIT_SERVER + "kafka";

    @Test
    public void testBuildTopicConfigsEmptyWhenUnset() {
        Properties props = new Properties();

        Map<String, String> configs = AuditMessageQueueUtils.buildTopicConfigs(props, PROP_PREFIX);

        assertTrue(configs.isEmpty());
    }

    @Test
    public void testBuildTopicConfigsSkipsBlankValues() {
        Properties props = new Properties();
        props.setProperty(PROP_PREFIX + "." + AuditServerConstants.PROP_TOPIC_RETENTION_MS, "   ");
        props.setProperty(PROP_PREFIX + "." + AuditServerConstants.PROP_TOPIC_COMPRESSION_TYPE, "lz4");
        props.setProperty(PROP_PREFIX + "." + AuditServerConstants.PROP_TOPIC_MIN_INSYNC_REPLICAS, "");

        Map<String, String> configs = AuditMessageQueueUtils.buildTopicConfigs(props, PROP_PREFIX);

        assertEquals(1, configs.size());
        assertEquals("lz4", configs.get("compression.type"));
    }

    @Test
    public void testBuildTopicConfigsMapsAllSetProperties() {
        Properties props = new Properties();
        props.setProperty(PROP_PREFIX + "." + AuditServerConstants.PROP_TOPIC_RETENTION_MS, "604800000");
        props.setProperty(PROP_PREFIX + "." + AuditServerConstants.PROP_TOPIC_COMPRESSION_TYPE, " lz4 ");
        props.setProperty(PROP_PREFIX + "." + AuditServerConstants.PROP_TOPIC_MIN_INSYNC_REPLICAS, "2");

        Map<String, String> configs = AuditMessageQueueUtils.buildTopicConfigs(props, PROP_PREFIX);

        assertEquals(3, configs.size());
        assertEquals("604800000", configs.get("retention.ms"));
        assertEquals("lz4", configs.get("compression.type"));
        assertEquals("2", configs.get("min.insync.replicas"));
    }

    @Test
    public void testBuildAdminClientConfigUsesBootstrapServers() {
        Properties props = new Properties();
        props.setProperty(PROP_PREFIX + "." + AuditServerConstants.PROP_BOOTSTRAP_SERVERS, "kafka:9092");
        props.setProperty(PROP_PREFIX + "." + AuditServerConstants.PROP_SECURITY_PROTOCOL, "PLAINTEXT");

        Map<String, Object> adminConfig = AuditMessageQueueUtils.buildAdminClientConfig(props, PROP_PREFIX);

        assertEquals("kafka:9092", adminConfig.get(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG));
        assertEquals("PLAINTEXT", adminConfig.get(AdminClientConfig.SECURITY_PROTOCOL_CONFIG));
    }
}
