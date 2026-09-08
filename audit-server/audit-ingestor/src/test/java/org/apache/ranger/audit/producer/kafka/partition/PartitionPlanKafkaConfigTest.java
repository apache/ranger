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

package org.apache.ranger.audit.producer.kafka.partition;

import org.apache.ranger.audit.server.AuditServerConstants;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class PartitionPlanKafkaConfigTest {
    private static final String PROP_PREFIX = AuditServerConstants.PROP_PREFIX_AUDIT_SERVER + "kafka";

    @Test
    public void testResolvePlanTopicNameUsesDefault() {
        Properties props = new Properties();
        assertEquals(AuditServerConstants.DEFAULT_PARTITION_PLAN_TOPIC, PartitionPlanKafkaConfig.resolvePlanTopicName(props, PROP_PREFIX));
    }

    @Test
    public void testResolvePlanTopicNameUsesOverride() {
        Properties props = new Properties();
        props.setProperty(PROP_PREFIX + "." + AuditServerConstants.PROP_PARTITION_PLAN_TOPIC, "custom_plan_topic");
        assertEquals("custom_plan_topic", PartitionPlanKafkaConfig.resolvePlanTopicName(props, PROP_PREFIX));
    }

    @Test
    public void testDynamicPartitionPlanDisabledByDefault() {
        assertFalse(PartitionPlanKafkaConfig.isDynamicPartitionPlanEnabled(new Properties(), PROP_PREFIX));
    }

    @Test
    public void testDynamicPartitionPlanEnabledFromProperty() {
        Properties props = new Properties();
        props.setProperty(PROP_PREFIX + "." + AuditServerConstants.PROP_PARTITION_PLAN_DYNAMIC_ENABLED, "true");
        assertTrue(PartitionPlanKafkaConfig.isDynamicPartitionPlanEnabled(props, PROP_PREFIX));
    }

    @Test
    public void testResolveRefreshIntervalUsesDefault() {
        assertEquals(AuditServerConstants.DEFAULT_PARTITION_PLAN_REFRESH_INTERVAL_MS, PartitionPlanKafkaConfig.resolveRefreshIntervalMs(new Properties(), PROP_PREFIX));
    }

    @Test
    public void testResolveConsumerPollTimeoutUsesDefault() {
        assertEquals(AuditServerConstants.DEFAULT_PARTITION_PLAN_CONSUMER_POLL_TIMEOUT_MS, PartitionPlanKafkaConfig.resolveConsumerPollTimeoutMs(new Properties(), PROP_PREFIX));
    }

    @Test
    public void testResolveConsumerPollTimeoutUsesOverride() {
        Properties props = new Properties();
        props.setProperty(PROP_PREFIX + "." + AuditServerConstants.PROP_PARTITION_PLAN_CONSUMER_POLL_TIMEOUT_MS, "1000");
        assertEquals(1000, PartitionPlanKafkaConfig.resolveConsumerPollTimeoutMs(props, PROP_PREFIX));
    }
}
