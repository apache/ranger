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

import org.apache.ranger.audit.producer.kafka.partition.constants.PartitionPlanConstants;
import org.apache.ranger.audit.producer.kafka.partition.model.PartitionPlan;
import org.apache.ranger.audit.server.AuditServerConstants;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;

public class PartitionPlanBootstrapTest {
    private static final String TOPIC = "ranger_audits";

    @Test
    public void testCreateInitialPlanMatchesStaticPartitionerLayout() {
        PartitionPlan plan = PartitionPlanBootstrap.createInitialPlan(PartitionPlanBootstrapConfig.create(TOPIC, new String[] {"hdfs", "hiveServer2"}, 3, 9));

        assertEquals(TOPIC, plan.getTopic());
        assertEquals(1, plan.getVersion());
        assertEquals(15, plan.getTopicPartitionCount());
        assertEquals(PartitionPlanConstants.BOOTSTRAP_UPDATED_BY, plan.getUpdatedBy());
        assertIterableEquals(List.of(0, 1, 2), plan.getPlugins().get("hdfs").getPartitions());
        assertIterableEquals(List.of(3, 4, 5), plan.getPlugins().get("hiveServer2").getPartitions());
        assertIterableEquals(List.of(6, 7, 8, 9, 10, 11, 12, 13, 14), plan.getBuffer().getPartitions());
    }

    @Test
    public void testCreateInitialPlanHonorsPluginOverrides() {
        PartitionPlanBootstrapConfig config = PartitionPlanBootstrapConfig.create(TOPIC, new String[] {"hdfs", "trino"}, 3, 4).withPluginOverride("hdfs", 5);
        PartitionPlan plan = PartitionPlanBootstrap.createInitialPlan(config);

        assertEquals(12, plan.getTopicPartitionCount());
        assertIterableEquals(List.of(0, 1, 2, 3, 4), plan.getPlugins().get("hdfs").getPartitions());
        assertIterableEquals(List.of(5, 6, 7), plan.getPlugins().get("trino").getPartitions());
        assertIterableEquals(List.of(8, 9, 10, 11), plan.getBuffer().getPartitions());
    }

    @Test
    public void testCreateInitialPlanEmptyPluginsUsesHashBasedTopicPartitions() {
        PartitionPlan plan = PartitionPlanBootstrap.createInitialPlan(
                new PartitionPlanBootstrapConfig(TOPIC, new String[0], 3, 9, 10, Collections.emptyMap()));

        assertEquals(10, plan.getTopicPartitionCount());
        assertEquals(0, plan.getPlugins().size());
        assertIterableEquals(List.of(0, 1, 2, 3, 4, 5, 6, 7, 8, 9), plan.getBuffer().getPartitions());
    }

    @Test
    public void testCreateInitialPlanFromProducerConfig() {
        String propPrefix = AuditServerConstants.PROP_PREFIX_AUDIT_SERVER;
        Map<String, Object> configs = new HashMap<>();
        configs.put(propPrefix + AuditServerConstants.PROP_CONFIGURED_PLUGINS, "hdfs,hiveServer2");
        configs.put(propPrefix + AuditServerConstants.PROP_TOPIC_PARTITIONS_PER_CONFIGURED_PLUGIN, 3);
        configs.put(propPrefix + AuditServerConstants.PROP_BUFFER_PARTITIONS, 9);

        PartitionPlan plan = PartitionPlanBootstrap.createInitialPlanFromProducerConfig(configs, TOPIC);

        assertEquals(15, plan.getTopicPartitionCount());
        assertIterableEquals(List.of(0, 1, 2), plan.getPlugins().get("hdfs").getPartitions());
        assertIterableEquals(List.of(6, 7, 8, 9, 10, 11, 12, 13, 14), plan.getBuffer().getPartitions());
    }

    @Test
    public void testBootstrapReturnsExistingPlan() {
        PartitionPlan existing = samplePlan();
        InMemoryPartitionPlanRegistry registry = new InMemoryPartitionPlanRegistry(existing);
        Map<String, Object> config = producerConfig();

        PartitionPlan plan = PartitionPlanBootstrap.bootstrapIfEmpty(registry, TOPIC, config);

        assertEquals(existing, plan);
        assertEquals(0, registry.getWriteCount());
    }

    @Test
    public void testBootstrapPublishesV1WhenEmpty() {
        InMemoryPartitionPlanRegistry registry = new InMemoryPartitionPlanRegistry(null);
        Map<String, Object> config = producerConfig();

        PartitionPlan plan = PartitionPlanBootstrap.bootstrapIfEmpty(registry, TOPIC, config);

        assertEquals(1, plan.getVersion());
        assertEquals(15, plan.getTopicPartitionCount());
        assertIterableEquals(plan.getPlugins().get("hdfs").getPartitions(), registry.readPlan(TOPIC).getPlugins().get("hdfs").getPartitions());
        assertEquals(1, registry.getWriteCount());
    }

    @Test
    public void testBootstrapAdoptsPeerPlanBeforePublish() {
        PartitionPlan peerPlan = samplePlan().toBuilder().updatedBy("peer").build();
        PeerPublishesOnSecondReadRegistry registry = new PeerPublishesOnSecondReadRegistry(peerPlan);

        PartitionPlan plan = PartitionPlanBootstrap.bootstrapIfEmpty(registry, TOPIC, producerConfig());

        assertEquals(peerPlan, plan);
        assertEquals(0, registry.getWriteCount());
    }

    private static Map<String, Object> producerConfig() {
        String propPrefix = AuditServerConstants.PROP_PREFIX_AUDIT_SERVER;
        Map<String, Object> config = new HashMap<>();
        config.put(propPrefix + AuditServerConstants.PROP_CONFIGURED_PLUGINS, "hdfs,hiveServer2");
        config.put(propPrefix + AuditServerConstants.PROP_TOPIC_PARTITIONS_PER_CONFIGURED_PLUGIN, 3);
        config.put(propPrefix + AuditServerConstants.PROP_BUFFER_PARTITIONS, 9);
        return config;
    }

    private static PartitionPlan samplePlan() {
        return PartitionPlanBootstrap.createInitialPlan(PartitionPlanBootstrapConfig.create(TOPIC, new String[] {"hdfs", "hiveServer2"}, 3, 9));
    }

    private static final class PeerPublishesOnSecondReadRegistry implements PartitionPlanRegistry {
        private final PartitionPlan peerPlan;
        private int readCount;
        private int writeCount;

        private PeerPublishesOnSecondReadRegistry(PartitionPlan peerPlan) {
            this.peerPlan = peerPlan;
        }

        @Override
        public PartitionPlan readPlan(String auditTopicKey) {
            readCount++;
            if (readCount >= 2) {
                return peerPlan;
            }
            return null;
        }

        @Override
        public void writePlan(String auditTopicKey, PartitionPlan plan) {
            writeCount++;
        }

        @Override
        public void close() {
        }

        private int getWriteCount() {
            return writeCount;
        }
    }

    private static class InMemoryPartitionPlanRegistry implements PartitionPlanRegistry {
        private final AtomicReference<PartitionPlan> planRef;
        private int writeCount;

        private InMemoryPartitionPlanRegistry(PartitionPlan initialPlan) {
            this.planRef = new AtomicReference<>(initialPlan);
        }

        @Override
        public PartitionPlan readPlan(String auditTopicKey) {
            return planRef.get();
        }

        @Override
        public void writePlan(String auditTopicKey, PartitionPlan plan) {
            planRef.set(plan);
            writeCount++;
        }

        @Override
        public void close() {
        }

        private int getWriteCount() {
            return writeCount;
        }
    }
}
