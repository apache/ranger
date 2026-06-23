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

import org.apache.ranger.audit.producer.kafka.partition.model.PartitionPlan;
import org.apache.ranger.audit.server.AuditServerConstants;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;

public class PartitionPlanBootstrapSupportTest {
    private static final String TOPIC = "ranger_audits";

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
