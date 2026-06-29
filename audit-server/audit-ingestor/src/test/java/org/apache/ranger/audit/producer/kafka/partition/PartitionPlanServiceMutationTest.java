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

import org.apache.ranger.audit.producer.kafka.partition.exception.PartitionPlanConflictException;
import org.apache.ranger.audit.producer.kafka.partition.exception.PartitionPlanException;
import org.apache.ranger.audit.producer.kafka.partition.model.OnboardPlugin;
import org.apache.ranger.audit.producer.kafka.partition.model.PartitionPlan;
import org.apache.ranger.audit.producer.kafka.partition.model.ServiceAllowlistEntry;
import org.apache.ranger.audit.producer.kafka.partition.model.UpdatePlugin;
import org.apache.ranger.audit.server.AuditServerConstants;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class PartitionPlanServiceMutationTest {
    private static final String TOPIC = "ranger_audits";

    private PartitionPlan initialPlan;

    @BeforeEach
    public void setUp() {
        initialPlan = PartitionPlanBootstrap.createInitialPlan(PartitionPlanBootstrapConfig.create(TOPIC, new String[] {"hdfs", "hiveServer2"}, 3, 9));
    }

    @AfterEach
    public void tearDown() {
        PartitionPlanHolder.getInstance().resetForTests();
    }

    @Test
    public void testOnboardPluginPublishesNextVersion() throws Exception {
        MutableRegistry registry = new MutableRegistry(initialPlan);
        PartitionPlanService service = service(registry, new NoOpAuditTopicPartitionGrower());

        PartitionPlan result = service.onboardPlugin(new OnboardPlugin("trino", 3, 1, trinoServices()), "ops");

        assertEquals(2, result.getVersion());
        assertEquals(2, registry.getPlan().getVersion());
        assertIterableEquals(List.of(6, 7, 8), result.getPlugins().get("trino").getPartitions());
        assertEquals("trino", result.getServices().get("dev_trino").getPluginId());
        assertEquals(1, registry.getWriteCount());
        assertEquals(result, PartitionPlanHolder.getInstance().getPlan());
    }

    @Test
    public void testOnboardPluginWithMultipleServices() throws Exception {
        MutableRegistry registry = new MutableRegistry(initialPlan);
        PartitionPlanService service = service(registry, new NoOpAuditTopicPartitionGrower());
        Map<String, ServiceAllowlistEntry> services = new LinkedHashMap<>();
        services.put("dev_hive", ServiceAllowlistEntry.ofUsers("hive"));
        services.put("dev_hive2", ServiceAllowlistEntry.ofUsers("hive2"));

        PartitionPlan result = service.onboardPlugin(new OnboardPlugin("trino", 3, 1, services), "ops");

        assertEquals(2, result.getVersion());
        assertEquals("trino", result.getServices().get("dev_hive").getPluginId());
        assertEquals("trino", result.getServices().get("dev_hive2").getPluginId());
    }

    @Test
    public void testUpdatePluginAddsAndRemovesServices() throws Exception {
        MutableRegistry registry = new MutableRegistry(initialPlan);
        PartitionPlanService service = service(registry, new NoOpAuditTopicPartitionGrower());
        Map<String, ServiceAllowlistEntry> services = new LinkedHashMap<>();
        services.put("dev_hive", ServiceAllowlistEntry.ofUsers("hive"));
        services.put("dev_hive2", ServiceAllowlistEntry.ofUsers("hive2"));
        PartitionPlan onboarded = service.onboardPlugin(new OnboardPlugin("trino", 3, 1, services), "ops");

        UpdatePlugin update = new UpdatePlugin(onboarded.getVersion(), null, Map.of("dev_hive3", ServiceAllowlistEntry.ofUsers("hive3")), null, List.of("dev_hive2"));
        PartitionPlan updated = service.updatePlugin("trino", update, "ops");

        assertEquals(3, updated.getVersion());
        assertTrue(updated.getServices().containsKey("dev_hive"));
        assertTrue(updated.getServices().containsKey("dev_hive3"));
        assertFalse(updated.getServices().containsKey("dev_hive2"));
    }

    @Test
    public void testUpdatePluginScalesViaAdditionalPartitions() throws Exception {
        MutableRegistry registry = new MutableRegistry(initialPlan);
        PartitionPlanService service = service(registry, new NoOpAuditTopicPartitionGrower());

        PartitionPlan result = service.updatePlugin("hiveServer2", new UpdatePlugin(1, 2, null, null, null), "ops");

        assertEquals(2, result.getVersion());
        assertEquals(17, result.getTopicPartitionCount());
        assertIterableEquals(List.of(3, 4, 5, 15, 16), result.getPlugins().get("hiveServer2").getPartitions());
    }

    @Test
    public void testOnboardPluginRejectsEmptyServicesMap() {
        MutableRegistry registry = new MutableRegistry(initialPlan);
        PartitionPlanService service = service(registry, new NoOpAuditTopicPartitionGrower());

        PartitionPlanException error = assertThrows(PartitionPlanException.class,
                () -> service.onboardPlugin(new OnboardPlugin("trino", 3, 1, Collections.emptyMap()), "ops"));

        assertTrue(error.getMessage().contains("services are required"));
    }

    @Test
    public void testOnboardPluginRejectsMissingServices() {
        MutableRegistry registry = new MutableRegistry(initialPlan);
        PartitionPlanService service = service(registry, new NoOpAuditTopicPartitionGrower());

        PartitionPlanException error = assertThrows(PartitionPlanException.class,
                () -> service.onboardPlugin(new OnboardPlugin("trino", 3, 1), "ops"));

        assertTrue(error.getMessage().contains("services are required"));
    }

    @Test
    public void testOnboardPluginRejectsBlankPluginId() {
        MutableRegistry registry = new MutableRegistry(initialPlan);
        PartitionPlanService service = service(registry, new NoOpAuditTopicPartitionGrower());

        PartitionPlanException error = assertThrows(PartitionPlanException.class,
                () -> service.onboardPlugin(new OnboardPlugin("  ", 3, 1), "ops"));

        assertTrue(error.getMessage().contains("pluginId"));
    }

    @Test
    public void testUpdatePluginRejectsInvalidAdditionalPartitions() {
        MutableRegistry registry = new MutableRegistry(initialPlan);
        PartitionPlanService service = service(registry, new NoOpAuditTopicPartitionGrower());

        PartitionPlanException error = assertThrows(PartitionPlanException.class,
                () -> service.updatePlugin("hiveServer2", new UpdatePlugin(1, 0, null, null, null), "ops"));

        assertTrue(error.getMessage().contains("additionalPartitions"));
    }

    @Test
    public void testOnboardPluginRejectsServiceWithoutAllowedUsers() {
        MutableRegistry registry = new MutableRegistry(initialPlan);
        PartitionPlanService service = service(registry, new NoOpAuditTopicPartitionGrower());
        Map<String, ServiceAllowlistEntry> services = Map.of("dev_trino", ServiceAllowlistEntry.ofUsers(List.of()));

        PartitionPlanException error = assertThrows(PartitionPlanException.class,
                () -> service.onboardPlugin(new OnboardPlugin("trino", 3, 1, services), "ops"));

        assertTrue(error.getMessage().contains("allowedUsers"));
    }

    @Test
    public void testStaleExpectedVersionReturnsConflict() {
        MutableRegistry registry = new MutableRegistry(initialPlan);
        PartitionPlanService service = service(registry, new NoOpAuditTopicPartitionGrower());

        PartitionPlanConflictException conflict = assertThrows(PartitionPlanConflictException.class,
                () -> service.onboardPlugin(new OnboardPlugin("trino", 3, 99, trinoServices()), "ops"));

        assertEquals(initialPlan, conflict.getCurrentPlan());
        assertEquals(0, registry.getWriteCount());
    }

    @Test
    public void testConflictWhenPeerPublishedBeforeWrite() {
        PartitionPlan peerPlan = initialPlan.toBuilder().version(2).updatedBy("peer").build();
        MutableRegistry registry = new MutableRegistry(initialPlan) {
            private final AtomicInteger reads = new AtomicInteger();

            @Override
            public PartitionPlan readPlan(String auditTopicKey) {
                if (reads.incrementAndGet() >= 2) {
                    return peerPlan;
                }
                return super.readPlan(auditTopicKey);
            }
        };
        PartitionPlanService service = service(registry, new NoOpAuditTopicPartitionGrower());

        PartitionPlanConflictException conflict = assertThrows(PartitionPlanConflictException.class,
                () -> service.onboardPlugin(new OnboardPlugin("trino", 3, 1, trinoServices()), "ops"));

        assertEquals(peerPlan, conflict.getCurrentPlan());
        assertEquals(0, registry.getWriteCount());
    }

    @Test
    public void testTopicGrowFailureSurfacesAsPlanException() {
        MutableRegistry registry = new MutableRegistry(initialPlan);
        PartitionPlanService service = service(registry, new FailingAuditTopicPartitionGrower());

        PartitionPlanException error = assertThrows(PartitionPlanException.class,
                () -> service.onboardPlugin(new OnboardPlugin("trino", 12, 1, trinoServices()), "ops"));

        assertTrue(error.getMessage().contains("grow audit topic"));
        assertEquals(0, registry.getWriteCount());
    }

    @Test
    public void testDuplicateOnboardReturnsCurrentPlanWithoutWrite() throws Exception {
        MutableRegistry registry = new MutableRegistry(initialPlan);
        PartitionPlanService service = service(registry, new NoOpAuditTopicPartitionGrower());
        Map<String, ServiceAllowlistEntry> services = Map.of("dev_trino", ServiceAllowlistEntry.ofUsers("trino"));
        OnboardPlugin request = new OnboardPlugin("trino", 3, 1, services);

        PartitionPlan first = service.onboardPlugin(request, "ops");
        assertEquals(1, registry.getWriteCount());

        OnboardPlugin retry = new OnboardPlugin("trino", 3, first.getVersion(), services);
        PartitionPlan second = service.onboardPlugin(retry, "ops");

        assertEquals(first, second);
        assertEquals(2, second.getVersion());
        assertEquals(1, registry.getWriteCount());
    }

    @Test
    public void testUpdatePluginStillAppendsOnRepeatScale() throws Exception {
        MutableRegistry registry = new MutableRegistry(initialPlan);
        PartitionPlanService service = service(registry, new NoOpAuditTopicPartitionGrower());

        service.updatePlugin("hiveServer2", new UpdatePlugin(1, 2, null, null, null), "ops");
        assertEquals(1, registry.getWriteCount());

        PartitionPlan second = service.updatePlugin("hiveServer2", new UpdatePlugin(2, 2, null, null, null), "ops");

        assertEquals(3, second.getVersion());
        assertEquals(2, registry.getWriteCount());
        assertIterableEquals(List.of(3, 4, 5, 15, 16, 17, 18), second.getPlugins().get("hiveServer2").getPartitions());
    }

    private static Map<String, ServiceAllowlistEntry> trinoServices() {
        return Map.of("dev_trino", ServiceAllowlistEntry.ofUsers("trino"));
    }

    private static PartitionPlanService service(MutableRegistry registry, KafkaAuditTopicPartitionGrower topicGrower) {
        return new PartitionPlanService(enabledConfig(), PartitionPlanHolder.getInstance(), new FixedPartitionPlanRegistryFactory(registry), topicGrower);
    }

    private static Properties enabledConfig() {
        Properties props = new Properties();
        props.setProperty(PartitionPlanService.INGESTOR_PROP_PREFIX + "." + AuditServerConstants.PROP_TOPIC_NAME, TOPIC);
        props.setProperty(PartitionPlanService.INGESTOR_PROP_PREFIX + "." + AuditServerConstants.PROP_PARTITION_PLAN_DYNAMIC_ENABLED, "true");
        return props;
    }

    private static final class FixedPartitionPlanRegistryFactory extends PartitionPlanRegistryFactory {
        private final PartitionPlanRegistry registry;

        private FixedPartitionPlanRegistryFactory(PartitionPlanRegistry registry) {
            this.registry = registry;
        }

        @Override
        public PartitionPlanRegistry open(Properties props, String propPrefix) {
            return registry;
        }
    }

    private static final class NoOpAuditTopicPartitionGrower extends KafkaAuditTopicPartitionGrower {
        @Override
        public void growAuditTopicToRequiredPartitionCount(Properties props, String propPrefix, String auditTopicName, int requiredPartitionCount) {
        }
    }

    private static final class FailingAuditTopicPartitionGrower extends KafkaAuditTopicPartitionGrower {
        @Override
        public void growAuditTopicToRequiredPartitionCount(Properties props, String propPrefix, String auditTopicName, int requiredPartitionCount) {
            throw new RuntimeException("kafka down");
        }
    }

    private static class MutableRegistry implements PartitionPlanRegistry {
        private PartitionPlan plan;
        private int writeCount;

        private MutableRegistry(PartitionPlan plan) {
            this.plan = plan;
        }

        @Override
        public PartitionPlan readPlan(String auditTopicKey) {
            return plan;
        }

        @Override
        public void writePlan(String auditTopicKey, PartitionPlan newPlan) {
            plan = newPlan;
            writeCount++;
        }

        @Override
        public void close() {
        }

        private PartitionPlan getPlan() {
            return plan;
        }

        private int getWriteCount() {
            return writeCount;
        }
    }
}
