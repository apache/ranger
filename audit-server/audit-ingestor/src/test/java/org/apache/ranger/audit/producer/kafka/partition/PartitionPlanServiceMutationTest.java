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
import org.apache.ranger.audit.producer.kafka.partition.model.OnboardService;
import org.apache.ranger.audit.producer.kafka.partition.model.PartitionPlan;
import org.apache.ranger.audit.producer.kafka.partition.model.PartitionPlanReplacement;
import org.apache.ranger.audit.producer.kafka.partition.model.PluginPartitionAssignment;
import org.apache.ranger.audit.producer.kafka.partition.model.PromotePlugin;
import org.apache.ranger.audit.producer.kafka.partition.model.PluginScale;
import org.apache.ranger.audit.producer.kafka.partition.model.ServiceAllowlistEntry;
import org.apache.ranger.audit.server.AuditServerConstants;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
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
    public void testPromotePluginPublishesNextVersion() throws Exception {
        MutableRegistry registry = new MutableRegistry(initialPlan);
        PartitionPlanService service = service(registry, new NoOpAuditTopicPartitionGrower());

        PartitionPlan result = service.promotePlugin(new PromotePlugin("trino", 3, 1), "ops");

        assertEquals(2, result.getVersion());
        assertEquals(2, registry.getPlan().getVersion());
        assertIterableEquals(List.of(6, 7, 8), result.getPlugins().get("trino").getPartitions());
        assertEquals(1, registry.getWriteCount());
        assertEquals(result, PartitionPlanHolder.getInstance().getPlan());
    }

    @Test
    public void testMergePlanAddsNewPluginOnly() throws Exception {
        MutableRegistry registry = new MutableRegistry(initialPlan);
        PartitionPlanService service = service(registry, new NoOpAuditTopicPartitionGrower());
        PartitionPlanReplacement request = new PartitionPlanReplacement(
                1,
                null,
                Map.of("trino", PluginPartitionAssignment.of(6, 7, 8)),
                null,
                null);

        PartitionPlan result = service.mergePartitionPlan(request, "ops");

        assertEquals(2, result.getVersion());
        assertIterableEquals(List.of(0, 1, 2), result.getPlugins().get("hdfs").getPartitions());
        assertIterableEquals(List.of(3, 4, 5), result.getPlugins().get("hiveServer2").getPartitions());
        assertIterableEquals(List.of(6, 7, 8), result.getPlugins().get("trino").getPartitions());
        assertIterableEquals(List.of(9, 10, 11, 12, 13, 14), result.getBuffer().getPartitions());
    }

    @Test
    public void testScalePluginAppendsTailPartitionsViaDedicatedEndpoint() throws Exception {
        MutableRegistry registry = new MutableRegistry(initialPlan);
        PartitionPlanService service = service(registry, new NoOpAuditTopicPartitionGrower());

        PartitionPlan result = service.scalePlugin("hiveServer2", new PluginScale(2, 1), "ops");

        assertEquals(2, result.getVersion());
        assertEquals(17, result.getTopicPartitionCount());
        assertIterableEquals(List.of(3, 4, 5, 15, 16), result.getPlugins().get("hiveServer2").getPartitions());
    }

    @Test
    public void testMergePlanRejectsExistingPluginDelta() {
        MutableRegistry registry = new MutableRegistry(initialPlan);
        PartitionPlanService service = service(registry, new NoOpAuditTopicPartitionGrower());
        PartitionPlanReplacement request = new PartitionPlanReplacement(
                1,
                null,
                Map.of("hiveServer2", PluginPartitionAssignment.of(3, 4, 5, 15, 16)),
                null,
                null);

        PartitionPlanException error = assertThrows(PartitionPlanException.class,
                () -> service.mergePartitionPlan(request, "ops"));

        assertTrue(error.getMessage().contains("different partition assignment"));
    }

    @Test
    public void testMergePlanCanAddServices() throws Exception {
        MutableRegistry registry = new MutableRegistry(initialPlan);
        PartitionPlanService service = service(registry, new NoOpAuditTopicPartitionGrower());
        Map<String, ServiceAllowlistEntry> services = new LinkedHashMap<>();
        services.put("dev_new_hive", ServiceAllowlistEntry.ofUsers("hive"));
        PartitionPlanReplacement request = new PartitionPlanReplacement(1, null, null, null, services);

        PartitionPlan result = service.mergePartitionPlan(request, "ops");

        assertEquals(2, result.getVersion());
        assertIterableEquals(List.of("hive"), result.getServices().get("dev_new_hive").getAllowedUsers());
    }

    @Test
    public void testPromotePluginRejectsBlankPluginId() {
        MutableRegistry registry = new MutableRegistry(initialPlan);
        PartitionPlanService service = service(registry, new NoOpAuditTopicPartitionGrower());

        PartitionPlanException error = assertThrows(PartitionPlanException.class,
                () -> service.promotePlugin(new PromotePlugin("  ", 3, 1), "ops"));

        assertTrue(error.getMessage().contains("pluginId"));
    }

    @Test
    public void testScalePluginRejectsInvalidAdditionalPartitions() {
        MutableRegistry registry = new MutableRegistry(initialPlan);
        PartitionPlanService service = service(registry, new NoOpAuditTopicPartitionGrower());

        PartitionPlanException error = assertThrows(PartitionPlanException.class,
                () -> service.scalePlugin("hiveServer2", new PluginScale(0, 1), "ops"));

        assertTrue(error.getMessage().contains("additionalPartitions"));
    }

    @Test
    public void testOnboardServiceRejectsMissingAllowedUsers() {
        MutableRegistry registry = new MutableRegistry(initialPlan);
        PartitionPlanService service = service(registry, new NoOpAuditTopicPartitionGrower());

        PartitionPlanException error = assertThrows(PartitionPlanException.class,
                () -> service.onboardService(new OnboardService("dev_trino", "trino", 3, List.of(), 1), "ops"));

        assertTrue(error.getMessage().contains("allowedUsers"));
    }

    @Test
    public void testOnboardRepoPromotesPluginAndUpsertsAllowlist() throws Exception {
        MutableRegistry registry = new MutableRegistry(initialPlan);
        PartitionPlanService service = service(registry, new NoOpAuditTopicPartitionGrower());

        PartitionPlan result = service.onboardService(new OnboardService("dev_trino", "trino", 3, List.of("trino"), 1), "ops");

        assertEquals(2, result.getVersion());
        assertIterableEquals(List.of(6, 7, 8), result.getPlugins().get("trino").getPartitions());
        assertIterableEquals(List.of("trino"), result.getServices().get("dev_trino").getAllowedUsers());
    }

    @Test
    public void testPromoteWithAllowlistUpsertsServices() throws Exception {
        MutableRegistry registry = new MutableRegistry(initialPlan);
        PartitionPlanService service = service(registry, new NoOpAuditTopicPartitionGrower());

        PartitionPlan result = service.promotePlugin(new PromotePlugin("trino", 3, 1, "dev_trino", List.of("trino")), "ops");

        assertEquals(2, result.getVersion());
        assertIterableEquals(List.of("trino"), result.getServices().get("dev_trino").getAllowedUsers());
    }

    @Test
    public void testStaleExpectedVersionReturnsConflict() {
        MutableRegistry registry = new MutableRegistry(initialPlan);
        PartitionPlanService service = service(registry, new NoOpAuditTopicPartitionGrower());

        PartitionPlanConflictException conflict = assertThrows(PartitionPlanConflictException.class,
                () -> service.promotePlugin(new PromotePlugin("trino", 3, 99), "ops"));

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
                () -> service.promotePlugin(new PromotePlugin("trino", 3, 1), "ops"));

        assertEquals(peerPlan, conflict.getCurrentPlan());
        assertEquals(0, registry.getWriteCount());
    }

    @Test
    public void testTopicGrowFailureSurfacesAsPlanException() {
        MutableRegistry registry = new MutableRegistry(initialPlan);
        PartitionPlanService service = service(registry, new FailingAuditTopicPartitionGrower());

        PartitionPlanException error = assertThrows(PartitionPlanException.class,
                () -> service.promotePlugin(new PromotePlugin("trino", 12, 1), "ops"));

        assertTrue(error.getMessage().contains("grow audit topic"));
        assertEquals(0, registry.getWriteCount());
    }

    @Test
    public void testDuplicateOnboardReturnsCurrentPlanWithoutWrite() throws Exception {
        MutableRegistry registry = new MutableRegistry(initialPlan);
        PartitionPlanService service = service(registry, new NoOpAuditTopicPartitionGrower());
        OnboardService request = new OnboardService("dev_trino", "trino", 3, List.of("trino"), 1);

        PartitionPlan first = service.onboardService(request, "ops");
        assertEquals(1, registry.getWriteCount());

        OnboardService retry = new OnboardService("dev_trino", "trino", 3, List.of("trino"), first.getVersion());
        PartitionPlan second = service.onboardService(retry, "ops");

        assertEquals(first, second);
        assertEquals(2, second.getVersion());
        assertEquals(1, registry.getWriteCount());
    }

    @Test
    public void testDuplicatePromoteReturnsCurrentPlanWithoutWrite() throws Exception {
        MutableRegistry registry = new MutableRegistry(initialPlan);
        PartitionPlanService service = service(registry, new NoOpAuditTopicPartitionGrower());
        PromotePlugin request = new PromotePlugin("trino", 3, 1);

        PartitionPlan first = service.promotePlugin(request, "ops");
        assertEquals(1, registry.getWriteCount());

        PromotePlugin retry = new PromotePlugin("trino", 3, first.getVersion());
        PartitionPlan second = service.promotePlugin(retry, "ops");

        assertEquals(first, second);
        assertEquals(2, second.getVersion());
        assertEquals(1, registry.getWriteCount());
    }

    @Test
    public void testPromoteConflictWhenPartitionCountDiffers() {
        MutableRegistry registry = new MutableRegistry(initialPlan);
        PartitionPlanService service = service(registry, new NoOpAuditTopicPartitionGrower());
        service.promotePlugin(new PromotePlugin("trino", 3, 1), "ops");

        PartitionPlanException error = assertThrows(PartitionPlanException.class,
                () -> service.promotePlugin(new PromotePlugin("trino", 5, 2), "ops"));

        assertTrue(error.getMessage().contains("requested 5"));
        assertEquals(1, registry.getWriteCount());
    }

    @Test
    public void testMergePlanNoOpWhenServicesUnchanged() throws Exception {
        MutableRegistry registry = new MutableRegistry(initialPlan);
        PartitionPlanService service = service(registry, new NoOpAuditTopicPartitionGrower());
        Map<String, ServiceAllowlistEntry> services = new LinkedHashMap<>();
        services.put("dev_hive", ServiceAllowlistEntry.ofUsers("hive"));
        service.mergePartitionPlan(new PartitionPlanReplacement(1, null, null, null, services), "ops");
        assertEquals(1, registry.getWriteCount());

        PartitionPlan result = service.mergePartitionPlan(new PartitionPlanReplacement(2, null, null, null, services), "ops");

        assertEquals(2, result.getVersion());
        assertEquals(1, registry.getWriteCount());
    }

    @Test
    public void testMergePlanStillAppliesWhenServicesChange() throws Exception {
        MutableRegistry registry = new MutableRegistry(initialPlan);
        PartitionPlanService service = service(registry, new NoOpAuditTopicPartitionGrower());
        Map<String, ServiceAllowlistEntry> services = new LinkedHashMap<>();
        services.put("dev_new_hive", ServiceAllowlistEntry.ofUsers("hive"));
        PartitionPlanReplacement request = new PartitionPlanReplacement(1, null, null, null, services);

        PartitionPlan result = service.mergePartitionPlan(request, "ops");

        assertEquals(2, result.getVersion());
        assertEquals(1, registry.getWriteCount());
    }

    @Test
    public void testScalePluginStillAppendsOnRepeat() throws Exception {
        MutableRegistry registry = new MutableRegistry(initialPlan);
        PartitionPlanService service = service(registry, new NoOpAuditTopicPartitionGrower());

        service.scalePlugin("hiveServer2", new PluginScale(2, 1), "ops");
        assertEquals(1, registry.getWriteCount());

        PartitionPlan second = service.scalePlugin("hiveServer2", new PluginScale(2, 2), "ops");

        assertEquals(3, second.getVersion());
        assertEquals(2, registry.getWriteCount());
        assertIterableEquals(List.of(3, 4, 5, 15, 16, 17, 18), second.getPlugins().get("hiveServer2").getPartitions());
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
