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

import org.apache.ranger.audit.producer.kafka.partition.exception.PartitionPlanException;
import org.apache.ranger.audit.producer.kafka.partition.model.PartitionPlan;
import org.apache.ranger.audit.producer.kafka.partition.model.ServiceAllowlistEntry;
import org.apache.ranger.audit.producer.kafka.partition.model.UpdatePlugin;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class PartitionPlanAllocatorTest {
    private PartitionPlan initialPlan;

    @BeforeEach
    public void setUp() {
        initialPlan = PartitionPlanBootstrap.createInitialPlan(PartitionPlanBootstrapConfig.create("ranger_audits", new String[] {"hdfs", "hiveServer2"}, 3, 9));
    }

    @Test
    public void testPromotePluginFromBuffer() {
        PartitionPlan next = PartitionPlanAllocator.promotePlugin(initialPlan, "trino", 3, "ops");

        assertEquals(2, next.getVersion());
        assertEquals(15, next.getTopicPartitionCount());
        assertIterableEquals(List.of(6, 7, 8), next.getPlugins().get("trino").getPartitions());
        assertIterableEquals(List.of(9, 10, 11, 12, 13, 14), next.getBuffer().getPartitions());
        assertIterableEquals(List.of(0, 1, 2), next.getPlugins().get("hdfs").getPartitions());
        assertIterableEquals(List.of(3, 4, 5), next.getPlugins().get("hiveServer2").getPartitions());
    }

    @Test
    public void testOnboardPluginWithMultipleServices() {
        Map<String, ServiceAllowlistEntry> services = new LinkedHashMap<>();
        services.put("dev_hive", ServiceAllowlistEntry.ofUsers("hive"));
        services.put("dev_hive2", ServiceAllowlistEntry.ofUsers("hive2"));

        PartitionPlan next = PartitionPlanAllocator.onboardPlugin(initialPlan, "trino", 3, services, "ops");

        assertEquals(2, next.getVersion());
        assertIterableEquals(List.of(6, 7, 8), next.getPlugins().get("trino").getPartitions());
        assertEquals("trino", next.getServices().get("dev_hive").getPluginId());
        assertEquals("trino", next.getServices().get("dev_hive2").getPluginId());
        assertIterableEquals(List.of("hive"), next.getServices().get("dev_hive").getAllowedUsers());
        assertIterableEquals(List.of("hive2"), next.getServices().get("dev_hive2").getAllowedUsers());
    }

    @Test
    public void testUpdatePluginAddsAndRemovesServices() {
        Map<String, ServiceAllowlistEntry> services = new LinkedHashMap<>();
        services.put("dev_hive", ServiceAllowlistEntry.ofUsers("hive"));
        services.put("dev_hive2", ServiceAllowlistEntry.ofUsers("hive2"));
        PartitionPlan onboarded = PartitionPlanAllocator.onboardPlugin(initialPlan, "trino", 3, services, "ops");

        Map<String, ServiceAllowlistEntry> addServices = Map.of("dev_hive3", ServiceAllowlistEntry.ofUsers("hive3"));
        UpdatePlugin update = new UpdatePlugin(onboarded.getVersion(), null, addServices, null, List.of("dev_hive2"));
        PartitionPlan updated = PartitionPlanAllocator.updatePlugin(onboarded, "trino", update, "ops");

        assertEquals(3, updated.getVersion());
        assertTrue(updated.getServices().containsKey("dev_hive"));
        assertTrue(updated.getServices().containsKey("dev_hive3"));
        assertFalse(updated.getServices().containsKey("dev_hive2"));
        assertEquals("trino", updated.getServices().get("dev_hive3").getPluginId());
    }

    @Test
    public void testUpdatePluginScalesViaAdditionalPartitions() {
        UpdatePlugin update = new UpdatePlugin(initialPlan.getVersion(), 3, null, null, null);
        PartitionPlan scaled = PartitionPlanAllocator.updatePlugin(initialPlan, "hiveServer2", update, "ops");

        assertEquals(2, scaled.getVersion());
        assertEquals(18, scaled.getTopicPartitionCount());
        assertIterableEquals(List.of(3, 4, 5, 15, 16, 17), scaled.getPlugins().get("hiveServer2").getPartitions());
    }

    @Test
    public void testPromotePluginGrowsTopicWhenBufferInsufficient() {
        PartitionPlan next = PartitionPlanAllocator.promotePlugin(initialPlan, "trino", 12, "ops");

        assertEquals(18, next.getTopicPartitionCount());
        assertIterableEquals(List.of(6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17), next.getPlugins().get("trino").getPartitions());
        assertEquals(0, next.getBuffer().size());
    }

    @Test
    public void testScalePluginAppendsTailOnly() {
        PartitionPlan promoted = PartitionPlanAllocator.promotePlugin(initialPlan, "trino", 3, "ops");
        PartitionPlan scaled   = PartitionPlanAllocator.scalePlugin(promoted, "hiveServer2", 3, "ops");

        assertEquals(3, scaled.getVersion());
        assertEquals(18, scaled.getTopicPartitionCount());
        assertIterableEquals(List.of(3, 4, 5, 15, 16, 17), scaled.getPlugins().get("hiveServer2").getPartitions());
        assertIterableEquals(List.of(0, 1, 2), scaled.getPlugins().get("hdfs").getPartitions());
        assertIterableEquals(List.of(6, 7, 8), scaled.getPlugins().get("trino").getPartitions());
    }

    @Test
    public void testPromoteAlreadyConfiguredPluginFails() {
        PartitionPlanException error = assertThrows(PartitionPlanException.class,
                () -> PartitionPlanAllocator.promotePlugin(initialPlan, "hdfs", 1, "ops"));
        assertTrue(error.getMessage().contains("requested 1"));
    }

    @Test
    public void testIsOnboardAlreadyAppliedWhenPluginServicesAndCountMatch() {
        Map<String, ServiceAllowlistEntry> services = Map.of("dev_trino", ServiceAllowlistEntry.ofUsers("trino"));
        PartitionPlan onboarded = PartitionPlanAllocator.onboardPlugin(initialPlan, "trino", 3, services, "ops");

        assertTrue(PartitionPlanAllocator.isOnboardAlreadyApplied(onboarded, "trino", 3, services));
        assertFalse(PartitionPlanAllocator.isOnboardAlreadyApplied(onboarded, "trino", 5, services));
    }

    @Test
    public void testPromoteConflictWhenPartitionCountDiffers() {
        PartitionPlan promoted = PartitionPlanAllocator.promotePlugin(initialPlan, "trino", 3, "ops");

        PartitionPlanException error = assertThrows(PartitionPlanException.class,
                () -> PartitionPlanAllocator.promotePlugin(promoted, "trino", 5, "ops"));

        assertTrue(error.getMessage().contains("requested 5"));
    }

    @Test
    public void testUpdatePluginRejectsRemoveForForeignService() {
        PartitionPlan withTaggedService = initialPlan.toBuilder()
                .services(Map.of("dev_hive", ServiceAllowlistEntry.ofUsers(List.of("hive"), "hdfs")))
                .build();

        UpdatePlugin update = new UpdatePlugin(withTaggedService.getVersion(), null, null, null, List.of("dev_hive"));
        PartitionPlanException error = assertThrows(PartitionPlanException.class,
                () -> PartitionPlanAllocator.updatePlugin(withTaggedService, "hiveServer2", update, "ops"));

        assertTrue(error.getMessage().contains("belongs to plugin"));
    }

    @Test
    public void testScaleUnknownPluginFails() {
        assertThrows(PartitionPlanException.class, () -> PartitionPlanAllocator.scalePlugin(initialPlan, "trino", 2, "ops"));
    }
}
