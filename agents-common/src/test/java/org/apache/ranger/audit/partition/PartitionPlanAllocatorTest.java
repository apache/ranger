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

package org.apache.ranger.audit.partition;

import org.apache.ranger.audit.partition.exception.PartitionPlanException;
import org.apache.ranger.audit.partition.model.PartitionPlan;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

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
        initialPlan = PartitionPlanTestSupport.preAssignedPlan();
    }

    @Test
    public void testPromotePluginFromBuffer() {
        PartitionPlan next = PartitionPlanAllocator.promotePlugin(initialPlan, "trino", 3, "ops", null);

        assertEquals(2, next.getVersion());
        assertEquals(9, next.getTopicPartitionCount());
        assertIterableEquals(List.of(7, 8, 9), next.getPlugins().get("trino").getPartitions());
        assertIterableEquals(List.of(), next.getBuffer().getPartitions());
        assertIterableEquals(List.of(1, 2, 3), next.getPlugins().get("hdfs").getPartitions());
        assertIterableEquals(List.of(4, 5, 6), next.getPlugins().get("hiveServer2").getPartitions());
    }

    @Test
    public void testPromotePluginGrowsTopicWhenBufferInsufficient() {
        PartitionPlan seed = PartitionPlanTestSupport.seedPlan();
        PartitionPlan next = PartitionPlanAllocator.promotePlugin(seed, "trino", 12, "ops", null);

        assertEquals(2, next.getVersion());
        assertEquals(12, next.getTopicPartitionCount());
        assertIterableEquals(List.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12), next.getPlugins().get("trino").getPartitions());
        assertIterableEquals(List.of(), next.getBuffer().getPartitions());
    }

    @Test
    public void testOnboardServicePromotesPluginAndAddsService() {
        PartitionPlan seed = PartitionPlanTestSupport.seedPlan();
        PartitionPlan next = PartitionPlanAllocator.onboardService(seed, "hiveServer2", "dev_hive", 6, "admin");

        assertEquals(2, next.getVersion());
        assertEquals(9, next.getTopicPartitionCount());
        assertIterableEquals(List.of(1, 2, 3, 4, 5, 6), next.getPlugins().get("hiveServer2").getPartitions());
        assertIterableEquals(List.of("dev_hive"), next.getPlugins().get("hiveServer2").getServices());
        assertIterableEquals(List.of(7, 8, 9), next.getBuffer().getPartitions());
    }

    @Test
    public void testOnboardServiceAddsToExistingPlugin() {
        PartitionPlan promoted = PartitionPlanAllocator.onboardService(PartitionPlanTestSupport.seedPlan(), "hiveServer2", "dev_hive", 6, "admin");
        PartitionPlan next     = PartitionPlanAllocator.onboardService(promoted, "hiveServer2", "prod_hive", 6, "admin");

        assertEquals(3, next.getVersion());
        assertIterableEquals(List.of("dev_hive", "prod_hive"), next.getPlugins().get("hiveServer2").getServices());
        assertIterableEquals(List.of(1, 2, 3, 4, 5, 6), next.getPlugins().get("hiveServer2").getPartitions());
    }

    @Test
    public void testRemoveService() {
        PartitionPlan onboarded = PartitionPlanAllocator.onboardService(PartitionPlanTestSupport.seedPlan(), "hiveServer2", "dev_hive", 6, "admin");
        PartitionPlan next      = PartitionPlanAllocator.removeService(onboarded, "dev_hive", "admin");

        assertEquals(3, next.getVersion());
        assertIterableEquals(List.of(), next.getPlugins().get("hiveServer2").getServices());
    }

    @Test
    public void testPromoteAlreadyConfiguredPluginFails() {
        PartitionPlanException error = assertThrows(PartitionPlanException.class,
                () -> PartitionPlanAllocator.promotePlugin(initialPlan, "hdfs", 1, "ops", null));
        assertTrue(error.getMessage().contains("requested 1"));
    }

    @Test
    public void testIsOnboardAlreadyAppliedWhenServiceAndPluginMatch() {
        PartitionPlan onboarded = PartitionPlanAllocator.onboardService(PartitionPlanTestSupport.seedPlan(), "hiveServer2", "dev_hive", 6, "admin");

        assertTrue(PartitionPlanAllocator.isOnboardAlreadyApplied(onboarded, "hiveServer2", "dev_hive", 6));
        assertFalse(PartitionPlanAllocator.isOnboardAlreadyApplied(onboarded, "hiveServer2", "prod_hive", 6));
    }

    @Test
    public void testPromoteConflictWhenPartitionCountDiffers() {
        PartitionPlan promoted = PartitionPlanAllocator.promotePlugin(initialPlan, "trino", 3, "ops", null);

        PartitionPlanException error = assertThrows(PartitionPlanException.class,
                () -> PartitionPlanAllocator.promotePlugin(promoted, "trino", 5, "ops", null));

        assertTrue(error.getMessage().contains("requested 5"));
    }

    @Test
    public void testUpdateServiceAllowedUsersBumpsVersionWhenMapChanges() {
        PartitionPlan next = PartitionPlanAllocator.updateServiceAllowedUsers(
                initialPlan, Map.of("dev_hive", List.of("hive")), "admin");

        assertEquals(2, next.getVersion());
        assertIterableEquals(List.of("hive"), next.getServiceAllowedUsers().get("dev_hive"));
    }

    @Test
    public void testUpdateServiceAllowedUsersIsNoOpWhenUnchanged() {
        PartitionPlan withUsers = PartitionPlanAllocator.updateServiceAllowedUsers(
                initialPlan, Map.of("dev_hive", List.of("hive")), "admin");
        PartitionPlan unchanged = PartitionPlanAllocator.updateServiceAllowedUsers(
                withUsers, Map.of("dev_hive", List.of("hive")), "admin");

        assertEquals(withUsers, unchanged);
    }
}
