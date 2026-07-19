/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information regarding copyright ownership.
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
import org.apache.ranger.audit.producer.kafka.partition.model.OnboardService;
import org.apache.ranger.audit.producer.kafka.partition.model.PartitionPlan;
import org.apache.ranger.audit.producer.kafka.partition.model.PartitionPlanReplacement;
import org.apache.ranger.audit.producer.kafka.partition.model.PluginPartitionAssignment;
import org.apache.ranger.audit.producer.kafka.partition.model.PluginScale;
import org.apache.ranger.audit.producer.kafka.partition.model.PromotePlugin;
import org.apache.ranger.audit.producer.kafka.partition.model.ServiceAllowlistEntry;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class PartitionPlanReplacementTest {
    private static final PartitionPlan CURRENT = PartitionPlan.builder()
            .topic("ranger_audits")
            .version(1)
            .topicPartitionCount(9)
            .plugins(Map.of(
                    "hdfs", PluginPartitionAssignment.of(0, 1, 2),
                    "hiveServer2", PluginPartitionAssignment.of(3, 4, 5)))
            .buffer(PluginPartitionAssignment.of(6, 7, 8))
            .services(Map.of("dev_hdfs", ServiceAllowlistEntry.ofUsers("hdfs")))
            .build();

    @Test
    public void testMergeInheritsEmptyOptionalFields() {
        PartitionPlanReplacement update = new PartitionPlanReplacement(1, null, null, null, null, null);

        PartitionPlan merged = update.toMergedPlan(CURRENT, "ops");

        assertEquals(9, merged.getTopicPartitionCount());
        assertEquals(CURRENT.getPlugins(), merged.getPlugins());
        assertEquals(CURRENT.getBuffer(), merged.getBuffer());
        assertEquals(CURRENT.getServices(), merged.getServices());
    }

    @Test
    public void testMergeAddsOnlyNewPluginAndServiceEntries() {
        PartitionPlanReplacement update = new PartitionPlanReplacement(
                1,
                null,
                Map.of("trino", PluginPartitionAssignment.of(6, 7, 8)),
                null,
                Map.of("dev_trino", ServiceAllowlistEntry.ofUsers("trino")),
                null);

        PartitionPlan merged = update.toMergedPlan(CURRENT, "ops");

        assertTrue(merged.getPlugins().containsKey("trino"));
        assertTrue(merged.getServices().containsKey("dev_trino"));
        assertTrue(merged.getPlugins().containsKey("hdfs"));
        assertTrue(merged.getServices().containsKey("dev_hdfs"));
    }

    @Test
    public void testHasMergeDeltaRequiresNonEmptyField() {
        assertFalse(new PartitionPlanReplacement(1, null, null, null, null, null).hasMergeDelta());
        assertTrue(new PartitionPlanReplacement(1, 12, null, null, null).hasMergeDelta());
        assertTrue(new PartitionPlanReplacement(1, null, Map.of("trino", PluginPartitionAssignment.of(6)), null, null, null).hasMergeDelta());
        assertTrue(new PartitionPlanReplacement(1, null, null, null, null, Map.of("hive", 1)).hasMergeDelta());
    }

    @Test
    public void testMergeUpsertsExistingServiceAllowlist() {
        PartitionPlanReplacement update = new PartitionPlanReplacement(
                1,
                null,
                null,
                null,
                Map.of("dev_hdfs", ServiceAllowlistEntry.ofUsers("hdfs", "backup")),
                null);

        PartitionPlan merged = update.toMergedPlan(CURRENT, "ops");

        assertIterableEquals(List.of("hdfs", "backup"), merged.getServices().get("dev_hdfs").getAllowedUsers());
    }

    @Test
    public void testMergeExistingPluginWithSameAssignmentIsNoOpDelta() {
        PartitionPlanReplacement update = new PartitionPlanReplacement(
                1,
                null,
                Map.of("hdfs", PluginPartitionAssignment.of(0, 1, 2)),
                null,
                null,
                null);

        PartitionPlan merged = update.toMergedPlan(CURRENT, "ops");

        assertTrue(CURRENT.sameContentAs(merged));
    }

    @Test
    public void testMergeExistingPluginWithDifferentAssignmentFails() {
        PartitionPlanReplacement update = new PartitionPlanReplacement(
                1,
                null,
                Map.of("hdfs", PluginPartitionAssignment.of(0, 1)),
                null,
                null,
                null);

        PartitionPlanException error = assertThrows(PartitionPlanException.class, () -> update.toMergedPlan(CURRENT, "ops"));

        assertTrue(error.getMessage().contains("different partition assignment"));
    }

    @Test
    public void testSameContentAsIgnoresVersionAndMetadata() {
        PartitionPlan withMetadata = CURRENT.toBuilder().version(99).updatedAt("later").updatedBy("other").build();

        assertTrue(CURRENT.sameContentAs(withMetadata));
    }

    @Test
    public void testRequestValidatorRejectsBlankPromotePluginId() {
        assertThrows(PartitionPlanException.class,
                () -> PartitionPlanRequestValidator.validatePromotePlugin(new PromotePlugin("", 2, 1)));
    }

    @Test
    public void testRequestValidatorRejectsOnboardServiceWithoutAllowedUsers() {
        assertThrows(PartitionPlanException.class,
                () -> PartitionPlanRequestValidator.validateOnboardService(
                        new OnboardService("dev_trino", "trino", 2, List.of("  "), 1)));
    }

    @Test
    public void testRequestValidatorRejectsScaleWithZeroAdditionalPartitions() {
        assertThrows(PartitionPlanException.class,
                () -> PartitionPlanRequestValidator.validateScalePlugin("hiveServer2", new PluginScale(0, 1)));
    }
}
