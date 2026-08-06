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
import org.apache.ranger.audit.partition.model.BufferEntry;
import org.apache.ranger.audit.partition.model.PartitionPlan;
import org.apache.ranger.audit.partition.model.PluginEntry;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class PartitionPlanValidatorTest {
    @Test
    public void testValidateAcceptsSeedPlan() {
        PartitionPlan plan = PartitionPlanTestSupport.seedPlan();
        assertDoesNotThrow(() -> PartitionPlanValidator.validate(plan, 9));
    }

    @Test
    public void testValidateAcceptsPreAssignedPlan() {
        PartitionPlan plan = PartitionPlanTestSupport.preAssignedPlan();
        assertDoesNotThrow(() -> PartitionPlanValidator.validate(plan));
    }

    @Test
    public void testValidateRejectsDuplicatePartitionIds() {
        Map<String, PluginEntry> plugins = new LinkedHashMap<>();
        plugins.put("hdfs", PluginEntry.ofPartitions(1, 2));
        PartitionPlan plan = PartitionPlan.builder()
                .topic("ranger_audits")
                .version(1)
                .topicPartitionCount(3)
                .plugins(plugins)
                .buffer(new BufferEntry(java.util.List.of(2, 3)))
                .build();
        assertThrows(PartitionPlanException.class, () -> PartitionPlanValidator.validate(plan));
    }

    @Test
    public void testValidateRejectsUnionSizeMismatch() {
        PartitionPlan plan = PartitionPlan.builder()
                .topic("ranger_audits")
                .version(1)
                .topicPartitionCount(10)
                .buffer(new BufferEntry(java.util.List.of(1, 2, 3, 4, 5, 6, 7, 8, 9)))
                .build();
        assertThrows(PartitionPlanException.class, () -> PartitionPlanValidator.validate(plan));
    }

    @Test
    public void testValidateRejectsZeroBasedPartitionId() {
        PartitionPlan plan = PartitionPlan.builder()
                .topic("ranger_audits")
                .version(1)
                .topicPartitionCount(1)
                .buffer(new BufferEntry(java.util.List.of(0)))
                .build();
        assertThrows(PartitionPlanException.class, () -> PartitionPlanValidator.validate(plan));
    }

    @Test
    public void testValidateRejectsDuplicateServiceAssignment() {
        Map<String, PluginEntry> plugins = new LinkedHashMap<>();
        plugins.put("hdfs", new PluginEntry(java.util.List.of(1, 2, 3), java.util.List.of("dev_hdfs")));
        plugins.put("hiveServer2", new PluginEntry(java.util.List.of(4, 5, 6), java.util.List.of("dev_hdfs")));
        PartitionPlan plan = PartitionPlan.builder()
                .topic("ranger_audits")
                .version(1)
                .topicPartitionCount(9)
                .plugins(plugins)
                .buffer(new BufferEntry(java.util.List.of(7, 8, 9)))
                .build();
        assertThrows(PartitionPlanException.class, () -> PartitionPlanValidator.validate(plan));
    }

    @Test
    public void testValidateRejectsKafkaPartitionCountBelowPlan() {
        PartitionPlan plan = PartitionPlanTestSupport.seedPlan();
        assertThrows(PartitionPlanException.class, () -> PartitionPlanValidator.validate(plan, 5));
    }

    @Test
    public void testValidateAcceptsKafkaPartitionCountAbovePlan() {
        PartitionPlan plan = PartitionPlanTestSupport.seedPlan();
        assertDoesNotThrow(() -> PartitionPlanValidator.validate(plan, 30));
    }

    @Test
    public void testValidateAppendOnlyRejectsReshuffle() {
        PartitionPlan current = PartitionPlanTestSupport.preAssignedPlan();
        Map<String, PluginEntry> reshuffled = new LinkedHashMap<>();
        reshuffled.put("hdfs", PluginEntry.ofPartitions(1, 2, 3, 4));
        reshuffled.put("hiveServer2", PluginEntry.ofPartitions(5, 6));
        PartitionPlan proposed = PartitionPlan.builder()
                .topic("ranger_audits")
                .version(2)
                .topicPartitionCount(9)
                .plugins(reshuffled)
                .buffer(new BufferEntry(java.util.List.of(7, 8, 9)))
                .build();
        assertThrows(PartitionPlanException.class, () -> PartitionPlanValidator.validateAppendOnly(current, proposed));
    }

    @Test
    public void testValidateAppendOnlyAcceptsTailGrowth() {
        PartitionPlan current = PartitionPlanTestSupport.preAssignedPlan();
        Map<String, PluginEntry> grown = new LinkedHashMap<>();
        grown.put("hdfs", PluginEntry.ofPartitions(1, 2, 3));
        grown.put("hiveServer2", PluginEntry.ofPartitions(4, 5, 6, 10, 11, 12));
        PartitionPlan proposed = PartitionPlan.builder()
                .topic("ranger_audits")
                .version(2)
                .topicPartitionCount(12)
                .plugins(grown)
                .buffer(new BufferEntry(java.util.List.of(7, 8, 9)))
                .build();
        assertDoesNotThrow(() -> PartitionPlanValidator.validateAppendOnly(current, proposed));
    }

    @Test
    public void testValidateRejectsEmptyServiceAllowedUsers() {
        Map<String, java.util.List<String>> allowlists = new LinkedHashMap<>();
        allowlists.put("dev_hive", java.util.Collections.emptyList());
        PartitionPlan plan = PartitionPlan.builder()
                .topic("ranger_audits")
                .version(1)
                .topicPartitionCount(9)
                .plugins(PartitionPlanTestSupport.preAssignedPlan().getPlugins())
                .buffer(new BufferEntry(java.util.List.of(7, 8, 9)))
                .serviceAllowedUsers(allowlists)
                .build();
        assertThrows(PartitionPlanException.class, () -> PartitionPlanValidator.validate(plan));
    }
}
