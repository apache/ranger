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
import org.apache.ranger.audit.producer.kafka.partition.model.PluginPartitionAssignment;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class PartitionPlanValidatorTest {
    @Test
    public void testValidateAcceptsWellFormedPlan() {
        PartitionPlan plan = PartitionPlanBootstrap.createInitialPlan(PartitionPlanBootstrapConfig.create("ranger_audits", new String[] {"hdfs"}, 2, 2));
        assertDoesNotThrow(() -> PartitionPlanValidator.validate(plan, 4));
    }

    @Test
    public void testValidateRejectsDuplicatePartitionIds() {
        Map<String, PluginPartitionAssignment> plugins = new LinkedHashMap<>();
        plugins.put("hdfs", PluginPartitionAssignment.of(0, 1));
        PartitionPlan plan = PartitionPlan.builder().topic("ranger_audits").version(1).topicPartitionCount(3).plugins(plugins).buffer(PluginPartitionAssignment.of(1, 2)).build();
        assertThrows(PartitionPlanException.class, () -> PartitionPlanValidator.validate(plan));
    }

    @Test
    public void testValidateRejectsKafkaPartitionMismatch() {
        PartitionPlan plan = PartitionPlanBootstrap.createInitialPlan(PartitionPlanBootstrapConfig.create("ranger_audits", new String[] {"hdfs"}, 2, 2));
        assertThrows(PartitionPlanException.class, () -> PartitionPlanValidator.validate(plan, 10));
    }

    @Test
    public void testValidateAppendOnlyRejectsReshuffle() {
        PartitionPlan current = PartitionPlan.builder().topic("ranger_audits").version(1).topicPartitionCount(6).putPlugin("hdfs", PluginPartitionAssignment.of(0, 1, 2)).putPlugin("hiveServer2", PluginPartitionAssignment.of(3, 4, 5)).buffer(PluginPartitionAssignment.empty()).build();

        Map<String, PluginPartitionAssignment> reshuffled = new LinkedHashMap<>();
        reshuffled.put("hdfs", PluginPartitionAssignment.of(0, 1, 2, 3));
        reshuffled.put("hiveServer2", PluginPartitionAssignment.of(4, 5));
        PartitionPlan proposed = PartitionPlan.builder().topic("ranger_audits").version(2).topicPartitionCount(6).plugins(reshuffled).buffer(PluginPartitionAssignment.empty()).build();

        assertThrows(PartitionPlanException.class, () -> PartitionPlanValidator.validateAppendOnly(current, proposed));
    }

    @Test
    public void testValidateAppendOnlyAcceptsTailGrowth() {
        PartitionPlan current = PartitionPlan.builder().topic("ranger_audits").version(1).topicPartitionCount(6).putPlugin("hdfs", PluginPartitionAssignment.of(0, 1, 2)).putPlugin("hiveServer2", PluginPartitionAssignment.of(3, 4, 5)).buffer(PluginPartitionAssignment.empty()).build();
        PartitionPlan proposed = PartitionPlan.builder().topic("ranger_audits").version(2).topicPartitionCount(9).putPlugin("hdfs", PluginPartitionAssignment.of(0, 1, 2)).putPlugin("hiveServer2", PluginPartitionAssignment.of(3, 4, 5, 6, 7, 8)).buffer(PluginPartitionAssignment.empty()).build();
        assertDoesNotThrow(() -> PartitionPlanValidator.validateAppendOnly(current, proposed));
    }
}
