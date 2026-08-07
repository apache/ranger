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

package org.apache.ranger.audit.producer.kafka.partition.model;

import org.apache.ranger.audit.producer.kafka.partition.PartitionPlanBootstrap;
import org.apache.ranger.audit.producer.kafka.partition.PartitionPlanBootstrapConfig;
import org.apache.ranger.audit.producer.kafka.partition.exception.PartitionPlanException;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class PartitionPlanJsonTest {
    @Test
    public void testRoundTripPreservesPlan() {
        PartitionPlan plan = PartitionPlanBootstrap.createInitialPlan(PartitionPlanBootstrapConfig.create("ranger_audits", new String[] {"hdfs", "hiveServer2"}, 3, 9));
        PartitionPlan parsed = PartitionPlan.fromJson(plan.toJson());

        assertEquals(plan.getTopic(), parsed.getTopic());
        assertEquals(plan.getVersion(), parsed.getVersion());
        assertEquals(plan.getTopicPartitionCount(), parsed.getTopicPartitionCount());
        assertIterableEquals(List.of(0, 1, 2), parsed.getPlugins().get("hdfs").getPartitions());
        assertIterableEquals(List.of(6, 7, 8, 9, 10, 11, 12, 13, 14), parsed.getBuffer().getPartitions());
    }

    @Test
    public void testRoundTripPreservesServices() {
        PartitionPlan plan = PartitionPlan.builder()
                .topic("ranger_audits")
                .version(2)
                .topicPartitionCount(6)
                .plugins(Map.of("hdfs", PluginPartitionAssignment.of(0, 1, 2)))
                .buffer(PluginPartitionAssignment.of(3, 4, 5))
                .services(Map.of("dev_hive", ServiceAllowlistEntry.ofUsers("hive")))
                .build();
        PartitionPlan parsed = PartitionPlan.fromJson(plan.toJson());

        assertIterableEquals(List.of("hive"), parsed.getServices().get("dev_hive").getAllowedUsers());
    }

    @Test
    public void testFromJsonRejectsInvalidPlan() {
        assertThrows(PartitionPlanException.class, () -> PartitionPlan.fromJson("{\"topic\":\"ranger_audits\",\"version\":1}"));
    }
}
