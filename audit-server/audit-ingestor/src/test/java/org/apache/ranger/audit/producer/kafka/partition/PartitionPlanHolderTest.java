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
import org.apache.ranger.audit.producer.kafka.partition.model.ServiceAllowlistEntry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class PartitionPlanHolderTest {
    private static final int TOPIC_PARTITIONS = 6;

    @AfterEach
    public void tearDown() {
        PartitionPlanHolder.getInstance().resetForTests();
    }

    @Test
    public void testGetAllowedUsersReturnsNullWhenNoPlanInstalled() {
        assertNull(PartitionPlanHolder.getInstance().getAllowedUsersForService("dev_hdfs"));
    }

    @Test
    public void testGetAllowedUsersReturnsNullWhenServicesBlockMissing() {
        PartitionPlan plan = basePlanBuilder()
                .build();
        PartitionPlanHolder.getInstance().install(plan, TOPIC_PARTITIONS);

        assertNull(PartitionPlanHolder.getInstance().getAllowedUsersForService("dev_hdfs"));
        assertNull(PartitionPlanHolder.getInstance().getAllowedUsersForService("dev_hive"));
    }

    @Test
    public void testGetAllowedUsersReturnsNullForRepoNotInPartialPlan() {
        Map<String, ServiceAllowlistEntry> services = new LinkedHashMap<>();
        services.put("dev_hdfs", ServiceAllowlistEntry.ofUsers("hdfs", "nn"));
        PartitionPlan plan = basePlanBuilder().services(services).build();
        PartitionPlanHolder.getInstance().install(plan, TOPIC_PARTITIONS);

        assertIterableEquals(List.of("hdfs", "nn"), PartitionPlanHolder.getInstance().getAllowedUsersForService("dev_hdfs"));
        assertNull(PartitionPlanHolder.getInstance().getAllowedUsersForService("dev_hive"));
        assertNull(PartitionPlanHolder.getInstance().getAllowedUsersForService("dev_trino"));
    }

    @Test
    public void testGetAllowedUsersReturnsRegistryUsersForOnboardedRepo() {
        Map<String, ServiceAllowlistEntry> services = Map.of("dev_hive", ServiceAllowlistEntry.ofUsers("hive"));
        PartitionPlan plan = basePlanBuilder().services(services).build();
        PartitionPlanHolder.getInstance().install(plan, TOPIC_PARTITIONS);

        Set<String> allowed = PartitionPlanHolder.getInstance().getAllowedUsersForService("dev_hive");

        assertIterableEquals(List.of("hive"), allowed);
        assertTrue(allowed != null && !allowed.contains("hdfs"));
    }

    @Test
    public void testInstallRejectsEmptyServiceAllowlist() {
        Map<String, ServiceAllowlistEntry> services = Map.of("dev_hdfs", new ServiceAllowlistEntry(List.of(), "test", null));
        PartitionPlan plan = basePlanBuilder().services(services).build();

        assertThrows(PartitionPlanException.class, () -> PartitionPlanHolder.getInstance().install(plan, TOPIC_PARTITIONS));
    }

    private static PartitionPlan.Builder basePlanBuilder() {
        return PartitionPlan.builder()
                .topic("ranger_audits")
                .version(1)
                .topicPartitionCount(TOPIC_PARTITIONS)
                .plugins(Map.of("hdfs", PluginPartitionAssignment.of(0, 1, 2)))
                .buffer(PluginPartitionAssignment.of(3, 4, 5));
    }
}
