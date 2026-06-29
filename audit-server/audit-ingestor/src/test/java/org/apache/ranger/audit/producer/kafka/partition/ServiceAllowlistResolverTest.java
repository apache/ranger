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
import org.apache.ranger.audit.producer.kafka.partition.model.PluginPartitionAssignment;
import org.apache.ranger.audit.producer.kafka.partition.model.ServiceAllowlistEntry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ServiceAllowlistResolverTest {
    private static final Map<String, Set<String>> STATIC = Map.of(
            "dev_hive", Set.of("hive"),
            "dev_hdfs", Set.of("hdfs", "nn"));

    @AfterEach
    public void tearDown() {
        PartitionPlanHolder.getInstance().resetForTests();
    }

    @Test
    public void testRejectsBlankServiceOrUser() {
        assertFalse(ServiceAllowlistResolver.isAllowedServiceUser("", "hive", true, PartitionPlanHolder.getInstance(), STATIC));
        assertFalse(ServiceAllowlistResolver.isAllowedServiceUser("dev_hive", "", true, PartitionPlanHolder.getInstance(), STATIC));
        assertFalse(ServiceAllowlistResolver.isAllowedServiceUser(null, "hive", true, PartitionPlanHolder.getInstance(), STATIC));
    }

    @Test
    public void testUsesStaticXmlWhenDynamicDisabled() {
        assertTrue(ServiceAllowlistResolver.isAllowedServiceUser("dev_hive", "hive", false, PartitionPlanHolder.getInstance(), STATIC));
        assertFalse(ServiceAllowlistResolver.isAllowedServiceUser("dev_hive", "hdfs", false, PartitionPlanHolder.getInstance(), STATIC));
    }

    @Test
    public void testUsesRegistryWhenRepoOnboarded() {
        installPartialPlan("dev_hdfs", "hdfs");

        assertTrue(ServiceAllowlistResolver.isAllowedServiceUser("dev_hdfs", "hdfs", true, PartitionPlanHolder.getInstance(), STATIC));
        assertFalse(ServiceAllowlistResolver.isAllowedServiceUser("dev_hdfs", "nn", true, PartitionPlanHolder.getInstance(), STATIC));
    }

    @Test
    public void testFallsBackToStaticXmlWhenRepoNotInRegistry() {
        installPartialPlan("dev_hdfs", "hdfs");

        assertTrue(ServiceAllowlistResolver.isAllowedServiceUser("dev_hive", "hive", true, PartitionPlanHolder.getInstance(), STATIC));
        assertFalse(ServiceAllowlistResolver.isAllowedServiceUser("dev_trino", "trino", true, PartitionPlanHolder.getInstance(), STATIC));
    }

    @Test
    public void testDeniesWhenUserNotInRegistryAllowlist() {
        Map<String, ServiceAllowlistEntry> services = Map.of("dev_hdfs", ServiceAllowlistEntry.ofUsers("hdfs"));
        PartitionPlan plan = basePlanBuilder().services(services).build();
        PartitionPlanHolder holder = PartitionPlanHolder.getInstance();
        holder.install(plan, 6);

        assertFalse(ServiceAllowlistResolver.isAllowedServiceUser("dev_hdfs", "nn", true, holder, STATIC));
    }

    private static void installPartialPlan(String repo, String user) {
        Map<String, ServiceAllowlistEntry> services = new LinkedHashMap<>();
        services.put(repo, ServiceAllowlistEntry.ofUsers(user));
        PartitionPlanHolder.getInstance().install(basePlanBuilder().services(services).build(), 6);
    }

    private static PartitionPlan.Builder basePlanBuilder() {
        return PartitionPlan.builder()
                .topic("ranger_audits")
                .version(1)
                .topicPartitionCount(6)
                .plugins(Map.of("hdfs", PluginPartitionAssignment.of(0, 1, 2)))
                .buffer(PluginPartitionAssignment.of(3, 4, 5));
    }
}
