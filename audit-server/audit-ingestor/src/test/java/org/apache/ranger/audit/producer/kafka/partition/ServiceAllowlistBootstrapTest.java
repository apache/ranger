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
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.apache.ranger.audit.server.AuditServerConstants.PROP_PREFIX_AUDIT_SERVER_SERVICE;
import static org.apache.ranger.audit.server.AuditServerConstants.PROP_SUFFIX_ALLOWED_USERS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ServiceAllowlistBootstrapTest {
    @AfterEach
    public void tearDown() {
        PartitionPlanHolder.getInstance().resetForTests();
    }

    @Test
    public void testLoadFromPropertiesParsesServiceRepos() {
        Properties props = new Properties();
        props.setProperty(PROP_PREFIX_AUDIT_SERVER_SERVICE + "dev_hive" + PROP_SUFFIX_ALLOWED_USERS, "hive");
        props.setProperty(PROP_PREFIX_AUDIT_SERVER_SERVICE + "dev_ozone" + PROP_SUFFIX_ALLOWED_USERS, "om, ozone");

        Map<String, ServiceAllowlistEntry> services = ServiceAllowlistBootstrap.loadAllowlistsFromProperties(props);

        assertEquals(2, services.size());
        assertIterableEquals(List.of("hive"), services.get("dev_hive").getAllowedUsers());
        assertIterableEquals(List.of("om", "ozone"), services.get("dev_ozone").getAllowedUsers());
    }

    @Test
    public void testEnrichServicesFromXmlIfMissingMergesInMemoryOnly() {
        PartitionPlan plan = PartitionPlan.builder()
                .topic("ranger_audits")
                .version(3)
                .topicPartitionCount(6)
                .plugins(Map.of("hdfs", PluginPartitionAssignment.of(0, 1, 2)))
                .buffer(PluginPartitionAssignment.of(3, 4, 5))
                .build();

        Properties props = new Properties();
        props.setProperty(PROP_PREFIX_AUDIT_SERVER_SERVICE + "dev_hive" + PROP_SUFFIX_ALLOWED_USERS, "hive");

        PartitionPlan enriched = ServiceAllowlistBootstrap.mergeSiteXmlAllowlistsWhenPlanServicesMissing(plan, props);

        assertEquals(3, enriched.getVersion());
        assertNotNull(enriched.getServices().get("dev_hive"));
        PartitionPlanHolder.getInstance().install(enriched, 6);
        assertIterableEquals(List.of("hive"), PartitionPlanHolder.getInstance().getAllowedUsersForService("dev_hive"));
    }

    @Test
    public void testMergeSkippedWhenPlanAlreadyHasPartialServices() {
        Map<String, ServiceAllowlistEntry> services = new LinkedHashMap<>();
        services.put("dev_hdfs", ServiceAllowlistEntry.ofUsers("hdfs"));
        PartitionPlan plan = PartitionPlan.builder()
                .topic("ranger_audits")
                .version(2)
                .topicPartitionCount(6)
                .plugins(Map.of("hdfs", PluginPartitionAssignment.of(0, 1, 2)))
                .buffer(PluginPartitionAssignment.of(3, 4, 5))
                .services(services)
                .build();

        Properties props = new Properties();
        props.setProperty(PROP_PREFIX_AUDIT_SERVER_SERVICE + "dev_hive" + PROP_SUFFIX_ALLOWED_USERS, "hive");

        PartitionPlan merged = ServiceAllowlistBootstrap.mergeSiteXmlAllowlistsWhenPlanServicesMissing(plan, props);

        assertEquals(plan, merged);
        assertEquals(1, merged.getServices().size());
        assertNotNull(merged.getServices().get("dev_hdfs"));
        assertTrue(merged.getServices().get("dev_hive") == null);
    }
}
