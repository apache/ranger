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

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.ranger.audit.producer.kafka.partition.model.PartitionPlan;
import org.apache.ranger.audit.producer.kafka.partition.model.PluginPartitionAssignment;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class PartitionPlanUpdateApplierTest {
    private static final String AUDIT_TOPIC = "ranger_audits";
    private static final String PLAN_TOPIC  = "ranger_audit_partition_plan";
    private static final int KAFKA_PARTITIONS = 6;

    private PartitionPlanHolder holder;
    private PartitionPlanUpdateApplier applier;

    @BeforeEach
    public void setUp() {
        holder = PartitionPlanHolder.getInstance();
        holder.resetForTests();
        holder.install(planWithVersion(1), KAFKA_PARTITIONS);
        applier = new PartitionPlanUpdateApplier(new Properties(), AUDIT_TOPIC, holder, () -> KAFKA_PARTITIONS);
    }

    @AfterEach
    public void tearDown() {
        holder.resetForTests();
    }

    @Test
    public void testIgnoresWrongRecordKey() {
        applier.applyRecordIfNewer(record("other-topic", planWithVersion(2).toJson()));

        assertEquals(1, holder.getLastInstalledVersion());
    }

    @Test
    public void testIgnoresSameVersion() {
        applier.applyRecordIfNewer(record(AUDIT_TOPIC, planWithVersion(1).toJson()));

        assertEquals(1, holder.getLastInstalledVersion());
    }

    @Test
    public void testIgnoresOlderVersion() {
        applier.applyRecordIfNewer(record(AUDIT_TOPIC, planWithVersion(0).toJson()));

        assertEquals(1, holder.getLastInstalledVersion());
    }

    @Test
    public void testInstallsNewerVersion() {
        applier.applyRecordIfNewer(record(AUDIT_TOPIC, planWithVersion(2).toJson()));

        assertEquals(2, holder.getLastInstalledVersion());
        assertEquals(2, holder.getPlan().getVersion());
    }

    @Test
    public void testIgnoresInvalidJson() {
        applier.applyRecordIfNewer(record(AUDIT_TOPIC, "{not-json"));

        assertEquals(1, holder.getLastInstalledVersion());
    }

    @Test
    public void testStartupSyncCanCatchPlanPublishedBeforeWatcherThread() {
        applier.applyRecordIfNewer(record(AUDIT_TOPIC, planWithVersion(2).toJson()));
        applier.applyRecordIfNewer(record(AUDIT_TOPIC, planWithVersion(3).toJson()));

        assertEquals(3, holder.getLastInstalledVersion());
    }

    private static PartitionPlan planWithVersion(int version) {
        return PartitionPlan.builder()
                .topic(AUDIT_TOPIC)
                .version(version)
                .topicPartitionCount(KAFKA_PARTITIONS)
                .plugins(Map.of("hdfs", PluginPartitionAssignment.of(0, 1, 2)))
                .buffer(PluginPartitionAssignment.of(3, 4, 5))
                .build();
    }

    private static ConsumerRecord<String, String> record(String key, String value) {
        return new ConsumerRecord<>(PLAN_TOPIC, 0, 0L, key, value);
    }
}
