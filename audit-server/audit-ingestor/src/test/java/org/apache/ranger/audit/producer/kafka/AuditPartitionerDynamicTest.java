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

package org.apache.ranger.audit.producer.kafka;

import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.ranger.audit.producer.kafka.partition.PartitionPlanBootstrap;
import org.apache.ranger.audit.producer.kafka.partition.PartitionPlanBootstrapConfig;
import org.apache.ranger.audit.producer.kafka.partition.PartitionPlanHolder;
import org.apache.ranger.audit.producer.kafka.partition.model.PartitionPlan;
import org.apache.ranger.audit.producer.kafka.partition.model.PluginPartitionAssignment;
import org.apache.ranger.audit.server.AuditServerConstants;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class AuditPartitionerDynamicTest {
    private static final String TOPIC = "ranger_audits";
    private static final Node BROKER  = new Node(0, "localhost", 9092);

    @AfterEach
    public void tearDown() {
        PartitionPlanHolder.getInstance().resetForTests();
    }

    @Test
    public void testStaticModeUnchangedWhenDynamicDisabled() {
        AuditPartitioner partitioner = new AuditPartitioner();
        partitioner.configure(staticProducerConfig());

        assertEquals(0, partitioner.partition(TOPIC, "hdfs", null, null, null, cluster(15)));
        assertEquals(1, partitioner.partition(TOPIC, "hdfs", null, null, null, cluster(15)));
        assertEquals(2, partitioner.partition(TOPIC, "hdfs", null, null, null, cluster(15)));
        assertEquals(0, partitioner.partition(TOPIC, "hdfs", null, null, null, cluster(15)));
        assertTrue(partitioner.partition(TOPIC, "unknownPlugin", null, null, null, cluster(15)) >= 6);
    }

    @Test
    public void testDynamicModeRoutesConfiguredPluginRoundRobin() {
        PartitionPlan plan = PartitionPlanBootstrap.createInitialPlan(PartitionPlanBootstrapConfig.create(TOPIC, new String[] {"hdfs", "hiveServer2"}, 3, 9));
        PartitionPlanHolder.getInstance().install(plan, 15);

        AuditPartitioner partitioner = new AuditPartitioner();
        partitioner.configure(dynamicProducerConfig());

        assertEquals(0, partitioner.partition(TOPIC, "hdfs", null, null, null, cluster(15)));
        assertEquals(1, partitioner.partition(TOPIC, "hdfs", null, null, null, cluster(15)));
        assertEquals(2, partitioner.partition(TOPIC, "hdfs", null, null, null, cluster(15)));
        assertEquals(0, partitioner.partition(TOPIC, "hdfs", null, null, null, cluster(15)));
        assertIterableEquals(List.of(3, 4, 5), List.of(
                partitioner.partition(TOPIC, "hiveServer2", null, null, null, cluster(15)),
                partitioner.partition(TOPIC, "hiveServer2", null, null, null, cluster(15)),
                partitioner.partition(TOPIC, "hiveServer2", null, null, null, cluster(15))));
    }

    @Test
    public void testDynamicModeRoutesUnknownPluginToBuffer() {
        PartitionPlan plan = PartitionPlanBootstrap.createInitialPlan(PartitionPlanBootstrapConfig.create(TOPIC, new String[] {"hdfs"}, 3, 3));
        PartitionPlanHolder.getInstance().install(plan, 6);

        AuditPartitioner partitioner = new AuditPartitioner();
        partitioner.configure(dynamicProducerConfig());

        int partition = partitioner.partition(TOPIC, "trino", null, null, null, cluster(6));
        assertTrue(partition >= 3 && partition <= 5);
    }

    @Test
    public void testDynamicModeUsesPlannedTailPartitionWhenClusterMetadataLagsAfterScale() {
        PartitionPlan planV1 = PartitionPlanBootstrap.createInitialPlan(
                PartitionPlanBootstrapConfig.create(TOPIC, new String[] {"hdfs"}, 3, 9));
        PartitionPlanHolder.getInstance().install(planV1, 12);

        AuditPartitioner partitioner = new AuditPartitioner();
        partitioner.configure(dynamicProducerConfig());

        // Simulate scale: plan now assigns tail partitions 12,13 but producer metadata still shows 12 partitions.
        PartitionPlan planV2 = planV1.toBuilder()
                .version(2)
                .topicPartitionCount(14)
                .plugins(Map.of("hdfs", PluginPartitionAssignment.of(0, 1, 2, 12, 13)))
                .buffer(PluginPartitionAssignment.ofRange(3, 11))
                .updatedBy("test")
                .build();
        PartitionPlanHolder.getInstance().install(planV2, 14);

        // Round-robin index 3 -> planned partition 12; must not clamp to 11 when cluster still reports 12 partitions.
        for (int i = 0; i < 3; i++) {
            partitioner.partition(TOPIC, "hdfs", null, null, null, cluster(12));
        }
        assertEquals(12, partitioner.partition(TOPIC, "hdfs", null, null, null, cluster(12)));
    }

    @Test
    public void testConcurrentPartitionWhilePlanSwaps() throws Exception {
        PartitionPlan planV1 = PartitionPlanBootstrap.createInitialPlan(PartitionPlanBootstrapConfig.create(TOPIC, new String[] {"hdfs"}, 2, 2));
        PartitionPlanHolder.getInstance().install(planV1, 4);

        AuditPartitioner partitioner = new AuditPartitioner();
        partitioner.configure(dynamicProducerConfig());
        Cluster cluster = cluster(4);

        PartitionPlan planV2 = planV1.toBuilder().version(2).updatedBy("test").build();

        ExecutorService executor = Executors.newFixedThreadPool(8);
        CountDownLatch startLatch = new CountDownLatch(1);
        AtomicInteger errors = new AtomicInteger();
        Set<Integer> seen = Collections.synchronizedSet(new HashSet<>());

        for (int i = 0; i < 8; i++) {
            executor.submit(() -> {
                try {
                    startLatch.await();
                    for (int j = 0; j < 200; j++) {
                        int p = partitioner.partition(TOPIC, "hdfs", null, null, null, cluster);
                        seen.add(p);
                    }
                } catch (Exception e) {
                    errors.incrementAndGet();
                }
            });
        }

        startLatch.countDown();
        PartitionPlanHolder.getInstance().install(planV2, 4);
        executor.shutdown();
        assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
        assertEquals(0, errors.get());
        assertTrue(seen.stream().allMatch(p -> p >= 0 && p < 4));
    }

    private static Map<String, Object> staticProducerConfig() {
        String propPrefix = AuditServerConstants.PROP_PREFIX_AUDIT_SERVER;
        Map<String, Object> config = new HashMap<>();
        config.put(propPrefix + AuditServerConstants.PROP_CONFIGURED_PLUGINS, "hdfs,hiveServer2");
        config.put(propPrefix + AuditServerConstants.PROP_TOPIC_PARTITIONS_PER_CONFIGURED_PLUGIN, 3);
        config.put(propPrefix + AuditServerConstants.PROP_TOPIC_PARTITIONS, 15);
        config.put(propPrefix + AuditServerConstants.PROP_BUFFER_PARTITIONS, 9);
        return config;
    }

    private static Map<String, Object> dynamicProducerConfig() {
        Map<String, Object> config = staticProducerConfig();
        config.put(AuditServerConstants.PROP_PREFIX_AUDIT_SERVER.substring(0, AuditServerConstants.PROP_PREFIX_AUDIT_SERVER.length() - 1) + "." + AuditServerConstants.PROP_PARTITION_PLAN_DYNAMIC_ENABLED, "true");
        return config;
    }

    private static Cluster cluster(int partitionCount) {
        PartitionInfo[] partitionInfos = new PartitionInfo[partitionCount];
        for (int i = 0; i < partitionCount; i++) {
            partitionInfos[i] = new PartitionInfo(TOPIC, i, BROKER, new Node[] {BROKER}, new Node[] {BROKER});
        }
        return new Cluster("cluster", Collections.singletonList(BROKER), List.of(partitionInfos), Collections.emptySet(), Collections.emptySet());
    }
}
