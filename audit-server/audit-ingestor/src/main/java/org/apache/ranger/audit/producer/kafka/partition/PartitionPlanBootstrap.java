/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.ranger.audit.producer.kafka.partition;

import org.apache.commons.lang3.StringUtils;
import org.apache.ranger.audit.producer.kafka.partition.constants.PartitionPlanConstants;
import org.apache.ranger.audit.producer.kafka.partition.exception.PartitionPlanException;
import org.apache.ranger.audit.producer.kafka.partition.model.PartitionPlan;
import org.apache.ranger.audit.producer.kafka.partition.model.PluginPartitionAssignment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;

/** Builds the initial bootstrap plan from legacy XML and seeds the registry when the plan topic is empty. */
public class PartitionPlanBootstrap {
    private static final Logger LOG = LoggerFactory.getLogger(PartitionPlanBootstrap.class);

    private PartitionPlanBootstrap() {
    }

    /** Builds the initial bootstrap plan ({@link PartitionPlanConstants#INITIAL_PLAN_VERSION}) using the same contiguous layout as static {@code AuditPartitioner}. */
    public static PartitionPlan createInitialPlan(PartitionPlanBootstrapConfig config) {
        if (config == null || StringUtils.isBlank(config.getAuditTopic())) {
            throw new PartitionPlanException("Audit topic and bootstrap config are required");
        }

        Map<String, PluginPartitionAssignment> plugins = new LinkedHashMap<>();
        int nextPartition = 0;
        for (String plugin : config.getConfiguredPlugins()) {
            if (StringUtils.isBlank(plugin)) {
                continue;
            }
            int count = config.getPartitionsForPlugin(plugin.trim());
            plugins.put(plugin.trim(), PluginPartitionAssignment.ofRange(nextPartition, nextPartition + count - 1));
            nextPartition += count;
        }

        int topicPartitionCount;
        if (nextPartition == 0) {
            topicPartitionCount = config.getHashBasedTopicPartitionCount();
        } else {
            topicPartitionCount = nextPartition + Math.max(1, config.getBufferPartitionCount());
        }
        PartitionPlan plan = PartitionPlan.builder()
                .topic(config.getAuditTopic())
                .version(PartitionPlanConstants.INITIAL_PLAN_VERSION)
                .topicPartitionCount(topicPartitionCount)
                .updatedAt(Instant.now().toString())
                .updatedBy(PartitionPlanConstants.BOOTSTRAP_UPDATED_BY)
                .plugins(plugins)
                .buffer(PluginPartitionAssignment.ofRange(nextPartition, topicPartitionCount - 1))
                .services(ServiceAllowlistBootstrap.loadAllowlistsFromServerConfig())
                .build();

        PartitionPlanValidator.validate(plan);
        return plan;
    }

    /** Builds the first plan from legacy ingestor producer/XML configuration. */
    public static PartitionPlan createInitialPlanFromProducerConfig(Map<String, ?> producerConfig, String auditTopic) {
        return createInitialPlan(PartitionPlanBootstrapConfig.fromProducerConfigMap(producerConfig, auditTopic));
    }

    /**
     * Empty-registry bootstrap: read registry, build the initial bootstrap plan from XML when no plan
     * exists, re-read before publish (concurrent pods), write once, then mandatory read-back.
     */
    public static PartitionPlan bootstrapIfEmpty(PartitionPlanRegistry registry, String auditTopic, Map<String, ?> producerConfig) {
        PartitionPlan plan = registry.readPlan(auditTopic);
        if (plan != null) {
            LOG.info("Partition plan version {} already present for audit topic '{}'", plan.getVersion(), auditTopic);
            return plan;
        }

        PartitionPlan localPlan = createInitialPlanFromProducerConfig(producerConfig, auditTopic);
        plan = registry.readPlan(auditTopic);
        if (plan != null) {
            LOG.info("Peer published partition plan version {} while bootstrapping audit topic '{}'", plan.getVersion(), auditTopic);
            return plan;
        }

        registry.writePlan(auditTopic, localPlan);
        long readBackDeadlineMs = System.currentTimeMillis() + 30_000L;
        while (System.currentTimeMillis() < readBackDeadlineMs) {
            plan = registry.readPlan(auditTopic);
            if (plan != null) {
                LOG.info("Bootstrap partition plan version {} published and read back for audit topic '{}'", plan.getVersion(), auditTopic);
                return plan;
            }
            try {
                Thread.sleep(500L);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        LOG.error("Mandatory read-back failed after publishing initial bootstrap plan for audit topic '{}'", auditTopic);
        throw new PartitionPlanException("Mandatory read-back failed after publishing bootstrap plan for audit topic '" + auditTopic + "'");
    }
}
