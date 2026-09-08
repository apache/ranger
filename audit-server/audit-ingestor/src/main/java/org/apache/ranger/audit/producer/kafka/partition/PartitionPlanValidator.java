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
import org.apache.ranger.audit.producer.kafka.partition.model.ServiceAllowlistEntry;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Checks partition plan shape and append-only updates. */
public class PartitionPlanValidator {
    private PartitionPlanValidator() {
    }

    public static void validate(PartitionPlan plan) {
        validate(plan, null);
    }

    /** When kafkaPartitionCount is set, it must match plan.topicPartitionCount. */
    public static void validate(PartitionPlan plan, Integer kafkaPartitionCount) {
        if (plan == null || StringUtils.isBlank(plan.getTopic()) || plan.getVersion() < PartitionPlanConstants.INITIAL_PLAN_VERSION || plan.getTopicPartitionCount() < 1) {
            throw new PartitionPlanException("Invalid partition plan");
        }
        if (kafkaPartitionCount != null && !kafkaPartitionCount.equals(plan.getTopicPartitionCount())) {
            throw new PartitionPlanException("topicPartitionCount does not match Kafka topic partition count");
        }

        Set<Integer> assigned = new HashSet<>();
        registerPartitions(plan.getBuffer().getPartitions(), plan.getTopicPartitionCount(), assigned, true);
        for (Map.Entry<String, PluginPartitionAssignment> entry : plan.getPlugins().entrySet()) {
            if (StringUtils.isBlank(entry.getKey())) {
                throw new PartitionPlanException("Plugin id is required");
            }
            registerPartitions(entry.getValue().getPartitions(), plan.getTopicPartitionCount(), assigned, false);
        }
        if (assigned.size() != plan.getTopicPartitionCount()) {
            throw new PartitionPlanException("Partition plan must assign every topic partition exactly once");
        }
        validateServices(plan.getServices());
    }

    /** When present, each service entry must declare at least one allowed short username. */
    public static void validateServices(Map<String, ServiceAllowlistEntry> services) {
        if (services == null || services.isEmpty()) {
            return;
        }
        for (Map.Entry<String, ServiceAllowlistEntry> entry : services.entrySet()) {
            if (StringUtils.isBlank(entry.getKey())) {
                throw new PartitionPlanException("Service repo name is required");
            }
            ServiceAllowlistEntry allowlistEntry = entry.getValue();
            if (allowlistEntry == null || allowlistEntry.getAllowedUsers().isEmpty()) {
                throw new PartitionPlanException("allowedUsers must not be empty for service '" + entry.getKey() + "'");
            }
        }
    }

    /** New plan must only add tail partitions; existing plugin lists stay unchanged in order. */
    public static void validateAppendOnly(PartitionPlan current, PartitionPlan proposed) {
        if (current == null || proposed == null) {
            throw new PartitionPlanException("Current and proposed plans are required");
        }
        if (proposed.getTopicPartitionCount() < current.getTopicPartitionCount() || proposed.getVersion() != current.getVersion() + 1) {
            throw new PartitionPlanException("Plan must grow partition count and increment version by one");
        }

        for (Map.Entry<String, PluginPartitionAssignment> entry : current.getPlugins().entrySet()) {
            String pluginId = entry.getKey();
            List<Integer> before = entry.getValue().getPartitions();
            PluginPartitionAssignment afterAssignment = proposed.getPlugins().get(pluginId);
            if (afterAssignment == null) {
                throw new PartitionPlanException("Append-only violation for plugin '" + pluginId + "'");
            }
            List<Integer> after = afterAssignment.getPartitions();
            if (after.size() < before.size()) {
                throw new PartitionPlanException("Append-only violation for plugin '" + pluginId + "'");
            }
            for (int i = 0; i < before.size(); i++) {
                if (!before.get(i).equals(after.get(i))) {
                    throw new PartitionPlanException("Append-only violation for plugin '" + pluginId + "' at index " + i);
                }
            }
        }
    }

    private static void registerPartitions(List<Integer> partitionIds, int topicPartitionCount, Set<Integer> assigned, boolean allowEmpty) {
        if (partitionIds.isEmpty()) {
            if (allowEmpty) {
                return;
            }
            throw new PartitionPlanException("Plugin partition list must not be empty");
        }
        for (int partitionId : partitionIds) {
            if (partitionId < 0 || partitionId >= topicPartitionCount || !assigned.add(partitionId)) {
                throw new PartitionPlanException("Invalid or duplicate partition id: " + partitionId);
            }
        }
    }
}
