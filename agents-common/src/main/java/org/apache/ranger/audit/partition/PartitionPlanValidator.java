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

package org.apache.ranger.audit.partition;

import org.apache.commons.lang3.StringUtils;
import org.apache.ranger.audit.partition.exception.PartitionPlanException;
import org.apache.ranger.audit.partition.model.PartitionPlan;
import org.apache.ranger.audit.partition.model.PluginEntry;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/** Checks partition plan shape and append-only updates. */
public final class PartitionPlanValidator {
    private PartitionPlanValidator() {
    }

    public static void validate(PartitionPlan plan) {
        validate(plan, null);
    }

    /**
     * When kafkaPartitionCount is set, it must be at least plan.topicPartitionCount.
     * Extra live Kafka partitions (e.g. after static-mode migration) are allowed.
     */
    public static void validate(PartitionPlan plan, Integer kafkaPartitionCount) {
        if (plan == null || StringUtils.isBlank(plan.getTopic()) || plan.getVersion() < AuditPartitionPlanConstants.INITIAL_PLAN_VERSION || plan.getTopicPartitionCount() < 1) {
            throw new PartitionPlanException("Invalid partition plan");
        }
        if (kafkaPartitionCount != null && kafkaPartitionCount < plan.getTopicPartitionCount()) {
            throw new PartitionPlanException("Kafka topic has fewer partitions than plan requires");
        }

        Set<Integer> assigned = new HashSet<>();
        registerPartitions(plan.getBuffer().getPartitions(), assigned, true);
        for (Map.Entry<String, PluginEntry> entry : plan.getPlugins().entrySet()) {
            if (StringUtils.isBlank(entry.getKey())) {
                throw new PartitionPlanException("Plugin id is required");
            }
            PluginEntry pluginEntry = entry.getValue();
            if (pluginEntry == null) {
                throw new PartitionPlanException("Plugin entry is required for '" + entry.getKey().trim() + "'");
            }
            registerPartitions(pluginEntry.getPartitions(), assigned, false);
        }
        if (assigned.size() != plan.getTopicPartitionCount()) {
            throw new PartitionPlanException("topicPartitionCount must equal the union of all assigned partition ids");
        }
        validateContiguousPartitionRange(assigned, plan.getTopicPartitionCount());
        validateServiceUniqueness(plan.getPlugins());
        validateServiceAllowedUsers(plan.getServiceAllowedUsers());
    }

    /**
     * When a service repo is listed in {@code serviceAllowedUsers}, it must have at least one
     * allowed short username (from Admin {@code policy.download.auth.users}).
     */
    public static void validateServiceAllowedUsers(Map<String, List<String>> serviceAllowedUsers) {
        if (serviceAllowedUsers == null || serviceAllowedUsers.isEmpty()) {
            return;
        }
        for (Map.Entry<String, List<String>> entry : serviceAllowedUsers.entrySet()) {
            if (StringUtils.isBlank(entry.getKey())) {
                throw new PartitionPlanException("Service repo name is required");
            }
            List<String> users = effectiveAllowedUsers(entry.getValue());
            if (users.isEmpty()) {
                throw new PartitionPlanException(
                        "allowedUsers must contain at least one effective user for service '" + entry.getKey().trim() + "'");
            }
        }
    }

    /** Each Ranger service repo name may appear in at most one plugin entry. */
    public static void validateServiceUniqueness(Map<String, PluginEntry> plugins) {
        if (plugins == null || plugins.isEmpty()) {
            return;
        }
        Set<String> seenServices = new HashSet<>();
        for (Map.Entry<String, PluginEntry> entry : plugins.entrySet()) {
            PluginEntry pluginEntry = entry.getValue();
            if (pluginEntry == null) {
                throw new PartitionPlanException("Plugin entry is required for '" + entry.getKey() + "'");
            }
            for (String serviceName : pluginEntry.getServices()) {
                if (!seenServices.add(serviceName)) {
                    throw new PartitionPlanException("Service '" + serviceName + "' is assigned to more than one plugin");
                }
            }
        }
    }

    /** New plan must only add tail partitions; existing plugin lists stay unchanged in order. */
    public static void validateAppendOnly(PartitionPlan current, PartitionPlan proposed) {
        if (current == null || proposed == null) {
            throw new PartitionPlanException("Current and proposed plans are required");
        }
        if (proposed.getTopicPartitionCount() < current.getTopicPartitionCount()) {
            throw new PartitionPlanException("Plan must not shrink topicPartitionCount");
        }
        if (proposed.getVersion() != current.getVersion() + 1) {
            throw new PartitionPlanException("Plan version must increment by one");
        }

        for (Map.Entry<String, PluginEntry> entry : current.getPlugins().entrySet()) {
            String pluginId = entry.getKey();
            List<Integer> before = entry.getValue().getPartitions();
            PluginEntry afterEntry = proposed.getPlugins().get(pluginId);
            if (afterEntry == null) {
                throw new PartitionPlanException("Append-only violation for plugin '" + pluginId + "'");
            }
            List<Integer> after = afterEntry.getPartitions();
            if (after.size() < before.size()) {
                throw new PartitionPlanException("Append-only violation for plugin '" + pluginId + "'");
            }
            if (!after.subList(0, before.size()).equals(before)) {
                throw new PartitionPlanException("Append-only violation for plugin '" + pluginId + "': existing partitions reshuffled");
            }
        }
    }

    private static List<String> effectiveAllowedUsers(List<String> rawUsers) {
        if (rawUsers == null || rawUsers.isEmpty()) {
            return List.of();
        }
        return rawUsers.stream()
                .flatMap(user -> PolicyDownloadAuthUsersUtil.parseUsers(user).stream())
                .collect(Collectors.toList());
    }

    private static void validateContiguousPartitionRange(Set<Integer> assigned, int topicPartitionCount) {
        for (int partitionId = 1; partitionId <= topicPartitionCount; partitionId++) {
            if (!assigned.contains(partitionId)) {
                throw new PartitionPlanException(
                        "Partition ids must cover contiguous range 1.." + topicPartitionCount + " (missing id " + partitionId + ")");
            }
        }
    }

    private static void registerPartitions(List<Integer> partitionIds, Set<Integer> assigned, boolean allowEmpty) {
        if (partitionIds.isEmpty()) {
            if (allowEmpty) {
                return;
            }
            throw new PartitionPlanException("Plugin partition list must not be empty");
        }
        for (int partitionId : partitionIds) {
            if (partitionId < 1 || !assigned.add(partitionId)) {
                throw new PartitionPlanException("Invalid or duplicate partition id: " + partitionId);
            }
        }
    }
}
