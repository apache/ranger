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
import org.apache.ranger.audit.partition.model.BufferEntry;
import org.apache.ranger.audit.partition.model.PartitionPlan;
import org.apache.ranger.audit.partition.model.PluginEntry;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/** Append-only plan updates for Admin-managed audit partition routing. */
public final class PartitionPlanAllocator {
    private PartitionPlanAllocator() {
    }

    /**
     * Onboard a Ranger service repo under a plugin type. Promotes the plugin from buffer when needed,
     * otherwise adds the service to an existing plugin entry.
     */
    public static PartitionPlan onboardService(PartitionPlan current, String pluginId, String serviceName, int partitionCount, String updatedBy) {
        requireMutationInputs(current, pluginId, partitionCount, updatedBy);
        if (StringUtils.isBlank(serviceName)) {
            throw new PartitionPlanException("serviceName is required");
        }
        String trimmedService = serviceName.trim();
        PluginEntry existing = current.getPlugins().get(pluginId);
        if (existing != null) {
            return addServiceToPlugin(current, pluginId, trimmedService, updatedBy);
        }
        return promotePlugin(current, pluginId, partitionCount, updatedBy, trimmedService);
    }

    /** Adds a service repo to an already-promoted plugin without changing partition assignment. */
    public static PartitionPlan addServiceToPlugin(PartitionPlan current, String pluginId, String serviceName, String updatedBy) {
        if (current == null) {
            throw new PartitionPlanException("Current plan is required");
        }
        PartitionPlanValidator.validate(current);
        if (StringUtils.isBlank(pluginId) || StringUtils.isBlank(serviceName) || StringUtils.isBlank(updatedBy)) {
            throw new PartitionPlanException("pluginId, serviceName, and updatedBy are required");
        }
        PluginEntry existing = current.getPlugins().get(pluginId);
        if (existing == null) {
            throw new PartitionPlanException("Plugin '" + pluginId + "' is not configured; promote it first");
        }
        String trimmedService = serviceName.trim();
        if (existing.getServices().contains(trimmedService)) {
            return current;
        }
        ensureServiceNotAssignedElsewhere(current.getPlugins(), pluginId, trimmedService);

        Map<String, PluginEntry> plugins = new LinkedHashMap<>(current.getPlugins());
        plugins.put(pluginId, existing.addService(trimmedService));
        return commitPlanUpdate(current, updatedBy, current.getTopicPartitionCount(), plugins, current.getBuffer().getPartitions());
    }

    /** Removes a service repo from whichever plugin currently owns it. */
    public static PartitionPlan removeService(PartitionPlan current, String serviceName, String updatedBy) {
        if (current == null) {
            throw new PartitionPlanException("Current plan is required");
        }
        PartitionPlanValidator.validate(current);
        if (StringUtils.isBlank(serviceName) || StringUtils.isBlank(updatedBy)) {
            throw new PartitionPlanException("serviceName and updatedBy are required");
        }
        String trimmedService = serviceName.trim();
        String owningPluginId = findPluginForService(current.getPlugins(), trimmedService);
        if (owningPluginId == null) {
            return current;
        }

        PluginEntry existing = Objects.requireNonNull(current.getPlugins().get(owningPluginId));
        List<String> remainingServices = new ArrayList<>(existing.getServices());
        remainingServices.remove(trimmedService);

        Map<String, PluginEntry> plugins = new LinkedHashMap<>(current.getPlugins());
        plugins.put(owningPluginId, existing.withServices(remainingServices));
        return commitPlanUpdate(current, updatedBy, current.getTopicPartitionCount(), plugins, current.getBuffer().getPartitions());
    }

    public static PartitionPlan promotePlugin(PartitionPlan current, String pluginId, int partitionCount, String updatedBy) {
        return promotePlugin(current, pluginId, partitionCount, updatedBy, null);
    }

    /**
     * Give a plugin its own partitions. Uses buffer IDs first; adds new tail IDs when buffer is too small.
     * Optionally attaches {@code serviceName} to the new plugin entry.
     */
    public static PartitionPlan promotePlugin(PartitionPlan current, String pluginId, int partitionCount, String updatedBy, String serviceName) {
        requireMutationInputs(current, pluginId, partitionCount, updatedBy);
        if (current.getPlugins().containsKey(pluginId)) {
            assertPromoteNotConflicting(current, pluginId, partitionCount, serviceName);
            throw new PartitionPlanException("Plugin '" + pluginId + "' already has dedicated partitions");
        }
        if (StringUtils.isNotBlank(serviceName)) {
            ensureServiceNotAssignedElsewhere(current.getPlugins(), pluginId, serviceName.trim());
        }

        List<Integer> remainingBuffer = new ArrayList<>(current.getBuffer().getPartitions());
        List<Integer> newPluginIds    = takeFromBuffer(remainingBuffer, partitionCount);
        int topicPartitionCount       = current.getTopicPartitionCount();
        int additionalNeeded          = partitionCount - newPluginIds.size();
        if (additionalNeeded > 0) {
            topicPartitionCount = appendTailPartitions(newPluginIds, topicPartitionCount, additionalNeeded, collectAssignedPartitionIds(current));
        }

        List<String> services = StringUtils.isNotBlank(serviceName) ? List.of(serviceName.trim()) : List.of();
        Map<String, PluginEntry> plugins = addPluginAssignment(current, pluginId, newPluginIds, services);
        return commitPlanUpdate(current, updatedBy, topicPartitionCount, plugins, remainingBuffer);
    }

    /** Add more partitions to an existing plugin by appending new tail IDs only. */
    public static PartitionPlan scalePlugin(PartitionPlan current, String pluginId, int additionalPartitions, String updatedBy) {
        requireMutationInputs(current, pluginId, additionalPartitions, updatedBy);
        if (!current.getPlugins().containsKey(pluginId)) {
            throw new PartitionPlanException("Plugin '" + pluginId + "' is not configured; promote it first");
        }

        List<Integer> pluginIds = new ArrayList<>(current.getPlugins().get(pluginId).getPartitions());
        int topicPartitionCount = appendTailPartitions(pluginIds, current.getTopicPartitionCount(), additionalPartitions, collectAssignedPartitionIds(current));

        Map<String, PluginEntry> plugins = addPluginAssignment(current, pluginId, pluginIds, current.getPlugins().get(pluginId).getServices());
        return commitPlanUpdate(current, updatedBy, topicPartitionCount, plugins, current.getBuffer().getPartitions());
    }

    public static boolean isOnboardAlreadyApplied(PartitionPlan current, String pluginId, String serviceName, int partitionCount) {
        if (current == null || StringUtils.isBlank(serviceName)) {
            return false;
        }
        PluginEntry existing = current.getPlugins().get(pluginId);
        if (existing == null) {
            return false;
        }
        return existing.getPartitions().size() == partitionCount && existing.getServices().contains(serviceName.trim());
    }

    public static boolean isPromoteAlreadyApplied(PartitionPlan current, String pluginId, int partitionCount) {
        if (current == null) {
            return false;
        }
        PluginEntry existing = current.getPlugins().get(pluginId);
        return existing != null && existing.getPartitions().size() == partitionCount;
    }

    /** Applies a merged plan with append-only checks against the current plan. */
    public static PartitionPlan replacePlan(PartitionPlan current, PartitionPlan proposed) {
        if (current == null || proposed == null) {
            throw new PartitionPlanException("Current and proposed plans are required");
        }
        if (!StringUtils.equals(current.getTopic(), proposed.getTopic())) {
            throw new PartitionPlanException("Proposed topic must match current topic");
        }
        PartitionPlan next = proposed.toBuilder().version(current.getVersion() + 1).build();
        PartitionPlanValidator.validate(next);
        PartitionPlanValidator.validateAppendOnly(current, next);
        return next;
    }

    /** Updates audit POST allow-list metadata; bumps version only when the map changes. */
    public static PartitionPlan updateServiceAllowedUsers(PartitionPlan current, Map<String, List<String>> serviceAllowedUsers, String updatedBy) {
        if (current == null) {
            throw new PartitionPlanException("Current plan is required");
        }
        PartitionPlanValidator.validate(current);
        if (StringUtils.isBlank(updatedBy)) {
            throw new PartitionPlanException("updatedBy is required");
        }

        Map<String, List<String>> normalized = PolicyDownloadAuthUsersUtil.normalizeServiceAllowedUsers(serviceAllowedUsers);
        if (Objects.equals(current.getServiceAllowedUsers(), normalized)) {
            return current;
        }

        PartitionPlan next = current.toBuilder()
                .version(current.getVersion() + 1)
                .serviceAllowedUsers(normalized)
                .updatedAt(Instant.now().toString())
                .updatedBy(updatedBy)
                .build();
        PartitionPlanValidator.validate(next);
        PartitionPlanValidator.validateAppendOnly(current, next);
        return next;
    }

    private static List<Integer> takeFromBuffer(List<Integer> bufferIds, int count) {
        List<Integer> taken = new ArrayList<>(Math.min(count, bufferIds.size()));
        while (taken.size() < count && !bufferIds.isEmpty()) {
            taken.add(bufferIds.remove(0));
        }
        return taken;
    }

    private static int appendTailPartitions(List<Integer> target, int topicPartitionCount, int count, Set<Integer> assigned) {
        int nextId = assigned.isEmpty() ? 1 : assigned.stream().mapToInt(Integer::intValue).max().orElse(0) + 1;
        for (int i = 0; i < count; i++) {
            target.add(nextId++);
            assigned.add(target.get(target.size() - 1));
        }
        return topicPartitionCount + count;
    }

    private static Set<Integer> collectAssignedPartitionIds(PartitionPlan plan) {
        Set<Integer> assigned = new HashSet<>(plan.getBuffer().getPartitions());
        for (PluginEntry entry : plan.getPlugins().values()) {
            assigned.addAll(entry.getPartitions());
        }
        return assigned;
    }

    private static Map<String, PluginEntry> addPluginAssignment(PartitionPlan current, String pluginId, List<Integer> partitionIds, List<String> services) {
        Map<String, PluginEntry> plugins = new LinkedHashMap<>(current.getPlugins());
        plugins.put(pluginId, new PluginEntry(partitionIds, services));
        return plugins;
    }

    private static PartitionPlan commitPlanUpdate(PartitionPlan current, String updatedBy, int topicPartitionCount, Map<String, PluginEntry> plugins, List<Integer> bufferIds) {
        PartitionPlan next = current.toBuilder()
                .version(current.getVersion() + 1)
                .topicPartitionCount(topicPartitionCount)
                .plugins(plugins)
                .buffer(new BufferEntry(bufferIds))
                .updatedAt(Instant.now().toString())
                .updatedBy(updatedBy)
                .build();
        PartitionPlanValidator.validate(next);
        PartitionPlanValidator.validateAppendOnly(current, next);
        return next;
    }

    private static String findPluginForService(Map<String, PluginEntry> plugins, String serviceName) {
        for (Map.Entry<String, PluginEntry> entry : plugins.entrySet()) {
            if (entry.getValue().getServices().contains(serviceName)) {
                return entry.getKey();
            }
        }
        return null;
    }

    private static void ensureServiceNotAssignedElsewhere(Map<String, PluginEntry> plugins, String pluginId, String serviceName) {
        for (Map.Entry<String, PluginEntry> entry : plugins.entrySet()) {
            if (!entry.getKey().equals(pluginId) && entry.getValue().getServices().contains(serviceName)) {
                throw new PartitionPlanException("Service '" + serviceName + "' is already assigned to plugin '" + entry.getKey() + "'");
            }
        }
    }

    private static void assertPromoteNotConflicting(PartitionPlan current, String pluginId, int partitionCount, String serviceName) {
        PluginEntry existing = Objects.requireNonNull(current.getPlugins().get(pluginId));
        if (existing.getPartitions().size() != partitionCount) {
            throw new PartitionPlanException("Plugin '" + pluginId + "' already has " + existing.getPartitions().size() + " dedicated partition(s); requested " + partitionCount);
        }
        if (StringUtils.isNotBlank(serviceName) && existing.getServices().contains(serviceName.trim())) {
            return;
        }
        if (StringUtils.isNotBlank(serviceName)) {
            throw new PartitionPlanException("Plugin '" + pluginId + "' already has dedicated partitions");
        }
    }

    private static void requireMutationInputs(PartitionPlan current, String pluginId, int partitionCount, String updatedBy) {
        if (current == null) {
            throw new PartitionPlanException("Current plan is required");
        }
        PartitionPlanValidator.validate(current);
        if (StringUtils.isBlank(pluginId) || partitionCount < 1 || StringUtils.isBlank(updatedBy)) {
            throw new PartitionPlanException("pluginId, partitionCount, and updatedBy are required");
        }
    }
}
