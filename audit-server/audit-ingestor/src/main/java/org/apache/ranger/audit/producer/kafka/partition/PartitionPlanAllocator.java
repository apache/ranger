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
import org.apache.ranger.audit.producer.kafka.partition.exception.PartitionPlanException;
import org.apache.ranger.audit.producer.kafka.partition.model.PartitionPlan;
import org.apache.ranger.audit.producer.kafka.partition.model.PluginPartitionAssignment;
import org.apache.ranger.audit.producer.kafka.partition.model.ServiceAllowlistEntry;
import org.apache.ranger.audit.producer.kafka.partition.model.UpdatePlugin;

import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

/** Append-only plan updates: promote unknown plugins and scale hot plugins without reshuffling. */
public class PartitionPlanAllocator {
    private PartitionPlanAllocator() {
    }

    /**
     * Onboard a plugin: promote from buffer and register service allowlists tagged with {@code pluginId}.
     */
    public static PartitionPlan onboardPlugin(PartitionPlan current, String pluginId, int partitionCount, Map<String, ServiceAllowlistEntry> servicesMap, String updatedBy) {
        requireMutationInputs(current, pluginId, partitionCount, updatedBy);
        if (current.getPlugins().containsKey(pluginId)) {
            assertOnboardNotConflicting(current, pluginId, partitionCount, servicesMap);
            throw new PartitionPlanException("Plugin '" + pluginId + "' already has dedicated partitions");
        }

        List<Integer> remainingBuffer = new ArrayList<>(current.getBuffer().getPartitions());
        List<Integer> newPluginIds    = takeFromBuffer(remainingBuffer, partitionCount);
        int topicPartitionCount       = appendTailPartitions(newPluginIds, current.getTopicPartitionCount(), partitionCount - newPluginIds.size());

        Map<String, PluginPartitionAssignment> plugins = addPluginAssignment(current, pluginId, newPluginIds);
        Map<String, ServiceAllowlistEntry> services    = mergeServicesWithPluginId(current.getServices(), servicesMap, pluginId);
        return commitPlanUpdate(current, updatedBy, topicPartitionCount, plugins, remainingBuffer, services);
    }

    /**
     * Update an onboarded plugin: scale tail partitions and/or mutate service allowlists in one version bump.
     */
    public static PartitionPlan updatePlugin(PartitionPlan current, String pluginId, UpdatePlugin updateRequest, String updatedBy) {
        if (current == null || updateRequest == null) {
            throw new PartitionPlanException("Current plan and update request are required");
        }
        if (StringUtils.isBlank(pluginId) || StringUtils.isBlank(updatedBy)) {
            throw new PartitionPlanException("pluginId and updatedBy are required");
        }
        PartitionPlanValidator.validate(current);

        Map<String, ServiceAllowlistEntry> services = new LinkedHashMap<>(current.getServices());
        Map<String, PluginPartitionAssignment> plugins = new LinkedHashMap<>(current.getPlugins());
        List<Integer> bufferIds = new ArrayList<>(current.getBuffer().getPartitions());
        int topicPartitionCount = current.getTopicPartitionCount();

        applyServiceRemovals(current, services, pluginId, updateRequest.getRemoveServices());
        applyServiceUpdates(current, services, pluginId, updateRequest.getUpdateServices());
        applyServiceAdditions(current, services, pluginId, updateRequest.getAddServices());

        Integer additionalPartitions = updateRequest.getAdditionalPartitions();
        if (additionalPartitions != null && additionalPartitions >= 1) {
            if (!plugins.containsKey(pluginId)) {
                throw new PartitionPlanException("Plugin '" + pluginId + "' is not configured; onboard it first");
            }
            List<Integer> pluginIds = new ArrayList<>(plugins.get(pluginId).getPartitions());
            topicPartitionCount = appendTailPartitions(pluginIds, topicPartitionCount, additionalPartitions);
            plugins.put(pluginId, new PluginPartitionAssignment(pluginIds));
        }

        return commitPlanUpdate(current, updatedBy, topicPartitionCount, plugins, bufferIds, services);
    }

    public static PartitionPlan promotePlugin(PartitionPlan current, String pluginId, int partitionCount, String updatedBy) {
        return promotePlugin(current, pluginId, partitionCount, updatedBy, null, null);
    }

    /**
     * Give a plugin its own partitions. Uses buffer IDs first; adds new tail IDs when buffer is too small.
     * Optionally upserts {@code services[repo]} in the same plan version when {@code repo} and {@code allowedUsers} are set.
     */
    public static PartitionPlan promotePlugin(PartitionPlan current, String pluginId, int partitionCount, String updatedBy, String repo, List<String> allowedUsers) {
        requireMutationInputs(current, pluginId, partitionCount, updatedBy);
        if (current.getPlugins().containsKey(pluginId)) {
            assertPromoteNotConflicting(current, pluginId, partitionCount, repo, allowedUsers);
            throw new PartitionPlanException("Plugin '" + pluginId + "' already has dedicated partitions");
        }

        List<Integer> remainingBuffer = new ArrayList<>(current.getBuffer().getPartitions());
        List<Integer> newPluginIds    = takeFromBuffer(remainingBuffer, partitionCount);
        int topicPartitionCount       = appendTailPartitions(newPluginIds, current.getTopicPartitionCount(), partitionCount - newPluginIds.size());

        Map<String, PluginPartitionAssignment> plugins = addPluginAssignment(current, pluginId, newPluginIds);
        Map<String, ServiceAllowlistEntry> services    = mergeServiceAllowlist(current.getServices(), repo, allowedUsers);
        return commitPlanUpdate(current, updatedBy, topicPartitionCount, plugins, remainingBuffer, services);
    }

    /**
     * Onboard a Ranger service repo: promote plugin partitions and upsert service allowlist atomically.
     */
    public static PartitionPlan onboardRepo(PartitionPlan current, String repo, String pluginId, int partitionCount, List<String> allowedUsers, String updatedBy) {
        if (StringUtils.isBlank(repo)) {
            throw new PartitionPlanException("repo is required");
        }
        if (allowedUsers == null || allowedUsers.isEmpty()) {
            throw new PartitionPlanException("allowedUsers are required");
        }
        return promotePlugin(current, pluginId, partitionCount, updatedBy, repo, allowedUsers);
    }

    /**
     * Add more partitions to an existing plugin by appending new tail IDs only.
     */
    public static PartitionPlan scalePlugin(PartitionPlan current, String pluginId, int additionalPartitions, String updatedBy) {
        requireMutationInputs(current, pluginId, additionalPartitions, updatedBy);
        if (!current.getPlugins().containsKey(pluginId)) {
            throw new PartitionPlanException("Plugin '" + pluginId + "' is not configured; promote it first");
        }

        List<Integer> pluginIds = new ArrayList<>(current.getPlugins().get(pluginId).getPartitions());
        int topicPartitionCount = appendTailPartitions(pluginIds, current.getTopicPartitionCount(), additionalPartitions);

        return commitPlanUpdate(current, updatedBy, topicPartitionCount, addPluginAssignment(current, pluginId, pluginIds), current.getBuffer().getPartitions(), current.getServices());
    }

    /**
     * True when plugin onboard with {@code partitionCount} and optional services already matches the current plan.
     */
    public static boolean isOnboardAlreadyApplied(PartitionPlan current, String pluginId, int partitionCount, Map<String, ServiceAllowlistEntry> servicesMap) {
        if (current == null || !pluginHasPartitionCount(current, pluginId, partitionCount)) {
            return false;
        }
        if (servicesMap == null || servicesMap.isEmpty()) {
            return true;
        }
        for (Map.Entry<String, ServiceAllowlistEntry> entry : servicesMap.entrySet()) {
            String repo = entry.getKey().trim();
            ServiceAllowlistEntry existing = current.getServices().get(repo);
            ServiceAllowlistEntry expected = withPluginId(entry.getValue(), pluginId);
            if (existing == null || !serviceEntryMatches(existing, expected, pluginId)) {
                return false;
            }
        }
        return true;
    }

    /**
     * True when a service-only update request is already satisfied (scale mutations are never treated as no-op).
     */
    public static boolean isUpdateAlreadyApplied(PartitionPlan current, String pluginId, UpdatePlugin updateRequest) {
        if (current == null || updateRequest == null) {
            return false;
        }
        Integer additionalPartitions = updateRequest.getAdditionalPartitions();
        if (additionalPartitions != null && additionalPartitions >= 1) {
            return false;
        }
        for (String repo : updateRequest.getRemoveServices()) {
            if (current.getServices().containsKey(repo.trim())) {
                return false;
            }
        }
        for (Map.Entry<String, ServiceAllowlistEntry> entry : updateRequest.getAddServices().entrySet()) {
            ServiceAllowlistEntry existing = current.getServices().get(entry.getKey().trim());
            ServiceAllowlistEntry expected = withPluginId(entry.getValue(), pluginId);
            if (existing == null || !serviceEntryMatches(existing, expected, pluginId)) {
                return false;
            }
        }
        for (Map.Entry<String, ServiceAllowlistEntry> entry : updateRequest.getUpdateServices().entrySet()) {
            ServiceAllowlistEntry existing = current.getServices().get(entry.getKey().trim());
            ServiceAllowlistEntry expected = withPluginId(entry.getValue(), pluginId);
            if (existing == null || !serviceEntryMatches(existing, expected, pluginId)) {
                return false;
            }
        }
        return updateRequest.hasMutationDelta();
    }

    /**
     * True when the plugin is already promoted with {@code partitionCount} slots and, when {@code repo} is set,
     * the service allowlist already matches {@code allowedUsers}.
     */
    public static boolean isPromoteAlreadyApplied(PartitionPlan current, String pluginId, int partitionCount, String repo, List<String> allowedUsers) {
        if (current == null || !pluginHasPartitionCount(current, pluginId, partitionCount)) {
            return false;
        }
        if (StringUtils.isNotBlank(repo)) {
            ServiceAllowlistEntry existing = current.getServices().get(repo.trim());
            return existing != null && existing.hasSameAllowedUsers(allowedUsers);
        }
        return true;
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

    private static void applyServiceRemovals(PartitionPlan current, Map<String, ServiceAllowlistEntry> services, String pluginId, List<String> removeServices) {
        for (String repoName : removeServices) {
            String repo = repoName.trim();
            verifyServiceOwnedByPlugin(current, repo, pluginId);
            services.remove(repo);
        }
    }

    private static void applyServiceUpdates(PartitionPlan current, Map<String, ServiceAllowlistEntry> services, String pluginId, Map<String, ServiceAllowlistEntry> updateServices) {
        for (Map.Entry<String, ServiceAllowlistEntry> entry : updateServices.entrySet()) {
            String repo = entry.getKey().trim();
            verifyServiceOwnedByPlugin(current, repo, pluginId);
            services.put(repo, withPluginId(entry.getValue(), pluginId));
        }
    }

    private static void applyServiceAdditions(PartitionPlan current, Map<String, ServiceAllowlistEntry> services, String pluginId, Map<String, ServiceAllowlistEntry> addServices) {
        for (Map.Entry<String, ServiceAllowlistEntry> entry : addServices.entrySet()) {
            String repo = entry.getKey().trim();
            ServiceAllowlistEntry existing = current.getServices().get(repo);
            if (existing != null) {
                assertServiceOwnedByPlugin(existing, repo, pluginId);
            }
            services.put(repo, withPluginId(entry.getValue(), pluginId));
        }
    }

    private static void verifyServiceOwnedByPlugin(PartitionPlan current, String repo, String pluginId) {
        ServiceAllowlistEntry existing = current.getServices().get(repo);
        if (existing == null) {
            throw new PartitionPlanException("Service '" + repo + "' is not configured");
        }
        assertServiceOwnedByPlugin(existing, repo, pluginId);
    }

    private static void assertServiceOwnedByPlugin(ServiceAllowlistEntry existing, String repo, String pluginId) {
        if (existing.getPluginId() != null && !Objects.equals(existing.getPluginId(), pluginId)) {
            throw new PartitionPlanException("Service '" + repo + "' belongs to plugin '" + existing.getPluginId() + "'");
        }
    }

    private static boolean serviceEntryMatches(ServiceAllowlistEntry existing, ServiceAllowlistEntry expected, String pluginId) {
        return existing.hasSameAllowedUsers(expected.getAllowedUsers()) && Objects.equals(existing.getPluginId(), pluginId);
    }

    private static ServiceAllowlistEntry withPluginId(ServiceAllowlistEntry entry, String pluginId) {
        if (entry == null) {
            throw new PartitionPlanException("Service allowlist entry is required");
        }
        return new ServiceAllowlistEntry(entry.getAllowedUsers(), entry.getSource(), entry.getNotes(), pluginId);
    }

    /** Pull up to count partition IDs from the front of the buffer list. */
    private static List<Integer> takeFromBuffer(List<Integer> bufferIds, int count) {
        List<Integer> taken = new ArrayList<>(Math.min(count, bufferIds.size()));
        while (taken.size() < count && !bufferIds.isEmpty()) {
            taken.add(bufferIds.remove(0));
        }
        return taken;
    }

    /** Append new tail partition IDs and return the new topic partition count. */
    private static int appendTailPartitions(List<Integer> target, int topicPartitionCount, int count) {
        for (int i = 0; i < count; i++) {
            target.add(topicPartitionCount++);
        }
        return topicPartitionCount;
    }

    private static Map<String, PluginPartitionAssignment> addPluginAssignment(PartitionPlan current, String pluginId, List<Integer> partitionIds) {
        Map<String, PluginPartitionAssignment> plugins = new LinkedHashMap<>(current.getPlugins());
        plugins.put(pluginId, new PluginPartitionAssignment(partitionIds));
        return plugins;
    }

    private static PartitionPlan commitPlanUpdate(PartitionPlan current, String updatedBy, int topicPartitionCount, Map<String, PluginPartitionAssignment> plugins, List<Integer> bufferIds, Map<String, ServiceAllowlistEntry> services) {
        PartitionPlan next = current.toBuilder()
                .version(current.getVersion() + 1)
                .topicPartitionCount(topicPartitionCount)
                .plugins(plugins)
                .buffer(new PluginPartitionAssignment(bufferIds))
                .services(services != null ? services : current.getServices())
                .updatedAt(Instant.now().toString())
                .updatedBy(updatedBy)
                .build();
        PartitionPlanValidator.validate(next);
        PartitionPlanValidator.validateAppendOnly(current, next);
        return next;
    }

    private static Map<String, ServiceAllowlistEntry> mergeServicesWithPluginId(Map<String, ServiceAllowlistEntry> currentServices, Map<String, ServiceAllowlistEntry> servicesMap, String pluginId) {
        Map<String, ServiceAllowlistEntry> services = new LinkedHashMap<>(currentServices);
        if (servicesMap == null || servicesMap.isEmpty()) {
            return services;
        }
        for (Map.Entry<String, ServiceAllowlistEntry> entry : servicesMap.entrySet()) {
            String repo = entry.getKey().trim();
            ServiceAllowlistEntry tagged = withPluginId(entry.getValue(), pluginId);
            ServiceAllowlistEntry existing = services.get(repo);
            if (existing != null && !existing.hasSameAllowedUsers(tagged.getAllowedUsers())) {
                throw new PartitionPlanException("Service '" + repo + "' already exists with different allowedUsers");
            }
            if (existing != null && existing.getPluginId() != null && !Objects.equals(existing.getPluginId(), pluginId)) {
                throw new PartitionPlanException("Service '" + repo + "' belongs to plugin '" + existing.getPluginId() + "'");
            }
            services.put(repo, tagged);
        }
        return services;
    }

    private static Map<String, ServiceAllowlistEntry> mergeServiceAllowlist(Map<String, ServiceAllowlistEntry> currentServices, String repo, List<String> allowedUsers) {
        Map<String, ServiceAllowlistEntry> services = new LinkedHashMap<>(currentServices);
        if (StringUtils.isNotBlank(repo) && allowedUsers != null && !allowedUsers.isEmpty()) {
            services.put(repo.trim(), ServiceAllowlistEntry.ofUsers(allowedUsers));
        }
        return services;
    }

    private static boolean pluginHasPartitionCount(PartitionPlan current, String pluginId, int partitionCount) {
        PluginPartitionAssignment assignment = current.getPlugins().get(pluginId);
        return assignment != null && assignment.getPartitions().size() == partitionCount;
    }

    private static void assertOnboardNotConflicting(PartitionPlan current, String pluginId, int partitionCount, Map<String, ServiceAllowlistEntry> servicesMap) {
        PluginPartitionAssignment existing = requireNonNull(current.getPlugins().get(pluginId));
        if (existing.getPartitions().size() != partitionCount) {
            throw new PartitionPlanException("Plugin '" + pluginId + "' already has " + existing.getPartitions().size() + " dedicated partition(s); requested " + partitionCount);
        }
        if (servicesMap != null) {
            for (Map.Entry<String, ServiceAllowlistEntry> entry : servicesMap.entrySet()) {
                String repo = entry.getKey().trim();
                ServiceAllowlistEntry serviceEntry = current.getServices().get(repo);
                if (serviceEntry != null && !serviceEntry.hasSameAllowedUsers(entry.getValue().getAllowedUsers())) {
                    throw new PartitionPlanException("Service '" + repo + "' already exists with different allowedUsers");
                }
            }
        }
    }

    private static void assertPromoteNotConflicting(PartitionPlan current, String pluginId, int partitionCount, String repo, List<String> allowedUsers) {
        PluginPartitionAssignment existing = requireNonNull(current.getPlugins().get(pluginId));
        if (existing.getPartitions().size() != partitionCount) {
            throw new PartitionPlanException("Plugin '" + pluginId + "' already has " + existing.getPartitions().size() + " dedicated partition(s); requested " + partitionCount);
        }
        if (StringUtils.isNotBlank(repo)) {
            ServiceAllowlistEntry serviceEntry = current.getServices().get(repo.trim());
            if (serviceEntry != null && !serviceEntry.hasSameAllowedUsers(allowedUsers)) {
                throw new PartitionPlanException("Service '" + repo.trim() + "' already exists with different allowedUsers");
            }
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
