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

package org.apache.ranger.audit.producer.kafka.partition.model;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.commons.lang3.StringUtils;
import org.apache.ranger.audit.producer.kafka.partition.exception.PartitionPlanException;

import java.io.Serializable;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Partial partition-plan update (REST PATCH). Omitted or empty fields are inherited from the current plan.
 * {@code plugins} and {@code services} merge only entries whose keys are not already present.
 */
@JsonAutoDetect(getterVisibility = Visibility.NONE, setterVisibility = Visibility.NONE, fieldVisibility = Visibility.ANY)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class PartitionPlanReplacement implements Serializable {
    private final int expectedVersion;
    private final Integer topicPartitionCount;
    private final Map<String, PluginPartitionAssignment> plugins;
    private final PluginPartitionAssignment buffer;
    private final Map<String, ServiceAllowlistEntry> services;

    @JsonCreator
    public PartitionPlanReplacement(@JsonProperty("expectedVersion") int expectedVersion,
            @JsonProperty("topicPartitionCount") Integer topicPartitionCount,
            @JsonProperty("plugins") Map<String, PluginPartitionAssignment> plugins,
            @JsonProperty("buffer") PluginPartitionAssignment buffer,
            @JsonProperty("services") Map<String, ServiceAllowlistEntry> services) {
        this.expectedVersion     = expectedVersion;
        this.topicPartitionCount = topicPartitionCount;
        this.plugins             = copyPlugins(plugins);
        this.buffer              = buffer;
        this.services            = copyServices(services);
    }

    public PartitionPlanReplacement(int expectedVersion, int topicPartitionCount,
            Map<String, PluginPartitionAssignment> plugins, PluginPartitionAssignment buffer,
            Map<String, ServiceAllowlistEntry> services) {
        this(expectedVersion, Integer.valueOf(topicPartitionCount), plugins, buffer, services);
    }

    private static Map<String, PluginPartitionAssignment> copyPlugins(Map<String, PluginPartitionAssignment> plugins) {
        if (plugins == null || plugins.isEmpty()) {
            return null;
        }
        return Collections.unmodifiableMap(new LinkedHashMap<>(plugins));
    }

    private static Map<String, ServiceAllowlistEntry> copyServices(Map<String, ServiceAllowlistEntry> services) {
        if (services == null || services.isEmpty()) {
            return null;
        }
        return Collections.unmodifiableMap(new LinkedHashMap<>(services));
    }

    public int getExpectedVersion() {
        return expectedVersion;
    }

    public Integer getTopicPartitionCount() {
        return topicPartitionCount;
    }

    public Map<String, PluginPartitionAssignment> getPlugins() {
        return plugins;
    }

    public PluginPartitionAssignment getBuffer() {
        return buffer;
    }

    public Map<String, ServiceAllowlistEntry> getServices() {
        return services;
    }

    /** True when the request carries at least one field to merge into the current plan. */
    public boolean hasMergeDelta() {
        return hasTopicPartitionCountDelta() || hasPluginsDelta() || hasBufferDelta() || hasServicesDelta();
    }

    public boolean hasTopicPartitionCountDelta() {
        return topicPartitionCount != null && topicPartitionCount >= 1;
    }

    public boolean hasPluginsDelta() {
        return plugins != null && !plugins.isEmpty();
    }

    public boolean hasBufferDelta() {
        return buffer != null && !buffer.getPartitions().isEmpty();
    }

    public boolean hasServicesDelta() {
        return services != null && !services.isEmpty();
    }

    /** Merges this delta into {@code currentPlan} and returns the proposed next plan (version not incremented). */
    public PartitionPlan toMergedPlan(PartitionPlan currentPlan, String updatedBy) {
        if (currentPlan == null) {
            throw new PartitionPlanException("Current partition plan is required");
        }

        int mergedTopicPartitionCount = hasTopicPartitionCountDelta()
                ? topicPartitionCount
                : currentPlan.getTopicPartitionCount();

        Map<String, PluginPartitionAssignment> mergedPlugins = new LinkedHashMap<>(currentPlan.getPlugins());
        List<PluginPartitionAssignment> newlyAddedPluginAssignments = new ArrayList<>();
        if (hasPluginsDelta()) {
            for (Map.Entry<String, PluginPartitionAssignment> pluginDeltaEntry : plugins.entrySet()) {
                String pluginId = pluginDeltaEntry.getKey();
                if (StringUtils.isBlank(pluginId)) {
                    throw new PartitionPlanException("Plugin id is required");
                }
                if (mergedPlugins.containsKey(pluginId)) {
                    PluginPartitionAssignment existingAssignment = mergedPlugins.get(pluginId);
                    PluginPartitionAssignment requestedAssignment = pluginDeltaEntry.getValue();
                    if (existingAssignment.equals(requestedAssignment)) {
                        continue;
                    }
                    throw new PartitionPlanException(
                            "Plugin '" + pluginId + "' already exists with different partition assignment; use PATCH /partition-plan/plugins/{pluginId} to grow it");
                }
                mergedPlugins.put(pluginId, pluginDeltaEntry.getValue());
                newlyAddedPluginAssignments.add(pluginDeltaEntry.getValue());
            }
        }

        PluginPartitionAssignment mergedBuffer = hasBufferDelta() ? buffer : currentPlan.getBuffer();

        Map<String, ServiceAllowlistEntry> mergedServices = new LinkedHashMap<>(currentPlan.getServices());
        if (hasServicesDelta()) {
            for (Map.Entry<String, ServiceAllowlistEntry> serviceDeltaEntry : services.entrySet()) {
                String serviceRepoName = serviceDeltaEntry.getKey();
                if (StringUtils.isBlank(serviceRepoName)) {
                    throw new PartitionPlanException("Service repo name is required");
                }
                mergedServices.put(serviceRepoName.trim(), serviceDeltaEntry.getValue());
            }
        }

        if (!newlyAddedPluginAssignments.isEmpty()) {
            mergedBuffer = removeAssignedPartitionsFromBuffer(mergedBuffer, newlyAddedPluginAssignments);
        }

        return PartitionPlan.builder()
                .topic(currentPlan.getTopic())
                .topicPartitionCount(mergedTopicPartitionCount)
                .plugins(mergedPlugins)
                .buffer(mergedBuffer)
                .services(mergedServices)
                .updatedAt(Instant.now().toString())
                .updatedBy(updatedBy)
                .build();
    }

    private static PluginPartitionAssignment removeAssignedPartitionsFromBuffer(
            PluginPartitionAssignment bufferAssignment, Iterable<PluginPartitionAssignment> newPluginAssignments) {
        List<Integer> remainingBufferPartitionIds = new ArrayList<>(bufferAssignment.getPartitions());
        for (PluginPartitionAssignment pluginAssignment : newPluginAssignments) {
            for (Integer partitionId : pluginAssignment.getPartitions()) {
                remainingBufferPartitionIds.remove(partitionId);
            }
        }
        return new PluginPartitionAssignment(remainingBufferPartitionIds);
    }
}
