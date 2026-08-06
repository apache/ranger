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

package org.apache.ranger.audit.partition.model;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.ranger.audit.partition.PartitionPlanValidator;
import org.apache.ranger.audit.partition.exception.PartitionPlanException;
import org.apache.ranger.authorization.utils.JsonUtils;

import java.io.Serializable;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

@JsonAutoDetect(getterVisibility = Visibility.NONE, setterVisibility = Visibility.NONE, fieldVisibility = Visibility.ANY)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class PartitionPlan implements Serializable {
    private static final long serialVersionUID = 1L;

    private final String topic;
    private final int    version;
    private final int    topicPartitionCount;
    private final String updatedAt;
    private final String updatedBy;
    private final Map<String, PluginEntry> plugins;
    private final BufferEntry              buffer;
    /** Per-repo audit POST allow-list ({@code policy.download.auth.users}); each listed repo needs ≥1 user. */
    private final Map<String, List<String>>  serviceAllowedUsers;

    @JsonCreator
    public PartitionPlan(@JsonProperty("topic") String topic, @JsonProperty("version") int version, @JsonProperty("topicPartitionCount") int topicPartitionCount, @JsonProperty("updatedAt") String updatedAt, @JsonProperty("updatedBy") String updatedBy, @JsonProperty("plugins") Map<String, PluginEntry> plugins, @JsonProperty("buffer") BufferEntry buffer, @JsonProperty("serviceAllowedUsers") Map<String, List<String>> serviceAllowedUsers) {
        this.topic               = topic;
        this.version             = version;
        this.topicPartitionCount = topicPartitionCount;
        this.updatedAt           = updatedAt;
        this.updatedBy           = updatedBy;
        this.plugins             = copyPlugins(plugins);
        this.buffer              = buffer != null ? buffer : BufferEntry.empty();
        this.serviceAllowedUsers = copyServiceAllowedUsers(serviceAllowedUsers);
    }

    public String getTopic() {
        return topic;
    }

    public int getVersion() {
        return version;
    }

    public int getTopicPartitionCount() {
        return topicPartitionCount;
    }

    public String getUpdatedAt() {
        return updatedAt;
    }

    public String getUpdatedBy() {
        return updatedBy;
    }

    public Map<String, PluginEntry> getPlugins() {
        return plugins;
    }

    public BufferEntry getBuffer() {
        return buffer;
    }

    public Map<String, List<String>> getServiceAllowedUsers() {
        return serviceAllowedUsers;
    }

    /** Compares routing payload; ignores version, updatedAt, updatedBy, and serviceAllowedUsers. */
    public boolean sameContentAs(PartitionPlan other) {
        if (other == null) {
            return false;
        }
        return topicPartitionCount == other.topicPartitionCount
                && Objects.equals(topic, other.topic)
                && Objects.equals(plugins, other.plugins)
                && Objects.equals(buffer, other.buffer);
    }

    public Builder toBuilder() {
        return new Builder(this);
    }

    public static Builder builder() {
        return new Builder();
    }

    public String toJson() {
        String json = JsonUtils.objectToJson(this);
        if (json == null) {
            throw new PartitionPlanException("Failed to serialize partition plan");
        }
        return json;
    }

    public static PartitionPlan fromJson(String json) {
        try {
            PartitionPlan plan = JsonUtils.jsonToObject(json, PartitionPlan.class);
            PartitionPlanValidator.validate(plan);
            return plan;
        } catch (PartitionPlanException e) {
            throw e;
        } catch (Exception e) {
            throw new PartitionPlanException("Failed to deserialize partition plan", e);
        }
    }

    @Override
    public boolean equals(Object otherPartitionPlanObj) {
        if (this == otherPartitionPlanObj) {
            return true;
        }
        if (otherPartitionPlanObj == null || getClass() != otherPartitionPlanObj.getClass()) {
            return false;
        }
        PartitionPlan otherPartitionPlan = (PartitionPlan) otherPartitionPlanObj;
        return version == otherPartitionPlan.version
                && topicPartitionCount == otherPartitionPlan.topicPartitionCount
                && Objects.equals(topic, otherPartitionPlan.topic)
                && Objects.equals(updatedAt, otherPartitionPlan.updatedAt)
                && Objects.equals(updatedBy, otherPartitionPlan.updatedBy)
                && Objects.equals(plugins, otherPartitionPlan.plugins)
                && Objects.equals(buffer, otherPartitionPlan.buffer)
                && Objects.equals(serviceAllowedUsers, otherPartitionPlan.serviceAllowedUsers);
    }

    @Override
    public int hashCode() {
        return Objects.hash(topic, version, topicPartitionCount, updatedAt, updatedBy, plugins, buffer, serviceAllowedUsers);
    }

    @Override
    public String toString() {
        return "PartitionPlan{topic='" + topic + "', version=" + version + ", topicPartitionCount=" + topicPartitionCount + ", plugins=" + plugins.keySet() + ", bufferSize=" + buffer.getPartitions().size() + ", serviceAllowedUsers=" + serviceAllowedUsers.keySet() + '}';
    }

    private static Map<String, PluginEntry> copyPlugins(Map<String, PluginEntry> plugins) {
        if (plugins == null || plugins.isEmpty()) {
            return Collections.emptyMap();
        }
        return Collections.unmodifiableMap(new LinkedHashMap<>(plugins));
    }

    private static Map<String, List<String>> copyServiceAllowedUsers(Map<String, List<String>> serviceAllowedUsers) {
        if (serviceAllowedUsers == null || serviceAllowedUsers.isEmpty()) {
            return Collections.emptyMap();
        }
        Map<String, List<String>> copy = new LinkedHashMap<>();
        for (Map.Entry<String, List<String>> entry : serviceAllowedUsers.entrySet()) {
            if (entry.getKey() == null || entry.getKey().isBlank()) {
                continue;
            }
            List<String> users = entry.getValue() == null ? Collections.emptyList() : List.copyOf(entry.getValue());
            copy.put(entry.getKey().trim(), users);
        }
        return Collections.unmodifiableMap(copy);
    }

    public static final class Builder {
        private String                       topic;
        private int                          version = 1;
        private int                          topicPartitionCount;
        private String                       updatedAt;
        private String                       updatedBy;
        private Map<String, PluginEntry>     plugins = new LinkedHashMap<>();
        private BufferEntry                  buffer  = BufferEntry.empty();
        private Map<String, List<String>>    serviceAllowedUsers = new LinkedHashMap<>();

        private Builder() {
        }

        private Builder(PartitionPlan plan) {
            this.topic               = plan.topic;
            this.version             = plan.version;
            this.topicPartitionCount = plan.topicPartitionCount;
            this.updatedAt           = plan.updatedAt;
            this.updatedBy           = plan.updatedBy;
            this.plugins             = new LinkedHashMap<>(plan.plugins);
            this.buffer              = plan.buffer;
            this.serviceAllowedUsers = new LinkedHashMap<>(plan.serviceAllowedUsers);
        }

        public Builder topic(String topic) {
            this.topic = topic;
            return this;
        }

        public Builder version(int version) {
            this.version = version;
            return this;
        }

        public Builder topicPartitionCount(int topicPartitionCount) {
            this.topicPartitionCount = topicPartitionCount;
            return this;
        }

        public Builder updatedAt(String updatedAt) {
            this.updatedAt = updatedAt;
            return this;
        }

        public Builder updatedBy(String updatedBy) {
            this.updatedBy = updatedBy;
            return this;
        }

        public Builder plugins(Map<String, PluginEntry> plugins) {
            this.plugins = plugins == null ? new LinkedHashMap<>() : new LinkedHashMap<>(plugins);
            return this;
        }

        public Builder putPlugin(String pluginId, PluginEntry entry) {
            this.plugins.put(pluginId, entry);
            return this;
        }

        public Builder buffer(BufferEntry buffer) {
            this.buffer = buffer != null ? buffer : BufferEntry.empty();
            return this;
        }

        public Builder serviceAllowedUsers(Map<String, List<String>> serviceAllowedUsers) {
            this.serviceAllowedUsers = serviceAllowedUsers == null ? new LinkedHashMap<>() : new LinkedHashMap<>(serviceAllowedUsers);
            return this;
        }

        public PartitionPlan build() {
            return new PartitionPlan(topic, version, topicPartitionCount, updatedAt, updatedBy, plugins, buffer, serviceAllowedUsers);
        }
    }
}
