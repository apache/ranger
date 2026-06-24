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

import java.io.Serializable;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/** Request body for POST /api/audit/partition-plan/plugins. {@code services} is required (non-empty) at validation. */
@JsonAutoDetect(getterVisibility = Visibility.NONE, setterVisibility = Visibility.NONE, fieldVisibility = Visibility.ANY)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class OnboardPlugin implements Serializable {
    private final String pluginId;
    private final int partitionCount;
    private final int expectedVersion;
    private final Map<String, ServiceAllowlistEntry> services;

    @JsonCreator
    public OnboardPlugin(@JsonProperty("pluginId") String pluginId, @JsonProperty("partitionCount") int partitionCount, @JsonProperty("expectedVersion") int expectedVersion, @JsonProperty("services") Map<String, ServiceAllowlistEntry> services) {
        this.pluginId        = pluginId;
        this.partitionCount  = partitionCount;
        this.expectedVersion = expectedVersion;
        this.services        = copyServices(services);
    }

    public OnboardPlugin(String pluginId, int partitionCount, int expectedVersion) {
        this(pluginId, partitionCount, expectedVersion, null);
    }

    private static Map<String, ServiceAllowlistEntry> copyServices(Map<String, ServiceAllowlistEntry> services) {
        if (services == null || services.isEmpty()) {
            return Collections.emptyMap();
        }
        return Collections.unmodifiableMap(new LinkedHashMap<>(services));
    }

    public String getPluginId() {
        return pluginId;
    }

    public int getPartitionCount() {
        return partitionCount;
    }

    public int getExpectedVersion() {
        return expectedVersion;
    }

    public Map<String, ServiceAllowlistEntry> getServices() {
        return services;
    }
}
