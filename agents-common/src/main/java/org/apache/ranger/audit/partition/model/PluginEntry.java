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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;

@JsonAutoDetect(getterVisibility = Visibility.NONE, setterVisibility = Visibility.NONE, fieldVisibility = Visibility.ANY)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class PluginEntry implements Serializable {
    private static final long serialVersionUID = 1L;

    private final List<Integer> partitions;
    private final List<String>  services;

    @JsonCreator
    public PluginEntry(@JsonProperty("partitions") List<Integer> partitions, @JsonProperty("services") List<String> services) {
        this.partitions = copyPartitions(partitions);
        this.services   = copyServices(services);
    }

    public static PluginEntry ofPartitions(int... partitionIds) {
        List<Integer> ids = new ArrayList<>(partitionIds.length);
        for (int id : partitionIds) {
            ids.add(id);
        }
        return new PluginEntry(ids, Collections.emptyList());
    }

    public static PluginEntry empty() {
        return new PluginEntry(Collections.emptyList(), Collections.emptyList());
    }

    public List<Integer> getPartitions() {
        return partitions;
    }

    public List<String> getServices() {
        return services;
    }

    public PluginEntry withPartitions(List<Integer> newPartitions) {
        return new PluginEntry(newPartitions, services);
    }

    public PluginEntry withServices(List<String> newServices) {
        return new PluginEntry(partitions, newServices);
    }

    public PluginEntry addService(String serviceName) {
        if (serviceName == null || serviceName.isBlank()) {
            return this;
        }
        LinkedHashSet<String> merged = new LinkedHashSet<>(services);
        merged.add(serviceName.trim());
        return new PluginEntry(partitions, List.copyOf(merged));
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        PluginEntry other = (PluginEntry) obj;
        return Objects.equals(partitions, other.partitions) && Objects.equals(services, other.services);
    }

    @Override
    public int hashCode() {
        return Objects.hash(partitions, services);
    }

    private static List<Integer> copyPartitions(List<Integer> partitions) {
        if (partitions == null || partitions.isEmpty()) {
            return Collections.emptyList();
        }
        return List.copyOf(partitions);
    }

    private static List<String> copyServices(List<String> services) {
        if (services == null || services.isEmpty()) {
            return Collections.emptyList();
        }
        LinkedHashSet<String> unique = new LinkedHashSet<>();
        for (String service : services) {
            if (service != null && !service.isBlank()) {
                unique.add(service.trim());
            }
        }
        return List.copyOf(unique);
    }
}
