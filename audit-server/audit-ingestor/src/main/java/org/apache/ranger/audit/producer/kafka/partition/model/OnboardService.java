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
import java.util.List;

@JsonAutoDetect(getterVisibility = Visibility.NONE, setterVisibility = Visibility.NONE, fieldVisibility = Visibility.ANY)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class OnboardService implements Serializable {
    private final String serviceName;
    private final String pluginId;
    private final int partitionCount;
    private final List<String> allowedUsers;
    private final int expectedVersion;

    @JsonCreator
    public OnboardService(@JsonProperty("serviceName") String serviceName, @JsonProperty("pluginId") String pluginId, @JsonProperty("partitionCount") int partitionCount, @JsonProperty("allowedUsers") List<String> allowedUsers, @JsonProperty("expectedVersion") int expectedVersion) {
        this.serviceName             = serviceName;
        this.pluginId         = pluginId;
        this.partitionCount   = partitionCount;
        this.allowedUsers     = allowedUsers == null ? Collections.emptyList() : List.copyOf(allowedUsers);
        this.expectedVersion  = expectedVersion;
    }

    public String getServiceName() {
        return serviceName;
    }

    public String getPluginId() {
        return pluginId;
    }

    public int getPartitionCount() {
        return partitionCount;
    }

    public List<String> getAllowedUsers() {
        return allowedUsers;
    }

    public int getExpectedVersion() {
        return expectedVersion;
    }
}
