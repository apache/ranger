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
public class PromotePlugin implements Serializable {
    private final String pluginId;
    private final int partitionCount;
    private final int expectedVersion;
    private final String repo;
    private final List<String> allowedUsers;

    @JsonCreator
    public PromotePlugin(@JsonProperty("pluginId") String pluginId, @JsonProperty("partitionCount") int partitionCount, @JsonProperty("expectedVersion") int expectedVersion, @JsonProperty("repo") String repo, @JsonProperty("allowedUsers") List<String> allowedUsers) {
        this.pluginId         = pluginId;
        this.partitionCount   = partitionCount;
        this.expectedVersion  = expectedVersion;
        this.repo             = repo;
        this.allowedUsers     = allowedUsers == null ? Collections.emptyList() : List.copyOf(allowedUsers);
    }

    public PromotePlugin(String pluginId, int partitionCount, int expectedVersion) {
        this(pluginId, partitionCount, expectedVersion, null, null);
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

    public String getRepo() {
        return repo;
    }

    public List<String> getAllowedUsers() {
        return allowedUsers;
    }
}
