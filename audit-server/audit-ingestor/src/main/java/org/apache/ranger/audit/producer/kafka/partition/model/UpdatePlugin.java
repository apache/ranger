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
import java.util.List;
import java.util.Map;

@JsonAutoDetect(getterVisibility = Visibility.NONE, setterVisibility = Visibility.NONE, fieldVisibility = Visibility.ANY)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class UpdatePlugin implements Serializable {
    private final int expectedVersion;
    private final Integer additionalPartitions;
    private final Map<String, ServiceAllowlistEntry> addServices;
    private final Map<String, ServiceAllowlistEntry> updateServices;
    private final List<String> removeServices;

    @JsonCreator
    public UpdatePlugin(@JsonProperty("expectedVersion") int expectedVersion, @JsonProperty("additionalPartitions") Integer additionalPartitions, @JsonProperty("addServices") Map<String, ServiceAllowlistEntry> addServices, @JsonProperty("updateServices") Map<String, ServiceAllowlistEntry> updateServices, @JsonProperty("removeServices") List<String> removeServices) {
        this.expectedVersion       = expectedVersion;
        this.additionalPartitions  = additionalPartitions;
        this.addServices           = copyServices(addServices);
        this.updateServices        = copyServices(updateServices);
        this.removeServices        = removeServices == null ? Collections.emptyList() : List.copyOf(removeServices);
    }

    private static Map<String, ServiceAllowlistEntry> copyServices(Map<String, ServiceAllowlistEntry> services) {
        if (services == null || services.isEmpty()) {
            return Collections.emptyMap();
        }
        return Collections.unmodifiableMap(new LinkedHashMap<>(services));
    }

    public int getExpectedVersion() {
        return expectedVersion;
    }

    public Integer getAdditionalPartitions() {
        return additionalPartitions;
    }

    public Map<String, ServiceAllowlistEntry> getAddServices() {
        return addServices;
    }

    public Map<String, ServiceAllowlistEntry> getUpdateServices() {
        return updateServices;
    }

    public List<String> getRemoveServices() {
        return removeServices;
    }

    /** True when at least one mutation field is present. */
    public boolean hasMutationDelta() {
        if (additionalPartitions != null && additionalPartitions >= 1) {
            return true;
        }
        if (!addServices.isEmpty()) {
            return true;
        }
        if (!updateServices.isEmpty()) {
            return true;
        }
        return !removeServices.isEmpty();
    }
}
