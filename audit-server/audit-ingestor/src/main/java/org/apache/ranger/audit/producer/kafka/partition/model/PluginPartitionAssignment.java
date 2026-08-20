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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

@JsonAutoDetect(getterVisibility = Visibility.NONE, setterVisibility = Visibility.NONE, fieldVisibility = Visibility.ANY)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class PluginPartitionAssignment implements java.io.Serializable {
    private final List<Integer> partitions;

    @JsonCreator
    public PluginPartitionAssignment(@JsonProperty("partitions") List<Integer> partitions) {
        if (partitions == null || partitions.isEmpty()) {
            this.partitions = Collections.emptyList();
        } else {
            this.partitions = List.copyOf(partitions);
        }
    }

    /** Returns an assignment with no partition IDs (empty buffer). */
    public static PluginPartitionAssignment empty() {
        return new PluginPartitionAssignment(Collections.emptyList());
    }

    /** Builds an assignment from explicit partition IDs. */
    public static PluginPartitionAssignment of(int... partitionIds) {
        List<Integer> ids = new ArrayList<>(partitionIds.length);
        for (int id : partitionIds) {
            ids.add(id);
        }
        return new PluginPartitionAssignment(ids);
    }

    /** Builds a contiguous inclusive partition ID range. */
    public static PluginPartitionAssignment ofRange(int startInclusive, int endInclusive) {
        if (endInclusive < startInclusive) {
            throw new IllegalArgumentException("Invalid partition range: " + startInclusive + "-" + endInclusive);
        }
        List<Integer> ids = new ArrayList<>(endInclusive - startInclusive + 1);
        for (int i = startInclusive; i <= endInclusive; i++) {
            ids.add(i);
        }
        return new PluginPartitionAssignment(ids);
    }

    public List<Integer> getPartitions() {
        return partitions;
    }

    public int size() {
        return partitions.size();
    }

    @Override
    public boolean equals(Object otherPluginPartitionAssignmentObj) {
        if (this == otherPluginPartitionAssignmentObj) {
            return true;
        }
        if (otherPluginPartitionAssignmentObj == null || getClass() != otherPluginPartitionAssignmentObj.getClass()) {
            return false;
        }
        PluginPartitionAssignment otherAssignment = (PluginPartitionAssignment) otherPluginPartitionAssignmentObj;
        return Objects.equals(partitions, otherAssignment.partitions);
    }

    @Override
    public int hashCode() {
        return Objects.hash(partitions);
    }
}
