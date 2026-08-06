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

/** Converts Admin-managed plan partition ids to Kafka producer partition indices. */
public final class PartitionPlanRoutingUtils {
    private PartitionPlanRoutingUtils() {
    }

    /**
     * Plan partition ids are 1-based logical ids ({@code 1..topicPartitionCount}).
     * Kafka partition indices are 0-based.
     */
    public static int toKafkaPartitionIndex(int plannedPartitionId) {
        if (plannedPartitionId < 1) {
            return 0;
        }
        return plannedPartitionId - 1;
    }

    /**
     * Returns a non-negative slot index in {@code [0, slotCount)} for hash-based buffer routing.
     * Uses {@link Math#floorMod(int, int)} so {@code Integer.MIN_VALUE} hash codes are safe.
     */
    public static int hashToSlotIndex(String key, int slotCount) {
        if (slotCount <= 0) {
            return 0;
        }
        if (StringUtils.isBlank(key)) {
            return 0;
        }
        return Math.floorMod(key.hashCode(), slotCount);
    }

    /**
     * Returns the Kafka partition index for a planned id, clamped to the effective topic size when metadata lags.
     */
    public static int resolveKafkaPartitionIndex(int plannedPartitionId, int effectiveTopicPartitionCount) {
        if (effectiveTopicPartitionCount <= 0) {
            return 0;
        }
        int kafkaIndex = toKafkaPartitionIndex(plannedPartitionId);
        if (kafkaIndex < 0) {
            return 0;
        }
        if (kafkaIndex >= effectiveTopicPartitionCount) {
            return effectiveTopicPartitionCount - 1;
        }
        return kafkaIndex;
    }
}
