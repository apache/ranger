/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ranger.audit.partition;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class PartitionPlanRoutingUtilsTest {
    @Test
    public void testToKafkaPartitionIndexUsesZeroBasedMapping() {
        assertEquals(0, PartitionPlanRoutingUtils.toKafkaPartitionIndex(1));
        assertEquals(8, PartitionPlanRoutingUtils.toKafkaPartitionIndex(9));
    }

    @Test
    public void testResolveKafkaPartitionIndexClampsToTopicSize() {
        assertEquals(8, PartitionPlanRoutingUtils.resolveKafkaPartitionIndex(9, 9));
        assertEquals(8, PartitionPlanRoutingUtils.resolveKafkaPartitionIndex(12, 9));
    }

    @Test
    public void testHashToSlotIndexUsesFloorModForMinHashCode() {
        String minHashKey = "polygenelubricants";
        assertEquals(Integer.MIN_VALUE, minHashKey.hashCode());
        assertEquals(2, PartitionPlanRoutingUtils.hashToSlotIndex(minHashKey, 5));
    }
}
