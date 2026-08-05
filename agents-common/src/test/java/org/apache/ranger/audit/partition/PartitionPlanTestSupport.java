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

import org.apache.ranger.audit.partition.model.BufferEntry;
import org.apache.ranger.audit.partition.model.PartitionPlan;
import org.apache.ranger.audit.partition.model.PluginEntry;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

final class PartitionPlanTestSupport {
    private PartitionPlanTestSupport() {
    }

    static PartitionPlan seedPlan() {
        return PartitionPlan.builder()
                .topic(AuditPartitionPlanConstants.DEFAULT_AUDIT_TOPIC)
                .version(AuditPartitionPlanConstants.INITIAL_PLAN_VERSION)
                .topicPartitionCount(9)
                .buffer(new BufferEntry(partitionRange(1, 9)))
                .build();
    }

    static PartitionPlan preAssignedPlan() {
        Map<String, PluginEntry> plugins = new LinkedHashMap<>();
        plugins.put("hdfs", PluginEntry.ofPartitions(1, 2, 3));
        plugins.put("hiveServer2", PluginEntry.ofPartitions(4, 5, 6));
        return PartitionPlan.builder()
                .topic(AuditPartitionPlanConstants.DEFAULT_AUDIT_TOPIC)
                .version(AuditPartitionPlanConstants.INITIAL_PLAN_VERSION)
                .topicPartitionCount(9)
                .plugins(plugins)
                .buffer(new BufferEntry(partitionRange(7, 9)))
                .build();
    }

    private static List<Integer> partitionRange(int startInclusive, int endInclusive) {
        List<Integer> ids = new ArrayList<>();
        for (int id = startInclusive; id <= endInclusive; id++) {
            ids.add(id);
        }
        return ids;
    }
}
