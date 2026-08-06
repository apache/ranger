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

package org.apache.ranger.audit.partition.model;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class PartitionPlanJsonTest {
    @Test
    public void testRoundTripSeedJson() {
        String seedJson = "{\"version\":1,\"topic\":\"ranger_audits\",\"topicPartitionCount\":9,\"plugins\":{},\"buffer\":{\"partitions\":[1,2,3,4,5,6,7,8,9]}}";
        PartitionPlan plan = PartitionPlan.fromJson(seedJson);

        assertEquals("ranger_audits", plan.getTopic());
        assertEquals(1, plan.getVersion());
        assertEquals(9, plan.getTopicPartitionCount());
        assertEquals(0, plan.getPlugins().size());
        assertEquals(9, plan.getBuffer().getPartitions().size());

        String roundTrip = plan.toJson();
        assertNotNull(roundTrip);
        PartitionPlan parsedAgain = PartitionPlan.fromJson(roundTrip);
        assertEquals(plan, parsedAgain);
    }

    @Test
    public void testRoundTripOnboardedPluginJson() {
        String json = "{\"version\":2,\"topic\":\"ranger_audits\",\"topicPartitionCount\":9,"
                + "\"plugins\":{\"hiveServer2\":{\"partitions\":[1,2,3,4,5,6],\"services\":[\"dev_hive\",\"prod_hive\"]}},"
                + "\"buffer\":{\"partitions\":[7,8,9]}}";
        PartitionPlan plan = PartitionPlan.fromJson(json);

        assertEquals(2, plan.getVersion());
        assertEquals(2, plan.getPlugins().get("hiveServer2").getServices().size());
        assertEquals(6, plan.getPlugins().get("hiveServer2").getPartitions().size());
    }

    @Test
    public void testRoundTripServiceAllowedUsersJson() {
        String json = "{\"version\":3,\"topic\":\"ranger_audits\",\"topicPartitionCount\":9,"
                + "\"plugins\":{},\"buffer\":{\"partitions\":[1,2,3,4,5,6,7,8,9]},"
                + "\"serviceAllowedUsers\":{\"dev_hive\":[\"hive\"],\"dev_ozone\":[\"om\"]}}";
        PartitionPlan plan = PartitionPlan.fromJson(json);

        assertEquals(3, plan.getVersion());
        assertIterableEquals(List.of("hive"), plan.getServiceAllowedUsers().get("dev_hive"));
        assertIterableEquals(List.of("om"), plan.getServiceAllowedUsers().get("dev_ozone"));

        PartitionPlan parsedAgain = PartitionPlan.fromJson(plan.toJson());
        assertEquals(plan, parsedAgain);
    }
}
