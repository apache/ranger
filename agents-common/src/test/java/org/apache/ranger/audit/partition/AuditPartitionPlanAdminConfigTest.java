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

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class AuditPartitionPlanAdminConfigTest {
    @Test
    public void testDefaultPartitionsPerPlugin() {
        Configuration config = new Configuration(false);
        assertEquals(3, AuditPartitionPlanAdminConfig.resolvePartitionsPerPlugin("hdfs", config));
    }

    @Test
    public void testGlobalDefaultFromSiteProperty() {
        Configuration config = new Configuration(false);
        config.set(AuditPartitionPlanConstants.PROP_ADMIN_PARTITIONS_PER_PLUGIN, "6");
        assertEquals(6, AuditPartitionPlanAdminConfig.resolvePartitionsPerPlugin("hdfs", config));
    }

    @Test
    public void testPerPluginOverride() {
        Configuration config = new Configuration(false);
        config.set(AuditPartitionPlanConstants.PROP_ADMIN_PARTITIONS_PER_PLUGIN, "3");
        config.set(AuditPartitionPlanConstants.PROP_ADMIN_PLUGIN_PARTITION_OVERRIDE_PREFIX + "hiveServer2", "9");
        assertEquals(3, AuditPartitionPlanAdminConfig.resolvePartitionsPerPlugin("hdfs", config));
        assertEquals(9, AuditPartitionPlanAdminConfig.resolvePartitionsPerPlugin("hiveServer2", config));
    }
}
