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

public final class AuditPartitionPlanConstants {
    public static final int    INITIAL_PLAN_VERSION = 1;
    public static final String DEFAULT_AUDIT_TOPIC  = "ranger_audits";

    /** Default partition slots allocated when a plugin is first promoted from buffer. */
    public static final int DEFAULT_PARTITIONS_PER_PLUGIN = 3;

    /** Admin site: {@code ranger-admin-default-site.xml} / {@code ranger-admin-site.xml}. */
    public static final String PROP_ADMIN_PARTITIONS_PER_PLUGIN = "ranger.admin.audit.partition.plan.partitions.per.plugin";

    /** Per-plugin override prefix, e.g. {@code ...plugin.partition.overrides.hiveServer2}. */
    public static final String PROP_ADMIN_PLUGIN_PARTITION_OVERRIDE_PREFIX = "ranger.admin.audit.partition.plan.plugin.partition.overrides.";

    private AuditPartitionPlanConstants() {
    }
}
