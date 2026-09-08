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

package org.apache.ranger.audit.producer.kafka.partition.constants;

/** Shared constants for the dynamic Kafka partition-plan registry. */
public class PartitionPlanConstants {
    /** Version number of the first XML-seeded initial bootstrap plan in an empty registry. */
    public static final int    INITIAL_PLAN_VERSION              = 1;
    /** {@code updatedBy} value on plans published by {@code PartitionPlanBootstrap}. */
    public static final String BOOTSTRAP_UPDATED_BY              = "bootstrap";
    public static final String PLAN_REGISTRY_CONSUMER_GROUP      = "ranger_audit_partition_plan_registry";
    public static final String PLAN_WATCHER_CONSUMER_GROUP       = "ranger_audit_partition_plan_watcher";

    private PartitionPlanConstants() {
    }
}
