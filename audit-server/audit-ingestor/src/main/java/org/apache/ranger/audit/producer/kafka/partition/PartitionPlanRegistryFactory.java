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

package org.apache.ranger.audit.producer.kafka.partition;

import java.util.Properties;

/** Opens a Kafka-backed {@link PartitionPlanRegistry} for REST mutations. */
public class PartitionPlanRegistryFactory {
    /** Creates a registry connected to the compacted plan topic. */
    public PartitionPlanRegistry open(Properties props, String propPrefix) throws Exception {
        return new KafkaPartitionPlanRegistry(props, propPrefix);
    }
}
