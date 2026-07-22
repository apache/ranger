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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ranger.audit.producer.kafka.partition;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.ranger.audit.producer.kafka.partition.model.PartitionPlan;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/** Applies compacted partition-plan Kafka records into {@link PartitionPlanHolder}. */
public class PartitionPlanUpdateApplier {
    private static final Logger LOG = LoggerFactory.getLogger(PartitionPlanUpdateApplier.class);

    @FunctionalInterface
    public interface AuditTopicPartitionCountSupplier {
        int getPartitionCount() throws Exception;
    }

    private final Properties props;
    private final String auditTopicKey;
    private final PartitionPlanHolder holder;
    private final AuditTopicPartitionCountSupplier partitionCountSupplier;

    public PartitionPlanUpdateApplier(
            Properties props,
            String auditTopicKey,
            PartitionPlanHolder holder,
            AuditTopicPartitionCountSupplier partitionCountSupplier) {
        this.props                   = props;
        this.auditTopicKey           = auditTopicKey;
        this.holder                  = holder != null ? holder : PartitionPlanHolder.getInstance();
        this.partitionCountSupplier  = partitionCountSupplier;
    }

    /** Installs a plan record when its version is newer than the in-memory copy. */
    public void applyRecordIfNewer(ConsumerRecord<String, String> record) {
        if (!auditTopicKey.equals(record.key())) {
            return;
        }
        try {
            PartitionPlan plan = PartitionPlan.fromJson(record.value());
            plan = ServiceAllowlistBootstrap.mergeSiteXmlAllowlistsWhenPlanServicesMissing(plan, props);
            if (plan.getVersion() <= holder.getLastInstalledVersion()) {
                return;
            }
            holder.install(plan, partitionCountSupplier.getPartitionCount());
            LOG.info("Installed partition plan version {} from Kafka offset {}", plan.getVersion(), record.offset());
        } catch (Exception e) {
            LOG.error("Ignoring invalid partition plan at offset {} for audit topic '{}'", record.offset(), auditTopicKey, e);
        }
    }
}
