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

package org.apache.ranger.audit.dispatcher.kafka;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Reusable ConsumerRebalanceListener for Kafka dispatcher group re-balancing.
 *
 * This listener handles graceful partition re-balancing by:
 * - Committing pending offsets before partitions are revoked
 * - Updating partition assignments when partitions are assigned
 * - Logging re-balancing events with customizable log prefixes
 *
 * Used by both AuditSolrDispatcher, AuditHDFSDispatcher and any new dispatchers added to the audit serve, ensuring
 * no message duplication during scaling operations.
 */
public class AuditDispatcherRebalanceListener implements ConsumerRebalanceListener {
    private static final Logger LOG = LoggerFactory.getLogger(AuditDispatcherRebalanceListener.class);

    private final String                                 workerId;
    private final String                                 destinationType;
    private final String                                 topicName;
    private final String                                 offsetCommitStrategy;
    private final String                                 dispatcherGroupId;
    private final KafkaConsumer<String, String>          workerDispatcher;
    private final Map<TopicPartition, OffsetAndMetadata> pendingOffsets;
    private final AtomicInteger                          messagesProcessedSinceLastCommit;
    private final AtomicLong                             lastCommitTime;
    private final List<Integer>                          assignedPartitions;

    public AuditDispatcherRebalanceListener(
            String workerId,
            String destinationType,
            String topicName,
            String offsetCommitStrategy,
            String dispatcherGroupId,
            KafkaConsumer<String, String> workerDispatcher,
            Map<TopicPartition, OffsetAndMetadata> pendingOffsets,
            AtomicInteger messagesProcessedSinceLastCommit,
            AtomicLong lastCommitTime,
            List<Integer> assignedPartitions) {
        this.workerId                         = workerId;
        this.destinationType                  = destinationType;
        this.topicName                        = topicName;
        this.offsetCommitStrategy             = offsetCommitStrategy;
        this.dispatcherGroupId                = dispatcherGroupId;
        this.workerDispatcher                 = workerDispatcher;
        this.pendingOffsets                   = pendingOffsets;
        this.messagesProcessedSinceLastCommit = messagesProcessedSinceLastCommit;
        this.lastCommitTime                   = lastCommitTime;
        this.assignedPartitions               = assignedPartitions;
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        LOG.info("[{}-REBALANCE] Worker '{}': Partitions REVOKED: {} (count: {})", destinationType, workerId, partitions, partitions.size());

        // Commit pending offsets before partitions are revoked
        if (!pendingOffsets.isEmpty()) {
            try {
                workerDispatcher.commitSync(pendingOffsets);
                LOG.info("[{}-REBALANCE] Worker '{}': Successfully committed {} pending offsets before rebalance",
                         destinationType, workerId, pendingOffsets.size());
                pendingOffsets.clear();
            } catch (Exception e) {
                LOG.error("[{}-REBALANCE] Worker '{}': Failed to commit offsets during rebalance",
                        destinationType, workerId, e);
            }
        }

        // Reset counters
        messagesProcessedSinceLastCommit.set(0);
        lastCommitTime.set(System.currentTimeMillis());
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        LOG.info("[{}-REBALANCE] Worker '{}': Partitions ASSIGNED: {} (count: {})", destinationType, workerId, partitions, partitions.size());

        // Update assigned partitions list for tracking
        assignedPartitions.clear();
        for (TopicPartition tp : partitions) {
            assignedPartitions.add(tp.partition());
        }

        // Log assignment details
        LOG.info("[{}-DISPATCHER-ASSIGNED] Worker '{}' | Topic: '{}' | Partitions: {} | Offset-Strategy: {} | Dispatcher-Group: {}",
                destinationType, workerId, topicName, assignedPartitions, offsetCommitStrategy, dispatcherGroupId);
    }
}
