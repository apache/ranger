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

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.ranger.audit.producer.kafka.partition.constants.PartitionPlanConstants;
import org.apache.ranger.audit.producer.kafka.partition.exception.PartitionPlanException;
import org.apache.ranger.audit.producer.kafka.partition.model.PartitionPlan;
import org.apache.ranger.audit.utils.AuditMessageQueueUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

/** Kafka compacted topic implementation of {@link PartitionPlanRegistry}. */
public class KafkaPartitionPlanRegistry implements PartitionPlanRegistry {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaPartitionPlanRegistry.class);

    private final Properties props;
    private final String propPrefix;
    private final String planTopic;
    private final int consumerPollTimeoutMs;
    private final KafkaProducer<String, String> producer;

    public KafkaPartitionPlanRegistry(Properties props, String propPrefix) throws Exception {
        this.props                 = props;
        this.propPrefix            = propPrefix;
        this.planTopic             = PartitionPlanKafkaConfig.resolvePlanTopicName(props, propPrefix);
        this.consumerPollTimeoutMs = PartitionPlanKafkaConfig.resolveConsumerPollTimeoutMs(props, propPrefix);
        AuditMessageQueueUtils.createPartitionPlanTopicIfNotExists(props, propPrefix);
        this.producer = new KafkaProducer<>(PartitionPlanKafkaConfig.producerConfig(props, propPrefix));
        LOG.info("Kafka partition plan registry ready for topic '{}'", planTopic);
    }

    /** Reads the latest compacted value for the audit topic key from partition 0. */
    @Override
    public PartitionPlan readPlan(String auditTopicKey) {
        requireAuditTopicKey(auditTopicKey);
        try (KafkaConsumer<String, String> consumer = openConsumer()) {
            return readLatestCompactedPlan(consumer, auditTopicKey);
        } catch (PartitionPlanException e) {
            throw e;
        } catch (Exception e) {
            LOG.error("Failed to read partition plan from Kafka topic '{}' for audit topic key '{}'", planTopic, auditTopicKey, e);
            throw new PartitionPlanException("Failed to read partition plan from Kafka topic '" + planTopic + "'", e);
        }
    }

    /** Publishes a new plan version to the compacted topic (key = audit topic name). */
    @Override
    public void writePlan(String auditTopicKey, PartitionPlan plan) {
        requireAuditTopicKey(auditTopicKey);
        if (plan == null) {
            throw new PartitionPlanException("Partition plan is required");
        }
        PartitionPlanValidator.validate(plan);
        try {
            producer.send(new ProducerRecord<>(planTopic, auditTopicKey, plan.toJson())).get();
            LOG.info("Wrote partition plan version {} for audit topic '{}' to '{}'", plan.getVersion(), auditTopicKey, planTopic);
        } catch (Exception e) {
            LOG.error("Failed to write partition plan version {} to Kafka topic '{}' for audit topic key '{}'", plan.getVersion(), planTopic, auditTopicKey, e);
            throw new PartitionPlanException("Failed to write partition plan to Kafka topic '" + planTopic + "'", e);
        }
    }

    @Override
    public void close() {
        producer.flush();
        producer.close();
    }

    private KafkaConsumer<String, String> openConsumer() throws Exception {
        return new KafkaConsumer<>(PartitionPlanKafkaConfig.consumerConfig(props, propPrefix, PartitionPlanConstants.PLAN_REGISTRY_CONSUMER_GROUP));
    }

    private PartitionPlan readLatestCompactedPlan(KafkaConsumer<String, String> consumer, String auditTopicKey) {
        TopicPartition partition = new TopicPartition(planTopic, 0);
        consumer.assign(Collections.singletonList(partition));
        consumer.seekToBeginning(Collections.singletonList(partition));

        PartitionPlan latest = null;
        long deadlineMs = System.currentTimeMillis() + Math.max(consumerPollTimeoutMs * 20L, 10_000L);

        while (System.currentTimeMillis() < deadlineMs) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(consumerPollTimeoutMs));
            for (ConsumerRecord<String, String> record : records) {
                if (auditTopicKey.equals(record.key())) {
                    latest = PartitionPlan.fromJson(record.value());
                }
            }
            if (records.isEmpty()) {
                long endOffset = consumer.endOffsets(Collections.singletonList(partition)).get(partition);
                long position  = consumer.position(partition);
                if (position >= endOffset) {
                    if (latest != null) {
                        break;
                    }
                    // Producer write may not be visible at log end yet (mandatory read-back path).
                }
            }
        }
        return latest;
    }

    private static void requireAuditTopicKey(String auditTopicKey) {
        if (StringUtils.isBlank(auditTopicKey)) {
            throw new PartitionPlanException("auditTopicKey is required");
        }
    }
}
