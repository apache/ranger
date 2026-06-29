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

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.ranger.audit.producer.kafka.partition.constants.PartitionPlanConstants;
import org.apache.ranger.audit.producer.kafka.partition.model.PartitionPlan;
import org.apache.ranger.audit.utils.AuditMessageQueueUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

/** Background thread: load plan from Kafka (or XML-seeded initial bootstrap plan), then incrementally refresh in-memory plan. */
public class PartitionPlanWatcher implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(PartitionPlanWatcher.class);

    private final Properties props;
    private final String propPrefix;
    private final String auditTopicKey;
    private final PartitionPlanHolder partitionPlanHolder;
    private final int refreshIntervalMs;
    private final int consumerPollTimeoutMs;
    private final String planTopic;

    private PartitionPlanRegistry registry;
    private KafkaConsumer<String, String> consumer;
    private PartitionPlanUpdateApplier planUpdateApplier;
    private Thread watcherThread;
    private volatile boolean running;

    public PartitionPlanWatcher(Properties props, String propPrefix, String auditTopicKey, PartitionPlanHolder holder) {
        this.props             = props;
        this.propPrefix        = propPrefix;
        this.auditTopicKey     = auditTopicKey;
        this.partitionPlanHolder            = holder != null ? holder : PartitionPlanHolder.getInstance();
        this.refreshIntervalMs     = PartitionPlanKafkaConfig.resolveRefreshIntervalMs(props, propPrefix);
        this.consumerPollTimeoutMs = PartitionPlanKafkaConfig.resolveConsumerPollTimeoutMs(props, propPrefix);
        this.planTopic             = PartitionPlanKafkaConfig.resolvePlanTopicName(props, propPrefix);
        this.planUpdateApplier     = new PartitionPlanUpdateApplier(props, auditTopicKey, this.partitionPlanHolder, this::resolveAuditTopicPartitionCount);
    }

    /** Blocking startup: empty-registry bootstrap, install plan, then start background refresh. */
    public void startBlocking() throws Exception {
        LOG.info("Starting partition plan watcher for audit topic '{}' (plan topic '{}', refresh {} ms, consumer poll {} ms)", auditTopicKey, planTopic, refreshIntervalMs, consumerPollTimeoutMs);
        registry = new KafkaPartitionPlanRegistry(props, propPrefix);
        Map<String, Object> producerConfig = buildProducerConfigMap();
        PartitionPlan plan = PartitionPlanBootstrap.bootstrapIfEmpty(registry, auditTopicKey, producerConfig);
        plan = ServiceAllowlistBootstrap.mergeSiteXmlAllowlistsWhenPlanServicesMissing(plan, props);
        int kafkaPartitionCount = resolveAuditTopicPartitionCount();
        partitionPlanHolder.install(plan, kafkaPartitionCount);
        openConsumerAtBeginning();
        pollIncrementalUpdates();
        running       = true;
        watcherThread = new Thread(this, "PartitionPlanWatcher");
        watcherThread.setDaemon(true);
        watcherThread.start();
        LOG.info("Partition plan watcher ready: version={}, auditTopicPartitions={}", plan.getVersion(), kafkaPartitionCount);
    }

    /** Stops the background refresh thread and closes Kafka clients. */
    public void stop() {
        running = false;
        if (watcherThread != null) {
            watcherThread.interrupt();
            try {
                watcherThread.join(5000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            watcherThread = null;
        }
        closeConsumer();
        if (registry != null) {
            registry.close();
            registry = null;
        }
        LOG.info("Partition plan watcher stopped");
    }

    @Override
    public void run() {
        while (running) {
            try {
                pollIncrementalUpdates();
                Thread.sleep(refreshIntervalMs);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                LOG.error("Partition plan refresh failed; keeping last known good plan version {}", partitionPlanHolder.getLastInstalledVersion(), e);
            }
        }
    }

    /** Drains new compacted plan records and installs any newer version into memory. */
    private void pollIncrementalUpdates() {
        if (consumer == null) {
            return;
        }
        ConsumerRecords<String, String> records;
        do {
            records = consumer.poll(Duration.ofMillis(consumerPollTimeoutMs));
            for (ConsumerRecord<String, String> record : records) {
                planUpdateApplier.applyRecordIfNewer(record);
            }
        } while (!records.isEmpty());
    }

    private void openConsumerAtBeginning() throws Exception {
        consumer = new KafkaConsumer<>(PartitionPlanKafkaConfig.consumerConfig(props, propPrefix, PartitionPlanConstants.PLAN_WATCHER_CONSUMER_GROUP));
        TopicPartition partition = new TopicPartition(planTopic, 0);
        consumer.assign(Collections.singletonList(partition));
        consumer.seekToBeginning(Collections.singletonList(partition));
    }

    private void closeConsumer() {
        if (consumer != null) {
            consumer.close();
            consumer = null;
        }
    }

    private int resolveAuditTopicPartitionCount() throws Exception {
        try {
            Map<String, Object> adminConfig = AuditMessageQueueUtils.buildAdminClientConfig(props, propPrefix);
            try (AdminClient admin = AdminClient.create(adminConfig)) {
                DescribeTopicsResult describeTopicsResult = admin.describeTopics(Collections.singletonList(auditTopicKey));
                TopicDescription topicDescription = describeTopicsResult.values().get(auditTopicKey).get();
                return topicDescription.partitions().size();
            }
        } catch (Exception e) {
            LOG.error("Failed to resolve partition count for audit topic '{}'", auditTopicKey, e);
            throw e;
        }
    }

    private Map<String, Object> buildProducerConfigMap() {
        Map<String, Object> producerConfig = new LinkedHashMap<>();
        String prefix = propPrefix + ".";
        for (String key : props.stringPropertyNames()) {
            if (key.startsWith(prefix)) {
                producerConfig.put(key, props.getProperty(key));
            }
        }
        return producerConfig;
    }
}
