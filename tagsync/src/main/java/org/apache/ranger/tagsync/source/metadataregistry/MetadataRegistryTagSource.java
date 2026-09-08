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

package org.apache.ranger.tagsync.source.metadataregistry;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.ranger.authorization.utils.JsonUtils;
import org.apache.ranger.plugin.util.ServiceTags;
import org.apache.ranger.tagsync.model.AbstractTagSource;
import org.apache.ranger.tagsync.process.TagSyncConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Kafka tag source for Metadata Registry {@code udf.governance} notifications.
 */
public class MetadataRegistryTagSource extends AbstractTagSource {
    private static final Logger LOG = LoggerFactory.getLogger(MetadataRegistryTagSource.class);

    public static final String TAGSYNC_METADATAREGISTRY_KAFKA_BOOTSTRAP =
            "ranger.tagsync.source.metadataregistry.kafka.bootstrap.servers";
    public static final String TAGSYNC_METADATAREGISTRY_KAFKA_TOPIC =
            "ranger.tagsync.source.metadataregistry.kafka.topic";
    public static final String TAGSYNC_METADATAREGISTRY_KAFKA_GROUP =
            "ranger.tagsync.source.metadataregistry.kafka.group.id";

    private static final String DEFAULT_TOPIC      = "udf.governance";
    private static final String DEFAULT_GROUP      = "ranger_metadataregistry_consumer";
    private static final int    MAX_WAIT_TIME_MS   = 1000;

    private int              maxBatchSize;
    private ConsumerRunnable consumerTask;
    private Thread           consumerThread;
    private Properties       tagSyncProperties;

    @Override
    public boolean initialize(Properties properties) {
        LOG.debug("==> MetadataRegistryTagSource.initialize()");

        tagSyncProperties = properties;
        boolean ret = MetadataRegistryResourceMapperUtil.initializeResourceMappers(properties);
        if (ret) {
            if (StringUtils.isBlank(properties.getProperty(TAGSYNC_METADATAREGISTRY_KAFKA_BOOTSTRAP))) {
                LOG.error("missing value for mandatory property '{}'", TAGSYNC_METADATAREGISTRY_KAFKA_BOOTSTRAP);
                ret = false;
            } else {
                maxBatchSize  = TagSyncConfig.getSinkMaxBatchSize(properties);
                consumerTask  = new ConsumerRunnable(properties);
            }
        }

        LOG.debug("<== MetadataRegistryTagSource.initialize(), result={}", ret);
        return ret;
    }

    @Override
    public boolean start() {
        LOG.debug("==> MetadataRegistryTagSource.start()");

        boolean ret = false;
        if (consumerTask != null) {
            consumerThread = new Thread(consumerTask);
            consumerThread.setDaemon(true);
            consumerThread.start();
            ret = true;
        } else {
            LOG.error("No Metadata Registry consumer task configured");
        }

        LOG.debug("<== MetadataRegistryTagSource.start(): ret={}", ret);
        return ret;
    }

    @Override
    public void stop() {
        if (consumerThread != null && consumerThread.isAlive()) {
            consumerThread.interrupt();
        }
        if (consumerTask != null) {
            consumerTask.close();
        }
    }

    private class ConsumerRunnable implements Runnable {
        private final KafkaConsumer<String, String> consumer;
        private final List<MetadataRegistryEntityWithTags> entitiesWithTags = new ArrayList<>();
        private final List<ConsumerRecord<String, String>> messages         = new ArrayList<>();
        private       ConsumerRecord<String, String>       lastUnhandledMessage;
        private       boolean                              isHandlingDeleteOps;

        private ConsumerRunnable(Properties properties) {
            Properties consumerProps = new Properties();
            consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                    properties.getProperty(TAGSYNC_METADATAREGISTRY_KAFKA_BOOTSTRAP));
            consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG,
                    properties.getProperty(TAGSYNC_METADATAREGISTRY_KAFKA_GROUP, DEFAULT_GROUP));
            consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
            consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            this.consumer = new KafkaConsumer<>(consumerProps);
            String topic = properties.getProperty(TAGSYNC_METADATAREGISTRY_KAFKA_TOPIC, DEFAULT_TOPIC);
            consumer.subscribe(Collections.singletonList(topic));
        }

        private void close() {
            consumer.close();
        }

        @Override
        public void run() {
            LOG.debug("==> MetadataRegistryTagSource.ConsumerRunnable.run()");

            while (!Thread.currentThread().isInterrupted()) {
                if (!TagSyncConfig.isTagSyncServiceActive()) {
                    sleepPassive();
                    continue;
                }
                try {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(MAX_WAIT_TIME_MS));
                    if (records.isEmpty()) {
                        if (CollectionUtils.isNotEmpty(entitiesWithTags)) {
                            buildAndUploadServiceTags();
                        }
                    } else {
                        for (ConsumerRecord<String, String> record : records) {
                            MetadataRegistryNotificationWrapper wrapper =
                                    MetadataRegistryNotificationParser.parse(record.value());
                            if (wrapper == null) {
                                lastUnhandledMessage = record;
                                continue;
                            }
                            if (MetadataRegistryNotificationMapper.isNotificationHandled(wrapper)) {
                                if ((wrapper.isEntityDeleteOp() && !isHandlingDeleteOps)
                                        || (!wrapper.isEntityDeleteOp() && isHandlingDeleteOps)) {
                                    if (CollectionUtils.isNotEmpty(entitiesWithTags)) {
                                        buildAndUploadServiceTags();
                                    }
                                    isHandlingDeleteOps = wrapper.isEntityDeleteOp();
                                }
                                entitiesWithTags.add(new MetadataRegistryEntityWithTags(wrapper));
                                messages.add(record);
                            } else {
                                MetadataRegistryNotificationMapper.logUnhandledNotification(wrapper);
                                lastUnhandledMessage = record;
                            }
                        }
                        if (CollectionUtils.isNotEmpty(entitiesWithTags) && entitiesWithTags.size() >= maxBatchSize) {
                            buildAndUploadServiceTags();
                        }
                    }
                    if (lastUnhandledMessage != null) {
                        commitToKafka(lastUnhandledMessage);
                        lastUnhandledMessage = null;
                    }
                } catch (Exception exception) {
                    LOG.error("Metadata Registry tag source consumer error", exception);
                    sleepQuietly(100);
                }
            }
        }

        private void buildAndUploadServiceTags() throws Exception {
            if (CollectionUtils.isEmpty(entitiesWithTags) || CollectionUtils.isEmpty(messages)) {
                return;
            }
            Map<String, ServiceTags> serviceTagsMap =
                    MetadataRegistryNotificationMapper.processEntities(entitiesWithTags);
            if (MapUtils.isNotEmpty(serviceTagsMap)) {
                for (Map.Entry<String, ServiceTags> entry : serviceTagsMap.entrySet()) {
                    if (isHandlingDeleteOps) {
                        entry.getValue().setOp(ServiceTags.OP_DELETE);
                        entry.getValue().setTagDefinitions(Collections.emptyMap());
                        entry.getValue().setTags(Collections.emptyMap());
                    } else {
                        entry.getValue().setOp(ServiceTags.OP_ADD_OR_UPDATE);
                    }
                    LOG.debug("Metadata Registry serviceTags={}", JsonUtils.objectToJson(entry.getValue()));
                    updateSink(entry.getValue());
                }
            }
            ConsumerRecord<String, String> latest = messages.get(messages.size() - 1);
            commitToKafka(latest);
            entitiesWithTags.clear();
            messages.clear();
        }

        private void commitToKafka(ConsumerRecord<String, String> record) {
            TopicPartition partition = new TopicPartition(record.topic(), record.partition());
            consumer.commitSync(Collections.singletonMap(
                    partition,
                    new OffsetAndMetadata(record.offset() + 1)));
        }

        private void sleepPassive() {
            try {
                Thread.sleep(TagSyncConfig.getTagSyncHAPassiveSleepInterval());
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
            }
        }

        private void sleepQuietly(long millis) {
            try {
                Thread.sleep(millis);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
