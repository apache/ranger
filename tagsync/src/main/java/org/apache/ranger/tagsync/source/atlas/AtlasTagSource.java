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

package org.apache.ranger.tagsync.source.atlas;

import org.apache.atlas.kafka.AtlasKafkaMessage;
import org.apache.atlas.kafka.NotificationProvider;
import org.apache.atlas.model.notification.EntityNotification;
import org.apache.atlas.notification.NotificationConsumer;
import org.apache.atlas.notification.NotificationInterface;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.kafka.common.TopicPartition;
import org.apache.ranger.authorization.utils.JsonUtils;
import org.apache.ranger.plugin.util.ServiceTags;
import org.apache.ranger.tagsync.model.AbstractTagSource;
import org.apache.ranger.tagsync.process.TagSyncConfig;
import org.apache.ranger.tagsync.source.atlasrest.RangerAtlasEntityWithTags;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class AtlasTagSource extends AbstractTagSource {
    private static final Logger LOG = LoggerFactory.getLogger(AtlasTagSource.class);

    public static final String TAGSYNC_ATLAS_PROPERTIES_FILE_NAME = "atlas-application.properties";
    public static final String TAGSYNC_ATLAS_KAFKA_ENDPOINTS    = "atlas.kafka.bootstrap.servers";
    public static final String TAGSYNC_ATLAS_ZOOKEEPER_ENDPOINT = "atlas.kafka.zookeeper.connect";
    public static final String TAGSYNC_ATLAS_CONSUMER_GROUP     = "atlas.kafka.entities.group.id";
    public static final int MAX_WAIT_TIME_IN_MILLIS = 1000;
    private int maxBatchSize;

    private ConsumerRunnable consumerTask;
    private Thread           myThread;

    @Override
    public boolean initialize(Properties properties) {
        LOG.debug("==> AtlasTagSource.initialize()");

        Properties atlasProperties = new Properties();

        boolean ret = AtlasResourceMapperUtil.initializeAtlasResourceMappers(properties);

        if (ret) {
            InputStream inputStream = getClass().getClassLoader().getResourceAsStream(TAGSYNC_ATLAS_PROPERTIES_FILE_NAME);

            if (inputStream != null) {
                try {
                    atlasProperties.load(inputStream);
                } catch (Exception exception) {
                    ret = false;
                    LOG.error("Cannot load Atlas application properties file, file-name: {}", TAGSYNC_ATLAS_PROPERTIES_FILE_NAME, exception);
                } finally {
                    try {
                        inputStream.close();
                    } catch (IOException ioException) {
                        LOG.error("Cannot close Atlas application properties file, file-name: {}", TAGSYNC_ATLAS_PROPERTIES_FILE_NAME, ioException);
                    }
                }
            } else {
                ret = false;
                LOG.error("Cannot find Atlas application properties file");
            }
        }

        if (ret) {
            if (StringUtils.isBlank(atlasProperties.getProperty(TAGSYNC_ATLAS_KAFKA_ENDPOINTS))) {
                ret = false;
                LOG.error("Value of property '{}' is not specified!", TAGSYNC_ATLAS_KAFKA_ENDPOINTS);
            }
            if (StringUtils.isBlank(atlasProperties.getProperty(TAGSYNC_ATLAS_ZOOKEEPER_ENDPOINT))) {
                ret = false;
                LOG.error("Value of property '{}' is not specified!", TAGSYNC_ATLAS_ZOOKEEPER_ENDPOINT);
            }
            if (StringUtils.isBlank(atlasProperties.getProperty(TAGSYNC_ATLAS_CONSUMER_GROUP))) {
                ret = false;
                LOG.error("Value of property '{}' is not specified!", TAGSYNC_ATLAS_CONSUMER_GROUP);
            }
        }

        if (ret) {
            NotificationInterface                          notification = NotificationProvider.get();
            List<NotificationConsumer<EntityNotification>> iterators    = notification.createConsumers(NotificationInterface.NotificationType.ENTITIES, 1);

            consumerTask = new ConsumerRunnable(iterators.get(0));
        }

        maxBatchSize = TagSyncConfig.getSinkMaxBatchSize(properties);

        LOG.debug("<== AtlasTagSource.initialize(), result={}", ret);

        return ret;
    }

    @Override
    public boolean start() {
        LOG.debug("==> AtlasTagSource.start()");

        if (consumerTask == null) {
            LOG.error("No consumerTask!!!");
        } else {
            myThread = new Thread(consumerTask);
            myThread.setDaemon(true);
            myThread.start();
        }
        LOG.debug("<== AtlasTagSource.start()");
        return myThread != null;
    }

    @Override
    public void stop() {
        if (myThread != null && myThread.isAlive()) {
            myThread.interrupt();
        }
    }

    private static String getPrintableEntityNotification(EntityNotificationWrapper notification) {
        StringBuilder sb = new StringBuilder();

        sb.append("{ Notification-Type: ").append(notification.getOpType()).append(", ");
        RangerAtlasEntityWithTags entityWithTags = new RangerAtlasEntityWithTags(notification);
        sb.append(entityWithTags);

        sb.append("}");
        return sb.toString();
    }

    private class ConsumerRunnable implements Runnable {
        private final NotificationConsumer<EntityNotification> consumer;

        private final List<RangerAtlasEntityWithTags>             atlasEntitiesWithTags = new ArrayList<>();
        private final List<AtlasKafkaMessage<EntityNotification>> messages              = new ArrayList<>();
        private       AtlasKafkaMessage<EntityNotification>       lastUnhandledMessage;

        private long    offsetOfLastMessageCommittedToKafka = -1L;
        private boolean isHandlingDeleteOps;

        private ConsumerRunnable(NotificationConsumer<EntityNotification> consumer) {
            this.consumer = consumer;
        }

        @Override
        public void run() {
            LOG.debug("==> ConsumerRunnable.run()");

            while (true) {
                if (TagSyncConfig.isTagSyncServiceActive()) {
                    LOG.debug("==> ConsumerRunnable.run() is running as server is active");
                    try {
                        List<AtlasKafkaMessage<EntityNotification>> newMessages = consumer.receive(MAX_WAIT_TIME_IN_MILLIS);

                        if (newMessages.isEmpty()) {
                            LOG.debug("AtlasTagSource.ConsumerRunnable.run: no message from NotificationConsumer within {} milliseconds", MAX_WAIT_TIME_IN_MILLIS);

                            if (CollectionUtils.isNotEmpty(atlasEntitiesWithTags)) {
                                buildAndUploadServiceTags();
                            }
                        } else {
                            for (AtlasKafkaMessage<EntityNotification> message : newMessages) {
                                EntityNotification notification = message != null ? message.getMessage() : null;

                                if (notification != null) {
                                    EntityNotificationWrapper notificationWrapper = null;
                                    try {
                                        notificationWrapper = new EntityNotificationWrapper(notification);
                                    } catch (Throwable e) {
                                        LOG.error("notification:[{}] has some issues..perhaps null entity??", notification, e);
                                    }
                                    if (notificationWrapper != null) {
                                        LOG.debug("Message-offset={}, Notification={}", message.getOffset(), getPrintableEntityNotification(notificationWrapper));

                                        if (AtlasNotificationMapper.isNotificationHandled(notificationWrapper)) {
                                            if ((notificationWrapper.getIsEntityDeleteOp() && !isHandlingDeleteOps) || (!notificationWrapper.getIsEntityDeleteOp() && isHandlingDeleteOps)) {
                                                if (CollectionUtils.isNotEmpty(atlasEntitiesWithTags)) {
                                                    buildAndUploadServiceTags();
                                                }
                                                isHandlingDeleteOps = !isHandlingDeleteOps;
                                            }

                                            atlasEntitiesWithTags.add(new RangerAtlasEntityWithTags(notificationWrapper));
                                            messages.add(message);
                                        } else {
                                            AtlasNotificationMapper.logUnhandledEntityNotification(notificationWrapper);
                                            lastUnhandledMessage = message;
                                        }
                                    }
                                } else {
                                    LOG.error("Null entityNotification received from Kafka!! Ignoring..");
                                }
                            }
                            if (CollectionUtils.isNotEmpty(atlasEntitiesWithTags) && atlasEntitiesWithTags.size() >= maxBatchSize) {
                                buildAndUploadServiceTags();
                            }
                        }
                        if (lastUnhandledMessage != null) {
                            commitToKafka(lastUnhandledMessage);
                            lastUnhandledMessage = null;
                        }
                    } catch (Exception exception) {
                        LOG.error("Caught exception..: ", exception);
                        // If transient error, retry after short interval
                        try {
                            Thread.sleep(100);
                        } catch (InterruptedException interrupted) {
                            LOG.error("Interrupted: ", interrupted);
                            LOG.error("Returning from thread. May cause process to be up but not processing events!!");
                            return;
                        }
                    }
                }
            }
        }

        private void buildAndUploadServiceTags() throws Exception {
            LOG.debug("==> buildAndUploadServiceTags()");

            if (CollectionUtils.isNotEmpty(atlasEntitiesWithTags) && CollectionUtils.isNotEmpty(messages)) {
                Map<String, ServiceTags> serviceTagsMap = AtlasNotificationMapper.processAtlasEntities(atlasEntitiesWithTags);

                if (MapUtils.isNotEmpty(serviceTagsMap)) {
                    if (serviceTagsMap.size() != 1) {
                        LOG.warn("Unexpected!! Notifications for more than one service received by AtlasTagSource.. Service-Names:[{}]", serviceTagsMap.keySet());
                    }
                    for (Map.Entry<String, ServiceTags> entry : serviceTagsMap.entrySet()) {
                        if (isHandlingDeleteOps) {
                            entry.getValue().setOp(ServiceTags.OP_DELETE);
                            entry.getValue().setTagDefinitions(Collections.EMPTY_MAP);
                            entry.getValue().setTags(Collections.EMPTY_MAP);
                        } else {
                            entry.getValue().setOp(ServiceTags.OP_ADD_OR_UPDATE);
                        }

                        LOG.debug("serviceTags= {}", JsonUtils.objectToJson(entry.getValue()));

                        updateSink(entry.getValue());
                    }
                }

                AtlasKafkaMessage<EntityNotification> latestMessageDeliveredToRanger = messages.get(messages.size() - 1);
                commitToKafka(latestMessageDeliveredToRanger);

                atlasEntitiesWithTags.clear();
                messages.clear();

                LOG.debug("Completed processing batch of messages of size:[{}] received from NotificationConsumer", messages.size());
            }

            LOG.debug("<== buildAndUploadServiceTags()");
        }

        private void commitToKafka(AtlasKafkaMessage<EntityNotification> messageToCommit) {
            LOG.debug("==> commitToKafka({})", messageToCommit);

            long messageOffset = messageToCommit.getOffset();
            int  partitionId   = messageToCommit.getPartition();

            if (offsetOfLastMessageCommittedToKafka < messageOffset) {
                TopicPartition partition = new TopicPartition(messageToCommit.getTopic(), partitionId);
                try {
                    LOG.debug("Committing message with offset:[{}] to Kafka", messageOffset);
                    consumer.commit(partition, messageOffset);
                    offsetOfLastMessageCommittedToKafka = messageOffset;
                } catch (Exception commitException) {
                    LOG.warn("Ranger tagsync already processed message at offset {}. Ignoring failure in committing message:[{}]", messageOffset, messageToCommit, commitException);
                }
            }

            LOG.debug("<== commitToKafka({})", messageToCommit);
        }
    }
}
