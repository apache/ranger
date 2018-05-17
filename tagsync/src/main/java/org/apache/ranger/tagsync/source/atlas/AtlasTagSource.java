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


import org.apache.atlas.kafka.NotificationProvider;
import org.apache.atlas.model.notification.EntityNotification;
import org.apache.atlas.notification.NotificationConsumer;
import org.apache.atlas.notification.NotificationInterface;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ranger.plugin.util.ServiceTags;
import org.apache.ranger.tagsync.model.AbstractTagSource;
import org.apache.atlas.kafka.AtlasKafkaMessage;
import org.apache.kafka.common.TopicPartition;
import org.apache.ranger.tagsync.source.atlasrest.RangerAtlasEntityWithTags;


import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Properties;

public class AtlasTagSource extends AbstractTagSource {
	private static final Log LOG = LogFactory.getLog(AtlasTagSource.class);

	public static final String TAGSYNC_ATLAS_PROPERTIES_FILE_NAME = "atlas-application.properties";

	public static final String TAGSYNC_ATLAS_KAFKA_ENDPOINTS = "atlas.kafka.bootstrap.servers";
	public static final String TAGSYNC_ATLAS_ZOOKEEPER_ENDPOINT = "atlas.kafka.zookeeper.connect";
	public static final String TAGSYNC_ATLAS_CONSUMER_GROUP = "atlas.kafka.entities.group.id";

	private ConsumerRunnable consumerTask;
	private Thread myThread = null;

	@Override
	public boolean initialize(Properties properties) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> AtlasTagSource.initialize()");
		}

		Properties atlasProperties = new Properties();

		boolean ret = AtlasResourceMapperUtil.initializeAtlasResourceMappers(properties);

		if (ret) {

			InputStream inputStream = getClass().getClassLoader().getResourceAsStream(TAGSYNC_ATLAS_PROPERTIES_FILE_NAME);

			if (inputStream != null) {
				try {
					atlasProperties.load(inputStream);
				} catch (Exception exception) {
					ret = false;
					LOG.error("Cannot load Atlas application properties file, file-name:" + TAGSYNC_ATLAS_PROPERTIES_FILE_NAME, exception);
				} finally {
					try {
						inputStream.close();
					} catch (IOException ioException) {
						LOG.error("Cannot close Atlas application properties file, file-name:\" + TAGSYNC_ATLAS_PROPERTIES_FILE_NAME", ioException);
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
				LOG.error("Value of property '" + TAGSYNC_ATLAS_KAFKA_ENDPOINTS + "' is not specified!");
			}
			if (StringUtils.isBlank(atlasProperties.getProperty(TAGSYNC_ATLAS_ZOOKEEPER_ENDPOINT))) {
				ret = false;
				LOG.error("Value of property '" + TAGSYNC_ATLAS_ZOOKEEPER_ENDPOINT + "' is not specified!");
			}
			if (StringUtils.isBlank(atlasProperties.getProperty(TAGSYNC_ATLAS_CONSUMER_GROUP))) {
				ret = false;
				LOG.error("Value of property '" + TAGSYNC_ATLAS_CONSUMER_GROUP + "' is not specified!");
			}
		}

		if (ret) {
			NotificationInterface notification = NotificationProvider.get();
			List<NotificationConsumer<EntityNotification>> iterators = notification.createConsumers(NotificationInterface.NotificationType.ENTITIES, 1);

			consumerTask = new ConsumerRunnable(iterators.get(0));
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== AtlasTagSource.initialize(), result=" + ret);
		}
		return ret;
	}

	@Override
	public boolean start() {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> AtlasTagSource.start()");
		}
		if (consumerTask == null) {
			LOG.error("No consumerTask!!!");
		} else {
			myThread = new Thread(consumerTask);
			myThread.setDaemon(true);
			myThread.start();
		}
		if (LOG.isDebugEnabled()) {
			LOG.debug("<== AtlasTagSource.start()");
		}
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
        sb.append(entityWithTags.toString());

		sb.append("}");
		return sb.toString();
	}

	private class ConsumerRunnable implements Runnable {

		private final NotificationConsumer<EntityNotification> consumer;

		private ConsumerRunnable(NotificationConsumer<EntityNotification> consumer) {
			this.consumer = consumer;
		}


		@Override
		public void run() {
			if (LOG.isDebugEnabled()) {
				LOG.debug("==> ConsumerRunnable.run()");
			}

			boolean seenCommitException = false;
			long offsetOfLastMessageDeliveredToRanger = -1L;

			while (true) {
				try {
					List<AtlasKafkaMessage<EntityNotification>> messages = consumer.receive(1000L);

					int index = 0;

					if (messages.size() > 0 && seenCommitException) {
						if (LOG.isDebugEnabled()) {
							LOG.debug("seenCommitException=[true], offsetOfLastMessageDeliveredToRanger=[" + offsetOfLastMessageDeliveredToRanger + "]");
						}
						for (; index < messages.size(); index++) {
							AtlasKafkaMessage<EntityNotification> message = messages.get(index);
							if (message.getOffset() <= offsetOfLastMessageDeliveredToRanger) {
								// Already delivered to Ranger
								TopicPartition partition = new TopicPartition("ATLAS_ENTITIES", message.getPartition());
								try {
									if (LOG.isDebugEnabled()) {
										LOG.debug("Committing previously commit-failed message with offset:[" + message.getOffset() + "]");
									}
									consumer.commit(partition, message.getOffset());
								} catch (Exception commitException) {
									LOG.warn("Ranger tagsync already processed message at offset " + message.getOffset() + ". Ignoring failure in committing this message and continuing to process next message", commitException);
									LOG.warn("This will cause Kafka to deliver this message:[" + message.getOffset() + "] repeatedly!! This may be unrecoverable error!!");
								}
							} else {
								break;
							}
						}
					}

					seenCommitException = false;
					offsetOfLastMessageDeliveredToRanger = -1L;

					for (; index < messages.size(); index++) {
						AtlasKafkaMessage<EntityNotification> message = messages.get(index);
						EntityNotification notification = message != null ? message.getMessage() : null;

						if (notification != null) {
							EntityNotificationWrapper notificationWrapper = null;
							try {
								notificationWrapper = new EntityNotificationWrapper(notification);
							} catch (Throwable e) {
								LOG.error("notification:[" + notification +"] has some issues..perhaps null entity??", e);
							}
							if (notificationWrapper != null) {
								if (LOG.isDebugEnabled()) {
									LOG.debug("Message-offset=" + message.getOffset() + ", Notification=" + getPrintableEntityNotification(notificationWrapper));
								}

								ServiceTags serviceTags = AtlasNotificationMapper.processEntityNotification(notificationWrapper);
								if (serviceTags != null) {
									updateSink(serviceTags);
								}
								offsetOfLastMessageDeliveredToRanger = message.getOffset();

								if (!seenCommitException) {
									TopicPartition partition = new TopicPartition("ATLAS_ENTITIES", message.getPartition());
									try {
										consumer.commit(partition, message.getOffset());
									} catch (Exception commitException) {
										seenCommitException = true;
										LOG.warn("Ranger tagsync processed message at offset " + message.getOffset() + ". Ignoring failure in committing this message and continuing to process next message", commitException);
									}
								}
							}
						} else {
							LOG.error("Null entityNotification received from Kafka!! Ignoring..");
						}
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
}

