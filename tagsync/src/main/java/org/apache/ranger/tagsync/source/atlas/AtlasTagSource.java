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


import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Provider;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.atlas.notification.NotificationConsumer;
import org.apache.atlas.notification.NotificationInterface;
import org.apache.atlas.notification.NotificationModule;
import org.apache.atlas.notification.entity.EntityNotification;

import org.apache.ranger.tagsync.model.AbstractTagSource;
import org.apache.ranger.plugin.util.ServiceTags;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.List;

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
			NotificationModule notificationModule = new NotificationModule();

			Injector injector = Guice.createInjector(notificationModule);

			Provider<NotificationInterface> consumerProvider = injector.getProvider(NotificationInterface.class);
			NotificationInterface notification = consumerProvider.get();
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

	private static String getPrintableEntityNotification(EntityNotification notification) {
		StringBuilder sb = new StringBuilder();

		sb.append("{ Notification-Type: ").append(notification.getOperationType()).append(", ");
		AtlasEntityWithTraits entityWithTraits = new AtlasEntityWithTraits(notification.getEntity(), notification.getAllTraits());
		sb.append(entityWithTraits.toString());
		sb.append("}");
		return sb.toString();
	}

	private class ConsumerRunnable implements Runnable {

		private final NotificationConsumer<EntityNotification> consumer;

		private ConsumerRunnable(NotificationConsumer<EntityNotification> consumer) {
			this.consumer = consumer;
		}

		private boolean hasNext() {
			boolean ret = false;
			try {
				ret = consumer.hasNext();
			} catch (Exception exception) {
				LOG.error("EntityNotification consumer threw exception, IGNORING...:", exception);
			}
			return ret;
		}

		@Override
		public void run() {
			if (LOG.isDebugEnabled()) {
				LOG.debug("==> ConsumerRunnable.run()");
			}
			while (true) {
				try {
					if (hasNext()) {
						EntityNotification notification = consumer.peek();
						if (notification != null) {
							if (LOG.isDebugEnabled()) {
								LOG.debug("Notification=" + getPrintableEntityNotification(notification));
							}

							ServiceTags serviceTags = AtlasNotificationMapper.processEntityNotification(notification);
							if (serviceTags != null) {
								updateSink(serviceTags);
							}
						} else {
							LOG.error("Null entityNotification received from Kafka!! Ignoring..");
						}
						// Move iterator forward
						consumer.next();
					}
				} catch (Exception exception) {
					LOG.error("Caught exception..: ", exception);
					return;
				}
			}
		}
	}
}

