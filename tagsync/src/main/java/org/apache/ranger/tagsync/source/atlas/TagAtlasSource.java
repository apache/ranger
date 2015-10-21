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

import com.google.gson.Gson;

import com.google.inject.Guice;
import com.google.inject.Injector;

import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.atlas.notification.NotificationModule;
import org.apache.atlas.notification.entity.EntityNotification;
import org.apache.atlas.notification.entity.EntityNotificationConsumer;
import org.apache.atlas.notification.entity.EntityNotificationConsumerProvider;
import org.apache.atlas.typesystem.api.Entity;
import org.apache.atlas.typesystem.api.Trait;
import org.apache.ranger.tagsync.model.TagSink;
import org.apache.ranger.tagsync.model.TagSource;
import org.apache.ranger.plugin.util.ServiceTags;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;

public class TagAtlasSource implements TagSource {
	private static final Log LOG = LogFactory.getLog(TagAtlasSource.class);

	public static final String TAGSYNC_ATLAS_PROPERTIES_FILE_NAME = "application.properties";

	public static final String TAGSYNC_ATLAS_KAFKA_ENDPOINTS = "atlas.notification.kafka.bootstrap.servers";
	public static final String TAGSYNC_ATLAS_ZOOKEEPER_ENDPOINT = "atlas.notification.kafka.zookeeper.connect";
	public static final String TAGSYNC_ATLAS_CONSUMER_GROUP = "atlas.notification.kafka.group.id";

	private TagSink tagSink;
	private Properties properties;
	private ConsumerRunnable consumerTask;

	@Override
	public boolean initialize(Properties properties) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> TagAtlasSource.initialize()");
		}

		boolean ret = true;

		if (properties == null || MapUtils.isEmpty(properties)) {
			LOG.error("No properties specified for TagFileSource initialization");
			this.properties = new Properties();
		} else {
			this.properties = properties;
		}

		Properties atlasProperties = new Properties();

		InputStream inputStream = getClass().getClassLoader().getResourceAsStream(TAGSYNC_ATLAS_PROPERTIES_FILE_NAME);

		if (inputStream != null) {
			try {
				atlasProperties.load(inputStream);
			} catch (IOException ioException) {
				ret = false;
				LOG.error("Cannot load Atlas application properties file, file-name:" + TAGSYNC_ATLAS_PROPERTIES_FILE_NAME);
			}
		} else {
			ret = false;
			LOG.error("Cannot find Atlas application properties file");
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

			EntityNotificationConsumerProvider consumerProvider = injector.getInstance(EntityNotificationConsumerProvider.class);

			consumerTask = new ConsumerRunnable(consumerProvider.get());
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== TagAtlasSource.initialize(), result=" + ret);
		}
		return ret;
	}

	@Override
	public void setTagSink(TagSink sink) {
		if (sink == null) {
			LOG.error("Sink is null!!!");
		} else {
			this.tagSink = sink;
		}
	}

	@Override
	public Thread start() {
		Thread consumerThread = null;
		if (consumerTask == null) {
			LOG.error("No consumerTask!!!");
		} else {
			consumerThread = new Thread(consumerTask);
			consumerThread.setDaemon(true);
			consumerThread.start();
		}
		return consumerThread;
	}

	@Override
	public void updateSink() throws Exception {
	}

	@Override
	public 	boolean isChanged() {
		return true;
	}

	// ----- inner class : ConsumerRunnable ------------------------------------

	private class ConsumerRunnable implements Runnable {

		private final EntityNotificationConsumer consumer;

		private ConsumerRunnable(EntityNotificationConsumer consumer) {
			this.consumer = consumer;
		}


		// ----- Runnable --------------------------------------------------------

		@Override
		public void run() {
			while (consumer.hasNext()) {
				try {
					EntityNotification notification = consumer.next();
					if (notification != null) {
						printNotification(notification);
						ServiceTags serviceTags = AtlasNotificationMapper.processEntityNotification(notification, properties);
						if (serviceTags == null) {
							if(LOG.isDebugEnabled()) {
								LOG.debug("Did not create ServiceTags structure for notification type:" + notification.getOperationType().name());
							}
						} else {
							if (LOG.isDebugEnabled()) {
								String serviceTagsJSON = new Gson().toJson(serviceTags);
								LOG.debug("Atlas notification mapped to serviceTags=" + serviceTagsJSON);
							}

							try {
								tagSink.uploadServiceTags(serviceTags);
							} catch (Exception exception) {
								LOG.error("uploadServiceTags() failed..", exception);
							}
						}
					}
				} catch(Exception e){
					LOG.error("Exception encountered when processing notification:", e);
				}
			}
		}

		public void printNotification(EntityNotification notification) {
			Entity entity = notification.getEntity();
			if (LOG.isDebugEnabled()) {
				LOG.debug("Notification-Type: " + notification.getOperationType().name());
				LOG.debug("Entity-Id: " + entity.getId().getGuid());
				LOG.debug("Entity-Type: " + entity.getTypeName());

				LOG.debug("----------- Entity Values ----------");


				for (Map.Entry<String, Object> entry : entity.getValues().entrySet()) {
					LOG.debug("		Name:" + entry.getKey());
					Object value = entry.getValue();
					LOG.debug("		Value:" + value);
				}

				LOG.debug("----------- Entity Traits ----------");


				for (Map.Entry<String, ? extends Trait> entry : entity.getTraits().entrySet()) {
					LOG.debug("			Trait-Name:" + entry.getKey());
					Trait trait = entry.getValue();
					LOG.debug("			Trait-Type:" + trait.getTypeName());
					Map<String, Object> traitValues = trait.getValues();
					for (Map.Entry<String, Object> valueEntry : traitValues.entrySet()) {
						LOG.debug("				Trait-Value-Name:" + valueEntry.getKey());
						LOG.debug("				Trait-Value:" + valueEntry.getValue());
					}
				}
			}
		}
	}
}
