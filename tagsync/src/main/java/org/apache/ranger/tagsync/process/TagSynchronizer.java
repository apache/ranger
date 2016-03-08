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

package org.apache.ranger.tagsync.process;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.apache.ranger.tagsync.model.TagSink;
import org.apache.ranger.tagsync.model.TagSource;

import java.util.*;

public class TagSynchronizer {

	private static final Logger LOG = Logger.getLogger(TagSynchronizer.class);

	private TagSink tagSink = null;
	private List<TagSource> tagSources;
	private Properties properties = null;

	private final Object shutdownNotifier = new Object();
	private volatile boolean isShutdownInProgress = false;

	public static void main(String[] args) {

		TagSynchronizer tagSynchronizer = new TagSynchronizer();

		TagSyncConfig config = TagSyncConfig.getInstance();

		Properties props = config.getProperties();

		tagSynchronizer.setProperties(props);

		boolean tagSynchronizerInitialized = tagSynchronizer.initialize();

		if (tagSynchronizerInitialized) {
			try {
				tagSynchronizer.run();
			} catch (Throwable t) {
				LOG.error("main thread caught exception..:", t);
				System.exit(1);
			}
		} else {
			LOG.error("TagSynchronizer failed to initialize correctly, exiting..");
			System.exit(1);
		}

	}

	TagSynchronizer() {
		this(null);
	}

	TagSynchronizer(Properties properties) {
		setProperties(properties);
	}

	void setProperties(Properties properties) {
		if (properties == null || MapUtils.isEmpty(properties)) {
			this.properties = new Properties();
		} else {
			this.properties = properties;
		}
	}

	public boolean initialize() {

		if (LOG.isDebugEnabled()) {
			LOG.debug("==> TagSynchronizer.initialize()");
		}

		printConfigurationProperties(properties);

		boolean ret = false;

		String tagSourceNames = TagSyncConfig.getTagSource(properties);

		if (StringUtils.isNotBlank(tagSourceNames)) {

			LOG.info("Initializing TAG source and sink");

			tagSink = initializeTagSink(properties);

			if (tagSink != null) {

				tagSources = initializeTagSources(tagSourceNames, properties);

				if (CollectionUtils.isNotEmpty(tagSources)) {
					for (TagSource tagSource : tagSources) {
						tagSource.setTagSink(tagSink);
					}
					ret = true;
				}
			}
		} else {
			LOG.error("'ranger.tagsync.source.impl.class' value is not specified or is empty!");
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== TagSynchronizer.initialize(" + tagSourceNames + ") : " + ret);
		}

		return ret;
	}

	public void run() throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> TagSynchronizer.run()");
		}

		isShutdownInProgress = false;

		try {
			boolean threadsStarted = tagSink.start();

			for (TagSource tagSource : tagSources) {
				threadsStarted = threadsStarted && tagSource.start();
			}

			if (threadsStarted) {
				synchronized(shutdownNotifier) {
					while(! isShutdownInProgress) {
						shutdownNotifier.wait();
					}
				}
			}
		} finally {
			LOG.info("Stopping all tagSources");

			for (TagSource tagSource : tagSources) {
				tagSource.stop();
			}

			LOG.info("Stopping tagSink");
			tagSink.stop();
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== TagSynchronizer.run()");
		}
	}

	public void shutdown(String reason) {
		LOG.info("Received shutdown(), reason=" + reason);

		synchronized(shutdownNotifier) {
			isShutdownInProgress = true;
			shutdownNotifier.notifyAll();
		}
	}

	static public void printConfigurationProperties(Properties properties) {
		LOG.info("--------------------------------");
		LOG.info("");
		LOG.info("Ranger-TagSync Configuration: {\n");
		if (MapUtils.isNotEmpty(properties)) {
			for (Map.Entry<Object, Object> entry : properties.entrySet()) {
				LOG.info("\tProperty-Name:" + entry.getKey());
				LOG.info("\tProperty-Value:" + entry.getValue());
				LOG.info("\n");
			}
		}
		LOG.info("\n}");
		LOG.info("");
		LOG.info("--------------------------------");
	}

	static public TagSink initializeTagSink(Properties properties) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> TagSynchronizer.initializeTagSink()");
		}

		TagSink ret = null;

		try {
			String tagSinkClassName = TagSyncConfig.getTagSinkClassName(properties);

			if (LOG.isDebugEnabled()) {
				LOG.debug("tagSinkClassName=" + tagSinkClassName);
			}
			@SuppressWarnings("unchecked")
			Class<TagSink> tagSinkClass = (Class<TagSink>) Class.forName(tagSinkClassName);

			ret = tagSinkClass.newInstance();

			if (!ret.initialize(properties)) {
				LOG.error("Failed to initialize TAG sink " + tagSinkClassName);

				ret = null;
			}
		} catch (Throwable t) {
			LOG.error("Failed to initialize TAG sink. Error details: ", t);
			ret = null;
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== TagSynchronizer.initializeTagSink()");
		}
		return ret;
	}

	static public List<TagSource> initializeTagSources(String tagSourceNames, Properties properties) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> TagSynchronizer.initializeTagSources(" + tagSourceNames + ")");
		}

		List<TagSource> ret = new ArrayList<TagSource>();

		String[] tagSourcesArray = tagSourceNames.split(",");

		List<String> tagSourceList = Arrays.asList(tagSourcesArray);

		try {
			for (String tagSourceName : tagSourceList) {

				String tagSourceClassName = TagSyncConfig.getTagSourceClassName(tagSourceName.trim());
				if (LOG.isDebugEnabled()) {
					LOG.debug("tagSourceClassName=" + tagSourceClassName);
				}

				@SuppressWarnings("unchecked")
				Class<TagSource> tagSourceClass = (Class<TagSource>) Class.forName(tagSourceClassName);
				TagSource tagSource = tagSourceClass.newInstance();

				if (LOG.isDebugEnabled()) {
					LOG.debug("Created instance of " + tagSourceClassName);
				}

				if (!tagSource.initialize(properties)) {
					LOG.error("Failed to initialize TAG source " + tagSourceClassName);

					ret.clear();
					break;
				}
				ret.add(tagSource);
			}

		} catch (Throwable t) {
			LOG.error("Failed to initialize TAG sources. Error details: ", t);
			ret.clear();
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== TagSynchronizer.initializeTagSources(" + tagSourceNames + ")");
		}

		return ret;
	}
}
