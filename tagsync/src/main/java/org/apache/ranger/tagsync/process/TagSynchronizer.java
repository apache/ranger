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

	private boolean shutdownFlag = false;
	private List<TagSource> tagSources;
	private Properties properties = null;

	public static void main(String[] args) {

		TagSynchronizer tagSynchronizer = new TagSynchronizer();

		TagSyncConfig config = TagSyncConfig.getInstance();

		Properties props = config.getProperties();

		tagSynchronizer.setProperties(props);

		boolean tagSynchronizerInitialized = tagSynchronizer.initialize();

		if (tagSynchronizerInitialized) {
			tagSynchronizer.run();
		} else {
			LOG.error("TagSynchronizer failed to initialize correctly, exiting..");
			System.exit(-1);
		}

	}

	public TagSynchronizer() {
		setProperties(null);
	}

	public TagSynchronizer(Properties properties) {
		setProperties(properties);
	}

	public void setProperties(Properties properties) {
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

			TagSink tagSink = initializeTagSink(properties);

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

	public void run() {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> TagSynchronizer.run()");
		}

		long shutdownCheckIntervalInMs = 60*1000;

		boolean tagSourcesStarted = true;

		try {
			for (TagSource tagSource : tagSources) {
				tagSourcesStarted = tagSourcesStarted && tagSource.start();
			}

			if (tagSourcesStarted) {
				while (!shutdownFlag) {
					try {
						LOG.debug("Sleeping for [" + shutdownCheckIntervalInMs + "] milliSeconds");
						Thread.sleep(shutdownCheckIntervalInMs);
					} catch (InterruptedException e) {
						LOG.error("Failed to wait for [" + shutdownCheckIntervalInMs + "] milliseconds before attempting to synchronize tag information ", e);
						break;
					}
				}
			}
		} catch (Throwable t) {
			LOG.error("tag-sync main thread got an error", t);
		} finally {
			LOG.info("Stopping all tagSources");

			for (TagSource tagSource : tagSources) {
				tagSource.stop();
			}
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== TagSynchronizer.run()");
		}
	}

	public void shutdown(String reason) {
		LOG.info("Received shutdown(), reason=" + reason);
		this.shutdownFlag = true;
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
