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

import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.apache.ranger.tagsync.model.TagSink;
import org.apache.ranger.tagsync.model.TagSource;

import java.util.Map;
import java.util.Properties;

public class TagSynchronizer {

	private static final Logger LOG = Logger.getLogger(TagSynchronizer.class);

	private boolean shutdownFlag = false;
	private TagSource tagSource = null;
	private Properties properties = null;

	public static void main(String[] args) {

		boolean tagSynchronizerInitialized = false;
		TagSynchronizer tagSynchronizer = new TagSynchronizer();

		while (!tagSynchronizerInitialized) {

			TagSyncConfig config = TagSyncConfig.getInstance();
			Properties props = config.getProperties();

			tagSynchronizer.setProperties(props);
			tagSynchronizerInitialized = tagSynchronizer.initialize();

			if (!tagSynchronizerInitialized) {
				LOG.error("TagSynchronizer failed to initialize correctly");

				try {
					LOG.error("Sleeping for [60] seconds before attempting to re-read configuration XML files");
					Thread.sleep(60 * 1000);
				} catch (InterruptedException e) {
					LOG.error("Failed to wait for [60] seconds", e);
				}
			}
		}

		tagSynchronizer.run();
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

		printConfigurationProperties();

		boolean ret = true;

		String tagSourceName = TagSyncConfig.getTagSource(properties);

		if (StringUtils.isBlank(tagSourceName) ||
				(!StringUtils.equalsIgnoreCase(tagSourceName, "file") && !StringUtils.equalsIgnoreCase(tagSourceName, "atlas"))) {
			ret = false;
			LOG.error("'ranger.tagsync.source.impl.class' value is invalid!, 'ranger.tagsync.source.impl.class'=" + tagSourceName + ". Supported 'ranger.tagsync.source.impl.class' values are : file, atlas");
		}

		if (ret) {

			try {
				LOG.info("Initializing TAG source and sink");
				// Initialize tagSink and tagSource
				String tagSourceClassName = TagSyncConfig.getTagSourceClassName(properties);
				String tagSinkClassName = TagSyncConfig.getTagSinkClassName(properties);

				if (LOG.isDebugEnabled()) {
					LOG.debug("tagSourceClassName=" + tagSourceClassName + ", tagSinkClassName=" + tagSinkClassName);
				}

				@SuppressWarnings("unchecked")
				Class<TagSource> tagSourceClass = (Class<TagSource>) Class.forName(tagSourceClassName);

				@SuppressWarnings("unchecked")
				Class<TagSink> tagSinkClass = (Class<TagSink>) Class.forName(tagSinkClassName);

				TagSink tagSink = tagSinkClass.newInstance();
				tagSource = tagSourceClass.newInstance();

				if (LOG.isDebugEnabled()) {
					LOG.debug("Created instance of " + tagSourceClassName + ", " + tagSinkClassName);
				}

				ret = tagSink.initialize(properties) && tagSource.initialize(properties);

				if (ret) {
					tagSource.setTagSink(tagSink);
				}

				LOG.info("Done initializing TAG source and sink");
			} catch (Throwable t) {
				LOG.error("Failed to initialize TAG source/sink. Error details: ", t);
				ret = false;
			}
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== TagSynchronizer.initialize(), result=" + ret);
		}

		return ret;
	}

	public void run() {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> TagSynchronizer.run()");
		}

		long shutdownCheckIntervalInMs = 60*1000;

		Thread tagSourceThread = null;

		try {
			tagSourceThread = tagSource.start();

			if (tagSourceThread != null) {
				while (!shutdownFlag) {
					try {
						LOG.debug("Sleeping for [" + shutdownCheckIntervalInMs + "] milliSeconds");
						Thread.sleep(shutdownCheckIntervalInMs);
					} catch (InterruptedException e) {
						LOG.error("Failed to wait for [" + shutdownCheckIntervalInMs + "] milliseconds before attempting to synchronize tag information", e);
					}
				}
			}
		} catch (Throwable t) {
			LOG.error("tag-sync thread got an error", t);
		} finally {
			if (tagSourceThread != null) {
				LOG.error("Shutting down the tag-sync thread");
				LOG.info("Interrupting tagSourceThread...");
				tagSourceThread.interrupt();
				try {
					tagSourceThread.join();
				} catch (InterruptedException interruptedException) {
					LOG.error("tagSourceThread.join() was interrupted");
				}
			} else {
				LOG.error("Could not start tagSource monitoring thread");
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

	public void printConfigurationProperties() {
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
}
