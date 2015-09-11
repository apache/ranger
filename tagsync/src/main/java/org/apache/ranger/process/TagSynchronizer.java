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

package org.apache.ranger.process;

import org.apache.commons.collections.MapUtils;
import org.apache.log4j.Logger;
import org.apache.ranger.model.TagSink;
import org.apache.ranger.model.TagSource;

import java.util.Map;
import java.util.Properties;

public class TagSynchronizer implements Runnable {

	private static final Logger LOG = Logger.getLogger(TagSynchronizer.class);

	private final static int MAX_INIT_RETRIES = 5;

	private boolean shutdownFlag = false;
	private TagSink tagSink = null;
	private TagSource tagSource = null;
	private Properties properties = null;


	public static void main(String[] args) {

		TagSyncConfig config = TagSyncConfig.getInstance();
		Properties props = config.getProperties();

		TagSynchronizer tagSynchronizer = new TagSynchronizer(props);

		tagSynchronizer.run();
	}

	public TagSynchronizer(Properties properties) {
		if (properties == null || MapUtils.isEmpty(properties)) {
			LOG.error("TagSynchronizer initialized with null properties!");
			this.properties = new Properties();
		} else {
			this.properties = properties;
		}
	}

	public TagSink getTagSink() {
		return tagSink;
	}

	public TagSource getTagSource() {
		return tagSource;
	}

	@Override
	public void run() {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> TagSynchronizer.run()");
		}
		try {
			long sleepTimeBetweenCycleInMillis = TagSyncConfig.getSleepTimeInMillisBetweenCycle(properties);

			boolean initDone = initLoop();

			if (initDone) {

				Thread tagSourceThread = tagSource.start();

				if (tagSourceThread != null) {
					while (!shutdownFlag) {
						try {
							LOG.debug("Sleeping for [" + sleepTimeBetweenCycleInMillis + "] milliSeconds");
							Thread.sleep(sleepTimeBetweenCycleInMillis);
						} catch (InterruptedException e) {
							LOG.error("Failed to wait for [" + sleepTimeBetweenCycleInMillis + "] milliseconds before attempting to synchronize tag information", e);
						}
					}
					if (shutdownFlag) {
						LOG.info("Interrupting tagSourceThread...");
						tagSourceThread.interrupt();
						try {
							tagSourceThread.join();
						} catch (InterruptedException interruptedException) {
							LOG.error("tagSourceThread.join() was interrupted");
						}
					}
				} else {
					LOG.error("Could not start tagSource monitoring thread");
				}
			} else {
				LOG.error("Failed to initialize TagSynchonizer after " + MAX_INIT_RETRIES + " retries. Exiting thread");
			}

		} catch (Throwable t) {
			LOG.error("tag-sync thread got an error", t);
		} finally {
			LOG.error("Shutting down the tag-sync thread");
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== TagSynchronizer.run()");
		}
	}

	public boolean initLoop() {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> TagSynchronizer.initLoop()");
		}
		boolean ret = false;

		long sleepTimeBetweenCycleInMillis = TagSyncConfig.getSleepTimeInMillisBetweenCycle(properties);

		for (int initRetries = 0; initRetries < MAX_INIT_RETRIES && !ret; initRetries++) {

			printConfigurationProperties();

			ret = init();

			if (!ret) {
				LOG.error("Failed to initialize TAG source/sink. Will retry after " + sleepTimeBetweenCycleInMillis + " milliseconds.");
				try {
					LOG.debug("Sleeping for [" + sleepTimeBetweenCycleInMillis + "] milliSeconds");
					Thread.sleep(sleepTimeBetweenCycleInMillis);
					properties = TagSyncConfig.getInstance().getProperties();
				} catch (Exception e) {
					LOG.error("Failed to wait for [" + sleepTimeBetweenCycleInMillis + "] milliseconds before attempting to initialize tag source/sink", e);
				}
			}
		}
		if (LOG.isDebugEnabled()) {
			LOG.debug("<== TagSynchronizer.initLoop()");
		}
		return ret;
	}

	public boolean init() {

		if (LOG.isDebugEnabled()) {
			LOG.debug("==> TagSynchronizer.init()");
		}
		boolean ret = false;
		try {
			LOG.info("Initializing TAG source and sink");
			// Initialize tagSink and tagSource
			String tagSourceClassName = TagSyncConfig.getTagSourceClassName(properties);
			String tagSinkClassName = TagSyncConfig.getTagSinkClassName(properties);

			if (LOG.isDebugEnabled()) {
				LOG.debug("tagSourceClassName=" + tagSourceClassName + ", tagSinkClassName=" + tagSinkClassName);
			}

			Class<TagSource> tagSourceClass = (Class<TagSource>) Class.forName(tagSourceClassName);
			Class<TagSink> tagSinkClass = (Class<TagSink>) Class.forName(tagSinkClassName);

			tagSink = tagSinkClass.newInstance();
			tagSource = tagSourceClass.newInstance();

			if (LOG.isDebugEnabled()) {
				LOG.debug("Created instance of " + tagSourceClassName + ", " + tagSinkClassName);
			}

			ret = tagSink.initialize(properties) && tagSource.initialize(properties);

			tagSource.setTagSink(tagSink);

			LOG.info("Done initializing TAG source and sink");
		} catch (Throwable t) {
			LOG.error("Failed to initialize TAG source/sink. Error details: ", t);
		}
		if (LOG.isDebugEnabled()) {
			LOG.debug("<== TagSynchronizer.init(), result=" + ret);
		}

		return ret;
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
