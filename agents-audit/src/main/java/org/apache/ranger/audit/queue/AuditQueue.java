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

package org.apache.ranger.audit.queue;

import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ranger.audit.provider.AuditHandler;
import org.apache.ranger.audit.provider.BaseAuditHandler;
import org.apache.ranger.audit.provider.MiscUtil;

public abstract class AuditQueue extends BaseAuditHandler {
	private static final Log LOG = LogFactory.getLog(AuditQueue.class);

	public static final int AUDIT_MAX_QUEUE_SIZE_DEFAULT = 1024 * 1024;
	public static final int AUDIT_BATCH_INTERVAL_DEFAULT_MS = 1000;
	public static final int AUDIT_BATCH_SIZE_DEFAULT = 1000;

	private int maxQueueSize = AUDIT_MAX_QUEUE_SIZE_DEFAULT;
	private int maxBatchInterval = AUDIT_BATCH_INTERVAL_DEFAULT_MS;
	private int maxBatchSize = AUDIT_BATCH_SIZE_DEFAULT;

	public static final String PROP_QUEUE = "queue";

	public static final String PROP_BATCH_SIZE = "batch.size";
	public static final String PROP_QUEUE_SIZE = "queue.size";
	public static final String PROP_BATCH_INTERVAL = "batch.interval.ms";

	public static final String PROP_FILE_SPOOL_ENABLE = "filespool.enable";
	public static final String PROP_FILE_SPOOL_WAIT_FOR_FULL_DRAIN = "filespool.drain.full.wait.ms";
	public static final String PROP_FILE_SPOOL_QUEUE_THRESHOLD = "filespool.drain.threshold.percent";

	final protected AuditHandler consumer;
	protected AuditFileSpool fileSpooler = null;

	private boolean isDrain = false;

	protected boolean fileSpoolerEnabled = false;
	protected int fileSpoolMaxWaitTime = 5 * 60 * 1000; // Default 5 minutes
	protected int fileSpoolDrainThresholdPercent = 80;

	/**
	 * @param consumer
	 */
	public AuditQueue(AuditHandler consumer) {
		this.consumer = consumer;
	}

	@Override
	public void init(Properties props, String basePropertyName) {
		LOG.info("BaseAuditProvider.init()");
		super.init(props, basePropertyName);

		setMaxBatchSize(MiscUtil.getIntProperty(props, propPrefix + "."
				+ PROP_BATCH_SIZE, getMaxBatchSize()));
		setMaxQueueSize(MiscUtil.getIntProperty(props, propPrefix + "."
				+ PROP_QUEUE_SIZE, getMaxQueueSize()));
		setMaxBatchInterval(MiscUtil.getIntProperty(props, propPrefix + "."
				+ PROP_BATCH_INTERVAL, getMaxBatchInterval()));

		fileSpoolerEnabled = MiscUtil.getBooleanProperty(props, propPrefix
				+ "." + PROP_FILE_SPOOL_ENABLE, false);
		String logFolderProp = MiscUtil.getStringProperty(props, propPrefix
				+ "." + AuditFileSpool.PROP_FILE_SPOOL_LOCAL_DIR);
		if (fileSpoolerEnabled || logFolderProp != null) {
			LOG.info("File spool is enabled for " + getName()
					+ ", logFolderProp=" + logFolderProp + ", " + propPrefix
					+ "." + AuditFileSpool.PROP_FILE_SPOOL_LOCAL_DIR + "="
					+ fileSpoolerEnabled);
			fileSpoolerEnabled = true;
			fileSpoolMaxWaitTime = MiscUtil.getIntProperty(props, propPrefix
					+ "." + PROP_FILE_SPOOL_WAIT_FOR_FULL_DRAIN,
					fileSpoolMaxWaitTime);
			fileSpoolDrainThresholdPercent = MiscUtil.getIntProperty(props,
					propPrefix + "." + PROP_FILE_SPOOL_QUEUE_THRESHOLD,
					fileSpoolDrainThresholdPercent);
			fileSpooler = new AuditFileSpool(this, consumer);
			if (!fileSpooler.init(props, basePropertyName)) {
				fileSpoolerEnabled = false;
				LOG.fatal("Couldn't initialize file spooler. Disabling it. queue="
						+ getName() + ", consumer=" + consumer.getName());
			}
		} else {
			LOG.info("File spool is disabled for " + getName());
		}

	}

	public AuditHandler getConsumer() {
		return consumer;
	}

	public boolean isDrain() {
		return isDrain;
	}

	public void setDrain(boolean isDrain) {
		this.isDrain = isDrain;
	}

	public int getMaxQueueSize() {
		return maxQueueSize;
	}

	public void setMaxQueueSize(int maxQueueSize) {
		this.maxQueueSize = maxQueueSize;
	}

	public int getMaxBatchInterval() {
		return maxBatchInterval;
	}

	public void setMaxBatchInterval(int maxBatchInterval) {
		this.maxBatchInterval = maxBatchInterval;
	}

	public int getMaxBatchSize() {
		return maxBatchSize;
	}

	public void setMaxBatchSize(int maxBatchSize) {
		this.maxBatchSize = maxBatchSize;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.ranger.audit.provider.AuditProvider#waitToComplete()
	 */
	@Override
	public void waitToComplete() {
		if (consumer != null) {
			consumer.waitToComplete(-1);
		}
	}

	@Override
	public void waitToComplete(long timeout) {
		if (consumer != null) {
			consumer.waitToComplete(timeout);
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.ranger.audit.provider.AuditProvider#flush()
	 */
	@Override
	public void flush() {
		if (consumer != null) {
			consumer.flush();
		}
	}

}
