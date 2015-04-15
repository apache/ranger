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
package org.apache.ranger.audit.provider;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ranger.audit.model.AuditEventBase;
import org.apache.ranger.audit.model.AuthzAuditEvent;

import com.google.gson.GsonBuilder;

import java.util.concurrent.atomic.AtomicLong;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Properties;

public abstract class BaseAuditProvider implements AuditProvider {
	private static final Log LOG = LogFactory.getLog(BaseAuditProvider.class);

	private static final String AUDIT_LOG_FAILURE_REPORT_MIN_INTERVAL_PROP = "xasecure.audit.log.failure.report.min.interval.ms";
	public static final int AUDIT_MAX_QUEUE_SIZE_DEFAULT = 1024 * 1024;
	public static final int AUDIT_BATCH_INTERVAL_DEFAULT_MS = 1000;
	public static final int AUDIT_BATCH_SIZE_DEFAULT = 1000;

	private AtomicLong lifeTimeInLogCount = new AtomicLong(0);

	private int mLogFailureReportMinIntervalInMs = 60 * 1000;

	private AtomicLong mFailedLogLastReportTime = new AtomicLong(0);
	private AtomicLong mFailedLogCountSinceLastReport = new AtomicLong(0);
	private AtomicLong mFailedLogCountLifeTime = new AtomicLong(0);

	public static final String PROP_NAME = "name";
	public static final String PROP_CLASS_NAME = "classname";
	public static final String PROP_QUEUE = "queue";

	public static final String PROP_BATCH_SIZE = "batch.size";
	public static final String PROP_QUEUE_SIZE = "queue.size";
	public static final String PROP_BATCH_INTERVAL = "batch.interval.ms";

	public static final String PROP_FILE_SPOOL_ENABLE = "filespool.enable";
	public static final String PROP_FILE_SPOOL_WAIT_FOR_FULL_DRAIN = "filespool.drain.full.wait.ms";
	public static final String PROP_FILE_SPOOL_QUEUE_THRESHOLD = "filespool.drain.threshold.percent";

	public static final String PROP_DEFAULT_PREFIX = "xasecure.audit.provider";

	private boolean isDrain = false;
	private String providerName = null;

	private int maxQueueSize = AUDIT_MAX_QUEUE_SIZE_DEFAULT;
	private int maxBatchInterval = AUDIT_BATCH_INTERVAL_DEFAULT_MS;
	private int maxBatchSize = AUDIT_BATCH_SIZE_DEFAULT;

	protected int failedRetryTimes = 3;
	protected int failedRetrySleep = 3 * 1000;

	protected AuditProvider consumer = null;
	protected AuditFileSpool fileSpooler = null;

	protected boolean fileSpoolerEnabled = false;
	protected int fileSpoolMaxWaitTime = 5 * 60 * 1000; // Default 5 minutes
	protected int fileSpoolDrainThresholdPercent = 80;

	int errorLogIntervalMS = 30 * 1000; // Every 30 seconds
	long lastErrorLogMS = 0;

	protected Properties props = null;

	public BaseAuditProvider() {
	}

	public BaseAuditProvider(AuditProvider consumer) {
		this.consumer = consumer;
	}

	@Override
	public void init(Properties props) {
		init(props, null);
	}

	@Override
	public void init(Properties props, String basePropertyName) {
		LOG.info("BaseAuditProvider.init()");
		this.props = props;
		String propPrefix = PROP_DEFAULT_PREFIX;
		if (basePropertyName != null) {
			propPrefix = basePropertyName;
		}
		LOG.info("propPrefix=" + propPrefix);
		// Get final token
		List<String> tokens = MiscUtil.toArray(propPrefix, ".");
		String finalToken = tokens.get(tokens.size() - 1);

		String name = MiscUtil.getStringProperty(props, basePropertyName + "."
				+ PROP_NAME);
		if (name != null && !name.isEmpty()) {
			providerName = name;
		}
		if (providerName == null) {
			providerName = finalToken;
			LOG.info("Using providerName from property prefix. providerName="
					+ providerName);
		}
		LOG.info("providerName=" + providerName);

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
			fileSpooler.init(props, basePropertyName);
		} else {
			LOG.info("File spool is disabled for " + getName());
		}

		try {
			new GsonBuilder().setDateFormat("yyyyMMdd-HH:mm:ss.SSS-Z").create();
		} catch (Throwable excp) {
			LOG.warn(
					"Log4jAuditProvider.init(): failed to create GsonBuilder object. events will be formated using toString(), instead of Json",
					excp);
		}

		mLogFailureReportMinIntervalInMs = MiscUtil.getIntProperty(props,
				AUDIT_LOG_FAILURE_REPORT_MIN_INTERVAL_PROP, 60 * 1000);

	}

	public AuditProvider getConsumer() {
		return consumer;
	}

	public void setConsumer(AuditProvider consumer) {
		this.consumer = consumer;
	}

	public void logFailedEvent(AuditEventBase event) {
		logFailedEvent(event, null);
	}

	public void logFailedEvent(AuditEventBase event, Throwable excp) {
		long now = System.currentTimeMillis();

		long timeSinceLastReport = now - mFailedLogLastReportTime.get();
		long countSinceLastReport = mFailedLogCountSinceLastReport
				.incrementAndGet();
		long countLifeTime = mFailedLogCountLifeTime.incrementAndGet();

		if (timeSinceLastReport >= mLogFailureReportMinIntervalInMs) {
			mFailedLogLastReportTime.set(now);
			mFailedLogCountSinceLastReport.set(0);

			if (excp != null) {
				LOG.warn(
						"failed to log audit event: "
								+ MiscUtil.stringify(event), excp);
			} else {
				LOG.warn("failed to log audit event: "
						+ MiscUtil.stringify(event));
			}

			if (countLifeTime > 1) { // no stats to print for the 1st failure
				LOG.warn("Log failure count: " + countSinceLastReport
						+ " in past "
						+ formatIntervalForLog(timeSinceLastReport) + "; "
						+ countLifeTime + " during process lifetime");
			}
		}
	}

	public void logFailedEvent(Collection<AuditEventBase> events, Throwable excp) {
		for (AuditEventBase event : events) {
			logFailedEvent(event, excp);
		}
	}

	public void logFailedEventJSON(String event, Throwable excp) {
		long now = System.currentTimeMillis();

		long timeSinceLastReport = now - mFailedLogLastReportTime.get();
		long countSinceLastReport = mFailedLogCountSinceLastReport
				.incrementAndGet();
		long countLifeTime = mFailedLogCountLifeTime.incrementAndGet();

		if (timeSinceLastReport >= mLogFailureReportMinIntervalInMs) {
			mFailedLogLastReportTime.set(now);
			mFailedLogCountSinceLastReport.set(0);

			if (excp != null) {
				LOG.warn("failed to log audit event: " + event, excp);
			} else {
				LOG.warn("failed to log audit event: " + event);
			}

			if (countLifeTime > 1) { // no stats to print for the 1st failure
				LOG.warn("Log failure count: " + countSinceLastReport
						+ " in past "
						+ formatIntervalForLog(timeSinceLastReport) + "; "
						+ countLifeTime + " during process lifetime");
			}
		}
	}

	public void logFailedEventJSON(Collection<String> events, Throwable excp) {
		for (String event : events) {
			logFailedEventJSON(event, excp);
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.apache.ranger.audit.provider.AuditProvider#log(org.apache.ranger.
	 * audit.model.AuditEventBase)
	 */
	@Override
	public boolean log(AuditEventBase event) {
		List<AuditEventBase> eventList = new ArrayList<AuditEventBase>();
		eventList.add(event);
		return log(eventList);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.apache.ranger.audit.provider.AuditProvider#logJSON(java.lang.String)
	 */
	@Override
	public boolean logJSON(String event) {
		AuditEventBase eventObj = MiscUtil.fromJson(event,
				AuthzAuditEvent.class);
		return log(eventObj);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.apache.ranger.audit.provider.AuditProvider#logJSON(java.util.Collection
	 * )
	 */
	@Override
	public boolean logJSON(Collection<String> events) {
		boolean ret = true;
		for (String event : events) {
			ret = logJSON(event);
			if (!ret) {
				break;
			}
		}
		return ret;
	}

	public void setName(String name) {
		providerName = name;
	}

	@Override
	public String getName() {
		return providerName;
	}

	@Override
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

	@Override
	public int getMaxBatchInterval() {
		return maxBatchInterval;
	}

	public void setMaxBatchInterval(int maxBatchInterval) {
		this.maxBatchInterval = maxBatchInterval;
	}

	@Override
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

	@Override
	public boolean isFlushPending() {
		return false;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.ranger.audit.provider.AuditProvider#getLastFlushTime()
	 */
	@Override
	public long getLastFlushTime() {
		if (consumer != null) {
			return consumer.getLastFlushTime();
		}
		return 0;
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

	public AtomicLong getLifeTimeInLogCount() {
		return lifeTimeInLogCount;
	}

	public long addLifeTimeInLogCount(long count) {
		return lifeTimeInLogCount.addAndGet(count);
	}

	public void logError(String msg) {
		long currTimeMS = System.currentTimeMillis();
		if (currTimeMS - lastErrorLogMS > errorLogIntervalMS) {
			LOG.error(msg);
			lastErrorLogMS = currTimeMS;
		}
	}

	public void logError(String msg, Throwable ex) {
		long currTimeMS = System.currentTimeMillis();
		if (currTimeMS - lastErrorLogMS > errorLogIntervalMS) {
			LOG.error(msg, ex);
			lastErrorLogMS = currTimeMS;
		}
	}

	public String getTimeDiffStr(long time1, long time2) {
		long timeInMs = Math.abs(time1 - time2);
		return formatIntervalForLog(timeInMs);
	}

	public String formatIntervalForLog(long timeInMs) {
		long hours = timeInMs / (60 * 60 * 1000);
		long minutes = (timeInMs / (60 * 1000)) % 60;
		long seconds = (timeInMs % (60 * 1000)) / 1000;
		long mSeconds = (timeInMs % (1000));

		if (hours > 0)
			return String.format("%02d:%02d:%02d.%03d hours", hours, minutes,
					seconds, mSeconds);
		else if (minutes > 0)
			return String.format("%02d:%02d.%03d minutes", minutes, seconds,
					mSeconds);
		else if (seconds > 0)
			return String.format("%02d.%03d seconds", seconds, mSeconds);
		else
			return String.format("%03d milli-seconds", mSeconds);
	}
}
