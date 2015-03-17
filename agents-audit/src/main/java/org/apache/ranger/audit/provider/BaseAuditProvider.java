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
import java.util.concurrent.atomic.AtomicLong;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public abstract class BaseAuditProvider implements AuditProvider {
	private static final Log LOG = LogFactory.getLog(BaseAuditProvider.class);

	private static final String AUDIT_LOG_FAILURE_REPORT_MIN_INTERVAL_PROP = "xasecure.audit.log.failure.report.min.interval.ms";
	public static final int AUDIT_ASYNC_MAX_QUEUE_SIZE_DEFAULT     = 10 * 1024;
	public static final int AUDIT_ASYNC_MAX_FLUSH_INTERVAL_DEFAULT =  5 * 1000;

	private int   mLogFailureReportMinIntervalInMs = 60 * 1000;

	private AtomicLong mFailedLogLastReportTime       = new AtomicLong(0);
	private AtomicLong mFailedLogCountSinceLastReport = new AtomicLong(0);
	private AtomicLong mFailedLogCountLifeTime        = new AtomicLong(0);
	private int maxQueueSize     =  AUDIT_ASYNC_MAX_QUEUE_SIZE_DEFAULT;
	private int maxFlushInterval = AUDIT_ASYNC_MAX_FLUSH_INTERVAL_DEFAULT;

	protected Properties props = null;

	public BaseAuditProvider() {
	}
	
	@Override
	public void init(Properties props) {
		LOG.info("BaseAuditProvider.init()");
		this.props = props;
		
		mLogFailureReportMinIntervalInMs = getIntProperty(props, AUDIT_LOG_FAILURE_REPORT_MIN_INTERVAL_PROP, 60 * 1000);

	}

	public void logFailedEvent(AuditEventBase event) {
		logFailedEvent(event, null);
 	}

	public void logFailedEvent(AuditEventBase event, Throwable excp) {
		long now = System.currentTimeMillis();
		
		long timeSinceLastReport  = now - mFailedLogLastReportTime.get();
		long countSinceLastReport = mFailedLogCountSinceLastReport.incrementAndGet();
		long countLifeTime        = mFailedLogCountLifeTime.incrementAndGet();

		if(timeSinceLastReport >= mLogFailureReportMinIntervalInMs) {
			mFailedLogLastReportTime.set(now);
			mFailedLogCountSinceLastReport.set(0);

			if(excp != null) {
				LOG.warn("failed to log audit event: " + MiscUtil.stringify(event), excp);
			} else {
				LOG.warn("failed to log audit event: " + MiscUtil.stringify(event));
			}

			if(countLifeTime > 1) { // no stats to print for the 1st failure
				LOG.warn("Log failure count: " + countSinceLastReport + " in past " + formatIntervalForLog(timeSinceLastReport) + "; " + countLifeTime + " during process lifetime");
			}
		}
 	}

	
	public int getMaxQueueSize() {
		return maxQueueSize;
	}

	public void setMaxQueueSize(int maxQueueSize) {
		this.maxQueueSize = maxQueueSize;
	}

	public int getMaxFlushInterval() {
		return maxFlushInterval;
	}

	public void setMaxFlushInterval(int maxFlushInterval) {
		this.maxFlushInterval = maxFlushInterval;
	}

	public static Map<String, String> getPropertiesWithPrefix(Properties props, String prefix) {
		Map<String, String> prefixedProperties = new HashMap<String, String>();

		if(props != null && prefix != null) {
			for(String key : props.stringPropertyNames()) {
				if(key == null) {
					continue;
				}

				String val = props.getProperty(key);

				if(key.startsWith(prefix)) {
					key = key.substring(prefix.length());

					if(key == null) {
						continue;
					}

					prefixedProperties.put(key, val);
				}
			}
		}

		return prefixedProperties;
	}
	
	public static boolean getBooleanProperty(Properties props, String propName, boolean defValue) {
		boolean ret = defValue;

		if(props != null && propName != null) {
			String val = props.getProperty(propName);

			if(val != null) {
				ret = Boolean.valueOf(val);
			}
		}

		return ret;
	}
	
	public static int getIntProperty(Properties props, String propName, int defValue) {
		int ret = defValue;

		if(props != null && propName != null) {
			String val = props.getProperty(propName);

			if(val != null) {
				try {
					ret = Integer.parseInt(val);
				} catch(NumberFormatException excp) {
					ret = defValue;
				}
			}
		}

		return ret;
	}
	
	
	public static String getStringProperty(Properties props, String propName) {
		String ret = null;

		if(props != null && propName != null) {
			String val = props.getProperty(propName);
			if ( val != null){
				ret = val;
			}
		}

		return ret;
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
