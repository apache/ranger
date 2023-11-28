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

package org.apache.ranger.plugin.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.commons.lang.StringUtils;

import java.lang.management.ThreadInfo;

public class RangerPerfTracer {
	protected final Logger logger;
	protected final String tag;
	protected final String data;
	protected final ThreadInfo threadInfo;
	protected final long   startTime;
	protected final long   userStartTime;

	private final static String tagEndMarker = "(";

	public static Logger getPerfLogger(String name) {
		return LoggerFactory.getLogger("org.apache.ranger.perf." + name);
	}

	public static Logger getPerfLogger(Class<?> cls) {
		return RangerPerfTracer.getPerfLogger(cls.getName());
	}

	public static boolean isPerfTraceEnabled(Logger logger) {
		return logger.isDebugEnabled();
	}

	public static RangerPerfTracer getPerfTracer(Logger logger, String tag) {
		String data = "";
		String realTag = "";

		if (tag != null) {
			int indexOfTagEndMarker = StringUtils.indexOf(tag, tagEndMarker);
			if (indexOfTagEndMarker != -1) {
				realTag = StringUtils.substring(tag, 0, indexOfTagEndMarker);
				if (!PerfDataRecorder.collectStatistics()) {
					data = StringUtils.substring(tag, indexOfTagEndMarker);
				}
			} else {
				realTag = tag;
			}
		}
		return RangerPerfTracerFactory.getPerfTracer(logger, realTag, data);
	}

	public static RangerPerfTracer getPerfTracer(Logger logger, String tag, String data) {
		return RangerPerfTracerFactory.getPerfTracer(logger, tag, data);
	}

	public static void log(RangerPerfTracer tracer) {
		if(tracer != null) {
			tracer.log();
		}
	}

	public static void logAlways(RangerPerfTracer tracer) {
		if(tracer != null) {
			tracer.logAlways();
		}
	}
	public RangerPerfTracer(Logger logger, String tag, String data, ThreadInfo threadInfo) {
		this.logger = logger;
		this.tag    = tag;
		this.data	= data;
		this.threadInfo = threadInfo;

		startTime = threadInfo == null ? System.nanoTime() : RangerPerfTracerFactory.threadMgmtBean.getThreadCpuTime(threadInfo.getThreadId());
		userStartTime = System.nanoTime();
	}

	public final long getElapsedUserTime() {
		return System.nanoTime() - userStartTime;
	}
	public final long getElapsedCpuTime() {
		if (threadInfo == null) {
			return getElapsedUserTime();
		} else {
			return RangerPerfTracerFactory.threadMgmtBean.getThreadCpuTime(threadInfo.getThreadId()) - startTime;
		}
	}

	public void log() {
		long elapsedTime = Math.max(getElapsedUserTime(), getElapsedCpuTime());
		long reportingThreshold = threadInfo == null ? 0L : (1000000/1000 - 1); // just about a microsecond

		if (elapsedTime > reportingThreshold) {
			logger.debug("[PERF]:" + (threadInfo != null ? threadInfo.getThreadName() : "") + ":" + tag + data + ":" + getElapsedCpuTime() + ":" + getElapsedUserTime());
		}
	}
	public void logAlways() {
		logger.debug("[PERF]:" + (threadInfo != null ? threadInfo.getThreadName() : "") + ":" + tag + data + ":" + getElapsedCpuTime() + ":" + getElapsedUserTime());
	}
}
