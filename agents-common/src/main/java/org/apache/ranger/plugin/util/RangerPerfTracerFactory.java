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

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;

public class RangerPerfTracerFactory {

	static volatile ThreadMXBean threadMgmtBean = null;
	private static boolean isThreadCPUTimeSupported = false;
	private static boolean isThreadCPUTimeEnabled = false;

	static RangerPerfTracer getPerfTracer(Logger logger, String tag, String data) {

		RangerPerfTracer ret = null;

		if (logger.isDebugEnabled()) {
			if (threadMgmtBean == null) {
				synchronized (RangerPerfTracerFactory.class) {
					if (threadMgmtBean == null) {
						threadMgmtBean = ManagementFactory.getThreadMXBean();
						isThreadCPUTimeSupported = threadMgmtBean.isThreadCpuTimeSupported();
						logger.info("ThreadCPUTimeSupported (by JVM)  = " + isThreadCPUTimeSupported);

						isThreadCPUTimeEnabled = threadMgmtBean.isThreadCpuTimeEnabled();
						logger.info("ThreadCPUTimeEnabled  = " + isThreadCPUTimeEnabled);

						if (isThreadCPUTimeSupported) {
							if (!isThreadCPUTimeEnabled) {
								threadMgmtBean.setThreadCpuTimeEnabled(true);
								isThreadCPUTimeEnabled = threadMgmtBean.isThreadCpuTimeEnabled();
							}
							logger.info("ThreadCPUTimeEnabled  = " + isThreadCPUTimeEnabled);
						}
					}
				}
			}
		}

		ThreadInfo threadInfo = null;
		if (isThreadCPUTimeSupported && isThreadCPUTimeEnabled) {
			threadInfo = threadMgmtBean.getThreadInfo(Thread.currentThread().getId());
		}

		if (PerfDataRecorder.collectStatistics()) {
			ret = new RangerPerfCollectorTracer(logger, tag, data, threadInfo);
		} else {
			ret = new RangerPerfTracer(logger, tag, data, threadInfo);
		}
		return ret;
	}
}
