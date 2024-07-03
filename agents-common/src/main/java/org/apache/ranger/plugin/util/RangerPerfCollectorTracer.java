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

import java.lang.management.ThreadInfo;

public class RangerPerfCollectorTracer extends RangerPerfTracer {

	public RangerPerfCollectorTracer(Logger logger, String tag, String data, ThreadInfo threadInfo) {
		super(logger, tag, data, threadInfo);
	}

	@Override
	public void log() {
		// Uncomment following line if the perf log for each individual call details to this needs to be logged in the perf log
		//super.log();
		long elapsedTime = Math.max(getElapsedUserTime(), getElapsedCpuTime());
		long reportingThreshold = threadInfo == null ? 0L : (1000000/1000 - 1); // just about a microsecond

		if (elapsedTime > reportingThreshold) {
			PerfDataRecorder.recordStatistic(tag, (getElapsedCpuTime()+500)/1000, (getElapsedUserTime() + 500)/1000);
		}
	}

	@Override
	public void logAlways() {
		// Uncomment following line if the perf log for each individual call details to this needs to be logged in the perf log
		//super.logAlways();

		// Collect elapsed time in microseconds
		PerfDataRecorder.recordStatistic(tag, (getElapsedCpuTime()+500)/1000, (getElapsedUserTime() + 500)/1000);	}
}
