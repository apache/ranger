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

import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class PerfDataRecorder {
	private static final Logger LOG  = LoggerFactory.getLogger(PerfDataRecorder.class);
	private static final Logger PERF = RangerPerfTracer.getPerfLogger(PerfDataRecorder.class);

	private static volatile PerfDataRecorder instance;
	final private Map<String, PerfStatistic> perfStatistics = Collections.synchronizedMap(new HashMap<>());
	private RangerReadWriteLock lock = null;

	public static void initialize(List<String> names) {
		initialize(true, 0, false, names);
	}

	public static void initialize(final boolean useRecorder, final int collectionIntervalInSeconds, final boolean usePerfDataLock, List<String> names) {
		if (useRecorder) {
			if (instance == null) {
				synchronized (PerfDataRecorder.class) {
					if (instance == null) {
						instance = new PerfDataRecorder(names);
						instance.lock = new RangerReadWriteLock(usePerfDataLock);
						if (collectionIntervalInSeconds > 0) {
							Thread statDumper = new StatisticsDumper(collectionIntervalInSeconds);
							statDumper.setName("Perf-Statistics-Dumper");
							statDumper.setDaemon(true);
							statDumper.start();
						}
					}
				}
			}
		}
	}

	public static boolean collectStatistics() {
		return instance != null;
	}

	public static void printStatistics() {
		if (instance != null) {
			instance.dumpStatistics();
		}
	}

	public static void clearStatistics() {
		if (instance != null) {
			try (RangerReadWriteLock.RangerLock writeLock = instance.lock.getWriteLock()) {
				instance.clear();
			}
		}
	}

	public static void recordStatistic(String tag, long cpuTime, long elapsedTime) {
		if (instance != null) {
			instance.record(tag, cpuTime, elapsedTime);
		}
	}

	private void dumpStatistics() {
		List<String> tags;

		try (RangerReadWriteLock.RangerLock readLock = lock.getReadLock()) {
			tags = new ArrayList<>(perfStatistics.keySet());
		}

		Collections.sort(tags);

		for (String tag : tags) {
			PerfStatistic perfStatistic = perfStatistics.get(tag);

			long averageTimeSpentCpu = 0L;
			long averageTimeSpent = 0L;

			if (perfStatistic.numberOfInvocations.get() != 0L) {
				averageTimeSpentCpu = perfStatistic.microSecondsSpentCpu.get()/perfStatistic.numberOfInvocations.get();
			}

			if (perfStatistic.numberOfInvocations.get() != 0L) {
				averageTimeSpent = perfStatistic.microSecondsSpent.get()/perfStatistic.numberOfInvocations.get();
			}

			String logMsg = "[" + tag + "]" +
                             " execCount: " + perfStatistic.numberOfInvocations.get() +
					", totalTimeTakenCpu: " + perfStatistic.microSecondsSpentCpu.get() + " μs" +
					", maxTimeTakenCpu: " + perfStatistic.maxTimeSpentCpu.get() + " μs" +
					", minTimeTakenCpu: " + perfStatistic.minTimeSpentCpu.get() + " μs" +
					", avgTimeTakenCpu: " + averageTimeSpentCpu + " μs" +
                             ", totalTimeTaken: " + perfStatistic.microSecondsSpent.get() + " μs" +
                             ", maxTimeTaken: " + perfStatistic.maxTimeSpent.get() + " μs" +
                             ", minTimeTaken: " + perfStatistic.minTimeSpent.get() + " μs" +
                             ", avgTimeTaken: " + averageTimeSpent + " μs";

			LOG.info(logMsg);
			PERF.debug(logMsg);
		}
	}

	private void clear() {
		perfStatistics.clear();
	}

	private void record(String tag, long cpuTime, long elapsedTime) {
		try (RangerReadWriteLock.RangerLock writeLock = lock.getWriteLock()) {

			PerfStatistic perfStatistic = perfStatistics.get(tag);

			if (perfStatistic == null) {
				synchronized (PerfDataRecorder.class) {
					perfStatistic = perfStatistics.get(tag);

					if (perfStatistic == null) {
						perfStatistic = new PerfStatistic();
						perfStatistics.put(tag, perfStatistic);
					}
				}
			}

			perfStatistic.addPerfDataItem(cpuTime, elapsedTime);

		}
	}

	private PerfDataRecorder(List<String> names) {
		if (CollectionUtils.isNotEmpty(names)) {
			for (String name : names) {
				// Create structure
				perfStatistics.put(name, new PerfStatistic());
			}
		}
	}

	public static Map<String, PerfStatistic> exposeStatistics() {
		if (instance != null) {
			return ImmutableMap.copyOf(instance.perfStatistics);
		}
		return ImmutableMap.of();
	}

	public static class PerfStatistic {
		private AtomicLong numberOfInvocations = new AtomicLong(0L);

		private AtomicLong microSecondsSpentCpu = new AtomicLong(0L);
		private AtomicLong minTimeSpentCpu = new AtomicLong(Long.MAX_VALUE);
		private AtomicLong maxTimeSpentCpu = new AtomicLong(Long.MIN_VALUE);

		private AtomicLong microSecondsSpent = new AtomicLong(0L);
		private AtomicLong minTimeSpent = new AtomicLong(Long.MAX_VALUE);
		private AtomicLong maxTimeSpent = new AtomicLong(Long.MIN_VALUE);

		void addPerfDataItem(final long cpuTime, final long timeTaken) {
			numberOfInvocations.getAndIncrement();
			microSecondsSpentCpu.getAndAdd(cpuTime);
			microSecondsSpent.getAndAdd(timeTaken);

			long min = minTimeSpentCpu.get();
			if(cpuTime < min) {
				minTimeSpentCpu.compareAndSet(min, cpuTime);
			}
			min = minTimeSpent.get();
			if(timeTaken < min) {
				minTimeSpent.compareAndSet(min, timeTaken);
			}

			long max = maxTimeSpentCpu.get();
			if(cpuTime > max) {
				maxTimeSpentCpu.compareAndSet(max, cpuTime);
			}
			max = maxTimeSpent.get();
			if(timeTaken > max) {
				maxTimeSpent.compareAndSet(max, timeTaken);
			}
		}

		public long getNumberOfInvocations() {
			return numberOfInvocations.get();
		}

		public long getMicroSecondsSpentCpu() {
			return microSecondsSpentCpu.get();
		}

		public long getMinTimeSpentCpu() {
			return minTimeSpentCpu.get();
		}

		public long getMaxTimeSpentCpu() {
			return maxTimeSpentCpu.get();
		}

		public long getMicroSecondsSpent() {
			return microSecondsSpent.get();
		}

		public long getMinTimeSpent() {
			return minTimeSpent.get();
		}

		public long getMaxTimeSpent() {
			return maxTimeSpent.get();
		}
	}

	private static class StatisticsDumper extends Thread {
		final int collectionIntervalInSeconds;
		StatisticsDumper(final int collectionIntervalInSeconds) {
			this.collectionIntervalInSeconds = collectionIntervalInSeconds;
		}
		@Override
		public void run() {
			while (true) {
				try {
					sleep(collectionIntervalInSeconds * 1000);
					printStatistics();
					clearStatistics();
				} catch (InterruptedException exception) {
					printStatistics();
					LOG.warn("Thread[" + this.getName() + "] was interrupted. Returning from thread. Performance statistics will NOT be dumped periodically!!");
					break;
				}
			}
		}
	}
}
