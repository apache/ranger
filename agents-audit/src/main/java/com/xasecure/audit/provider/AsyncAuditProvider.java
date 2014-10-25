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

 package com.xasecure.audit.provider;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.xasecure.audit.model.AuditEventBase;

public class AsyncAuditProvider extends MultiDestAuditProvider implements
		Runnable {

	private static final Log LOG = LogFactory.getLog(AsyncAuditProvider.class);

	private static int sThreadCount = 0;

	private BlockingQueue<AuditEventBase> mQueue = null;
	private Thread  mThread           = null;
	private boolean mStopThread       = false;
	private String  mName             = null;
	private int     mMaxQueueSize     = -1;
	private int     mMaxFlushInterval = -1;

	// Summary of logs handled
	private AtomicLong lifeTimeInLogCount  = new AtomicLong(0); // Total count, including drop count
	private AtomicLong lifeTimeOutLogCount = new AtomicLong(0);
	private AtomicLong lifeTimeDropCount   = new AtomicLong(0);
	private AtomicLong intervalInLogCount  = new AtomicLong(0);
	private AtomicLong intervalOutLogCount = new AtomicLong(0);
	private AtomicLong intervalDropCount   = new AtomicLong(0);
	private long lastIntervalLogTime   = System.currentTimeMillis();
	private int  intervalLogDurationMS = 60000;

	public AsyncAuditProvider(String name, int maxQueueSize, int maxFlushInterval) {
		LOG.info("AsyncAuditProvider(" + name + "): creating..");

		mName             = name;
		mMaxQueueSize     = maxQueueSize;
		mMaxFlushInterval = maxFlushInterval;

		mQueue = new ArrayBlockingQueue<AuditEventBase>(mMaxQueueSize);
	}

	public AsyncAuditProvider(String name, int maxQueueSize, int maxFlushInterval, AuditProvider provider) {
		this(name, maxQueueSize, maxFlushInterval);

		addAuditProvider(provider);
	}

	public int getIntervalLogDurationMS() {
		return intervalLogDurationMS;
	}

	public void setIntervalLogDurationMS(int intervalLogDurationMS) {
		this.intervalLogDurationMS = intervalLogDurationMS;
	}

	@Override
	public void log(AuditEventBase event) {
		LOG.debug("AsyncAuditProvider.logEvent(AuditEventBase)");

		queueEvent(event);
	}

	@Override
	public void start() {
		mThread = new Thread(this, "AsyncAuditProvider" + (++sThreadCount));

		mThread.setDaemon(true);
		mThread.start();

		super.start();
	}

	@Override
	public void stop() {
		mStopThread = true;

		try {
			mThread.join();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		super.stop();
	}

	@Override
	public void waitToComplete() {
		waitToComplete(0);

		super.waitToComplete();
	}

	@Override
	public void run() {
		LOG.info("==> AsyncAuditProvider.run()");

		while (!mStopThread) {
			try {
				AuditEventBase event = dequeueEvent();

				if (event != null) {
					super.log(event);
				} else {
					flush();
				}
			} catch (Exception excp) {
				LOG.error("AsyncAuditProvider.run()", excp);
			}
		}

		try {
			flush();
		} catch (Exception excp) {
			LOG.error("AsyncAuditProvider.run()", excp);
		}

		LOG.info("<== AsyncAuditProvider.run()");
	}

	private void queueEvent(AuditEventBase event) {
		// Increase counts
		lifeTimeInLogCount.incrementAndGet();
		intervalInLogCount.incrementAndGet();

		if(! mQueue.offer(event)) {
			lifeTimeDropCount.incrementAndGet();
			intervalDropCount.incrementAndGet();
		}
	}

	private AuditEventBase dequeueEvent() {
		AuditEventBase ret = mQueue.poll();

		try {
			while(ret == null && !mStopThread) {
				logSummaryIfRequired();

				if (mMaxFlushInterval > 0 && isFlushPending()) {
					long timeTillNextFlush = getTimeTillNextFlush();

					if (timeTillNextFlush <= 0) {
						break; // force flush
					}

					ret = mQueue.poll(timeTillNextFlush, TimeUnit.MILLISECONDS);
				} else {
					// Let's wake up for summary logging
					long waitTime = intervalLogDurationMS - (System.currentTimeMillis() - lastIntervalLogTime);
					waitTime = waitTime <= 0 ? intervalLogDurationMS : waitTime;

					ret = mQueue.poll(waitTime, TimeUnit.MILLISECONDS);
				}
			}
		} catch(InterruptedException excp) {
			LOG.error("AsyncAuditProvider.dequeueEvent()", excp);
		}

		if(ret != null) {
			lifeTimeOutLogCount.incrementAndGet();
			intervalOutLogCount.incrementAndGet();
		}

		logSummaryIfRequired();

		return ret;
	}

	private void logSummaryIfRequired() {
		long intervalSinceLastLog = System.currentTimeMillis() - lastIntervalLogTime;

		if (intervalSinceLastLog > intervalLogDurationMS) {
			if (intervalInLogCount.get() > 0 || intervalOutLogCount.get() > 0 ) {
				long queueSize = mQueue.size();

				LOG.info("AsyncAuditProvider-stats:" + mName + ": past " + formatTimeForLog(intervalSinceLastLog)
						+ ": inLogs=" + intervalInLogCount.get()
						+ ", outLogs=" + intervalOutLogCount.get()
						+ ", dropped=" + intervalDropCount.get()
						+ ", currentQueueSize=" + queueSize);

				LOG.info("AsyncAuditProvider-stats:" + mName + ": process lifetime"
						+ ": inLogs=" + lifeTimeInLogCount.get()
						+ ", outLogs=" + lifeTimeOutLogCount.get()
						+ ", dropped=" + lifeTimeDropCount.get());
			}

			lastIntervalLogTime = System.currentTimeMillis();
			intervalInLogCount.set(0);
			intervalOutLogCount.set(0);
			intervalDropCount.set(0);
		}
	}

	private boolean isEmpty() {
		return mQueue.isEmpty();
	}

	private void waitToComplete(long maxWaitSeconds) {
		LOG.debug("==> AsyncAuditProvider.waitToComplete()");

		for (long waitTime = 0; !isEmpty()
				&& (maxWaitSeconds <= 0 || maxWaitSeconds > waitTime); waitTime++) {
			try {
				Thread.sleep(1000);
			} catch (Exception excp) {
				// ignore
			}
		}

		LOG.debug("<== AsyncAuditProvider.waitToComplete()");
	}

	private String getTimeDiffStr(long time1, long time2) {
		long timeInMs = Math.abs(time1 - time2);
		return formatTimeForLog(timeInMs);
	}

	private String formatTimeForLog(long timeInMs) {
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

	private long getTimeTillNextFlush() {
		long timeTillNextFlush = mMaxFlushInterval;

		if (mMaxFlushInterval > 0) {
			long lastFlushTime = getLastFlushTime();

			if (lastFlushTime != 0) {
				long timeSinceLastFlush = System.currentTimeMillis()
						- lastFlushTime;

				if (timeSinceLastFlush >= mMaxFlushInterval)
					timeTillNextFlush = 0;
				else
					timeTillNextFlush = mMaxFlushInterval - timeSinceLastFlush;
			}
		}

		return timeTillNextFlush;
	}
}
