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

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ranger.audit.model.AuditEventBase;

public class AsyncAuditProvider extends MultiDestAuditProvider implements
		Runnable {

	private static final Log LOG = LogFactory.getLog(AsyncAuditProvider.class);

	private static int sThreadCount = 0;

	private BlockingQueue<AuditEventBase> mQueue = null;
	private Thread  mThread           = null;
	private String  mName             = null;
	private int     mMaxQueueSize     = 10 * 1024;
	private int     mMaxFlushInterval = 5000; // 5 seconds

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

		if(maxQueueSize < 1) {
			LOG.warn("AsyncAuditProvider(" + name + "): invalid maxQueueSize=" + maxQueueSize + ". will use default " + mMaxQueueSize);

			maxQueueSize = mMaxQueueSize;
		}

		mName             = name;
		mMaxQueueSize     = maxQueueSize;
		mMaxFlushInterval = maxFlushInterval;

		mQueue = new ArrayBlockingQueue<AuditEventBase>(mMaxQueueSize);
	}

	public AsyncAuditProvider(String name, int maxQueueSize, int maxFlushInterval, AuditHandler provider) {
		this(name, maxQueueSize, maxFlushInterval);

		addAuditProvider(provider);
	}

	@Override
	public void init(Properties props) {
		LOG.info("AsyncAuditProvider(" + mName + ").init()");

		super.init(props);
	}

	public int getIntervalLogDurationMS() {
		return intervalLogDurationMS;
	}

	public void setIntervalLogDurationMS(int intervalLogDurationMS) {
		this.intervalLogDurationMS = intervalLogDurationMS;
	}

	@Override
	public boolean log(AuditEventBase event) {
		LOG.debug("AsyncAuditProvider.logEvent(AuditEventBase)");

		queueEvent(event);
		return true;
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
		mThread.interrupt();

		try {
			mThread.join();
		} catch (InterruptedException excp) {
			LOG.error("AsyncAuditProvider.stop(): failed while waiting for thread to exit", excp);
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

		while (true) {
			AuditEventBase event = null;
			try {
				event = dequeueEvent();

				if (event != null) {
					super.log(event);
				} else {
					flush();
				}
			} catch (InterruptedException excp) {
				break;
			} catch (Exception excp) {
				logFailedEvent(event, excp);
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

	private AuditEventBase dequeueEvent() throws InterruptedException {
		AuditEventBase ret = mQueue.poll();

		while(ret == null) {
			logSummaryIfRequired();

//			if (mMaxFlushInterval > 0 && isFlushPending()) {
//				long timeTillNextFlush = getTimeTillNextFlush();
//
//				if (timeTillNextFlush <= 0) {
//					break; // force flush
//				}
//
//				ret = mQueue.poll(timeTillNextFlush, TimeUnit.MILLISECONDS);
//			} else {
				// Let's wake up for summary logging
				long waitTime = intervalLogDurationMS - (System.currentTimeMillis() - lastIntervalLogTime);
				waitTime = waitTime <= 0 ? intervalLogDurationMS : waitTime;

				ret = mQueue.poll(waitTime, TimeUnit.MILLISECONDS);
//			}
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

				LOG.info("AsyncAuditProvider-stats:" + mName + ": past " + formatIntervalForLog(intervalSinceLastLog)
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

	public void waitToComplete(long maxWaitSeconds) {
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

//	private long getTimeTillNextFlush() {
//		long timeTillNextFlush = mMaxFlushInterval;
//
//		if (mMaxFlushInterval > 0) {
//			long lastFlushTime = getLastFlushTime();
//
//			if (lastFlushTime != 0) {
//				long timeSinceLastFlush = System.currentTimeMillis()
//						- lastFlushTime;
//
//				if (timeSinceLastFlush >= mMaxFlushInterval)
//					timeTillNextFlush = 0;
//				else
//					timeTillNextFlush = mMaxFlushInterval - timeSinceLastFlush;
//			}
//		}
//
//		return timeTillNextFlush;
//	}
}
