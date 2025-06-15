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

import org.apache.ranger.audit.model.AuditEventBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class AsyncAuditProvider extends MultiDestAuditProvider implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(AsyncAuditProvider.class);

    private static final int mStopLoopIntervalSecs           = 1; // 1 second
    private static final int mWaitToCompleteLoopIntervalSecs = 1; // 1 second
    private static       int sThreadCount;

    private final BlockingQueue<AuditEventBase> mQueue;
    private final String                        mName;

    // Summary of logs handled
    private final AtomicLong lifeTimeInLogCount  = new AtomicLong(0); // Total count, including drop count
    private final AtomicLong lifeTimeOutLogCount = new AtomicLong(0);
    private final AtomicLong lifeTimeDropCount   = new AtomicLong(0);
    private final AtomicLong intervalInLogCount  = new AtomicLong(0);
    private final AtomicLong intervalOutLogCount = new AtomicLong(0);
    private final AtomicLong intervalDropCount   = new AtomicLong(0);

    private Thread mThread;
    private int    mMaxQueueSize         = 10 * 1024;
    private int    mMaxFlushInterval     = 5000; // 5 seconds
    private long   lastIntervalLogTime   = System.currentTimeMillis();
    private int    intervalLogDurationMS = 60000;
    private long   lastFlushTime         = System.currentTimeMillis();

    public AsyncAuditProvider(String name, int maxQueueSize, int maxFlushInterval) {
        LOG.info("AsyncAuditProvider({}): creating..", name);

        if (maxQueueSize < 1) {
            LOG.warn("AsyncAuditProvider({}): invalid maxQueueSize={}. will use default {}", name, maxQueueSize, mMaxQueueSize);

            maxQueueSize = mMaxQueueSize;
        }

        mName             = name;
        mMaxQueueSize     = maxQueueSize;
        mMaxFlushInterval = maxFlushInterval;

        mQueue = new ArrayBlockingQueue<>(mMaxQueueSize);
    }

    public AsyncAuditProvider(String name, int maxQueueSize, int maxFlushInterval, AuditHandler provider) {
        this(name, maxQueueSize, maxFlushInterval);

        addAuditProvider(provider);
    }

    @Override
    public void init(Properties props) {
        LOG.info("AsyncAuditProvider({}).init()", mName);

        super.init(props);
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
        LOG.info("==> AsyncAuditProvider.stop()");

        try {
            LOG.info("Interrupting child thread of {}...", mName);

            mThread.interrupt();

            while (mThread.isAlive()) {
                try {
                    LOG.info("Waiting for child thread of {} to exit.  Sleeping for {} secs", mName, mStopLoopIntervalSecs);

                    mThread.join(mStopLoopIntervalSecs * 1000L);
                } catch (InterruptedException e) {
                    LOG.warn("Interrupted while waiting for child thread to join!  Proceeding with stop", e);
                    break;
                }
            }

            super.stop();
        } finally {
            LOG.info("<== AsyncAuditProvider.stop()");
        }
    }

    @Override
    public void waitToComplete() {
        waitToComplete(0);

        super.waitToComplete();
    }

    public void waitToComplete(long maxWaitSeconds) {
        LOG.debug("==> AsyncAuditProvider.waitToComplete()");

        try {
            for (long waitTime = 0; !isEmpty() && (maxWaitSeconds <= 0 || maxWaitSeconds > waitTime); waitTime += mWaitToCompleteLoopIntervalSecs) {
                try {
                    LOG.info("{} messages yet to be flushed by {}.  Sleeoping for {} sec", mQueue.size(), mName, mWaitToCompleteLoopIntervalSecs);

                    Thread.sleep(mWaitToCompleteLoopIntervalSecs * 1000L);
                } catch (InterruptedException excp) {
                    // someone really wants service to exit, abandon unwritten audits and exit.
                    LOG.warn("Caught interrupted exception! {} messages still unflushed!  Won't wait for queue to flush, exiting...", mQueue.size(), excp);

                    break;
                }
            }
        } finally {
            LOG.debug("<== AsyncAuditProvider.waitToComplete()");
        }
    }

    public int getIntervalLogDurationMS() {
        return intervalLogDurationMS;
    }

    public void setIntervalLogDurationMS(int intervalLogDurationMS) {
        this.intervalLogDurationMS = intervalLogDurationMS;
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
                    lastFlushTime = System.currentTimeMillis();

                    flush();
                }
            } catch (InterruptedException excp) {
                LOG.info("AsyncAuditProvider.run - Interrupted!  Breaking out of while loop.");

                break;
            } catch (Exception excp) {
                logFailedEvent(event, excp);
            }
        }

        try {
            lastFlushTime = System.currentTimeMillis();

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

        if (!mQueue.offer(event)) {
            lifeTimeDropCount.incrementAndGet();
            intervalDropCount.incrementAndGet();
        }
    }

    private AuditEventBase dequeueEvent() throws InterruptedException {
        AuditEventBase ret = mQueue.poll();

        while (ret == null) {
            logSummaryIfRequired();

            if (mMaxFlushInterval > 0) {
                long timeTillNextFlush = getTimeTillNextFlush();

                if (timeTillNextFlush <= 0) {
                    break; // force flush
                }

                ret = mQueue.poll(timeTillNextFlush, TimeUnit.MILLISECONDS);
            } else {
                // Let's wake up for summary logging
                long waitTime = intervalLogDurationMS - (System.currentTimeMillis() - lastIntervalLogTime);

                if (waitTime <= 0) {
                    waitTime = intervalLogDurationMS;
                }

                ret = mQueue.poll(waitTime, TimeUnit.MILLISECONDS);
            }
        }

        if (ret != null) {
            lifeTimeOutLogCount.incrementAndGet();
            intervalOutLogCount.incrementAndGet();
        }

        logSummaryIfRequired();

        return ret;
    }

    private void logSummaryIfRequired() {
        long intervalSinceLastLog = System.currentTimeMillis() - lastIntervalLogTime;

        if (intervalSinceLastLog > intervalLogDurationMS) {
            if (intervalInLogCount.get() > 0 || intervalOutLogCount.get() > 0) {
                long queueSize = mQueue.size();

                LOG.info("AsyncAuditProvider-stats:{}: past {}: inLogs={}, outLogs={}, dropped={}, currentQueueSize={}", mName, formatIntervalForLog(intervalSinceLastLog), intervalInLogCount, intervalOutLogCount, intervalDropCount, queueSize);
                LOG.info("AsyncAuditProvider-stats:{}: process lifetime: inLogs={}, outLogs={}, dropped={}", mName, lifeTimeInLogCount, lifeTimeOutLogCount, lifeTimeDropCount);
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

    private long getTimeTillNextFlush() {
        long timeTillNextFlush = mMaxFlushInterval;

        if (mMaxFlushInterval > 0) {
            if (lastFlushTime != 0) {
                long timeSinceLastFlush = System.currentTimeMillis() - lastFlushTime;

                if (timeSinceLastFlush >= mMaxFlushInterval) {
                    timeTillNextFlush = 0;
                } else {
                    timeTillNextFlush = mMaxFlushInterval - timeSinceLastFlush;
                }
            }
        }

        return timeTillNextFlush;
    }
}
