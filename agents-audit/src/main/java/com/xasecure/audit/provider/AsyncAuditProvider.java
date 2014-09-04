package com.xasecure.audit.provider;

import java.util.LinkedList;
import java.util.Queue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.xasecure.audit.model.AuditEventBase;

public class AsyncAuditProvider extends MultiDestAuditProvider implements
		Runnable {

	private static final Log LOG = LogFactory.getLog(AsyncAuditProvider.class);

	private static int sThreadCount = 0;

	private Queue<AuditEventBase> mQueue = new LinkedList<AuditEventBase>();
	private Thread mThread = null;
	private boolean mStopThread = false;
	private int mMaxQueueSize = -1;
	private int mResumeQueueSize = 0;
	private int mMaxFlushInterval = -1;
	private long mFirstDropTime = 0;
	private int mDropCount = 0;

	// Summary of logs handled
	private long lifeTimeLogCount = 0; // Total count, including drop count
	private long lifeTimeDropCount = 0;
	private long intervalLogCount = 0;
	private long intervalDropCount = 0;
	private long lastIntervalLogTime = System.currentTimeMillis();
	private int intervalLogDurationMS = 60000;

	public AsyncAuditProvider() {
		LOG.info("AsyncAuditProvider: creating..");
	}

	public AsyncAuditProvider(AuditProvider provider) {
		LOG.info("AsyncAuditProvider: creating..");

		addAuditProvider(provider);
	}

	public int getIntervalLogDurationMS() {
		return intervalLogDurationMS;
	}

	public void setIntervalLogDurationMS(int intervalLogDurationMS) {
		this.intervalLogDurationMS = intervalLogDurationMS;
	}

	public void setMaxQueueSize(int maxQueueSize) {
		mMaxQueueSize = maxQueueSize > 0 ? maxQueueSize : Integer.MAX_VALUE;
	}

	public void setMaxFlushInterval(int maxFlushInterval) {
		mMaxFlushInterval = maxFlushInterval;
	}

	public void setResumeQueueSize(int resumeQueueSize) {
		mResumeQueueSize = resumeQueueSize > 0 ? resumeQueueSize : 0;
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

		synchronized (mQueue) {
			mQueue.notify();
		}

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

		try {
			while (!mStopThread) {
				AuditEventBase event = dequeueEvent();

				if (event != null) {
					super.log(event);
				} else {
					flush();
				}
			}

			flush();
		} catch (Exception excp) {
			LOG.error("AsyncAuditProvider.run()", excp);
		}

		LOG.info("<== AsyncAuditProvider.run()");
	}

	private void queueEvent(AuditEventBase event) {
		synchronized (mQueue) {

			// Running this within synchronized block to avoid multiple
			// logs from different threads
			// Running this upfront so the log interval is fixed

			// Log summary if required
			logSummaryIfRequired();

			// Increase counts
			lifeTimeLogCount++;
			intervalLogCount++;

			int maxQueueSize = mMaxQueueSize;

			// if we are currently dropping, don't resume until the queue size
			// goes to mResumeQueueSize
			if (mDropCount > 0) {
				maxQueueSize = (mResumeQueueSize < maxQueueSize ? mResumeQueueSize
						: (int) (maxQueueSize / 100.0 * 80)) + 1;
			}

			if (mQueue.size() < maxQueueSize) {
				mQueue.add(event);
				mQueue.notify();

				if (mDropCount > 0) {
					String pauseDuration = getTimeDiffStr(System.currentTimeMillis(),
							mFirstDropTime);

					LOG.warn("AsyncAuditProvider: resumes after dropping "
							+ mDropCount + " audit logs in past "
							+ pauseDuration);

					mDropCount = 0;
					mFirstDropTime = 0;
				}
			} else {
				if (mDropCount == 0) {
					mFirstDropTime = System.currentTimeMillis();
	
					LOG.warn("AsyncAuditProvider: queue size is at max ("
							+ mMaxQueueSize
							+ "); further audit logs will be dropped until queue clears up");
				}

				mDropCount++;
				lifeTimeDropCount++;
				intervalDropCount++;
			}

		}
	}

	private AuditEventBase dequeueEvent() {
		synchronized (mQueue) {
			while (mQueue.isEmpty()) {
				if (mStopThread) {
					return null;
				}
				try {
					// Log summary if required
					logSummaryIfRequired();

					if (mMaxFlushInterval > 0 && isFlushPending()) {
						long timeTillNextFlush = getTimeTillNextFlush();

						if (timeTillNextFlush <= 0) {
							return null; // force flush
						}

						mQueue.wait(timeTillNextFlush);
					} else {
						// Let's wake up for summary logging
						long waitTime = intervalLogDurationMS
								- (System.currentTimeMillis() - lastIntervalLogTime);
						waitTime = waitTime <= 0 ? intervalLogDurationMS
								: waitTime;
						mQueue.wait(waitTime);
					}
				} catch (InterruptedException excp) {
					LOG.error("AsyncAuditProvider.dequeueEvent()", excp);

					return null;
				}
			}

			AuditEventBase ret = mQueue.remove();

			return ret;
		}
	}

	private void logSummaryIfRequired() {
		if (System.currentTimeMillis() - lastIntervalLogTime > intervalLogDurationMS) {
			// Log interval and life time summary

			if (intervalLogCount > 0) {
				LOG.info("AsyncAuditProvider: Interval="
						+ formatTimeForLog(intervalLogDurationMS) + ", logs="
						+ intervalLogCount + ", dropped=" + intervalDropCount);
				LOG.info("AsyncAuditProvider: Process Lifetime, logs="
						+ lifeTimeLogCount + ", dropped=" + lifeTimeDropCount);
			}
			lastIntervalLogTime = System.currentTimeMillis();
			intervalLogCount = 0;
			intervalDropCount = 0;
		}
	}

	private boolean isEmpty() {
		synchronized (mQueue) {
			return mQueue.isEmpty();
		}
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
