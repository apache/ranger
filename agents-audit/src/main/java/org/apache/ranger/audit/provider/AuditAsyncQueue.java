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

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.LinkedTransferQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ranger.audit.model.AuditEventBase;

/**
 * This is a non-blocking queue with no limit on capacity.
 */
public class AuditAsyncQueue extends BaseAuditProvider implements Runnable {
	private static final Log logger = LogFactory.getLog(AuditAsyncQueue.class);

	LinkedTransferQueue<AuditEventBase> queue = new LinkedTransferQueue<AuditEventBase>();
	Thread consumerThread = null;

	static int threadCount = 0;
	static final String DEFAULT_NAME = "async";

	public AuditAsyncQueue() {
		setName(DEFAULT_NAME);
	}

	public AuditAsyncQueue(AuditProvider consumer) {
		super(consumer);
		setName(DEFAULT_NAME);
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
		// Add to the queue and return ASAP
		if (queue.size() >= getMaxQueueSize()) {
			return false;
		}
		queue.add(event);
		addLifeTimeInLogCount(1);
		return true;
	}

	@Override
	public boolean log(Collection<AuditEventBase> events) {
		for (AuditEventBase event : events) {
			log(event);
		}
		return true;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.ranger.audit.provider.AuditProvider#start()
	 */
	@Override
	public void start() {
		if(consumer != null) {
			consumer.start();
		}
		
		consumerThread = new Thread(this, this.getClass().getName()
				+ (threadCount++));
		consumerThread.setDaemon(true);
		consumerThread.start();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.ranger.audit.provider.AuditProvider#stop()
	 */
	@Override
	public void stop() {
		setDrain(true);
		try {
			consumerThread.interrupt();
		} catch (Throwable t) {
			// ignore any exception
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.ranger.audit.provider.AuditProvider#isFlushPending()
	 */
	@Override
	public boolean isFlushPending() {
		if (queue.isEmpty()) {
			return consumer.isFlushPending();
		}
		return true;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Runnable#run()
	 */
	@Override
	public void run() {
		while (true) {
			try {
				AuditEventBase event = null;
				if (!isDrain()) {
					// For Transfer queue take() is blocking
					event = queue.take();
				} else {
					// For Transfer queue poll() is non blocking
					event = queue.poll();
				}
				if (event != null) {
					Collection<AuditEventBase> eventList = new ArrayList<AuditEventBase>();
					eventList.add(event);
					// TODO: Put a limit. Hard coding to 1000 (use batch size
					// property)
					queue.drainTo(eventList, 1000 - 1);
					consumer.log(eventList);
					eventList.clear();
				}
			} catch (InterruptedException e) {
				logger.info(
						"Caught exception in consumer thread. Mostly to about loop",
						e);
			} catch (Throwable t) {
				logger.error("Caught error during processing request.", t);
			}
			if (isDrain() && queue.isEmpty()) {
				break;
			}
		}
		try {
			// Call stop on the consumer
			consumer.stop();
		} catch (Throwable t) {
			logger.error("Error while calling stop on consumer.", t);
		}
	}

}
