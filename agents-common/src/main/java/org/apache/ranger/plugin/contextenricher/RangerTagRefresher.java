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

package org.apache.ranger.plugin.contextenricher;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public abstract class RangerTagRefresher extends Thread implements RangerTagRetriever {
	private static final Log LOG = LogFactory.getLog(RangerTagRefresher.class);

	private long pollingIntervalMs;
	private volatile boolean isStarted = false;

	protected RangerTagRefresher(final long pollingIntervalMs) {
		this.pollingIntervalMs = pollingIntervalMs;
	}

	public final long getPollingIntervalMs() {
		return pollingIntervalMs;
	}

	public final void setPollingIntervalMs(long pollingIntervalMs) {
		this.pollingIntervalMs = pollingIntervalMs;
	}

	public boolean getIsStarted() { return isStarted; }

	public final void cleanup() { this.stopRetriever(); }

	@Override
	public void run() {

		if (LOG.isDebugEnabled()) {
			LOG.debug("==> RangerTagRefresher(pollingIntervalMs=" + pollingIntervalMs + ").run()");
		}

		while (true) {

			retrieveTags();

			if (pollingIntervalMs > 0) {
				try {
					Thread.sleep(pollingIntervalMs);
				} catch (InterruptedException excp) {
					LOG.info("RangerTagRefresher(pollingIntervalMs=" + pollingIntervalMs + ").run() : interrupted! Exiting thread", excp);
					break;
				}
			} else {
				break;
			}

		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== RangerTagRefresher().run()");
		}
	}

	public final boolean startRetriever() {

		boolean ret = isStarted;

		if (!ret) {
			synchronized (this) {

				ret = isStarted;

				if (!ret) {
					try {
						super.start();
						ret = isStarted = true;
					} catch (Exception excp) {
						LOG.error("RangerTagRefresher.startRetriever() - failed to start, exception=" + excp);
					}
				}
			}
		}

		return ret;
	}

	public final void stopRetriever() {

		super.interrupt();

		try {
			super.join();
		} catch (InterruptedException excp) {
			LOG.error("RangerTagRefresher(): error while waiting for thread to exit", excp);
		}
	}
}

