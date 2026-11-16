/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ranger.plugin.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Consumer;

import static java.util.Objects.requireNonNull;

final class ServiceAccountUsersRefresher extends Thread {
    private static final Logger LOG = LoggerFactory.getLogger(ServiceAccountUsersRefresher.class);

    private final Callable<Map<String, Set<String>>> serviceAccountUsersLoader;
    private final Consumer<Map<String, Set<String>>> receiver;
    private final long                                pollingIntervalMs;
    private final BlockingQueue<DownloadTrigger>      downloadQueue = new LinkedBlockingQueue<>();
    private       Timer                               downloadTimer;

    ServiceAccountUsersRefresher(Callable<Map<String, Set<String>>> serviceAccountUsersLoader, Consumer<Map<String, Set<String>>> receiver, long pollingIntervalMs) {
        if (pollingIntervalMs <= 0) {
            throw new IllegalArgumentException("pollingIntervalMs must be greater than zero");
        }

        this.serviceAccountUsersLoader = requireNonNull(serviceAccountUsersLoader, "serviceAccountUsersLoader");
        this.receiver                  = requireNonNull(receiver, "receiver");
        this.pollingIntervalMs         = pollingIntervalMs;

        setName("ServiceAccountUsersRefresher-" + getId());
        setDaemon(true);
    }

    void startRefresher() {
        refresh();

        try {
            start();

            downloadTimer = new Timer("serviceAccountUsersDownloadTimer", true);
            downloadTimer.schedule(new DownloaderTask(downloadQueue), pollingIntervalMs, pollingIntervalMs);

            LOG.info("Started service-account user refresher with polling interval {} ms", pollingIntervalMs);
        } catch (RuntimeException excp) {
            LOG.error("Failed to start service-account user refresher", excp);

            stopRefresher();

            throw excp;
        }
    }

    void stopRefresher() {
        boolean wasStarted = downloadTimer != null || isAlive();
        Timer downloadTimer = this.downloadTimer;

        this.downloadTimer = null;

        if (downloadTimer != null) {
            downloadTimer.cancel();
        }

        if (isAlive()) {
            interrupt();

            boolean interrupted = false;
            boolean joined      = false;

            while (!joined) {
                try {
                    join();

                    joined = true;
                } catch (InterruptedException excp) {
                    LOG.warn("Interrupted while waiting for service-account user refresher to stop", excp);

                    interrupted = true;
                }
            }

            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }

        if (wasStarted) {
            LOG.info("Stopped service-account user refresher");
        }
    }

    @Override
    public void run() {
        while (true) {
            DownloadTrigger trigger = null;

            try {
                trigger = downloadQueue.take();

                refresh();
            } catch (InterruptedException excp) {
                LOG.info("Service-account user refresher interrupted; exiting");

                break;
            } finally {
                if (trigger != null) {
                    trigger.signalCompletion();
                }
            }
        }
    }

    void refresh() {
        try {
            Map<String, Set<String>> serviceAccountUsers = requireNonNull(serviceAccountUsersLoader.call(),
                    "Ranger Admin returned null service-account users");

            receiver.accept(serviceAccountUsers);

            LOG.debug("Refreshed service-account users for {} services", serviceAccountUsers.size());
        } catch (Exception excp) {
            LOG.warn("Failed to refresh service-account users; retaining the last successful snapshot", excp);
        }
    }
}
