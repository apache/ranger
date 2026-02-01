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

package org.apache.ranger.plugin.policyengine;

import com.sun.net.httpserver.HttpServer;
import org.apache.hadoop.conf.Configuration;
import org.apache.ranger.plugin.util.RangerRESTClient;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class TestRangerRESTClientDeadlock {
    private static final Logger LOG = LoggerFactory.getLogger(TestRangerRESTClientDeadlock.class);
    private HttpServer httpServer;
    private RangerRESTClient restClient;

    private void setupServerAndClient(CountDownLatch serverLatch) throws Exception {
        httpServer = HttpServer.create(new InetSocketAddress(0), 0);
        httpServer.createContext("/", exchange -> {
            LOG.info("Server: Received request, returning 503...");
            serverLatch.countDown(); // Signal the test thread
            exchange.sendResponseHeaders(503, -1);
            exchange.close();
        });
        httpServer.start();
        String serverUrl = "http://localhost:" + httpServer.getAddress().getPort();
        Configuration conf = new Configuration();
        restClient = new RangerRESTClient(serverUrl, null, conf);
        restClient.setMaxRetryAttempts(10);
        restClient.setRetryIntervalMs(1000);
    }

    @AfterEach
    public void tearDown() {
        if (httpServer != null) {
            httpServer.stop(0);
        }
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    public void testDeadlockWhenStoppingDuringRetry() throws Exception {
        CountDownLatch serverReceivedRequest = new CountDownLatch(1);
        CountDownLatch retryStarted = new CountDownLatch(1);
        CountDownLatch joinCompleted = new CountDownLatch(1);
        AtomicBoolean deadlockDetected = new AtomicBoolean(false);

        setupServerAndClient(serverReceivedRequest);
        Thread refresherThread = getRefresherThread(retryStarted);

        assertTrue(retryStarted.await(5, TimeUnit.SECONDS), "Retry should have started");
        assertTrue(serverReceivedRequest.await(10, TimeUnit.SECONDS), "Server should have received a request");

        refresherThread.interrupt();

        Thread joinThread = getJoinThread(refresherThread, joinCompleted);
        // Wait for join to complete (with timeout to detect deadlock)
        boolean joined = joinCompleted.await(15, TimeUnit.SECONDS);

        if (!joined) {
            deadlockDetected.set(true);
            LOG.error("DEADLOCK DETECTED: join() did not complete within 15 seconds!");
            LOG.error("This confirms that RangerRESTClient ignores InterruptedException during retry.");
            joinThread.interrupt();
            refresherThread.interrupt();
            fail("Deadlock detected: Thread.join() did not complete within timeout. " +
                    "This reproduces the issue where RangerRESTClient ignores InterruptedException " +
                    "during retry, causing PolicyRefresher.stopRefresher() to hang indefinitely.");
        }
        assertFalse(deadlockDetected.get(), "No deadlock should occur.");
        LOG.info("<== Test completed - no deadlock occurred.");
    }

    private static Thread getJoinThread(Thread refresherThread, CountDownLatch joinCompleted) {
        Thread joinThread = new Thread(() -> {
            try {
                long startTime = System.currentTimeMillis();
                refresherThread.join();
                long duration = System.currentTimeMillis() - startTime;
                LOG.info("MainThread: join() completed in {} ms", duration);
                joinCompleted.countDown();
            } catch (InterruptedException e) {
                LOG.error("MainThread: join() was interrupted", e);
            }
        });
        joinThread.start();
        return joinThread;
    }

    private Thread getRefresherThread(CountDownLatch retryStarted) {
        Thread refresherThread = new Thread(() -> {
            try {
                LOG.info("RefresherThread: Starting GET request (will trigger retries)...");
                retryStarted.countDown();
                restClient.get("/service/policies/download", null);
                LOG.info("RefresherThread: GET request completed (unexpected)");
            } catch (Exception e) {
                LOG.info("RefresherThread: Exited with exception: {}", e.getMessage());
            }
        }, "RefresherThread");
        refresherThread.start();
        return refresherThread;
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    public void testRetryLogicWorksNormally() throws Exception {
        LOG.info("==> Starting test to verify normal retry behavior");
        CountDownLatch serverReceivedRequest = new CountDownLatch(1);
        setupServerAndClient(serverReceivedRequest);
        restClient.setMaxRetryAttempts(3);
        restClient.setRetryIntervalMs(1000);
        long startTime = System.currentTimeMillis();
        try {
            restClient.get("/service/policies/download", null);
            fail("Expected exception due to 503 errors");
        } catch (Exception e) {
            long duration = System.currentTimeMillis() - startTime;
            LOG.info("Request failed after {}ms (expected): {}", duration, e.getMessage());
            assertTrue(duration >= 3000, "Should have retried with delays");
        }
        LOG.info("<== Test passed - retry logic works normally");
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    public void testInterruptWorksWithMultipleURLs() throws Exception {
        LOG.info("==> Starting test with multiple URLs");
        CountDownLatch serverReceivedRequest = new CountDownLatch(1);
        setupServerAndClient(serverReceivedRequest);
        // Create a second server
        HttpServer httpServer2 = HttpServer.create(new InetSocketAddress(0), 0);
        httpServer2.createContext("/", exchange -> {
            LOG.info("Server2: Received request, returning 503...");
            exchange.sendResponseHeaders(503, -1);
            exchange.close();
        });
        httpServer2.start();

        try {
            String serverUrl1 = "http://localhost:" + httpServer.getAddress().getPort();
            String serverUrl2 = "http://localhost:" + httpServer2.getAddress().getPort();
            String multiUrl = serverUrl1 + "," + serverUrl2;

            Configuration conf = new Configuration();
            RangerRESTClient multiUrlClient = new RangerRESTClient(multiUrl, null, conf);
            multiUrlClient.setMaxRetryAttempts(5);
            multiUrlClient.setRetryIntervalMs(1000);

            CountDownLatch started = new CountDownLatch(1);
            Thread testThread = new Thread(() -> {
                try {
                    started.countDown();
                    multiUrlClient.get("/test", null);
                } catch (Exception e) {
                    LOG.info("Multi-URL thread exited: {}", e.getMessage());
                }
            });

            testThread.start();
            assertTrue(started.await(5, TimeUnit.SECONDS));
            assertTrue(serverReceivedRequest.await(10, TimeUnit.SECONDS), "Server should have received a request");

            long startTime = System.currentTimeMillis();
            testThread.interrupt();
            testThread.join(5000);
            long duration = System.currentTimeMillis() - startTime;

            assertFalse(testThread.isAlive(), "Thread should exit even with multiple URLs");
            assertTrue(duration < 5000, "Should exit quickly: " + duration + "ms");

            LOG.info("<== Test passed - interrupt works with multiple URLs");
        } finally {
            httpServer2.stop(0);
        }
    }
}
