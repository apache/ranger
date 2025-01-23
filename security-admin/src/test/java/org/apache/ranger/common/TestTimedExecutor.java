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

package org.apache.ranger.common;

import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestTimedExecutor {
    private static final Logger LOG = LoggerFactory.getLogger(TestTimedExecutor.class);

    static final String format = "%15s id: %2d";

    private final TimedExecutor executor = new TimedExecutor();

    private TimedExecutorConfigurator configurator;

    static void recordResult(ConcurrentMap<String, AtomicInteger> results, String key) {
        if (results.containsKey(key)) {
            results.get(key).incrementAndGet();
        } else {
            AtomicInteger previous = results.putIfAbsent(key, new AtomicInteger(1));
            if (previous != null) {  // a value was already associated with the key
                previous.incrementAndGet();
            }
        }
    }

    static void recordResult(ConcurrentMap<String, AtomicInteger> results, Exception e) {
        String exceptionName = e.getClass().getCanonicalName();
        recordResult(results, exceptionName);
    }

    @Before
    public void before() {
    }

    @Test
    public void test() throws InterruptedException {
        /*
         * Create a pool with 2 threads and queue size of 3 such that 6th item should get rejected right away due to capacity.
         */
        int poolSize  = 2;
        int queueSize = 3;
        configurator = new TimedExecutorConfigurator(poolSize, queueSize);
        // Just to avoid thread shutting down and restarting set keep alive to high value.
        executor.initialize(configurator);

        // now create 2 callables that would keep waiting unless we ask them to proceed
        // create an executor which would simulate simultaneous threads calling into executor to perform lookups
        ExecutorService       executorService = Executors.newCachedThreadPool();
        List<Future<Integer>> futures         = new ArrayList<>();
        /*
         * We would have 2 permits for 10 callables, such that
         * - 2 should succeed
         * - 5 should timeout (2 in pool + 3 in queue)
         * - 3 should get rejected.
         */
        Semaphore semaphore = new Semaphore(2);
        /*
         * We need a latch to keep track of when the processing is done so we can check the results of teh test
         */
        CountDownLatch latch = new CountDownLatch(10);
        // Callables will record exception in this map
        final ConcurrentMap<String, AtomicInteger> results = new ConcurrentHashMap<>();
        for (int i = 0; i < 10; i++) {
            LookupTask      lookupTask = new LookupTask(i, semaphore);
            TimedTask       timedTask  = new TimedTask(executor, lookupTask, 1, TimeUnit.SECONDS, results, latch);
            Future<Integer> aFuture    = executorService.submit(timedTask);
            futures.add(aFuture);
        }
        // Let's wait for the threads to finish
        LOG.debug("Starting to wait for threadpool to finish");
        latch.await();
        /*
         * depending on how threads get scheduled the count in results would vary, except we know for sure that.
         * - 2 must succeed since we have exactly 2 permits available.
         * - sum of timed out and rejected must be equal to 8.
         * - at least 3 and no more than 5 tasks must get rejected.
         * - at least 3 and no more than 5 tasks must get timed out
         */
        int successCount  = results.get("success").get();
        int timeoutCount  = results.get("java.util.concurrent.TimeoutException").get();
        int rejectedCount = results.get("java.util.concurrent.RejectedExecutionException").get();
        assertEquals("success count", 2, successCount);
        assertTrue("timeout[" + timeoutCount + "]: 3 <= count(timeout) <= 5", timeoutCount >= 3 && timeoutCount <= 5);
        assertTrue("rejected[" + rejectedCount + "]: 3 <= count(timeout) <= 5", rejectedCount >= 3 && rejectedCount <= 5);
        assertEquals("total should equal 10", 10, successCount + timeoutCount + rejectedCount);
        executor.shutdown();
    }

    static class LookupTask implements Callable<Integer> {
        final int id;
        private final Semaphore semaphore;

        public LookupTask(int id1, Semaphore latch) {
            id        = id1;
            semaphore = latch;
        }

        @Override
        public Integer call() throws Exception {
            LOG.debug(String.format(format, "Starting", id));
            semaphore.acquire();
            LOG.debug(String.format(format, "Acquired", id));
            LOG.debug(String.format(format, "Ended", id));
            return id;
        }

        int getId() {
            return id;
        }
    }

    static class TimedTask implements Callable<Integer> {
        final LookupTask                           callable;
        final TimedExecutor                        executor;
        final ConcurrentMap<String, AtomicInteger> results;
        final long                                 timeout;
        final TimeUnit                             unit;
        final CountDownLatch                       latch;

        public TimedTask(TimedExecutor executor, LookupTask callable, int timout, TimeUnit unit, ConcurrentMap<String, AtomicInteger> results, CountDownLatch latch) {
            this.callable = callable;
            this.executor = executor;
            this.results  = results;
            this.timeout  = timout;
            this.unit     = unit;
            this.latch    = latch;
        }

        @Override
        public Integer call() throws Exception {
            int id = callable.getId();
            LOG.debug(String.format(format, "Submitting", id));
            try {
                Integer result = executor.timedTask(callable, timeout, unit);
                LOG.debug(String.format(format, "Finished", id));
                recordResult(results, "success");
                return result;
            } catch (Exception e) {
                LOG.debug(String.format(format, "Exception", id));
                recordResult(results, e);
                // re-throw caught exception
                throw e;
            } finally {
                latch.countDown();
            }
        }
    }
}
