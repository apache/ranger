/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import org.apache.ranger.plugin.util.RangerCache.RefreshMode;
import org.apache.ranger.plugin.util.RangerCache.RefreshableValue;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.*;

public class RangerCacheTest {
    private static final int CACHE_THREAD_COUNT               = 25;
    private static final int VALUE_VALIDITY_DURATION_MS       = 3 * 1000;
    private static final int VALUE_REFRESH_TIMEOUT_MS         = 10;
    private static final int VALUE_INIT_TIMEOUT_MS            = -1; // no timeout for init
    private static final int VALUE_LOAD_TIME_TYPICAL_MAX_MS   =  8;
    private static final int VALUE_LOAD_TIME_FAIL_MAX_MS      = 100;
    private static final int VALUE_LOAD_TIME_LONG_MIN_MS      = VALUE_VALIDITY_DURATION_MS / 2;
    private static final int VALUE_LOAD_TIME_VERY_LONG_MIN_MS = VALUE_VALIDITY_DURATION_MS;

    private static final int USER_COUNT                = 100;
    private static final int CACHE_CLIENT_THREAD_COUNT =  20;
    private static final int CACHE_LOOKUP_COUNT        = 200;
    private static final int CACHE_LOOKUP_INTERVAL_MS  =   5;

    private static final boolean IS_DEBUG_ENABLED = false;

    /*
     * Test cases:
     *  1. successful initial load and refresh
     *  2. failure in initial load
     *  3. failure in refresh
     *  4. long initial load      - just above half the value validity period
     *  5. long refresh           - just above half the value validity period
     *  6. very long initial load - above the value validity period
     *  7. very long refresh      - above the value validity period
     */
    private static final String USERNAME_PREFIX_TYPICAL_LOAD      = "typical_";
    private static final String USERNAME_PREFIX_FAILED_FIRST_INIT = "failedFirstInit_";
    private static final String USERNAME_PREFIX_FAILED_INIT       = "failedInit_";
    private static final String USERNAME_PREFIX_FAILED_REFRESH    = "failedRefresh_";
    private static final String USERNAME_PREFIX_REMOVED           = "removed_";
    private static final String USERNAME_PREFIX_LONG_INIT         = "longInit_";
    private static final String USERNAME_PREFIX_LONG_REFRESH      = "longRefresh_";
    private static final String USERNAME_PREFIX_VERY_LONG_INIT    = "veryLongInit_";
    private static final String USERNAME_PREFIX_VERY_LONG_REFRESH = "veryLongRefresh_";

    private final Random random = new Random();


    @Test
    public void testOnAccessRefreshCacheMultiThreadedGet() throws Throwable {
        UserGroupCache cache = createCache(RefreshMode.ON_ACCESS);

        runMultiThreadedGet("testOnAccessRefreshCacheMultiThreadedGet", cache);
    }

    @Test
    public void testOnScheduleRefreshCacheMultiThreadedGet() throws Throwable {
        UserGroupCache cache = createCache(RefreshMode.ON_SCHEDULE);

        runMultiThreadedGet("testOnScheduleRefreshCacheMultiThreadedGet", cache);
    }

    @Test
    public void testOnScheduleRefreshCacheRemoveKey() throws Exception {
        UserGroupCache     cache    = createCache(RefreshMode.ON_SCHEDULE);
        ThreadPoolExecutor executor = new ThreadPoolExecutor(CACHE_CLIENT_THREAD_COUNT, CACHE_CLIENT_THREAD_COUNT, 0, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>());
        List<Future<?>>    futures  = new ArrayList<>();

        long startTimeMs = System.currentTimeMillis();

        // submit tasks to access cache from multiple threads
        for (String user : cache.stats.keySet()) {
            Future<?> future = executor.submit(new GetGroupsForUserFromCache(cache, user, 0));

            futures.add(future);
        }

        log(String.format("waiting for %s submitted tasks to complete", futures.size()));
        for (Future<?> future : futures) {
            future.get();
        }

        log(String.format("all submitted tasks completed: timeTaken=%sms", (System.currentTimeMillis() - startTimeMs)));

        executor.shutdown();

        for (String user : cache.stats.keySet()) {
            cache.remove(user);
        }

        assertEquals("cache should have no users", 0, cache.getKeys().size());

        log(String.format("all entries in the cache are now removed: timeTaken=%sms", (System.currentTimeMillis() - startTimeMs)));
    }

    private void runMultiThreadedGet(String testName, UserGroupCache cache) throws Throwable {
        ThreadPoolExecutor executor = new ThreadPoolExecutor(CACHE_CLIENT_THREAD_COUNT, CACHE_CLIENT_THREAD_COUNT, 0, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>());
        List<Future<?>>    futures  = new ArrayList<>();

        long startTimeMs = System.currentTimeMillis();

        for (int i = 0; i < CACHE_LOOKUP_COUNT; i++) {
            for (String user : cache.stats.keySet()) {
                Future<?> future = executor.submit(new GetGroupsForUserFromCache(cache, user, i));

                futures.add(future);
            }
        }

        log(String.format("waiting for %s submitted tasks to complete", futures.size()));
        for (Future<?> future : futures) {
            future.get();
        }

        executor.shutdown();

        printStats(testName, System.currentTimeMillis() - startTimeMs, cache);
    }

    private UserGroupCache createCache(RefreshMode refreshMode) {
        UserGroupCache ret = new UserGroupCache("ug", CACHE_THREAD_COUNT, refreshMode, VALUE_VALIDITY_DURATION_MS, VALUE_INIT_TIMEOUT_MS, VALUE_REFRESH_TIMEOUT_MS);

        // initialize cache with users
        for (int i = 0; i < USER_COUNT; i++) {
            ret.addUserStats(getUserName(i));
        }

        // prime the cache with empty entry for each user, to avoid the test from making excessive concurrent calls to enter into cache
        for (String user : ret.stats.keySet()) {
            ret.addIfAbsent(user);
        }

        return ret;
    }

    private String getUserName(int index) {
        int percent = (index % USER_COUNT) * 100 / USER_COUNT;
        final String ret;

        if (percent < 88) {
            ret = USERNAME_PREFIX_TYPICAL_LOAD;
        } else if (percent < 89) {
            ret = USERNAME_PREFIX_FAILED_FIRST_INIT;
        } else if (percent < 90) {
            ret = USERNAME_PREFIX_FAILED_INIT;
        } else if (percent < 91) {
            ret = USERNAME_PREFIX_FAILED_REFRESH;
        } else if (percent < 92) {
            ret = USERNAME_PREFIX_REMOVED;
        } else if (percent < 94) {
            ret = USERNAME_PREFIX_LONG_INIT;
        } else if (percent < 96) {
            ret = USERNAME_PREFIX_LONG_REFRESH;
        } else if (percent < 98) {
            ret = USERNAME_PREFIX_VERY_LONG_INIT;
        } else {
            ret = USERNAME_PREFIX_VERY_LONG_REFRESH;
        }

        return String.format("%s%04d", ret, index);
    }

    private void log(String msg) {
        System.out.println(new Date() + " [" + Thread.currentThread().getName() + "] " + msg);
    }

    private void testLoadWait(UserGroupCache.UserStats userStats, RefreshableValue<List<String>> currVal) throws Exception {
        boolean fail = false;
        long    sleepTimeMs;
        String  userName = userStats.userName;

        if (currVal == null) { // initial load
            if (userName.startsWith(USERNAME_PREFIX_LONG_INIT)) {
                sleepTimeMs = VALUE_LOAD_TIME_LONG_MIN_MS + random.nextInt(VALUE_LOAD_TIME_TYPICAL_MAX_MS);
            } else if (userName.startsWith(USERNAME_PREFIX_VERY_LONG_INIT)) {
                sleepTimeMs = VALUE_LOAD_TIME_VERY_LONG_MIN_MS + random.nextInt(VALUE_LOAD_TIME_TYPICAL_MAX_MS);
            } else if (userName.startsWith(USERNAME_PREFIX_FAILED_FIRST_INIT)) {
                sleepTimeMs = random.nextInt(VALUE_LOAD_TIME_FAIL_MAX_MS);

                fail = userStats.load.count.get() == 0;
            } else if (userName.startsWith(USERNAME_PREFIX_FAILED_INIT)) {
                sleepTimeMs = random.nextInt(VALUE_LOAD_TIME_FAIL_MAX_MS);

                fail = true;
            } else {
                sleepTimeMs = random.nextInt(VALUE_LOAD_TIME_TYPICAL_MAX_MS);
            }
        } else { // refresh
            if (userName.startsWith(USERNAME_PREFIX_LONG_REFRESH)) {
                sleepTimeMs = VALUE_LOAD_TIME_LONG_MIN_MS + random.nextInt(VALUE_LOAD_TIME_TYPICAL_MAX_MS);
            } else if (userName.startsWith(USERNAME_PREFIX_VERY_LONG_REFRESH)) {
                sleepTimeMs = VALUE_LOAD_TIME_VERY_LONG_MIN_MS + random.nextInt(VALUE_LOAD_TIME_TYPICAL_MAX_MS);
            } else if (userName.startsWith(USERNAME_PREFIX_FAILED_REFRESH)) {
                sleepTimeMs = random.nextInt(VALUE_LOAD_TIME_FAIL_MAX_MS);

                fail = true;
            } else if (userName.startsWith(USERNAME_PREFIX_REMOVED)) {
                sleepTimeMs = random.nextInt(VALUE_LOAD_TIME_TYPICAL_MAX_MS);

                fail = true;
            } else {
                sleepTimeMs = random.nextInt(VALUE_LOAD_TIME_TYPICAL_MAX_MS);
            }
        }

        sleep(sleepTimeMs);

        if (fail) {
            if (userName.startsWith(USERNAME_PREFIX_REMOVED)) {
                throw new RangerCache.KeyNotFoundException(userName + ": user not found");
            } else {
                throw new Exception("failed to retrieve value");
            }
        }
    }

    private void sleep(long timeoutMs) {
        try {
            Thread.sleep(timeoutMs);
        } catch (InterruptedException excp) {
            // ignore
        }
    }

    /* Sample output:
     *
    testOnAccessRefreshCacheMultiThreadedGet(): timeTaken=7489ms
      cache: loaderThreads=25, refreshMode=ON_ACCESS, valueValidityMs=3000, valueInitTimeoutMs=-1, valueRefreshTimeoutMs=10
      test:  cacheKeyCount=100, cacheClientThreads=20, lookupCount=200, lookupIntervalMs=5
      userPrefix=failedFirstInit_ userCount=1    loadCount=4     getCount=200     avgLoadTime=19.750    avgGetTime=0.520
      userPrefix=failedInit_      userCount=1    loadCount=99    getCount=99      avgLoadTime=57.283    avgGetTime=57.566
      userPrefix=failedRefresh_   userCount=1    loadCount=3     getCount=200     avgLoadTime=21.667    avgGetTime=0.165
      userPrefix=longInit_        userCount=2    loadCount=4     getCount=322     avgLoadTime=756.000   avgGetTime=9.416
      userPrefix=longRefresh_     userCount=2    loadCount=4     getCount=400     avgLoadTime=756.500   avgGetTime=2.885
      userPrefix=removed_         userCount=1    loadCount=5     getCount=200     avgLoadTime=5.400     avgGetTime=0.205
      userPrefix=typical_         userCount=88   loadCount=264   getCount=17600   avgLoadTime=4.405     avgGetTime=0.147
      userPrefix=veryLongInit_    userCount=2    loadCount=4     getCount=236     avgLoadTime=1507.250  avgGetTime=25.691
      userPrefix=veryLongRefresh_ userCount=2    loadCount=4     getCount=400     avgLoadTime=1506.500  avgGetTime=5.260

    ****** Detailed stats for each user ******
    failedFirstInit_0088: lastValue([group-1, group-2, group-3]), load(count: 4, totalTime: 79, minTime: 0, maxTime: 65, avgTime: 19.750), get(count: 200, totalTime: 104, minTime: 0, maxTime: 65, avgTime: 0.520)
    failedInit_0089: lastValue(null), load(count: 99, totalTime: 5671, minTime: 0, maxTime: 110, avgTime: 57.283), get(count: 99, totalTime: 5699, minTime: 0, maxTime: 110, avgTime: 57.566)
    failedRefresh_0090: lastValue([group-1]), load(count: 3, totalTime: 65, minTime: 6, maxTime: 33, avgTime: 21.667), get(count: 200, totalTime: 33, minTime: 0, maxTime: 12, avgTime: 0.165)
    longInit_0092: lastValue([group-1, group-2]), load(count: 2, totalTime: 1513, minTime: 3, maxTime: 1510, avgTime: 756.500), get(count: 161, totalTime: 1513, minTime: 0, maxTime: 1510, avgTime: 9.398)
    longInit_0093: lastValue([group-1, group-2]), load(count: 2, totalTime: 1511, minTime: 3, maxTime: 1508, avgTime: 755.500), get(count: 161, totalTime: 1519, minTime: 0, maxTime: 1508, avgTime: 9.435)
    longRefresh_0094: lastValue([group-1, group-2]), load(count: 2, totalTime: 1513, minTime: 3, maxTime: 1510, avgTime: 756.500), get(count: 200, totalTime: 585, minTime: 0, maxTime: 39, avgTime: 2.925)
    longRefresh_0095: lastValue([group-1, group-2]), load(count: 2, totalTime: 1513, minTime: 4, maxTime: 1509, avgTime: 756.500), get(count: 200, totalTime: 569, minTime: 0, maxTime: 38, avgTime: 2.845)
    removed_0091: lastValue([group-1]), load(count: 5, totalTime: 27, minTime: 3, maxTime: 8, avgTime: 5.400), get(count: 200, totalTime: 41, minTime: 0, maxTime: 9, avgTime: 0.205)
    typical_0000: lastValue([group-1, group-2, group-3]), load(count: 3, totalTime: 17, minTime: 2, maxTime: 8, avgTime: 5.667), get(count: 200, totalTime: 19, minTime: 0, maxTime: 10, avgTime: 0.095)
    ...
    typical_0087: lastValue([group-1, group-2, group-3]), load(count: 3, totalTime: 10, minTime: 1, maxTime: 6, avgTime: 3.333), get(count: 200, totalTime: 13, minTime: 0, maxTime: 7, avgTime: 0.065)
    veryLongInit_0096: lastValue([group-1, group-2]), load(count: 2, totalTime: 3011, minTime: 5, maxTime: 3006, avgTime: 1505.500), get(count: 118, totalTime: 3032, minTime: 0, maxTime: 3015, avgTime: 25.695)
    veryLongInit_0097: lastValue([group-1, group-2]), load(count: 2, totalTime: 3018, minTime: 13, maxTime: 3005, avgTime: 1509.000), get(count: 118, totalTime: 3031, minTime: 0, maxTime: 3014, avgTime: 25.686)
    veryLongRefresh_0098: lastValue([group-1, group-2]), load(count: 2, totalTime: 3012, minTime: 6, maxTime: 3006, avgTime: 1506.000), get(count: 200, totalTime: 1050, minTime: 0, maxTime: 21, avgTime: 5.250)
    veryLongRefresh_0099: lastValue([group-1, group-2]), load(count: 2, totalTime: 3014, minTime: 0, maxTime: 3014, avgTime: 1507.000), get(count: 200, totalTime: 1054, minTime: 0, maxTime: 19, avgTime: 5.270)
     *
     */
    private void printStats(String testName, long timeTakenMs, UserGroupCache cache) {
        log(String.format("%s(): timeTaken=%sms", testName, timeTakenMs));
        log(String.format("  cache: loaderThreads=%s, refreshMode=%s, valueValidityMs=%s, valueInitTimeoutMs=%s, valueRefreshTimeoutMs=%s", cache.getLoaderThreadsCount(), cache.getRefreshMode(), cache.getValueValidityPeriodMs(), cache.getValueInitLoadTimeoutMs(), cache.getValueRefreshLoadTimeoutMs()));
        log(String.format("  test:  cacheKeyCount=%s, cacheClientThreads=%s, lookupCount=%s, lookupIntervalMs=%s", cache.stats.size(), CACHE_CLIENT_THREAD_COUNT, CACHE_LOOKUP_COUNT, CACHE_LOOKUP_INTERVAL_MS));

        printStats(cache.stats, USERNAME_PREFIX_FAILED_FIRST_INIT);
        printStats(cache.stats, USERNAME_PREFIX_FAILED_INIT);
        printStats(cache.stats, USERNAME_PREFIX_FAILED_REFRESH);
        printStats(cache.stats, USERNAME_PREFIX_LONG_INIT);
        printStats(cache.stats, USERNAME_PREFIX_LONG_REFRESH);
        printStats(cache.stats, USERNAME_PREFIX_REMOVED);
        printStats(cache.stats, USERNAME_PREFIX_TYPICAL_LOAD);
        printStats(cache.stats, USERNAME_PREFIX_VERY_LONG_INIT);
        printStats(cache.stats, USERNAME_PREFIX_VERY_LONG_REFRESH);

        log("");
        log("****** Detailed stats for each user ******");

        // print stats for all users, in a predictable order
        List<String> userNames = new ArrayList<>(cache.stats.keySet());

        Collections.sort(userNames);

        for (String userName : userNames) {
            log(String.format("%s", cache.stats.get(userName)));
        }
    }

    private void printStats(Map<String, UserGroupCache.UserStats> stats, String userNamePrefix) {
        long userCount = 0, loadCount = 0, getCount = 0, totalLoadTime = 0, totalGetTime = 0;

        for (Map.Entry<String, UserGroupCache.UserStats> entry : stats.entrySet()) {
            String userName = entry.getKey();

            if (!userName.startsWith(userNamePrefix)) {
                continue;
            }

            UserGroupCache.UserStats userStats = entry.getValue();

            userCount++;
            loadCount     += userStats.load.count.get();
            getCount      += userStats.get.count.get();
            totalLoadTime += userStats.load.totalTime.get();
            totalGetTime  += userStats.get.totalTime.get();
        }

        log(String.format("  userPrefix=%-16s userCount=%-4s loadCount=%-5s getCount=%-7s avgLoadTime=%-9.3f avgGetTime=%-6.3f", userNamePrefix, userCount, loadCount, getCount, (totalLoadTime / (float)loadCount), (totalGetTime / (float)getCount)));
    }

    // multiple instances of this class are used by the test to simulate simultaneous access to cache to obtain groups for users
    private class GetGroupsForUserFromCache implements Runnable {
        private final UserGroupCache cache;
        private final String         userName;
        private final int            lookupCount;

        public GetGroupsForUserFromCache(UserGroupCache cache, String userName, int lookupCount) {
            this.cache       = cache;
            this.userName    = userName;
            this.lookupCount = lookupCount;
        }

        @Override
        public void run() {
            UserGroupCache.UserStats userStats = cache.getUserStats(userName);

            // test threads can be blocked by values that take a long time to initialize
            // avoid this by restricting such values to have only one pending call until they are initialized
            if (!cache.isLoaded(userName) && userStats.inProgressCount.get() > 0) {
                if (userName.startsWith(USERNAME_PREFIX_FAILED_INIT) || userName.startsWith(USERNAME_PREFIX_LONG_INIT) || userName.startsWith(USERNAME_PREFIX_VERY_LONG_INIT)) {
                    if (IS_DEBUG_ENABLED) {
                        log(String.format("[%s] [lookupCount=%s] get(%s): aborted, as initial loading is already in progress for this user", Thread.currentThread().getName(), lookupCount, userName));
                    }

                    return;
                }
            }

            userStats.inProgressCount.getAndIncrement();

            long         startTime  = System.currentTimeMillis();
            List<String> userGroups = cache.get(userName);
            long         timeTaken  = System.currentTimeMillis() - startTime;

            userStats.inProgressCount.getAndDecrement();

            userStats.get.record(timeTaken);

            if (userName.startsWith(USERNAME_PREFIX_FAILED_INIT)) {
                assertNull("userGroups should be null for user=" + userName + ", lookupCount=" + lookupCount, userGroups);
            } else if (userName.startsWith(USERNAME_PREFIX_FAILED_FIRST_INIT)) {
                if (lookupCount == 0) {
                    assertNull("userGroups should be null after first lookup for user=" + userName + ", lookupCount=" + lookupCount, userGroups);
                } else {
                    assertNotNull("userGroups should be null only after first lookup for user=" + userName + ", lookupCount=" + lookupCount, userGroups);
                }
            } else {
                assertNotNull("userGroups should not be null for user=" + userName + ", lookupCount=" + lookupCount, userGroups);
            }

            userStats.lastValue = userGroups;

            if (IS_DEBUG_ENABLED) {
                log(String.format("[%s] [lookupCount=%s] get(%s): timeTaken=%s, userGroups=%s", Thread.currentThread().getName(), lookupCount, userName, timeTaken, userGroups));
            }

            sleep(CACHE_LOOKUP_INTERVAL_MS);
        }
    }

    private class UserGroupCache extends RangerCache<String, List<String>> {
        private final Map<String, UserStats> stats = new HashMap<>();

        public UserGroupCache(String name, int loaderThreadsCount, RefreshMode refreshMode, long valueValidityPeriodMs, long valueInitLoadTimeoutMs, long valueRefreshLoadTimeoutMs) {
            super(name, null, loaderThreadsCount, refreshMode, valueValidityPeriodMs, valueInitLoadTimeoutMs, valueRefreshLoadTimeoutMs);

            setLoader(new UserGroupLoader());
        }

        public void addUserStats(String userName) {
            stats.put(userName, new UserStats(userName));
        }

        public UserStats getUserStats(String userName) {
            return stats.get(userName);
        }

        //
        // this class implements value-loader interface used by the cache to populate and refresh the cache
        //   load() method simulates loading of groups for the given user
        //
        private class UserGroupLoader extends ValueLoader<String, List<String>> {
            public UserGroupLoader() {
            }

            @Override
            public RefreshableValue<List<String>> load(String userName, RefreshableValue<List<String>> currVal, Object context) throws Exception {
                long startTimeMs = System.currentTimeMillis();

                UserStats userStats = stats.get(userName);

                try {
                    testLoadWait(userStats, currVal); // simulate various load conditions, depending on the userName

                    // simply append 'group-#' to current value, where # is the number of groups including this one
                    final List<String> value = currVal != null && currVal.getValue() != null ? new ArrayList<>(currVal.getValue()) : new ArrayList<>();

                    value.add("group-" + (value.size() + 1));

                    return new RefreshableValue<>(value);
                } finally {
                    userStats.load.record(System.currentTimeMillis() - startTimeMs);
                }
            }
        }

        private class UserStats {
            final String       userName;
            final TimedCounter get             = new TimedCounter();
            final TimedCounter load            = new TimedCounter();
            final AtomicLong   inProgressCount = new AtomicLong();
            List<String>       lastValue;

            public UserStats(String userName) {
                this.userName = userName;
            }

            @Override
            public String toString() {
                return userName + ": lastValue(" + lastValue + "), load(" + load + "), get(" + get + ")";
            }
        }
    }

    private static class TimedCounter {
        final AtomicLong count     = new AtomicLong();
        final AtomicLong totalTime = new AtomicLong();
        final AtomicLong minTime   = new AtomicLong(Long.MAX_VALUE);
        final AtomicLong maxTime   = new AtomicLong();

        public void record(long timeTaken) {
            count.getAndIncrement();
            totalTime.addAndGet(timeTaken);

            long minTimeTaken = minTime.get();
            long maxTimeTaken = maxTime.get();

            if (timeTaken < minTimeTaken) {
                minTime.compareAndSet(minTimeTaken, timeTaken);
            }

            if (timeTaken > maxTimeTaken) {
                maxTime.compareAndSet(maxTimeTaken, timeTaken);
            }
        }

        @Override
        public String toString() {
            return "count: " + count.get() + ", totalTime: " + totalTime.get() + ", minTime: " + minTime.get() + ", maxTime: " + maxTime.get() + ", avgTime: " + (getAvgTimeMs());
        }

        private String getAvgTimeMs() {
            long totalTime = this.totalTime.get();
            long count     = this.count.get();

            return String.format("%.3f", (count != 0 ? (totalTime / (double)count) : -1));
        }
    }
}
