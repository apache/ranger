/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.ranger.authentication;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class LoginAttemptTracker {
    private static final long CLEANUP_INTERVAL_MS = TimeUnit.MINUTES.toMillis(5);

    private final IpAttemptCounter     ipAttempts;
    private final AccountFanoutTracker accountFanout;
    private final boolean              accountThrottlingEnabled;

    public LoginAttemptTracker(int maxIpFailures, long ipWindowMs, long ipLockoutMs) {
        this(maxIpFailures, ipWindowMs, ipLockoutMs, false, 3, ipWindowMs, 1_000, 5_000);
    }

    public LoginAttemptTracker(int maxIpFailures, long ipWindowMs, long ipLockoutMs,
                               boolean accountThrottlingEnabled,
                               int distinctIpThreshold, long accountWindowMs,
                               long accountDelayStepMs, long accountMaxDelayMs) {
        this.ipAttempts               = new IpAttemptCounter(maxIpFailures, ipWindowMs, ipLockoutMs);
        this.accountFanout            = new AccountFanoutTracker(distinctIpThreshold, accountWindowMs, accountDelayStepMs, accountMaxDelayMs);
        this.accountThrottlingEnabled = accountThrottlingEnabled;
    }

    public boolean isBlocked(String sourceIp) {
        return ipAttempts.isBlocked(sourceIp);
    }

    public boolean isBlocked(String sourceIp, String account) {
        return isBlocked(sourceIp);
    }

    public long getAccountDelayMs(String account, String sourceIp) {
        if (!accountThrottlingEnabled) {
            return 0;
        }
        return accountFanout.getDelayMs(account, sourceIp);
    }

    public void recordFailure(String sourceIp) {
        recordFailure(sourceIp, null);
    }

    public void recordFailure(String sourceIp, String account) {
        ipAttempts.recordFailure(sourceIp);
        if (accountThrottlingEnabled) {
            accountFanout.recordFailure(account, sourceIp);
        }
    }

    public void recordSuccess(String sourceIp) {
        recordSuccess(sourceIp, null);
    }

    public void recordSuccess(String sourceIp, String account) {
        ipAttempts.recordSuccess(sourceIp);
        if (accountThrottlingEnabled) {
            accountFanout.recordSuccess(account);
        }
    }

    private static final class IpAttemptCounter {
        private final ConcurrentHashMap<String, IpRecord> attempts = new ConcurrentHashMap<>();
        private final int                                 maxFailures;
        private final long                                windowMs;
        private final long                                lockoutMs;
        private volatile long                             lastCleanupTime;

        IpAttemptCounter(int maxFailures, long windowMs, long lockoutMs) {
            this.maxFailures = maxFailures;
            this.windowMs    = windowMs;
            this.lockoutMs   = lockoutMs;
        }

        boolean isBlocked(String key) {
            if (key == null || key.isEmpty()) {
                return false;
            }
            IpRecord record = attempts.get(key);
            if (record == null) {
                return false;
            }
            return record.lockedUntil > System.currentTimeMillis();
        }

        void recordFailure(String key) {
            if (key == null || key.isEmpty()) {
                return;
            }
            maybeCleanup();
            long now = System.currentTimeMillis();
            IpRecord record = attempts.computeIfAbsent(key, k -> new IpRecord(now));
            synchronized (record) {
                if (now - record.windowStart > windowMs) {
                    record.windowStart = now;
                    record.failCount.set(0);
                }
                record.lastActivity = now;
                int count = record.failCount.incrementAndGet();
                if (count >= maxFailures) {
                    record.lockedUntil = now + lockoutMs;
                }
            }
        }

        void recordSuccess(String key) {
            if (key == null || key.isEmpty()) {
                return;
            }
            attempts.remove(key);
        }

        private void maybeCleanup() {
            long now = System.currentTimeMillis();
            if (now - lastCleanupTime < CLEANUP_INTERVAL_MS) {
                return;
            }
            lastCleanupTime = now;
            long cutoff = now - windowMs;
            Iterator<Map.Entry<String, IpRecord>> it = attempts.entrySet().iterator();
            while (it.hasNext()) {
                IpRecord record = it.next().getValue();
                if (record.lastActivity < cutoff && record.lockedUntil <= now) {
                    it.remove();
                }
            }
        }
    }

    private static final class AccountFanoutTracker {
        private final ConcurrentHashMap<String, FanoutRecord> accounts = new ConcurrentHashMap<>();
        private final int                                       distinctIpThreshold;
        private final long                                      windowMs;
        private final long                                      delayStepMs;
        private final long                                      maxDelayMs;
        private volatile long                                   lastCleanupTime;

        AccountFanoutTracker(int distinctIpThreshold, long windowMs, long delayStepMs, long maxDelayMs) {
            this.distinctIpThreshold = distinctIpThreshold;
            this.windowMs            = windowMs;
            this.delayStepMs         = delayStepMs;
            this.maxDelayMs          = maxDelayMs;
        }

        long getDelayMs(String account, String sourceIp) {
            if (account == null || account.isEmpty() || sourceIp == null || sourceIp.isEmpty()) {
                return 0;
            }
            long now = System.currentTimeMillis();
            FanoutRecord record = accounts.get(account);
            int distinctCount = getDistinctIpCount(record, now);
            int projectedDistinct = distinctCount;
            if (record == null || !record.containsIp(sourceIp, now)) {
                projectedDistinct = distinctCount + 1;
            }
            if (projectedDistinct < distinctIpThreshold) {
                return 0;
            }
            long ipsOverLimit = projectedDistinct - distinctIpThreshold + 1L;
            return exponentialDelayMs(ipsOverLimit);
        }

        private long exponentialDelayMs(long ipsOverLimit) {
            // Start at stepMs, double for each extra distinct IP past threshold, cap at maxDelayMs.
            long delay = delayStepMs;
            for (long level = 1; level < ipsOverLimit; level++) {
                if (delay >= maxDelayMs) {
                    return maxDelayMs;
                }
                delay *= 2;
            }
            return Math.min(delay, maxDelayMs);
        }

        void recordFailure(String account, String sourceIp) {
            if (account == null || account.isEmpty() || sourceIp == null || sourceIp.isEmpty()) {
                return;
            }
            maybeCleanup();
            long now = System.currentTimeMillis();
            FanoutRecord record = accounts.computeIfAbsent(account, k -> new FanoutRecord(now, windowMs));
            synchronized (record) {
                resetWindowIfExpired(record, now);
                record.failingIps.add(sourceIp);
                record.lastActivity = now;
            }
        }

        void recordSuccess(String account) {
            if (account == null || account.isEmpty()) {
                return;
            }
            accounts.remove(account);
        }

        private int getDistinctIpCount(FanoutRecord record, long now) {
            if (record == null) {
                return 0;
            }
            synchronized (record) {
                resetWindowIfExpired(record, now);
                return record.failingIps.size();
            }
        }

        private static void resetWindowIfExpired(FanoutRecord record, long now) {
            if (now - record.windowStart > record.windowMs) {
                record.windowStart = now;
                record.failingIps.clear();
            }
        }

        private void maybeCleanup() {
            long now = System.currentTimeMillis();
            if (now - lastCleanupTime < CLEANUP_INTERVAL_MS) {
                return;
            }
            lastCleanupTime = now;
            long cutoff = now - windowMs;
            Iterator<Map.Entry<String, FanoutRecord>> it = accounts.entrySet().iterator();
            while (it.hasNext()) {
                FanoutRecord record = it.next().getValue();
                if (record.lastActivity < cutoff) {
                    it.remove();
                }
            }
        }
    }

    private static final class IpRecord {
        final AtomicInteger failCount = new AtomicInteger(0);
        volatile long windowStart;
        volatile long lastActivity;
        volatile long lockedUntil;

        IpRecord(long now) {
            this.windowStart  = now;
            this.lastActivity = now;
        }
    }

    private static final class FanoutRecord {
        final Set<String> failingIps = new HashSet<>();
        final long        windowMs;
        volatile long windowStart;
        volatile long lastActivity;

        FanoutRecord(long now, long windowMs) {
            this.windowMs     = windowMs;
            this.windowStart  = now;
            this.lastActivity = now;
        }

        boolean containsIp(String sourceIp, long now) {
            if (now - windowStart > windowMs) {
                windowStart = now;
                failingIps.clear();
            }
            return failingIps.contains(sourceIp);
        }
    }
}
