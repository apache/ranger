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

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class LoginAttemptTracker {
    private static final long CLEANUP_INTERVAL_MS = TimeUnit.MINUTES.toMillis(5);

    private final AttemptCounter ipAttempts;
    private final AttemptCounter accountAttempts;
    private final boolean        accountLockoutEnabled;

    public LoginAttemptTracker(int maxIpFailures, long ipWindowMs, long ipLockoutMs) {
        this(maxIpFailures, ipWindowMs, ipLockoutMs, true, maxIpFailures, ipWindowMs, ipLockoutMs);
    }

    public LoginAttemptTracker(int maxIpFailures, long ipWindowMs, long ipLockoutMs,
                               boolean accountLockoutEnabled,
                               int maxAccountFailures, long accountWindowMs, long accountLockoutMs) {
        this.ipAttempts             = new AttemptCounter(maxIpFailures, ipWindowMs, ipLockoutMs);
        this.accountAttempts        = new AttemptCounter(maxAccountFailures, accountWindowMs, accountLockoutMs);
        this.accountLockoutEnabled  = accountLockoutEnabled;
    }

    public boolean isBlocked(String sourceIp) {
        return ipAttempts.isBlocked(sourceIp);
    }

    public boolean isAccountBlocked(String account) {
        return accountLockoutEnabled && accountAttempts.isBlocked(account);
    }

    public boolean isBlocked(String sourceIp, String account) {
        return isBlocked(sourceIp) || isAccountBlocked(account);
    }

    public void recordFailure(String sourceIp) {
        recordFailure(sourceIp, null);
    }

    public void recordFailure(String sourceIp, String account) {
        ipAttempts.recordFailure(sourceIp);
        if (accountLockoutEnabled) {
            accountAttempts.recordFailure(account);
        }
    }

    public void recordSuccess(String sourceIp) {
        recordSuccess(sourceIp, null);
    }

    public void recordSuccess(String sourceIp, String account) {
        ipAttempts.recordSuccess(sourceIp);
        if (accountLockoutEnabled) {
            accountAttempts.recordSuccess(account);
        }
    }

    private static final class AttemptCounter {
        private final ConcurrentHashMap<String, Record> attempts = new ConcurrentHashMap<>();
        private final int                               maxFailures;
        private final long                              windowMs;
        private final long                              lockoutMs;
        private volatile long                           lastCleanupTime;

        AttemptCounter(int maxFailures, long windowMs, long lockoutMs) {
            this.maxFailures = maxFailures;
            this.windowMs    = windowMs;
            this.lockoutMs   = lockoutMs;
        }

        boolean isBlocked(String key) {
            if (key == null || key.isEmpty()) {
                return false;
            }
            Record record = attempts.get(key);
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
            Record record = attempts.computeIfAbsent(key, k -> new Record(now));
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
            Iterator<Map.Entry<String, Record>> it = attempts.entrySet().iterator();
            while (it.hasNext()) {
                Record record = it.next().getValue();
                if (record.lastActivity < cutoff && record.lockedUntil <= now) {
                    it.remove();
                }
            }
        }
    }

    private static final class Record {
        final AtomicInteger failCount = new AtomicInteger(0);
        volatile long windowStart;
        volatile long lastActivity;
        volatile long lockedUntil;

        Record(long now) {
            this.windowStart  = now;
            this.lastActivity = now;
        }
    }
}
