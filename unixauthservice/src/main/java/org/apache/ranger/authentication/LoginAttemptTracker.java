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
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class LoginAttemptTracker {
    private static final long CLEANUP_INTERVAL_MS = TimeUnit.MINUTES.toMillis(5);

    private final ConcurrentHashMap<String, IpRecord> attemptsByIp = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, AccountRecord> attemptsByAccount = new ConcurrentHashMap<>();

    private final int maxFailures;
    private final long windowMs;
    private final long lockoutMs;

    private final boolean accountFanoutEnabled;
    private final int accountDistinctIpThreshold;
    private final long accountWindowMs;
    private final long accountBaseDelayMs;
    private final long accountMaxDelayMs;

    private volatile long lastCleanupTime;

    public LoginAttemptTracker(int maxFailures, long windowMs, long lockoutMs) {
        this(maxFailures, windowMs, lockoutMs, false, 0, 0, 0, 0);
    }

    public LoginAttemptTracker(int maxFailures, long windowMs, long lockoutMs,
                               boolean accountFanoutEnabled, int accountDistinctIpThreshold,
                               long accountWindowMs, long accountBaseDelayMs, long accountMaxDelayMs) {
        this.maxFailures = maxFailures;
        this.windowMs = windowMs;
        this.lockoutMs = lockoutMs;
        this.lastCleanupTime = System.currentTimeMillis();

        this.accountFanoutEnabled = accountFanoutEnabled;
        this.accountDistinctIpThreshold = accountDistinctIpThreshold;
        this.accountWindowMs = accountWindowMs;
        this.accountBaseDelayMs = accountBaseDelayMs;
        this.accountMaxDelayMs = accountMaxDelayMs;
    }

    public boolean isBlocked(String sourceIp) {
        IpRecord record = attemptsByIp.get(sourceIp);

        if (record == null) {
            return false;
        }

        return record.lockedUntil > System.currentTimeMillis();
    }

    public long getAccountDelayMs(String account) {
        if (!accountFanoutEnabled || account == null || account.isEmpty()) {
            return 0;
        }

        AccountRecord record = attemptsByAccount.get(account);

        if (record == null) {
            return 0;
        }

        int distinctIpCount;

        synchronized (record) {
            if (System.currentTimeMillis() - record.windowStart > accountWindowMs) {
                return 0;
            }

            distinctIpCount = record.failingIps.size();
        }

        if (distinctIpCount <= accountDistinctIpThreshold) {
            return 0;
        }

        long delay = accountBaseDelayMs * (distinctIpCount - accountDistinctIpThreshold);

        return Math.min(delay, accountMaxDelayMs);
    }

    public void recordFailure(String sourceIp) {
        recordFailure(sourceIp, null);
    }

    public void recordFailure(String sourceIp, String account) {
        maybeCleanup();
        recordIpFailure(sourceIp);

        if (accountFanoutEnabled && account != null && !account.isEmpty()) {
            recordAccountFailure(account, sourceIp);
        }
    }

    private void recordIpFailure(String sourceIp) {
        long now = System.currentTimeMillis();
        IpRecord record = attemptsByIp.computeIfAbsent(sourceIp, k -> new IpRecord(now));

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

    private void recordAccountFailure(String account, String sourceIp) {
        long now = System.currentTimeMillis();
        AccountRecord record = attemptsByAccount.computeIfAbsent(account, k -> new AccountRecord(now));

        synchronized (record) {
            if (now - record.windowStart > accountWindowMs) {
                record.windowStart = now;
                record.failingIps.clear();
            }

            record.lastActivity = now;
            record.failingIps.add(sourceIp);
        }
    }

    public void recordSuccess(String sourceIp) {
        recordSuccess(sourceIp, null);
    }

    public void recordSuccess(String sourceIp, String account) {
        attemptsByIp.remove(sourceIp);

        if (accountFanoutEnabled && account != null && !account.isEmpty()) {
            attemptsByAccount.remove(account);
        }
    }

    private void maybeCleanup() {
        long now = System.currentTimeMillis();

        if (now - lastCleanupTime < CLEANUP_INTERVAL_MS) {
            return;
        }

        lastCleanupTime = now;
        long ipCutoff = now - windowMs;
        Iterator<Map.Entry<String, IpRecord>> ipIt = attemptsByIp.entrySet().iterator();
        while (ipIt.hasNext()) {
            IpRecord record = ipIt.next().getValue();

            if (record.lastActivity < ipCutoff && record.lockedUntil <= now) {
                ipIt.remove();
            }
        }

        if (accountFanoutEnabled) {
            long accountCutoff = now - accountWindowMs;
            Iterator<Map.Entry<String, AccountRecord>> acctIt = attemptsByAccount.entrySet().iterator();
            while (acctIt.hasNext()) {
                AccountRecord record = acctIt.next().getValue();
                if (record.lastActivity < accountCutoff) {
                    acctIt.remove();
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
            this.windowStart = now;
            this.lastActivity = now;
        }
    }

    private static final class AccountRecord {
        final Set<String> failingIps = ConcurrentHashMap.newKeySet();
        volatile long windowStart;
        volatile long lastActivity;

        AccountRecord(long now) {
            this.windowStart = now;
            this.lastActivity = now;
        }
    }
}
