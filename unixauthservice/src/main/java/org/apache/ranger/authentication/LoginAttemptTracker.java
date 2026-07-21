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
    private final ConcurrentHashMap<String, Record> attemptsByIp = new ConcurrentHashMap<>();

    private final int maxFailures;
    private final long windowMs;
    private final long lockoutMs;
    private volatile long lastCleanupTime;

    public LoginAttemptTracker(int maxFailures, long windowMs, long lockoutMs) {
        this.maxFailures = maxFailures;
        this.windowMs = windowMs;
        this.lockoutMs = lockoutMs;
        this.lastCleanupTime = System.currentTimeMillis();
    }

    public boolean isBlocked(String sourceIp) {
        Record record = attemptsByIp.get(sourceIp);
        if (record == null) {
            return false;
        }
        return record.lockedUntil > System.currentTimeMillis();
    }

    public void recordFailure(String sourceIp) {
        maybeCleanup();
        long now = System.currentTimeMillis();
        Record record = attemptsByIp.computeIfAbsent(sourceIp, k -> new Record(now));
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

    public void recordSuccess(String sourceIp) {
        attemptsByIp.remove(sourceIp);
    }

    private void maybeCleanup() {
        long now = System.currentTimeMillis();
        if (now - lastCleanupTime < CLEANUP_INTERVAL_MS) {
            return;
        }
        lastCleanupTime = now;
        long cutoff = now - windowMs;
        Iterator<Map.Entry<String, Record>> it = attemptsByIp.entrySet().iterator();
        while (it.hasNext()) {
            Record record = it.next().getValue();
            if (record.lastActivity < cutoff && record.lockedUntil <= now) {
                it.remove();
            }
        }
    }

    private static final class Record {
        final AtomicInteger failCount = new AtomicInteger(0);
        volatile long windowStart;
        volatile long lastActivity;
        volatile long lockedUntil;

        Record(long now) {
            this.windowStart = now;
            this.lastActivity = now;
        }
    }
}
