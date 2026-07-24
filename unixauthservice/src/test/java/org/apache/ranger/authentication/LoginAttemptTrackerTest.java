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

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class LoginAttemptTrackerTest {
    @Test
    public void allowsAttemptsBelowThreshold() {
        LoginAttemptTracker tracker = new LoginAttemptTracker(5, 60_000, 30_000);
        String ip = "10.0.0.1";
        for (int i = 0; i < 4; i++) {
            assertFalse(tracker.isBlocked(ip), "should not be blocked before threshold");
            tracker.recordFailure(ip);
        }
        assertFalse(tracker.isBlocked(ip), "still not blocked at 4 failures with threshold 5");
    }

    @Test
    public void locksOutAfterThresholdReached() {
        LoginAttemptTracker tracker = new LoginAttemptTracker(5, 60_000, 30_000);
        String ip = "10.0.0.2";
        for (int i = 0; i < 5; i++) {
            tracker.recordFailure(ip);
        }
        assertTrue(tracker.isBlocked(ip), "should be locked out at threshold");
    }

    @Test
    public void successClearsFailureHistory() {
        LoginAttemptTracker tracker = new LoginAttemptTracker(5, 60_000, 30_000);
        String ip = "10.0.0.3";
        for (int i = 0; i < 4; i++) {
            tracker.recordFailure(ip);
        }
        tracker.recordSuccess(ip);
        assertFalse(tracker.isBlocked(ip));
        // failure count should have reset - one more failure should not lock out
        tracker.recordFailure(ip);
        assertFalse(tracker.isBlocked(ip));
    }

    @Test
    public void windowExpiryResetsFailureCount() throws InterruptedException {
        // very short window so the test runs fast
        LoginAttemptTracker tracker = new LoginAttemptTracker(3, 50, 30_000);
        String ip = "10.0.0.4";
        tracker.recordFailure(ip);
        tracker.recordFailure(ip);
        assertFalse(tracker.isBlocked(ip));
        Thread.sleep(100); // let the window expire
        tracker.recordFailure(ip); // should start a fresh window, count = 1
        assertFalse(tracker.isBlocked(ip), "window should have reset the failure count");
    }

    @Test
    public void reLocksImmediatelyIfRetriedRightAfterLockoutExpires() throws InterruptedException {
        // short lockout, long window - so lockout expires but window doesn't
        LoginAttemptTracker tracker = new LoginAttemptTracker(3, 60_000, 50);
        String ip = "10.0.0.5";
        tracker.recordFailure(ip);
        tracker.recordFailure(ip);
        tracker.recordFailure(ip);
        assertTrue(tracker.isBlocked(ip));
        Thread.sleep(100); // let the lockout expire, window is still active
        assertFalse(tracker.isBlocked(ip), "lockout should have expired");
        tracker.recordFailure(ip); // one more failure within the same window
        assertTrue(tracker.isBlocked(ip), "should re-lock immediately since window's failure count carried over");
    }

    @Test
    public void independentIpsTrackedSeparately() {
        LoginAttemptTracker tracker = new LoginAttemptTracker(3, 60_000, 30_000);
        for (int i = 0; i < 3; i++) {
            tracker.recordFailure("10.0.0.6");
        }
        assertTrue(tracker.isBlocked("10.0.0.6"));
        assertFalse(tracker.isBlocked("10.0.0.7"), "a different source IP must not be affected");
    }

    // threshold=3, base delay=200ms, max delay=2000ms, account window=60s
    private static LoginAttemptTracker newFanoutTracker() {
        return new LoginAttemptTracker(100, 60_000, 30_000, true, 3, 60_000, 200, 2000);
    }

    @Test
    public void fanout_legitimateSingleIpNeverDelayed() {
        LoginAttemptTracker tracker = newFanoutTracker();
        // simulate a real admin mistyping their password 5 times from ONE ip
        for (int i = 0; i < 5; i++) {
            tracker.recordFailure("10.0.0.1", "alice");
        }
        assertEquals(0, tracker.getAccountDelayMs("alice"), "single-IP retries must never trigger an account delay");
    }

    @Test
    public void fanout_distinctIpsAcrossThresholdTriggerDelay() {
        LoginAttemptTracker tracker = newFanoutTracker();
        tracker.recordFailure("10.0.0.1", "bob");
        tracker.recordFailure("10.0.0.2", "bob");
        assertEquals(0, tracker.getAccountDelayMs("bob"), "2 distinct IPs, threshold 3 - no delay yet");
        tracker.recordFailure("10.0.0.3", "bob");
        assertEquals(0, tracker.getAccountDelayMs("bob"), "3 distinct IPs, at threshold - still no delay");
        tracker.recordFailure("10.0.0.4", "bob");
        assertTrue(tracker.getAccountDelayMs("bob") > 0, "4th distinct IP crosses the threshold - delay should apply");
    }

    @Test
    public void fanout_delayScalesLinearlyBeforeCap() {
        LoginAttemptTracker tracker = newFanoutTracker();
        String account = "carol";
        for (int i = 0; i < 4; i++) {
            tracker.recordFailure("10.0.1." + i, account); // 4 distinct -> 1 over threshold
        }
        assertEquals(200, tracker.getAccountDelayMs(account), "4 distinct IPs -> exactly 1x base delay");
        tracker.recordFailure("10.0.1.4", account); // 5 distinct -> 2 over threshold
        assertEquals(400, tracker.getAccountDelayMs(account), "5 distinct IPs -> exactly 2x base delay");
        tracker.recordFailure("10.0.1.5", account); // 6 distinct -> 3 over threshold
        assertEquals(600, tracker.getAccountDelayMs(account), "6 distinct IPs -> exactly 3x base delay");
        tracker.recordFailure("10.0.1.6", account); // 7 distinct -> 4 over threshold
        assertEquals(800, tracker.getAccountDelayMs(account), "7 distinct IPs -> exactly 4x base delay");
    }

    @Test
    public void fanout_delayCapsInsteadOfGrowingUnbounded() {
        LoginAttemptTracker tracker = newFanoutTracker();
        String account = "carol_capped";
        for (int i = 0; i < 30; i++) {
            tracker.recordFailure("10.0.1." + i, account); // well past the threshold
        }
        assertEquals(2000, tracker.getAccountDelayMs(account), "delay must cap at accountMaxDelayMs regardless of distinct-IP count");
    }

    @Test
    public void fanout_successClearsAccountStateImmediately() {
        LoginAttemptTracker tracker = newFanoutTracker();
        String account = "dave";
        for (int i = 0; i < 5; i++) {
            tracker.recordFailure("10.0.2." + i, account);
        }
        assertTrue(tracker.getAccountDelayMs(account) > 0, "delay should be present before success");
        tracker.recordSuccess("10.0.2.99", account);
        assertEquals(0, tracker.getAccountDelayMs(account), "delay must clear immediately after a successful login");
    }

    @Test
    public void fanout_windowExpiryResetsAccountState() throws InterruptedException {
        // short account window so the test runs fast
        LoginAttemptTracker tracker = new LoginAttemptTracker(100, 60_000, 30_000, true, 2, 80, 200, 2000);
        String account = "erin";
        tracker.recordFailure("10.0.3.1", account);
        tracker.recordFailure("10.0.3.2", account);
        tracker.recordFailure("10.0.3.3", account); // 3 distinct, over threshold of 2 -> delay
        assertTrue(tracker.getAccountDelayMs(account) > 0, "delay should be present within the window");
        Thread.sleep(150); // let the account window expire
        assertEquals(0, tracker.getAccountDelayMs(account), "delay must clear once the window has expired, even without a success");
    }

    @Test
    public void fanout_disabledMeansAlwaysZero() {
        // fan-out disabled entirely
        LoginAttemptTracker tracker = new LoginAttemptTracker(100, 60_000, 30_000, false, 1, 60_000, 200, 2000);
        String account = "frank";
        for (int i = 0; i < 10; i++) {
            tracker.recordFailure("10.0.4." + i, account);
        }
        assertEquals(0, tracker.getAccountDelayMs(account), "fan-out disabled must mean the delay is always 0, regardless of distinct IPs");
    }

    @Test
    public void fanout_ipLockoutStillIndependentAndUnaffected() {
        LoginAttemptTracker tracker = new LoginAttemptTracker(3, 60_000, 30_000, true, 2, 60_000, 200, 2000);
        for (int i = 0; i < 3; i++) {
            tracker.recordFailure("10.0.5.1", "grace"); // same IP, 3 failures -> IP lockout still fires
        }
        assertTrue(tracker.isBlocked("10.0.5.1"), "existing per-IP hard lockout must still work unchanged alongside account fan-out tracking");
    }

    @Test
    public void fanout_sameIpRetriedManyTimesCountsOnce() {
        LoginAttemptTracker tracker = newFanoutTracker();
        String account = "henry";
        for (int i = 0; i < 20; i++) {
            tracker.recordFailure("10.0.6.1", account); // SAME ip, 20 times
        }
        assertEquals(0, tracker.getAccountDelayMs(account), "20 retries from one IP must never cross the distinct-IP threshold");
    }

    @Test
    public void fanout_independentAccountsDoNotCrossContaminate() {
        LoginAttemptTracker tracker = newFanoutTracker();
        for (int i = 0; i < 10; i++) {
            tracker.recordFailure("10.0.8." + i, "jack"); // fan-out attack on "jack"
        }
        assertTrue(tracker.getAccountDelayMs("jack") > 0, "the attacked account should be delayed");
        assertEquals(0, tracker.getAccountDelayMs("kate"), "an unrelated account must be completely unaffected");
    }

    @Test
    public void fanout_nullOrEmptyAccountIsSafeNoOp() {
        LoginAttemptTracker tracker = newFanoutTracker();
        // simulates PasswordValidator's request==null / unparsable-request path
        for (int i = 0; i < 10; i++) {
            tracker.recordFailure("10.0.9." + i); // 1-arg overload, no account at all
        }
        assertEquals(0, tracker.getAccountDelayMs(null), "no-account failures must never populate any account delay");
        assertEquals(0, tracker.getAccountDelayMs(""), "empty-string account must also always be a no-op");
        tracker.recordFailure("10.0.9.1", "");   // explicit empty string account
        tracker.recordFailure("10.0.9.1", null); // explicit null account
        assertEquals(0, tracker.getAccountDelayMs(""), "explicit empty/null account args must never throw and never trigger a delay");
    }

    @Test
    public void fanout_concurrentFailuresFromManyIpsAreThreadSafe() throws InterruptedException {
        // Mirrors PasswordValidator's real threading model: one thread per
        // connection, all potentially hitting the same account concurrently.
        LoginAttemptTracker tracker = newFanoutTracker();
        String account = "concurrent_victim";
        int threadCount = 50;
        Thread[] threads = new Thread[threadCount];
        for (int i = 0; i < threadCount; i++) {
            final String ip = "10.1.0." + i; // 50 distinct IPs
            threads[i] = new Thread(() -> tracker.recordFailure(ip, account));
        }
        for (Thread t : threads) {
            t.start();
        }
        for (Thread t : threads) {
            t.join(5000);
        }
        // 50 distinct IPs, threshold 3 -> 47 over threshold -> would be 47*200=9400ms
        // uncapped, so this must land exactly at the configured cap.
        assertEquals(2000, tracker.getAccountDelayMs(account), "50 concurrent distinct-IP failures must land exactly at the cap, with no race corruption");
        // an unrelated account, hit concurrently at the same time, must still
        // read back as unaffected - proves no cross-account leakage under
        // concurrent access either.
        assertEquals(0, tracker.getAccountDelayMs("bystander"), "an untouched account must remain unaffected during concurrent activity");
    }
}
