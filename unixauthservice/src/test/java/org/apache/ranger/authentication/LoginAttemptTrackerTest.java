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
}
