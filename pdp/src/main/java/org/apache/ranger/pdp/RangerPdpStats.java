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

package org.apache.ranger.pdp;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class RangerPdpStats {
    private final AtomicBoolean serverStarted        = new AtomicBoolean(false);
    private final AtomicBoolean authorizerInitialized = new AtomicBoolean(false);
    private final AtomicBoolean acceptingRequests     = new AtomicBoolean(false);

    private final AtomicLong totalRequests        = new AtomicLong(0);
    private final AtomicLong totalAuthzSuccess    = new AtomicLong(0);
    private final AtomicLong totalAuthzBadRequest = new AtomicLong(0);
    private final AtomicLong totalAuthzErrors     = new AtomicLong(0);
    private final AtomicLong totalAuthFailures    = new AtomicLong(0);
    private final AtomicLong totalLatencyNanos    = new AtomicLong(0);

    public boolean isServerStarted() {
        return serverStarted.get();
    }

    public void setServerStarted(boolean value) {
        serverStarted.set(value);
    }

    public boolean isAuthorizerInitialized() {
        return authorizerInitialized.get();
    }

    public void setAuthorizerInitialized(boolean value) {
        authorizerInitialized.set(value);
    }

    public boolean isAcceptingRequests() {
        return acceptingRequests.get();
    }

    public void setAcceptingRequests(boolean value) {
        acceptingRequests.set(value);
    }

    public void recordRequestSuccess(long elapsedNanos) {
        totalRequests.incrementAndGet();
        totalAuthzSuccess.incrementAndGet();
        totalLatencyNanos.addAndGet(Math.max(0L, elapsedNanos));
    }

    public void recordRequestBadRequest(long elapsedNanos) {
        totalRequests.incrementAndGet();
        totalAuthzBadRequest.incrementAndGet();
        totalLatencyNanos.addAndGet(Math.max(0L, elapsedNanos));
    }

    public void recordRequestError(long elapsedNanos) {
        totalRequests.incrementAndGet();
        totalAuthzErrors.incrementAndGet();
        totalLatencyNanos.addAndGet(Math.max(0L, elapsedNanos));
    }

    public void recordAuthFailure() {
        totalAuthFailures.incrementAndGet();
    }

    public long getTotalRequests() {
        return totalRequests.get();
    }

    public long getTotalAuthzSuccess() {
        return totalAuthzSuccess.get();
    }

    public long getTotalAuthzBadRequest() {
        return totalAuthzBadRequest.get();
    }

    public long getTotalAuthzErrors() {
        return totalAuthzErrors.get();
    }

    public long getTotalAuthFailures() {
        return totalAuthFailures.get();
    }

    public long getTotalLatencyNanos() {
        return totalLatencyNanos.get();
    }

    public long getAverageLatencyMs() {
        long requests = totalRequests.get();
        return requests > 0 ? (totalLatencyNanos.get() / requests) / 1_000_000L : 0L;
    }
}
