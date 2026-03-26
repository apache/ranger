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

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class RangerPdpStatsTest {
    @Test
    public void testRequestCountersAndAverageLatency() {
        RangerPdpStats stats = new RangerPdpStats();

        stats.recordRequestSuccess(5_000_000L);    // 5 ms
        stats.recordRequestBadRequest(15_000_000L); // 15 ms
        stats.recordRequestError(10_000_000L);      // 10 ms
        stats.recordAuthFailure();

        assertEquals(3L, stats.getTotalRequests());
        assertEquals(1L, stats.getTotalAuthzSuccess());
        assertEquals(1L, stats.getTotalAuthzBadRequest());
        assertEquals(1L, stats.getTotalAuthzErrors());
        assertEquals(1L, stats.getTotalAuthFailures());
        assertEquals(30_000_000L, stats.getTotalLatencyNanos());
        assertEquals(10L, stats.getAverageLatencyMs());
    }

    @Test
    public void testNegativeLatencyIsClampedToZero() {
        RangerPdpStats stats = new RangerPdpStats();

        stats.recordRequestSuccess(-100L);

        assertEquals(0L, stats.getTotalLatencyNanos());
        assertEquals(0L, stats.getAverageLatencyMs());
    }
}
