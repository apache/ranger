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

package org.apache.ranger.tagsync.metrics.source;

import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicLong;

@Component
public class RangerTagSyncMetricsSourceTags extends RangerTagSyncMetricsSourceBase {
    private static final AtomicLong totalUploads = new AtomicLong(0L);
    private static final AtomicLong totalEvents  = new AtomicLong(0L);
    private static       Long       totalTimeEvents = 0L;
    private static Long totalTimeUploads = 0L;

    public RangerTagSyncMetricsSourceTags() {
        super("tagsync", "Tagsync");
    }

    public static Long getTotalTimeUploads() {
        return totalTimeUploads;
    }

    public static void updateTotalUploadsTime(Long totalTime) {
        totalTimeUploads += totalTime;
    }

    public static Long getTotalTimeEvents() {
        return totalTimeEvents;
    }

    public static void updateTotalEventsTime(Long totalTime) {
        totalTimeEvents += totalTime;
    }

    public static Long getTotalEvents() {
        return totalEvents.get();
    }

    public static void updateTotalEvents(long events) {
        totalEvents.addAndGet(events);
    }

    public static Long getTotalUploads() {
        return totalUploads.get();
    }

    public static void updateTotalUploads() {
        totalUploads.incrementAndGet();
    }

    @Override
    protected void refresh() {
        metricsMap.put("TotalUploads", getTotalUploads());
        metricsMap.put("TotalEvents", getTotalEvents());
        metricsMap.put("TotalTimeForEvents", getTotalTimeEvents());
        metricsMap.put("TotalTimeForUploads", getTotalTimeUploads());
    }
}
