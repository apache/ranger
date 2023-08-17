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

package org.apache.ranger.metrics.source;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.OperatingSystemMXBean;
import java.lang.management.ThreadMXBean;
import java.lang.management.ThreadInfo;
import java.util.Objects;

import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.ranger.metrics.RangerMetricsInfo;

public class RangerMetricsJvmSource extends RangerMetricsSource {
    private long  memoryCurrent;
    private long  memoryMaximum;
    private long  gcCountTotal;
    private long  gcTimeTotal;
    private long  gcTimeMax;
    private int   threadsBusy;
    private int   threadsBlocked;
    private int   threadsWaiting;
    private int   threadsRemaining;
    private int   processorsAvailable;
    private float systemLoadAverage;

    private final String context;

    public RangerMetricsJvmSource(String context) {
        this.context = context;
    }

    @Override
    protected void refresh() {
        // Memory
        MemoryMXBean memoryMXBean = ManagementFactory.getMemoryMXBean();
        memoryCurrent = memoryMXBean.getHeapMemoryUsage().getUsed();
        memoryMaximum = memoryMXBean.getHeapMemoryUsage().getCommitted();

        // Threads
        // reset
        threadsBusy = threadsBlocked = threadsWaiting = threadsRemaining = 0;

        ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
        for (Long threadID : threadMXBean.getAllThreadIds()) {
            ThreadInfo threadInfo = threadMXBean.getThreadInfo(threadID, 0);
            if (Objects.nonNull(threadInfo)) {
                switch(threadInfo.getThreadState().toString()) {
                    case "RUNNABLE": threadsBusy++;    break;
                    case "BLOCKED" : threadsBlocked++; break;
                    case "WAITING" : threadsWaiting++; break;

                    default        : threadsRemaining++; break;
                }
            }
        }

        // Load
        OperatingSystemMXBean osMXBean = ManagementFactory.getOperatingSystemMXBean();
        systemLoadAverage = (float) osMXBean.getSystemLoadAverage();
        processorsAvailable = osMXBean.getAvailableProcessors();

        // GC
        long totalGarbageCollections = 0;
        long garbageCollectionTime = 0;

        for(GarbageCollectorMXBean gc : ManagementFactory.getGarbageCollectorMXBeans()) {

            long count = gc.getCollectionCount();

            if(count >= 0) {
                totalGarbageCollections += count;
            }

            long time = gc.getCollectionTime();

            if(time >= 0) {
                garbageCollectionTime += time;
            }

            if (time > gcTimeMax) {
                gcTimeMax = time;
            }
        }

        gcCountTotal = totalGarbageCollections;
        gcTimeTotal = garbageCollectionTime;
    }

    @Override
    protected void update(MetricsCollector collector, boolean all) {
        collector.addRecord("RangerJvm")
        .setContext(this.context)
        .addGauge(new RangerMetricsInfo("MemoryCurrent", "Ranger current memory utilization"), memoryCurrent)
        .addGauge(new RangerMetricsInfo("MemoryMax", "Ranger max memory utilization"), memoryMaximum)
        .addGauge(new RangerMetricsInfo("GcCountTotal", "Ranger app total GCs"), gcCountTotal)
        .addGauge(new RangerMetricsInfo("GcTimeTotal", "Ranger app total GC time"), gcTimeTotal)
        .addGauge(new RangerMetricsInfo("GcTimeMax", "Ranger app MAX GC time"), gcTimeMax)
        .addGauge(new RangerMetricsInfo("ThreadsBusy", "Ranger busy threads"), threadsBusy)
        .addGauge(new RangerMetricsInfo("ThreadsBlocked", "Ranger blocked threads"), threadsBlocked)
        .addGauge(new RangerMetricsInfo("ThreadsWaiting", "Ranger waiting threads"), threadsWaiting)
        .addGauge(new RangerMetricsInfo("ThreadsRemaining", "Ranger remaining threads"), threadsRemaining)
        .addGauge(new RangerMetricsInfo("ProcessorsAvailable", "Ranger Processors available"), processorsAvailable)
        .addGauge(new RangerMetricsInfo("SystemLoadAvg", "Ranger System Load Average"), systemLoadAverage);
    }
}
