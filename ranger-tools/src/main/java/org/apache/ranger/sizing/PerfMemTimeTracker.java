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

package org.apache.ranger.sizing;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;

public class PerfMemTimeTracker {
    public static final String STR_INTENT    = "  ";
    public static final String STR_FIELD_SEP = "|";

    private final String tag;
    private final long   startTime;
    private       long   endTime;
    private       long   gcTime;
    private final long   startMemory;
    private       long   endMemory;
    private       List<PerfMemTimeTracker> children = null;

    public PerfMemTimeTracker(String tag) {
        this.tag       = tag;
        this.startTime = System.currentTimeMillis();

        Runtime rt = Runtime.getRuntime();

        rt.runFinalization();
        rt.gc();

        this.gcTime      = System.currentTimeMillis() - startTime;
        this.startMemory = rt.totalMemory() - rt.freeMemory();
    }

    public void stop() {
        long gcStartTime = System.currentTimeMillis();

        Runtime rt = Runtime.getRuntime();

        rt.runFinalization();
        rt.gc();

        this.endTime   = System.currentTimeMillis();
        this.gcTime    += (endTime - gcStartTime);
        this.endMemory = rt.totalMemory() - rt.freeMemory();
    }

    public void addChild(PerfMemTimeTracker tracker) {
        if (children == null) {
            children = new ArrayList<>();
        }

        children.add(tracker);
    }

    public String getTag() {
        return tag;
    }

    public long getStartTime() {
        return startTime;
    }

    public long getEndTime() {
        return endTime;
    }

    public long getGcTime() {
        return gcTime;
    }

    public long getStartMemory() {
        return startMemory;
    }

    public long getEndMemory() {
        return endMemory;
    }

    public List<PerfMemTimeTracker> getChildren() {
        return children;
    }

    public long getTimeTaken() {
        return endTime - startTime - getTotalGcTime();
    }

    public long getTotalGcTime() {
        long ret = gcTime;

        if (children != null) {
            for (PerfMemTimeTracker child : children) {
                ret += child.getTotalGcTime();
            }
        }

        return ret;
    }

    public long getMemoryDelta() {
        return endMemory - startMemory;
    }

    public void print(PrintStream out, boolean printHeader) {
        if (printHeader) {
            out.println("Task|Time (ms)|Memory (bytes)");
        }

        print("", out);
    }

    @Override
    public String toString() {
        return tag + ", Memory: (start: " + startMemory + ", end: " + endMemory + ", delta: " + getMemoryDelta() + ") bytes, TimeTaken: " + getTimeTaken() + "ms";
    }

    private void print(String intentString, PrintStream out) {
        out.println(intentString + tag + STR_FIELD_SEP + getTimeTaken() + STR_FIELD_SEP + getMemoryDelta());

        if (children != null) {
            for (PerfMemTimeTracker child : children) {
                child.print(intentString + STR_INTENT, out);
            }
        }
    }
}
