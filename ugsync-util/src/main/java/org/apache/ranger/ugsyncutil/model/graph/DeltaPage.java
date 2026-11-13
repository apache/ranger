/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ranger.ugsyncutil.model.graph;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public final class DeltaPage<T> {
    private final List<DeltaEntry<T>> entries;
    private final String deltaLink;
    private final boolean resynced;

    public DeltaPage(List<DeltaEntry<T>> entries, String deltaLink) {
        this(entries, deltaLink, false);
    }

    public DeltaPage(List<DeltaEntry<T>> entries, String deltaLink, boolean resynced) {
        this.entries = entries == null ? Collections.<DeltaEntry<T>>emptyList() : Collections.unmodifiableList(new ArrayList<>(entries));
        this.deltaLink = deltaLink;
        this.resynced = resynced;
    }

    public List<DeltaEntry<T>> getEntries() {
        return entries;
    }

    public String getDeltaLink() {
        return deltaLink;
    }

    /**
     * True when this page was produced by a forced full resync after the previous delta
     * link expired or was invalidated. The caller should treat such a cycle as a
     * reconciliation (snapshot-diff delete computation), because a resync does not replay
     * {@code @removed} tombstones for objects deleted during the gap.
     */
    public boolean isResynced() {
        return resynced;
    }

    public int size() {
        return entries.size();
    }

    public boolean isEmpty() {
        return entries.isEmpty();
    }

    @Override
    public String toString() {
        return "DeltaPage{entries=" + entries.size() + ", deltaLink=" + (deltaLink == null ? "null" : "present") + '}';
    }
}
