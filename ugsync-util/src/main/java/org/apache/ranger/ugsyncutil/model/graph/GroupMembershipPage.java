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

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public final class GroupMembershipPage {
    private final List<DeltaEntry<GraphGroup>> groups;
    private final Map<String, Set<String>> membersByGroupId;
    private final String deltaLink;
    private final boolean resynced;

    public GroupMembershipPage(List<DeltaEntry<GraphGroup>> groups, Map<String, Set<String>> membersByGroupId, String deltaLink, boolean resynced) {
        this.groups = (groups == null) ? Collections.emptyList() : groups;
        this.membersByGroupId = (membersByGroupId == null) ? new HashMap<>() : membersByGroupId;
        this.deltaLink = deltaLink;
        this.resynced = resynced;
    }

    public List<DeltaEntry<GraphGroup>> getGroups() {
        return groups;
    }

    public Map<String, Set<String>> getMembersByGroupId() {
        return membersByGroupId;
    }

    public Set<String> getMembers(String groupId) {
        Set<String> m = membersByGroupId.get(groupId);
        return (m == null) ? new LinkedHashSet<>() : m;
    }

    public String getDeltaLink() {
        return deltaLink;
    }

    public boolean isResynced() {
        return resynced;
    }
}
