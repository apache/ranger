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

package org.apache.ranger.ugsyncutil.model;

public class EntraIdSyncSourceInfo {
    private String tenantId;
    private String graphBaseUrl;
    private String authMode;
    private String membershipMode;
    private String incrementalSync;
    private String groupFilter;

    private long totalUsersSynced;
    private long totalGroupsSynced;
    private long totalUsersDeleted;
    private long totalGroupsDeleted;

    public String getTenantId() {
        return tenantId;
    }

    public void setTenantId(String tenantId) {
        this.tenantId = tenantId;
    }

    public String getGraphBaseUrl() {
        return graphBaseUrl;
    }

    public void setGraphBaseUrl(String graphBaseUrl) {
        this.graphBaseUrl = graphBaseUrl;
    }

    public String getAuthMode() {
        return authMode;
    }

    public void setAuthMode(String authMode) {
        this.authMode = authMode;
    }

    public String getMembershipMode() {
        return membershipMode;
    }

    public void setMembershipMode(String membershipMode) {
        this.membershipMode = membershipMode;
    }

    public String getIncrementalSync() {
        return incrementalSync;
    }

    public void setIncrementalSync(String incrementalSync) {
        this.incrementalSync = incrementalSync;
    }

    public String getGroupFilter() {
        return groupFilter;
    }

    public void setGroupFilter(String groupFilter) {
        this.groupFilter = groupFilter;
    }

    public long getTotalUsersSynced() {
        return totalUsersSynced;
    }

    public void setTotalUsersSynced(long totalUsersSynced) {
        this.totalUsersSynced = totalUsersSynced;
    }

    public long getTotalGroupsSynced() {
        return totalGroupsSynced;
    }

    public void setTotalGroupsSynced(long totalGroupsSynced) {
        this.totalGroupsSynced = totalGroupsSynced;
    }

    public long getTotalUsersDeleted() {
        return totalUsersDeleted;
    }

    public void setTotalUsersDeleted(long totalUsersDeleted) {
        this.totalUsersDeleted = totalUsersDeleted;
    }

    public long getTotalGroupsDeleted() {
        return totalGroupsDeleted;
    }

    public void setTotalGroupsDeleted(long totalGroupsDeleted) {
        this.totalGroupsDeleted = totalGroupsDeleted;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        toString(sb);
        return sb.toString();
    }

    public StringBuilder toString(StringBuilder sb) {
        sb.append("EntraIdSyncSourceInfo [tenantId= ").append(tenantId);
        sb.append(", graphBaseUrl= ").append(graphBaseUrl);
        sb.append(", authMode= ").append(authMode);
        sb.append(", membershipMode= ").append(membershipMode);
        sb.append(", incrementalSync= ").append(incrementalSync);
        sb.append(", groupFilter= ").append(groupFilter);
        sb.append(", totalUsersSynced= ").append(totalUsersSynced);
        sb.append(", totalGroupsSynced= ").append(totalGroupsSynced);
        sb.append(", totalUsersDeleted= ").append(totalUsersDeleted);
        sb.append(", totalGroupsDeleted= ").append(totalGroupsDeleted);
        sb.append("]");
        return sb;
    }
}
