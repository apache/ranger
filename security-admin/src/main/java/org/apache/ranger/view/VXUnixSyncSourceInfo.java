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

package org.apache.ranger.view;

/**
 * UserGroupInfo
 */

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.apache.ranger.json.JsonDateSerializer;

import java.sql.Date;
import java.text.SimpleDateFormat;
import java.util.TimeZone;

@JsonAutoDetect(getterVisibility = Visibility.NONE, setterVisibility = Visibility.NONE, fieldVisibility = Visibility.ANY)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class VXUnixSyncSourceInfo implements java.io.Serializable {
    private static final long serialVersionUID = 1L;

    private String unixBackend;
    private String fileName;
    @JsonSerialize(using = JsonDateSerializer.class)
    private Date syncTime;
    @JsonSerialize(using = JsonDateSerializer.class)
    private Date lastModified;
    private String minUserId;
    private String minGroupId;
    private long   totalUsersSynced;
    private long   totalGroupsSynced;
    private long   totalUsersDeleted;
    private long   totalGroupsDeleted;

    public VXUnixSyncSourceInfo() {
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public Date getSyncTime() {
        return syncTime;
    }

    public void setSyncTime(Date syncTime) {
        this.syncTime = syncTime;
    }

    public Date getLastModified() {
        return lastModified;
    }

    public void setLastModified(Date lastModified) {
        this.lastModified = lastModified;
    }

    public String getUnixBackend() {
        return unixBackend;
    }

    public void setUnixBackend(String unixBackend) {
        this.unixBackend = unixBackend;
    }

    public String getMinUserId() {
        return minUserId;
    }

    public void setMinUserId(String minUserId) {
        this.minUserId = minUserId;
    }

    public String getMinGroupId() {
        return minGroupId;
    }

    public void setMinGroupId(String minGroupId) {
        this.minGroupId = minGroupId;
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
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
        dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));

        sb.append("{\"unixBackend\":\"").append(unixBackend);
        sb.append("\", \"fileName\":\"").append(fileName);
        sb.append("\", \"syncTime\":\"").append(dateFormat.format(syncTime == null ? new java.util.Date() : syncTime));
        sb.append("\", \"lastModified\":\"").append(dateFormat.format(lastModified == null ? new java.util.Date() : lastModified));
        sb.append("\", \"minUserId\":\"").append(minUserId);
        sb.append("\", \"minGroupId\":\"").append(minGroupId);
        sb.append("\", \"totalUsersSynced\":\"").append(totalUsersSynced);
        sb.append("\", \"totalGroupsSynced\":\"").append(totalGroupsSynced);
        sb.append("\", \"totalUsersDeleted\":\"").append(totalUsersDeleted);
        sb.append("\", \"totalGroupsDeleted\":\"").append(totalGroupsDeleted);
        sb.append("\"}");
        return sb;
    }
}
