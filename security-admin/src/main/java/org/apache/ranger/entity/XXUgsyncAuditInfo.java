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

package org.apache.ranger.entity;

import org.apache.ranger.common.AppConstants;

import javax.persistence.Cacheable;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.SequenceGenerator;
import javax.persistence.Table;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;

import java.util.Date;
import java.util.Objects;

@Entity
@Cacheable
@Table(name = "x_ugsync_audit_info")
public class XXUgsyncAuditInfo extends XXDBBase implements java.io.Serializable {
    private static final long serialVersionUID = 1L;

    @Id
    @SequenceGenerator(name = "X_UGSYNC_AUDIT_INFO_SEQ", sequenceName = "X_UGSYNC_AUDIT_INFO_SEQ", allocationSize = 1)
    @GeneratedValue(strategy = GenerationType.AUTO, generator = "X_UGSYNC_AUDIT_INFO_SEQ")
    @Column(name = "id")
    protected Long id;

    @Temporal(TemporalType.TIMESTAMP)
    @Column(name = "event_time")
    protected Date eventTime;

    @Column(name = "user_name")
    protected String userName;

    @Column(name = "sync_source")
    protected String syncSource;

    @Column(name = "no_of_new_users")
    protected Long noOfNewUsers;

    @Column(name = "no_of_new_groups")
    protected Long noOfNewGroups;

    @Column(name = "no_of_modified_users")
    protected Long noOfModifiedUsers;

    @Column(name = "no_of_modified_groups")
    protected Long noOfModifiedGroups;

    @Column(name = "sync_source_info")
    protected String syncSourceInfo;

    @Column(name = "session_id")
    protected String sessionId;

    /**
     * Default constructor. This will set all the attributes to default value.
     */
    public XXUgsyncAuditInfo() {
    }

    public static boolean equals(Object object1, Object object2) {
        if (object1 == object2) {
            return true;
        }

        if ((object1 == null) || (object2 == null)) {
            return false;
        }

        return object1.equals(object2);
    }

    public int getMyClassType() {
        return AppConstants.CLASS_TYPE_UGYNC_AUDIT_INFO;
    }

    public String getMyDisplayValue() {
        return null;
    }

    public Long getId() {
        return this.id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }

    /**
     * Checks for all attributes except referenced db objects
     *
     * @return true if all attributes match
     */
    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        } else if (!super.equals(obj)) {
            return false;
        }

        XXUgsyncAuditInfo other = (XXUgsyncAuditInfo) obj;

        return Objects.equals(id, other.id) &&
                Objects.equals(eventTime, other.eventTime) &&
                Objects.equals(userName, other.userName) &&
                Objects.equals(syncSource, other.syncSource) &&
                Objects.equals(noOfNewUsers, other.noOfNewUsers) &&
                Objects.equals(noOfNewGroups, other.noOfNewGroups) &&
                Objects.equals(noOfModifiedUsers, other.noOfModifiedUsers) &&
                Objects.equals(noOfModifiedGroups, other.noOfModifiedGroups) &&
                Objects.equals(syncSourceInfo, other.syncSourceInfo) &&
                Objects.equals(sessionId, other.sessionId);
    }

    /**
     * This return the bean content in string format
     *
     * @return formatedStr
     */
    @Override
    public String toString() {
        String str = "XXUgsyncAuditInfo={";
        str += "id={" + id + "} ";
        str += "eventTime={" + eventTime + "} ";
        str += "userName={" + userName + "} ";
        str += "syncSource={" + syncSource + "} ";
        str += "noOfNewUsers={" + noOfNewUsers + "} ";
        str += "noOfNewGroups={" + noOfNewGroups + "} ";
        str += "noOfModifiedUsers={" + noOfModifiedUsers + "} ";
        str += "noOfModifiedGroups={" + noOfModifiedGroups + "} ";
        str += "syncSourceInfo={" + syncSourceInfo + "} ";
        str += "sessionId={" + sessionId + "} ";
        str += "}";
        return str;
    }

    public Date getEventTime() {
        return eventTime;
    }

    public void setEventTime(Date eventTime) {
        this.eventTime = eventTime;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String getSyncSource() {
        return syncSource;
    }

    public void setSyncSource(String syncSource) {
        this.syncSource = syncSource;
    }

    public Long getNoOfNewUsers() {
        return noOfNewUsers;
    }

    public void setNoOfNewUsers(Long noOfUsers) {
        this.noOfNewUsers = noOfUsers;
    }

    public Long getNoOfModifiedUsers() {
        return noOfModifiedUsers;
    }

    public void setNoOfModifiedUsers(Long noOfModifiedUsers) {
        this.noOfModifiedUsers = noOfModifiedUsers;
    }

    public Long getNoOfNewGroups() {
        return noOfNewGroups;
    }

    public void setNoOfNewGroups(Long noOfNewGroups) {
        this.noOfNewGroups = noOfNewGroups;
    }

    public Long getNoOfModifiedGroups() {
        return noOfModifiedGroups;
    }

    public void setNoOfModifiedGroups(Long noOfModifiedGroups) {
        this.noOfModifiedGroups = noOfModifiedGroups;
    }

    public String getSyncSourceInfo() {
        return syncSourceInfo;
    }

    public void setSyncSourceInfo(String syncSourceInfo) {
        this.syncSourceInfo = syncSourceInfo;
    }

    public String getSessionId() {
        return sessionId;
    }

    public void setSessionId(String sessionId) {
        this.sessionId = sessionId;
    }
}
