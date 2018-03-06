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

import javax.persistence.*;
import javax.xml.bind.annotation.XmlRootElement;
import java.util.Date;

@Entity
@Cacheable
@XmlRootElement
@Table(name = "x_ugsync_audit_info")
public class XXUgsyncAuditInfo extends XXDBBase implements java.io.Serializable {
	private static final long serialVersionUID = 1L;

	@Id
	@SequenceGenerator(name = "X_UGSYNC_AUDIT_INFO_SEQ", sequenceName = "X_UGSYNC_AUDIT_INFO_SEQ", allocationSize = 1)
	@GeneratedValue(strategy = GenerationType.AUTO, generator = "X_UGSYNC_AUDIT_INFO_SEQ")
	@Column(name = "id")
	protected Long id;

	@Temporal(TemporalType.TIMESTAMP)
	@Column(name="event_time"   )
	protected Date eventTime;

	@Column(name = "user_name")
	protected String userName;

	@Column(name = "sync_source")
	protected String syncSource;

	@Column(name = "no_of_users")
	protected Long noOfUsers;

	@Column(name = "no_of_groups")
	protected Long noOfGroups;

	@Column(name = "sync_source_info")
	protected String syncSourceInfo;

	@Column(name="session_id")
	protected String sessionId;

	/**
	 * Default constructor. This will set all the attributes to default value.
	 */
	public XXUgsyncAuditInfo() {
	}

	public int getMyClassType( ) {
	    return AppConstants.CLASS_TYPE_UGYNC_AUDIT_INFO;
	}

	public String getMyDisplayValue() {
		return null;
	}

	public void setId(Long id) {
		this.id = id;
	}

	public Long getId() {
		return this.id;
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

	public Long getNoOfUsers() {
		return noOfUsers;
	}

	public void setNoOfUsers(Long noOfUsers) {
		this.noOfUsers = noOfUsers;
	}

	public Long getNoOfGroups() {
		return noOfGroups;
	}

	public void setNoOfGroups(Long noOfGroups) {
		this.noOfGroups = noOfGroups;
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

	/**
	 * This return the bean content in string format
	 * @return formatedStr
	*/
	@Override
	public String toString( ) {
		String str = "XXUgsyncAuditInfo={";
		str += "id={" + id + "} ";
		str += "eventTime={" + eventTime + "} ";
		str += "userName={" + userName + "} ";
		str += "syncSource={" + syncSource + "} ";
		str += "noOfUsers={" + noOfUsers + "} ";
		str += "noOfGroups={" + noOfGroups + "} ";
		str += "syncSourceInfo={" + syncSourceInfo + "} ";
		str += "sessionId={" + sessionId + "} ";
		str += "}";
		return str;
	}

	/**
	 * Checks for all attributes except referenced db objects
	 * @return true if all attributes match
	*/
	@Override
	public boolean equals( Object obj) {
		if (obj == null)
			return false;
		if (this == obj)
			return true;
		if (getClass() != obj.getClass())
			return false;
		XXUgsyncAuditInfo other = (XXUgsyncAuditInfo) obj;
		if ((this.id == null && other.id != null) || (this.id != null && !this.id.equals(other.id))) {
			return false;
		}
		if ((this.eventTime == null && other.eventTime != null) || (this.eventTime != null && !this.eventTime.equals(other.eventTime))) {
			return false;
		}
		if ((this.userName == null && other.userName != null) || (this.userName != null && !this.userName.equals(other.userName))) {
			return false;
		}
		if ((this.syncSource == null && other.syncSource != null) || (this.syncSource != null && !this.syncSource.equals(other.syncSource))) {
			return false;
		}
		if ((this.noOfUsers == null && other.noOfUsers != null) || (this.noOfUsers != null && !this.noOfUsers.equals(other.noOfUsers))) {
			return false;
		}
		if ((this.noOfGroups == null && other.noOfGroups != null) || (this.noOfGroups != null && !this.noOfGroups.equals(other.noOfGroups))) {
			return false;
		}
		if ((this.syncSourceInfo == null && other.syncSourceInfo != null) || (this.syncSourceInfo != null && !this.syncSourceInfo.equals(other.syncSourceInfo))) {
			return false;
		}
		if ((this.sessionId == null && other.sessionId != null) || (this.sessionId != null && !this.sessionId.equals(other.sessionId))) {
			return false;
		}
		return true;
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

}
