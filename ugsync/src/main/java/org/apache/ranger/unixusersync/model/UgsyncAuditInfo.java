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

 package org.apache.ranger.unixusersync.model;

public class UgsyncAuditInfo {

	private String userName;
	private Long noOfUsers;
	private Long noOfGroups;
	private String 	syncSource;
	private String sessionId;
	private LdapSyncSourceInfo ldapSyncSourceInfo;
	private UnixSyncSourceInfo unixSyncSourceInfo;
	private FileSyncSourceInfo fileSyncSourceInfo;

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

	public String getSyncSource() {
		return syncSource;
	}

	public void setSyncSource(String syncSource) {
		this.syncSource = syncSource;
	}

	public LdapSyncSourceInfo getLdapSyncSourceInfo() {
		return ldapSyncSourceInfo;
	}

	public void setLdapSyncSourceInfo(LdapSyncSourceInfo ldapSyncSourceInfo) {
		this.ldapSyncSourceInfo = ldapSyncSourceInfo;
	}

	public UnixSyncSourceInfo getUnixSyncSourceInfo() {
		return unixSyncSourceInfo;
	}

	public void setUnixSyncSourceInfo(UnixSyncSourceInfo unixSyncSourceInfo) {
		this.unixSyncSourceInfo = unixSyncSourceInfo;
	}

	public FileSyncSourceInfo getFileSyncSourceInfo() {
		return fileSyncSourceInfo;
	}

	public void setFileSyncSourceInfo(FileSyncSourceInfo fileSyncSourceInfo) {
		this.fileSyncSourceInfo = fileSyncSourceInfo;
	}

	public String getUserName() {
		return userName;
	}

	public void setUserName(String userName) {
		this.userName = userName;
	}

	public String getSessionId() {
		return sessionId;
	}

	public void setSessionId(String sessionId) {
		this.sessionId = sessionId;
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		toString(sb);
		return sb.toString();
	}

	public StringBuilder toString(StringBuilder sb) {
		sb.append("UgsyncAuditInfo [No. of users= ").append(noOfUsers);
		sb.append(", No. of groups= ").append(noOfGroups);
		sb.append(", syncSource= ").append(syncSource);
		sb.append(", ldapSyncSourceInfo= ").append(ldapSyncSourceInfo);
		sb.append(", unixSyncSourceInfo= ").append(unixSyncSourceInfo);
		sb.append(", fileSyncSourceInfo= ").append(fileSyncSourceInfo);
		sb.append("]");
		return sb;
	}
}
