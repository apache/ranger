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

public class LdapSyncSourceInfo {
	private String ldapUrl;
	private String incrementalSycn;
	private String userSearchFilter;
	private String groupSearchFilter;
	private String groupHierarchyLevel;

	public String getLdapUrl() {
		return ldapUrl;
	}

	public void setLdapUrl(String ldapUrl) {
		this.ldapUrl = ldapUrl;
	}

	public String isIncrementalSycn() {
		return incrementalSycn;
	}

	public void setIncrementalSycn(String incrementalSycn) {
		this.incrementalSycn = incrementalSycn;
	}

	public String getUserSearchFilter() {
		return userSearchFilter;
	}

	public void setUserSearchFilter(String userSearchFilter) {
		this.userSearchFilter = userSearchFilter;
	}

	public String getGroupSearchFilter() {
		return groupSearchFilter;
	}

	public void setGroupSearchFilter(String groupSearchFilter) {
		this.groupSearchFilter = groupSearchFilter;
	}

	public String getGroupHierarchyLevel() {
		return groupHierarchyLevel;
	}

	public void setGroupHierarchyLevel(String groupHierarchyLevel) {
		this.groupHierarchyLevel = groupHierarchyLevel;
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		toString(sb);
		return sb.toString();
	}

	public StringBuilder toString(StringBuilder sb) {
		sb.append("LdapSycnSourceInfo [ldapUrl= ").append(ldapUrl);
		sb.append(", isIncrementalSync= ").append(incrementalSycn);
		sb.append(", userSearchFilter= ").append(userSearchFilter);
		sb.append(", groupSearchFilter= ").append(groupSearchFilter);
		sb.append(", groupHierarchyLevel= ").append(groupHierarchyLevel);
		sb.append("]");
		return sb;
	}
}
