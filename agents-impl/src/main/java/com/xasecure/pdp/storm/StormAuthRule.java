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

 package com.xasecure.pdp.storm;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.FilenameUtils;

import com.xasecure.authorization.utils.StringUtil;

public class StormAuthRule {
	private String topologyName ;
	private List<String> accessTypeList ;
	private List<String> groupList ;
	private List<String> userList;
	private boolean auditEnabled ;
	
	
	public StormAuthRule(String topologyName, List<String> accessTypeList,
			List<String> userList, List<String> groupList, boolean auditEnabled) {
		super();
		this.topologyName = topologyName;
		this.accessTypeList = accessTypeList;
		if (this.accessTypeList == null) {
			this.accessTypeList = new ArrayList<String>();
		}
		this.userList = userList;
		if (this.userList == null) {
			this.userList = new ArrayList<String>();
		}

		this.groupList = groupList;
		if (this.groupList == null) {
			this.groupList = new ArrayList<String>();
		}
		
		this.auditEnabled = auditEnabled ;
	}
	
	public String getTopologyName() {
		return topologyName;
	}
	public void setTopologyName(String topologyName) {
		this.topologyName = topologyName;
	}
	public List<String> getAccessTypeList() {
		return accessTypeList;
	}
	public void setAccessTypeList(List<String> accessTypeList) {
		this.accessTypeList = accessTypeList;
	}
	public List<String> getGroupList() {
		return groupList;
	}
	public void setGroupList(List<String> groupList) {
		this.groupList = groupList;
	}
	public List<String> getUserList() {
		return userList;
	}
	public void setUserList(List<String> userList) {
		this.userList = userList;
	}

	public boolean isMatchedTopology(String aTopologyName) {
		
		boolean ret = false ;
		
		if (aTopologyName == null || aTopologyName.length() == 0) {
			ret = "*".equals(this.topologyName) ;
		}
		else {
			ret = (aTopologyName.equals(this.topologyName) ||  FilenameUtils.wildcardMatch(aTopologyName,this.topologyName)) ;
		}
		return ret ;
	}

	public boolean isOperationAllowed(String aOperationName) {
		return this.accessTypeList.contains(aOperationName);
	}
	
	private static final String PUBLIC_GROUP_NAME = "public" ;

	public boolean isUserAllowed(String aUserName, String[] aGroupList) {
		
		boolean accessAllowed = false ;
		
		if ( this.userList.contains(aUserName) ) {
			accessAllowed = true ;
		}
		else if (this.groupList.contains(PUBLIC_GROUP_NAME)) {
			accessAllowed = true ;
		}
		else if (aGroupList != null ) {
			for(String userGroup : aGroupList ) {
				if (this.groupList.contains(userGroup) ) {
					accessAllowed = true ;
					break ;
				}
			}
		}
		
		return accessAllowed ;
	}

	public boolean getAuditEnabled() {
		return this.auditEnabled ;
	}
	
	@Override
	public String toString() {
		return "StormAuthRule: { topologyName: [" + topologyName + "]," +
			    "userList: [" + StringUtil.toString(userList) + "]" + 
			    "groupList: [" + StringUtil.toString(groupList) + "]" + 
			    "accessTypeList: [" + StringUtil.toString(accessTypeList) + "]" + 
			    "auditEnabled: [" + auditEnabled  + "] }";
 	}
	
}
