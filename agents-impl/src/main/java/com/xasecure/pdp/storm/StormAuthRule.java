package com.xasecure.pdp.storm;

import java.util.ArrayList;
import java.util.List;

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
			ret = (aTopologyName.equals(this.topologyName) || aTopologyName.matches(this.topologyName)) ;
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
			    "userList: [" + toList(userList) + "]" + 
			    "groupList: [" + toList(groupList) + "]" + 
			    "accessTypeList: [" + toList(accessTypeList) + "]" + 
			    "auditEnabled: [" + auditEnabled  + "] }";
 	}
	
	private String toList(List<String> strList) {
		StringBuilder sb = new StringBuilder() ;
		if (strList != null) {
			for(String s : strList) {
				sb.append(s).append(",") ;
			}
		}
		return sb.toString() ;
	}
	
	
	
}
