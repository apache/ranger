package com.xasecure.unixusersync.model;

import com.google.gson.annotations.SerializedName;

public class XUserGroupInfo {

	private String userId ;
	@SerializedName("name") 
	private String groupName ;
	private String parentGroupId ;

	
	
	public String getUserId() {
		return userId;
	}

	public void setUserId(String userId) {
		this.userId = userId;
	}

	public String getGroupName() {
		return groupName;
	}

	public void setGroupName(String groupName) {
		this.groupName = groupName;
	}
	
	public String getParentGroupId() {
		return parentGroupId;
	}

	public void setParentGroupId(String parentGroupId) {
		this.parentGroupId = parentGroupId;
	}

	
	
}
