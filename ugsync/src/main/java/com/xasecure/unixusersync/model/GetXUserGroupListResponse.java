package com.xasecure.unixusersync.model;

import java.util.List;

import com.google.gson.annotations.SerializedName;

public class GetXUserGroupListResponse {
	private int totalCount ;

	@SerializedName("vXGroupUsers")
	List<XUserGroupInfo> xusergroupInfoList ;

	public int getTotalCount() {
		return totalCount;
	}

	public void setTotalCount(int totalCount) {
		this.totalCount = totalCount;
	}

	public List<XUserGroupInfo> getXusergroupInfoList() {
		return xusergroupInfoList;
	}

	public void setXusergroupInfoList(List<XUserGroupInfo> xusergroupInfoList) {
		this.xusergroupInfoList = xusergroupInfoList;
	}

	
	
}
