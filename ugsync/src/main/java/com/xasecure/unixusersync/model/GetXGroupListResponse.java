package com.xasecure.unixusersync.model;

import java.util.List;

import com.google.gson.annotations.SerializedName;

public class GetXGroupListResponse {
	private int totalCount ;

	@SerializedName("vXGroups")
	List<XGroupInfo> xgroupInfoList ;

	public int getTotalCount() {
		return totalCount;
	}

	public void setTotalCount(int totalCount) {
		this.totalCount = totalCount;
	}

	public List<XGroupInfo> getXgroupInfoList() {
		return xgroupInfoList;
	}

	public void setXgroupInfoList(List<XGroupInfo> xgroupInfoList) {
		this.xgroupInfoList = xgroupInfoList;
	}
	
	

}
