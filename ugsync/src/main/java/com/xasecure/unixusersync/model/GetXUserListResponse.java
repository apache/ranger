package com.xasecure.unixusersync.model;

import java.util.List;

import com.google.gson.annotations.SerializedName;

public class GetXUserListResponse {

	private int totalCount ;

	@SerializedName("vXUsers")
	List<XUserInfo> xuserInfoList ;
	
	public int getTotalCount() {
		return totalCount;
	}

	public void setTotalCount(int totalCount) {
		this.totalCount = totalCount;
	}

	public List<XUserInfo> getXuserInfoList() {
		return xuserInfoList;
	}

	public void setXuserInfoList(List<XUserInfo> xuserInfoList) {
		this.xuserInfoList = xuserInfoList;
	}

	
	
}
