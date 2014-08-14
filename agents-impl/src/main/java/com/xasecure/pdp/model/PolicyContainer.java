package com.xasecure.pdp.model;

import java.util.List;

import com.google.gson.annotations.SerializedName;

public class PolicyContainer {
	
	@SerializedName("repository_name")
	private String 	repositoryName ;
	
	@SerializedName("last_updated") 
	private long   lastUpdatedTimeInEpoc ;
	
	@SerializedName("acl")
	private List<Policy>	acl;

	public String getRepositoryName() {
		return repositoryName;
	}
	public void setRepositoryName(String repositoryName) {
		this.repositoryName = repositoryName;
	}
	public long getLastUpdatedTimeInEpoc() {
		return lastUpdatedTimeInEpoc;
	}
	public void setLastUpdatedTimeInEpoc(long lastUpdatedTimeInEpoc) {
		this.lastUpdatedTimeInEpoc = lastUpdatedTimeInEpoc;
	}
	public List<Policy> getAcl() {
		return acl;
	}
	public void setAcl(List<Policy> acl) {
		this.acl = acl;
	}
}
