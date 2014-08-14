package com.xasecure.pdp.model;

import java.util.ArrayList;
import java.util.List;

public class RolePermission {
	
	private List<String>	users ;
	private List<String> 	groups ;
	private List<String>	access ;
	private List<String>	ipAddress ;
	
	public RolePermission() {
		users  = new ArrayList<String>() ;
		groups = new ArrayList<String>() ;
		access = new ArrayList<String>() ;
	}
	

	public List<String> getUsers() {
		return users;
	}

	public void setUsers(List<String> users) {
		this.users = users;
	}

	public List<String> getGroups() {
		return groups;
	}
	
	public void setGroups(List<String> groups) {
		this.groups = groups;
	}
	
	public List<String> getAccess() {
		return this.access;
	}
	
	public List<String> getIpAddress() {
		return this.ipAddress;
	}
	
	public void setIpAddress(List<String> ipAddress) {
		this.ipAddress = ipAddress ;
	}
	
	public void setAccess(List<String> access) {
		this.access = access ;
	}
	
}
