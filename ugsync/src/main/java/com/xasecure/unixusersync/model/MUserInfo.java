package com.xasecure.unixusersync.model;

public class MUserInfo {
	
	private String loginId ;
	private String firstName ; 
	private String lastName ; 
	private String emailAddress ; 
	private String[] userRoleList = { "ROLE_USER" } ;
	
	
	public String getLoginId() {
		return loginId;
	}
	public void setLoginId(String loginId) {
		this.loginId = loginId;
	}
	public String getFirstName() {
		return firstName;
	}
	public void setFirstName(String firstName) {
		this.firstName = firstName;
	}
	public String getLastName() {
		return lastName;
	}
	public void setLastName(String lastName) {
		this.lastName = lastName;
	}
	public String getEmailAddress() {
		return emailAddress;
	}
	public void setEmailAddress(String emailAddress) {
		this.emailAddress = emailAddress;
	}
	public String[] getUserRoleList() {
		return userRoleList;
	}
	public void setUserRoleList(String[] userRoleList) {
		this.userRoleList = userRoleList;
	} 
	
}
