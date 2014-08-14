package com.xasecure.view;

/*
 * Copyright (c) 2014 XASecure
 * All rights reserved.
 *
 * This software is the confidential and proprietary information of
 * XASecure ("Confidential Information").  You shall not
 * disclose such Confidential Information and shall use it only in
 * accordance with the terms of the license agreement you entered into
 * with XASecure
 */

/**
 * Permission map
 * @author tushar
 */

import java.util.List;

import javax.xml.bind.annotation.*;

import org.codehaus.jackson.annotate.JsonAutoDetect;
import org.codehaus.jackson.annotate.JsonAutoDetect.Visibility;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.map.annotate.JsonSerialize;

@JsonAutoDetect(getterVisibility = Visibility.NONE, setterVisibility = Visibility.NONE, fieldVisibility = Visibility.ANY)
@JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
@XmlRootElement
public class VXPermObj implements java.io.Serializable {
	private static final long serialVersionUID = 1L;

	/**
	 * List of userName
	 */
	protected List<String> userList;
	/**
	 * List of groupName
	 */
	protected List<String> groupList;
	/**
	 * List of permission
	 */
	protected List<String> permList;
	/**
	 * IP address for groups
	 */
	protected String ipAddress;

	/**
	 * Default constructor. This will set all the attributes to default value.
	 */
	public VXPermObj() {

	}

	/**
	 * @return the userList
	 */
	public List<String> getUserList() {
		return userList;
	}

	/**
	 * @param userList
	 *            the userList to set
	 */
	public void setUserList(List<String> userList) {
		this.userList = userList;
	}

	/**
	 * @return the groupList
	 */
	public List<String> getGroupList() {
		return groupList;
	}

	/**
	 * @param groupList
	 *            the groupList to set
	 */
	public void setGroupList(List<String> groupList) {
		this.groupList = groupList;
	}

	/**
	 * @return the permList
	 */
	public List<String> getPermList() {
		return permList;
	}

	/**
	 * @param permList
	 *            the permList to set
	 */
	public void setPermList(List<String> permList) {
		this.permList = permList;
	}

	/**
	 * @return the ipAddress
	 */
	public String getIpAddress() {
		return ipAddress;
	}

	/**
	 * @param ipAddress
	 *            the ipAddress to set
	 */
	public void setIpAddress(String ipAddress) {
		this.ipAddress = ipAddress;
	}

	/**
	 * This return the bean content in string format
	 * 
	 * @return formatedStr
	 */
	public String toString() {
		String str = "VXPermMap={";
		str += super.toString();
		str += "userList={" + userList + "} ";
		str += "groupList={" + groupList + "} ";
		str += "permList={" + permList + "} ";
		str += "ipAddress={" + ipAddress + "} ";
		str += "}";
		return str;
	}
}
