/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package org.apache.ranger.view;

import org.codehaus.jackson.annotate.JsonAutoDetect;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.map.annotate.JsonSerialize;
import org.springframework.beans.factory.annotation.Autowired;

import javax.xml.bind.annotation.XmlRootElement;

@JsonAutoDetect(getterVisibility= JsonAutoDetect.Visibility.NONE, setterVisibility= JsonAutoDetect.Visibility.NONE, fieldVisibility= JsonAutoDetect.Visibility.ANY)
@JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL )
@JsonIgnoreProperties(ignoreUnknown=true)
@XmlRootElement
public class RangerManagementConfig extends VXDataObject implements java.io.Serializable {
	private static final long serialVersionUID = 1L;

	@Autowired
	protected boolean isUserGroupManagementEnabled;

	@Autowired
	protected boolean isAdminDelegationEnabled;

	@Autowired
	protected boolean isServiceManagementEnabled;


	/**
	 * Default constructor. This will set all the attributes to default value.
	 */
	public RangerManagementConfig() {
	}

	/**
	 *
	 * @return the isUserGroupManagementEnabled
	 */
	public boolean isUserGroupManagementEnabled() {
		return isUserGroupManagementEnabled;
	}

	/**
	 *
	 * @param userGroupManagementEnabled sets the value of isUserGroupManagementEnabled
	 */
	public void setUserGroupManagementEnabled(boolean userGroupManagementEnabled) {
		isUserGroupManagementEnabled = userGroupManagementEnabled;
	}

	/**
	 *
	 * @return the isAdminDelegationEnabled
	 */
	public boolean isAdminDelegationEnabled() {
		return isAdminDelegationEnabled;
	}

	/**
	 *
	 * @param adminDelegationEnabled sets the value of isAdminDelegationEnabled
	 */
	public void setAdminDelegationEnabled(boolean adminDelegationEnabled) {
		isAdminDelegationEnabled = adminDelegationEnabled;
	}

	/**
	 *
	 * @return the isServiceManagementEnabled
	 */
	public boolean isServiceManagementEnabled() {
		return isServiceManagementEnabled;
	}

	/**
	 *
	 * @param serviceManagementEnabled sets the value of isServiceManagementEnabled
	 */
	public void setServiceManagementEnabled(boolean serviceManagementEnabled) {
		isServiceManagementEnabled = serviceManagementEnabled;
	}

	@Override
	public String toString() {
		String str = "RangerManagementConfig = [ ";
		str += "isUserGroupManagementEnabled: " + isUserGroupManagementEnabled + ", ";
		str += "isAdminDelegationEnabled: " + isAdminDelegationEnabled + ", ";
		str += "isServiceManagementEnabled: " + isServiceManagementEnabled + " ]";
		return str;
	}
}
