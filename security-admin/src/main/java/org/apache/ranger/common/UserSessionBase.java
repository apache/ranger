/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

 package org.apache.ranger.common;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.ranger.entity.XXAuthSession;
import org.apache.ranger.entity.XXPortalUser;

public class UserSessionBase implements Serializable {

	private static final long serialVersionUID = 1L;

	XXPortalUser xXPortalUser;
	XXAuthSession xXAuthSession;
	private boolean userAdmin;
	private int authProvider = RangerConstants.USER_APP;
	private List<String> userRoleList = new ArrayList<String>();
	int clientTimeOffsetInMinute = 0;
	public Long getUserId() {
		if (xXPortalUser != null) {
			return xXPortalUser.getId();
		}
		return null;
	}

	public String getLoginId() {
		if (xXPortalUser != null) {
			return xXPortalUser.getLoginId();
		}
		return null;
	}

	public Long getSessionId() {
		if (xXAuthSession != null) {
			return xXAuthSession.getId();
		}
		return null;
	}

	

	public boolean isUserAdmin() {
		return userAdmin;
	}

	

	
	public void setUserAdmin(boolean userAdmin) {
		this.userAdmin = userAdmin;
	}

	public XXPortalUser getXXPortalUser() {
		return xXPortalUser;
	}

	public String getUserName() {
		if (xXPortalUser != null) {
			return xXPortalUser.getFirstName() + " " + xXPortalUser.getLastName();
		}
		return null;
	}

	public void setXXAuthSession(XXAuthSession gjAuthSession) {
		this.xXAuthSession = gjAuthSession;
	}

	public void setXXPortalUser(XXPortalUser gjUser) {
		this.xXPortalUser = gjUser;
	}

	public void setAuthProvider(int userSource) {
		this.authProvider = userSource;
	}

	public void setUserRoleList(List<String> strRoleList) {
		this.userRoleList = strRoleList;
	}
	public List<String> getUserRoleList() {
		return this.userRoleList;
	}

	public int getAuthProvider() {
		return this.authProvider;
	}

	public int getClientTimeOffsetInMinute() {
		return clientTimeOffsetInMinute;
	}

	public void setClientTimeOffsetInMinute(int clientTimeOffsetInMinute) {
		this.clientTimeOffsetInMinute = clientTimeOffsetInMinute;
	}

}
