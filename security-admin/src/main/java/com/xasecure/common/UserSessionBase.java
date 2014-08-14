package com.xasecure.common;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.xasecure.entity.XXAuthSession;
import com.xasecure.entity.XXPortalUser;

public class UserSessionBase implements Serializable {

	private static final long serialVersionUID = 1L;

	XXPortalUser xXPortalUser;
	XXAuthSession xXAuthSession;
	private boolean userAdmin;
	private int authProvider = XAConstants.USER_APP;
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
