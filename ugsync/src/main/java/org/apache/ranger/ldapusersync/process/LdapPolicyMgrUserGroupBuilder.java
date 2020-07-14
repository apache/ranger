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

package org.apache.ranger.ldapusersync.process;

import java.io.IOException;
import java.net.UnknownHostException;
import java.security.KeyStore;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.regex.Pattern;

import javax.security.auth.Subject;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.Cookie;
import javax.ws.rs.core.NewCookie;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.hadoop.security.SecureClientLogin;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.ranger.plugin.util.URLEncoderUtil;
import org.apache.ranger.unixusersync.config.UserGroupSyncConfig;
import org.apache.ranger.unixusersync.model.*;
import org.apache.ranger.unixusersync.process.RangerUgSyncRESTClient;
import org.apache.ranger.usergroupsync.UserGroupSink;

import com.google.common.collect.Table;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.sun.jersey.api.client.ClientResponse;

public class LdapPolicyMgrUserGroupBuilder implements UserGroupSink {

private static final Logger LOG = Logger.getLogger(LdapPolicyMgrUserGroupBuilder.class);
	
	private static final String AUTHENTICATION_TYPE = "hadoop.security.authentication";	
	private String AUTH_KERBEROS = "kerberos";
	private static final String PRINCIPAL = "ranger.usersync.kerberos.principal";
	private static final String KEYTAB = "ranger.usersync.kerberos.keytab";
	private static final String NAME_RULE = "hadoop.security.auth_to_local";
	
	public static final String PM_USER_LIST_URI  = "/service/xusers/users/";				// GET
	private static final String PM_ADD_USER_GROUP_INFO_URI = "/service/xusers/users/userinfo";	// POST
	
	private static final String PM_ADD_GROUP_USER_INFO_URI = "/service/xusers/groups/groupinfo";	// POST
	
	public static final String PM_GROUP_LIST_URI = "/service/xusers/groups/";				// GET
	private static final String PM_ADD_GROUP_URI = "/service/xusers/groups/";				// POST
	
	private static final String PM_DEL_USER_GROUP_LINK_URI = "/service/xusers/group/${groupName}/user/${userName}"; // DELETE
	
	public static final String PM_USER_GROUP_MAP_LIST_URI = "/service/xusers/groupusers/";		// GET
	
	public static final String PM_GET_GROUP_USER_MAP_LIST_URI = "/service/xusers/groupusers/groupName/${groupName}";		// GET
	
	private static final String PM_ADD_LOGIN_USER_URI = "/service/users/default";			// POST

	private static final String PM_AUDIT_INFO_URI = "/service/xusers/ugsync/auditinfo/";				// POST

	private static final String GROUP_SOURCE_EXTERNAL ="1";

	private static String LOCAL_HOSTNAME = "unknown";
	private String recordsToPullPerCall = "1000";
	private boolean isMockRun = false;
	private String policyMgrBaseUrl;
	private Cookie sessionId=null;
	private boolean isValidRangerCookie=false;
	List<NewCookie> cookieList=new ArrayList<>();

	private UserGroupSyncConfig  config = UserGroupSyncConfig.getInstance();

	private UserGroupInfo				usergroupInfo = new UserGroupInfo();
	private GroupUserInfo				groupuserInfo = new GroupUserInfo();
	private volatile RangerUgSyncRESTClient ldapUgSyncClient;
	
	Table<String, String, String> groupsUsersTable;

	private String authenticationType = null;
	String principal;
	String keytab;
	String nameRules;
    Map<String, String> userMap = new LinkedHashMap<String, String>();
    Map<String, String> groupMap = new LinkedHashMap<String, String>();
    private boolean isRangerCookieEnabled;
    private String rangerCookieName;
	static {
		try {
			LOCAL_HOSTNAME = java.net.InetAddress.getLocalHost().getCanonicalHostName();
		} catch (UnknownHostException e) {
			LOCAL_HOSTNAME = "unknown";
		}
	}
	
	synchronized public void init() throws Throwable {
		recordsToPullPerCall = config.getMaxRecordsPerAPICall();
		policyMgrBaseUrl = config.getPolicyManagerBaseURL();
		isMockRun = config.isMockRunEnabled();
		isRangerCookieEnabled = config.isUserSyncRangerCookieEnabled();
		rangerCookieName = config.getRangerAdminCookieName();

		if (isMockRun) {
			LOG.setLevel(Level.DEBUG);
		}
		sessionId=null;
		String keyStoreFile =  config.getSSLKeyStorePath();
		String trustStoreFile = config.getSSLTrustStorePath();
		String keyStoreFilepwd = config.getSSLKeyStorePathPassword();
		String trustStoreFilepwd = config.getSSLTrustStorePathPassword();
		String keyStoreType = KeyStore.getDefaultType();
		String trustStoreType = KeyStore.getDefaultType();
		authenticationType = config.getProperty(AUTHENTICATION_TYPE,"simple");
		try {
			principal = SecureClientLogin.getPrincipal(config.getProperty(PRINCIPAL,""), LOCAL_HOSTNAME);
		} catch (IOException ignored) {
			 // do nothing
		}
		keytab = config.getProperty(KEYTAB,"");
		nameRules = config.getProperty(NAME_RULE,"DEFAULT");
		ldapUgSyncClient = new RangerUgSyncRESTClient(policyMgrBaseUrl, keyStoreFile, keyStoreFilepwd, keyStoreType,
				trustStoreFile, trustStoreFilepwd, trustStoreType, authenticationType, principal, keytab,
				config.getPolicyMgrUserName(), config.getPolicyMgrPassword());

        String userGroupRoles = config.getGroupRoleRules();
        if (userGroupRoles != null && !userGroupRoles.isEmpty()) {
            getRoleForUserGroups(userGroupRoles);
        }
        if (LOG.isDebugEnabled()) {
			LOG.debug("PolicyMgrUserGroupBuilder.init()==> PolMgrBaseUrl : "+policyMgrBaseUrl+" KeyStore File : "+keyStoreFile+" TrustStore File : "+trustStoreFile+ "Authentication Type : "+authenticationType);
		}
    }

	@Override
	public void addOrUpdateUser(String userName, List<String> groups) throws Throwable {

	}

	@Override
	public void addOrUpdateGroup(String groupName, Map<String, String> groupAttrs) throws Throwable {
		//* Build the group info object and do the rest call
			if ( ! isMockRun ) {
				if ( addGroupInfo(groupName, groupAttrs) == null) {
					String msg = "Failed to add addorUpdate group info";
					LOG.error(msg);
					throw new Exception(msg);
				}
			}

	}
	
	private XGroupInfo addGroupInfo(final String groupName, Map<String, String> groupAttrs){
		XGroupInfo ret = null;
		XGroupInfo group = null;

		if (LOG.isDebugEnabled()) {
			LOG.debug("INFO: addPMXAGroup(" + groupName + ")");
		}
		if (! isMockRun) {
			group = addXGroupInfo(groupName, groupAttrs);
		}
		if (authenticationType != null && AUTH_KERBEROS.equalsIgnoreCase(authenticationType) && SecureClientLogin.isKerberosCredentialExists(principal,keytab)) {
			try {
				LOG.info("Using principal = " + principal + " and keytab = " + keytab);
				Subject sub = SecureClientLogin.loginUserFromKeytab(principal, keytab, nameRules);
				final XGroupInfo groupInfo = group;
				ret = Subject.doAs(sub, new PrivilegedAction<XGroupInfo>() {
					@Override
					public XGroupInfo run() {
						try {
							return getAddedGroupInfo(groupInfo);
						} catch (Exception e) {
							LOG.error("Failed to build Group List : ", e);
						}
						return null;
					}
				});
				return ret;
			} catch (Exception e) {
				LOG.error("Failed to Authenticate Using given Principal and Keytab : ", e);
			}
			return null;
		} else {
			return getAddedGroupInfo(group);
		}	
	}
	
	private XGroupInfo addXGroupInfo(String aGroupName, Map<String, String> groupAttrs) {
		
		XGroupInfo addGroup = new XGroupInfo();
		
		addGroup.setName(aGroupName);
		
		addGroup.setDescription(aGroupName + " - add from Unix box");
		
		addGroup.setGroupType("1");

		addGroup.setGroupSource(GROUP_SOURCE_EXTERNAL);
		Gson gson = new Gson();
		addGroup.setOtherAttributes(gson.toJson(groupAttrs));
		groupuserInfo.setXgroupInfo(addGroup);

		return addGroup;
	}

	private XGroupInfo getAddedGroupInfo(XGroupInfo group){	
		XGroupInfo ret = null;
		String response = null;
		ClientResponse clientRes = null;
		Gson gson = new GsonBuilder().create();
		String jsonString = gson.toJson(group);
		String relativeUrl = PM_ADD_GROUP_URI;

		if(isRangerCookieEnabled){
			response = cookieBasedUploadEntity(group, relativeUrl);
		}
		else {
			if (LOG.isDebugEnabled()) {
				LOG.debug("Group" + jsonString);
			}
			try {
				clientRes = ldapUgSyncClient.post(relativeUrl, null, group);
				if (clientRes != null) {
					response = clientRes.getEntity(String.class);
				}
			}
			catch(Throwable t){
				LOG.error("Failed to get response, Error is : ", t);
			}
		}

		if ( LOG.isDebugEnabled() ) {
			LOG.debug("RESPONSE: [" + response + "]");
		}

		ret = gson.fromJson(response, XGroupInfo.class);

		return ret;
	}

	public static void main(String[] args) throws Throwable {
		LdapPolicyMgrUserGroupBuilder  ugbuilder = new LdapPolicyMgrUserGroupBuilder();
		ugbuilder.init();

	}

	@Override
	public void addOrUpdateUser(String userName) throws Throwable {

	}

	@Override
	public void addOrUpdateUser(String userName, Map<String, String> userAttrs, List<String> groups) throws Throwable {
		// First add to x_portal_user
		if (LOG.isDebugEnabled()) {
			LOG.debug("INFO: addPMAccount(" + userName + ")");
		}
		if (! isMockRun) {
			if (addMUser(userName, userAttrs) == null) {
				String msg = "Failed to add portal user";
				LOG.error(msg);
				throw new Exception(msg);
			}
		}
		//* Build the user group info object with empty groups and do the rest call
		if ( ! isMockRun ) {
			// If the rest call to ranger admin fails,
			// propagate the failure to the caller for retry in next sync cycle.
			if (addUserGroupInfo(userName, userAttrs, groups) == null ) {
				String msg = "Failed to add addorUpdate user group info";
				LOG.error(msg);
				throw new Exception(msg);
			}
		}
	}

	private UserGroupInfo addUserGroupInfo(String userName, Map<String, String> userAttrs, List<String> groups){
		if(LOG.isDebugEnabled()) {
	 		LOG.debug("==> LdapPolicyMgrUserGroupBuilder.addUserGroupInfo " + userName + " and groups");
	 	}
		UserGroupInfo ret = null;
		XUserInfo user = null;
		if (LOG.isDebugEnabled()) {
			LOG.debug("INFO: addPMXAUser(" + userName + ")");
		}
		
		if (! isMockRun) {
			user = addXUserInfo(userName, userAttrs);
		}
		if (CollectionUtils.isNotEmpty(groups)) {
			for (String g : groups) {
				if (LOG.isDebugEnabled()) {
					LOG.debug("INFO: addPMXAGroupToUser(" + userName + "," + g + ")");
				}
			}
		}
		if (! isMockRun ) {
			addXUserGroupInfo(user, groups);
		}

		if (authenticationType != null && AUTH_KERBEROS.equalsIgnoreCase(authenticationType) && SecureClientLogin.isKerberosCredentialExists(principal, keytab)){
			try {
				Subject sub = SecureClientLogin.loginUserFromKeytab(principal, keytab, nameRules);
				final UserGroupInfo result = ret;
				ret = Subject.doAs(sub, new PrivilegedAction<UserGroupInfo>() {
					@Override
					public UserGroupInfo run() {
						try {
							return getUsergroupInfo(result);
						} catch (Exception e) {
							LOG.error("Failed to add User Group Info : ", e);
						}
						return null;
					}
				});
				return ret;
			} catch (Exception e) {
				LOG.error("Failed to Authenticate Using given Principal and Keytab : " , e);
			}
			return null;
		}else{
			return getUsergroupInfo(ret);
		}
	}
	
	private XUserInfo addXUserInfo(String aUserName, Map<String, String> userAttrs) {

		if (LOG.isDebugEnabled()) {
			LOG.debug("==> LdapPolicyMgrUserGroupBuilder.addXUserInfo " + aUserName + " and " + userAttrs);
		}

		XUserInfo xuserInfo = new XUserInfo();

		xuserInfo.setName(aUserName);

		xuserInfo.setDescription(aUserName + " - add from Unix box");
		if (MapUtils.isNotEmpty(userAttrs)) {
			Gson gson = new Gson();
			xuserInfo.setOtherAttributes(gson.toJson(userAttrs));
		}
        if (userMap.containsKey(aUserName)) {
            List<String> roleList = new ArrayList<String>();
            roleList.add(userMap.get(aUserName));
            xuserInfo.setUserRoleList(roleList);
        }
		usergroupInfo.setXuserInfo(xuserInfo);

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== LdapPolicyMgrUserGroupBuilder.addXUserInfo " + aUserName + " and " + userAttrs);
		}
		
		return xuserInfo;
	}

	private void addXUserGroupInfo(XUserInfo aUserInfo, List<String> aGroupList) {

		if(LOG.isDebugEnabled()) {
			LOG.debug("==> LdapPolicyMgrUserGroupBuilder.addXUserGroupInfo ");
		}
		
		List<XGroupInfo> xGroupInfoList = new ArrayList<XGroupInfo>();

		if (CollectionUtils.isNotEmpty(aGroupList)) {
			for (String groupName : aGroupList) {
				XGroupInfo group = addXGroupInfo(groupName, null);
				xGroupInfoList.add(group);
				addXUserGroupInfo(aUserInfo, group);
			}
		}
		
		usergroupInfo.setXgroupInfo(xGroupInfoList);
		if(LOG.isDebugEnabled()) {
			LOG.debug("<== LdapPolicyMgrUserGroupBuilder.addXUserGroupInfo ");
		}
	}
	
	private XUserGroupInfo addXUserGroupInfo(XUserInfo aUserInfo, XGroupInfo aGroupInfo) {
		
		
	    XUserGroupInfo ugInfo = new XUserGroupInfo();
		
		ugInfo.setUserId(aUserInfo.getId());
		
		ugInfo.setGroupName(aGroupInfo.getName());
		
		// ugInfo.setParentGroupId("1");
		
        return ugInfo;
	}

	private UserGroupInfo getUsergroupInfo(UserGroupInfo ret) {
		if(LOG.isDebugEnabled()){
			LOG.debug("==> LdapPolicyMgrUserGroupBuilder.getUsergroupInfo(UserGroupInfo ret)");
		}
		String response = null;
		ClientResponse clientRes = null;
		Gson gson = new GsonBuilder().create();
		String jsonString = gson.toJson(usergroupInfo);
		String relativeUrl = PM_ADD_USER_GROUP_INFO_URI;

		if (LOG.isDebugEnabled()) {
			LOG.debug("USER GROUP MAPPING" + jsonString);
		}
		if(isRangerCookieEnabled){
			response = cookieBasedUploadEntity(usergroupInfo,relativeUrl);
		}
		else {
			try {
				clientRes = ldapUgSyncClient.post(relativeUrl, null, usergroupInfo);
				if (clientRes != null) {
					response = clientRes.getEntity(String.class);
				}
			}
			catch(Throwable t){
				LOG.error("Failed to get response, Error is : ", t);
			}
		}
		if ( LOG.isDebugEnabled() ) {
			LOG.debug("RESPONSE: [" + response + "]");
		}
		ret = gson.fromJson(response, UserGroupInfo.class);

		if(LOG.isDebugEnabled()){
			LOG.debug("<== LdapPolicyMgrUserGroupBuilder.getUsergroupInfo (UserGroupInfo ret)");
		}
		return ret;
	}

	@Override
	public void addOrUpdateGroup(String groupName, List<String> users) throws Throwable {
	}

	@Override
	public void addOrUpdateGroup(String groupName, Map<String, String> groupAttrs, List<String> users) throws Throwable {
		// First get the existing group user mappings from Ranger admin.
		// Then compute the delta and send the updated group user mappings to ranger admin.
		if (LOG.isDebugEnabled()) {
			LOG.debug("addOrUpdateGroup for " + groupName + " with users: " + users);
		}
		GroupUserInfo groupUserInfo = null;
		if (authenticationType != null && AUTH_KERBEROS.equalsIgnoreCase(authenticationType) && SecureClientLogin.isKerberosCredentialExists(principal,keytab)) {
			try {
				LOG.info("Using principal = " + principal + " and keytab = " + keytab);
				Subject sub = SecureClientLogin.loginUserFromKeytab(principal, keytab, nameRules);
				final String gName = groupName;
				groupUserInfo = Subject.doAs(sub, new PrivilegedAction<GroupUserInfo>() {
					@Override
					public GroupUserInfo run() {
						try {
							return getGroupUserInfo(gName);
						} catch (Exception e) {
							LOG.error("Failed to build Group List : ", e);
						}
						return null;
					}
				});
			} catch (Exception e) {
				LOG.error("Failed to Authenticate Using given Principal and Keytab : ", e);
			}
		} else {
			groupUserInfo = getGroupUserInfo(groupName);
		}	
		
        List<String> oldUsers = new ArrayList<String>();
        Map<String, List<String>> oldUserMap = new HashMap<String, List<String>>();
        if (groupUserInfo != null && groupUserInfo.getXuserInfo() != null) {
            for (XUserInfo xUserInfo : groupUserInfo.getXuserInfo()) {
                oldUsers.add(xUserInfo.getName());
                oldUserMap.put(xUserInfo.getName(), xUserInfo.getUserRoleList());
            }
			if (LOG.isDebugEnabled()) {
				LOG.debug("Returned users for group " + groupUserInfo.getXgroupInfo().getName() + " are: " + oldUsers);
			}
		}
		
		List<String> addUsers = new ArrayList<String>();
		List<String> delUsers = new ArrayList<String>();
		
		for (String user : oldUsers) {
			if (!users.contains(user)) {
				delUsers.add(user);
			}
		}
		if (oldUsers.isEmpty()) {
			addUsers = users;
		} else {
            for (String user : users) {
                if (!oldUsers.contains(user)|| !(oldUserMap.get(user).contains(groupMap.get(groupName)))) {
                    addUsers.add(user);
                }
			}
		}
		if (LOG.isDebugEnabled()) {
			LOG.debug("addUsers = " + addUsers);
		}
		delXGroupUserInfo(groupName, delUsers);
		
		//* Add user to group mapping in the x_group_user table. 
		//* Here the assumption is that the user already exists in x_portal_user table.
		if ( ! isMockRun ) {
			// If the rest call to ranger admin fails, 
			// propagate the failure to the caller for retry in next sync cycle.
			if (addGroupUserInfo(groupName, groupAttrs, addUsers) == null ) {
				String msg = "Failed to add addorUpdate group user info";
				LOG.error(msg);
				throw new Exception(msg);
			}
		}
	}

	@Override
	public void postUserGroupAuditInfo(UgsyncAuditInfo ugsyncAuditInfo) throws Throwable {
		if (! isMockRun) {
			addUserGroupAuditInfo(ugsyncAuditInfo);
		}

	}

	private void addUserGroupAuditInfo(UgsyncAuditInfo auditInfo) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("INFO: addAuditInfo(" + auditInfo.getNoOfNewUsers() + ", " + auditInfo.getNoOfNewGroups() +
					", " + auditInfo.getNoOfModifiedUsers() + ", " + auditInfo.getNoOfModifiedGroups() +
					", " + auditInfo.getSyncSource() + ")");
		}

		if (authenticationType != null
				&& AUTH_KERBEROS.equalsIgnoreCase(authenticationType)
				&& SecureClientLogin.isKerberosCredentialExists(principal,
				keytab)) {
			try {
				Subject sub = SecureClientLogin.loginUserFromKeytab(principal, keytab, nameRules);
				final UgsyncAuditInfo auditInfoFinal = auditInfo;
				Subject.doAs(sub, new PrivilegedAction<Void>() {
					@Override
					public Void run() {
						try {
							getUserGroupAuditInfo(auditInfoFinal);
						} catch (Exception e) {
							LOG.error("Failed to add User : ", e);
						}
						return null;
					}
				});
				return;
			} catch (Exception e) {
				LOG.error("Failed to Authenticate Using given Principal and Keytab : " , e);
			}
			return;
		} else {
			getUserGroupAuditInfo(auditInfo);
		}
	}


	private void getUserGroupAuditInfo(UgsyncAuditInfo userInfo) {
		if(LOG.isDebugEnabled()){
			LOG.debug("==> PolicyMgrUserGroupBuilder.getUserGroupAuditInfo()");
		}
		String response = null;
		ClientResponse clientRes = null;
		Gson gson = new GsonBuilder().create();
		String relativeUrl = PM_AUDIT_INFO_URI;

		if(isRangerCookieEnabled){
			response = cookieBasedUploadEntity(userInfo, relativeUrl);
		}
		else {
			try {
				clientRes = ldapUgSyncClient.post(relativeUrl, null, userInfo);
				if (clientRes != null) {
					response = clientRes.getEntity(String.class);
				}
			}
			catch(Throwable t){
				LOG.error("Failed to get response, Error is : ", t);
			}
		}
		if (LOG.isDebugEnabled()) {
			LOG.debug("RESPONSE[" + response + "]");
		}
		gson.fromJson(response, UgsyncAuditInfo.class);

		if(LOG.isDebugEnabled()){
			LOG.debug("AuditInfo Creation successful ");
			LOG.debug("<== LdapPolicyMgrUserGroupBuilder.getUserGroupAuditInfo()");
		}
	}

	private void delXGroupUserInfo(final String groupName, List<String> userList) {
		if(LOG.isDebugEnabled()) {
	 		LOG.debug("==> LdapPolicyMgrUserGroupBuilder.delXGroupUserInfo " + groupName + " and " + userList);
	 	}
		for(final String userName : userList) {
			if (authenticationType != null && AUTH_KERBEROS.equalsIgnoreCase(authenticationType) && SecureClientLogin.isKerberosCredentialExists(principal, keytab)) {
				try {
					LOG.info("Using principal = " + principal + " and keytab = " + keytab);
					Subject sub = SecureClientLogin.loginUserFromKeytab(principal, keytab, nameRules);
					Subject.doAs(sub, new PrivilegedAction<Void>() {
						@Override
						public Void run() {
							try {
								delXGroupUserInfo(groupName, userName);
							} catch (Exception e) {
								LOG.error("Failed to build Group List : ", e);
							}
							return null;
						}
					});
				} catch (Exception e) {
					LOG.error("Failed to Authenticate Using given Principal and Keytab : ",e);
				}
			} else {
				delXGroupUserInfo(groupName, userName);
			}
		}
	}

	private void delXGroupUserInfo(String groupName, String userName) {

		if (LOG.isDebugEnabled()) {
			LOG.debug("==> LdapPolicyMgrUserGroupBuilder.delXUserGroupInfo()");
		}

		try {
			ClientResponse response = null;

			String relativeUrl = PM_DEL_USER_GROUP_LINK_URI.replaceAll(Pattern.quote("${groupName}"),
					   URLEncoderUtil.encodeURIParam(groupName)).replaceAll(Pattern.quote("${userName}"), URLEncoderUtil.encodeURIParam(userName));
			if (isRangerCookieEnabled) {
				if (sessionId != null && isValidRangerCookie) {
					response = ldapUgSyncClient.delete(relativeUrl, null, sessionId);
					if (response != null) {
						if (!(response.toString().contains(relativeUrl))) {
							response.setStatus(HttpServletResponse.SC_NOT_FOUND);
							sessionId = null;
							isValidRangerCookie = false;
						} else if (response.getStatus() == HttpServletResponse.SC_UNAUTHORIZED) {
							LOG.warn("response from ranger is 401 unauthorized");
							sessionId = null;
							isValidRangerCookie = false;
						} else if (response.getStatus() == HttpServletResponse.SC_NO_CONTENT
								|| response.getStatus() == HttpServletResponse.SC_OK) {
							cookieList = response.getCookies();
							for (NewCookie cookie : cookieList) {
								if (cookie.getName().equalsIgnoreCase(rangerCookieName)) {
									sessionId = cookie.toCookie();
									isValidRangerCookie = true;
									break;
								}
							}
						}

						if (response.getStatus() != HttpServletResponse.SC_OK && response.getStatus() != HttpServletResponse.SC_NO_CONTENT
								&& response.getStatus() != HttpServletResponse.SC_BAD_REQUEST) {
							sessionId = null;
							isValidRangerCookie = false;
						}
					}
				}
			}
			else {
				response = ldapUgSyncClient.delete(relativeUrl, null);
			}
		    if ( LOG.isDebugEnabled() ) {
		    	LOG.debug("RESPONSE: [" + response.toString() + "]");
		    }
		} catch (Exception e) {
			LOG.warn( "ERROR: Unable to delete GROUP: " + groupName  + " from USER:" + userName , e);
		}
		if (LOG.isDebugEnabled()) {
			LOG.debug("<== LdapPolicyMgrUserGroupBuilder.delXUserGroupInfo()");
		}
	}
	
	private GroupUserInfo addGroupUserInfo(String groupName, Map<String, String> groupAttrs, List<String> users){
		if(LOG.isDebugEnabled()) {
	 		LOG.debug("==> LdapPolicyMgrUserGroupBuilder.addGroupUserInfo " + groupName + " and " + users);
	 	}
		GroupUserInfo ret = null;
		XGroupInfo group = null;

		if (LOG.isDebugEnabled()) {
			LOG.debug("INFO: addPMXAGroup(" + groupName + ")");
		}
		if (! isMockRun) {
			group = addXGroupInfo(groupName, groupAttrs);
		}
		for(String u : users) {
			if (LOG.isDebugEnabled()) {
				LOG.debug("INFO: addPMXAGroupToUser(" + groupName + "," + u + ")");
			}
		}
		if (! isMockRun ) {
			addXGroupUserInfo(group, users);
		}
		if (authenticationType != null && AUTH_KERBEROS.equalsIgnoreCase(authenticationType) && SecureClientLogin.isKerberosCredentialExists(principal, keytab)){
			try {
				Subject sub = SecureClientLogin.loginUserFromKeytab(principal, keytab, nameRules);
				final GroupUserInfo result = ret;
				ret = Subject.doAs(sub, new PrivilegedAction<GroupUserInfo>() {
					@Override
					public GroupUserInfo run() {
						try {
							return getGroupUserInfo(result);
						} catch (Exception e) {
							LOG.error("Failed to add User Group Info : ", e);
						}
						return null;
					}
				});
				return ret;
			} catch (Exception e) {
				LOG.error("Failed to Authenticate Using given Principal and Keytab : " , e);
			}
			return null;
		}else{
			return getGroupUserInfo(ret);
		}
	}
	
	private void addXGroupUserInfo(XGroupInfo aGroupInfo, List<String> aUserList) {

		List<XUserInfo> xUserInfoList = new ArrayList<XUserInfo>();

		for(String userName : aUserList) {
			XUserInfo user = addXUserInfo(userName, null);
			xUserInfoList.add(user);
			addXUserGroupInfo(user, aGroupInfo);
		}

		groupuserInfo.setXuserInfo(xUserInfoList);
	}

	private GroupUserInfo getGroupUserInfo(GroupUserInfo ret) {
		if(LOG.isDebugEnabled()){
			LOG.debug("==> LdapPolicyMgrUserGroupBuilder.getGroupUserInfo(GroupUserInfo ret)");
		}
		String response = null;
		ClientResponse clientRes = null;
		String relativeUrl = PM_ADD_GROUP_USER_INFO_URI;
		Gson gson = new GsonBuilder().create();
		

        if (groupuserInfo != null
                && groupuserInfo.getXgroupInfo() != null
                && groupuserInfo.getXuserInfo() != null
                && groupMap
                        .containsKey(groupuserInfo.getXgroupInfo().getName())
                && groupuserInfo.getXuserInfo().size() > 0) {
            List<String> userRoleList = new ArrayList<String>();
            userRoleList.add(groupMap.get(groupuserInfo.getXgroupInfo()
                    .getName()));
            int i = groupuserInfo.getXuserInfo().size();
            for (int j = 0; j < i; j++) {
                if (userMap.containsKey(groupuserInfo.getXuserInfo().get(j)
                        .getName())) {
                    List<String> userRole = new ArrayList<String>();
                    userRole.add(userMap.get(groupuserInfo.getXuserInfo()
                            .get(j).getName()));
                    groupuserInfo.getXuserInfo().get(j)
                            .setUserRoleList(userRole);
                } else {
                    groupuserInfo.getXuserInfo().get(j)
                            .setUserRoleList(userRoleList);
                }
            }
        }
        String jsonString = gson.toJson(groupuserInfo);
        if (LOG.isDebugEnabled()) {
            LOG.debug("GROUP USER MAPPING" + jsonString);
        }

        if(isRangerCookieEnabled){
			response = cookieBasedUploadEntity(groupuserInfo,relativeUrl);
		}
        else {
			try {
				clientRes = ldapUgSyncClient.post(relativeUrl, null, groupuserInfo);
				if (clientRes != null) {
					response = clientRes.getEntity(String.class);
				}
			}catch(Throwable t){
				LOG.error("Failed to get response, Error is : ", t);
			}
        }
        if (LOG.isDebugEnabled()) {
			LOG.debug("RESPONSE: [" + response + "]");
		}
		ret = gson.fromJson(response, GroupUserInfo.class);
		if(LOG.isDebugEnabled()){
			LOG.debug("<== LdapPolicyMgrUserGroupBuilder.getGroupUserInfo(GroupUserInfo ret)");
		}
		return ret;
	}

	
	private MUserInfo addMUser(String aUserName, Map<String, String> userAttrs) {
		MUserInfo ret = null;
		MUserInfo userInfo = new MUserInfo();

		userInfo.setLoginId(aUserName);
		userInfo.setFirstName(aUserName);
        userInfo.setLastName(aUserName);
		Gson gson = new Gson();
        userInfo.setOtherAttributes(gson.toJson(userAttrs));
        String str[] = new String[1];
        if (userMap.containsKey(aUserName)) {
            str[0] = userMap.get(aUserName);
        }
        userInfo.setUserRoleList(str);
        if (authenticationType != null
                && AUTH_KERBEROS.equalsIgnoreCase(authenticationType)
                && SecureClientLogin.isKerberosCredentialExists(principal,
                        keytab)) {
            try {
				Subject sub = SecureClientLogin.loginUserFromKeytab(principal, keytab, nameRules);
				final MUserInfo result = ret;
				final MUserInfo userInfoFinal = userInfo;
				ret = Subject.doAs(sub, new PrivilegedAction<MUserInfo>() {
					@Override
					public MUserInfo run() {
						try {
							return getMUser(userInfoFinal, result);
						} catch (Exception e) {
							LOG.error("Failed to add User : ", e);
						}
						return null;
					}
				});
				return ret;
			} catch (Exception e) {
				LOG.error("Failed to Authenticate Using given Principal and Keytab : " , e);
			}
			return null;
		} else {
			return getMUser(userInfo, ret);
		}
	}


	private MUserInfo getMUser(MUserInfo userInfo, MUserInfo ret) {
		if(LOG.isDebugEnabled()){
			LOG.debug("==> LdapPolicyMgrUserGroupBuilder.getMUser()");
		}
		String response = null;
		ClientResponse clientRes = null;
		Gson gson = new GsonBuilder().create();
		String relativeUrl = PM_ADD_LOGIN_USER_URI;
		if (isRangerCookieEnabled) {
			response = cookieBasedUploadEntity(userInfo, relativeUrl);
		} else {
			try {
				clientRes = ldapUgSyncClient.post(relativeUrl, null, userInfo);
				if (clientRes != null) {
					response = clientRes.getEntity(String.class);
				}
			} catch (Exception e) {
				LOG.error("Failed to get response, Error is : " + e.getMessage());
			}
		}
		if (LOG.isDebugEnabled()) {
			LOG.debug("RESPONSE[" + response + "]");
		}
	    ret = gson.fromJson(response, MUserInfo.class);

	    if (LOG.isDebugEnabled()) {
			LOG.debug("MUser Creation successful " + ret);
			LOG.debug("<== LdapPolicyMgrUserGroupBuilder.getMUser()");
		}
		return ret;
	}
	
	public GroupUserInfo getGroupUserInfo(String groupName) {
		GroupUserInfo ret = null;
		if(LOG.isDebugEnabled()){
			LOG.debug("==> LdapPolicyMgrUserGroupBuilder.getGroupUserInfo(String groupName)");
		}
		try {
			String response = null;
			ClientResponse clientRes = null;
			Gson gson = new GsonBuilder().create();
			String relativeUrl = PM_GET_GROUP_USER_MAP_LIST_URI.replaceAll(Pattern.quote("${groupName}"),
					   URLEncoderUtil.encodeURIParam(groupName));

			if (isRangerCookieEnabled) {
				response = cookieBasedGetEntity(relativeUrl, 0);
			}
			else {
				clientRes = ldapUgSyncClient.get(relativeUrl, null);
				if (clientRes != null) {
					response = clientRes.getEntity(String.class);
				}
			}
			if(LOG.isDebugEnabled()){
				LOG.debug("RESPONSE for " + relativeUrl + ": [" + response + "]");
			}

		    ret = gson.fromJson(response, GroupUserInfo.class);

		} catch (Exception e) {

			LOG.warn( "ERROR: Unable to get group user mappings for: " + groupName, e);
		}
		if(LOG.isDebugEnabled()){
			LOG.debug("<== LdapPolicyMgrUserGroupBuilder.getGroupUserInfo(String groupName)");
		}
		return ret;
	}

	private String cookieBasedUploadEntity(Object obj, String apiURL ) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> LdapPolicyMgrUserGroupBuilder.cookieBasedUploadEntity()");
		}
		String response = null;
		if (sessionId != null && isValidRangerCookie) {
			response = tryUploadEntityWithCookie(obj, apiURL);
		}
		else{
			response = tryUploadEntityWithCred(obj, apiURL);
		}
		if (LOG.isDebugEnabled()) {
			LOG.debug("<== LdapPolicyMgrUserGroupBuilder.cookieBasedUploadEntity()");
		}
		return response;
	}

	private String cookieBasedGetEntity(String apiURL ,int retrievedCount) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> LdapPolicyMgrUserGroupBuilder.cookieBasedGetEntity()");
		}
		String response = null;
		if (sessionId != null && isValidRangerCookie) {
			response = tryGetEntityWithCookie(apiURL,retrievedCount);
		}
		else{
			response = tryGetEntityWithCred(apiURL,retrievedCount);
		}
		if (LOG.isDebugEnabled()) {
			LOG.debug("<== LdapPolicyMgrUserGroupBuilder.cookieBasedGetEntity()");
		}
		return response;
	}

	private String tryUploadEntityWithCookie(Object obj, String apiURL) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> LdapPolicyMgrUserGroupBuilder.tryUploadEntityWithCookie()");
		}
		String response = null;
		ClientResponse clientResp = null;
		try {
			clientResp = ldapUgSyncClient.post(apiURL, null, obj, sessionId);
		}
		catch(Throwable t){
			LOG.error("Failed to get response, Error is : ", t);
		}
		if (clientResp != null) {
			if (!(clientResp.toString().contains(apiURL))) {
				clientResp.setStatus(HttpServletResponse.SC_NOT_FOUND);
				sessionId = null;
				isValidRangerCookie = false;
			} else if (clientResp.getStatus() == HttpServletResponse.SC_UNAUTHORIZED) {
				sessionId = null;
				isValidRangerCookie = false;
			} else if (clientResp.getStatus() == HttpServletResponse.SC_NO_CONTENT || clientResp.getStatus() == HttpServletResponse.SC_OK) {
				List<NewCookie> respCookieList = clientResp.getCookies();
				for (NewCookie cookie : respCookieList) {
					if (cookie.getName().equalsIgnoreCase(rangerCookieName)) {
						if (!(sessionId.getValue().equalsIgnoreCase(cookie.toCookie().getValue()))) {
							sessionId = cookie.toCookie();
						}
						isValidRangerCookie = true;
						break;
					}
				}
			}

			if (clientResp.getStatus() != HttpServletResponse.SC_OK	&& clientResp.getStatus() != HttpServletResponse.SC_NO_CONTENT
					&& clientResp.getStatus() != HttpServletResponse.SC_BAD_REQUEST) {
				sessionId = null;
				isValidRangerCookie = false;
			}
			clientResp.bufferEntity();
			response = clientResp.getEntity(String.class);
		}
		if (LOG.isDebugEnabled()) {
			LOG.debug("<== LdapPolicyMgrUserGroupBuilder.tryUploadEntityWithCookie()");
		}
		return response;
	}


	private String tryUploadEntityWithCred(Object obj, String apiURL){
		if(LOG.isDebugEnabled()){
			LOG.debug("==> LdapPolicyMgrUserGroupBuilder.tryUploadEntityInfoWithCred()");
		}
		String response = null;
		ClientResponse clientResp = null;
		Gson gson = new GsonBuilder().create();
		String jsonString = gson.toJson(obj);

		if ( LOG.isDebugEnabled() ) {
		   LOG.debug("USER GROUP MAPPING" + jsonString);
		}
		try{
			clientResp = ldapUgSyncClient.post(apiURL, null, obj);
		}
		catch(Throwable t){
			LOG.error("Failed to get response, Error is : ", t);
		}
		if (clientResp != null) {
			if (!(clientResp.toString().contains(apiURL))) {
				clientResp.setStatus(HttpServletResponse.SC_NOT_FOUND);
			} else if (clientResp.getStatus() == HttpServletResponse.SC_UNAUTHORIZED) {
				LOG.warn("Credentials response from ranger is 401.");
			} else if (clientResp.getStatus() == HttpServletResponse.SC_OK || clientResp.getStatus() == HttpServletResponse.SC_NO_CONTENT) {
				cookieList = clientResp.getCookies();
				for (NewCookie cookie : cookieList) {
					if (cookie.getName().equalsIgnoreCase(rangerCookieName)) {
						sessionId = cookie.toCookie();
						isValidRangerCookie = true;
						LOG.info("valid cookie saved ");
						break;
					}
				}
			}
			if (clientResp.getStatus() != HttpServletResponse.SC_OK && clientResp.getStatus() != HttpServletResponse.SC_NO_CONTENT
					&& clientResp.getStatus() != HttpServletResponse.SC_BAD_REQUEST) {
				sessionId = null;
				isValidRangerCookie = false;
			}
			clientResp.bufferEntity();
			response = clientResp.getEntity(String.class);
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== LdapPolicyMgrUserGroupBuilder.tryUploadEntityInfoWithCred()");
		}
		return response;
	}

	private String tryGetEntityWithCred(String apiURL, int retrievedCount) {
		if(LOG.isDebugEnabled()){
			LOG.debug("==> LdapPolicyMgrUserGroupBuilder.tryGetEntityWithCred()");
		}
		String response = null;
		ClientResponse clientResp = null;

		Map<String, String> queryParams = new HashMap<String, String>();
		queryParams.put("pageSize", recordsToPullPerCall);
		queryParams.put("startIndex", String.valueOf(retrievedCount));
		try{
			clientResp = ldapUgSyncClient.get(apiURL, queryParams);
		}
		catch(Throwable t){
			LOG.error("Failed to get response, Error is : ", t);
		}
		if (clientResp != null) {
			if (!(clientResp.toString().contains(apiURL))) {
				clientResp.setStatus(HttpServletResponse.SC_NOT_FOUND);
			} else if (clientResp.getStatus() == HttpServletResponse.SC_UNAUTHORIZED) {
				LOG.warn("Credentials response from ranger is 401.");
			} else if (clientResp.getStatus() == HttpServletResponse.SC_OK || clientResp.getStatus() == HttpServletResponse.SC_NO_CONTENT) {
				cookieList = clientResp.getCookies();
				for (NewCookie cookie : cookieList) {
					if (cookie.getName().equalsIgnoreCase(rangerCookieName)) {
						sessionId = cookie.toCookie();
						isValidRangerCookie = true;
						LOG.info("valid cookie saved ");
						break;
					}
				}
			}
			if (clientResp.getStatus() != HttpServletResponse.SC_OK && clientResp.getStatus() != HttpServletResponse.SC_NO_CONTENT
					&& clientResp.getStatus() != HttpServletResponse.SC_BAD_REQUEST) {
				sessionId = null;
				isValidRangerCookie = false;
			}
			clientResp.bufferEntity();
			response = clientResp.getEntity(String.class);
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== LdapPolicyMgrUserGroupBuilder.tryGetEntityWithCred()");
		}
		return response;
	}


	private String tryGetEntityWithCookie(String apiURL, int retrievedCount) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> LdapPolicyMgrUserGroupBuilder.tryGetEntityWithCookie()");
		}
		String response = null;
		ClientResponse clientResp = null;

		Map<String, String> queryParams = new HashMap<String, String>();
		queryParams.put("pageSize", recordsToPullPerCall);
		queryParams.put("startIndex", String.valueOf(retrievedCount));
		try {
			clientResp = ldapUgSyncClient.get(apiURL, queryParams, sessionId);
		}
		catch(Throwable t){
			LOG.error("Failed to get response, Error is : ", t);
		}
		if (clientResp != null) {
			if (!(clientResp.toString().contains(apiURL))) {
				clientResp.setStatus(HttpServletResponse.SC_NOT_FOUND);
				sessionId = null;
				isValidRangerCookie = false;
			} else if (clientResp.getStatus() == HttpServletResponse.SC_UNAUTHORIZED) {
				sessionId = null;
				isValidRangerCookie = false;
			} else if (clientResp.getStatus() == HttpServletResponse.SC_NO_CONTENT || clientResp.getStatus() == HttpServletResponse.SC_OK) {
				List<NewCookie> respCookieList = clientResp.getCookies();
				for (NewCookie cookie : respCookieList) {
					if (cookie.getName().equalsIgnoreCase(rangerCookieName)) {
						if (!(sessionId.getValue().equalsIgnoreCase(cookie.toCookie().getValue()))) {
							sessionId = cookie.toCookie();
						}
						isValidRangerCookie = true;
						break;
					}
				}
			}

			if (clientResp.getStatus() != HttpServletResponse.SC_OK	&& clientResp.getStatus() != HttpServletResponse.SC_NO_CONTENT
					&& clientResp.getStatus() != HttpServletResponse.SC_BAD_REQUEST) {
				sessionId = null;
				isValidRangerCookie = false;
			}
			clientResp.bufferEntity();
			response = clientResp.getEntity(String.class);
		}
		if (LOG.isDebugEnabled()) {
			LOG.debug("<== LdapPolicyMgrUserGroupBuilder.tryGetEntityWithCookie()");
		}
		return response;
	}

    private void getRoleForUserGroups(String userGroupRolesData) {
        String roleDelimiter = config.getRoleDelimiter();
        String userGroupDelimiter = config.getUserGroupDelimiter();
        String userNameDelimiter = config.getUserGroupNameDelimiter();
        if (roleDelimiter == null || roleDelimiter.isEmpty()) {
            roleDelimiter = "&";
        }
        if (userGroupDelimiter == null || userGroupDelimiter.isEmpty()) {
            userGroupDelimiter = ":";
        }
        if (userNameDelimiter == null || userNameDelimiter.isEmpty()) {
            userNameDelimiter = ",";
        }
        StringTokenizer str = new StringTokenizer(userGroupRolesData,
                roleDelimiter);
        int flag = 0;
        String userGroupCheck = null;
        String roleName = null;
        while (str.hasMoreTokens()) {
            flag = 0;
            String tokens = str.nextToken();
            if (tokens != null && !tokens.isEmpty()) {
                StringTokenizer userGroupRoles = new StringTokenizer(tokens,
                        userGroupDelimiter);
                if (userGroupRoles != null) {
                    while (userGroupRoles.hasMoreElements()) {
                        String userGroupRolesTokens = userGroupRoles
                                .nextToken();
                        if (userGroupRolesTokens != null
                                && !userGroupRolesTokens.isEmpty()) {
                            flag++;
                            switch (flag) {
                            case 1:
                                roleName = userGroupRolesTokens;
                                break;
                            case 2:
                                userGroupCheck = userGroupRolesTokens;
                                break;
                            case 3:
                                StringTokenizer userGroupNames = new StringTokenizer(
                                        userGroupRolesTokens, userNameDelimiter);
                                if (userGroupNames != null) {
                                    while (userGroupNames.hasMoreElements()) {
                                        String userGroup = userGroupNames
                                                .nextToken();
                                        if (userGroup != null
                                                && !userGroup.isEmpty()) {
                                            if (userGroupCheck.trim().equalsIgnoreCase("u")) {
                                                userMap.put(userGroup.trim(), roleName.trim());
                                            } else if (userGroupCheck.trim().equalsIgnoreCase("g")) {
                                                groupMap.put(userGroup.trim(),
                                                        roleName.trim());
                                            }
                                        }
                                    }
                                }
                                break;
                            default:
                                userMap.clear();
                                groupMap.clear();
                                break;
                            }
                        }
                    }
                }
            }
        }
    }
}
