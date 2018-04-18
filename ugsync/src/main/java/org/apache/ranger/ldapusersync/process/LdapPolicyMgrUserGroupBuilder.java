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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.UnknownHostException;
import java.security.KeyStore;
import java.security.PrivilegedAction;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.StringTokenizer;
import java.util.regex.Pattern;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSession;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.security.auth.Subject;
import javax.ws.rs.core.MediaType;

import org.apache.hadoop.security.SecureClientLogin;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.ranger.unixusersync.config.UserGroupSyncConfig;
import org.apache.ranger.unixusersync.model.*;
import org.apache.ranger.usergroupsync.UserGroupSink;
import org.apache.ranger.usersync.util.UserSyncUtil;

import com.google.common.collect.Table;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import com.sun.jersey.api.client.filter.HTTPBasicAuthFilter;
import com.sun.jersey.client.urlconnection.HTTPSProperties;

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
	private boolean isMockRun = false;
	private String policyMgrBaseUrl;
	
	private UserGroupSyncConfig  config = UserGroupSyncConfig.getInstance();

	private UserGroupInfo				usergroupInfo = new UserGroupInfo();
	private GroupUserInfo				groupuserInfo = new GroupUserInfo();
	
	Table<String, String, String> groupsUsersTable;
	
	private String keyStoreFile =  null;
	private String keyStoreFilepwd = null;
	private String trustStoreFile = null;
	private String trustStoreFilepwd = null;
	private String keyStoreType = null;
	private String trustStoreType = null;
	private HostnameVerifier hv =  null;

	private SSLContext sslContext = null;
	private String authenticationType = null;
	String principal;
	String keytab;
	String nameRules;
    Map<String, String> userMap = new LinkedHashMap<String, String>();
    Map<String, String> groupMap = new LinkedHashMap<String, String>();

	static {
		try {
			LOCAL_HOSTNAME = java.net.InetAddress.getLocalHost().getCanonicalHostName();
		} catch (UnknownHostException e) {
			LOCAL_HOSTNAME = "unknown";
		}
	}
	
	synchronized public void init() throws Throwable {
		policyMgrBaseUrl = config.getPolicyManagerBaseURL();
		isMockRun = config.isMockRunEnabled();
		
		if (isMockRun) {
			LOG.setLevel(Level.DEBUG);
		}
		
		keyStoreFile =  config.getSSLKeyStorePath();
		keyStoreFilepwd = config.getSSLKeyStorePathPassword();
		trustStoreFile = config.getSSLTrustStorePath();
		trustStoreFilepwd = config.getSSLTrustStorePathPassword();
		keyStoreType = KeyStore.getDefaultType();
		trustStoreType = KeyStore.getDefaultType();
		authenticationType = config.getProperty(AUTHENTICATION_TYPE,"simple");
		try {
			principal = SecureClientLogin.getPrincipal(config.getProperty(PRINCIPAL,""), LOCAL_HOSTNAME);
		} catch (IOException ignored) {
			 // do nothing
		}
		keytab = config.getProperty(KEYTAB,"");
		nameRules = config.getProperty(NAME_RULE,"DEFAULT");
        String userGroupRoles = config.getGroupRoleRules();
        if (userGroupRoles != null && !userGroupRoles.isEmpty()) {
            getRoleForUserGroups(userGroupRoles);
        }
    }

	@Override
	public void addOrUpdateUser(String userName, List<String> groups) throws Throwable {
		//* Add user to groups mapping in the x_user table. 
		//* Here the assumption is that the user already exists in x_portal_user table.
		if ( ! isMockRun ) {
			// If the rest call to ranger admin fails, 
			// propagate the failure to the caller for retry in next sync cycle.
			if (addUserGroupInfo(userName, groups) == null ) {
				String msg = "Failed to add addorUpdate user group info";
				LOG.error(msg);
				throw new Exception(msg);
			}
		}

	}

	@Override
	public void addOrUpdateGroup(String groupName) throws Throwable {
		//* Build the group info object and do the rest call
			if ( ! isMockRun ) {
				if ( addGroupInfo(groupName) == null) {
					String msg = "Failed to add addorUpdate group info";
					LOG.error(msg);
					throw new Exception(msg);
				}
			}

	}
	
	private XGroupInfo addGroupInfo(final String groupName){
		XGroupInfo ret = null;
		XGroupInfo group = null;
		
		LOG.debug("INFO: addPMXAGroup(" + groupName + ")");
		if (! isMockRun) {
			group = addXGroupInfo(groupName);
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
	
	private XGroupInfo addXGroupInfo(String aGroupName) {
		
		XGroupInfo addGroup = new XGroupInfo();
		
		addGroup.setName(aGroupName);
		
		addGroup.setDescription(aGroupName + " - add from Unix box");
		
		addGroup.setGroupType("1");

		addGroup.setGroupSource(GROUP_SOURCE_EXTERNAL);
		groupuserInfo.setXgroupInfo(addGroup);

		return addGroup;
	}

	private XGroupInfo getAddedGroupInfo(XGroupInfo group){	
		XGroupInfo ret = null;
		
		Client c = getClient();
		
		WebResource r = c.resource(getURL(PM_ADD_GROUP_URI));
		
		Gson gson = new GsonBuilder().create();
		
		String jsonString = gson.toJson(group);
		
		LOG.debug("Group" + jsonString);
		
		String response = r.accept(MediaType.APPLICATION_JSON_TYPE).type(MediaType.APPLICATION_JSON_TYPE).post(String.class, jsonString);
		
		LOG.debug("RESPONSE: [" + response + "]");
		
		ret = gson.fromJson(response, XGroupInfo.class);
		
		return ret;	
	}

	public static void main(String[] args) throws Throwable {
		LdapPolicyMgrUserGroupBuilder  ugbuilder = new LdapPolicyMgrUserGroupBuilder();
		ugbuilder.init();

	}

	@Override
	public void addOrUpdateUser(String userName) throws Throwable {
		// First add to x_portal_user
		LOG.debug("INFO: addPMAccount(" + userName + ")" );
		if (! isMockRun) {
			if (addMUser(userName) == null) {
		        String msg = "Failed to add portal user";
		        LOG.error(msg);
		        throw new Exception(msg);
			}
		}
		List<String> groups = new ArrayList<String>();
		//* Build the user group info object with empty groups and do the rest call
		if ( ! isMockRun ) {
			// If the rest call to ranger admin fails, 
			// propagate the failure to the caller for retry in next sync cycle.
			if (addUserGroupInfo(userName, groups) == null ) {
				String msg = "Failed to add addorUpdate user group info";
				LOG.error(msg);
				throw new Exception(msg);
			}
		}
		
	}
	
	private UserGroupInfo addUserGroupInfo(String userName, List<String> groups){
		if(LOG.isDebugEnabled()) {
	 		LOG.debug("==> LdapPolicyMgrUserGroupBuilder.addUserGroupInfo " + userName + " and groups");
	 	}
		UserGroupInfo ret = null;
		XUserInfo user = null;
		LOG.debug("INFO: addPMXAUser(" + userName + ")");
		
		if (! isMockRun) {
			user = addXUserInfo(userName);
		}
		for(String g : groups) {
				LOG.debug("INFO: addPMXAGroupToUser(" + userName + "," + g + ")" );
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
	
	private XUserInfo addXUserInfo(String aUserName) {
		
		XUserInfo xuserInfo = new XUserInfo();

		xuserInfo.setName(aUserName);
		
		xuserInfo.setDescription(aUserName + " - add from Unix box");
        if (userMap.containsKey(aUserName)) {
            List<String> roleList = new ArrayList<String>();
            roleList.add(userMap.get(aUserName));
            xuserInfo.setUserRoleList(roleList);
        }
		usergroupInfo.setXuserInfo(xuserInfo);
		
		return xuserInfo;
	}

	private void addXUserGroupInfo(XUserInfo aUserInfo, List<String> aGroupList) {
		
		List<XGroupInfo> xGroupInfoList = new ArrayList<XGroupInfo>();
		
		for(String groupName : aGroupList) {
			XGroupInfo group = addXGroupInfo(groupName);
			xGroupInfoList.add(group);
			addXUserGroupInfo(aUserInfo, group);
		}
		
		usergroupInfo.setXgroupInfo(xGroupInfoList);
	}
	
	private XUserGroupInfo addXUserGroupInfo(XUserInfo aUserInfo, XGroupInfo aGroupInfo) {
		
		
	    XUserGroupInfo ugInfo = new XUserGroupInfo();
		
		ugInfo.setUserId(aUserInfo.getId());
		
		ugInfo.setGroupName(aGroupInfo.getName());
		
		// ugInfo.setParentGroupId("1");
		
        return ugInfo;
	}

	private UserGroupInfo getUsergroupInfo(UserGroupInfo ret) {
		Client c = getClient();
		
		WebResource r = c.resource(getURL(PM_ADD_USER_GROUP_INFO_URI));
		
		Gson gson = new GsonBuilder().create();
		
		String jsonString = gson.toJson(usergroupInfo);
		
		LOG.debug("USER GROUP MAPPING" + jsonString);
		
		String response = r.accept(MediaType.APPLICATION_JSON_TYPE).type(MediaType.APPLICATION_JSON_TYPE).post(String.class, jsonString);
		
		LOG.debug("RESPONSE: [" + response + "]");
		
		ret = gson.fromJson(response, UserGroupInfo.class);
		
		return ret;
	}

	@Override
	public void addOrUpdateGroup(String groupName, List<String> users) throws Throwable {
		// First get the existing group user mappings from Ranger admin.
		// Then compute the delta and send the updated group user mappings to ranger admin.
		LOG.debug("addOrUpdateGroup for " + groupName + " with users: " + users);
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
        LOG.debug("Returned users for group " + groupUserInfo.getXgroupInfo().getName() + " are: " + oldUsers);
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
		
		LOG.debug("addUsers = " + addUsers);
		delXGroupUserInfo(groupName, delUsers);
		
		//* Add user to group mapping in the x_group_user table. 
		//* Here the assumption is that the user already exists in x_portal_user table.
		if ( ! isMockRun ) {
			// If the rest call to ranger admin fails, 
			// propagate the failure to the caller for retry in next sync cycle.
			if (addGroupUserInfo(groupName, addUsers) == null ) {
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
		LOG.debug("INFO: addAuditInfo(" + auditInfo.getNoOfNewUsers() + ", " + auditInfo.getNoOfNewGroups() +
				", " + auditInfo.getNoOfModifiedUsers() + ", " + auditInfo.getNoOfModifiedGroups() +
				", " + auditInfo.getSyncSource() + ")" );

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
		Client c = getClient();

		WebResource r = c.resource(getURL(PM_AUDIT_INFO_URI));

		Gson gson = new GsonBuilder().create();

		String jsonString = gson.toJson(userInfo);

		String response = r.accept(MediaType.APPLICATION_JSON_TYPE).type(MediaType.APPLICATION_JSON_TYPE).post(String.class, jsonString);

		LOG.debug("RESPONSE[" + response + "]");

		gson.fromJson(response, UgsyncAuditInfo.class);

		LOG.debug("AuditInfo Creation successful ");
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
	
		try {

			Client c = getClient();

			String uri = PM_DEL_USER_GROUP_LINK_URI.replaceAll(Pattern.quote("${groupName}"),
					   UserSyncUtil.encodeURIParam(groupName)).replaceAll(Pattern.quote("${userName}"), UserSyncUtil.encodeURIParam(userName));

			WebResource r = c.resource(getURL(uri));

		    ClientResponse response = r.delete(ClientResponse.class);

		    if ( LOG.isDebugEnabled() ) {
		    	LOG.debug("RESPONSE: [" + response.toString() + "]");
		    }

		} catch (Exception e) {

			LOG.warn( "ERROR: Unable to delete GROUP: " + groupName  + " from USER:" + userName , e);
		}

	}
	
	private GroupUserInfo addGroupUserInfo(String groupName, List<String> users){
		if(LOG.isDebugEnabled()) {
	 		LOG.debug("==> LdapPolicyMgrUserGroupBuilder.addGroupUserInfo " + groupName + " and " + users);
	 	}
		GroupUserInfo ret = null;
		XGroupInfo group = null;
		
		LOG.debug("INFO: addPMXAGroup(" + groupName + ")" );
		if (! isMockRun) {
			group = addXGroupInfo(groupName);
		}
		for(String u : users) {
				LOG.debug("INFO: addPMXAGroupToUser(" + groupName + "," + u + ")" );
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
			XUserInfo user = addXUserInfo(userName);
			xUserInfoList.add(user);
			addXUserGroupInfo(user, aGroupInfo);
		}

		groupuserInfo.setXuserInfo(xUserInfoList);
	}

	private GroupUserInfo getGroupUserInfo(GroupUserInfo ret) {
		Client c = getClient();
		
		WebResource r = c.resource(getURL(PM_ADD_GROUP_USER_INFO_URI));
		
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
		LOG.debug("GROUP USER MAPPING" + jsonString);
		
		String response = r.accept(MediaType.APPLICATION_JSON_TYPE).type(MediaType.APPLICATION_JSON_TYPE).post(String.class, jsonString);
		
		LOG.debug("RESPONSE: [" + response + "]");
		
		ret = gson.fromJson(response, GroupUserInfo.class);
		
		return ret;
	}

	
	private MUserInfo addMUser(String aUserName) {
		MUserInfo ret = null;
		MUserInfo userInfo = new MUserInfo();

		userInfo.setLoginId(aUserName);
		userInfo.setFirstName(aUserName);
        userInfo.setLastName(aUserName);
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
		Client c = getClient();
	
	    WebResource r = c.resource(getURL(PM_ADD_LOGIN_USER_URI));
	
	    Gson gson = new GsonBuilder().create();

	    String jsonString = gson.toJson(userInfo);
	
	    String response = r.accept(MediaType.APPLICATION_JSON_TYPE).type(MediaType.APPLICATION_JSON_TYPE).post(String.class, jsonString);
	
	    LOG.debug("RESPONSE[" + response + "]");
	
	    ret = gson.fromJson(response, MUserInfo.class);
	
	    LOG.debug("MUser Creation successful " + ret);
		
		return ret;
	}
	
	public GroupUserInfo getGroupUserInfo(String groupName) {
		GroupUserInfo ret = null;
		try {

			Client c = getClient();

			String uri = PM_GET_GROUP_USER_MAP_LIST_URI.replaceAll(Pattern.quote("${groupName}"),
					   UserSyncUtil.encodeURIParam(groupName));

			WebResource r = c.resource(getURL(uri));

			String response = r.accept(MediaType.APPLICATION_JSON_TYPE).get(String.class);
			
		    Gson gson = new GsonBuilder().create();
	
		    LOG.debug("RESPONSE for " + uri + ": [" + response + "]");
	
		    ret = gson.fromJson(response, GroupUserInfo.class);
		    
		} catch (Exception e) {

			LOG.warn( "ERROR: Unable to get group user mappings for: " + groupName, e);
		}
		return ret;
	}
	
	private String getURL(String uri) {
		String ret = null;
		ret = policyMgrBaseUrl + (uri.startsWith("/") ? uri : ("/" + uri));
		return ret;
	}

	private synchronized Client getClient() {
		
		Client ret = null;
		
		if (policyMgrBaseUrl.startsWith("https://")) {
			
			ClientConfig config = new DefaultClientConfig();
			
			if (sslContext == null) {
				
				try {

				KeyManager[] kmList = null;
				TrustManager[] tmList = null;
	
				if (keyStoreFile != null && keyStoreFilepwd != null) {
	
					KeyStore keyStore = KeyStore.getInstance(keyStoreType);
					InputStream in = null;
					try {
						in = getFileInputStream(keyStoreFile);
						if (in == null) {
							LOG.error("Unable to obtain keystore from file [" + keyStoreFile + "]");
							return ret;
						}
						keyStore.load(in, keyStoreFilepwd.toCharArray());
						KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
						keyManagerFactory.init(keyStore, keyStoreFilepwd.toCharArray());
						kmList = keyManagerFactory.getKeyManagers();
					}
					finally {
						if (in != null) {
							in.close();
						}
					}
					
				}
	
				if (trustStoreFile != null && trustStoreFilepwd != null) {
	
					KeyStore trustStore = KeyStore.getInstance(trustStoreType);
					InputStream in = null;
					try {
						in = getFileInputStream(trustStoreFile);
						if (in == null) {
							LOG.error("Unable to obtain keystore from file [" + trustStoreFile + "]");
							return ret;
						}
						trustStore.load(in, trustStoreFilepwd.toCharArray());
						TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
						trustManagerFactory.init(trustStore);
						tmList = trustManagerFactory.getTrustManagers();
					}
					finally {
						if (in != null) {
							in.close();
						}
					}
				}

				sslContext = SSLContext.getInstance("SSL");
	
				sslContext.init(kmList, tmList, new SecureRandom());

				hv = new HostnameVerifier() {
					public boolean verify(String urlHostName, SSLSession session) {
						return session.getPeerHost().equals(urlHostName);
					}
				};
				}
				catch(Throwable t) {
					throw new RuntimeException("Unable to create SSLConext for communication to policy manager", t);
				}

			}

			config.getProperties().put(HTTPSProperties.PROPERTY_HTTPS_PROPERTIES, new HTTPSProperties(hv, sslContext));

			ret = Client.create(config);

			
		}
		else {
			ClientConfig cc = new DefaultClientConfig();
		    cc.getProperties().put(ClientConfig.PROPERTY_FOLLOW_REDIRECTS, true);
		    ret = Client.create(cc);	
		}
		if(!(authenticationType != null && AUTH_KERBEROS.equalsIgnoreCase(authenticationType) && SecureClientLogin.isKerberosCredentialExists(principal, keytab))){
			if(ret!=null){
				 String username = config.getPolicyMgrUserName();
				 String password = config.getPolicyMgrPassword();
				 if(username!=null && !username.trim().isEmpty() && password!=null && !password.trim().isEmpty()){
					 ret.addFilter(new HTTPBasicAuthFilter(username, password));
				 }
			}
		}
		return ret;
	}
	
	private InputStream getFileInputStream(String path) throws FileNotFoundException {

		InputStream ret = null;

		File f = new File(path);

		if (f.exists()) {
			ret = new FileInputStream(f);
		} else {
			ret = LdapPolicyMgrUserGroupBuilder.class.getResourceAsStream(path);
			
			if (ret == null) {
				if (! path.startsWith("/")) {
					ret = getClass().getResourceAsStream("/" + path);
				}
			}
			
			if (ret == null) {
				ret = ClassLoader.getSystemClassLoader().getResourceAsStream(path);
				if (ret == null) {
					if (! path.startsWith("/")) {
						ret = ClassLoader.getSystemResourceAsStream("/" + path);
					}
				}
			}
		}

		return ret;
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
