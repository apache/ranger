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

 package org.apache.ranger.unixusersync.process;

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
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
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
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.Cookie;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.NewCookie;

import org.apache.hadoop.security.SecureClientLogin;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.ranger.unixusersync.config.UserGroupSyncConfig;
import org.apache.ranger.unixusersync.model.GetXGroupListResponse;
import org.apache.ranger.unixusersync.model.GetXUserGroupListResponse;
import org.apache.ranger.unixusersync.model.GetXUserListResponse;
import org.apache.ranger.unixusersync.model.MUserInfo;
import org.apache.ranger.unixusersync.model.UgsyncAuditInfo;
import org.apache.ranger.unixusersync.model.UserGroupInfo;
import org.apache.ranger.unixusersync.model.XGroupInfo;
import org.apache.ranger.unixusersync.model.XUserGroupInfo;
import org.apache.ranger.unixusersync.model.XUserInfo;
import org.apache.ranger.usergroupsync.UserGroupSink;
import org.apache.ranger.usersync.util.UserSyncUtil;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import com.sun.jersey.api.client.filter.HTTPBasicAuthFilter;
import com.sun.jersey.client.urlconnection.HTTPSProperties;

public class PolicyMgrUserGroupBuilder implements UserGroupSink {

	private static final Logger LOG = Logger.getLogger(PolicyMgrUserGroupBuilder.class);

	private static final String AUTHENTICATION_TYPE = "hadoop.security.authentication";
	private String AUTH_KERBEROS = "kerberos";
	private static final String PRINCIPAL = "ranger.usersync.kerberos.principal";
	private static final String KEYTAB = "ranger.usersync.kerberos.keytab";
	private static final String NAME_RULE = "hadoop.security.auth_to_local";

	public static final String PM_USER_LIST_URI  = "/service/xusers/users/";				// GET
	private static final String PM_ADD_USER_GROUP_INFO_URI = "/service/xusers/users/userinfo";	// POST

	public static final String PM_GROUP_LIST_URI = "/service/xusers/groups/";				// GET
	private static final String PM_ADD_GROUP_URI = "/service/xusers/groups/";				// POST


	public static final String PM_USER_GROUP_MAP_LIST_URI = "/service/xusers/groupusers/";		// GET

	private static final String PM_DEL_USER_GROUP_LINK_URI = "/service/xusers/group/${groupName}/user/${userName}"; // DELETE

	private static final String PM_ADD_LOGIN_USER_URI = "/service/users/default";			// POST
	private static final String PM_AUDIT_INFO_URI = "/service/xusers/ugsync/auditinfo/";				// POST

	private static final String GROUP_SOURCE_EXTERNAL ="1";

	private static final String RANGER_ADMIN_COOKIE_NAME = "RANGERADMINSESSIONID";
	private static String LOCAL_HOSTNAME = "unknown";
	private String recordsToPullPerCall = "1000";
	private boolean isMockRun = false;
	private String policyMgrBaseUrl;

	private Cookie sessionId=null;
	private boolean isValidRangerCookie=false;
	List<NewCookie> cookieList=new ArrayList<>();

	private UserGroupSyncConfig  config = UserGroupSyncConfig.getInstance();

	private UserGroupInfo				usergroupInfo = new UserGroupInfo();
	private List<XGroupInfo> 			xgroupList = new ArrayList<XGroupInfo>();
	private List<XUserInfo> 			xuserList = new ArrayList<XUserInfo>();
	private List<XUserGroupInfo> 		xusergroupList = new ArrayList<XUserGroupInfo>();
	private HashMap<String,XUserInfo>  	userId2XUserInfoMap = new HashMap<String,XUserInfo>();
	private HashMap<String,XUserInfo>  	userName2XUserInfoMap = new HashMap<String,XUserInfo>();
	private HashMap<String,XGroupInfo>  groupName2XGroupInfoMap = new HashMap<String,XGroupInfo>();

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
	private int noOfNewUsers;
	private int noOfNewGroups;
	private int noOfModifiedUsers;
	private int noOfModifiedGroups;
	private HashSet<String> newUserList = new HashSet<String>();
	private HashSet<String> modifiedUserList = new HashSet<String>();
	private HashSet<String> newGroupList = new HashSet<String>();
	private HashSet<String> modifiedGroupList = new HashSet<String>();
	private boolean isRangerCookieEnabled;
	boolean isStartupFlag = false;

	static {
		try {
			LOCAL_HOSTNAME = java.net.InetAddress.getLocalHost().getCanonicalHostName();
		} catch (UnknownHostException e) {
			LOCAL_HOSTNAME = "unknown";
		}
	}


	public static void main(String[] args) throws Throwable {
		PolicyMgrUserGroupBuilder  ugbuilder = new PolicyMgrUserGroupBuilder();
		ugbuilder.init();
	}


	synchronized public void init() throws Throwable {
		recordsToPullPerCall = config.getMaxRecordsPerAPICall();
		policyMgrBaseUrl = config.getPolicyManagerBaseURL();
		isMockRun = config.isMockRunEnabled();
		noOfNewUsers = 0;
		noOfModifiedUsers = 0;
		noOfNewGroups = 0;
		noOfModifiedGroups = 0;
		isStartupFlag = true;
		isRangerCookieEnabled = config.isUserSyncRangerCookieEnabled();
		if (isMockRun) {
			LOG.setLevel(Level.DEBUG);
		}
		sessionId=null;
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
		buildUserGroupInfo();
	}

	private void buildUserGroupInfo() throws Throwable {
		if(authenticationType != null && AUTH_KERBEROS.equalsIgnoreCase(authenticationType) && SecureClientLogin.isKerberosCredentialExists(principal, keytab)){
			if(LOG.isDebugEnabled()) {
				LOG.debug("==> Kerberos Environment : Principal is " + principal + " and Keytab is " + keytab);
			}
		}
		if (authenticationType != null && AUTH_KERBEROS.equalsIgnoreCase(authenticationType) && SecureClientLogin.isKerberosCredentialExists(principal, keytab)) {
			try {
				LOG.info("Using principal = " + principal + " and keytab = " + keytab);
				Subject sub = SecureClientLogin.loginUserFromKeytab(principal, keytab, nameRules);
				Subject.doAs(sub, new PrivilegedAction<Void>() {
					@Override
					public Void run() {
						try {
							buildGroupList();
							buildUserList();
							buildUserGroupLinkList();
							rebuildUserGroupMap();
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
			buildGroupList();
			buildUserList();
			buildUserGroupLinkList();
			rebuildUserGroupMap();
			if (LOG.isDebugEnabled()) {
				this.print();
			}
		}
	}

	private String getURL(String uri) {
		String ret = null;
		ret = policyMgrBaseUrl + (uri.startsWith("/") ? uri : ("/" + uri));
		return ret;
	}


	private void rebuildUserGroupMap() {

		for(XUserInfo user : xuserList) {
			addUserToList(user);
		}

		for(XGroupInfo group : xgroupList) {
			addGroupToList(group);
		}


		for(XUserGroupInfo ug : xusergroupList) {
			addUserGroupToList(ug);
		}


	}


	private void addUserToList(XUserInfo aUserInfo) {
		if (! xuserList.contains(aUserInfo)) {
			xuserList.add(aUserInfo);
		}

		String userId = aUserInfo.getId();

		if (userId != null) {
			userId2XUserInfoMap.put(userId, aUserInfo);
		}

		String userName = aUserInfo.getName();

		if (userName != null) {
			userName2XUserInfoMap.put(userName, aUserInfo);
		}
	}


	private void addGroupToList(XGroupInfo aGroupInfo) {

		if (! xgroupList.contains(aGroupInfo) ) {
			xgroupList.add(aGroupInfo);
		}

		if (aGroupInfo.getName() != null) {
			groupName2XGroupInfoMap.put(aGroupInfo.getName(), aGroupInfo);
		}

	}

	private void addUserGroupToList(XUserGroupInfo ugInfo) {
		String userId = ugInfo.getUserId();

		if (userId != null) {
			XUserInfo user = userId2XUserInfoMap.get(userId);

			if (user != null) {
				List<String> groups = user.getGroups();
				if (! groups.contains(ugInfo.getGroupName())) {
					groups.add(ugInfo.getGroupName());
				}
			}
		}
	}

	private void addUserGroupInfoToList(XUserInfo userInfo, XGroupInfo groupInfo) {
		String userId = userInfo.getId();

		if (userId != null) {
			XUserInfo user = userId2XUserInfoMap.get(userId);

			if (user != null) {
				List<String> groups = user.getGroups();
				if (! groups.contains(groupInfo.getName())) {
					groups.add(groupInfo.getName());
				}
			}
		}
	}

	private void delUserGroupFromList(XUserInfo userInfo, XGroupInfo groupInfo) {
		List<String> groups = userInfo.getGroups();
		if (groups.contains(groupInfo.getName())) {
			groups.remove(groupInfo.getName());
		}
	}

	private void print() {
		LOG.debug("Number of users read [" + xuserList.size() + "]");
		for(XUserInfo user : xuserList) {
			LOG.debug("USER: " + user.getName());
			for(String group : user.getGroups()) {
				LOG.debug("\tGROUP: " + group);
			}
		}
	}

	@Override
	public void addOrUpdateUser(String userName, List<String> groups) throws Throwable {

		UserGroupInfo ugInfo		  = new UserGroupInfo();
		XUserInfo user = userName2XUserInfoMap.get(userName);

		if (groups == null) {
			groups = new ArrayList<String>();
		}
		if (user == null) {    // Does not exists
			//noOfNewUsers++;
			newUserList.add(userName);
			for (String group : groups) {
				if (groupName2XGroupInfoMap.containsKey(group) && !newGroupList.contains(group)) {
					modifiedGroupList.add(group);
				} else {
					//LOG.info("Adding new group " + group + " for user = " + userName);
					newGroupList.add(group);
				}
			}
			LOG.debug("INFO: addPMAccount(" + userName + ")" );
			if (! isMockRun) {
				if (addMUser(userName) == null) {
					String msg = "Failed to add portal user";
					LOG.error(msg);
					throw new Exception(msg);
				}
			}

			//* Build the user group info object and do the rest call
 			if ( ! isMockRun ) {
 				// If the rest call to ranger admin fails,
 				// propagate the failure to the caller for retry in next sync cycle.
 				if (addUserGroupInfo(userName,groups) == null ) {
 					String msg = "Failed to add addorUpdate user group info";
 					LOG.error(msg);
 					throw new Exception(msg);
 				}
 			}

		}
		else {					// Validate group memberships
			List<String> oldGroups = user.getGroups();
			List<String> addGroups = new ArrayList<String>();
			List<String> delGroups = new ArrayList<String>();
			List<String> updateGroups = new ArrayList<String>();
			XGroupInfo tempXGroupInfo=null;
			for(String group : groups) {
				if (! oldGroups.contains(group)) {
					addGroups.add(group);
					if (!groupName2XGroupInfoMap.containsKey(group)) {
						newGroupList.add(group);
					} else {
						modifiedGroupList.add(group);
					}
				}else{
					tempXGroupInfo=groupName2XGroupInfoMap.get(group);
					if(tempXGroupInfo!=null && ! GROUP_SOURCE_EXTERNAL.equals(tempXGroupInfo.getGroupSource())){
						updateGroups.add(group);
					}
				}
			}

			for(String group : oldGroups) {
				if (! groups.contains(group) ) {
					delGroups.add(group);
				}
			}

 			for(String g : addGroups) {
 				LOG.debug("INFO: addPMXAGroupToUser(" + userName + "," + g + ")" );
 			}
            if (!isMockRun) {
                if (!addGroups.isEmpty()) {
                    XUserInfo obj = addXUserInfo(userName);
                    if (obj != null) {
                        for (String group : addGroups) {
                            String value = groupMap.get(group);
                            if (value != null) {
                                List<String> userRoleList = new ArrayList<String>();
                                userRoleList.add(value);
                                if (userMap.containsKey(obj.getName())) {
                                    List<String> userRole = new ArrayList<String>();
                                    userRole.add(userMap.get(obj.getName()));
                                    if (!obj.getUserRoleList().equals(userRole)) {
                                        obj.setUserRoleList(userRole);

                                    }
                                } else if (!obj.getUserRoleList().equals(userRoleList)) {
                                    obj.setUserRoleList(userRoleList);
                                }
                            }
                        }
                    }
                    ugInfo.setXuserInfo(obj);
                    ugInfo.setXgroupInfo(getXGroupInfoList(addGroups));
                    try {
                        // If the rest call to ranger admin fails,
                        // propagate the failure to the caller for retry in next
                        // sync cycle.
                        if (addUserGroupInfo(ugInfo) == null) {
                            String msg = "Failed to add user group info";
                            LOG.error(msg);
                            throw new Exception(msg);
                        }
                    } catch (Throwable t) {
                        LOG.error("PolicyMgrUserGroupBuilder.addUserGroupInfo failed for user-group entry: "
								+ ugInfo.toString() + " with exception: ", t);
                    }
                }
                addXUserGroupInfo(user, addGroups);
            }

 			for(String g : delGroups) {
 				LOG.debug("INFO: delPMXAGroupFromUser(" + userName + "," + g + ")" );
 			}

 			if (! isMockRun ) {
 				delXUserGroupInfo(user, delGroups);
				//Remove groups from user mapping
				userName2XUserInfoMap.get(userName).deleteGroups(delGroups);
				LOG.debug(userName2XUserInfoMap.get(userName).getGroups());
 			}
			if (! isMockRun) {
                if (!updateGroups.isEmpty()) {
                    XUserInfo obj = addXUserInfo(userName);
                    if (obj != null) {
                        for (String group : updateGroups) {
                            String value = groupMap.get(group);
                            if (value != null) {
                                List<String> userRoleList = new ArrayList<String>();
                                userRoleList.add(value);
                                if (userMap.containsKey(obj.getName())) {
                                    List<String> userRole = new ArrayList<String>();
                                    userRole.add(userMap.get(obj.getName()));
                                    if (!obj.getUserRoleList().equals(userRole)) {
                                        obj.setUserRoleList(userRole);
                                    }
                                } else if (!obj.getUserRoleList().equals(
                                        userRoleList)) {
                                    obj.setUserRoleList(userRoleList);
                                }
                            }
                        }
                    }
                    ugInfo.setXuserInfo(obj);
                    ugInfo.setXgroupInfo(getXGroupInfoList(updateGroups));
                    try {
                        // If the rest call to ranger admin fails,
                        // propagate the failure to the caller for retry in next
                        // sync cycle.
                        if (addUserGroupInfo(ugInfo) == null) {
                            String msg = "Failed to add user group info";
                            LOG.error(msg);
                            throw new Exception(msg);
                        }
                    } catch (Throwable t) {
                        LOG.error("PolicyMgrUserGroupBuilder.addUserGroupInfo failed with exception: "
                                + t.getMessage()
                                + ", for user-group entry: "
                                + ugInfo);
                    }
                }
            }
            if (!isMockRun) {
                XUserInfo obj = addXUserInfo(userName);
                boolean roleFlag = false;
                if (obj != null && updateGroups.isEmpty()
                        && addGroups.isEmpty()) {
                    if (userMap.containsKey(obj.getName())) {
                        List<String> userRole = new ArrayList<String>();
                        userRole.add(userMap.get(obj.getName()));
                        if (!obj.getUserRoleList().equals(userRole)) {
                            obj.setUserRoleList(userRole);
                            roleFlag = true;
                        }
                    } else {
                        for (String group : groups) {
                            String value = groupMap.get(group);
                            if (value != null) {
                                List<String> userRoleList = new ArrayList<String>();
                                userRoleList.add(value);
                                if (!obj.getUserRoleList().equals(userRoleList)) {
                                    obj.setUserRoleList(userRoleList);
                                    roleFlag = true;
                                }
                            }
                        }

                    }
                    ugInfo.setXuserInfo(obj);
                    ugInfo.setXgroupInfo(getXGroupInfoList(groups));
                }
                if (roleFlag) {
                    try {
                        // If the rest call to ranger admin fails,
                        // propagate the failure to the caller for retry in next
                        // sync cycle.
                        if (addUserGroupInfo(ugInfo) == null) {
                            String msg = "Failed to add user group info";
                            LOG.error(msg);
                            throw new Exception(msg);
                        }
                    } catch (Throwable t) {
                        LOG.error("PolicyMgrUserGroupBuilder.addUserGroupInfo failed with exception: "
                                + t.getMessage()
                                + ", for user-group entry: "
                                + ugInfo);
                    }
                }
            }
			//LOG.info("Adding new groups " + addGroups + " for user = " + userName);
			if (isStartupFlag) {
				modifiedGroupList.addAll(oldGroups);
				LOG.debug("Adding user to modified user list: " + userName + ": " + oldGroups);
				modifiedUserList.add(userName);

			} else {
				if (!addGroups.isEmpty() || !delGroups.isEmpty() || !updateGroups.isEmpty()) {
					modifiedUserList.add(userName);
				}
				modifiedGroupList.addAll(updateGroups);
				modifiedGroupList.addAll(delGroups);
			}
		}
	}

	private void buildGroupList() {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> PolicyMgrUserGroupBuilder.buildGroupList()");
		}
		Client c = getClient();
		int totalCount = 100;
		int retrievedCount = 0;
		while (retrievedCount < totalCount) {
			String response = null;
			Gson gson = new GsonBuilder().create();
			if (isRangerCookieEnabled) {
				response = cookieBasedGetEntity(PM_GROUP_LIST_URI, retrievedCount);
			} else {
				WebResource r = c.resource(getURL(PM_GROUP_LIST_URI)).queryParam("pageSize", recordsToPullPerCall)
						.queryParam("startIndex", String.valueOf(retrievedCount));

				response = r.accept(MediaType.APPLICATION_JSON_TYPE).get(String.class);
			}
			LOG.debug("RESPONSE: [" + response + "]");

			GetXGroupListResponse groupList = gson.fromJson(response, GetXGroupListResponse.class);

			totalCount = groupList.getTotalCount();

			if (groupList.getXgroupInfoList() != null) {
				xgroupList.addAll(groupList.getXgroupInfoList());
				retrievedCount = xgroupList.size();

				for (XGroupInfo g : groupList.getXgroupInfoList()) {
					LOG.debug("GROUP:  Id:" + g.getId() + ", Name: " + g.getName() + ", Description: "
							+ g.getDescription());
				}
			}
		}
		if (LOG.isDebugEnabled()) {
			LOG.debug("<== PolicyMgrUserGroupBuilder.buildGroupList()");
		}
	}

	private void buildUserList() {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> PolicyMgrUserGroupBuilder.buildUserList()");
		}
		Client c = getClient();
		int totalCount = 100;
		int retrievedCount = 0;
		while (retrievedCount < totalCount) {
			String response = null;
			Gson gson = new GsonBuilder().create();
			if (isRangerCookieEnabled) {
				response = cookieBasedGetEntity(PM_USER_LIST_URI, retrievedCount);
			} else {
				WebResource r = c.resource(getURL(PM_USER_LIST_URI)).queryParam("pageSize", recordsToPullPerCall)
						.queryParam("startIndex", String.valueOf(retrievedCount));
				response = r.accept(MediaType.APPLICATION_JSON_TYPE).get(String.class);
			}
			LOG.debug("RESPONSE: [" + response + "]");
			GetXUserListResponse userList = gson.fromJson(response, GetXUserListResponse.class);

			totalCount = userList.getTotalCount();

			if (userList.getXuserInfoList() != null) {
				xuserList.addAll(userList.getXuserInfoList());
				retrievedCount = xuserList.size();

				for (XUserInfo u : userList.getXuserInfoList()) {
					LOG.debug("USER: Id:" + u.getId() + ", Name: " + u.getName() + ", Description: "
							+ u.getDescription());
				}
			}
		}
		if (LOG.isDebugEnabled()) {
			LOG.debug("<== PolicyMgrUserGroupBuilder.buildUserList()");
		}
	}

	private void buildUserGroupLinkList() {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> PolicyMgrUserGroupBuilder.buildUserGroupLinkList()");
		}
		Client c = getClient();
		int totalCount = 100;
		int retrievedCount = 0;

		while (retrievedCount < totalCount) {
			String response = null;
			Gson gson = new GsonBuilder().create();
			if (isRangerCookieEnabled) {
				response = cookieBasedGetEntity(PM_USER_GROUP_MAP_LIST_URI, retrievedCount);
			} else {
				WebResource r = c.resource(getURL(PM_USER_GROUP_MAP_LIST_URI))
						.queryParam("pageSize", recordsToPullPerCall)
						.queryParam("startIndex", String.valueOf(retrievedCount));

				response = r.accept(MediaType.APPLICATION_JSON_TYPE).get(String.class);
			}
			LOG.debug("RESPONSE: [" + response + "]");

			GetXUserGroupListResponse usergroupList = gson.fromJson(response, GetXUserGroupListResponse.class);

			totalCount = usergroupList.getTotalCount();

			if (usergroupList.getXusergroupInfoList() != null) {
				xusergroupList.addAll(usergroupList.getXusergroupInfoList());
				retrievedCount = xusergroupList.size();

				for (XUserGroupInfo ug : usergroupList.getXusergroupInfoList()) {
					LOG.debug("USER_GROUP: UserId:" + ug.getUserId() + ", Name: " + ug.getGroupName());
				}
			}
		}
		if (LOG.isDebugEnabled()) {
			LOG.debug("<== PolicyMgrUserGroupBuilder.buildUserGroupLinkList()");
		}
	}

	private UserGroupInfo addUserGroupInfo(String userName, List<String> groups){
		if(LOG.isDebugEnabled()) {
	 		LOG.debug("==> PolicyMgrUserGroupBuilder.addUserGroupInfo " + userName + " and groups");
	 	}
		UserGroupInfo ret = null;
		XUserInfo user = null;

		LOG.debug("INFO: addPMXAUser(" + userName + ")" );
		if (! isMockRun) {
			user = addXUserInfo(userName);
            if (!groups.isEmpty() && user != null) {
                for (String group : groups) {
                    String value = groupMap.get(group);
                    if (value != null) {
                        List<String> userRoleList = new ArrayList<String>();
                        userRoleList.add(value);
                        if (userMap.containsKey(user.getName())) {
                            List<String> userRole = new ArrayList<String>();
                            userRole.add(userMap.get(user.getName()));
                            user.setUserRoleList(userRole);
                        } else {
                            user.setUserRoleList(userRoleList);
                        }
                    }
                }
            }
            usergroupInfo.setXuserInfo(user);
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

	private UserGroupInfo getUsergroupInfo(UserGroupInfo ret) {
		if(LOG.isDebugEnabled()){
			LOG.debug("==> PolicyMgrUserGroupBuilder.getUsergroupInfo(UserGroupInfo ret)");
		}
		String response = null;
		Gson gson = new GsonBuilder().create();
		String jsonString = gson.toJson(usergroupInfo);
		if (LOG.isDebugEnabled()) {
			LOG.debug("USER GROUP MAPPING" + jsonString);
		}
		if(isRangerCookieEnabled){
			response = cookieBasedUploadEntity(jsonString,PM_ADD_USER_GROUP_INFO_URI);
		}
		else{
			Client c = getClient();
			WebResource r = c.resource(getURL(PM_ADD_USER_GROUP_INFO_URI));
			try{
				response = r.accept(MediaType.APPLICATION_JSON_TYPE).type(MediaType.APPLICATION_JSON_TYPE).post(String.class, jsonString);
			}
			catch(Throwable t){
				LOG.error("Failed to communicate Ranger Admin : ", t);
			}
		}
		if ( LOG.isDebugEnabled() ) {
			LOG.debug("RESPONSE: [" + response + "]");
		}
		ret = gson.fromJson(response, UserGroupInfo.class);

		if ( ret != null) {

			XUserInfo xUserInfo = ret.getXuserInfo();
			addUserToList(xUserInfo);

			for(XGroupInfo xGroupInfo : ret.getXgroupInfo()) {
				addGroupToList(xGroupInfo);
				addUserGroupInfoToList(xUserInfo,xGroupInfo);
			}
		}

		if(LOG.isDebugEnabled()){
			LOG.debug("<== PolicyMgrUserGroupBuilder.getUsergroupInfo (UserGroupInfo ret)");
		}
		return ret;
	}

	private UserGroupInfo getUserGroupInfo(UserGroupInfo usergroupInfo) {
		UserGroupInfo ret = null;
		if(LOG.isDebugEnabled()){
			LOG.debug("==> PolicyMgrUserGroupBuilder.getUsergroupInfo(UserGroupInfo ret, UserGroupInfo usergroupInfo)");
		}
		String response = null;
		Gson gson = new GsonBuilder().create();
		String jsonString = gson.toJson(usergroupInfo);
		if (LOG.isDebugEnabled()) {
			LOG.debug("USER GROUP MAPPING" + jsonString);
		}
		if(isRangerCookieEnabled){
			response = cookieBasedUploadEntity(jsonString,PM_ADD_USER_GROUP_INFO_URI);
		}
		else{
			Client c = getClient();
			WebResource r = c.resource(getURL(PM_ADD_USER_GROUP_INFO_URI));
			try{
				response=r.accept(MediaType.APPLICATION_JSON_TYPE).type(MediaType.APPLICATION_JSON_TYPE).post(String.class, jsonString);
			}catch(Throwable t){
				LOG.error("Failed to communicate Ranger Admin : ", t);
			}
		}
		if (LOG.isDebugEnabled()) {
			LOG.debug("RESPONSE: [" + response + "]");
		}
		ret = gson.fromJson(response, UserGroupInfo.class);
		if ( ret != null) {

			XUserInfo xUserInfo = ret.getXuserInfo();
			addUserToList(xUserInfo);

            for (XGroupInfo xGroupInfo : ret.getXgroupInfo()) {
                addGroupToList(xGroupInfo);
                addUserGroupInfoToList(xUserInfo, xGroupInfo);
            }
		}
		if(LOG.isDebugEnabled()){
			LOG.debug("<== PolicyMgrUserGroupBuilder.getUsergroupInfo(UserGroupInfo ret, UserGroupInfo usergroupInfo)");
		}
		return ret;
	}


	private String tryUploadEntityWithCookie(String jsonString, String apiURL) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> PolicyMgrUserGroupBuilder.tryUploadEntityWithCookie()");
		}
		String response = null;
		ClientResponse clientResp = null;
		WebResource webResource = createWebResourceForCookieAuth(apiURL);
		WebResource.Builder br = webResource.getRequestBuilder().cookie(sessionId);
		try{
			clientResp=br.accept(MediaType.APPLICATION_JSON_TYPE).type(MediaType.APPLICATION_JSON_TYPE).post(ClientResponse.class, jsonString);
		}
		catch(Throwable t){
			LOG.error("Failed to communicate Ranger Admin : ", t);
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
				cookieList = clientResp.getCookies();
				for (NewCookie cookie : cookieList) {
					if (cookie.getName().equalsIgnoreCase(RANGER_ADMIN_COOKIE_NAME)) {
						sessionId = cookie.toCookie();
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
			LOG.debug("<== PolicyMgrUserGroupBuilder.tryUploadEntityWithCookie()");
		}
		return response;
	}


	private String tryUploadEntityWithCred(String jsonString,String apiURL){
		if(LOG.isDebugEnabled()){
			LOG.debug("==> PolicyMgrUserGroupBuilder.tryUploadEntityInfoWithCred()");
		}
		String response = null;
		ClientResponse clientResp = null;
		Client c = getClient();
		WebResource r = c.resource(getURL(apiURL));
		if ( LOG.isDebugEnabled() ) {
		   LOG.debug("USER GROUP MAPPING" + jsonString);
		}
		try{
			clientResp=r.accept(MediaType.APPLICATION_JSON_TYPE).type(MediaType.APPLICATION_JSON_TYPE).post(ClientResponse.class, jsonString);
		}
		catch(Throwable t){
			LOG.error("Failed to communicate Ranger Admin : ", t);
		}
		if (clientResp != null) {
			if (!(clientResp.toString().contains(apiURL))) {
				clientResp.setStatus(HttpServletResponse.SC_NOT_FOUND);
			} else if (clientResp.getStatus() == HttpServletResponse.SC_UNAUTHORIZED) {
				LOG.warn("Credentials response from ranger is 401.");
			} else if (clientResp.getStatus() == HttpServletResponse.SC_OK || clientResp.getStatus() == HttpServletResponse.SC_NO_CONTENT) {
				cookieList = clientResp.getCookies();
				for (NewCookie cookie : cookieList) {
					if (cookie.getName().equalsIgnoreCase(RANGER_ADMIN_COOKIE_NAME)) {
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
			LOG.debug("<== PolicyMgrUserGroupBuilder.tryUploadEntityInfoWithCred()");
		}
		return response;
	}


	private UserGroupInfo addUserGroupInfo(UserGroupInfo usergroupInfo){
		if(LOG.isDebugEnabled()) {
	 		LOG.debug("==> PolicyMgrUserGroupBuilder.addUserGroupInfo");
	 	}
		UserGroupInfo ret = null;
		if (authenticationType != null && AUTH_KERBEROS.equalsIgnoreCase(authenticationType) && SecureClientLogin.isKerberosCredentialExists(principal, keytab)) {
			try {
				Subject sub = SecureClientLogin.loginUserFromKeytab(principal, keytab, nameRules);
				final UserGroupInfo ugInfo = usergroupInfo;
				ret = Subject.doAs(sub, new PrivilegedAction<UserGroupInfo>() {
					@Override
					public UserGroupInfo run() {
						try {
							return getUserGroupInfo(ugInfo);
						} catch (Exception e) {
							LOG.error("Failed to add User Group Info : ", e);
						}
						return null;
					}
				});
				return ret;
			} catch (Exception e) {
				LOG.error("Failed to Authenticate Using given Principal and Keytab : ",e);
			}
		} else {
			try {
				ret = getUserGroupInfo(usergroupInfo);
			} catch (Throwable t) {
				LOG.error("Failed to add User Group Info : ", t);
			}
		}
		if (LOG.isDebugEnabled()) {
			LOG.debug("<== PolicyMgrUserGroupBuilder.addUserGroupInfo");
		}
		return ret;
	}

	private XUserInfo addXUserInfo(String aUserName) {

		XUserInfo xuserInfo = new XUserInfo();

		xuserInfo.setName(aUserName);

		xuserInfo.setDescription(aUserName + " - add from Unix box");

		usergroupInfo.setXuserInfo(xuserInfo);

		return xuserInfo;
	}


	private XGroupInfo addXGroupInfo(String aGroupName) {

		XGroupInfo addGroup = new XGroupInfo();

		addGroup.setName(aGroupName);

		addGroup.setDescription(aGroupName + " - add from Unix box");

		addGroup.setGroupType("1");

		addGroup.setGroupSource(GROUP_SOURCE_EXTERNAL);

		return addGroup;
	}


	private void addXUserGroupInfo(XUserInfo aUserInfo, List<String> aGroupList) {

		List<XGroupInfo> xGroupInfoList = new ArrayList<XGroupInfo>();

		for(String groupName : aGroupList) {
			XGroupInfo group = groupName2XGroupInfoMap.get(groupName);
			if (group == null) {
				group = addXGroupInfo(groupName);
			}
			xGroupInfoList.add(group);
			addXUserGroupInfo(aUserInfo, group);
		}

		usergroupInfo.setXgroupInfo(xGroupInfoList);
	}

	private List<XGroupInfo> getXGroupInfoList(List<String> aGroupList) {

		List<XGroupInfo> xGroupInfoList = new ArrayList<XGroupInfo>();
		for(String groupName : aGroupList) {
			XGroupInfo group = groupName2XGroupInfoMap.get(groupName);
			if (group == null) {
				group = addXGroupInfo(groupName);
			}else if(!GROUP_SOURCE_EXTERNAL.equals(group.getGroupSource())){
				group.setGroupSource(GROUP_SOURCE_EXTERNAL);
			}
			xGroupInfoList.add(group);
		}
		return xGroupInfoList;
	}



   private XUserGroupInfo addXUserGroupInfo(XUserInfo aUserInfo, XGroupInfo aGroupInfo) {


	    XUserGroupInfo ugInfo = new XUserGroupInfo();

		ugInfo.setUserId(aUserInfo.getId());

		ugInfo.setGroupName(aGroupInfo.getName());

		// ugInfo.setParentGroupId("1");

        return ugInfo;
	}


	private void delXUserGroupInfo(final XUserInfo aUserInfo, List<String> aGroupList) {
		for(String groupName : aGroupList) {
			final XGroupInfo group = groupName2XGroupInfoMap.get(groupName);
			if (group != null) {
				if (authenticationType != null && AUTH_KERBEROS.equalsIgnoreCase(authenticationType) && SecureClientLogin.isKerberosCredentialExists(principal, keytab)) {
					try {
						LOG.debug("Using principal = " + principal + " and keytab = " + keytab);
						Subject sub = SecureClientLogin.loginUserFromKeytab(principal, keytab, nameRules);
						Subject.doAs(sub, new PrivilegedAction<Void>() {
							@Override
							public Void run() {
								try {
									delXUserGroupInfo(aUserInfo, group);
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
					delXUserGroupInfo(aUserInfo, group);
				}
			}
		}
	}

	private void delXUserGroupInfo(XUserInfo aUserInfo, XGroupInfo aGroupInfo) {

		if (LOG.isDebugEnabled()) {
			LOG.debug("==> PolicyMgrUserGroupBuilder.delXUserGroupInfo()");
		}

		String groupName = aGroupInfo.getName();

		String userName  = aUserInfo.getName();

		try {
			ClientResponse response = null;
			String uri = PM_DEL_USER_GROUP_LINK_URI.replaceAll(Pattern.quote("${groupName}"),
					   UserSyncUtil.encodeURIParam(groupName)).replaceAll(Pattern.quote("${userName}"), UserSyncUtil.encodeURIParam(userName));
			if (isRangerCookieEnabled) {
				if (sessionId != null && isValidRangerCookie) {
					WebResource webResource = createWebResourceForCookieAuth(uri);
					WebResource.Builder br = webResource.getRequestBuilder().cookie(sessionId);
					response = br.delete(ClientResponse.class);
					if (response != null) {
						if (!(response.toString().contains(uri))) {
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
								if (cookie.getName().equalsIgnoreCase(RANGER_ADMIN_COOKIE_NAME)) {
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
				} else {
					Client c = getClient();
					WebResource r = c.resource(getURL(uri));
					response = r.delete(ClientResponse.class);
					if (response != null) {
						if (!(response.toString().contains(uri))) {
							response.setStatus(HttpServletResponse.SC_NOT_FOUND);
						} else if (response.getStatus() == HttpServletResponse.SC_UNAUTHORIZED) {
							LOG.warn("Credentials response from ranger is 401.");
						} else if (response.getStatus() == HttpServletResponse.SC_OK
								|| response.getStatus() == HttpServletResponse.SC_NO_CONTENT) {
							cookieList = response.getCookies();
							for (NewCookie cookie : cookieList) {
								if (cookie.getName().equalsIgnoreCase(RANGER_ADMIN_COOKIE_NAME)) {
									sessionId = cookie.toCookie();
									isValidRangerCookie = true;
									LOG.info("valid cookie saved ");
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
			} else {
			Client c = getClient();
			WebResource r = c.resource(getURL(uri));

		    response = r.delete(ClientResponse.class);
			}
		    if ( LOG.isDebugEnabled() ) {
		    	LOG.debug("RESPONSE: [" + response.toString() + "]");
		    }

		    if (response.getStatus() == 200) {
		    	delUserGroupFromList(aUserInfo, aGroupInfo);
		    }

		} catch (Exception e) {

			LOG.warn( "ERROR: Unable to delete GROUP: " + groupName  + " from USER:" + userName , e);
		}
		if (LOG.isDebugEnabled()) {
			LOG.debug("<== PolicyMgrUserGroupBuilder.delXUserGroupInfo()");
		}

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
		if (authenticationType != null && AUTH_KERBEROS.equalsIgnoreCase(authenticationType) && SecureClientLogin.isKerberosCredentialExists(principal, keytab)) {
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
			LOG.debug("==> PolicyMgrUserGroupBuilder.getMUser()");
		}
		String response = null;
		Gson gson = new GsonBuilder().create();
		String jsonString = gson.toJson(userInfo);
		if (isRangerCookieEnabled) {
			response = cookieBasedUploadEntity(jsonString, PM_ADD_LOGIN_USER_URI);
		} else {
			Client c = getClient();
			WebResource r = c.resource(getURL(PM_ADD_LOGIN_USER_URI));
			response = r.accept(MediaType.APPLICATION_JSON_TYPE).type(MediaType.APPLICATION_JSON_TYPE)
					.post(String.class, jsonString);
		}
		if (LOG.isDebugEnabled()) {
			LOG.debug("RESPONSE[" + response + "]");
		}
		ret = gson.fromJson(response, MUserInfo.class);
		if (LOG.isDebugEnabled()) {
			LOG.debug("MUser Creation successful " + ret);
			LOG.debug("<== PolicyMgrUserGroupBuilder.getMUser()");
		}
		return ret;
	}

	private String cookieBasedUploadEntity(String jsonString, String apiURL ) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> PolicyMgrUserGroupBuilder.cookieBasedUploadEntity()");
		}
		String response = null;
		if (sessionId != null && isValidRangerCookie) {
			response = tryUploadEntityWithCookie(jsonString,apiURL);
		}
		else{
			response = tryUploadEntityWithCred(jsonString,apiURL);
		}
		if (LOG.isDebugEnabled()) {
			LOG.debug("<== PolicyMgrUserGroupBuilder.cookieBasedUploadEntity()");
		}
		return response;
	}

	private String cookieBasedGetEntity(String apiURL ,int retrievedCount) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> PolicyMgrUserGroupBuilder.cookieBasedGetEntity()");
		}
		String response = null;
		if (sessionId != null && isValidRangerCookie) {
			response = tryGetEntityWithCookie(apiURL,retrievedCount);
		}
		else{
			response = tryGetEntityWithCred(apiURL,retrievedCount);
		}
		if (LOG.isDebugEnabled()) {
			LOG.debug("<== PolicyMgrUserGroupBuilder.cookieBasedGetEntity()");
		}
		return response;
	}

	private String tryGetEntityWithCred(String apiURL, int retrievedCount) {
		if(LOG.isDebugEnabled()){
			LOG.debug("==> PolicyMgrUserGroupBuilder.tryGetEntityWithCred()");
		}
		String response = null;
		ClientResponse clientResp = null;
		Client c = getClient();
		WebResource r = c.resource(getURL(apiURL))
				.queryParam("pageSize", recordsToPullPerCall)
				.queryParam("startIndex", String.valueOf(retrievedCount));

		try{
			clientResp=r.accept(MediaType.APPLICATION_JSON_TYPE).get(ClientResponse.class);
		}
		catch(Throwable t){
			LOG.error("Failed to communicate Ranger Admin : ", t);
		}
		if (clientResp != null) {
			if (!(clientResp.toString().contains(apiURL))) {
				clientResp.setStatus(HttpServletResponse.SC_NOT_FOUND);
			} else if (clientResp.getStatus() == HttpServletResponse.SC_UNAUTHORIZED) {
				LOG.warn("Credentials response from ranger is 401.");
			} else if (clientResp.getStatus() == HttpServletResponse.SC_OK || clientResp.getStatus() == HttpServletResponse.SC_NO_CONTENT) {
				cookieList = clientResp.getCookies();
				for (NewCookie cookie : cookieList) {
					if (cookie.getName().equalsIgnoreCase(RANGER_ADMIN_COOKIE_NAME)) {
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
			LOG.debug("<== PolicyMgrUserGroupBuilder.tryGetEntityWithCred()");
		}
		return response;
	}


	private String tryGetEntityWithCookie(String apiURL, int retrievedCount) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> PolicyMgrUserGroupBuilder.tryGetEntityWithCookie()");
		}
		String response = null;
		ClientResponse clientResp = null;
		WebResource webResource = createWebResourceForCookieAuth(apiURL).queryParam("pageSize", recordsToPullPerCall).queryParam("startIndex", String.valueOf(retrievedCount));
		WebResource.Builder br = webResource.getRequestBuilder().cookie(sessionId);
		try{
			clientResp=br.accept(MediaType.APPLICATION_JSON_TYPE).get(ClientResponse.class);
		}
		catch(Throwable t){
			LOG.error("Failed to communicate Ranger Admin : ", t);
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
				cookieList = clientResp.getCookies();
				for (NewCookie cookie : cookieList) {
					if (cookie.getName().equalsIgnoreCase(RANGER_ADMIN_COOKIE_NAME)) {
						sessionId = cookie.toCookie();
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
			LOG.debug("<== PolicyMgrUserGroupBuilder.tryGetEntityWithCookie()");
		}
		return response;
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

	private WebResource createWebResourceForCookieAuth(String url) {
		Client cookieClient = getClient();
		cookieClient.removeAllFilters();
		WebResource ret = cookieClient.resource(getURL(url));
		return ret;
	}

	private InputStream getFileInputStream(String path) throws FileNotFoundException {

		InputStream ret = null;

		File f = new File(path);

		if (f.exists()) {
			ret = new FileInputStream(f);
		} else {
			ret = PolicyMgrUserGroupBuilder.class.getResourceAsStream(path);

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


	@Override
	public void addOrUpdateGroup(String groupName) throws Throwable{
		XGroupInfo group = groupName2XGroupInfoMap.get(groupName);

		if (group == null) {    // Does not exists

			//* Build the group info object and do the rest call
 			if ( ! isMockRun ) {
				group = addGroupInfo(groupName);
 				if ( group != null) {
 					addGroupToList(group);
 				} else {
 					String msg = "Failed to add addorUpdate group info";
 					LOG.error(msg);
 					throw new Exception(msg);
 				}
 			}
		}
	}

	private XGroupInfo addGroupInfo(final String groupName){
		XGroupInfo ret = null;
		XGroupInfo group = null;

		LOG.debug("INFO: addPMXAGroup(" + groupName + ")" );
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

	private XGroupInfo getAddedGroupInfo(XGroupInfo group){
		XGroupInfo ret = null;
		String response = null;
		Gson gson = new GsonBuilder().create();
		String jsonString = gson.toJson(group);
		if(isRangerCookieEnabled){
			response = cookieBasedUploadEntity(jsonString,PM_ADD_GROUP_URI);
		}
		else{
			Client c = getClient();
			WebResource r = c.resource(getURL(PM_ADD_GROUP_URI));
			if (LOG.isDebugEnabled()) {
				LOG.debug("Group" + jsonString);
			}
			try{
				response = r.accept(MediaType.APPLICATION_JSON_TYPE).type(MediaType.APPLICATION_JSON_TYPE).post(String.class, jsonString);
			}
			catch(Throwable t){
				LOG.error("Failed to communicate Ranger Admin : ", t);
			}
		}

		if ( LOG.isDebugEnabled() ) {
			LOG.debug("RESPONSE: [" + response + "]");
		}

		ret = gson.fromJson(response, XGroupInfo.class);

		return ret;
	}


	@Override
	public void addOrUpdateUser(String user) throws Throwable {
		// TODO Auto-generated method stub

	}


	@Override
	public void addOrUpdateGroup(String groupName, List<String> users) throws Throwable {
		if (users == null || users.isEmpty()) {
			if (groupName2XGroupInfoMap.containsKey(groupName)) {
				modifiedGroupList.add(groupName);
			} else {
				newGroupList.add(groupName);
			}
		}
		addOrUpdateGroup(groupName);

	}

	
	@Override
	public void postUserGroupAuditInfo(UgsyncAuditInfo ugsyncAuditInfo) throws Throwable {
		if (! isMockRun) {
			addUserGroupAuditInfo(ugsyncAuditInfo);
		}
		noOfNewUsers = 0;
		noOfNewGroups = 0;
		noOfModifiedUsers = 0;
		noOfModifiedGroups = 0;
		isStartupFlag = false;
		newUserList.clear();
		modifiedUserList.clear();
		newGroupList.clear();
		modifiedGroupList.clear();
	}

	private UgsyncAuditInfo addUserGroupAuditInfo(UgsyncAuditInfo auditInfo) {
		UgsyncAuditInfo ret = null;

		if (auditInfo == null) {
			LOG.error("Failed to generate user group audit info");
			return ret;
		}
		noOfNewUsers = newUserList.size();
		noOfModifiedUsers = modifiedUserList.size();
		noOfNewGroups = newGroupList.size();
		noOfModifiedGroups = modifiedGroupList.size();

		auditInfo.setNoOfNewUsers(Integer.toUnsignedLong(noOfNewUsers));
		auditInfo.setNoOfNewGroups(Integer.toUnsignedLong(noOfNewGroups));
		auditInfo.setNoOfModifiedUsers(Integer.toUnsignedLong(noOfModifiedUsers));
		auditInfo.setNoOfModifiedGroups(Integer.toUnsignedLong(noOfModifiedGroups));
		auditInfo.setSessionId("");
		LOG.debug("INFO: addAuditInfo(" + auditInfo.getNoOfNewUsers() + ", " + auditInfo.getNoOfNewGroups()
				+ ", " + auditInfo.getNoOfModifiedUsers() + ", " + auditInfo.getNoOfModifiedGroups()
				+ ", " + auditInfo.getSyncSource() + ")");

		if (authenticationType != null
				&& AUTH_KERBEROS.equalsIgnoreCase(authenticationType)
				&& SecureClientLogin.isKerberosCredentialExists(principal,
				keytab)) {
			try {
				Subject sub = SecureClientLogin.loginUserFromKeytab(principal, keytab, nameRules);
				final UgsyncAuditInfo auditInfoFinal = auditInfo;
				ret = Subject.doAs(sub, new PrivilegedAction<UgsyncAuditInfo>() {
					@Override
					public UgsyncAuditInfo run() {
						try {
							return getUserGroupAuditInfo(auditInfoFinal);
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
			return ret;
		} else {
			return getUserGroupAuditInfo(auditInfo);
		}
	}


	private UgsyncAuditInfo getUserGroupAuditInfo(UgsyncAuditInfo userInfo) {
		if(LOG.isDebugEnabled()){
			LOG.debug("==> PolicyMgrUserGroupBuilder.getUserGroupAuditInfo()");
		}

		String response = null;

		Gson gson = new GsonBuilder().create();
		String jsonString = gson.toJson(userInfo);
		if(isRangerCookieEnabled){
			response = cookieBasedUploadEntity(jsonString, PM_AUDIT_INFO_URI);
		}
		else{
			Client c = getClient();
			WebResource r = c.resource(getURL(PM_AUDIT_INFO_URI));
			try{
				response = r.accept(MediaType.APPLICATION_JSON_TYPE).type(MediaType.APPLICATION_JSON_TYPE).post(String.class, jsonString);
			}
			catch(Throwable t){
				LOG.error("Failed to communicate Ranger Admin : ", t);
			}
		}
		if (LOG.isDebugEnabled()) {
			LOG.debug("RESPONSE[" + response + "]");
		}
		UgsyncAuditInfo ret = gson.fromJson(response, UgsyncAuditInfo.class);

		LOG.debug("AuditInfo Creation successful ");

		if(LOG.isDebugEnabled()){
			LOG.debug("<== PolicyMgrUserGroupBuilder.getUserGroupAuditInfo()");
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
