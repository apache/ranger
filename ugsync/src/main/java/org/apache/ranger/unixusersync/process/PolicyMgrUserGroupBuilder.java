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
import java.util.List;
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

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import com.sun.jersey.api.client.filter.HTTPBasicAuthFilter;
import com.sun.jersey.client.urlconnection.HTTPSProperties;

import org.apache.ranger.unixusersync.config.UserGroupSyncConfig;
import org.apache.ranger.unixusersync.model.GetXGroupListResponse;
import org.apache.ranger.unixusersync.model.GetXUserGroupListResponse;
import org.apache.ranger.unixusersync.model.GetXUserListResponse;
import org.apache.ranger.unixusersync.model.MUserInfo;
import org.apache.ranger.unixusersync.model.XGroupInfo;
import org.apache.ranger.unixusersync.model.XUserGroupInfo;
import org.apache.ranger.unixusersync.model.XUserInfo;
import org.apache.ranger.unixusersync.model.UserGroupInfo;
import org.apache.ranger.usergroupsync.UserGroupSink;
import org.apache.ranger.usersync.util.UserSyncUtil;

public class PolicyMgrUserGroupBuilder implements UserGroupSink {
	
	private static final Logger LOG = Logger.getLogger(PolicyMgrUserGroupBuilder.class) ;
	
	private static final String AUTHENTICATION_TYPE = "hadoop.security.authentication";	
	private String AUTH_KERBEROS = "kerberos";
	private static final String PRINCIPAL = "ranger.usersync.kerberos.principal";
	private static final String KEYTAB = "ranger.usersync.kerberos.keytab";
	private static final String NAME_RULE = "hadoop.security.auth_to_local";
	
	public static final String PM_USER_LIST_URI  = "/service/xusers/users/" ;				// GET
	private static final String PM_ADD_USER_URI  = "/service/xusers/users/" ;				// POST
	private static final String PM_ADD_USER_GROUP_INFO_URI = "/service/xusers/users/userinfo" ;	// POST
	
	public static final String PM_GROUP_LIST_URI = "/service/xusers/groups/" ;				// GET
	private static final String PM_ADD_GROUP_URI = "/service/xusers/groups/" ;				// POST
	
	
	public static final String PM_USER_GROUP_MAP_LIST_URI = "/service/xusers/groupusers/" ;		// GET
	private static final String PM_ADD_USER_GROUP_LINK_URI = "/service/xusers/groupusers/" ;	// POST
	
	private static final String PM_DEL_USER_GROUP_LINK_URI = "/service/xusers/group/${groupName}/user/${userName}" ; // DELETE
	
	private static final String PM_ADD_LOGIN_USER_URI = "/service/users/default" ;			// POST
	private static final String GROUP_SOURCE_EXTERNAL ="1";
	
	private static String LOCAL_HOSTNAME = "unknown" ;
	private String recordsToPullPerCall = "1000" ;
	private boolean isMockRun = false ;
	private String policyMgrBaseUrl ;
	
	private UserGroupSyncConfig  config = UserGroupSyncConfig.getInstance() ;

	private UserGroupInfo				usergroupInfo = new UserGroupInfo();
	private List<XGroupInfo> 			xgroupList = new ArrayList<XGroupInfo>() ;
	private List<XUserInfo> 			xuserList = new ArrayList<XUserInfo>() ;
	private List<XUserGroupInfo> 		xusergroupList = new ArrayList<XUserGroupInfo>() ;
	private HashMap<String,XUserInfo>  	userId2XUserInfoMap = new HashMap<String,XUserInfo>() ;
	private HashMap<String,XUserInfo>  	userName2XUserInfoMap = new HashMap<String,XUserInfo>() ;
	private HashMap<String,XGroupInfo>  groupName2XGroupInfoMap = new HashMap<String,XGroupInfo>() ;
	
	private String keyStoreFile =  null ;
	private String keyStoreFilepwd = null; 
	private String trustStoreFile = null ;
	private String trustStoreFilepwd = null ;
	private String keyStoreType = null ;
	private String trustStoreType = null ;
	private HostnameVerifier hv =  null ;

	private SSLContext sslContext = null ;
	private String authenticationType = null;
	String principal;
	String keytab;
	String nameRules;
	
	static {
		try {
			LOCAL_HOSTNAME = java.net.InetAddress.getLocalHost().getCanonicalHostName();
		} catch (UnknownHostException e) {
			LOCAL_HOSTNAME = "unknown" ;
		} 
	}
	
	
	public static void main(String[] args) throws Throwable {
		PolicyMgrUserGroupBuilder  ugbuilder = new PolicyMgrUserGroupBuilder() ;
		ugbuilder.init() ;
//		ugbuilder.print();
//		ugbuilder.addMUser("testuser") ;
//		ugbuilder.addXUserInfo("testuser") ;
//		ugbuilder.addXGroupInfo("testgroup") ;
// 		XUserInfo u = ugbuilder.addXUserInfo("testuser") ;
//		XGroupInfo g = ugbuilder.addXGroupInfo("testgroup") ;
//		 ugbuilder.addXUserGroupInfo(u, g) ;
		
	}

	
	synchronized public void init() throws Throwable {
		recordsToPullPerCall = config.getMaxRecordsPerAPICall() ;
		policyMgrBaseUrl = config.getPolicyManagerBaseURL() ;
		isMockRun = config.isMockRunEnabled() ;
		
		if (isMockRun) {
			LOG.setLevel(Level.DEBUG) ;
		}
		
		keyStoreFile =  config.getSSLKeyStorePath() ;
		keyStoreFilepwd = config.getSSLKeyStorePathPassword() ; 
		trustStoreFile = config.getSSLTrustStorePath() ;
		trustStoreFilepwd = config.getSSLTrustStorePathPassword() ;
		keyStoreType = KeyStore.getDefaultType() ;
		trustStoreType = KeyStore.getDefaultType() ;
		authenticationType = config.getProperty(AUTHENTICATION_TYPE,"simple");
		try {
			principal = SecureClientLogin.getPrincipal(config.getProperty(PRINCIPAL,""), LOCAL_HOSTNAME);
		} catch (IOException ignored) {
			 // do nothing
		}
		keytab = config.getProperty(KEYTAB,"");
		nameRules = config.getProperty(NAME_RULE,"DEFAULT");
		buildUserGroupInfo() ;
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
							buildUserGroupLinkList() ;
							rebuildUserGroupMap() ;
							if (LOG.isDebugEnabled()) {
							//	this.print(); 
							}
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
			buildUserGroupLinkList() ;
			rebuildUserGroupMap() ;
			if (LOG.isDebugEnabled()) {
				this.print(); 
			}
		}	
	}
	
	private String getURL(String uri) {
		String ret = null ;
		ret = policyMgrBaseUrl + (uri.startsWith("/") ? uri : ("/" + uri)) ;
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
			xuserList.add(aUserInfo) ;
		}
		
		String userId = aUserInfo.getId() ;
		
		if (userId != null) {
			userId2XUserInfoMap.put(userId, aUserInfo) ;
		}
		
		String userName = aUserInfo.getName();
		
		if (userName != null) {
			userName2XUserInfoMap.put(userName, aUserInfo) ;
		}
	}
	

	private void addGroupToList(XGroupInfo aGroupInfo) {
		
		if (! xgroupList.contains(aGroupInfo) ) {
			xgroupList.add(aGroupInfo) ;
		}

		if (aGroupInfo.getName() != null) {
			groupName2XGroupInfoMap.put(aGroupInfo.getName(), aGroupInfo) ;
		}

	}
	
	private void addUserGroupToList(XUserGroupInfo ugInfo) {
		String userId = ugInfo.getUserId() ;
		
		if (userId != null) {
			XUserInfo user = userId2XUserInfoMap.get(userId) ;
			
			if (user != null) {
				List<String> groups = user.getGroups() ;
				if (! groups.contains(ugInfo.getGroupName())) {
					groups.add(ugInfo.getGroupName()) ;
				}
			}
		}
	}
	
	private void addUserGroupInfoToList(XUserInfo userInfo, XGroupInfo groupInfo) {
		String userId = userInfo.getId();
		
		if (userId != null) {
			XUserInfo user = userId2XUserInfoMap.get(userId) ;
			
			if (user != null) {
				List<String> groups = user.getGroups() ;
				if (! groups.contains(groupInfo.getName())) {
					groups.add(groupInfo.getName()) ;
				}
			}
		}
	}

	private void delUserGroupFromList(XUserInfo userInfo, XGroupInfo groupInfo) {
		List<String> groups = userInfo.getGroups() ;
		if (groups.contains(groupInfo.getName())) {
			groups.remove(groupInfo.getName()) ;
		}
	}
	
	private void print() {
		LOG.debug("Number of users read [" + xuserList.size() + "]");
		for(XUserInfo user : xuserList) {
			LOG.debug("USER: " + user.getName()) ;
			for(String group : user.getGroups()) {
				LOG.debug("\tGROUP: " + group) ;
			}
		}
	}

	@Override
	public void addOrUpdateUser(String userName, List<String> groups) {
		
		UserGroupInfo ugInfo		  = new UserGroupInfo();
		XUserInfo user = userName2XUserInfoMap.get(userName) ;
		
		if (groups == null) {
			groups = new ArrayList<String>() ;
		}
		
		if (user == null) {    // Does not exists

			LOG.debug("INFO: addPMAccount(" + userName + ")" ) ;
			if (! isMockRun) {
				addMUser(userName) ;
			}
			
			//* Build the user group info object and do the rest call
 			if ( ! isMockRun ) {
 				addUserGroupInfo(userName,groups);
 			}

		}
		else {					// Validate group memberships
			List<String> oldGroups = user.getGroups() ;
			List<String> addGroups = new ArrayList<String>() ;
			List<String> delGroups = new ArrayList<String>() ;
			List<String> updateGroups = new ArrayList<String>() ;
			XGroupInfo tempXGroupInfo=null;
			for(String group : groups) {
				if (! oldGroups.contains(group)) {
					addGroups.add(group) ;
				}else{
					tempXGroupInfo=groupName2XGroupInfoMap.get(group);
					if(tempXGroupInfo!=null && ! GROUP_SOURCE_EXTERNAL.equals(tempXGroupInfo.getGroupSource())){
						updateGroups.add(group);
					}
				}
			}
			
			for(String group : oldGroups) {
				if (! groups.contains(group) ) {
					delGroups.add(group) ;
				}
			}

 			for(String g : addGroups) {
 				LOG.debug("INFO: addPMXAGroupToUser(" + userName + "," + g + ")" ) ;
 			}
 			if (! isMockRun) {
 				if (!addGroups.isEmpty()){
 					ugInfo.setXuserInfo(addXUserInfo(userName));
 				    ugInfo.setXgroupInfo(getXGroupInfoList(addGroups));
					try{
						addUserGroupInfo(ugInfo);
					}catch(Throwable t){
						LOG.error("PolicyMgrUserGroupBuilder.addUserGroupInfo failed with exception: " + t.getMessage()
						+ ", for user-group entry: " + ugInfo);
					}
 				}
 				addXUserGroupInfo(user, addGroups) ;
 			}
 			
 			for(String g : delGroups) {
 				LOG.debug("INFO: delPMXAGroupFromUser(" + userName + "," + g + ")" ) ;
 			}
 			
 			if (! isMockRun ) {
 				delXUserGroupInfo(user, delGroups) ;
 			}
			if (! isMockRun) {
				if (!updateGroups.isEmpty()){
					ugInfo.setXuserInfo(addXUserInfo(userName));
					ugInfo.setXgroupInfo(getXGroupInfoList(updateGroups));
					try{
						addUserGroupInfo(ugInfo);
					}catch(Throwable t){
						LOG.error("PolicyMgrUserGroupBuilder.addUserGroupInfo failed with exception: " + t.getMessage()
						+ ", for user-group entry: " + ugInfo);
					}
				}
			}
		}
	}
	
	private void buildGroupList() {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> PolicyMgrUserGroupBuilder.buildGroupList");
		}		
		Client c = getClient() ;
		
		int totalCount = 100 ;
		int retrievedCount = 0 ;
		 	    
		while (retrievedCount < totalCount) {
			WebResource r = c.resource(getURL(PM_GROUP_LIST_URI))
					.queryParam("pageSize", recordsToPullPerCall)
					.queryParam("startIndex", String.valueOf(retrievedCount)) ;
			
		String response = r.accept(MediaType.APPLICATION_JSON_TYPE).get(String.class);
		    
		LOG.debug("RESPONSE: [" + response + "]") ;
		    		    
		Gson gson = new GsonBuilder().create() ;

		GetXGroupListResponse groupList = gson.fromJson(response, GetXGroupListResponse.class) ;
				    
		totalCount = groupList.getTotalCount() ;
		
			if (groupList.getXgroupInfoList() != null) {
				xgroupList.addAll(groupList.getXgroupInfoList());
				retrievedCount = xgroupList.size();

				for (XGroupInfo g : groupList.getXgroupInfoList()) {
					LOG.debug("GROUP:  Id:" + g.getId() + ", Name: "+ g.getName() + ", Description: "+ g.getDescription());
				}
			}
		}
	}
	
	private void buildUserList() {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> PolicyMgrUserGroupBuilder.buildUserList");
		}
		Client c = getClient() ;	
	    
	    int totalCount = 100 ;
	    int retrievedCount = 0 ;
	    
	    while (retrievedCount < totalCount) {
		    
		    WebResource r = c.resource(getURL(PM_USER_LIST_URI))
		    					.queryParam("pageSize", recordsToPullPerCall)
		    					.queryParam("startIndex", String.valueOf(retrievedCount)) ;
		    
		    String response = r.accept(MediaType.APPLICATION_JSON_TYPE).get(String.class);
		    
		    Gson gson = new GsonBuilder().create() ;
	
		    LOG.debug("RESPONSE: [" + response + "]") ;
	
		    GetXUserListResponse userList = gson.fromJson(response, GetXUserListResponse.class) ;
		    
		    totalCount = userList.getTotalCount() ;
		    
		    if (userList.getXuserInfoList() != null) {
		    	xuserList.addAll(userList.getXuserInfoList()) ;
		    	retrievedCount = xuserList.size() ;

		    	for(XUserInfo u : userList.getXuserInfoList()) {
			    	LOG.debug("USER: Id:" + u.getId() + ", Name: " + u.getName() + ", Description: " + u.getDescription()) ;
			    }
		    }
	    }
	}
	
	private void buildUserGroupLinkList() {
		if(LOG.isDebugEnabled()) {
	 		LOG.debug("==> PolicyMgrUserGroupBuilder.buildUserGroupLinkList");
	 	}
		Client c = getClient() ;
	    
	    int totalCount = 100 ;
	    int retrievedCount = 0 ;
	    
	    while (retrievedCount < totalCount) {
		    
		    WebResource r = c.resource(getURL(PM_USER_GROUP_MAP_LIST_URI))
		    					.queryParam("pageSize", recordsToPullPerCall)
		    					.queryParam("startIndex", String.valueOf(retrievedCount)) ;
		    
		    String response = r.accept(MediaType.APPLICATION_JSON_TYPE).get(String.class);
		    
		    LOG.debug("RESPONSE: [" + response + "]") ;
		    
		    Gson gson = new GsonBuilder().create() ;
	
		    GetXUserGroupListResponse usergroupList = gson.fromJson(response, GetXUserGroupListResponse.class) ;
		    
		    totalCount = usergroupList.getTotalCount() ;
		    
		    if (usergroupList.getXusergroupInfoList() != null) {
		    	xusergroupList.addAll(usergroupList.getXusergroupInfoList()) ;
		    	retrievedCount = xusergroupList.size() ;

		    	for(XUserGroupInfo ug : usergroupList.getXusergroupInfoList()) {
			    	LOG.debug("USER_GROUP: UserId:" + ug.getUserId() + ", Name: " + ug.getGroupName()) ;
			    }
		    }
	    }
	}

	private UserGroupInfo addUserGroupInfo(String userName, List<String> groups){
		if(LOG.isDebugEnabled()) {
	 		LOG.debug("==> PolicyMgrUserGroupBuilder.addUserGroupInfo " + userName + " and groups");
	 	}
		UserGroupInfo ret = null;
		XUserInfo user = null;
		
		LOG.debug("INFO: addPMXAUser(" + userName + ")" ) ;
		if (! isMockRun) {
			user = addXUserInfo(userName) ;
		}
		
		for(String g : groups) {
				LOG.debug("INFO: addPMXAGroupToUser(" + userName + "," + g + ")" ) ;
		}
		if (! isMockRun ) { 
			addXUserGroupInfo(user, groups) ;
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
		Client c = getClient();
		
		WebResource r = c.resource(getURL(PM_ADD_USER_GROUP_INFO_URI));
		
		Gson gson = new GsonBuilder().create();
		
		String jsonString = gson.toJson(usergroupInfo);
		
		LOG.debug("USER GROUP MAPPING" + jsonString);
		
		String response = r.accept(MediaType.APPLICATION_JSON_TYPE).type(MediaType.APPLICATION_JSON_TYPE).post(String.class, jsonString) ;
		
		LOG.debug("RESPONSE: [" + response + "]") ;
		
		ret = gson.fromJson(response, UserGroupInfo.class);
		
		if ( ret != null) {
			
			XUserInfo xUserInfo = ret.getXuserInfo();
			addUserToList(xUserInfo);
			
			for(XGroupInfo xGroupInfo : ret.getXgroupInfo()) {
				addGroupToList(xGroupInfo);
				addUserGroupInfoToList(xUserInfo,xGroupInfo);
			}
		}
		
		return ret;
	}

	private void getUserGroupInfo(UserGroupInfo ret, UserGroupInfo usergroupInfo) {
		Client c = getClient();

		WebResource r = c.resource(getURL(PM_ADD_USER_GROUP_INFO_URI));

		Gson gson = new GsonBuilder().create();

		String jsonString = gson.toJson(usergroupInfo);
		if ( LOG.isDebugEnabled() ) {
		   LOG.debug("USER GROUP MAPPING" + jsonString);
		}

		String response = null;
		try{
			response=r.accept(MediaType.APPLICATION_JSON_TYPE).type(MediaType.APPLICATION_JSON_TYPE).post(String.class, jsonString) ;
		}catch(Throwable t){
			LOG.error("Failed to communicate Ranger Admin : ", t);
		}
		if ( LOG.isDebugEnabled() ) {
			LOG.debug("RESPONSE: [" + response + "]") ;
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
	}
	
	private void addUserGroupInfo(UserGroupInfo usergroupInfo){
		if(LOG.isDebugEnabled()) {
	 		LOG.debug("==> PolicyMgrUserGroupBuilder.addUserGroupInfo");
	 	}
		UserGroupInfo ret = null;
		if (authenticationType != null && AUTH_KERBEROS.equalsIgnoreCase(authenticationType) && SecureClientLogin.isKerberosCredentialExists(principal, keytab)) {
			try {
				Subject sub = SecureClientLogin.loginUserFromKeytab(principal, keytab, nameRules);
				final UserGroupInfo result = ret;
				final UserGroupInfo ugInfo = usergroupInfo;
				Subject.doAs(sub, new PrivilegedAction<Void>() {
					@Override
					public Void run() {
						try {
							getUserGroupInfo(result, ugInfo);
						} catch (Exception e) {
							LOG.error("Failed to add User Group Info : ", e);
						}
						return null;
					}
				});
			} catch (Exception e) {
				LOG.error("Failed to Authenticate Using given Principal and Keytab : ",e);
			}
		} else {
			try {
				getUserGroupInfo(ret, usergroupInfo);
			} catch (Throwable t) {
				LOG.error("Failed to add User Group Info : ", t);
			}
		}
	}

	private XUserInfo addXUserInfo(String aUserName) {
		
		XUserInfo xuserInfo = new XUserInfo() ;

		xuserInfo.setName(aUserName);
		
		xuserInfo.setDescription(aUserName + " - add from Unix box") ;
	   	
		usergroupInfo.setXuserInfo(xuserInfo);
		
		return xuserInfo ;
	}
	

	private XGroupInfo addXGroupInfo(String aGroupName) {
		
		XGroupInfo addGroup = new XGroupInfo() ;
		
		addGroup.setName(aGroupName);
		
		addGroup.setDescription(aGroupName + " - add from Unix box") ;
		
		addGroup.setGroupType("1") ;

		addGroup.setGroupSource(GROUP_SOURCE_EXTERNAL);

		return addGroup ;
	}

	
	private void addXUserGroupInfo(XUserInfo aUserInfo, List<String> aGroupList) {
		
		List<XGroupInfo> xGroupInfoList = new ArrayList<XGroupInfo>();
		
		for(String groupName : aGroupList) {
			XGroupInfo group = groupName2XGroupInfoMap.get(groupName) ;
			if (group == null) {
				group = addXGroupInfo(groupName) ;
			}
			xGroupInfoList.add(group);
			addXUserGroupInfo(aUserInfo, group) ;
		}
		
		usergroupInfo.setXgroupInfo(xGroupInfoList);
	}
	
	private List<XGroupInfo> getXGroupInfoList(List<String> aGroupList) {

		List<XGroupInfo> xGroupInfoList = new ArrayList<XGroupInfo>();
		for(String groupName : aGroupList) {
			XGroupInfo group = groupName2XGroupInfoMap.get(groupName) ;
			if (group == null) {
				group = addXGroupInfo(groupName) ;
			}else if(!GROUP_SOURCE_EXTERNAL.equals(group.getGroupSource())){
				group.setGroupSource(GROUP_SOURCE_EXTERNAL);
			}
			xGroupInfoList.add(group);
		}
		return xGroupInfoList;
	}
	

	
   private XUserGroupInfo addXUserGroupInfo(XUserInfo aUserInfo, XGroupInfo aGroupInfo) {
		
	   
	    XUserGroupInfo ugInfo = new XUserGroupInfo() ;
		
		ugInfo.setUserId(aUserInfo.getId());
		
		ugInfo.setGroupName(aGroupInfo.getName()) ;
		
		// ugInfo.setParentGroupId("1");
		
        return ugInfo;
	}

	
	private void delXUserGroupInfo(final XUserInfo aUserInfo, List<String> aGroupList) {
		for(String groupName : aGroupList) {
			final XGroupInfo group = groupName2XGroupInfoMap.get(groupName) ;
			if (group != null) {
				if (authenticationType != null && AUTH_KERBEROS.equalsIgnoreCase(authenticationType) && SecureClientLogin.isKerberosCredentialExists(principal, keytab)) {
					try {
						LOG.info("Using principal = " + principal + " and keytab = " + keytab);
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
	
		String groupName = aGroupInfo.getName();

		String userName  = aUserInfo.getName();
		
		try {

			Client c = getClient() ;

			String uri = PM_DEL_USER_GROUP_LINK_URI.replaceAll(Pattern.quote("${groupName}"), 
					   UserSyncUtil.encodeURIParam(groupName)).replaceAll(Pattern.quote("${userName}"), UserSyncUtil.encodeURIParam(userName));

			WebResource r = c.resource(getURL(uri)) ;

		    ClientResponse response = r.delete(ClientResponse.class) ;

		    if ( LOG.isDebugEnabled() ) {
		    	LOG.debug("RESPONSE: [" + response.toString() + "]") ;
		    }

		    if (response.getStatus() == 200) {
		    	delUserGroupFromList(aUserInfo, aGroupInfo) ;
		    }
 
		} catch (Exception e) {

			LOG.warn( "ERROR: Unable to delete GROUP: " + groupName  + " from USER:" + userName , e) ;
		}

	}
	
	private MUserInfo addMUser(String aUserName) {
		MUserInfo ret = null;
		MUserInfo userInfo = new MUserInfo();

		userInfo.setLoginId(aUserName);
		userInfo.setFirstName(aUserName);
		userInfo.setLastName(aUserName);

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
		Client c = getClient() ;
	    
	    WebResource r = c.resource(getURL(PM_ADD_LOGIN_USER_URI)) ;
	    
	    Gson gson = new GsonBuilder().create() ;

	    String jsonString = gson.toJson(userInfo) ;
	    
	    String response = r.accept(MediaType.APPLICATION_JSON_TYPE).type(MediaType.APPLICATION_JSON_TYPE).post(String.class, jsonString) ;
	    
	    LOG.debug("RESPONSE[" + response + "]") ;
	    
	    ret = gson.fromJson(response, MUserInfo.class) ;
	    
	    LOG.debug("MUser Creation successful " + ret);
		
		return ret ;
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
					InputStream in = null ;
					try {
						in = getFileInputStream(keyStoreFile) ;
						if (in == null) {
							LOG.error("Unable to obtain keystore from file [" + keyStoreFile + "]");
							return ret ;
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
					InputStream in = null ;
					try {
						in = getFileInputStream(trustStoreFile) ;
						if (in == null) {
							LOG.error("Unable to obtain keystore from file [" + trustStoreFile + "]");
							return ret ;
						}
						trustStore.load(in, trustStoreFilepwd.toCharArray());
						TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
						trustManagerFactory.init(trustStore);
						tmList = trustManagerFactory.getTrustManagers();
					}
					finally {
						if (in != null) {
							in.close() ;
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
				 if(username==null||password==null||username.trim().isEmpty()||password.trim().isEmpty()){
					 username=config.getDefaultPolicyMgrUserName();
					 password=config.getDefaultPolicyMgrPassword();
				 }
				 if(username!=null && password!=null){
					 ret.addFilter(new HTTPBasicAuthFilter(username, password));
				 }
			}
		}
		return ret ;
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
				ret = ClassLoader.getSystemClassLoader().getResourceAsStream(path) ;
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
	public void addOrUpdateGroup(String groupName) {
		XGroupInfo group = groupName2XGroupInfoMap.get(groupName) ;
		
		if (group == null) {    // Does not exists
			
			//* Build the group info object and do the rest call
 			if ( ! isMockRun ) {
 				group = addGroupInfo(groupName);
 				if ( group != null) {
 					addGroupToList(group);
 				}
 			}
		}
	}
	
	private XGroupInfo addGroupInfo(final String groupName){
		XGroupInfo ret = null;
		XGroupInfo group = null;
		
		LOG.debug("INFO: addPMXAGroup(" + groupName + ")" ) ;
		if (! isMockRun) {
			group = addXGroupInfo(groupName) ;
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
		
		Client c = getClient();
		
		WebResource r = c.resource(getURL(PM_ADD_GROUP_URI));
		
		Gson gson = new GsonBuilder().create();
		
		String jsonString = gson.toJson(group);
		
		LOG.debug("Group" + jsonString);
		
		String response = r.accept(MediaType.APPLICATION_JSON_TYPE).type(MediaType.APPLICATION_JSON_TYPE).post(String.class, jsonString) ;
		
		LOG.debug("RESPONSE: [" + response + "]") ;
		
		ret = gson.fromJson(response, XGroupInfo.class);
		
		return ret;	
	}

	
}
