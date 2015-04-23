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
import java.io.InputStream;
import java.net.UnknownHostException;
import java.security.KeyStore;
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
import javax.ws.rs.core.MediaType;

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

public class PolicyMgrUserGroupBuilder implements UserGroupSink {
	
	private static final Logger LOG = Logger.getLogger(PolicyMgrUserGroupBuilder.class) ;
	
	public static final String PM_USER_LIST_URI  = "/service/xusers/users/" ;				// GET
	private static final String PM_ADD_USER_URI  = "/service/xusers/users/" ;				// POST
	private static final String PM_ADD_USER_GROUP_INFO_URI = "/service/xusers/users/userinfo" ;	// POST
	
	public static final String PM_GROUP_LIST_URI = "/service/xusers/groups/" ;				// GET
	private static final String PM_ADD_GROUP_URI = "/service/xusers/groups/" ;				// POST
	
	
	public static final String PM_USER_GROUP_MAP_LIST_URI = "/service/xusers/groupusers/" ;		// GET
	private static final String PM_ADD_USER_GROUP_LINK_URI = "/service/xusers/groupusers/" ;	// POST
	
	private static final String PM_DEL_USER_GROUP_LINK_URI = "/service/xusers/group/${groupName}/user/${userName}" ; // DELETE
	
	private static final String PM_ADD_LOGIN_USER_URI = "/service/users/default" ;			// POST
	
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

	
	static {
		try {
			LOCAL_HOSTNAME = java.net.InetAddress.getLocalHost().getHostName();
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
		
		buildUserGroupInfo() ;
	}
	
	private void buildUserGroupInfo() throws Throwable {
		buildGroupList(); 
		buildUserList();
		buildUserGroupLinkList() ;
		rebuildUserGroupMap() ;
		if (LOG.isDebugEnabled()) {
			this.print(); 
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
			
			for(String group : groups) {
				if (! oldGroups.contains(group)) {
					addGroups.add(group) ;
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
 				addXUserGroupInfo(user, addGroups) ;
 			}
 			
 			for(String g : delGroups) {
 				LOG.debug("INFO: delPMXAGroupFromUser(" + userName + "," + g + ")" ) ;
 			}
 			
 			if (! isMockRun ) {
 				delXUserGroupInfo(user, delGroups) ;
 			}
			
		}
	}
	
	
	private void buildGroupList() {
		
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
		    	xgroupList.addAll(groupList.getXgroupInfoList()) ;
		    	retrievedCount = xgroupList.size() ;

		    	for(XGroupInfo g : groupList.getXgroupInfoList()) {
		    		LOG.debug("GROUP:  Id:" + g.getId() + ", Name: " + g.getName() + ", Description: " + g.getDescription()) ;
			    }
		    }
	    }

	}

	
	private void buildUserList() {
		
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
	
	

	
   private XUserGroupInfo addXUserGroupInfo(XUserInfo aUserInfo, XGroupInfo aGroupInfo) {
		
	    XUserGroupInfo ugInfo = new XUserGroupInfo() ;
		
		ugInfo.setUserId(aUserInfo.getId());
		
		ugInfo.setGroupName(aGroupInfo.getName()) ;
		
		// ugInfo.setParentGroupId("1");
		
	    return ugInfo ;
	}

	
	private void delXUserGroupInfo(XUserInfo aUserInfo, List<String> aGroupList) {
		for(String groupName : aGroupList) {
			XGroupInfo group = groupName2XGroupInfoMap.get(groupName) ;
			if (group != null) {
				delXUserGroupInfo(aUserInfo, group) ;
			}
		}
	}

	private void delXUserGroupInfo(XUserInfo aUserInfo, XGroupInfo aGroupInfo) {
		
		Client c = getClient() ;
	    
	    String uri = PM_DEL_USER_GROUP_LINK_URI.replaceAll(Pattern.quote("${groupName}"), aGroupInfo.getName()).replaceAll(Pattern.quote("${userName}"), aUserInfo.getName()) ;
	    
	    WebResource r = c.resource(getURL(uri)) ;
	    
	    ClientResponse response = r.delete(ClientResponse.class) ;
	    
	    LOG.debug("RESPONSE: [" + response.toString() + "]") ;

	    
	    if (response.getStatus() == 200) {
	    	delUserGroupFromList(aUserInfo, aGroupInfo) ;
	    }
		
	}
	
	
	private MUserInfo addMUser(String aUserName) {
		
		MUserInfo ret = null ;
		
		MUserInfo userInfo = new MUserInfo() ;

		userInfo.setLoginId(aUserName);
		userInfo.setFirstName(aUserName);
		userInfo.setLastName(aUserName);
		userInfo.setEmailAddress(aUserName + "@" + LOCAL_HOSTNAME);
	
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
		return ret ;
	}
	
	private InputStream getFileInputStream(String path) throws FileNotFoundException {

		InputStream ret = null;

		File f = new File(path);

		if (f.exists()) {
			ret = new FileInputStream(f);
		} else {
			ret = getClass().getResourceAsStream(path);
			
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

	
}
