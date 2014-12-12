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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.ranger.unixusersync.config.UserGroupSyncConfig;
import org.apache.ranger.usergroupsync.UserGroupSink;
import org.apache.ranger.usergroupsync.UserGroupSource;

public class UnixUserGroupBuilder implements UserGroupSource {
	
	private static final Logger LOG = Logger.getLogger(UnixUserGroupBuilder.class) ;

	public static final String UNIX_USER_PASSWORD_FILE = "/etc/passwd" ;
	public static final String UNIX_GROUP_FILE = "/etc/group" ;

	private UserGroupSyncConfig config = UserGroupSyncConfig.getInstance() ;
	private Map<String,List<String>>  	user2GroupListMap = new HashMap<String,List<String>>();
	private Map<String,List<String>>  	internalUser2GroupListMap = new HashMap<String,List<String>>();
	private Map<String,String>			groupId2groupNameMap = new HashMap<String,String>() ;
	private int 						minimumUserId  = 0 ;
	
	private long  passwordFileModiiedAt = 0 ;
	private long  groupFileModifiedAt = 0 ;
		
	
	public static void main(String[] args) throws Throwable {
		UnixUserGroupBuilder  ugbuilder = new UnixUserGroupBuilder() ;
		ugbuilder.init();
		ugbuilder.print(); 
	}
	
	public UnixUserGroupBuilder() {
		minimumUserId = Integer.parseInt(config.getMinUserId()) ;
		LOG.debug("Minimum UserId: " + minimumUserId) ;
	}

	@Override
	public void init() throws Throwable {
		buildUserGroupInfo() ;
	}
	
	@Override
	public boolean isChanged() {
		long TempPasswordFileModiiedAt = new File(UNIX_USER_PASSWORD_FILE).lastModified() ;
		if (passwordFileModiiedAt != TempPasswordFileModiiedAt) {
			return true ;
		}
		
		long TempGroupFileModifiedAt = new File(UNIX_GROUP_FILE).lastModified() ;
		if (groupFileModifiedAt != TempGroupFileModifiedAt) {
			return true ;
		}
		
		return false ;
	}
	

	@Override
	public void updateSink(UserGroupSink sink) throws Throwable {
		buildUserGroupInfo() ;

		for (Map.Entry<String, List<String>> entry : user2GroupListMap.entrySet()) {
		    String       user   = entry.getKey();
		    List<String> groups = entry.getValue();
		    
		    sink.addOrUpdateUser(user, groups);
		}
	}
	
	
	private void buildUserGroupInfo() throws Throwable {
		user2GroupListMap = new HashMap<String,List<String>>();
		groupId2groupNameMap = new HashMap<String,String>() ;

		buildUnixGroupList(); 
		buildUnixUserList();
		if (LOG.isDebugEnabled()) {
			print() ;
		}
	}
	
	private void print() {
		for(String user : user2GroupListMap.keySet()) {
			LOG.debug("USER:" + user) ;
			List<String> groups = user2GroupListMap.get(user) ;
			if (groups != null) {
				for(String group : groups) {
					LOG.debug("\tGROUP: " + group) ;
				}
			}
		}
	}
	
	private void buildUnixUserList() throws Throwable {
		
		File f = new File(UNIX_USER_PASSWORD_FILE) ;

		if (f.exists()) {
			
			
			BufferedReader reader = null ;
			
			reader = new BufferedReader(new FileReader(f)) ;
			
			String line = null ;
			
			while ( (line = reader.readLine()) != null) {
				
				if (line.trim().isEmpty()) 
					continue ;
				
				String[] tokens = line.split(":") ;
				
				int len = tokens.length ;
				
				if (len < 2) {
					continue ;
				}
				
				String userName = tokens[0] ;
				String userId = tokens[2] ;
				String groupId = tokens[3] ;
				
				int numUserId = -1 ; 
				
				try {
					numUserId = Integer.parseInt(userId) ;
				}
				catch(NumberFormatException nfe) {
					LOG.warn("Unix UserId: [" + userId + "]: can not be parsed as valid int. considering as  -1.", nfe);
					numUserId = -1 ;
				}
									
				if (numUserId >= minimumUserId ) {
					String groupName = groupId2groupNameMap.get(groupId) ;
					if (groupName != null) {
						List<String> groupList = new ArrayList<String>();
						groupList.add(groupName);
						// do we already know about this use's membership to other groups?  If so add those, too
						if (internalUser2GroupListMap.containsKey(userName)) {
							groupList.addAll(internalUser2GroupListMap.get(userName));
						}
						user2GroupListMap.put(userName, groupList);
					}
					else {
						// we are ignoring the possibility that this user was present in /etc/groups.
						LOG.warn("Group Name could not be found for group id: [" + groupId + "]. Skipping adding user [" + userName + "] with id [" + userId + "].") ;
					}
				}
				else {
					LOG.debug("Skipping user [" + userName + "] since its userid [" + userId + "] is less than minuserid limit [" + minimumUserId + "].");
				}
			}
			
			reader.close() ;
			
			passwordFileModiiedAt = f.lastModified() ;

		}
	}

	
	private void buildUnixGroupList() throws Throwable {
		
		File f = new File(UNIX_GROUP_FILE) ;
		
		if (f.exists()) {
			
			BufferedReader reader = null ;
			
			reader = new BufferedReader(new FileReader(f)) ;
			
			String line = null ;
			
			
			
			while ( (line = reader.readLine()) != null) {

				if (line.trim().isEmpty()) 
					continue ;
				
				String[] tokens = line.split(":") ;
				
				int len = tokens.length ;
				
				if (len < 2) {
					continue ;
				}
				
				String groupName = tokens[0] ;
				String groupId = tokens[2] ;
				String groupMembers = null ;
				
				if (tokens.length > 3) {
					groupMembers = tokens[3] ;
				}
				
				if (groupId2groupNameMap.containsKey(groupId)) {
					groupId2groupNameMap.remove(groupId) ;
				}
				
				groupId2groupNameMap.put(groupId,groupName) ;
				// also build an internal map of users to their group list which is consulted by user list creator
				if (groupMembers != null && ! groupMembers.trim().isEmpty()) {
					for(String user : groupMembers.split(",")) {
						List<String> groupList = internalUser2GroupListMap.get(user) ;
						if (groupList == null) {
							groupList = new ArrayList<String>() ;
							internalUser2GroupListMap.put(user, groupList) ;
						}
						if (! groupList.contains(groupName)) {
							groupList.add(groupName) ;
						}
					}
				}

				
			}
			
			reader.close() ;
			
			groupFileModifiedAt = f.lastModified() ;

		
		}
	}
	

}
