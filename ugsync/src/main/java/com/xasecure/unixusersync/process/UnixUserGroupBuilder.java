package com.xasecure.unixusersync.process;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.xasecure.unixusersync.config.UserGroupSyncConfig;
import com.xasecure.usergroupsync.UserGroupSink;
import com.xasecure.usergroupsync.UserGroupSource;

public class UnixUserGroupBuilder implements UserGroupSource {
	
	private static final Logger LOG = Logger.getLogger(UnixUserGroupBuilder.class) ;

	public static final String UNIX_USER_PASSWORD_FILE = "/etc/passwd" ;
	public static final String UNIX_GROUP_FILE = "/etc/group" ;

	private UserGroupSyncConfig config = UserGroupSyncConfig.getInstance() ;
	private Map<String,List<String>>  	user2GroupListMap = new HashMap<String,List<String>>();
	private Map<String,String>			groupId2groupNameMap = new HashMap<String,String>() ;
	private List<String>				userList = new ArrayList<String>() ;
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
		userList = new ArrayList<String>() ;

		buildUnixGroupList(); 
		buildUnixUserList();
		if (LOG.isDebugEnabled()) {
			print() ;
		}
	}
	
	public void print() {
		for(String user : userList) {
			LOG.debug("USER:" + user) ;
			List<String> groups = user2GroupListMap.get(user) ;
			if (groups != null) {
				for(String group : groups) {
					LOG.debug("\tGROUP: " + group) ;
				}
			}
		}
	}
	
	private List<String>  getUserList() {
		return userList ;
	}
	
	private List<String>  getGroupListForUser(String aUserName) {
		return user2GroupListMap.get(aUserName) ;
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
					userList.add(userName) ;
					String groupName = groupId2groupNameMap.get(groupId) ;
					if (groupName != null) {
						List<String> groupList = user2GroupListMap.get(userName) ;
						if (groupList == null) {
							groupList = new ArrayList<String>() ;
							user2GroupListMap.put(userName, groupList) ;
						}
						if (! groupList.contains(groupName)) {
							groupList.add(groupName) ;
						}
					}
					else {
						LOG.warn("Group Name could not be found for group id: [" + groupId + "]") ;
					}
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
				
				if (groupMembers != null && ! groupMembers.trim().isEmpty()) {
					for(String user : groupMembers.split(",")) {
						List<String> groupList = user2GroupListMap.get(user) ;
						if (groupList == null) {
							groupList = new ArrayList<String>() ;
							user2GroupListMap.put(user, groupList) ;
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
