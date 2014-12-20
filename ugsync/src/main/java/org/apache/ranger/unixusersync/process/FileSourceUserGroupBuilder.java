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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.ranger.unixusersync.config.UserGroupSyncConfig;
import org.apache.ranger.unixusersync.model.UserGroupList;
import org.apache.ranger.usergroupsync.UserGroupSink;
import org.apache.ranger.usergroupsync.UserGroupSource;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class FileSourceUserGroupBuilder  implements UserGroupSource {

	private static final Logger LOG = Logger.getLogger(FileSourceUserGroupBuilder.class) ;
	
	public static  String DEFAULT_USER_GROUP_FILE = null;
	public static String USER_GROUP_FILE = null;
	
	private static boolean isManualRun=false;
	private Map<String,List<String>>  user2GroupListMap = new HashMap<String,List<String>>();
	private UserGroupSyncConfig config = UserGroupSyncConfig.getInstance() ;
	private UserGroupSink ugSink     = null ;
	private long  usergroupFileModified = 0 ;
	
	public static void main(String[] args) throws Throwable {
		if (args.length > 0) {
			isManualRun=true;
			USER_GROUP_FILE = args[0];
		}	
		FileSourceUserGroupBuilder  filesourceUGBuilder = new FileSourceUserGroupBuilder() ;
		filesourceUGBuilder.init();
		if ( LOG.isDebugEnabled()) {
			filesourceUGBuilder.print(); 
		}
	}
	
	@Override
	public void init() throws Throwable {
		DEFAULT_USER_GROUP_FILE = config.getUserSyncFileSource();
		buildUserGroupInfo();
		if (isManualRun) {
			ugSink = UserGroupSyncConfig.getInstance().getUserGroupSink();
			LOG.info("initializing sink: " + ugSink.getClass().getName());
			ugSink.init();
			updateSink(ugSink);
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
	
	@Override
	public boolean isChanged() {
		long TempUserGroupFileModifedAt = new File(DEFAULT_USER_GROUP_FILE).lastModified() ;
		if (usergroupFileModified != TempUserGroupFileModifedAt) {
			return true ;
		}
		return false;
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

	public void buildUserGroupInfo() throws Throwable {
		user2GroupListMap = new HashMap<String,List<String>>();
		buildUserGroupList();
		if ( LOG.isDebugEnabled()) {
			print(); 
		}
	}
	
	public void buildUserGroupList() throws Throwable {
	
		Gson gson = new GsonBuilder().create() ;
		
		UserGroupList usergrouplist = new UserGroupList();
		
		String usergroupFile = null;
		
		if (isManualRun) {
			usergroupFile = USER_GROUP_FILE;
		} else {
			usergroupFile = DEFAULT_USER_GROUP_FILE;
		}
		
		if (usergroupFile == null){
			throw new Exception("User Group Source File is not Configured. Please maintain in unixauthservice.properties or pass it as command line argument for org.apache.ranger.unixusersync.process.FileSourceUserGroupBuilder");
		}
		
		File f = new File(usergroupFile);
		
		if (f.exists()) {
			
			BufferedReader bfr  = new BufferedReader(new FileReader(f));
			
			String line = null ;
			
			while ((line = bfr.readLine()) != null) {
			
				if (line.trim().isEmpty()) 
					continue ;
				
				usergrouplist = gson.fromJson(line,UserGroupList.class);
				
				String user = usergrouplist.getUser();
				
				List<String> groups = usergrouplist.getGroups();
				
				if ( user != null) {
					user2GroupListMap.put(user, groups);
				}
			}
			
			bfr.close();
			usergroupFileModified = f.lastModified() ;
		}

	}
}