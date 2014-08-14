package com.xasecure.unixusersync.poc;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashMap;

public class XaSecureClientUserGroupMapping {
	
	private static String strLine;
	private static final String TAG_USER_NAME = "name";
	private static final String TAG_USER_ID = "userId";
	private static final String TAG_GROUP_ID = "id";
	
	public static ArrayList<HashMap<String, String>> buildClientUserGroupMapping(String passwdFile){
	
	ArrayList<HashMap<String, String>> ClientUserGroupMapping = new ArrayList<HashMap<String, String>>();

	try{
		FileReader file = new FileReader(passwdFile);
		
	    BufferedReader br = new BufferedReader(file);
	  
		
	    while ((strLine = br.readLine()) != null)  {
	    	
	    	ListXaSecureUser userList = ListXaSecureUser.parseUser(strLine);
		 
	    	HashMap<String, String> map = new HashMap<String, String>();
         
	    	// adding each child node to HashMap key => value
	    	map.put(TAG_USER_NAME, userList.getName());
	    	map.put(TAG_USER_ID, userList.getUid());
	    	map.put(TAG_GROUP_ID, userList.getGid());
	    	
	    	// adding HashList to ArrayList
            ClientUserGroupMapping.add(map);
            
			// System.out.println(userList.getName() + " " + userList.getUid() + " " + userList.getGid());
		  }
	
	    file.close();
		}catch (Exception e){//Catch exception if any
			System.err.println("Error: " + e.getMessage());
		}
		return ClientUserGroupMapping;
	}
}
