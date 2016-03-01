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

 package org.apache.ranger.unixusersync.poc;


import java.util.ArrayList;
import java.util.HashMap;


public class RangerUpdateUserGroupMapping {
	
  private static ArrayList<HashMap<String, String>> usergroupmapping = null;
  
  private static ArrayList<HashMap<String, String>> clientusergroupmapping = null;
  
  private static void compare_and_update(ArrayList<HashMap<String, String>> fromDB, ArrayList<HashMap<String, String>> fromFile ){
	
	  for(HashMap<String, String> hmap: fromDB) {
		  String dbuserid = hmap.get("id");
		  String dbgroupid = hmap.get("userId");
		  if ( !findMatchingUserGroupId(dbuserid,dbgroupid,fromFile)) {
			  	System.out.println(dbuserid + " " + dbgroupid);
		  } 
			   
	  }
  }
  
  private static boolean findMatchingUserGroupId(String dbuserid, String dbgroupid,ArrayList<HashMap<String, String>>  fromFile) {
	  
	  boolean matchFound = false;
	  
	  for(HashMap<String, String> fhmap: fromFile) {
		  String fileuserid = fhmap.get("id");
		  String filegroupid = fhmap.get("userId");
		  
		  if ( dbuserid.equals(fileuserid) && dbgroupid.equals(filegroupid) ) {
			  matchFound = true;
			  return matchFound;
		  }
	  }
	  return matchFound;
  }
  
  public static void main(String[] args) {

	  /*
	//get user group mapping from DB
	usergroupmapping = RangerUserGroupMapping.buildUserGroupMapping(url) ;
	
	//get user group mapping from client system file
	clientusergroupmapping = RangerClientUserGroupMapping.buildClientUserGroupMapping(passwdfile) ;
	
	compare_and_update(usergroupmapping,clientusergroupmapping);
	
	*/
	}
}

