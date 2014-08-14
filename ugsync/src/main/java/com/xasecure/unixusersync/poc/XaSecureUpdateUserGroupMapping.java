package com.xasecure.unixusersync.poc;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;


public class XaSecureUpdateUserGroupMapping {
	
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
	usergroupmapping = XaSecureUserGroupMapping.buildUserGroupMapping(url) ;
	
	//get user group mapping from client system file
	clientusergroupmapping = XaSecureClientUserGroupMapping.buildClientUserGroupMapping(passwdfile) ;
	
	compare_and_update(usergroupmapping,clientusergroupmapping);
	
	*/
	}
}

