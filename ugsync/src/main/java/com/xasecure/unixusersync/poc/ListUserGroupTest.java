package com.xasecure.unixusersync.poc;

import java.util.ArrayList;
import java.util.HashMap;


public class ListUserGroupTest {
	
	//test code for client user group mapping fetch
 	
  private static String passwdfile = "C:\\git\\xa_server\\conf\\client\\passwd";
		   		
  private static ArrayList<HashMap<String, String>> clientusergroupmapping = null;
  
  public static void main(String[] args) {

	clientusergroupmapping = XaSecureClientUserGroupMapping.buildClientUserGroupMapping(passwdfile) ;
	System.out.println(clientusergroupmapping);
	}
}