package com.xasecure.unixusersync.poc;

import java.io.BufferedReader;
import java.io.FileReader;


public class ListUserTest
{
 public static String strLine;

 public static void main(String args[])
  {
	 
  try{
  
	  FileReader file = new FileReader("C:\\git\\xa_server\\conf\\client\\passwd");
      BufferedReader br = new BufferedReader(file);
  
	  while ((strLine = br.readLine()) != null)   {
		 ListXaSecureUser userList = ListXaSecureUser.parseUser(strLine);
		 System.out.println(userList.getName() + " " + userList.getUid() + " " + userList.getGid());
	  }
	
	  file.close();
    }catch (Exception e){//Catch exception if any
    	System.err.println("Error: " + e.getMessage());
    }
  }
}

