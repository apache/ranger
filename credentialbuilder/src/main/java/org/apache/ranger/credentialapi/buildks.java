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

 package org.apache.ranger.credentialapi;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.Field;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.alias.CredentialShell;
import org.apache.hadoop.util.GenericOptionsParser;

public class buildks {
	public static void main(String[] args) {
		buildks buildksOBJ=new buildks();
		String command=null;
		try{
			if(args!=null && args.length>=3){
				command=args[0];
				if(command!=null && !command.trim().isEmpty()){
					if(command.equalsIgnoreCase("create")){
						buildksOBJ.createCredential(args);
					}else if(command.equalsIgnoreCase("list")){
						buildksOBJ.listCredential(args);
					}else{
						System.out.println(command +" is not supported in current version of CredentialBuilder API.");
						System.exit(1);
					}
				}
			}else{
				System.out.println("Invalid Command line argument.");
				System.exit(1);
			}
		}catch(Exception ex){
			ex.printStackTrace();
			System.exit(1);
		}
	}
	
	public int createCredential(String args[]){
		int returnCode=-1;
		String command=null;
    	String alias=null;
    	String valueOption=null;
    	String credential=null;
    	String providerOption=null;
    	String providerPath=null;
    	String tempCredential=null;
		try{	    		    	
	    	if(args!=null && args.length==6)
	    	{
	    		command=args[0];
	    		alias=args[1];
	    		valueOption=args[2];
	    		credential=args[3];
	    		providerOption=args[4];
	    		providerPath=args[5];
				if(!isValidCreateCommand(command,alias,valueOption,credential,providerOption,providerPath)){
	    			return returnCode;
				}
				deleteInvalidKeystore(providerPath);
	    		tempCredential=CredentialReader.getDecryptedString(providerPath, alias);
	    	}else{
	    		return returnCode;
	    	}
	    	
	    	if(tempCredential==null){
	    		returnCode=createKeyStore(args);
	    	}else{
	    		try{
	    			System.out.println("The alias " + alias + " already exists!! Will try to delete first.");
	    			boolean isSilentMode = true;
	    			String argsDelete[]=new String[4];
	    			argsDelete[0]="delete";
	    			argsDelete[1]=alias;
	    			argsDelete[2]=providerOption;
	    			argsDelete[3]=providerPath;
	    			returnCode=deleteCredential(argsDelete, isSilentMode);
	    			if(returnCode==0){
	    	    		returnCode=createKeyStore(args);
	    	    	}
	    		}catch(Exception ex){
	    			returnCode=-1;
	    		}
	    	}
	    }catch(Exception ex){
    		ex.printStackTrace();
    	}
		return returnCode;
	}	
	
	public int createKeyStore(String args[]){
		int returnCode=-1;
		try{
	    	String command=null;
	    	String alias=null;
	    	String valueOption=null;
	    	String credential=null;
	    	String providerOption=null;
	    	String providerPath=null;	    	
	    	if(args!=null && args.length==6)
	    	{
	    		command=args[0];
	    		alias=args[1];
	    		valueOption=args[2];
	    		credential=args[3];
	    		providerOption=args[4];
	    		providerPath=args[5];
				if(!isValidCreateCommand(command,alias,valueOption,credential,providerOption,providerPath)){
	    			return returnCode;
	    		}	    		
		    	displayCommand(args);
	    	}else{
	    		return returnCode;
	    	}	    	
	    	
	    	CredentialShell cs = new CredentialShell();
	    	Configuration conf = new Configuration();	
	    	//parse argument
	    	GenericOptionsParser parser = new GenericOptionsParser(conf, args);
	        //set the configuration back, so that Tool can configure itself
	    	cs.setConf(conf);
	    	//get valid and remaining argument
	    	String[] toolArgs = parser.getRemainingArgs();	    	
	    	//execute command in CredentialShell
			// int i = 0;
			//  for(String s : toolArgs) {
			//		System.out.println("TooArgs [" + i + "] = [" + s + "]");
		    //		i++;
			// }
	    	returnCode= cs.run(toolArgs);
	    	//if response code is zero then success else failure	    	
	    	//System.out.println("Response Code:"+returnCode);	    	
		}catch(IOException ex){
    		ex.printStackTrace();
    	} catch(Exception ex){
    		ex.printStackTrace();
    	}
		return returnCode;
	}
	public int createCredentialFromUserInput(){
		int returnCode=-1;
		try{
			String[] args=null;
	    	String command=null;
	    	String alias=null;
	    	String valueOption=null;
	    	String credential=null;
	    	String providerOption=null;
	    	String providerPath=null;	    	
	    	//below code can ask user to input if command line input fails	    		
    		System.out.println("Enter Alias Name:");
    		BufferedReader bufferRead = new BufferedReader(new InputStreamReader(System.in));        		
    		alias = bufferRead.readLine();
    		System.out.println("Enter password:");
    		credential = bufferRead.readLine();
    		System.out.println("Enter .jceks output file name with path:");
    		providerPath = bufferRead.readLine();
			if(providerPath!=null && !providerPath.trim().isEmpty() && !providerPath.startsWith("localjceks://file")&&!providerPath.startsWith("jceks://file"))
			{
    			if(providerPath.startsWith("/")){
					providerPath="jceks://file"+providerPath;
				}else{
					providerPath="jceks://file/"+providerPath;
				}
        	}	        	
    		command="create";
    		valueOption="-value";
    		providerOption="-provider";
			if(!isValidCreateCommand(command,alias,valueOption,credential,providerOption,providerPath)){
    			return returnCode;
    		}
    		args=new String[6];
    		args[0]=command;
    		args[1]=alias;
    		args[2]=valueOption;
    		args[3]=credential;
    		args[4]=providerOption;
    		args[5]=providerPath;    		    	
	    	CredentialShell cs = new CredentialShell();
	    	Configuration conf = new Configuration();	
	    	//parse argument
	    	GenericOptionsParser parser = new GenericOptionsParser(conf, args);
	        //set the configuration back, so that Tool can configure itself
	    	cs.setConf(conf);
	    	//get valid and remaining argument
	    	String[] toolArgs = parser.getRemainingArgs();	    		
	    	//execute command in CredentialShell
	    	returnCode= cs.run(toolArgs);
	    	//if response code is zero then success else failure	    	
	    	//System.out.println("Response Code:"+returnCode);	    	
		}catch(IOException ex){
    		ex.printStackTrace();
    	} catch(Exception ex){
    		ex.printStackTrace();
    	}
		return returnCode;
	}	
	
	public int listCredential(String args[]){
		int returnCode=-1;
		String command=null;
		String providerOption=null;
		String providerPath=null;
		try{	    		    	
	    	if(args!=null && args.length==3)
	    	{
				command=args[0];
				providerOption=args[1];
				providerPath=args[2];
				if(!isValidListCommand(command,providerOption,providerPath)){
					return returnCode;
				}
	    		//display command which need to be executed or entered
	    		displayCommand(args);
	    	}else{
	    		return returnCode;
	    	}	    	
	    	CredentialShell cs = new CredentialShell();
	    	Configuration conf = new Configuration();	
	    	//parse argument
	    	GenericOptionsParser parser = new GenericOptionsParser(conf, args);
	        //set the configuration back, so that Tool can configure itself
	    	cs.setConf(conf);
	    	//get valid and remaining argument
	    	String[] toolArgs = parser.getRemainingArgs();	    		
	    	//execute command in CredentialShell
	    	returnCode= cs.run(toolArgs);
	    	//if response code is zero then success else failure	    	
	    	//System.out.println("Response Code:"+returnCode);	    	
		}catch(IOException ex){
    		ex.printStackTrace();
    	} catch(Exception ex){
    		ex.printStackTrace();
    	}
		return returnCode;
	}	
	
	public int deleteCredential(String args[], boolean isSilentMode){
		int returnCode=-1;
		try{	    		    	
	    	if(args!=null && args.length==4)
	    	{
		        // for non-interactive, insert argument "-f" if needed
		        if(isSilentMode && isCredentialShellInteractiveEnabled()) {
			        String[] updatedArgs = new String[5];
			        updatedArgs[0] = args[0];
			        updatedArgs[1] = args[1];
			        updatedArgs[2] = "-f";
			        updatedArgs[3] = args[2];
			        updatedArgs[4] = args[3];

			        args = updatedArgs;
			    }

	    		//display command which need to be executed or entered
	    		displayCommand(args);
	    	}else{
	    		return returnCode;
	    	}	    	
	    	CredentialShell cs = new CredentialShell();
	    	Configuration conf = new Configuration();	
	    	//parse argument
	    	GenericOptionsParser parser = new GenericOptionsParser(conf, args);
	        //set the configuration back, so that Tool can configure itself
	    	cs.setConf(conf);
	    	//get valid and remaining argument
	    	String[] toolArgs = parser.getRemainingArgs();	    		
	    	//execute command in CredentialShell
	    	returnCode= cs.run(toolArgs);
	    	//if response code is zero then success else failure	    	
	    	//System.out.println("Response Code:"+returnCode);	    	
		}catch(IOException ex){
    		ex.printStackTrace();
    	} catch(Exception ex){
    		ex.printStackTrace();
    	}
		return returnCode;
	}	
	
	public static boolean isValidCreateCommand(String command,String alias,String valueOption,String credential,String providerOption,String providerPath)
    {
		boolean isValid=true;
		try{
        	if(command==null || !"create".equalsIgnoreCase(command.trim()))
        	{
        		System.out.println("Invalid create phrase in credential creation command!!");
        		System.out.println("Expected:'create' Found:'"+command+"'");
				displaySyntax("create");
        		return false;
        	}
        	if(alias==null || "".equalsIgnoreCase(alias.trim()))
        	{
        		System.out.println("Invalid alias name phrase in credential creation command!!");
        		System.out.println("Found:'"+alias+"'");
				displaySyntax("create");
        		return false;
        	}
        	if(valueOption==null || !"-value".equalsIgnoreCase(valueOption.trim()))
        	{
        		System.out.println("Invalid value option switch in credential creation command!!");
        		System.out.println("Expected:'-value' Found:'"+valueOption+"'");
				displaySyntax("create");
        		return false;
        	}
        	if(valueOption==null || !"-value".equalsIgnoreCase(valueOption.trim()))
        	{
        		System.out.println("Invalid value option in credential creation command!!");
        		System.out.println("Expected:'-value' Found:'"+valueOption+"'");
				displaySyntax("create");
        		return false;
        	}
        	if(credential==null)
        	{
        		System.out.println("Invalid credential value in credential creation command!!");
        		System.out.println("Found:"+credential);
				displaySyntax("create");
        		return false;
        	}
        	if(providerOption==null || !"-provider".equalsIgnoreCase(providerOption.trim()))
        	{
        		System.out.println("Invalid provider option in credential creation command!!");
        		System.out.println("Expected:'-provider' Found:'"+providerOption+"'");
				displaySyntax("create");
        		return false;
        	}
			if(providerPath==null || "".equalsIgnoreCase(providerPath.trim()) || (!providerPath.startsWith("localjceks://") && !providerPath.startsWith("jceks://")))
        	{
        		System.out.println("Invalid provider option in credential creation command!!");
        		System.out.println("Found:'"+providerPath+"'");
				displaySyntax("create");
        		return false;
        	}
    	}catch(Exception ex){    	
    		System.out.println("Invalid input or runtime error! Please try again.");
    		System.out.println("Input:"+command+" "+alias+" "+valueOption+" "+credential+" "+providerOption+" "+providerPath);
			displaySyntax("create");
    		ex.printStackTrace();
    		return false;
    	}            	
    	return isValid;
    }

	public static boolean isValidListCommand(String command,String providerOption,String providerPath){
		boolean isValid=true;
		try{
			if(command==null || !"list".equalsIgnoreCase(command.trim())){
				System.out.println("Invalid list phrase in credential get command!!");
				System.out.println("Expected:'list' Found:'"+command+"'");
				displaySyntax("list");
				return false;
			}

			if(providerOption==null || !"-provider".equalsIgnoreCase(providerOption.trim()))
			{
				System.out.println("Invalid provider option in credential get command!!");
				System.out.println("Expected:'-provider' Found:'"+providerOption+"'");
				displaySyntax("list");
				return false;
			}
			if(providerPath==null || "".equalsIgnoreCase(providerPath.trim()) || (!providerPath.startsWith("localjceks://") && !providerPath.startsWith("jceks://")))
			{
				System.out.println("Invalid provider option in credential get command!!");
				System.out.println("Found:'"+providerPath+"'");
				displaySyntax("list");
				return false;
			}
		}catch(Exception ex){
			System.out.println("Invalid input or runtime error! Please try again.");
			System.out.println("Input:"+command+" "+providerOption+" "+providerPath);
			displaySyntax("list");
			ex.printStackTrace();
			return false;
		}
		return isValid;
	}
	
	public static void displayCommand(String args[])
    {
		String debugOption = System.getProperty("debug");
		if (debugOption != null && "TRUE".equalsIgnoreCase(debugOption)) {
			StringBuilder tempBuffer=new StringBuilder("");
			if(args!=null && args.length>0){
				for (String arg : args) {
					tempBuffer.append(arg).append(" ");
				}
				System.out.println("Command to execute:["+tempBuffer+"]");
			}
		}
    }
	
	public static void displaySyntax(String command){
		if(command!=null && command.trim().equalsIgnoreCase("create")){
			System.out.println("Correct syntax is:create <aliasname> -value <password> -provider <jceks://file/filepath>");
			System.out.println("sample command is:create myalias -value password123 -provider jceks://file/tmp/ks/myks.jceks");
		}
		if(command!=null && command.trim().equalsIgnoreCase("list")){
			System.out.println("Correct syntax is:list -provider <jceks://file/filepath>");
			System.out.println("sample command is:list -provider jceks://file/tmp/ks/myks.jceks");
		}
		if(command!=null && command.trim().equalsIgnoreCase("get")){
			System.out.println("Correct syntax is:get <aliasname> -provider <jceks://file/filepath>");
			System.out.println("sample command is:get myalias -provider jceks://file/tmp/ks/myks.jceks");
		}
	}
	public String getCredential(String args[]){
		String command=null;
		String alias=null;
		String providerOption=null;
		String providerPath=null;
		String tempCredential=null;
		try{
			if(args!=null && args.length==4){
				command=args[0];
				alias=args[1];
				providerOption=args[2];
				providerPath=args[3];
				if(!isValidGetCommand(command,alias,providerOption,providerPath)){
					displaySyntax("get");
				}else{
					tempCredential=CredentialReader.getDecryptedString(providerPath, alias);
				}
			}else{
				displaySyntax("get");
			}
			if(tempCredential==null){
				System.out.println("Alias "+ alias +" does not exist!!");
			}
			}catch(Exception ex){
				ex.printStackTrace();
			}
			return tempCredential;
	}

	public static boolean isValidGetCommand(String command,String alias,String providerOption,String providerPath){
		boolean isValid=true;
		try{
			if(command==null || !"get".equalsIgnoreCase(command.trim())){
				System.out.println("Invalid get phrase in credential get command!!");
				System.out.println("Expected:'get' Found:'"+command+"'");
				displaySyntax("get");
				return false;
			}
			if(alias==null || "".equalsIgnoreCase(alias.trim()))
			{
				System.out.println("Invalid alias name phrase in credential get command!!");
				System.out.println("Found:'"+alias+"'");
				displaySyntax("get");
				return false;
			}
			if(providerOption==null || !"-provider".equalsIgnoreCase(providerOption.trim()))
			{
				System.out.println("Invalid provider option in credential get command!!");
				System.out.println("Expected:'-provider' Found:'"+providerOption+"'");
				displaySyntax("get");
				return false;
			}
			if(providerPath==null || "".equalsIgnoreCase(providerPath.trim()) || (!providerPath.startsWith("localjceks://") && !providerPath.startsWith("jceks://")))
			{
				System.out.println("Invalid provider option in credential get command!!");
				System.out.println("Found:'"+providerPath+"'");
				displaySyntax("get");
				return false;
			}
		}catch(Exception ex){
			System.out.println("Invalid input or runtime error! Please try again.");
			System.out.println("Input:"+command+" "+alias+" "+providerOption+" "+providerPath);
			displaySyntax("get");
			ex.printStackTrace();
			return false;
		}
		return isValid;
	}

	private static boolean isCredentialShellInteractiveEnabled() {
		boolean ret = false;
		
		String fieldName = "interactive";
		
		CredentialShell cs = new CredentialShell();
		
		try {
			Field interactiveField = cs.getClass().getDeclaredField(fieldName);
			
			if (interactiveField != null) {
				interactiveField.setAccessible(true);
				ret = interactiveField.getBoolean(cs);
				System.out.println("FOUND value of [" + fieldName + "] field in the Class [" + cs.getClass().getName() + "] = [" + ret + "]");
			}
		} catch (Throwable e) {
			System.out.println("Unable to find the value of [" + fieldName + "] field in the Class [" + cs.getClass().getName() + "]. Skiping -f option");
			e.printStackTrace();
			ret = false;
		}
		
		return ret;
		
	}

	public void deleteInvalidKeystore(String providerPath){
		if(providerPath!=null){
			String keystore=null;
			if(providerPath.startsWith("jceks://file")){
				keystore=providerPath.replace("jceks://file","");
			}else if(providerPath.startsWith("localjceks://file")){
				keystore=providerPath.replace("jceks://file","");
			}else{
				keystore=providerPath;
			}
			if(keystore!=null && !keystore.isEmpty()){
				File file =new File(keystore);
				if(file!=null && file.exists() && file.length()==0){
					System.out.println("Provider file '"+keystore+"' is in invalid state or corrupt!! will try to delete first.");
					file.delete();
					file=null;
				}
			}
		}
	}
}
