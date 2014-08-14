package com.hortonworks.credentialapi;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.alias.CredentialShell;
import org.apache.hadoop.util.GenericOptionsParser;

public class buildks {
	public static void main(String[] args) {
		buildks buildksOBJ=new buildks();
		buildksOBJ.createCredential(args);
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
	    		if(!isValidInput(command,alias,valueOption,credential,providerOption,providerPath)){
	    			return returnCode;
	    		}	    		
	    		tempCredential=CredentialReader.getDecryptedString(providerPath, alias);
	    	}else{  
	    		return returnCode;
	    	}
	    	
	    	if(tempCredential==null){
	    		returnCode=createKeyStore(args);
	    	}else{
	    		try{
	    			System.out.println("Alias already exist!! will try to delete first.");
	    			String argsDelete[]=new String[4];
	    			argsDelete[0]="delete";
	    			argsDelete[1]=alias;
	    			argsDelete[2]=providerOption;
	    			argsDelete[3]=providerPath;
	    			returnCode=deleteCredential(argsDelete);
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
	    		if(!isValidInput(command,alias,valueOption,credential,providerOption,providerPath)){
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
			// int i = 0 ;
			//  for(String s : toolArgs) {
			//		System.out.println("TooArgs [" + i + "] = [" + s + "]") ;
		    //		i++ ;
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
    		if(providerPath!=null && !providerPath.trim().isEmpty() && !providerPath.startsWith("jceks://file"))
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
    		if(!isValidInput(command,alias,valueOption,credential,providerOption,providerPath)){
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
		try{	    		    	
	    	if(args!=null && args.length==3)
	    	{
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
	
	public int deleteCredential(String args[]){
		int returnCode=-1;
		try{	    		    	
	    	if(args!=null && args.length==4)
	    	{
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
	
	public static boolean isValidInput(String command,String alias,String valueOption,String credential,String providerOption,String providerPath)
    {
		boolean isValid=true;
		try{
        	if(command==null || !"create".equalsIgnoreCase(command.trim()))
        	{
        		System.out.println("Invalid create phrase in credential creation command!!");
        		System.out.println("Expected:'create' Found:'"+command+"'");
        		displaySyntax();
        		return false;
        	}
        	if(alias==null || "".equalsIgnoreCase(alias.trim()))
        	{
        		System.out.println("Invalid alias name phrase in credential creation command!!");
        		System.out.println("Found:'"+alias+"'");
        		displaySyntax();
        		return false;
        	}
        	if(valueOption==null || !"-value".equalsIgnoreCase(valueOption.trim()))
        	{
        		System.out.println("Invalid value option switch in credential creation command!!");
        		System.out.println("Expected:'-value' Found:'"+valueOption+"'");
        		displaySyntax();
        		return false;
        	}
        	if(valueOption==null || !"-value".equalsIgnoreCase(valueOption.trim()))
        	{
        		System.out.println("Invalid value option in credential creation command!!");
        		System.out.println("Expected:'-value' Found:'"+valueOption+"'");
        		displaySyntax();
        		return false;
        	}
        	if(credential==null)
        	{
        		System.out.println("Invalid credential value in credential creation command!!");
        		System.out.println("Found:"+credential);
        		displaySyntax();
        		return false;
        	}
        	if(providerOption==null || !"-provider".equalsIgnoreCase(providerOption.trim()))
        	{
        		System.out.println("Invalid provider option in credential creation command!!");
        		System.out.println("Expected:'-provider' Found:'"+providerOption+"'");
        		displaySyntax();
        		return false;
        	}
        	if(providerPath==null || "".equalsIgnoreCase(providerPath.trim()) || !providerPath.startsWith("jceks://"))
        	{
        		System.out.println("Invalid provider option in credential creation command!!");
        		System.out.println("Found:'"+providerPath+"'");
        		displaySyntax();
        		return false;
        	}
    	}catch(Exception ex){    	
    		System.out.println("Invalid input or runtime error! Please try again.");
    		System.out.println("Input:"+command+" "+alias+" "+valueOption+" "+credential+" "+providerOption+" "+providerPath);
    		displaySyntax();
    		ex.printStackTrace();
    		return false;
    	}            	
    	return isValid;
    }
	
	public static void displayCommand(String args[])
    {
		StringBuffer tempBuffer=new StringBuffer("");
		if(args!=null && args.length>0){
			for(int index=0;index<args.length;index++){
				tempBuffer.append(args[index]+" ");
			}
			System.out.println("Command to execute:["+tempBuffer+"]");
		}
		
    }
	
	public static void displaySyntax()
    {
		System.out.println("Correct syntax is:create <aliasname> -value <password> -provider <jceks://file/filepath>");
		System.out.println("sample command is:create myalias -value password123 -provider jceks://file/tmp/ks/myks.jceks");	            		 
	}

}
