package com.xasecure.authentication;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.List;

import org.apache.log4j.Logger;

public class PasswordValidator implements Runnable {

	private static final Logger LOG = Logger.getLogger(PasswordValidator.class) ;
	
	private static String validatorProgram = null ;

	private static List<String> adminUserList ;

	private static String adminRoleNames ;

	private Socket client ;
	
	public PasswordValidator(Socket client) {
		this.client = client ;
	}

	@Override
	public void run() {
		BufferedReader reader = null ;
		PrintWriter writer = null;

		String userName = null ;

		try {
			reader = new BufferedReader(new InputStreamReader(client.getInputStream())) ;
			writer = new PrintWriter(new OutputStreamWriter(client.getOutputStream())) ;
			String request = reader.readLine() ;
			
			if (request.startsWith("LOGIN:")) {
				String line = request.substring(6).trim() ;
				int passwordAt = line.indexOf(' ') ;
				if (passwordAt != -1) {
					userName = line.substring(0,passwordAt).trim() ;
				}
			}

			if (validatorProgram == null) {
				String res = "FAILED: Unable to validate credentials." ;
				writer.println(res) ;
				writer.flush(); 
				LOG.error("Response [" + res + "] for user: " + userName + " as ValidatorProgram is not defined in configuration.") ;

			}
			else {
				
				BufferedReader pReader = null ;
				PrintWriter pWriter = null;
				Process p =  null;
				
				try {
					p = Runtime.getRuntime().exec(validatorProgram) ;
					
					pReader = new BufferedReader(new InputStreamReader(p.getInputStream())) ;
					
					pWriter = new PrintWriter(new OutputStreamWriter(p.getOutputStream())) ;
					
					pWriter.println(request) ; pWriter.flush(); 
	
					String res = pReader.readLine() ;


					if (res != null && res.startsWith("OK")) {
						if (adminRoleNames != null && adminUserList != null) {
							if (adminUserList.contains(userName)) {
								res = res + " " + adminRoleNames ;
							}
						}
					}

					LOG.info("Response [" + res + "] for user: " + userName);
					
					writer.println(res) ; writer.flush(); 
				}
				finally {
					if (p != null) {
						p.destroy();
					}
				}
			}
			
		}
		catch(Throwable t) {
			if (writer != null){
				String res = "FAILED: unable to validate due to error " + t ;
				writer.println(res) ;
				LOG.error("Response [" + res + "] for user: " + userName, t);

			}
		}
		finally {
			try {
				if (client != null) {
					client.close(); 
				}
			}
			catch(IOException ioe){
				// Ignore exception
			}
			finally {
				client = null;
			}
		}
	}
	
	
	public static String getValidatorProgram() {
		return validatorProgram;
	}

	public static void setValidatorProgram(String validatorProgram) {
		PasswordValidator.validatorProgram = validatorProgram;
	}

	public static List<String> getAdminUserList() {
		return adminUserList;
	}

	public static void setAdminUserList(List<String> adminUserList) {
		PasswordValidator.adminUserList = adminUserList;
	}

	public static String getAdminRoleNames() {
		return adminRoleNames;
	}

	public static void setAdminRoleNames(String adminRoleNames) {
		PasswordValidator.adminRoleNames = adminRoleNames;
	}

}
