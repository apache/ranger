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

 package com.xasecure.authentication;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.KeyStore;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLServerSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;

import org.apache.log4j.Logger;

import com.xasecure.usergroupsync.UserGroupSync;

public class UnixAuthenticationService {

	private static final Logger LOG = Logger.getLogger(UnixAuthenticationService.class) ;
	
	private static final String serviceName = "UnixAuthenticationService" ;
	
	private static final String SSL_ALGORITHM = "SSLv3" ;
	private static final String REMOTE_LOGIN_AUTH_SERVICE_PORT_PARAM = "authServicePort" ;
	private static final String SSL_KEYSTORE_PATH_PARAM = "keyStore" ;
	private static final String SSL_KEYSTORE_PATH_PASSWORD_PARAM = "keyStorePassword" ;
	private static final String SSL_TRUSTSTORE_PATH_PARAM = "trustStore" ;
	private static final String SSL_TRUSTSTORE_PATH_PASSWORD_PARAM = "trustStorePassword" ;
	private static final String CRED_VALIDATOR_PROG = "passwordValidatorPath" ;
	private static final String ADMIN_USER_LIST_PARAM = "admin.users" ;
	private static final String ADMIN_ROLE_LIST_PARAM = "admin.roleNames" ;
	private static final String SSL_ENABLED_PARAM = "useSSL" ;
	
	


	private String keyStorePath ;
	private String keyStorePathPassword ;
	private String trustStorePath ;
	private String trustStorePathPassword ;
	private List<String>  adminUserList = new ArrayList<String>() ;
	private String adminRoleNames ;
	
	private int  portNum ;
	
	private boolean SSLEnabled = false ;
	

	public static void main(String[] args) {
		UnixAuthenticationService service = new UnixAuthenticationService() ;
		service.run() ;
	}

	public UnixAuthenticationService() {
	}

	
	public void run() {
		try {
			startUnixUserGroupSyncProcess() ;
			init() ;
			startService() ;
		}
		catch(Throwable t) {
			LOG.error("ERROR: Service: " + serviceName , t);
		}
		finally {
			LOG.info("Service: " + serviceName + " - STOPPED.");
		}
	}
	
	private void startUnixUserGroupSyncProcess() {
		//
		//  Start the synchronization service ...
		//
		UserGroupSync syncProc = new UserGroupSync() ;
		Thread newSyncProcThread = new Thread(syncProc) ;
		newSyncProcThread.setName("UnixUserSyncThread");
		newSyncProcThread.setDaemon(false);
		newSyncProcThread.start(); 
	}
	

	//TODO: add more validation code
	private void init() throws Throwable {
		InputStream in = getFileInputStream("unixauthservice.properties") ;
		Properties prop = new Properties() ;
		prop.load(in);
		keyStorePath = prop.getProperty(SSL_KEYSTORE_PATH_PARAM) ;
		keyStorePathPassword = prop.getProperty(SSL_KEYSTORE_PATH_PASSWORD_PARAM) ;
		trustStorePath  = prop.getProperty(SSL_TRUSTSTORE_PATH_PARAM) ;
		trustStorePathPassword = prop.getProperty(SSL_TRUSTSTORE_PATH_PASSWORD_PARAM) ;
		portNum = Integer.parseInt(prop.getProperty(REMOTE_LOGIN_AUTH_SERVICE_PORT_PARAM)) ;
		String validatorProg = prop.getProperty(CRED_VALIDATOR_PROG) ;
		if (validatorProg != null) {
			PasswordValidator.setValidatorProgram(validatorProg);
		}
		
		String adminUsers = prop.getProperty(ADMIN_USER_LIST_PARAM) ; 
		
		if (adminUsers != null && adminUsers.trim().length() > 0) {
			for(String u : adminUsers.split(",")) {
				LOG.info("Adding Admin User:"  + u.trim());
				adminUserList.add(u.trim()) ;
			}
			PasswordValidator.setAdminUserList(adminUserList);
		}
		
		
		adminRoleNames = prop.getProperty(ADMIN_ROLE_LIST_PARAM) ;
		
		if (adminRoleNames != null) {
			LOG.info("Adding Admin Group:" + adminRoleNames);
			PasswordValidator.setAdminRoleNames(adminRoleNames) ;
		}
		
		String SSLEnabledProp = prop.getProperty(SSL_ENABLED_PARAM) ;
		
		SSLEnabled = (SSLEnabledProp != null &&  (SSLEnabledProp.equalsIgnoreCase("true"))) ;
		
//		LOG.info("Key:" + keyStorePath);
//		LOG.info("KeyPassword:" + keyStorePathPassword);
//		LOG.info("TrustStore:" + trustStorePath);
//		LOG.info("TrustStorePassword:" + trustStorePathPassword);
//		LOG.info("PortNum:" + portNum);
//		LOG.info("ValidatorProg:" + validatorProg);
		
	}
	
	
	public void startService() throws Throwable {
		
		SSLContext context =  SSLContext.getInstance(SSL_ALGORITHM) ;
		
		KeyManager[] km = null ;

		if (keyStorePath != null) {
			KeyStore ks = KeyStore.getInstance(KeyStore.getDefaultType()) ;
			
			InputStream in = null ;
			
			in = getFileInputStream(keyStorePath) ;
			
			try {
				ks.load(in, keyStorePathPassword.toCharArray());
			}
			finally {
				if (in != null) {
					in.close(); 
				}
			}
			
			KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm()) ;
			kmf.init(ks, keyStorePathPassword.toCharArray());
			km = kmf.getKeyManagers() ;
		}
		
		
		TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());  
		
		KeyStore trustStoreKeyStore = null ;
		
		if (trustStorePath != null) {
			trustStoreKeyStore = KeyStore.getInstance(KeyStore.getDefaultType()) ;
			
			InputStream in = null ;
			
			in = getFileInputStream(trustStorePath) ;
			
			try {
				trustStoreKeyStore.load(in, trustStorePathPassword.toCharArray());
			}
			finally {
				if (in != null) {
					in.close(); 
				}
			}
		}
		
		trustManagerFactory.init(trustStoreKeyStore);  
		
		TrustManager[] tm = trustManagerFactory.getTrustManagers() ;
				
		SecureRandom random = new SecureRandom() ;
		
		context.init(km, tm, random);
		
		SSLServerSocketFactory sf = context.getServerSocketFactory() ; 

		ServerSocket socket = (SSLEnabled ? sf.createServerSocket(portNum) :  new ServerSocket(portNum) ) ;
				
		Socket client = null ;
		
		while ( (client = socket.accept()) != null ) {
			Thread clientValidatorThread = new Thread(new PasswordValidator(client)) ;
			clientValidatorThread.start(); 
		}

	}
	
	private InputStream getFileInputStream(String path) throws FileNotFoundException {
		
		InputStream ret = null;
		
		File f = new File(path) ;
		
		if (f.exists()) {
			ret = new FileInputStream(f) ;
		}
		else {
			ret = getClass().getResourceAsStream(path) ;
			if (ret == null) {
				ret = getClass().getResourceAsStream("/" + path) ;
			}
		}
		
		return ret ;
	}


}
