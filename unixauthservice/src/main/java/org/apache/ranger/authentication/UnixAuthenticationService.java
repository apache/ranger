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

 package org.apache.ranger.authentication;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.KeyStore;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLServerSocket;
import javax.net.ssl.SSLServerSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.log4j.Logger;
import org.apache.ranger.credentialapi.CredentialReader;
import org.apache.ranger.usergroupsync.UserGroupSync;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

public class UnixAuthenticationService {

	private static final Logger LOG = Logger.getLogger(UnixAuthenticationService.class);
	
	private static final String serviceName = "UnixAuthenticationService";
	
	private static final String SSL_ALGORITHM = "TLS";
	private static final String REMOTE_LOGIN_AUTH_SERVICE_PORT_PARAM = "ranger.usersync.port";
	
	private static final String SSL_KEYSTORE_PATH_PARAM = "ranger.usersync.keystore.file";
	private static final String SSL_TRUSTSTORE_PATH_PARAM = "ranger.usersync.truststore.file";
	
	private static final String SSL_KEYSTORE_PATH_PASSWORD_ALIAS = "usersync.ssl.key.password";
	private static final String SSL_TRUSTSTORE_PATH_PASSWORD_ALIAS = "usersync.ssl.truststore.password";

	private static final String CRED_VALIDATOR_PROG = "ranger.usersync.passwordvalidator.path";
	private static final String ADMIN_USER_LIST_PARAM = "admin.users";
	private static final String ADMIN_ROLE_LIST_PARAM = "admin.roleNames";
	private static final String SSL_ENABLED_PARAM = "ranger.usersync.ssl";
	
	private static final String CREDSTORE_FILENAME_PARAM = "ranger.usersync.credstore.filename";
	
	private String keyStorePath;
	private String keyStorePathPassword;
	private String trustStorePath;
	private String trustStorePathPassword;
	private List<String>  adminUserList = new ArrayList<String>();
	private String adminRoleNames;
	
	private int  portNum;
	
	private boolean SSLEnabled = false;
	
	static private boolean enableUnixAuth = false;
	
	private static final String[] UGSYNC_CONFIG_XML_FILES = { "ranger-ugsync-default.xml",  "ranger-ugsync-site.xml" };
	private static final String    PROPERTY_ELEMENT_TAGNAME = "property";
	private static final String    NAME_ELEMENT_TAGNAME = "name";
	private static final String    VALUE_ELEMENT_TAGNAME = "value";

	public static void main(String[] args) {
		if (args.length > 0) {
			for (String s : args) {
				if ("-enableUnixAuth".equalsIgnoreCase(s)) {
					enableUnixAuth = true;
					break;
				}
			}
		}
		UnixAuthenticationService service = new UnixAuthenticationService();
		service.run();
	}

	public UnixAuthenticationService() {
	}

	
	public void run() {
		try {
			LOG.info("Starting User Sync Service!");
			startUnixUserGroupSyncProcess();
			Thread.sleep(5000);
			if (enableUnixAuth) {
				LOG.info("Enabling Unix Auth Service!");
			    init();
			    startService();
			} else {
				LOG.info("Unix Auth Service Disabled!");
			}
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
		UserGroupSync syncProc = new UserGroupSync();
		Thread newSyncProcThread = new Thread(syncProc);
		newSyncProcThread.setName("UnixUserSyncThread");
		// If this thread is set as daemon, then the entire process will terminate if enableUnixAuth is false
        // Therefore this is marked as non-daemon thread. Don't change the following line
		newSyncProcThread.setDaemon(false);
		newSyncProcThread.start();
	}
	

	//TODO: add more validation code
	private void init() throws Throwable {
		Properties prop = new Properties();
		
		for (String fn : UGSYNC_CONFIG_XML_FILES ) {
		
			InputStream in = getFileInputStream(fn);
	
			if (in != null) {
				try {
					DocumentBuilderFactory xmlDocumentBuilderFactory = DocumentBuilderFactory.newInstance();
					xmlDocumentBuilderFactory.setFeature("http://xml.org/sax/features/external-general-entities", false);
                        		xmlDocumentBuilderFactory.setFeature("http://xml.org/sax/features/external-parameter-entities", false);
					xmlDocumentBuilderFactory.setIgnoringComments(true);
					xmlDocumentBuilderFactory.setNamespaceAware(true);
					DocumentBuilder xmlDocumentBuilder = xmlDocumentBuilderFactory.newDocumentBuilder();
					Document xmlDocument = xmlDocumentBuilder.parse(in);
					xmlDocument.getDocumentElement().normalize();
	
					NodeList nList = xmlDocument.getElementsByTagName(PROPERTY_ELEMENT_TAGNAME);
	
					for (int temp = 0; temp < nList.getLength(); temp++) {
	
						Node nNode = nList.item(temp);
	
						if (nNode.getNodeType() == Node.ELEMENT_NODE) {
	
							Element eElement = (Element) nNode;
	
							String propertyName = "";
							String propertyValue = "";
							if (eElement.getElementsByTagName(NAME_ELEMENT_TAGNAME).item(
									0) != null) {
								propertyName = eElement
										.getElementsByTagName(NAME_ELEMENT_TAGNAME)
										.item(0).getTextContent().trim();
							}
							if (eElement.getElementsByTagName(VALUE_ELEMENT_TAGNAME)
									.item(0) != null) {
								propertyValue = eElement
										.getElementsByTagName(VALUE_ELEMENT_TAGNAME)
										.item(0).getTextContent().trim();
							}
	
							//LOG.info("Adding Property:[" + propertyName + "] Value:["+ propertyValue + "]");
							if (prop.get(propertyName) != null ) {
								prop.remove(propertyName);
	 						}
							prop.put(propertyName, propertyValue);
						}
					}
				}
				finally {
					try {
						in.close();
					}
					catch(IOException ioe) {
						LOG.debug("Close streams failure. Detail: \n", ioe);
					}
				}
			}
		}
		
		String credStoreFileName = prop.getProperty(CREDSTORE_FILENAME_PARAM);
		
		keyStorePath = prop.getProperty(SSL_KEYSTORE_PATH_PARAM);
		
		if (credStoreFileName == null) {
			throw new RuntimeException("Credential file is not defined. param = [" + CREDSTORE_FILENAME_PARAM + "]");
		}
		
		File credFile = new File(credStoreFileName);
		
		if (! credFile.exists()) {
			throw new RuntimeException("Credential file [" + credStoreFileName + "]: does not exists." );
		}
		
		if ( ! credFile.canRead() ) {
			throw new RuntimeException("Credential file [" + credStoreFileName + "]: can not be read." );
		}
		
		keyStorePathPassword = CredentialReader.getDecryptedString(credStoreFileName, SSL_KEYSTORE_PATH_PASSWORD_ALIAS);
		trustStorePathPassword = CredentialReader.getDecryptedString(credStoreFileName,SSL_TRUSTSTORE_PATH_PASSWORD_ALIAS);
		
		trustStorePath  = prop.getProperty(SSL_TRUSTSTORE_PATH_PARAM);
		portNum = Integer.parseInt(prop.getProperty(REMOTE_LOGIN_AUTH_SERVICE_PORT_PARAM));
		String validatorProg = prop.getProperty(CRED_VALIDATOR_PROG);
		if (validatorProg != null) {
			PasswordValidator.setValidatorProgram(validatorProg);
		}
		
		String adminUsers = prop.getProperty(ADMIN_USER_LIST_PARAM);
		
		if (adminUsers != null && adminUsers.trim().length() > 0) {
			for(String u : adminUsers.split(",")) {
				LOG.info("Adding Admin User:"  + u.trim());
				adminUserList.add(u.trim());
			}
			PasswordValidator.setAdminUserList(adminUserList);
		}
		
		
		adminRoleNames = prop.getProperty(ADMIN_ROLE_LIST_PARAM);
		
		if (adminRoleNames != null) {
			LOG.info("Adding Admin Group:" + adminRoleNames);
			PasswordValidator.setAdminRoleNames(adminRoleNames);
		}
		
		String SSLEnabledProp = prop.getProperty(SSL_ENABLED_PARAM);
		
		SSLEnabled = (SSLEnabledProp != null &&  (SSLEnabledProp.equalsIgnoreCase("true")));
		
//		LOG.info("Key:" + keyStorePath);
//		LOG.info("KeyPassword:" + keyStorePathPassword);
//		LOG.info("TrustStore:" + trustStorePath);
//		LOG.info("PortNum:" + portNum);
//		LOG.info("ValidatorProg:" + validatorProg);
		
	}
	
	
	public void startService() throws Throwable {
		
		SSLContext context =  SSLContext.getInstance(SSL_ALGORITHM);
		
		KeyManager[] km = null;

		if (keyStorePath != null && ! keyStorePath.isEmpty()) {
			KeyStore ks = KeyStore.getInstance(KeyStore.getDefaultType());
			
			InputStream in = null;
			
			in = getFileInputStream(keyStorePath);
			
			try {
				if (keyStorePathPassword == null) {
					keyStorePathPassword  = "";
				}
				ks.load(in, keyStorePathPassword.toCharArray());
			}
			finally {
				if (in != null) {
					in.close();
				}
			}
			
			KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
			kmf.init(ks, keyStorePathPassword.toCharArray());
			km = kmf.getKeyManagers();
		}
		
		
		TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
		
		KeyStore trustStoreKeyStore = null;
		
		if (trustStorePath != null && ! trustStorePath.isEmpty()) {
			trustStoreKeyStore = KeyStore.getInstance(KeyStore.getDefaultType());
			
			InputStream in = null;
			
			in = getFileInputStream(trustStorePath);
			
			try {
				if (trustStorePathPassword == null) {
					trustStorePathPassword = "";
				}
				trustStoreKeyStore.load(in, trustStorePathPassword.toCharArray());
			}
			finally {
				if (in != null) {
					in.close();
				}
			}
		}
		
		trustManagerFactory.init(trustStoreKeyStore);
		
		TrustManager[] tm = trustManagerFactory.getTrustManagers();
				
		SecureRandom random = new SecureRandom();
		
		context.init(km, tm, random);
		
		SSLServerSocketFactory sf = context.getServerSocketFactory();

		ServerSocket socket = (SSLEnabled ? sf.createServerSocket(portNum) :  new ServerSocket(portNum) );
		
		if (SSLEnabled) {
			SSLServerSocket secureSocket = (SSLServerSocket) socket;
			String[] protocols = secureSocket.getEnabledProtocols();
			Set<String> allowedProtocols = new HashSet<String>();
			for(String ep : protocols) {
				if (! ep.toUpperCase().startsWith("SSLV3")) {
					LOG.info("Enabling Protocol: [" + ep + "]");
					allowedProtocols.add(ep);
				}
				else {
					LOG.info("Disabling Protocol: [" + ep + "]");
				}
			}
			
			if (!allowedProtocols.isEmpty()) {
				secureSocket.setEnabledProtocols(allowedProtocols.toArray(new String[0]));
			}
		}
		
				
		Socket client = null;
		
		try {
		
			while ( (client = socket.accept()) != null ) {
				Thread clientValidatorThread = new Thread(new PasswordValidator(client));
				clientValidatorThread.start();
			}
		} catch (IOException e) {
			socket.close();
			throw(e);
		}

	}
	
	private InputStream getFileInputStream(String path) throws FileNotFoundException {
		
		InputStream ret = null;
		
		File f = new File(path);
		
		if (f.exists()) {
			ret = new FileInputStream(f);
		}
		else {
			ret = getClass().getResourceAsStream(path);
			if (ret == null) {
				ret = getClass().getResourceAsStream("/" + path);
			}
		}
		
		return ret;
	}

}
