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

 package org.apache.ranger.plugin.client;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.security.auth.Subject;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.security.SecureClientLogin;
import org.apache.ranger.plugin.util.PasswordUtils;

public abstract class BaseClient {
	private static final Log LOG = LogFactory.getLog(BaseClient.class) ;


	private static final String DEFAULT_NAME_RULE = "DEFAULT";


	private String serviceName ;
  	private String defaultConfigFile ;
	private Subject loginSubject ;
	private HadoopConfigHolder configHolder;
	
	protected Map<String,String> connectionProperties ;
	
  public BaseClient(String svcName, Map<String,String> connectionProperties) {
    this(svcName, connectionProperties, null);
  }

	public BaseClient(String serivceName, Map<String,String> connectionProperties, String defaultConfigFile) {
		this.serviceName = serivceName ;
		this.connectionProperties = connectionProperties ;
		this.defaultConfigFile = defaultConfigFile ;
		init() ;
		login() ;
	}
	
	
	private void init() {
		if (connectionProperties == null) {
			configHolder = HadoopConfigHolder.getInstance(serviceName) ;
		}
		else {
			configHolder = HadoopConfigHolder.getInstance(serviceName,connectionProperties, defaultConfigFile) ;
		}
	}
	
	
	protected void login() {
		ClassLoader prevCl = Thread.currentThread().getContextClassLoader() ;
		String errMsg = " You can still save the repository and start creating "
				+ "policies, but you would not be able to use autocomplete for "
				+ "resource names. Check ranger_admin.log for more info.";
		try {
			//Thread.currentThread().setContextClassLoader(configHolder.getClassLoader());
			 String lookupPrincipal = SecureClientLogin.getPrincipal(configHolder.getLookupPrincipal(), java.net.InetAddress.getLocalHost().getCanonicalHostName());
			 String lookupKeytab = configHolder.getLookupKeytab();
			 String nameRules = configHolder.getNameRules();
			 if(StringUtils.isEmpty(nameRules)){
				 if(LOG.isDebugEnabled()){
					 LOG.debug("Name is empty. Setting Name Rule as 'DEFAULT'");
				 }
				 nameRules = DEFAULT_NAME_RULE;
			 }
			 String userName = configHolder.getUserName() ;
			 if(StringUtils.isEmpty(lookupPrincipal) || StringUtils.isEmpty(lookupKeytab)){				
				 if (userName == null) {
					 String msgDesc = "Unable to find login username for hadoop environment, ["
						+ serviceName + "]";
					 HadoopException hdpException = new HadoopException(msgDesc);
					 hdpException.generateResponseDataMap(false, msgDesc + errMsg, msgDesc + errMsg,
						null, null);

					 throw hdpException;
				 }
				 String keyTabFile = configHolder.getKeyTabFile() ;
				 if (keyTabFile != null) {
					 if ( configHolder.isKerberosAuthentication() ) {
						 LOG.info("Init Login: security enabled, using username/keytab");
						 loginSubject = SecureClientLogin.loginUserFromKeytab(userName, keyTabFile, nameRules) ;
					 }
					 else {
						 LOG.info("Init Login: using username");
						 loginSubject = SecureClientLogin.login(userName) ;
					 }
				 }
				 else {
					 String encryptedPwd = configHolder.getPassword() ;
					 String password = PasswordUtils.decryptPassword(encryptedPwd);
					 if ( configHolder.isKerberosAuthentication() ) {
						 LOG.info("Init Login: using username/password");
						 loginSubject = SecureClientLogin.loginUserWithPassword(userName, password) ;
					 }
					 else {
						 LOG.info("Init Login: security not enabled, using username");
						 loginSubject = SecureClientLogin.login(userName) ;
					 }
				 }
			 }else{
				 if ( configHolder.isKerberosAuthentication() ) {
					 LOG.info("Init Lookup Login: security enabled, using lookupPrincipal/lookupKeytab");
					 loginSubject = SecureClientLogin.loginUserFromKeytab(lookupPrincipal, lookupKeytab, nameRules) ;
				 }else{
					 LOG.info("Init Login: security not enabled, using username");
					 loginSubject = SecureClientLogin.login(userName);					 
				 }
			 }
		} catch (IOException ioe) {
			String msgDesc = "Unable to login to Hadoop environment ["
					+ serviceName + "]";

			HadoopException hdpException = new HadoopException(msgDesc, ioe);
			hdpException.generateResponseDataMap(false, getMessage(ioe) +  errMsg,
					msgDesc + errMsg, null, null);
			throw hdpException;
		} catch (SecurityException se) {
			String msgDesc = "Unable to login to Hadoop environment ["
					+ serviceName + "]";
			HadoopException hdpException = new HadoopException(msgDesc, se);
			hdpException.generateResponseDataMap(false, getMessage(se) +  errMsg,
					msgDesc + errMsg, null, null);
			throw hdpException;
		} finally {
			Thread.currentThread().setContextClassLoader(prevCl);
		}
	}
	
	public String getSerivceName() {
		return serviceName ;
	}

	protected Subject getLoginSubject() {
		return loginSubject;
	}

	protected HadoopConfigHolder getConfigHolder() {
		return configHolder;
	}
	
	public static void generateResponseDataMap(boolean connectivityStatus,
			String message, String description, Long objectId,
			String fieldName, HashMap<String, Object> responseData) {
		responseData.put("connectivityStatus", connectivityStatus);
		responseData.put("message", message);
		responseData.put("description", description);
		responseData.put("objectId", objectId);
		responseData.put("fieldName", fieldName);
	}

	public static String getMessage(Throwable excp) {
		List<String> errList = new ArrayList<String>();
		while (excp != null) {
			if (!errList.contains(excp.getMessage() + ". \n")) {
				if (excp.getMessage() != null && !(excp.getMessage().equalsIgnoreCase(""))) {
					errList.add(excp.getMessage() + ". \n");
				}
			}
			excp = excp.getCause();
		}
		return StringUtils.join(errList, "");
	}

	/*public static Map<String, String> getMaskedConfigMap(Map<String, String> configMap){
		Map<String, String> maskedMap=new HashMap<String, String>();
		maskedMap.putAll(configMap);
		if(maskedMap!=null && maskedMap.containsKey("password")){
			maskedMap.put("password", "*****");
		}
		return maskedMap;
	}*/
}
