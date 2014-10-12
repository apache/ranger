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

 package com.xasecure.hadoop.client.config;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import javax.security.auth.Subject;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.security.SecureClientLogin;

import com.xasecure.hadoop.client.HadoopFS;
import com.xasecure.hadoop.client.exceptions.HadoopException;

public abstract class BaseClient {
	private static final Log LOG = LogFactory.getLog(HadoopFS.class) ;
	
	private String dataSource ;
	private Subject loginSubject ;
	private HadoopConfigHolder configHolder;
	
	protected HashMap<String,String> connectionProperties ;
	
	public BaseClient(String dataSource) {
		this.dataSource = dataSource ;
		init() ;
		login() ;
	}
	
	public BaseClient(String dataSource, HashMap<String,String> connectionProperties) {
		this.dataSource = dataSource ;
		this.connectionProperties = connectionProperties ;
		init() ;
		login() ;
	}
	
	
	private void init() {
		if (connectionProperties == null) {
			configHolder = HadoopConfigHolder.getInstance(dataSource) ;
		}
		else {
			configHolder = HadoopConfigHolder.getInstance(dataSource,connectionProperties) ;
		}
	}
	
	
	protected void login() {
		ClassLoader prevCl = Thread.currentThread().getContextClassLoader() ;
		String errMsg = " You can still save the repository and start creating "
				+ "policies, but you would not be able to use autocomplete for "
				+ "resource names. Check xa_portal.log for more info.";
		try {
			Thread.currentThread().setContextClassLoader(configHolder.getClassLoader());
			String userName = configHolder.getUserName() ;
			if (userName == null) {
				String msgDesc = "Unable to find login username for hadoop environment, ["
						+ dataSource + "]";
				HadoopException hdpException = new HadoopException(msgDesc);
				hdpException.generateResponseDataMap(false, msgDesc, msgDesc + errMsg,
						null, null);

				throw hdpException;
			}
			String keyTabFile = configHolder.getKeyTabFile() ;
			if (keyTabFile != null) {
				if ( configHolder.isKerberosAuthentication() ) {
					LOG.info("Init Login: security enabled, using username/keytab");
					loginSubject = SecureClientLogin.loginUserFromKeytab(userName, keyTabFile) ;
				}
				else {
					LOG.info("Init Login: using username");
					loginSubject = SecureClientLogin.login(userName) ;
				}
			}
			else {
				String password = configHolder.getPassword() ;
				if ( configHolder.isKerberosAuthentication() ) {
					LOG.info("Init Login: using username/password");
					loginSubject = SecureClientLogin.loginUserWithPassword(userName, password) ;
				}
				else {
					LOG.info("Init Login: security not enabled, using username");
					loginSubject = SecureClientLogin.login(userName) ;
				}
			}
		} catch (IOException ioe) {
			String msgDesc = "Unable to login to Hadoop environment ["
					+ dataSource + "]";

			HadoopException hdpException = new HadoopException(msgDesc, ioe);
			hdpException.generateResponseDataMap(false, getMessage(ioe),
					msgDesc + errMsg, null, null);
			throw hdpException;
		} catch (SecurityException se) {
			String msgDesc = "Unable to login to Hadoop environment ["
					+ dataSource + "]";
			HadoopException hdpException = new HadoopException(msgDesc, se);
			hdpException.generateResponseDataMap(false, getMessage(se),
					msgDesc + errMsg, null, null);
			throw hdpException;
		} finally {
			Thread.currentThread().setContextClassLoader(prevCl);
		}
	}
	
	public String getDataSource() {
		return dataSource ;
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
	
}
