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

package com.xasecure.storm.client;

import java.io.IOException;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.security.auth.Subject;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.AppConfigurationEntry.LoginModuleControlFlag;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;

import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.security.KrbPasswordSaverLoginModule;
import org.apache.hadoop.security.authentication.util.KerberosUtil;
import org.apache.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.xasecure.hadoop.client.config.BaseClient;
import com.xasecure.hadoop.client.exceptions.HadoopException;
import com.xasecure.storm.client.json.model.Topology;
import com.xasecure.storm.client.json.model.TopologyListResponse;

public class StormClient {
	
	public static final Logger LOG = Logger.getLogger(StormClient.class) ;

	private static final String EXPECTED_MIME_TYPE = "application/json";
	
	private static final String TOPOLOGY_LIST_API_ENDPOINT = "/api/v1/topology/summary" ;
	

	String stormUIUrl;
	String userName;
	String password;

	public StormClient(String aStormUIUrl, String aUserName, String aPassword) {
		
		this.stormUIUrl = aStormUIUrl;
		this.userName = aUserName ;
		this.password = aPassword;
		
		if (LOG.isDebugEnabled()) {
			LOG.debug("Storm Client is build with url [" + aStormUIUrl + "] user: [" + aUserName + "], password: [" + "" + "]");
		}

	}

	public List<String> getTopologyList(final String topologyNameMatching) {
		
		LOG.debug("Getting Storm topology list for topologyNameMatching : " +
				topologyNameMatching);
		final String errMsg = " You can still save the repository and start creating "
				+ "policies, but you would not be able to use autocomplete for "
				+ "resource names. Check xa_portal.log for more info.";
		
		List<String> ret = new ArrayList<String>();
		
		PrivilegedAction<ArrayList<String>> topologyListGetter = new PrivilegedAction<ArrayList<String>>() {
			@Override
			public ArrayList<String> run() {
				
				ArrayList<String> lret = new ArrayList<String>();
				
				String url = stormUIUrl + TOPOLOGY_LIST_API_ENDPOINT ;
				
				Client client = null ;
				ClientResponse response = null ;
				
				try {
					client = Client.create() ;
					
					WebResource webResource = client.resource(url);
					
					response = webResource.accept(EXPECTED_MIME_TYPE)
						    .get(ClientResponse.class);
					
					LOG.info("getTopologyList():calling " + url);
					
					if (response != null) {
						LOG.info("getTopologyList():response.getStatus()= " + response.getStatus());	
						if (response.getStatus() == 200) {
							String jsonString = response.getEntity(String.class);
							Gson gson = new GsonBuilder().setPrettyPrinting().create();
							TopologyListResponse topologyListResponse = gson.fromJson(jsonString, TopologyListResponse.class);
							if (topologyListResponse != null) {
								if (topologyListResponse.getTopologyList() != null) {
									for(Topology topology : topologyListResponse.getTopologyList()) {
										String toplogyName = topology.getName() ;
										if (toplogyName != null) {
											if (topologyNameMatching == null || topologyNameMatching.isEmpty() || FilenameUtils.wildcardMatch(topology.getName(), topologyNameMatching)) {
												lret.add(toplogyName) ;
											}
										}
									}
								}
							}
						} else{
							LOG.info("getTopologyList():response.getStatus()= " + response.getStatus() + " for URL " + url);	
							String jsonString = response.getEntity(String.class);
							LOG.info(jsonString);
						}
					} else {
						String msgDesc = "Unable to get a valid response for "
								+ "expected mime type : [" + EXPECTED_MIME_TYPE
								+ "] URL : " + url + " - got null response.";
						LOG.error(msgDesc);
						HadoopException hdpException = new HadoopException(msgDesc);
						hdpException.generateResponseDataMap(false, msgDesc,
								msgDesc + errMsg, null, null);
						throw hdpException;
					}
				} catch (HadoopException he) {
					throw he;
				} catch (Throwable t) {
					String msgDesc = "Exception while getting Storm TopologyList."
							+ " URL : " + url;
					HadoopException hdpException = new HadoopException(msgDesc,
							t);
					LOG.error(msgDesc, t);

					hdpException.generateResponseDataMap(false,
							BaseClient.getMessage(t), msgDesc + errMsg, null,
							null);
					throw hdpException;
					
				} finally {
					if (response != null) {
						response.close();
					}
					
					if (client != null) {
						client.destroy(); 
					}
				
				}
				return lret ;
			}
		} ;
		
		try {
			ret = executeUnderKerberos(this.userName, this.password, topologyListGetter) ;
		} catch (IOException e) {
			LOG.error("Unable to get Topology list from [" + stormUIUrl + "]", e) ;
		}
		
		return ret;
	}
	
	public static <T> T executeUnderKerberos(String userName, String password,
			PrivilegedAction<T> action) throws IOException {
		
		final String errMsg = " You can still save the repository and start creating "
				+ "policies, but you would not be able to use autocomplete for "
				+ "resource names. Check xa_portal.log for more info.";
		class MySecureClientLoginConfiguration extends
				javax.security.auth.login.Configuration {

			private String userName;
			private String password ;

			MySecureClientLoginConfiguration(String aUserName,
					String password) {
				this.userName = aUserName;
				this.password = password;
			}

			@Override
			public AppConfigurationEntry[] getAppConfigurationEntry(
					String appName) {

				Map<String, String> kerberosOptions = new HashMap<String, String>();
				kerberosOptions.put("principal", this.userName);
				kerberosOptions.put("debug", "true");
				kerberosOptions.put("useKeyTab", "false");
				kerberosOptions.put(KrbPasswordSaverLoginModule.USERNAME_PARAM, this.userName);
				kerberosOptions.put(KrbPasswordSaverLoginModule.PASSWORD_PARAM, this.password);
				kerberosOptions.put("doNotPrompt", "false");
				kerberosOptions.put("useFirstPass", "true");
				kerberosOptions.put("tryFirstPass", "false");
				kerberosOptions.put("storeKey", "true");
				kerberosOptions.put("refreshKrb5Config", "true");

				AppConfigurationEntry KEYTAB_KERBEROS_LOGIN = null;
				AppConfigurationEntry KERBEROS_PWD_SAVER = null;
				try {
					KEYTAB_KERBEROS_LOGIN = new AppConfigurationEntry(
							KerberosUtil.getKrb5LoginModuleName(),
							AppConfigurationEntry.LoginModuleControlFlag.REQUIRED,
							kerberosOptions);
					KERBEROS_PWD_SAVER = new AppConfigurationEntry(KrbPasswordSaverLoginModule.class.getName(), LoginModuleControlFlag.REQUIRED, kerberosOptions);

				} catch (IllegalArgumentException e) {
					String msgDesc = "executeUnderKerberos: Exception while getting Storm TopologyList.";
					HadoopException hdpException = new HadoopException(msgDesc,
							e);
					LOG.error(msgDesc, e);

					hdpException.generateResponseDataMap(false,
							BaseClient.getMessage(e), msgDesc + errMsg, null,
							null);
					throw hdpException;
				}
                
				LOG.info("getAppConfigurationEntry():" + kerberosOptions.get("principal"));
				
                return new AppConfigurationEntry[] { KERBEROS_PWD_SAVER, KEYTAB_KERBEROS_LOGIN };
			}

		};

		T ret = null;

		Subject subject = null;
		LoginContext loginContext = null;

		try {
		    subject = new Subject();
			LOG.info("executeUnderKerberos():user=" + userName + ",pass=");
			LOG.info("executeUnderKerberos():Creating config..");
			MySecureClientLoginConfiguration loginConf = new MySecureClientLoginConfiguration(
					userName, password);
			LOG.info("executeUnderKerberos():Creating Context..");
			loginContext = new LoginContext("hadoop-keytab-kerberos", subject,
					null, loginConf);
			
			LOG.info("executeUnderKerberos():Logging in..");
			loginContext.login();

			Subject loginSubj = loginContext.getSubject();

			if (loginSubj != null) {
				ret = Subject.doAs(loginSubj, action);
			}
		} catch (LoginException le) {
			String msgDesc = "executeUnderKerberos: Login failure using given"
					+ " configuration parameters, username : `" + userName + "`.";
			HadoopException hdpException = new HadoopException(msgDesc, le);
			LOG.error(msgDesc, le);

			hdpException.generateResponseDataMap(false,
					BaseClient.getMessage(le), msgDesc + errMsg, null, null);
			throw hdpException;
		} catch (SecurityException se) {
			String msgDesc = "executeUnderKerberos: Exception while getting Storm TopologyList.";
			HadoopException hdpException = new HadoopException(msgDesc, se);
			LOG.error(msgDesc, se);

			hdpException.generateResponseDataMap(false,
					BaseClient.getMessage(se), msgDesc + errMsg, null, null);
			throw hdpException;

		} finally {
			if (loginContext != null) {
				if (subject != null) {
					try {
						loginContext.logout();
					} catch (LoginException e) {
						throw new IOException("logout failure", e);
					}
				}
			}
		}

		return ret;
	}

	public static HashMap<String, Object> testConnection(String dataSource,
			HashMap<String, String> connectionProperties) {

		List<String> strList = new ArrayList<String>();
		String errMsg = " You can still save the repository and start creating "
				+ "policies, but you would not be able to use autocomplete for "
				+ "resource names. Check xa_portal.log for more info.";
		boolean connectivityStatus = false;
		HashMap<String, Object> responseData = new HashMap<String, Object>();

		StormClient stormClient = getStormClient(dataSource,
				connectionProperties);
		strList = getStormResources(stormClient, "");

		if (strList != null && (strList.size() != 0)) {
			connectivityStatus = true;
		}

		if (connectivityStatus) {
			String successMsg = "TestConnection Successful";
			BaseClient.generateResponseDataMap(connectivityStatus, successMsg,
					successMsg, null, null, responseData);
		} else {
			String failureMsg = "Unable to retrive any topologies using given parameters.";
			BaseClient.generateResponseDataMap(connectivityStatus, failureMsg,
					failureMsg + errMsg, null, null, responseData);
		}

		return responseData;
	}

	public static StormClient getStormClient(String dataSourceName,
			Map<String, String> configMap) {
		StormClient stormClient = null;
		LOG.debug("Getting StormClient for datasource: " + dataSourceName
				+ "configMap: " + configMap);
		String errMsg = " You can still save the repository and start creating "
				+ "policies, but you would not be able to use autocomplete for "
				+ "resource names. Check xa_portal.log for more info.";
		if (configMap == null || configMap.isEmpty()) {
			String msgDesc = "Could not connect as Connection ConfigMap is empty.";
			LOG.error(msgDesc);
			HadoopException hdpException = new HadoopException(msgDesc);
			hdpException.generateResponseDataMap(false, msgDesc, msgDesc
					+ errMsg, null, null);
			throw hdpException;
		} else {
			String stormUrl = configMap.get("nimbus.url");
			String stormAdminUser = configMap.get("username");
			String stormAdminPassword = configMap.get("password");
			stormClient = new StormClient(stormUrl, stormAdminUser,
					stormAdminPassword);
		}
		return stormClient;
	}

	public static List<String> getStormResources(final StormClient stormClient,
			String topologyName) {

		List<String> resultList = new ArrayList<String>();
		String errMsg = " You can still save the repository and start creating "
				+ "policies, but you would not be able to use autocomplete for "
				+ "resource names. Check xa_portal.log for more info.";

		try {
			if (stormClient == null) {
				String msgDesc = "Unable to get Storm resources: StormClient is null.";
				LOG.error(msgDesc);
				HadoopException hdpException = new HadoopException(msgDesc);
				hdpException.generateResponseDataMap(false, msgDesc, msgDesc
						+ errMsg, null, null);
				throw hdpException;
			}

			if (topologyName != null) {
				String finalTopologyNameMatching = (topologyName == null) ? ""
						: topologyName.trim();
				resultList = stormClient
						.getTopologyList(finalTopologyNameMatching);
			}
		} catch (HadoopException he) {
			throw he;
		} catch (Exception e) {
			String msgDesc = "getStormResources: Unable to get Storm resources.";
			LOG.error(msgDesc, e);
			HadoopException hdpException = new HadoopException(msgDesc);

			hdpException.generateResponseDataMap(false,
					BaseClient.getMessage(e), msgDesc + errMsg, null, null);
			throw hdpException;
		}
		return resultList;
	}
	
}
