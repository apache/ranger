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
			LOG.debug("Storm Client is build with url [" + aStormUIUrl + "] user: [" + aUserName + "], password: [" + aPassword + "]");
		}

	}

	public List<String> getTopologyList(final String topologyNameMatching) {
		
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
					
					if (response != null) {
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
							
						}
					}
				}
				finally {
					
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
				kerberosOptions.put("debug", "false");
				kerberosOptions.put("useKeyTab", "false");
				kerberosOptions.put(KrbPasswordSaverLoginModule.USERNAME_PARAM, this.userName);
				kerberosOptions.put(KrbPasswordSaverLoginModule.PASSWORD_PARAM, this.password);
				kerberosOptions.put("doNotPrompt", "true");
				kerberosOptions.put("useFirstPass", "true");
				kerberosOptions.put("tryFirstPass","false") ;
				kerberosOptions.put("storeKey", "true");
				kerberosOptions.put("refreshKrb5Config", "true");



				AppConfigurationEntry KEYTAB_KERBEROS_LOGIN = new AppConfigurationEntry(
						KerberosUtil.getKrb5LoginModuleName(),
						AppConfigurationEntry.LoginModuleControlFlag.REQUIRED, kerberosOptions);
				return new AppConfigurationEntry[] { KEYTAB_KERBEROS_LOGIN };
			}

		}
		;

		T ret = null;

		Subject subject = null;
		LoginContext loginContext = null;

		try {
			subject = new Subject();
			MySecureClientLoginConfiguration loginConf = new MySecureClientLoginConfiguration(
					userName, password);
			loginContext = new LoginContext("hadoop-keytab-kerberos", subject,
					null, loginConf);
			loginContext.login();

			Subject loginSubj = loginContext.getSubject();

			if (loginSubj != null) {
				ret = Subject.doAs(loginSubj, action);
			}
		} catch (LoginException le) {
			throw new IOException("Login failure", le);
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
}
