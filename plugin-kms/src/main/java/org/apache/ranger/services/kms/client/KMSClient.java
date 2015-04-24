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

package org.apache.ranger.services.kms.client;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;
import org.apache.ranger.plugin.client.BaseClient;
import org.apache.ranger.plugin.client.HadoopException;
import org.apache.ranger.services.kms.client.KMSClient;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;

public class KMSClient {

	public static final Logger LOG = Logger.getLogger(KMSClient.class) ;

	private static final String EXPECTED_MIME_TYPE = "application/json";
	
	private static final String KMS_LIST_API_ENDPOINT = "v1/keys/names?user.name=${userName}";			//GET
	
	private static final String errMessage =  " You can still save the repository and start creating "
											  + "policies, but you would not be able to use autocomplete for "
											  + "resource names. Check xa_portal.log for more info.";
	
	String provider;
	String username;
	String password;

	public  KMSClient(String provider, String username, String password) {
		provider = provider.replaceAll("kms://","");
		provider = provider.replaceAll("http@","http://");		
		this.provider = provider;
		this.username = username ;
		this.password = password;
		
		if (LOG.isDebugEnabled()) {
			LOG.debug("Kms Client is build with url [" + provider + "] user: [" + username + "]");
		}		
	}
	
	public List<String> getKeyList(final String keyNameMatching, final List<String> existingKeyList) {
		
		if (LOG.isDebugEnabled()) {
			LOG.debug("Getting Kms Key list for keyNameMatching : " + keyNameMatching);
		}
		final String errMsg = errMessage;
		List<String> lret = new ArrayList<String>();				
		String keyLists = KMS_LIST_API_ENDPOINT.replaceAll(Pattern.quote("${userName}"), username);
		String uri = provider + (provider.endsWith("/") ? keyLists : ("/" + keyLists));		
		Client client = null ;
		ClientResponse response = null ;
				
		try {
			client = Client.create() ;
			
			WebResource webResource = client.resource(uri);
			
			response = webResource.accept(EXPECTED_MIME_TYPE).get(ClientResponse.class);
			
			if (LOG.isDebugEnabled()) {
				LOG.debug("getKeyList():calling " + uri);
			}
			
			if (response != null) {
				if (LOG.isDebugEnabled()) {
					LOG.debug("getKeyList():response.getStatus()= " + response.getStatus());	
				}
				if (response.getStatus() == 200) {
					String jsonString = response.getEntity(String.class);
					Gson gson = new GsonBuilder().setPrettyPrinting().create();
					@SuppressWarnings("unchecked")
					List<String> keys = gson.fromJson(jsonString, List.class) ;					
					if (keys != null) {
						for ( String key : keys) {
							if ( existingKeyList != null && existingKeyList.contains(key)) {
						        	continue;
						        }
								if (keyNameMatching == null || keyNameMatching.isEmpty() || key.startsWith(keyNameMatching)) {
										if (LOG.isDebugEnabled()) {
											LOG.debug("getKeyList():Adding kmsKey " + key);
										}
										lret.add(key) ;
									}
							}
						}							
				 }else if (response.getStatus() == 401) {
					 LOG.info("getKeyList():response.getStatus()= " + response.getStatus() + " for URL " + uri + ", so returning null list");
					 return lret;
				 }else if (response.getStatus() == 403) {
					 LOG.info("getKeyList():response.getStatus()= " + response.getStatus() + " for URL " + uri + ", so returning null list");
					 return lret;
				 }else {
					 LOG.info("getKeyList():response.getStatus()= " + response.getStatus() + " for URL " + uri + ", so returning null list");	
					 String jsonString = response.getEntity(String.class);
					 LOG.info(jsonString);
					 lret = null;
				}
			}else {
				String msgDesc = "Unable to get a valid response for "
						+ "expected mime type : [" + EXPECTED_MIME_TYPE
						+ "] URL : " + uri + " - got null response.";
				LOG.error(msgDesc);
				HadoopException hdpException = new HadoopException(msgDesc);
				hdpException.generateResponseDataMap(false, msgDesc, msgDesc + errMsg, null, null);
				lret = null;
				throw hdpException;
			}
		} catch (HadoopException he) {
			lret = null;
			throw he;
		}catch (Throwable t) {
			String msgDesc = "Exception while getting Kms Key List. URL : " + uri;
			HadoopException hdpException = new HadoopException(msgDesc, t);
			LOG.error(msgDesc, t);
			hdpException.generateResponseDataMap(false, BaseClient.getMessage(t), msgDesc + errMsg, null, null);
			lret = null;
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
		
	public static HashMap<String, Object> testConnection(String serviceName, Map<String, String> configs) {

		List<String> strList = new ArrayList<String>();
		String errMsg = errMessage;
		boolean connectivityStatus = false;
		HashMap<String, Object> responseData = new HashMap<String, Object>();

		KMSClient kmsClient = getKmsClient(serviceName, configs);
		strList = getKmsKey(kmsClient, "", null);
		if (strList != null) {
			connectivityStatus = true;
		}
		if (connectivityStatus) {
			String successMsg = "TestConnection Successful";
			BaseClient.generateResponseDataMap(connectivityStatus, successMsg,
					successMsg, null, null, responseData);
		} else {
			String failureMsg = "Unable to retrieve any Kms Key using given parameters.";
			BaseClient.generateResponseDataMap(connectivityStatus, failureMsg,
					failureMsg + errMsg, null, null, responseData);
		}

		return responseData;
	}

	public static KMSClient getKmsClient(String serviceName,
			Map<String, String> configs) {
		KMSClient kmsClient = null;
		if (LOG.isDebugEnabled()) {
			LOG.debug("Getting KmsClient for datasource: " + serviceName
					+ "configMap: " + configs);
		}
		String errMsg = errMessage;
		if (configs == null || configs.isEmpty()) {
			String msgDesc = "Could not connect as Connection ConfigMap is empty.";
			LOG.error(msgDesc);
			HadoopException hdpException = new HadoopException(msgDesc);
			hdpException.generateResponseDataMap(false, msgDesc, msgDesc
					+ errMsg, null, null);
			throw hdpException;
		} else {
			String kmsUrl 		= configs.get("provider");
			String kmsUserName = configs.get("username");
			String kmsPassWord = configs.get("password");
			kmsClient 			= new KMSClient (kmsUrl, kmsUserName,
										 		  kmsPassWord);
	
		}
		return kmsClient;
	}

	public static List<String> getKmsKey (final KMSClient kmsClient, String keyName, List<String> existingKeyName) {

		List<String> resultList = new ArrayList<String>();
		String errMsg = errMessage;

		try {
			if (kmsClient == null) {
				String msgDesc = "Unable to get Kms Key : KmsClient is null.";
				LOG.error(msgDesc);
				HadoopException hdpException = new HadoopException(msgDesc);
				hdpException.generateResponseDataMap(false, msgDesc, msgDesc
						+ errMsg, null, null);
				throw hdpException;
			}

			if (keyName != null) {
				String finalkmsKeyName = keyName.trim();
				resultList = kmsClient.getKeyList(finalkmsKeyName,existingKeyName);
				if (resultList != null) {
					if (LOG.isDebugEnabled()) {
						LOG.debug("Returning list of " + resultList.size() + " Kms Keys");
					}
				}
			}
		} catch (HadoopException he) {
			resultList = null;
			throw he;
		} catch (Exception e) {
			String msgDesc = "Unable to get a valid response from the provider";
			LOG.error(msgDesc, e);
			HadoopException hdpException = new HadoopException(msgDesc);
			hdpException.generateResponseDataMap(false, msgDesc, msgDesc + errMsg, null, null);
			resultList = null;
			throw hdpException;
		}
		return resultList;
	}	
}
