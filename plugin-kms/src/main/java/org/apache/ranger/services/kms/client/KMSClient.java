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
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.apache.ranger.plugin.client.BaseClient;
import org.apache.ranger.plugin.client.HadoopException;
import org.apache.ranger.services.kms.client.KMSClient;
import org.apache.ranger.services.kms.client.json.model.KMSSchedulerResponse;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;

public class KMSClient {

	public static final Logger LOG = Logger.getLogger(KMSClient.class) ;

	private static final String EXPECTED_MIME_TYPE = "application/json";
	
	private static final String KMS_LIST_API_ENDPOINT = "/ws/v1/cluster/scheduler" ;
	
	private static final String errMessage =  " You can still save the repository and start creating "
											  + "policies, but you would not be able to use autocomplete for "
											  + "resource names. Check xa_portal.log for more info.";

	
	String kmsQUrl;
	String userName;
	String password;

	public  KMSClient(String kmsQueueUrl, String kmsUserName, String kmsPassWord) {
		
		this.kmsQUrl = kmsQueueUrl;
		this.userName = kmsUserName ;
		this.password = kmsPassWord;
		
		if (LOG.isDebugEnabled()) {
			LOG.debug("Kms Client is build with url [" + kmsQueueUrl + "] user: [" + kmsPassWord + "], password: [" + "" + "]");
		}
		
	}
	
	public List<String> getQueueList(final String queueNameMatching, final List<String> existingQueueList) {
		
		if (LOG.isDebugEnabled()) {
			LOG.debug("Getting Kms queue list for queueNameMatching : " + queueNameMatching);
		}
		final String errMsg 			= errMessage;
		
		List<String> ret = new ArrayList<String>();
		
		Callable<List<String>> kmsQueueListGetter = new Callable<List<String>>() {
			@Override
			public List<String> call() {
				
				List<String> lret = new ArrayList<String>();
				
				String url = kmsQUrl + KMS_LIST_API_ENDPOINT ;
				
				Client client = null ;
				ClientResponse response = null ;
				
				try {
					client = Client.create() ;
					
					WebResource webResource = client.resource(url);
					
					response = webResource.accept(EXPECTED_MIME_TYPE)
						    .get(ClientResponse.class);
					
					if (LOG.isDebugEnabled()) {
						LOG.debug("getQueueList():calling " + url);
					}
					
					if (response != null) {
						if (LOG.isDebugEnabled()) {
							LOG.debug("getQueueList():response.getStatus()= " + response.getStatus());	
						}
						if (response.getStatus() == 200) {
							String jsonString = response.getEntity(String.class);
							Gson gson = new GsonBuilder().setPrettyPrinting().create();
							KMSSchedulerResponse kmsQResponse = gson.fromJson(jsonString, KMSSchedulerResponse.class);
							if (kmsQResponse != null) {
								List<String>  kmsQueueList = kmsQResponse.getQueueNames();
								if (kmsQueueList != null) {
									for ( String kmsQueueName : kmsQueueList) {
										if ( existingQueueList != null && existingQueueList.contains(kmsQueueName)) {
								        	continue;
								        }
										if (queueNameMatching == null || queueNameMatching.isEmpty()
												|| kmsQueueName.startsWith(queueNameMatching)) {
												if (LOG.isDebugEnabled()) {
													LOG.debug("getQueueList():Adding kmsQueue " + kmsQueueName);
												}
												lret.add(kmsQueueName) ;
											}
										}
									}
								}
						 } else{
							LOG.info("getQueueList():response.getStatus()= " + response.getStatus() + " for URL " + url + ", so returning null list");	
							String jsonString = response.getEntity(String.class);
							LOG.info(jsonString);
							lret = null;
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
					String msgDesc = "Exception while getting Kms Queue List."
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
			ret = timedTask(kmsQueueListGetter, 5, TimeUnit.SECONDS);
		} catch ( Exception e) {
			LOG.error("Unable to get Kms Queue list from [" + kmsQUrl + "]", e) ;
		}
		
		return ret;
	}
		
	public static HashMap<String, Object> testConnection(String serviceName,
			Map<String, String> configs) {

		List<String> strList = new ArrayList<String>();
		String errMsg = errMessage;
		boolean connectivityStatus = false;
		HashMap<String, Object> responseData = new HashMap<String, Object>();

		KMSClient kmsClient = getKmsClient(serviceName,
				configs);
		strList = getKmsResource(kmsClient, "",null);

		if (strList != null) {
			connectivityStatus = true;
		}

		if (connectivityStatus) {
			String successMsg = "TestConnection Successful";
			BaseClient.generateResponseDataMap(connectivityStatus, successMsg,
					successMsg, null, null, responseData);
		} else {
			String failureMsg = "Unable to retrieve any Kms Queues using given parameters.";
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

	public static List<String> getKmsResource (final KMSClient kmsClient,
			String yanrQname, List<String> existingQueueName) {

		List<String> resultList = new ArrayList<String>();
		String errMsg = errMessage;

		try {
			if (kmsClient == null) {
				String msgDesc = "Unable to get Kms Queue : KmsClient is null.";
				LOG.error(msgDesc);
				HadoopException hdpException = new HadoopException(msgDesc);
				hdpException.generateResponseDataMap(false, msgDesc, msgDesc
						+ errMsg, null, null);
				throw hdpException;
			}

			if (yanrQname != null) {
				String finalkmsQueueName = (yanrQname == null) ? ""
						: yanrQname.trim();
				resultList = kmsClient
						.getQueueList(finalkmsQueueName,existingQueueName);
				if (resultList != null) {
					if (LOG.isDebugEnabled()) {
						LOG.debug("Returning list of " + resultList.size() + " Kms Queues");
					}
				}
			}
		} catch (HadoopException he) {
			throw he;
		} catch (Exception e) {
			String msgDesc = "getKmsResource: Unable to get Kms resources.";
			LOG.error(msgDesc, e);
			HadoopException hdpException = new HadoopException(msgDesc);

			hdpException.generateResponseDataMap(false,
					BaseClient.getMessage(e), msgDesc + errMsg, null, null);
			throw hdpException;
		}
		return resultList;
	}
	
	public static <T> T timedTask(Callable<T> callableObj, long timeout,
			TimeUnit timeUnit) throws Exception {
		return callableObj.call();
	}

}
