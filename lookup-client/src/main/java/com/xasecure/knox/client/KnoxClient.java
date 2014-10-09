/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.xasecure.knox.client;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.filter.HTTPBasicAuthFilter;
import com.xasecure.hadoop.client.config.BaseClient;
import com.xasecure.hadoop.client.exceptions.HadoopException;

public class KnoxClient {

	private static final String EXPECTED_MIME_TYPE = "application/json";
	private static final Log LOG = LogFactory.getLog(KnoxClient.class);

	private String knoxUrl;
	private String userName;
	private String password;
	
	/*
   Sample curl calls to Knox to discover topologies
	 curl -ivk -u admin:admin-password https://localhost:8443/gateway/admin/api/v1/topologies
	 curl -ivk -u admin:admin-password https://localhost:8443/gateway/admin/api/v1/topologies/admin
	*/
	
	public KnoxClient(String knoxUrl, String userName, String password) {
		LOG.debug("Constructed KnoxClient with knoxUrl: " + knoxUrl +
				", userName: " + userName);
		this.knoxUrl = knoxUrl;
		this.userName = userName;
		this.password = password;
	}

	public  List<String> getTopologyList(String topologyNameMatching) {
		
		// sample URI: https://hdp.example.com:8443/gateway/admin/api/v1/topologies
		LOG.debug("Getting Knox topology list for topologyNameMatching : " +
				topologyNameMatching);
		List<String> topologyList = new ArrayList<String>();
		String errMsg = " You can still save the repository and start creating "
				+ "policies, but you would not be able to use autocomplete for "
				+ "resource names. Check xa_portal.log for more info.";
		if ( topologyNameMatching == null ||  topologyNameMatching.trim().isEmpty()) {
			topologyNameMatching = "";
		}
		try {

			Client client = null;
			ClientResponse response = null;

			try {
				client = Client.create();;
				
				client.addFilter(new HTTPBasicAuthFilter(userName, password));
				WebResource webResource = client.resource(knoxUrl);
				response = webResource.accept(EXPECTED_MIME_TYPE)
					    .get(ClientResponse.class);
				LOG.debug("Knox topology list response: " + response);
				if (response != null) {

					if (response.getStatus() == 200) {
						String jsonString = response.getEntity(String.class);
						LOG.debug("Knox topology list response JSON string: "+ jsonString);
						
						ObjectMapper objectMapper = new ObjectMapper();
						
						JsonNode rootNode = objectMapper.readTree(jsonString);
						JsonNode topologyNode = rootNode.findValue("topology");
						if (topologyNode == null) {
							return topologyList;
						}
						Iterator<JsonNode> elements = topologyNode.getElements();
						while (elements.hasNext()) {
							JsonNode element = elements.next();
							String topologyName = element.get("name").getValueAsText();
							LOG.debug("Found Knox topologyName: " + topologyName);
							if (topologyName.startsWith(topologyNameMatching)) {
								topologyList.add(topologyName);
							}
						}
					} else {
						LOG.error("Got invalid  REST response from: "+ knoxUrl + ", responsStatus: " + response.getStatus());
					}

				} else {
					String msgDesc = "Unable to get a valid response for "
							+ "isFileChanged() call for KnoxUrl : [" + knoxUrl
							+ "] - got null response.";
					LOG.error(msgDesc);
					HadoopException hdpException = new HadoopException(msgDesc);
					hdpException.generateResponseDataMap(false, msgDesc,
							msgDesc + errMsg, null, null);
					throw hdpException;
				}

			} finally {
				if (response != null) {
					response.close();
				}
				if (client != null) {
					client.destroy();
				}
			}
		} catch (HadoopException he) {
			throw he;
		} catch (Throwable t) {
			String msgDesc = "Exception on REST call to KnoxUrl : " + knoxUrl + ".";
			HadoopException hdpException = new HadoopException(msgDesc, t);
			LOG.error(msgDesc, t);

			hdpException.generateResponseDataMap(false,
					BaseClient.getMessage(t), msgDesc + errMsg, null, null);
			throw hdpException;
		} finally {
		}
		return topologyList;
	}

	
	public List<String> getServiceList(String topologyName, String serviceNameMatching) {
		
		// sample URI: .../admin/api/v1/topologies/<topologyName>
		
		List<String> serviceList = new ArrayList<String>();
		String errMsg = " You can still save the repository and start creating "
				+ "policies, but you would not be able to use autocomplete for "
				+ "resource names. Check xa_portal.log for more info.";
		if ( serviceNameMatching == null ||  serviceNameMatching.trim().isEmpty()) {
			serviceNameMatching = "";
		}
		try {

			Client client = null;
			ClientResponse response = null;

			try {
				client = Client.create();;
				
				client.addFilter(new HTTPBasicAuthFilter(userName, password));
				
				WebResource webResource = client.resource(knoxUrl + "/" + topologyName);
				
				response = webResource.accept(EXPECTED_MIME_TYPE)
					    .get(ClientResponse.class);
				LOG.debug("Knox service lookup response: " + response);
				if (response != null) {
					
					if (response.getStatus() == 200) {
						String jsonString = response.getEntity(String.class);
						LOG.debug("Knox service look up response JSON string: " + jsonString);
						
						ObjectMapper objectMapper = new ObjectMapper();
						
						JsonNode rootNode = objectMapper.readTree(jsonString);
						JsonNode topologyNode = rootNode.findValue("topology");
						JsonNode servicesNode = topologyNode.get("services");
						Iterator<JsonNode> services = servicesNode.getElements();
						while (services.hasNext()) {
							JsonNode service = services.next();
							String serviceName = service.get("role").getValueAsText();
							LOG.debug("Knox serviceName: " + serviceName);
							if (serviceName.startsWith(serviceNameMatching)) {
								serviceList.add(serviceName);
							}
						}
					} else {
						LOG.error("Got invalid  REST response from: "+ knoxUrl + ", responsStatus: " + response.getStatus());
					}

				} else {
					String msgDesc = "Unable to get a valid response for "
							+ "isFileChanged() call for KnoxUrl : [" + knoxUrl
							+ "] - got null response.";
					LOG.error(msgDesc);
					HadoopException hdpException = new HadoopException(msgDesc);
					hdpException.generateResponseDataMap(false, msgDesc,
							msgDesc + errMsg, null, null);
					throw hdpException;
				}

			} finally {
				if (response != null) {
					response.close();
				}
				if (client != null) {
					client.destroy();
				}
			}
		} catch (HadoopException he) {
			throw he;
		} catch (Throwable t) {
			String msgDesc = "Exception on REST call to KnoxUrl : " + knoxUrl + ".";
			HadoopException hdpException = new HadoopException(msgDesc, t);
			LOG.error(msgDesc, t);

			hdpException.generateResponseDataMap(false,
					BaseClient.getMessage(t), msgDesc + errMsg, null, null);
			throw hdpException;

		} finally {
		}
		return serviceList;
	}

	public static void main(String[] args) {

		KnoxClient knoxClient = null;

		if (args.length != 3) {
			System.err.println("USAGE: java " + KnoxClient.class.getName()
					+ " knoxUrl userName password [sslConfigFileName]");
			System.exit(1);
		}

		try {
			knoxClient = new KnoxClient(args[0], args[1], args[2]);
			List<String> topologyList = knoxClient.getTopologyList("");
			if ((topologyList == null) || topologyList.isEmpty()) {
				System.out.println("No knox topologies found");
			} else {
				for (String topology : topologyList) {
					System.out.println("Found Topology: " + topology);
					List<String> serviceList = knoxClient.getServiceList(topology, "");
					if ((serviceList == null) || serviceList.isEmpty()) {
						System.out.println("No services found for knox topology: " + topology);
					} else {
						for (String service : serviceList) {
							System.out.println("	Found service for topology: " + service +", " + topology);
						}
					}
				}
			}
		} finally {
		}
	}
	
	public static HashMap<String, Object> testConnection(String dataSource,
			HashMap<String, String> connectionProperties) {

		List<String> strList = new ArrayList<String>();
		String errMsg = " You can still save the repository and start creating "
				+ "policies, but you would not be able to use autocomplete for "
				+ "resource names. Check xa_portal.log for more info.";
		boolean connectivityStatus = false;
		HashMap<String, Object> responseData = new HashMap<String, Object>();

		KnoxClient knoxClient = getKnoxClient(dataSource, connectionProperties);
		strList = getKnoxResources(knoxClient, "", null);

		if (strList != null && (strList.size() != 0)) {
			connectivityStatus = true;
		}
		
		if (connectivityStatus) {
			String successMsg = "TestConnection Successful";
			BaseClient.generateResponseDataMap(connectivityStatus, successMsg, successMsg,
					null, null, responseData);
		} else {
			String failureMsg = "Unable to retrieve any topologies/services using given parameters.";
			BaseClient.generateResponseDataMap(connectivityStatus, failureMsg, failureMsg + errMsg,
					null, null, responseData);
		}
		
		return responseData;
	}

	public static KnoxClient getKnoxClient(String dataSourceName,
			Map<String, String> configMap) {
		KnoxClient knoxClient = null;
		LOG.debug("Getting knoxClient for datasource: " + dataSourceName
				+ "configMap: " + configMap);
		String errMsg = " You can still save the repository and start creating "
				+ "policies, but you would not be able to use autocomplete for "
				+ "resource names. Check xa_portal.log for more info.";
		if (configMap == null || configMap.isEmpty()) {
			String msgDesc = "Could not connect as Connection ConfigMap is empty.";
			LOG.error(msgDesc);
			HadoopException hdpException = new HadoopException(msgDesc);
			hdpException.generateResponseDataMap(false, msgDesc, msgDesc + errMsg, null,
					null);
			throw hdpException;
		} else {
			String knoxUrl = configMap.get("knox.url");
			String knoxAdminUser = configMap.get("username");
			String knoxAdminPassword = configMap.get("password");
			knoxClient = new KnoxClient(knoxUrl, knoxAdminUser,
					knoxAdminPassword);
		}
		return knoxClient;
	}

	public static List<String> getKnoxResources(final KnoxClient knoxClient,
			String topologyName, String serviceName) {

		List<String> resultList = new ArrayList<String>();
		String errMsg = " You can still save the repository and start creating "
				+ "policies, but you would not be able to use autocomplete for "
				+ "resource names. Check xa_portal.log for more info.";

		try {
			if (knoxClient == null) {
				// LOG.error("Unable to get knox resources: knoxClient is null");
				// return new ArrayList<String>();
				String msgDesc = "Unable to get knox resources: knoxClient is null.";
				LOG.error(msgDesc);
				HadoopException hdpException = new HadoopException(msgDesc);
				hdpException.generateResponseDataMap(false, msgDesc, msgDesc + errMsg,
						null, null);
				throw hdpException;
			}

			final Callable<List<String>> callableObj;
			if (serviceName != null) {
				final String finalServiceNameMatching = serviceName.trim();
				final String finalTopologyName = topologyName;
				callableObj = new Callable<List<String>>() {
					@Override
					public List<String> call() {
						return knoxClient.getServiceList(finalTopologyName,
								finalServiceNameMatching);
					}
				};

			} else {
				final String finalTopologyNameMatching = (topologyName == null) ? ""
						: topologyName.trim();
				callableObj = new Callable<List<String>>() {
					@Override
					public List<String> call() {
						return knoxClient
								.getTopologyList(finalTopologyNameMatching);
					}
				};
			}
			resultList = timedTask(callableObj, 5, TimeUnit.SECONDS);

		} catch (HadoopException he) {
			throw he;
		} catch (Exception e) {
			String msgDesc = "Unable to get knox resources.";
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
