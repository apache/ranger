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

package org.apache.ranger.services.knox.client;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import com.sun.jersey.client.urlconnection.HTTPSProperties;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;
import org.apache.ranger.plugin.client.BaseClient;
import org.apache.ranger.plugin.client.HadoopException;
import org.apache.ranger.plugin.util.PasswordUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import javax.net.ssl.SSLContext;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

/**
 * @author zhaoshuaihua
 */
public class KnoxClient {

	private static final String EXPECTED_MIME_TYPE = "application/json";
	private static final Logger LOG = LoggerFactory.getLogger(KnoxClient.class);

	private String knoxUrl;
	private String userName;
	private String password;
	
	/*
   Sample curl calls to Knox to discover topologies
         curl -ivk -u <user-name>:<user-password> https://localhost:8443/gateway/admin/api/v1/topologies
         curl -ivk -u <user-name>:<user-password> https://localhost:8443/gateway/admin/api/v1/topologies/admin
	*/
	
	public KnoxClient(String knoxUrl, String userName, String password) {
		LOG.debug("Constructed KnoxClient with knoxUrl: " + knoxUrl +
				", userName: " + userName);
		this.knoxUrl = knoxUrl;
		this.userName = userName;
		this.password = password;
	}

	public List<String> getTopologyList(String topologyNameMatching,List<String> knoxTopologyList) {
		
		// sample URI: https://hdp.example.com:8443/gateway/admin/api/v1/topologies
		LOG.debug("Getting Knox topology list for topologyNameMatching : " +
				topologyNameMatching);
		List<String> topologyList = new ArrayList<String>();
		String errMsg = " You can still save the repository and start creating "
				+ "policies, but you would not be able to use autocomplete for "
				+ "resource names. Check ranger_admin.log for more info.";
		if (topologyNameMatching == null ||  topologyNameMatching.trim().isEmpty()) {
			topologyNameMatching = "";
		}

		String decryptedPwd=null;
		try {
			decryptedPwd=PasswordUtils.decryptPassword(password);
		} catch(Exception ex) {
			LOG.info("Password decryption failed; trying knox connection with received password string");
			decryptedPwd=null;
		} finally {
			if (decryptedPwd==null) {
				decryptedPwd=password;
			}
		}
		try {
			CloseableHttpClient httpClient = null;
			HttpGet httpGet = null;
			HttpResponse response = null;
			try {
				httpClient = HttpClientUtil.buildNoSslHttpClient(userName, decryptedPwd);

				httpGet = new HttpGet(knoxUrl);
				response = httpClient.execute(httpGet);

				LOG.debug("Knox topology list response: " + response);
				if (response != null) {

					if (response.getStatusLine().getStatusCode() == 200) {

						String jsonString = EntityUtils.toString(response.getEntity());

						LOG.debug("Knox topology list response JSON string: "+ jsonString);
						if (StringUtils.isEmpty(jsonString)) {
							return topologyList;
						}

						DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
						DocumentBuilder builder = factory.newDocumentBuilder();
						// 将XML字符串解析为Document对象
						Document document = builder.parse(new InputSource(new StringReader(jsonString)));
						// 获取所有topology节点
						NodeList topologyNodes = document.getElementsByTagName("topology");

						for (int i = 0; i < topologyNodes.getLength(); i++) {
							Node topologyNode = topologyNodes.item(i);
							if (topologyNode.getNodeType() == Node.ELEMENT_NODE) {
								Element element = (Element) topologyNode;
								// 获取name节点的值
								Node nameNode = element.getElementsByTagName("name").item(0);
								String topologyName = nameNode.getTextContent();
								System.out.println("Found Knox topologyName: " + topologyName);
								if (knoxTopologyList != null && topologyName != null && knoxTopologyList.contains(topologyNameMatching)) {
									continue;
								}

                    			if (topologyName != null && ("*".equals(topologyNameMatching) || topologyName.startsWith(topologyNameMatching))) {
                        			topologyList.add(topologyName);
                    			}
							}
						}
					} else {
						LOG.error("Got invalid REST response from: " + knoxUrl + ", responseStatus: " + response.getStatusLine().getStatusCode());
					}

				} else {
					String msgDesc = "Unable to get a valid response for "
							+ "getTopologyList() call for KnoxUrl : [" + knoxUrl
							+ "] - got null response.";
					LOG.error(msgDesc);
					HadoopException hdpException = new HadoopException(msgDesc);
					hdpException.generateResponseDataMap(false, msgDesc,
							msgDesc + errMsg, null, null);
					throw hdpException;
				}

			} finally {
				if (httpClient != null) {
					httpClient.close();
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
		}
		if (LOG.isDebugEnabled()) {
			LOG.debug("<== KnoxClient.getTopologyList() Topology Matching: " + topologyNameMatching + " Result : " + topologyList.toString());
		}
		return topologyList;
	}

	
	public List<String> getServiceList(List<String> knoxTopologyList, String serviceNameMatching, List<String> knoxServiceList) {
		
		// sample URI: .../admin/api/v1/topologies/<topologyName>
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> KnoxClient.getServiceList() Service Name: " + serviceNameMatching );
		}
		List<String> serviceList = new ArrayList<String>();
		String errMsg = " You can still save the repository and start creating "
				+ "policies, but you would not be able to use autocomplete for "
				+ "resource names. Check ranger_admin.log for more info.";
		if (serviceNameMatching == null ||  serviceNameMatching.trim().isEmpty()) {
			serviceNameMatching = "";
		}
		String decryptedPwd=null;
		try {
			decryptedPwd=PasswordUtils.decryptPassword(password);
		} catch(Exception ex) {
			LOG.info("Password decryption failed; trying knox connection with received password string");
			decryptedPwd=null;
		} finally {
			if (decryptedPwd==null) {
				decryptedPwd=password;
			}
		}
		try{
			CloseableHttpClient httpClient = null;
			HttpGet httpGet = null;
			HttpResponse response = null;
			try {
				httpClient = HttpClientUtil.buildNoSslHttpClient(userName, decryptedPwd);
				for (String topologyName : knoxTopologyList) {
					httpGet = new HttpGet(knoxUrl+ "/" + topologyName);
					response = httpClient.execute(httpGet);

					LOG.debug("Knox service lookup response: " + response);
					if (response != null) {

						if (response.getStatusLine().getStatusCode() == 200) {
							String jsonString = EntityUtils.toString(response.getEntity());
							LOG.debug("Knox service lookup response JSON string: " + jsonString);

							DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
							DocumentBuilder builder = factory.newDocumentBuilder();
							// 将XML字符串解析为Document对象
							Document document = builder.parse(new InputSource(new StringReader(jsonString)));
							// 获取所有topology节点
							NodeList topologyNodes = document.getElementsByTagName("topology");

							if (topologyNodes != null) {
								for (int i = 0; i < topologyNodes.getLength(); i++) {
									Element topologyNode = (Element) topologyNodes.item(i);
									NodeList servicesNodes = topologyNode.getElementsByTagName("service");

									for (int j = 0; j < servicesNodes.getLength(); j++) {
										Element serviceElement = (Element) servicesNodes.item(j);
										String serviceName = serviceElement.getElementsByTagName("role").item(0).getTextContent();
										LOG.debug("Knox serviceName: " + serviceName);
										if (serviceName == null || (knoxServiceList != null && knoxServiceList.contains(serviceName))) {
											continue;
										}
										if (serviceName.startsWith(serviceNameMatching) || "*".equals(serviceNameMatching)) {
											serviceList.add(serviceName);
										}
									}
								}
							}
						} else {
							LOG.error("Got invalid  REST response from: " + knoxUrl + ", responsStatus: " + response.getStatusLine().getStatusCode());
						}

					} else {
						String msgDesc = "Unable to get a valid response for "
								+ "getServiceList() call for KnoxUrl : [" + knoxUrl
								+ "] - got null response.";
						LOG.error(msgDesc);
						HadoopException hdpException = new HadoopException(msgDesc);
						hdpException.generateResponseDataMap(false, msgDesc,
								msgDesc + errMsg, null, null);
						throw hdpException;
					}
				}
			} catch (IOException | ParserConfigurationException | SAXException e) {
                throw new RuntimeException(e);
            } finally{
				if (httpClient != null) {
					httpClient.close();
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

		knoxClient = new KnoxClient(args[0], args[1], args[2]);
		List<String> topologyList = knoxClient.getTopologyList("",null);
		if ((topologyList == null) || topologyList.isEmpty()) {
			System.out.println("No knox topologies found");
		} else {
			List<String> serviceList = knoxClient.getServiceList(topologyList,"*",null);
			if ((serviceList == null) || serviceList.isEmpty()) {
				System.out.println("No services found for knox topology: ");
			} else {
				for (String service : serviceList) {
					System.out.println("	Found service for topology: " + service );
				}
			}
		}
	}
	
	public static Map<String, Object> connectionTest(String serviceName,
										  		Map<String, String> configs) {

		String errMsg = " You can still save the repository and start creating "
				+ "policies, but you would not be able to use autocomplete for "
				+ "resource names. Check ranger_admin.log for more info.";
		boolean connectivityStatus = false;
		Map<String, Object> responseData = new HashMap<String, Object>();

		KnoxClient knoxClient = getKnoxClient(serviceName, configs);
		List<String> strList = getKnoxResources(knoxClient, "", null,null,null);

		if (strList != null && (strList.size() != 0)) {
			connectivityStatus = true;
		}
		
		if (connectivityStatus) {
			String successMsg = "ConnectionTest Successful";
			BaseClient.generateResponseDataMap(connectivityStatus, successMsg, successMsg,
					null, null, responseData);
		} else {
			String failureMsg = "Unable to retrieve any topologies/services using given parameters.";
			BaseClient.generateResponseDataMap(connectivityStatus, failureMsg, failureMsg + errMsg,
					null, null, responseData);
		}
		
		return responseData;
	}

	public static KnoxClient getKnoxClient(String serviceName,
										   Map<String, String> configs) {
		KnoxClient knoxClient = null;
		if(LOG.isDebugEnabled()){
			LOG.debug("Getting knoxClient for ServiceName: " + serviceName);
			LOG.debug("configMap: " + configs);
		}
		String errMsg = " You can still save the repository and start creating "
				+ "policies, but you would not be able to use autocomplete for "
				+ "resource names. Check ranger_admin.log for more info.";
		if ( configs != null && !configs.isEmpty()) {
			String knoxUrl = configs.get("knox.url");
			String knoxAdminUser = configs.get("username");
			String knoxAdminPassword = configs.get("password");
			knoxClient = new KnoxClient(knoxUrl, knoxAdminUser,
										knoxAdminPassword);
		} else {
			String msgDesc = "Could not connect as Connection ConfigMap is empty.";
			LOG.error(msgDesc);
			HadoopException hdpException = new HadoopException(msgDesc);
			hdpException.generateResponseDataMap(false, msgDesc, msgDesc + errMsg, null,
					null);
			throw hdpException;
		}
		return knoxClient;
	}

	public static List<String> getKnoxResources(final KnoxClient knoxClient,
			String topologyName, String serviceName, List<String> knoxTopologyList, List<String> knoxServiceList) {

		if (LOG.isDebugEnabled() ) {
			LOG.debug("==> KnoxClient.getKnoxResource " + "topology: " + topologyName + "Service Name: " + serviceName);
		}

		List<String> resultList = new ArrayList<String>();
		String errMsg = " You can still save the repository and start creating "
				+ "policies, but you would not be able to use autocomplete for "
				+ "resource names. Check ranger_admin.log for more info.";

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
				final String 	   finalServiceNameMatching = serviceName.trim();
				final List<String> finalknoxServiceList		= knoxServiceList;
				final List<String> finalTopologyList 		= knoxTopologyList;
				callableObj = new Callable<List<String>>() {
					@Override
					public List<String> call() {
						return knoxClient.getServiceList(finalTopologyList,
								finalServiceNameMatching,finalknoxServiceList);
					}
				};

			} else {
				final String finalTopologyNameMatching  = (topologyName == null) ? ""
						: topologyName.trim();
				final List<String> finalknoxTopologyList = knoxTopologyList;
				callableObj = new Callable<List<String>>() {
					@Override
					public List<String> call() {
						return knoxClient
								.getTopologyList(finalTopologyNameMatching,finalknoxTopologyList);
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
		if (LOG.isDebugEnabled()) {
			LOG.debug("<== KnoxClient.getKnoxResources() Result : "+ resultList  );
		}
		return resultList;
	}

	public static <T> T timedTask(Callable<T> callableObj, long timeout,
			TimeUnit timeUnit) throws Exception {
		return callableObj.call();
	}

}
