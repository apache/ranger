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
import java.util.Iterator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.filter.HTTPBasicAuthFilter;

public class KnoxClient {

	private static final String EXPECTED_MIME_TYPE = "application/json";
	private static final Log LOG = LogFactory.getLog(KnoxClient.class);

	private String knoxUrl;
	private String userName;
	private String password;
	
	/*
	 curl -ivk -u admin:admin-password https://localhost:8443/gateway/admin/api/v1/topologies
	 curl -ivk -u admin:admin-password https://localhost:8443/gateway/admin/api/v1/topologies/admin
	 curl -ivk -u admin:admin-password https://localhost:8443/gateway/admin/api/v1/topologies/sandbox
	 curl -ivk -u admin:admin-password https://localhost:8443/gateway/admin/api/v1/topologies/hdp
	 curl -ivk -u admin:admin-password https://localhost:8443/gateway/admin/api/v1/topologies/hdp1
	 curl -ivk -u admin:admin-password https://localhost:8443/gateway/admin/api/v1/topologies/hdp2
	 curl -ivk -u admin:admin-password https://localhost:8443/gateway/admin/api/v1/topologies/xa
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
						// JsonNode rootNode = objectMapper.readTree(getKnoxMockResponseTopologies());
						
						Iterator<JsonNode> elements = rootNode.getElements();
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
					LOG.error("Unable to get a valid response for isFileChanged()  call for ["
							+ knoxUrl + "] - got null response.");
				}

			} finally {
				if (response != null) {
					response.close();
				}
				if (client != null) {
					client.destroy();
				}
			}
		} catch (Throwable t) {
			LOG.error("Exception on REST call to: " + knoxUrl, t);
			t.printStackTrace();
		} finally {
		}
		return topologyList;
	}

	
	public List<String> getServiceList(String topologyName, String serviceNameMatching) {
		
		// sample URI: .../admin/api/v1/topologies/<topologyName>
		
		List<String> serviceList = new ArrayList<String>();
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
				// WebResource webResource = client.resource(knoxUrl);
				
				response = webResource.accept(EXPECTED_MIME_TYPE)
					    .get(ClientResponse.class);
				LOG.debug("Knox service lookup response: " + response);
				if (response != null) {
					
					if (response.getStatus() == 200) {
						String jsonString = response.getEntity(String.class);
						LOG.debug("Knox service look up response JSON string: " + jsonString);
						
						ObjectMapper objectMapper = new ObjectMapper();
						
						JsonNode rootNode = objectMapper.readTree(jsonString);
						//JsonNode rootNode = objectMapper.readTree(getKnoxMockResponseTopology());
						
						JsonNode servicesNode = rootNode.get("services");
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
					LOG.error("Unable to get a valid response for isFileChanged()  call for ["
							+ knoxUrl + "] - got null response.");
				}

			} finally {
				if (response != null) {
					response.close();
				}
				if (client != null) {
					client.destroy();
				}
			}
		} catch (Throwable t) {
			LOG.error("Exception on REST call to: " + knoxUrl, t);
			t.printStackTrace();
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

	String getKnoxMockResponseTopologies() {
		
		// See https://docs.google.com/a/hortonworks.com/document/d/1fSs1xAMP2IeE24TOtywRrFMl81BHG72LeWu-O6WTq78/edit
		return 		
				"[" +
		 		    "{" +
		 		    	"  \"name\": \"hdp1\", " +
		 		    	"  \"timestamp\": 1405540981000, " +
		 		    	" \"href\": \"https://hdp.example.com:8443/gateway/admin/api/v1/topologies/hdp1\",  " +
		 		    	" \"url\": \"https://hdp.example.com:8443/gateway/hdp1\" " +
		 		    	"}, " +
				    "{ " +
				    	"  \"name\": \"hdp2\", " +
				    	"  \"timestamp\": 1405540981000, " +
				    	" \"href\": \"https://hdp.example.com:8443/gateway/admin/api/v1/topologies/hdp2\",  " +
				    	" \"url\": \"https://hdp.example.com:8443/gateway/hdp2\" " +
				    	"}" + 
				"]";
	}
	
	String getKnoxMockResponseTopology() {
		
		// See https://docs.google.com/a/hortonworks.com/document/d/1fSs1xAMP2IeE24TOtywRrFMl81BHG72LeWu-O6WTq78/edit
		return  
		"{" +
			"\"name\": \"hdp1\"," + 
			"\"providers\": [" +
				"{" + 
					"\"enabled\": true, " +
					"\"name\": null, " +
					"\"params\": {" +
						"\"main.ldapRealm\": \"org.apache.hadoop.gateway.shirorealm.KnoxLdapRealm\", " +
						"\"main.ldapRealm.userDnTemplate\": \"uid={0},ou=people,dc=hadoop,dc=apache,dc=org\"," +
         				"\"main.ldapRealm.contextFactory.url\": \"ldap://hdp.example.com:33389\"," +
         				"\"main.ldapRealm.contextFactory.authenticationMechanism\": \"simple\"," +
         				"\"urls./**\": \"authcBasic\"" +
						"}, " +
					"\"role\": \"authentication\"" +
				"}, " +
				 "{" +
	                "\"enabled\": true," +
	                "\"name\": \"Pseudo\"," +
	               "\"params\": {" + "}," +
	                "\"role\": \"identity-assertion\"" +
	            "}, " +
	            "{" +
	                    "\"enabled\": false," +
	                    "\"name\": null," +
	                    "\"params\": {" + "}," +
	                    "\"role\": null" +
	              "}" +
			"], " +
			"\"services\": [" +
				"{" +
                 	"\"params\": {" + "}," +
                 	"\"role\": \"KNOXADMIN\"," +
                 	"\"url\": null" +
                 	"}," +
                 "{" +
                 	"\"params\": {" + "}, " +
                 	"\"role\": \"WEBHDFS\"," +
                 	"\"url\": \"http://hdp.example.com:50070/webhdfs\"" +
                 	"}" +
                 	"]," +
             "\"timestamp\": 1405541437000" +
		"}";
	}
	
}
