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

package org.apache.ranger.services.atlas.client;

import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import javax.security.auth.Subject;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.NewCookie;

import org.apache.log4j.Logger;
import org.apache.ranger.plugin.client.BaseClient;
import org.apache.ranger.plugin.client.HadoopException;
import org.apache.ranger.plugin.util.PasswordUtils;
import org.apache.ranger.services.atlas.json.model.ResourceEntityResponse;
import org.apache.ranger.services.atlas.json.model.ResourceOperationResponse;
import org.apache.ranger.services.atlas.json.model.ResourceOperationResponse.Results;
import org.apache.ranger.services.atlas.json.model.ResourceTaxonomyResponse;
import org.apache.ranger.services.atlas.json.model.ResourceTermResponse;
import org.apache.ranger.services.atlas.json.model.ResourceTypeResponse;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.core.util.MultivaluedMapImpl;

public class AtlasClient extends BaseClient {

	private static final Logger LOG = Logger.getLogger(AtlasClient.class);
	private static final String EXPECTED_MIME_TYPE = "application/json";
	private static final String WEB_RESOURCE_CONTENT_TYPE = "application/x-www-form-urlencoded";
	private static final String ATLAS_STATUS_API_ENDPOINT = "/j_spring_security_check";
	/*** TYPE **/
	private static final String ATLAS_LIST_TYPE_API_ENDPOINT = "/api/atlas/types/";
	/**** ENTITY **/
	private static final String ATLAS_ENTITY_LIST_API_ENDPOINT = "/api/atlas/v1/entities";
	/*** TERM **/
	private static final String ATLAS_LIST_TERM_API_ENDPOINT = "/api/atlas/v1/taxonomies/Catalog/terms/";
	/*** TAXONOMY **/
	private static final String ATLAS_LIST_TAXONOMY_API_ENDPOINT = "/api/atlas/v1/taxonomies/";
	/*** OPERATION **/
	private static final String ATLAS_OPERATION_SEARCH_API_ENDPOINT = "/api/atlas/discovery/search/gremlin/query=";
	private static final String errMessage = " You can still save the repository and start creating "
			+ "policies, but you would not be able to use autocomplete for "
			+ "resource names. Check ranger_admin.log for more info.";

	private String atlasUrl;
	private String userName;
	private String password;
	private String statusUrl;

	public AtlasClient(String serviceName, Map<String, String> configs) {

		super(serviceName, configs, "atlas-client");

		this.atlasUrl = configs.get("atlas.rest.address");
		this.userName = configs.get("username");
		this.password = configs.get("password");
		this.statusUrl = atlasUrl + ATLAS_STATUS_API_ENDPOINT;
		if (this.atlasUrl == null || this.atlasUrl.isEmpty()) {
			LOG.error("No value found for configuration 'atlas.rest.address'. Atlas resource lookup will fail");
		}
		if (this.userName == null || this.userName.isEmpty()) {
			LOG.error("No value found for configuration 'username'. Atlas resource lookup will fail");
		}
		if (this.password == null || this.password.isEmpty()) {
			LOG.error("No value found for configuration 'password'. Atlas resource lookup will fail");
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("Atlas Client is build with url [" + this.atlasUrl + "] user: [" + this.userName
					+ "], password: [" + "*********" + "]");
		}
	}

	public List<String> getResourceList(final String resourceNameMatching, final String atlasResourceParameter,
			final List<String> existingResourceList) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("Getting Atlas Resource list for resourceNameMatching : " + resourceNameMatching);
		}
		final String errMsg = errMessage;
		List<String> ret = null;
		Callable<List<String>> callableAtlasResourceListGetter = new Callable<List<String>>() {

			@Override
			public List<String> call() {
				List<String> atlasResourceListGetter = null;
				Subject subj = getLoginSubject();
				if (subj != null) {
					atlasResourceListGetter = Subject.doAs(subj, new PrivilegedAction<List<String>>() {
						@Override
						public List<String> run() {
							Client client = null;
							List<String> lret = new ArrayList<String>();
							try {
								client = Client.create();

								if (null == resourceNameMatching || "".equals(resourceNameMatching)) {
									lret = connectionTestResource(resourceNameMatching, atlasResourceParameter,
											existingResourceList, client);
								} else if ("type".equals(resourceNameMatching)) {
									lret = getTypeResource(resourceNameMatching, atlasResourceParameter,
											existingResourceList, client);
								} else if ("term".equals(resourceNameMatching)) {
									lret = getTermResource(resourceNameMatching, atlasResourceParameter,
											existingResourceList, client);
								} else if ("taxonomy".equals(resourceNameMatching)) {
									lret = getTaxonomyResource(resourceNameMatching, atlasResourceParameter,
											existingResourceList, client);
								} else if ("entity".equals(resourceNameMatching)) {
									lret = getEntityResource(resourceNameMatching, atlasResourceParameter,
											existingResourceList, client);
								} else if ("operation".equals(resourceNameMatching)) {
									lret = getOperationResource(resourceNameMatching, atlasResourceParameter,
											existingResourceList, client);
								}
							} catch (Throwable t) {
								String msgDesc = "Exception while getting Atlas Resource List.";
								HadoopException hdpException = new HadoopException(msgDesc, t);
								LOG.error(msgDesc, t);
								hdpException.generateResponseDataMap(false, BaseClient.getMessage(t), msgDesc + errMsg,
										null, null);
								throw hdpException;
							} finally {
								if (client != null) {
									client.destroy();
								}
							}
							return lret;
						}
					});
				}
				return atlasResourceListGetter;
			}
		};
		try {
			ret = timedTask(callableAtlasResourceListGetter, 5, TimeUnit.SECONDS);
		} catch (Throwable t) {
			LOG.error("Unable to get Atlas Resource list", t);
			String msgDesc = "Unable to get a valid response for " + "expected mime type : [" + EXPECTED_MIME_TYPE
					+ "] ";
			HadoopException hdpException = new HadoopException(msgDesc, t);
			LOG.error(msgDesc, t);
			hdpException.generateResponseDataMap(false, BaseClient.getMessage(t), msgDesc + errMsg, null, null);
			throw hdpException;
		}
		return ret;
	}

	private ClientResponse getStatusResponse(Client client) {
		final String errMsg = errMessage;
		ClientResponse statusResponse = null;
		try {
			WebResource webResource = client.resource(statusUrl);
			MultivaluedMap<String, String> formData = new MultivaluedMapImpl();
			formData.add("j_username", userName);
			String decryptedPwd = null;
			try {
				decryptedPwd = PasswordUtils.decryptPassword(password);
			} catch (Exception ex) {
				LOG.info("Password decryption failed; trying Atlas connection with received password string");
				decryptedPwd = null;
			} finally {
				if (decryptedPwd == null) {
					decryptedPwd = password;
				}
			}
			formData.add("j_password", decryptedPwd);
			try {
				statusResponse = webResource.type(WEB_RESOURCE_CONTENT_TYPE).post(ClientResponse.class,
						formData);
			} catch (Exception e) {
				String msgDesc = "Unable to get a valid statusResponse for expected mime type : ["
						+ WEB_RESOURCE_CONTENT_TYPE + "] URL : " + statusUrl + " - got null response.";
				LOG.error(msgDesc);
			}
			if (LOG.isDebugEnabled()) {
				LOG.debug("getStatusResponse():calling " + statusUrl);
			}
			if (statusResponse != null) {
				if (LOG.isDebugEnabled()) {
					LOG.debug("getStatusResponse():response.getStatus()= " + statusResponse.getStatus());
				}
			}
		} catch (Throwable t) {
			String msgDesc = "Exception while getting Atlas Resource List." + " URL : " + statusUrl;
			HadoopException hdpException = new HadoopException(msgDesc, t);
			LOG.error(msgDesc, t);
			hdpException.generateResponseDataMap(false, BaseClient.getMessage(t), msgDesc + errMsg, null, null);
			throw hdpException;
		}
		return statusResponse;
	}

	public List<String> connectionTestResource(final String resourceNameMatching, final String atlasResourceParameter,
			List<String> existingResourceList, Client client) {
		List<String> lret = new ArrayList<String>();
		final String errMsg = errMessage;
		String testConnectiontUrl = atlasUrl + ATLAS_LIST_TYPE_API_ENDPOINT;
		ClientResponse statusResponse = null;
		ClientResponse resultResponse = null;
		try {
			statusResponse = getStatusResponse(client);
			if (statusResponse != null) {
				if (LOG.isDebugEnabled()) {
					LOG.debug("getTypeResource():response.getStatus()= " + statusResponse.getStatus());
				}
				if (statusResponse.getStatus() == 200) {
					WebResource webResourceTestConnection = client.resource(testConnectiontUrl);
					WebResource.Builder builder = webResourceTestConnection.getRequestBuilder();
					for (NewCookie cook : statusResponse.getCookies()) {
						builder = builder.cookie(cook);
					}
					resultResponse = builder.get(ClientResponse.class);
					lret.add(resultResponse.getEntity(String.class));
				} else {
					LOG.info("connectionTestResource():response.getStatus()= " + statusResponse.getStatus()
							+ " for URL " + statusUrl + ", so returning null list");
					LOG.info(statusResponse.getEntity(String.class));
					lret = null;
				}
			}
		} catch (Throwable t) {
			lret = null;
			String msgDesc = "Exception while getting Atlas Resource List." + " URL : " + statusUrl;
			HadoopException hdpException = new HadoopException(msgDesc, t);
			LOG.error(msgDesc, t);
			hdpException.generateResponseDataMap(false, BaseClient.getMessage(t), msgDesc + errMsg, null, null);
			throw hdpException;
		} finally {
			if (statusResponse != null) {
				statusResponse.close();
			}
			if (resultResponse != null) {
				resultResponse.close();
			}
		}
		return lret;
	}

	public List<String> getTypeResource(final String resourceNameMatching, final String atlasResourceParameter,
			List<String> existingResourceList, Client client) {
		List<String> lret = new ArrayList<String>();
		final String errMsg = errMessage;
		ClientResponse statusResponse = null;
		ClientResponse resultResponse = null;
		try {
			statusResponse = getStatusResponse(client);
			if (statusResponse != null) {
				if (LOG.isDebugEnabled()) {
					LOG.debug("getTypeResource():response.getStatus()= " + statusResponse.getStatus());
				}
				if (statusResponse.getStatus() == 200) {
					WebResource webResourceType = client.resource(atlasUrl + ATLAS_LIST_TYPE_API_ENDPOINT);
					WebResource.Builder builder = webResourceType.getRequestBuilder();
					for (NewCookie cook : statusResponse.getCookies()) {
						builder = builder.cookie(cook);
					}
					resultResponse = builder.get(ClientResponse.class);
					if (resultResponse != null) {
						String jsonString = resultResponse.getEntity(String.class).toString();
						Gson gson = new Gson();
						List<String> responseResourceList = new ArrayList<String>();
						ResourceTypeResponse resourceTypeResponses = gson.fromJson(jsonString,
								ResourceTypeResponse.class);
						if (resourceTypeResponses != null) {
							responseResourceList = resourceTypeResponses.getResults();
						}
						if (responseResourceList != null) {
							for (String responseResource : responseResourceList) {
								if (responseResource != null) {
									if (existingResourceList != null && existingResourceList.contains(responseResource)) {
										continue;
									}
									if (atlasResourceParameter == null || atlasResourceParameter.isEmpty()
											|| responseResource.startsWith(atlasResourceParameter)) {
										if (LOG.isDebugEnabled()) {
											LOG.debug("getTypeResource():Adding existing Resource " + responseResource);
										}
										lret.add(responseResource);
									}
								}
							}
						}
					}
				}
			}
		} catch (Throwable t) {
			String msgDesc = "Exception while getting Atlas TypeResource List." + " URL : " + atlasUrl
					+ ATLAS_LIST_TYPE_API_ENDPOINT;
			HadoopException hdpException = new HadoopException(msgDesc, t);
			LOG.error(msgDesc, t);
			hdpException.generateResponseDataMap(false, BaseClient.getMessage(t), msgDesc + errMsg, null, null);
			throw hdpException;
		} finally {
			if (statusResponse != null) {
				statusResponse.close();
			}
			if (resultResponse != null) {
				resultResponse.close();
			}
		}
		return lret;
	}

	public List<String> getEntityResource(final String resourceNameMatching, final String atlasResourceParameter,
			List<String> existingResourceList, Client client) {
		List<String> lret = new ArrayList<String>();
		final String errMsg = errMessage;
		ClientResponse statusResponse = null;
		ClientResponse resultResponse = null;

		try {
			statusResponse = getStatusResponse(client);
			if (statusResponse != null) {
				if (LOG.isDebugEnabled()) {
					LOG.debug("getEntityResource():response.getStatus() = " + statusResponse.getStatus());
				}
				if (statusResponse.getStatus() == 200) {
					WebResource webResourceEntity = client.resource(atlasUrl + ATLAS_ENTITY_LIST_API_ENDPOINT);
					WebResource.Builder builder = webResourceEntity.getRequestBuilder();
					for (NewCookie cook : statusResponse.getCookies()) {
						builder = builder.cookie(cook);
					}
					resultResponse = builder.get(ClientResponse.class);
					if (resultResponse != null) {
						String jsonString = resultResponse.getEntity(String.class).toString();
						Gson gson = new Gson();
						List<String> responseResourceList = new ArrayList<String>();
						List<ResourceEntityResponse> resourceEntityResponses = gson.fromJson(jsonString,
								new TypeToken<List<ResourceEntityResponse>>() {
								}.getType());
						if (resourceEntityResponses != null) {
							for (ResourceEntityResponse resourceEntityResponse : resourceEntityResponses) {
								if (resourceEntityResponse != null) {
									responseResourceList.add(resourceEntityResponse.getName());
								}
							}
							if (responseResourceList != null) {
								for (String responseResource : responseResourceList) {
									if (responseResource != null) {
										if (existingResourceList != null
												&& existingResourceList.contains(responseResource)) {
											continue;
										}
										if (atlasResourceParameter == null || atlasResourceParameter.isEmpty()
												|| responseResource.startsWith(atlasResourceParameter)) {
											if (LOG.isDebugEnabled()) {
												LOG.debug("getEntityResource():Adding existing Resource "
														+ responseResource);
											}
											lret.add(responseResource);
										}
									}
								}
							}
						}
					}
				}
			}
		} catch (Throwable t) {
			String msgDesc = "Exception while getting Atlas getEntityResource List." + " URL : " + atlasUrl
					+ ATLAS_ENTITY_LIST_API_ENDPOINT;
			HadoopException hdpException = new HadoopException(msgDesc, t);
			LOG.error(msgDesc, t);
			hdpException.generateResponseDataMap(false, BaseClient.getMessage(t), msgDesc + errMsg, null, null);
			throw hdpException;
		} finally {
			if (statusResponse != null) {
				statusResponse.close();
			}
			if (resultResponse != null) {
				resultResponse.close();
			}
		}
		return lret;
	}

	public List<String> getTermResource(final String resourceNameMatching, final String atlasResourceParameter,
			List<String> existingResourceList, Client client) {
		List<String> lret = new ArrayList<String>();
		final String errMsg = errMessage;
		ClientResponse statusResponse = null;
		ClientResponse resultResponse = null;
		try {
			statusResponse = getStatusResponse(client);
			if (statusResponse != null) {
				if (LOG.isDebugEnabled()) {
					LOG.debug("getTermResource():response.getStatus()= " + statusResponse.getStatus());
				}
				if (statusResponse.getStatus() == 200) {
					WebResource webResourceTerm = client.resource(atlasUrl + ATLAS_LIST_TERM_API_ENDPOINT);
					WebResource.Builder builder = webResourceTerm.getRequestBuilder();
					for (NewCookie cook : statusResponse.getCookies()) {
						builder = builder.cookie(cook);
					}
					resultResponse = builder.get(ClientResponse.class);
					if (resultResponse != null) {
						String jsonString = resultResponse.getEntity(String.class).toString();
						Gson gson = new Gson();
						List<String> responseResourceList = new ArrayList<String>();
						List<ResourceTermResponse> resourceTermResponses = gson.fromJson(jsonString,
								new TypeToken<List<ResourceTermResponse>>() {
								}.getType());
						for (ResourceTermResponse resourceTermResponse : resourceTermResponses) {
							responseResourceList.add(resourceTermResponse.getName());
						}
						if (responseResourceList != null) {
							for (String responseResource : responseResourceList) {
								if (responseResource != null) {
									if (existingResourceList != null && existingResourceList.contains(responseResource)) {
										continue;
									}
									if (atlasResourceParameter == null || atlasResourceParameter.isEmpty()
											|| responseResource.startsWith(atlasResourceParameter)) {
										if (LOG.isDebugEnabled()) {
											LOG.debug("getTermResource():Adding existing Resource " + responseResource);
										}
										lret.add(responseResource);
									}
								}
							}
						}
					}
				}
			}
		} catch (Throwable t) {
			String msgDesc = "Exception while getting Atlas getTermResource List." + " URL : " + atlasUrl
					+ ATLAS_LIST_TERM_API_ENDPOINT;
			HadoopException hdpException = new HadoopException(msgDesc, t);
			LOG.error(msgDesc, t);
			hdpException.generateResponseDataMap(false, BaseClient.getMessage(t), msgDesc + errMsg, null, null);
			throw hdpException;
		} finally {
			if (statusResponse != null) {
				statusResponse.close();
			}
			if (resultResponse != null) {
				resultResponse.close();
			}
		}
		return lret;
	}

	public List<String> getTaxonomyResource(final String resourceNameMatching, final String atlasResourceParameter,
			List<String> existingResourceList, Client client) {
		List<String> lret = new ArrayList<String>();
		final String errMsg = errMessage;
		ClientResponse statusResponse = null;
		ClientResponse resultResponse = null;
		try {
			statusResponse = getStatusResponse(client);
			if (statusResponse != null) {
				if (LOG.isDebugEnabled()) {
					LOG.debug("getTaxonomyResource():response.getStatus()= " + statusResponse.getStatus());
				}
				if (statusResponse.getStatus() == 200) {
					WebResource webResourceTaxonomy = client.resource(atlasUrl + ATLAS_LIST_TAXONOMY_API_ENDPOINT);
					WebResource.Builder builder = webResourceTaxonomy.getRequestBuilder();
					for (NewCookie cook : statusResponse.getCookies()) {
						builder = builder.cookie(cook);
					}
					resultResponse = builder.get(ClientResponse.class);
					if (resultResponse != null) {
						String jsonString = resultResponse.getEntity(String.class).toString();
						Gson gson = new Gson();
						List<String> responseResourceList = new ArrayList<String>();
						List<ResourceTaxonomyResponse> resourceTaxonomyResponses = gson.fromJson(jsonString,
								new TypeToken<List<ResourceTaxonomyResponse>>() {
								}.getType());
						for (ResourceTaxonomyResponse resourceTaxonomyResponse : resourceTaxonomyResponses) {
							responseResourceList.add(resourceTaxonomyResponse.getName());
						}
						if (responseResourceList != null) {
							for (String responseResource : responseResourceList) {
								if (responseResource != null) {
									if (existingResourceList != null && existingResourceList.contains(responseResource)) {
										continue;
									}
									if (atlasResourceParameter == null || atlasResourceParameter.isEmpty()
											|| responseResource.startsWith(atlasResourceParameter)) {
										if (LOG.isDebugEnabled()) {
											LOG.debug("getTaxonomyResource():Adding existing Resource " + responseResource);
										}
										lret.add(responseResource);
									}
								}
							}
						}
					}
				}
			}
		} catch (Throwable t) {
			String msgDesc = "Exception while getting Atlas TaxonomyResource List." + " URL : " + atlasUrl
					+ ATLAS_LIST_TAXONOMY_API_ENDPOINT;
			HadoopException hdpException = new HadoopException(msgDesc, t);
			LOG.error(msgDesc, t);
			hdpException.generateResponseDataMap(false, BaseClient.getMessage(t), msgDesc + errMsg, null, null);
			throw hdpException;
		} finally {
			if (statusResponse != null) {
				statusResponse.close();
			}
			if (resultResponse != null) {
				resultResponse.close();
			}
		}
		return lret;
	}

	public List<String> getOperationResource(final String resourceNameMatching, final String atlasResourceParameter,
			List<String> existingResourceList, Client client) {
		List<String> lret = new ArrayList<String>();
		final String errMsg = errMessage;
		ClientResponse statusResponse = null;
		ClientResponse resultResponse = null;
		try {
			statusResponse = getStatusResponse(client);
			if (statusResponse != null) {
				if (LOG.isDebugEnabled()) {
					LOG.debug("getOperationResource():response.getStatus()= " + statusResponse.getStatus());
				}
				if (statusResponse.getStatus() == 200) {
					WebResource webResourceEntity = client.resource(atlasUrl + ATLAS_OPERATION_SEARCH_API_ENDPOINT);
					WebResource.Builder builder = webResourceEntity.getRequestBuilder();
					for (NewCookie cook : statusResponse.getCookies()) {
						builder = builder.cookie(cook);
					}
					resultResponse = builder.get(ClientResponse.class);
					if (resultResponse != null) {
						String jsonString = resultResponse.getEntity(String.class).toString();
						Gson gson = new Gson();
						List<String> responseResourceList = new ArrayList<String>();
						List<ResourceOperationResponse> resourceOperationResponses = gson.fromJson(jsonString,
								new TypeToken<List<ResourceOperationResponse>>() {
								}.getType());
						for (ResourceOperationResponse resourceOperationResponse : resourceOperationResponses) {
							List<Results> results = resourceOperationResponse.getResults();
							for (Results result : results) {
								responseResourceList.add(result.getResult());
							}
						}
						if (responseResourceList != null) {
							for (String responseResource : responseResourceList) {
								if (responseResource != null) {
									if (existingResourceList != null && existingResourceList.contains(responseResource)) {
										continue;
									}
									if (atlasResourceParameter == null || atlasResourceParameter.isEmpty()
											|| responseResource.startsWith(atlasResourceParameter)) {
										if (LOG.isDebugEnabled()) {
											LOG.debug("getOperationResource():Adding existing Resource "
													+ responseResource);
										}
										lret.add(responseResource);
									}
								}
							}
						}
					}
				}
			}
		} catch (Throwable t) {
			String msgDesc = "Exception while getting Atlas  OperationResource List." + " URL : " + atlasUrl
					+ ATLAS_OPERATION_SEARCH_API_ENDPOINT;
			HadoopException hdpException = new HadoopException(msgDesc, t);
			LOG.error(msgDesc, t);
			hdpException.generateResponseDataMap(false, BaseClient.getMessage(t), msgDesc + errMsg, null, null);
			throw hdpException;

		} finally {
			if (statusResponse != null) {
				statusResponse.close();
			}
			if (resultResponse != null) {
				resultResponse.close();
			}
		}
		return lret;
	}

	public static HashMap<String, Object> connectionTest(String serviceName, Map<String, String> configs) {

		String errMsg = errMessage;
		boolean connectivityStatus = false;
		HashMap<String, Object> responseData = new HashMap<String, Object>();
		AtlasClient atlasClient = getAtlasClient(serviceName, configs);
		List<String> strList = getAtlasResource(atlasClient, "", "", null);

		if (strList != null && strList.size() > 0) {
			if (LOG.isDebugEnabled()) {
				LOG.debug("TESTING Resource list size" + strList.size() + " Atlas Resource");
			}
			connectivityStatus = true;
		}
		if (connectivityStatus) {
			String successMsg = "ConnectionTest Successful";
			BaseClient.generateResponseDataMap(connectivityStatus, successMsg, successMsg, null, null, responseData);
		} else {
			String failureMsg = "Unable to retrieve any Atlas Resource using given parameters.";
			BaseClient.generateResponseDataMap(connectivityStatus, failureMsg, failureMsg + errMsg, null, null,
					responseData);
		}
		return responseData;
	}

	public static AtlasClient getAtlasClient(String serviceName, Map<String, String> configs) {
		AtlasClient atlasClient = null;
		if (LOG.isDebugEnabled()) {
			LOG.debug("Getting AtlasClient for datasource: " + serviceName);
		}
		String errMsg = errMessage;
		if (configs == null || configs.isEmpty()) {
			String msgDesc = "Could not connect as Connection ConfigMap is empty.";
			LOG.error(msgDesc);
			HadoopException hdpException = new HadoopException(msgDesc);
			hdpException.generateResponseDataMap(false, msgDesc, msgDesc + errMsg, null, null);
			throw hdpException;
		} else {
			atlasClient = new AtlasClient(serviceName, configs);
		}
		return atlasClient;
	}

	public static List<String> getAtlasResource(final AtlasClient atlasClient, String atlasResourceName,
			String atlasResourceParameter, List<String> existingAtlasResourceName) {

		List<String> resultList = new ArrayList<String>();
		String errMsg = errMessage;

		try {
			if (atlasClient == null) {
				String msgDesc = "Unable to get Atlas Resource : AtlasClient is null.";
				LOG.error(msgDesc);
				HadoopException hdpException = new HadoopException(msgDesc);
				hdpException.generateResponseDataMap(false, msgDesc, msgDesc + errMsg, null, null);
				throw hdpException;
			}

			if (atlasResourceName != null) {
				String finalAtlasResourceName = atlasResourceName.trim();
				resultList = atlasClient.getResourceList(finalAtlasResourceName, atlasResourceParameter,
						existingAtlasResourceName);
				if (resultList != null) {
					if (LOG.isDebugEnabled()) {
						LOG.debug("Returning list of " + resultList.size() + " Atlas Resources");
					}
				}
			}
		} catch (Throwable t) {
			String msgDesc = "getAtlasResource: Unable to get Atlas Resources.";
			LOG.error(msgDesc, t);
			HadoopException hdpException = new HadoopException(msgDesc);
			hdpException.generateResponseDataMap(false, BaseClient.getMessage(t), msgDesc + errMsg, null, null);
			throw hdpException;
		}
		return resultList;
	}

	public static <T> T timedTask(Callable<T> callableObj, long timeout, TimeUnit timeUnit) throws Exception {
		return callableObj.call();
	}
}
