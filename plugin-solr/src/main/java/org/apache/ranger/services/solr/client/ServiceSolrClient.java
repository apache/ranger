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

package org.apache.ranger.services.solr.client;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.apache.ranger.plugin.client.BaseClient;
import org.apache.ranger.plugin.service.ResourceLookupContext;
import org.apache.ranger.plugin.util.PasswordUtils;
import org.apache.ranger.plugin.util.TimedEventUtil;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrResponse;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.CoreAdminRequest;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.response.CoreAdminResponse;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.params.CoreAdminParams.CoreAdminAction;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.commons.collections.CollectionUtils;

public class ServiceSolrClient {
	private static final Logger LOG = Logger.getLogger(ServiceSolrClient.class);

	enum RESOURCE_TYPE {
		COLLECTION, FIELD
	}

	private static final String errMessage = " You can still save the repository and start creating "
			+ "policies, but you would not be able to use autocomplete for "
			+ "resource names. Check server logs for more info.";

	private static final String COLLECTION_KEY = "collection";
	private static final String FIELD_KEY = "field";
	private static final long LOOKUP_TIMEOUT_SEC = 5;

	private String username;
	private String password;
	private SolrClient solrClient = null;
	private boolean isSolrCloud = true;

	public ServiceSolrClient(SolrClient solrClient,
			boolean isSolrCloud, Map<String, String> configs) {
		this.solrClient = solrClient;
		this.isSolrCloud = isSolrCloud;
		this.username = configs.get("username");
		this.password = configs.get("password");
	}

	public Map<String, Object> connectionTest() throws Exception {
		String errMsg = errMessage;
		Map<String, Object> responseData = new HashMap<String, Object>();

		try {
			getCollectionList(null);
			// If it doesn't throw exception, then assume the instance is
			// reachable
			String successMsg = "ConnectionTest Successful";
			BaseClient.generateResponseDataMap(true, successMsg,
					successMsg, null, null, responseData);
		} catch (IOException e) {
			LOG.error("Error connecting to Solr. solrClient=" + solrClient, e);
			String failureMsg = "Unable to connect to Solr instance."
					+ e.getMessage();
			BaseClient.generateResponseDataMap(false, failureMsg,
					failureMsg + errMsg, null, null, responseData);

		}

		return responseData;
	}

	public List<String> getCollectionList(List<String> ignoreCollectionList)
			throws Exception {
		if (!isSolrCloud) {
			return getCoresList(ignoreCollectionList);
		}

		CollectionAdminRequest<?> request = new CollectionAdminRequest.List();
		String decPassword = getDecryptedPassword();
        if (username != null && decPassword != null) {
		    request.setBasicAuthCredentials(username, decPassword);
		}
		SolrResponse response = request.process(solrClient);

		List<String> list = new ArrayList<String>();
		List<String> responseCollectionList = (ArrayList<String>)response.getResponse().get("collections");
		if(CollectionUtils.isEmpty(responseCollectionList)) {
			return list;
		}
		for (String responseCollection : responseCollectionList) {
			if (ignoreCollectionList == null
					|| !ignoreCollectionList.contains(responseCollection)) {
				list.add(responseCollection);
			}
		}
		return list;
	}

	public List<String> getCoresList(List<String> ignoreCollectionList)
			throws Exception {
		CoreAdminRequest request = new CoreAdminRequest();
		request.setAction(CoreAdminAction.STATUS);
		String decPassword = getDecryptedPassword();
        if (username != null && decPassword != null) {
		    request.setBasicAuthCredentials(username, decPassword);
		}
		CoreAdminResponse cores = request.process(solrClient);
		// List of the cores
		List<String> coreList = new ArrayList<String>();
		for (int i = 0; i < cores.getCoreStatus().size(); i++) {
			if (ignoreCollectionList == null
					|| !ignoreCollectionList.contains(cores.getCoreStatus()
							.getName(i))) {
				coreList.add(cores.getCoreStatus().getName(i));
			}
		}
		return coreList;
	}

	public List<String> getFieldList(String collection,
			List<String> ignoreFieldList) throws Exception {
		// TODO: Best is to get the collections based on the collection value
		// which could contain wild cards
		String queryStr = "";
		if (collection != null && !collection.isEmpty()) {
			queryStr += "/" + collection;
		}
		queryStr += "/schema/fields";
		SolrQuery query = new SolrQuery();
		query.setRequestHandler(queryStr);
		QueryRequest req = new QueryRequest(query);
		String decPassword = getDecryptedPassword();
		if (username != null && decPassword != null) {
		    req.setBasicAuthCredentials(username, decPassword);
		}
		QueryResponse response = req.process(solrClient);

		List<String> fieldList = new ArrayList<String>();
		if (response != null && response.getStatus() == 0) {
			@SuppressWarnings("unchecked")
			List<SimpleOrderedMap<String>> fields = (ArrayList<SimpleOrderedMap<String>>) response
					.getResponse().get("fields");
			for (SimpleOrderedMap<String> fmap : fields) {
				String fieldName = fmap.get("name");
				if (ignoreFieldList == null
						|| !ignoreFieldList.contains(fieldName)) {
					fieldList.add(fieldName);
				}
			}
		} else {
			LOG.error("Error getting fields for collection=" + collection
					+ ", response=" + response);
		}
		return fieldList;
	}

	public List<String> getFieldList(List<String> collectionList,
			List<String> ignoreFieldList) throws Exception {

		Set<String> fieldSet = new LinkedHashSet<String>();
		if (collectionList == null || collectionList.size() == 0) {
			return getFieldList((String) null, ignoreFieldList);
		}
		for (String collection : collectionList) {
			try {
				fieldSet.addAll(getFieldList(collection, ignoreFieldList));
			} catch (Exception ex) {
				LOG.error("Error getting fields.", ex);
			}
		}
		return new ArrayList<String>(fieldSet);
	}

	public List<String> getResources(ResourceLookupContext context) {

		String userInput = context.getUserInput();
		String resource = context.getResourceName();
		Map<String, List<String>> resourceMap = context.getResources();
		List<String> resultList = null;
		List<String> collectionList = null;
		List<String> fieldList = null;

		RESOURCE_TYPE lookupResource = RESOURCE_TYPE.COLLECTION;

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== getResources() UserInput: \""
					+ userInput + "\" resource : " + resource
					+ " resourceMap: " + resourceMap);
		}

		if (userInput != null && resource != null) {
			if (resourceMap != null && !resourceMap.isEmpty()) {
				collectionList = resourceMap.get(COLLECTION_KEY);
				fieldList = resourceMap.get(FIELD_KEY);
			}
			switch (resource.trim().toLowerCase()) {
			case COLLECTION_KEY:
				lookupResource = RESOURCE_TYPE.COLLECTION;
				break;
			case FIELD_KEY:
				lookupResource = RESOURCE_TYPE.FIELD;
				break;
			default:
				break;
			}
		}

		if (userInput != null) {
			try {
				Callable<List<String>> callableObj = null;
				final String userInputFinal = userInput;

				final List<String> finalCollectionList = collectionList;
				final List<String> finalFieldList = fieldList;

				if (lookupResource == RESOURCE_TYPE.COLLECTION) {
					// get the collection list for given Input
					callableObj = new Callable<List<String>>() {
						@Override
						public List<String> call() {
							List<String> retList = new ArrayList<String>();
							try {
								List<String> list = getCollectionList(finalCollectionList);
								if (userInputFinal != null
										&& !userInputFinal.isEmpty()) {
									for (String value : list) {
										if (value.startsWith(userInputFinal)) {
											retList.add(value);
										}
									}
								} else {
									retList.addAll(list);
								}
							} catch (Exception ex) {
								LOG.error("Error getting collection.", ex);
							}
							return retList;
						};
					};
				} else if (lookupResource == RESOURCE_TYPE.FIELD) {
					callableObj = new Callable<List<String>>() {
						@Override
						public List<String> call() {
							List<String> retList = new ArrayList<String>();
							try {
								List<String> list = getFieldList(
										finalCollectionList, finalFieldList);
								if (userInputFinal != null
										&& !userInputFinal.isEmpty()) {
									for (String value : list) {
										if (value.startsWith(userInputFinal)) {
											retList.add(value);
										}
									}
								} else {
									retList.addAll(list);
								}
							} catch (Exception ex) {
								LOG.error("Error getting collection.", ex);
							}
							return retList;
						};
					};
				}
				// If we need to do lookup
				if (callableObj != null) {
					synchronized (this) {
						resultList = TimedEventUtil.timedTask(callableObj,
								LOOKUP_TIMEOUT_SEC, TimeUnit.SECONDS);
					}
				}
			} catch (Exception e) {
				LOG.error("Unable to get hive resources.", e);
			}
		}

		return resultList;
	}

	private String getDecryptedPassword() {
	    String decryptedPwd = null;
        try {
            decryptedPwd = PasswordUtils.decryptPassword(password);
        } catch (Exception ex) {
            LOG.info("Password decryption failed; trying Solr connection with received password string");
            decryptedPwd = null;
        } finally {
            if (decryptedPwd == null) {
                decryptedPwd = password;
            }
        }

        return decryptedPwd;
	}
}
