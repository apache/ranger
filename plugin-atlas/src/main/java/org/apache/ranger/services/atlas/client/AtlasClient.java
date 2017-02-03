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

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.core.util.MultivaluedMapImpl;

public class AtlasClient extends BaseClient {

	private static final Logger LOG = Logger.getLogger(AtlasClient.class);
	private static final String EXPECTED_MIME_TYPE = "application/json";
	private static final String ATLAS_STATUS_API_ENDPOINT = "/j_spring_security_check";
	private static final String ATLAS_LIST_TERM_API_ENDPOINT = "/api/atlas/types";
	private static final String errMessage =  " You can still save the repository and start creating "
											  + "policies, but you would not be able to use autocomplete for "
											  + "resource names. Check ranger_admin.log for more info.";

	private String atlasUrl;
	private String userName;
	private String password;

	public  AtlasClient(String serviceName, Map<String, String> configs) {

		super(serviceName,configs,"atlas-client");

		this.atlasUrl = configs.get("atlas.rest.address");
		this.userName = configs.get("username");
		this.password = configs.get("password");
		if (this.atlasUrl == null || this.atlasUrl.isEmpty()) {
			LOG.error("No value found for configuration 'atlas.rest.address'. Atlas resource lookup will fail");
        }
		if (this.userName == null || this.userName.isEmpty()) {
            LOG.error("No value found for configuration 'usename'. Atlas resource lookup will fail");
        }
		if (this.password == null || this.password.isEmpty()) {
            LOG.error("No value found for configuration 'password'. Atlas resource lookup will fail");
        }

		if (LOG.isDebugEnabled()) {
			LOG.debug("Atlas Client is build with url [" + this.atlasUrl + "] user: [" + this.userName + "], password: [" + "*********" + "]");
		}
	}

	public List<String> getTermList( String termNameMatching, List<String> existingTermList) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("Getting Atlas Terms list for termNameMatching : " + termNameMatching);
		}
		final String errMsg = errMessage;
		List<String> ret = null;

		Callable<List<String>> callableAtlasTermListGetter = new Callable<List<String>>() {

			@Override
			public List<String> call() {
				List<String> atlasTermListGetter = null;

				Subject subj = getLoginSubject();

				if (subj != null) {
					atlasTermListGetter = Subject.doAs(subj, new PrivilegedAction<List<String>>() {

					@Override
					public List<String> run() {

						List<String> lret = new ArrayList<String>();

						String statusUrl = atlasUrl + ATLAS_STATUS_API_ENDPOINT;
						String resultUrl = atlasUrl + ATLAS_LIST_TERM_API_ENDPOINT;

						Client client = null;
						ClientResponse statusResponse = null;
						ClientResponse resultResponse = null;

						try {
							client = Client.create();
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
								statusResponse = webResource.type("application/x-www-form-urlencoded").post(
										ClientResponse.class, formData);
							} catch (Exception e) {
								String msgDesc = "Unable to get a valid statusResponse for "
										+ "expected mime type : [" + EXPECTED_MIME_TYPE
										+ "] URL : " + statusUrl + " - got null response.";
								LOG.error(msgDesc);
							}

							if (LOG.isDebugEnabled()) {
								LOG.debug("getTermList():calling " + statusUrl);
							}

							if (statusResponse != null) {
								if (LOG.isDebugEnabled()) {
								LOG.debug("getTermList():response.getStatus()= " + statusResponse.getStatus());
                                                               }
								if (statusResponse.getStatus() == 200) {
									WebResource webResource2 = client
											.resource(resultUrl);
									WebResource.Builder builder = webResource2.getRequestBuilder();
									for (NewCookie cook : statusResponse.getCookies()) {			                                                                       builder = builder.cookie(cook);
									}
									resultResponse = builder.get(ClientResponse.class);
									lret.add(resultResponse.getEntity(String.class));
								} else{
									LOG.info("getTermList():response.getStatus()= " + statusResponse.getStatus() + " for URL " + statusUrl + ", so returning null list");
									LOG.info(statusResponse.getEntity(String.class));
									lret = null;
								}
							}
						}  catch (Throwable t) {
							lret = null;
							String msgDesc = "Exception while getting Atlas Term List."
									+ " URL : " + statusUrl;
							HadoopException hdpException = new HadoopException(msgDesc,
										t);
							LOG.error(msgDesc, t);
							hdpException.generateResponseDataMap(false,
									BaseClient.getMessage(t), msgDesc + errMsg, null,
									null);
							throw hdpException;

						} finally {
							if (statusResponse != null) {
								statusResponse.close();
							}
							if (resultResponse != null) {
								resultResponse.close();
							}

							if (client != null) {
								client.destroy();
							}
						}
						return lret;
					}
				  } );
				}
				return atlasTermListGetter;
			  }
			};
		try {
			ret = timedTask(callableAtlasTermListGetter, 5, TimeUnit.SECONDS);
		} catch ( Throwable t) {
			LOG.error("Unable to get Atlas Terms list from [" + atlasUrl + "]", t);
			String msgDesc = "Unable to get a valid response for "
					+ "expected mime type : [" + EXPECTED_MIME_TYPE
					+ "] URL : " + atlasUrl;
			HadoopException hdpException = new HadoopException(msgDesc,
					t);
			LOG.error(msgDesc, t);

			hdpException.generateResponseDataMap(false,
					BaseClient.getMessage(t), msgDesc + errMsg, null,
					null);
			throw hdpException;
		}
		return ret;
	}

	public static HashMap<String, Object> connectionTest(String serviceName,
			Map<String, String> configs) {

		String errMsg = errMessage;
		boolean connectivityStatus = false;
		HashMap<String, Object> responseData = new HashMap<String, Object>();

		AtlasClient AtlasClient = getAtlasClient(serviceName,
				configs);
		List<String> strList = getAtlasTermResource(AtlasClient, "",null);

		if (strList != null && strList.size() > 0 ) {
			if (LOG.isDebugEnabled()) {
				LOG.debug("TESTING Term list size" + strList.size() + " Atlas Terms");
			}
			connectivityStatus = true;
		}

		if (connectivityStatus) {
			String successMsg = "ConnectionTest Successful";
			BaseClient.generateResponseDataMap(connectivityStatus, successMsg,
					successMsg, null, null, responseData);
		} else {
			String failureMsg = "Unable to retrieve any Atlas Terms using given parameters.";
			BaseClient.generateResponseDataMap(connectivityStatus, failureMsg,
					failureMsg + errMsg, null, null, responseData);
		}

		return responseData;
	}

	public static AtlasClient getAtlasClient(String serviceName,
			Map<String, String> configs) {
		AtlasClient AtlasClient = null;
		if (LOG.isDebugEnabled()) {
			LOG.debug("Getting AtlasClient for datasource: " + serviceName);
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
			AtlasClient = new AtlasClient (serviceName, configs);
		}
		return AtlasClient;
	}

	public static List<String> getAtlasTermResource (final AtlasClient atlasClient,
			String atlasTermName, List<String> existingAtlasTermName) {

		List<String> resultList = new ArrayList<String>();
		String errMsg = errMessage;

		try {
			if (atlasClient == null) {
				String msgDesc = "Unable to get Atlas Terms : AtlasClient is null.";
				LOG.error(msgDesc);
				HadoopException hdpException = new HadoopException(msgDesc);
				hdpException.generateResponseDataMap(false, msgDesc, msgDesc
						+ errMsg, null, null);
				throw hdpException;
			}

			if (atlasTermName != null) {
				String finalAtlasTermName = atlasTermName.trim();
				resultList = atlasClient
						.getTermList(finalAtlasTermName,existingAtlasTermName);
				if (resultList != null) {
					if (LOG.isDebugEnabled()) {
						LOG.debug("Returning list of " + resultList.size() + " Atlas Terms");
					}
				}
			}
		}catch (Throwable t) {
			String msgDesc = "getAtlasResource: Unable to get Atlas resources.";
			LOG.error(msgDesc, t);
			HadoopException hdpException = new HadoopException(msgDesc);

			hdpException.generateResponseDataMap(false,
					BaseClient.getMessage(t), msgDesc + errMsg, null, null);
			throw hdpException;
		}

		return resultList;
	}

	public static <T> T timedTask(Callable<T> callableObj, long timeout,
			TimeUnit timeUnit) throws Exception {
		return callableObj.call();
	}
}
