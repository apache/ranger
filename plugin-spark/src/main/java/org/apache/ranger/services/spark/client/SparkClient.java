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

package org.apache.ranger.services.spark.client;

import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.security.auth.Subject;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.http.HttpStatus;
import org.apache.log4j.Logger;
import org.apache.ranger.plugin.client.BaseClient;
import org.apache.ranger.plugin.client.HadoopException;
import org.apache.ranger.plugin.util.PasswordUtils;
import org.apache.ranger.services.spark.client.json.model.SparkProjectResponse;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.filter.HTTPBasicAuthFilter;

public class SparkClient extends BaseClient {

	private static final Logger LOG = Logger.getLogger(SparkClient.class);

	private static final String EXPECTED_MIME_TYPE = "application/json";

	private static final String KYLIN_LIST_API_ENDPOINT = "/spark/api/projects";

	private static final String ERROR_MESSAGE = " You can still save the repository and start creating "
			+ "policies, but you would not be able to use autocomplete for "
			+ "resource names. Check ranger_admin.log for more info.";

	private String sparkUrl;
	private String userName;
	private String password;

	public SparkClient(String serviceName, Map<String, String> configs) {

		super(serviceName, configs, "spark-client");

		this.sparkUrl = configs.get("spark.url");
		this.userName = configs.get("username");
		this.password = configs.get("password");

		if (StringUtils.isEmpty(this.sparkUrl)) {
			LOG.error("No value found for configuration 'spark.url'. Spark resource lookup will fail.");
		}
		if (StringUtils.isEmpty(this.userName)) {
			LOG.error("No value found for configuration 'username'. Spark resource lookup will fail.");
		}
		if (StringUtils.isEmpty(this.password)) {
			LOG.error("No value found for configuration 'password'. Spark resource lookup will fail.");
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("Spark client is build with url [" + this.sparkUrl + "], user: [" + this.userName
					+ "], password: [" + "*********" + "].");
		}
	}

	public List<String> getProjectList(final String projectMatching, final List<String> existingProjects) {

		if (LOG.isDebugEnabled()) {
			LOG.debug("Getting spark project list for projectMatching: " + projectMatching + ", existingProjects: "
					+ existingProjects);
		}
		Subject subj = getLoginSubject();
		if (subj == null) {
			return Collections.emptyList();
		}

		List<String> ret = Subject.doAs(subj, new PrivilegedAction<List<String>>() {

			@Override
			public List<String> run() {

				ClientResponse response = getClientResponse(sparkUrl, userName, password);

				List<SparkProjectResponse> projectResponses = getSparkProjectResponse(response);

				if (CollectionUtils.isEmpty(projectResponses)) {
					return Collections.emptyList();
				}

				List<String> projects = getProjectFromResponse(projectMatching, existingProjects, projectResponses);

				return projects;
			}
		});

		if (LOG.isDebugEnabled()) {
			LOG.debug("Getting spark project list result: " + ret);
		}
		return ret;
	}

	private static ClientResponse getClientResponse(String sparkUrl, String userName, String password) {
		ClientResponse response = null;
		String[] sparkUrls = sparkUrl.trim().split("[,;]");
		if (ArrayUtils.isEmpty(sparkUrls)) {
			return null;
		}

		Client client = Client.create();
		String decryptedPwd = PasswordUtils.getDecryptPassword(password);
		client.addFilter(new HTTPBasicAuthFilter(userName, decryptedPwd));

		for (String currentUrl : sparkUrls) {
			if (StringUtils.isBlank(currentUrl)) {
				continue;
			}

			String url = currentUrl.trim() + KYLIN_LIST_API_ENDPOINT;
			try {
				response = getProjectResponse(url, client);

				if (response != null) {
					if (response.getStatus() == HttpStatus.SC_OK) {
						break;
					} else {
						response.close();
					}
				}
			} catch (Throwable t) {
				String msgDesc = "Exception while getting spark response, sparkUrl: " + url;
				LOG.error(msgDesc, t);
			}
		}
		client.destroy();

		return response;
	}

	private List<SparkProjectResponse> getSparkProjectResponse(ClientResponse response) {
		List<SparkProjectResponse> projectResponses = null;
		try {
			if (response != null && response.getStatus() == HttpStatus.SC_OK) {
				String jsonString = response.getEntity(String.class);
				Gson gson = new GsonBuilder().setPrettyPrinting().create();

				projectResponses = gson.fromJson(jsonString, new TypeToken<List<SparkProjectResponse>>() {
				}.getType());
			} else {
				String msgDesc = "Unable to get a valid response for " + "expected mime type : [" + EXPECTED_MIME_TYPE
						+ "], sparkUrl: " + sparkUrl + " - got null response.";
				LOG.error(msgDesc);
				HadoopException hdpException = new HadoopException(msgDesc);
				hdpException.generateResponseDataMap(false, msgDesc, msgDesc + ERROR_MESSAGE, null, null);
				throw hdpException;
			}
		} catch (HadoopException he) {
			throw he;
		} catch (Throwable t) {
			String msgDesc = "Exception while getting spark project response, sparkUrl: " + sparkUrl;
			HadoopException hdpException = new HadoopException(msgDesc, t);

			LOG.error(msgDesc, t);

			hdpException.generateResponseDataMap(false, BaseClient.getMessage(t), msgDesc + ERROR_MESSAGE, null, null);
			throw hdpException;

		} finally {
			if (response != null) {
				response.close();
			}
		}
		return projectResponses;
	}

	private static ClientResponse getProjectResponse(String url, Client client) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("getProjectResponse():calling " + url);
		}

		WebResource webResource = client.resource(url);

		ClientResponse response = webResource.accept(EXPECTED_MIME_TYPE).get(ClientResponse.class);

		if (response != null) {
			if (LOG.isDebugEnabled()) {
				LOG.debug("getProjectResponse():response.getStatus()= " + response.getStatus());
			}
			if (response.getStatus() != HttpStatus.SC_OK) {
				LOG.warn("getProjectResponse():response.getStatus()= " + response.getStatus() + " for URL " + url
						+ ", failed to get spark project list.");
				String jsonString = response.getEntity(String.class);
				LOG.warn(jsonString);
			}
		}
		return response;
	}

	private static List<String> getProjectFromResponse(String projectMatching, List<String> existingProjects,
			List<SparkProjectResponse> projectResponses) {
		List<String> projcetNames = new ArrayList<String>();
		for (SparkProjectResponse project : projectResponses) {
			String projectName = project.getName();
			if (CollectionUtils.isNotEmpty(existingProjects) && existingProjects.contains(projectName)) {
				continue;
			}
			if (StringUtils.isEmpty(projectMatching) || projectMatching.startsWith("*")
					|| projectName.toLowerCase().startsWith(projectMatching.toLowerCase())) {
				if (LOG.isDebugEnabled()) {
					LOG.debug("getProjectFromResponse(): Adding spark project " + projectName);
				}
				projcetNames.add(projectName);
			}
		}
		return projcetNames;
	}

	public static Map<String, Object> connectionTest(String serviceName, Map<String, String> configs) {
		SparkClient sparkClient = getSparkClient(serviceName, configs);
		List<String> strList = sparkClient.getProjectList(null, null);

		boolean connectivityStatus = false;
		if (CollectionUtils.isNotEmpty(strList)) {
			if (LOG.isDebugEnabled()) {
				LOG.debug("ConnectionTest list size" + strList.size() + " spark projects");
			}
			connectivityStatus = true;
		}

		Map<String, Object> responseData = new HashMap<String, Object>();
		if (connectivityStatus) {
			String successMsg = "ConnectionTest Successful";
			BaseClient.generateResponseDataMap(connectivityStatus, successMsg, successMsg, null, null, responseData);
		} else {
			String failureMsg = "Unable to retrieve any spark projects using given parameters.";
			BaseClient.generateResponseDataMap(connectivityStatus, failureMsg, failureMsg + ERROR_MESSAGE, null, null,
					responseData);
		}

		return responseData;
	}

	public static SparkClient getSparkClient(String serviceName, Map<String, String> configs) {
		SparkClient sparkClient = null;
		if (LOG.isDebugEnabled()) {
			LOG.debug("Getting SparkClient for datasource: " + serviceName);
		}
		if (MapUtils.isEmpty(configs)) {
			String msgDesc = "Could not connect spark as connection configMap is empty.";
			LOG.error(msgDesc);
			HadoopException hdpException = new HadoopException(msgDesc);
			hdpException.generateResponseDataMap(false, msgDesc, msgDesc + ERROR_MESSAGE, null, null);
			throw hdpException;
		} else {
			sparkClient = new SparkClient(serviceName, configs);
		}
		return sparkClient;
	}
}
