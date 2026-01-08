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
package org.apache.ranger.services.seatunnel.client;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.security.KeyStore;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.ranger.plugin.client.BaseClient;
import org.apache.ranger.plugin.util.JsonUtilsV2;
import org.apache.ranger.plugin.util.PasswordUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

public class SeatunnelClient {

    private static final String MIME_TYPE = "application/json";
    private static final Logger LOG = LoggerFactory.getLogger(SeatunnelClient.class);
    private static final String LOGIN_ENDPOINT = "user/login";
    private static final String WORKSPACE_ENDPOINT = "resources/workspace";
    private static final String DATASOURCE_ENDPOINT = "resources/datasource";
    private static final String JOB_DEFINITION_ENDPOINT = "resources/job_definition";
    private static final String USER_ENDPOINT = "resources/user";
    private static final String VIRTUAL_TABLE_ENDPOINT = "resources/virtual_table";
    private static final String TOKEN_HEADER = "token";

    private HttpClient client;
    private final Map<String, String> configs;
    private final String seatunnelWebUrl;
    private final String userName;
    private final String password;
    private String apiToken;

    public SeatunnelClient(Map<String, String> configs) throws Exception {
        this.configs = configs;
        String url = configs.get("seatunnel.web.url");
        this.userName = configs.get("username");
        this.password = configs.get("password");
        if (StringUtils.isEmpty(url) || StringUtils.isEmpty(this.userName) || StringUtils.isEmpty(this.password)) {
            throw new IllegalArgumentException("URL, username, and password cannot be null or empty.");
        }
        this.seatunnelWebUrl = url.endsWith("/") ? url : url + "/";
        initHttpClient();
    }

    private void initHttpClient() throws Exception {
        this.client = HttpClient.newBuilder()
                .sslContext(getSSLContext())
                .build();
    }

    private boolean isSSLEnabled(String url) {
        return !StringUtils.isEmpty(url) && url.toLowerCase().startsWith("https");
    }

    private SSLContext getSSLContext() throws Exception {
        String protocol = getOrDefault("seatunnel.web.ssl.protocol", "TLSv1.2");
        if (!isSSLEnabled(seatunnelWebUrl)) {
            SSLContext sslContext = SSLContext.getInstance(protocol);
            sslContext.init(null, null, new java.security.SecureRandom());
            return sslContext;
        }
        String trustStorePath = configs.get("seatunnel.web.ssl.truststore");
        if (StringUtils.isEmpty(trustStorePath) || !new File(trustStorePath).exists()) {
            throw new IllegalArgumentException("TrustStore path is either empty or does not exist.");
        }
        String trustStorePassword = configs.get("seatunnel.web.ssl.truststore.password");
        if (StringUtils.isEmpty(trustStorePassword)) {
            throw new IllegalArgumentException("TrustStore password is empty.");
        }
        String trustStoreType = getOrDefault("seatunnel.web.ssl.truststore.type", "JKS");

        // Load the truststore
        KeyStore trustStore = KeyStore.getInstance(trustStoreType);
        try (var trustStoreStream = new FileInputStream(trustStorePath)) {
            trustStore.load(trustStoreStream, trustStorePassword.toCharArray());
        }

        String trustManagerFactoryAlgorithm = getOrDefault("seatunnel.web.ssl.TrustManagerFactory.algorithm", TrustManagerFactory.getDefaultAlgorithm());
        // Initialize TrustManagerFactory with the truststore
        TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(trustManagerFactoryAlgorithm);
        trustManagerFactory.init(trustStore);

        // Create and initialize the SSLContext
        SSLContext sslContext = SSLContext.getInstance(protocol);
        sslContext.init(null, trustManagerFactory.getTrustManagers(), new java.security.SecureRandom());
        return sslContext;
    }

    private String getOrDefault(String key, String defaultValue) {
        return StringUtils.defaultIfEmpty(configs.get(key), defaultValue);
    }

    public void login() throws IOException, InterruptedException {
        String decryptedPwd = PasswordUtils.decryptPassword(password);
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(seatunnelWebUrl + LOGIN_ENDPOINT))
                .POST(HttpRequest.BodyPublishers.ofString("{\"username\":\"" + userName + "\", \"password\":\"" + decryptedPwd + "\"}"))
                .header("Content-Type", MIME_TYPE)
                .build();
        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        handleLoginResponse(response);
    }

    private void handleLoginResponse(HttpResponse<String> response) throws IOException {
        if (response.statusCode() == 200) {
            JsonNode responseBody = JsonUtilsV2.getMapper().readTree(response.body());
            if (responseBody.get("success").asBoolean()) {
                JsonNode data = responseBody.get("data");
                this.apiToken = data.get(TOKEN_HEADER).asText();
            } else {
                throw new IOException("Login failed: " + responseBody.get("msg").asText());
            }
        } else {
            throw new IOException("Login failed: " + response.statusCode());
        }
    }

    public List<String> getWorkspaceList(String searchName, List<String> existingResource) throws IOException, InterruptedException {
        return getListFromEndpoint(WORKSPACE_ENDPOINT, "workspace names", null, searchName, existingResource);
    }

    public List<String> getDatasourceList(String workspaceName, String searchName, List<String> existingResource) throws IOException, InterruptedException {
        return getListFromEndpoint(DATASOURCE_ENDPOINT, "datasource names", workspaceName, searchName, existingResource);
    }

    public List<String> getJobList(String workspaceName, String searchName, List<String> existingResource) throws IOException, InterruptedException {
        return getListFromEndpoint(JOB_DEFINITION_ENDPOINT, "job names", workspaceName, searchName, existingResource);
    }

    public List<String> getUserList(String searchName, List<String> existingResource) throws IOException, InterruptedException {
        return getListFromEndpoint(USER_ENDPOINT, "user names", null, searchName, existingResource);
    }

    public List<String> getVirtualTableList(String workspaceName, String searchName, List<String> existingResource) throws IOException, InterruptedException {
        return getListFromEndpoint(VIRTUAL_TABLE_ENDPOINT, "virtual table names", workspaceName, searchName, existingResource);
    }

    private List<String> getListFromEndpoint(String endpoint, String listName, String workspaceName, String searchName, List<String> existingResources) throws IOException, InterruptedException {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(buildUrl(seatunnelWebUrl + endpoint, workspaceName, searchName)))
                .GET()
                .header(TOKEN_HEADER, getToken())
                .build();

        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        return handleListResponse(response, listName, existingResources);
    }

    private String buildUrl(String baseUrl, String workspaceName, String searchName) {
        StringBuilder url = new StringBuilder(baseUrl);
        if (workspaceName != null || searchName != null) {
            url.append("?");
            if (workspaceName != null) {
                url.append("workspaceName=").append(workspaceName);
            }
            if (searchName != null) {
                if (workspaceName != null) {
                    url.append("&");
                }
                url.append("searchName=").append(searchName);
            }
        }
        return url.toString();
    }

    private String getToken() throws IOException, InterruptedException {
        if (apiToken == null) {
            login();
        }
        return apiToken;
    }

    private List<String> handleListResponse(HttpResponse<String> response, String listName, List<String> existingResources) throws IOException {
        if (response.statusCode() == 200) {
            JsonNode responseBody = JsonUtilsV2.getMapper().readTree(response.body());
            if (responseBody.get("success").asBoolean()) {
                List<String> list = new ArrayList<>();
                JsonNode dataArray = responseBody.get("data");
                for (JsonNode dataNode : dataArray) {
                    String resourceName = dataNode.asText();
                    if (existingResources != null && existingResources.contains(resourceName)) {
                        continue;
                    }
                    list.add(resourceName);
                }
                return list;
            } else {
                throw new IOException("Failed to get " + listName + ": " + responseBody.get("msg").asText());
            }
        } else {
            throw new IOException("Failed to get " + listName + ": " + response.statusCode());
        }
    }

    public static Map<String, Object> connectionTest(String serviceName, Map<String, String> configs) {
        String errMsg = " You can still save the repository and start creating policies, but you would not be able to use autocomplete for resource names. Check ranger_admin.log for more info.";
        Map<String, Object> responseData = new HashMap<>();
        try {
            SeatunnelClient seatunnelClient = getSeatunnelClient(serviceName, configs);
            seatunnelClient.getWorkspaceList("", null);
            String successMsg = "ConnectionTest Successful";
            String description = "Connected and fetched workspace list successfully.";
            BaseClient.generateResponseDataMap(true, successMsg, description, null, null, responseData);
        } catch (Exception e) {
            String failureMsg = "Unable to connect and get list of workspaces using given parameters.";
            String exceptionMessage = e.getMessage() == null ? e.toString() : e.getMessage();
            BaseClient.generateResponseDataMap(false, exceptionMessage, failureMsg + errMsg, null, null, responseData);
        }

        return responseData;
    }

    public static SeatunnelClient getSeatunnelClient(String serviceName, Map<String, String> configs) throws Exception {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Getting Seatunnel Client for ServiceName: {}", serviceName);
            LOG.debug("configMap: {}", configs);
        }
        if (configs == null || configs.isEmpty()) {
            String msgDesc = "Could not connect as Connection ConfigMap is empty.";
            LOG.error(msgDesc);
            throw new IllegalArgumentException(msgDesc);
        }

        return new SeatunnelClient(configs);
    }

    public void close() {
        apiToken = null;
    }

}