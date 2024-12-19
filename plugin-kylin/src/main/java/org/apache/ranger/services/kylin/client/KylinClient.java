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

package org.apache.ranger.services.kylin.client;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.filter.HTTPBasicAuthFilter;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.http.HttpStatus;
import org.apache.ranger.plugin.client.BaseClient;
import org.apache.ranger.plugin.client.HadoopException;
import org.apache.ranger.plugin.util.PasswordUtils;
import org.apache.ranger.services.kylin.client.json.model.KylinProjectResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;

import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class KylinClient extends BaseClient {
    private static final Logger LOG = LoggerFactory.getLogger(KylinClient.class);

    private static final String EXPECTED_MIME_TYPE      = "application/json";
    private static final String KYLIN_LIST_API_ENDPOINT = "/kylin/api/projects";
    private static final String ERROR_MESSAGE           = " You can still save the repository and start creating policies, but you would not be able to use autocomplete for resource names. Check ranger_admin.log for more info.";

    private final String kylinUrl;
    private final String userName;
    private final String password;

    public KylinClient(String serviceName, Map<String, String> configs) {
        super(serviceName, configs, "kylin-client");

        this.kylinUrl = configs.get("kylin.url");
        this.userName = configs.get("username");
        this.password = configs.get("password");

        if (StringUtils.isEmpty(this.kylinUrl)) {
            LOG.error("No value found for configuration 'kylin.url'. Kylin resource lookup will fail.");
        }

        if (StringUtils.isEmpty(this.userName)) {
            LOG.error("No value found for configuration 'username'. Kylin resource lookup will fail.");
        }

        if (StringUtils.isEmpty(this.password)) {
            LOG.error("No value found for configuration 'password'. Kylin resource lookup will fail.");
        }

        LOG.debug("Kylin client is build with url [{}], user: [{}], password: [*********].", this.kylinUrl, this.userName);
    }

    public static Map<String, Object> connectionTest(String serviceName, Map<String, String> configs) {
        KylinClient  kylinClient        = getKylinClient(serviceName, configs);
        List<String> strList            = kylinClient.getProjectList(null, null);
        boolean      connectivityStatus = false;

        if (CollectionUtils.isNotEmpty(strList)) {
            LOG.debug("ConnectionTest list size{} kylin projects", strList.size());

            connectivityStatus = true;
        }

        Map<String, Object> responseData = new HashMap<>();

        if (connectivityStatus) {
            String successMsg = "ConnectionTest Successful";

            BaseClient.generateResponseDataMap(true, successMsg, successMsg, null, null, responseData);
        } else {
            String failureMsg = "Unable to retrieve any kylin projects using given parameters.";

            BaseClient.generateResponseDataMap(false, failureMsg, failureMsg + ERROR_MESSAGE, null, null, responseData);
        }

        return responseData;
    }

    public static KylinClient getKylinClient(String serviceName, Map<String, String> configs) {
        KylinClient kylinClient;

        LOG.debug("Getting KylinClient for datasource: {}", serviceName);

        if (MapUtils.isEmpty(configs)) {
            String msgDesc = "Could not connect kylin as connection configMap is empty.";

            LOG.error(msgDesc);

            HadoopException hdpException = new HadoopException(msgDesc);

            hdpException.generateResponseDataMap(false, msgDesc, msgDesc + ERROR_MESSAGE, null, null);

            throw hdpException;
        } else {
            kylinClient = new KylinClient(serviceName, configs);
        }

        return kylinClient;
    }

    public List<String> getProjectList(final String projectMatching, final List<String> existingProjects) {
        LOG.debug("Getting kylin project list for projectMatching: {}, existingProjects: {}", projectMatching, existingProjects);

        Subject subj = getLoginSubject();

        if (subj == null) {
            return Collections.emptyList();
        }

        List<String> ret = Subject.doAs(subj, (PrivilegedAction<List<String>>) () -> {
            ClientResponse response                     = getClientResponse(kylinUrl, userName, password);
            List<KylinProjectResponse> projectResponses = getKylinProjectResponse(response);

            if (CollectionUtils.isEmpty(projectResponses)) {
                return Collections.emptyList();
            }

            return getProjectFromResponse(projectMatching, existingProjects, projectResponses);
        });

        LOG.debug("Getting kylin project list result: {}", ret);

        return ret;
    }

    private static ClientResponse getClientResponse(String kylinUrl, String userName, String password) {
        ClientResponse response  = null;
        String[]       kylinUrls = kylinUrl.trim().split("[,;]");

        if (ArrayUtils.isEmpty(kylinUrls)) {
            return null;
        }

        Client client       = Client.create();
        String decryptedPwd = PasswordUtils.getDecryptPassword(password);

        client.addFilter(new HTTPBasicAuthFilter(userName, decryptedPwd));

        for (String currentUrl : kylinUrls) {
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
                String msgDesc = "Exception while getting kylin response, kylinUrl: " + url;

                LOG.error(msgDesc, t);
            }
        }

        client.destroy();

        return response;
    }

    private List<KylinProjectResponse> getKylinProjectResponse(ClientResponse response) {
        List<KylinProjectResponse> projectResponses;

        try {
            if (response != null) {
                if (response.getStatus() == HttpStatus.SC_OK) {
                    String jsonString = response.getEntity(String.class);
                    Gson   gson       = new GsonBuilder().setPrettyPrinting().create();

                    projectResponses = gson.fromJson(jsonString, new TypeToken<List<KylinProjectResponse>>() {}.getType());
                } else {
                    String msgDesc = "Unable to get a valid response for " + "expected mime type : [" + EXPECTED_MIME_TYPE + "], kylinUrl: " + kylinUrl + " - got http response code " + response.getStatus();

                    LOG.error(msgDesc);

                    HadoopException hdpException = new HadoopException(msgDesc);

                    hdpException.generateResponseDataMap(false, msgDesc, msgDesc + ERROR_MESSAGE, null, null);

                    throw hdpException;
                }
            } else {
                String msgDesc = "Unable to get a valid response for " + "expected mime type : [" + EXPECTED_MIME_TYPE + "], kylinUrl: " + kylinUrl + " - got null response.";

                LOG.error(msgDesc);

                HadoopException hdpException = new HadoopException(msgDesc);

                hdpException.generateResponseDataMap(false, msgDesc, msgDesc + ERROR_MESSAGE, null, null);

                throw hdpException;
            }
        } catch (HadoopException he) {
            throw he;
        } catch (Throwable t) {
            String          msgDesc      = "Exception while getting kylin project response, kylinUrl: " + kylinUrl;
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
        LOG.debug("getProjectResponse():calling {}", url);

        WebResource    webResource = client.resource(url);
        ClientResponse response    = webResource.accept(EXPECTED_MIME_TYPE).get(ClientResponse.class);

        if (response != null) {
            LOG.debug("getProjectResponse():response.getStatus()= {}", response.getStatus());

            if (response.getStatus() != HttpStatus.SC_OK) {
                LOG.warn("getProjectResponse():response.getStatus()= {} for URL {}, failed to get kylin project list.", response.getStatus(), url);

                String jsonString = response.getEntity(String.class);

                LOG.warn(jsonString);
            }
        }

        return response;
    }

    private static List<String> getProjectFromResponse(String projectMatching, List<String> existingProjects, List<KylinProjectResponse> projectResponses) {
        List<String> projectNames = new ArrayList<>();

        for (KylinProjectResponse project : projectResponses) {
            String projectName = project.getName();

            if (CollectionUtils.isNotEmpty(existingProjects) && existingProjects.contains(projectName)) {
                continue;
            }

            if (StringUtils.isEmpty(projectMatching) || projectMatching.startsWith("*") || projectName.toLowerCase().startsWith(projectMatching.toLowerCase())) {
                LOG.debug("getProjectFromResponse(): Adding kylin project {}", projectName);

                projectNames.add(projectName);
            }
        }

        return projectNames;
    }
}
