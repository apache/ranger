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

package org.apache.ranger.services.elasticsearch.client;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.http.HttpStatus;
import org.apache.ranger.plugin.client.BaseClient;
import org.apache.ranger.plugin.client.HadoopException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;
import javax.ws.rs.core.MediaType;

import java.lang.reflect.Type;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ElasticsearchClient extends BaseClient {
    private static final Logger LOG = LoggerFactory.getLogger(ElasticsearchClient.class);

    private static final String ELASTICSEARCH_INDEX_API_ENDPOINT = "/_all";

    private final String elasticsearchUrl;
    private final String userName;

    public ElasticsearchClient(String serviceName, Map<String, String> configs) {
        super(serviceName, configs, "elasticsearch-client");

        this.elasticsearchUrl = configs.get("elasticsearch.url");
        this.userName         = configs.get("username");

        if (StringUtils.isEmpty(this.elasticsearchUrl)) {
            LOG.error("No value found for configuration 'elasticsearch.url'. Elasticsearch resource lookup will fail.");
        }

        if (StringUtils.isEmpty(this.userName)) {
            LOG.error("No value found for configuration 'username'. Elasticsearch resource lookup will fail.");
        }

        LOG.debug("Elasticsearch client is build with url: [{}], user: [{}].", this.elasticsearchUrl, this.userName);
    }

    public static Map<String, Object> connectionTest(String serviceName, Map<String, String> configs) {
        ElasticsearchClient elasticsearchClient = getElasticsearchClient(serviceName, configs);
        List<String>        indexList           = elasticsearchClient.getIndexList(null, null);

        boolean connectivityStatus = false;

        if (CollectionUtils.isNotEmpty(indexList)) {
            LOG.debug("ConnectionTest list size {} elasticsearch indices.", indexList.size());

            connectivityStatus = true;
        }

        Map<String, Object> responseData = new HashMap<>();

        if (connectivityStatus) {
            String successMsg = "ConnectionTest Successful.";

            BaseClient.generateResponseDataMap(true, successMsg, successMsg, null, null, responseData);
        } else {
            String failureMsg = "Unable to retrieve any elasticsearch indices using given parameters.";

            BaseClient.generateResponseDataMap(false, failureMsg, failureMsg + DEFAULT_ERROR_MESSAGE, null, null, responseData);
        }

        return responseData;
    }

    public static ElasticsearchClient getElasticsearchClient(String serviceName, Map<String, String> configs) {
        ElasticsearchClient elasticsearchClient;

        LOG.debug("Getting elasticsearchClient for datasource: {}", serviceName);

        if (MapUtils.isEmpty(configs)) {
            String msgDesc = "Could not connect elasticsearch as connection configMap is empty.";

            LOG.error(msgDesc);

            HadoopException hdpException = new HadoopException(msgDesc);

            hdpException.generateResponseDataMap(false, msgDesc, msgDesc + DEFAULT_ERROR_MESSAGE, null, null);

            throw hdpException;
        } else {
            elasticsearchClient = new ElasticsearchClient(serviceName, configs);
        }

        return elasticsearchClient;
    }

    public List<String> getIndexList(final String indexMatching, final List<String> existingIndices) {
        LOG.debug("Get elasticsearch index list for indexMatching: {}, existingIndices: {}", indexMatching, existingIndices);

        Subject subj = getLoginSubject();

        if (subj == null) {
            return Collections.emptyList();
        }

        List<String> ret = Subject.doAs(subj, (PrivilegedAction<List<String>>) () -> {
            String indexApi;

            if (StringUtils.isNotEmpty(indexMatching)) {
                indexApi = '/' + indexMatching;

                if (!indexApi.endsWith("*")) {
                    indexApi += "*";
                }
            } else {
                indexApi = ELASTICSEARCH_INDEX_API_ENDPOINT;
            }

            ClientResponse      response        = getClientResponse(elasticsearchUrl, indexApi, userName);
            Map<String, Object> index2detailMap = getElasticsearchResourceResponse(response, new TypeToken<HashMap<String, Object>>() {}.getType());

            if (MapUtils.isEmpty(index2detailMap)) {
                return Collections.emptyList();
            }

            Set<String> indexResponses = index2detailMap.keySet();

            if (CollectionUtils.isEmpty(indexResponses)) {
                return Collections.emptyList();
            }

            return filterResourceFromResponse(indexMatching, existingIndices, new ArrayList<>(indexResponses));
        });

        LOG.debug("Get elasticsearch index list result: {}", ret);

        return ret;
    }

    private static ClientResponse getClientResponse(String elasticsearchUrl, String elasticsearchApi, String userName) {
        String[] elasticsearchUrls = elasticsearchUrl.trim().split("[,;]");

        if (ArrayUtils.isEmpty(elasticsearchUrls)) {
            return null;
        }

        ClientResponse response = null;
        Client         client   = Client.create();

        for (String currentUrl : elasticsearchUrls) {
            if (StringUtils.isBlank(currentUrl)) {
                continue;
            }

            String url = currentUrl.trim() + elasticsearchApi;

            try {
                response = getClientResponse(url, client, userName);

                if (response != null) {
                    if (response.getStatus() == HttpStatus.SC_OK) {
                        break;
                    } else {
                        response.close();
                    }
                }
            } catch (Throwable t) {
                String msgDesc = "Exception while getting elasticsearch response, elasticsearchUrl: " + url;

                LOG.error(msgDesc, t);
            }
        }

        client.destroy();

        return response;
    }

    private static ClientResponse getClientResponse(String url, Client client, String userName) {
        LOG.debug("getClientResponse():calling {}", url);

        ClientResponse response = client.resource(url).accept(MediaType.APPLICATION_JSON).header("userName", userName).get(ClientResponse.class);

        if (response != null) {
            LOG.debug("getClientResponse():response.getStatus()= {}", response.getStatus());

            if (response.getStatus() != HttpStatus.SC_OK) {
                LOG.warn("getClientResponse():response.getStatus()= {} for URL {}, failed to get elasticsearch resource list, response= {}", response.getStatus(), url, response.getEntity(String.class));
            }
        }

        return response;
    }

    private <T> T getElasticsearchResourceResponse(ClientResponse response, Type type) {
        T resource;

        try {
            if (response != null && response.getStatus() == HttpStatus.SC_OK) {
                String jsonString = response.getEntity(String.class);
                Gson   gson       = new GsonBuilder().setPrettyPrinting().create();

                resource = gson.fromJson(jsonString, type);
            } else {
                String msgDesc = "Unable to get a valid response for " + "expected mime type : [" + MediaType.APPLICATION_JSON + "], elasticsearchUrl: " + elasticsearchUrl + " - got null response.";

                LOG.error(msgDesc);

                HadoopException hdpException = new HadoopException(msgDesc);

                hdpException.generateResponseDataMap(false, msgDesc, msgDesc + DEFAULT_ERROR_MESSAGE, null, null);

                throw hdpException;
            }
        } catch (HadoopException he) {
            throw he;
        } catch (Throwable t) {
            String msgDesc = "Exception while getting elasticsearch resource response, elasticsearchUrl: " + elasticsearchUrl;

            HadoopException hdpException = new HadoopException(msgDesc, t);

            LOG.error(msgDesc, t);

            hdpException.generateResponseDataMap(false, BaseClient.getMessage(t), msgDesc + DEFAULT_ERROR_MESSAGE, null, null);

            throw hdpException;
        } finally {
            if (response != null) {
                response.close();
            }
        }

        return resource;
    }

    private static List<String> filterResourceFromResponse(String resourceMatching, List<String> existingResources, List<String> resourceResponses) {
        List<String> resources = new ArrayList<>();

        for (String resourceResponse : resourceResponses) {
            if (CollectionUtils.isNotEmpty(existingResources) && existingResources.contains(resourceResponse)) {
                continue;
            }

            if (StringUtils.isEmpty(resourceMatching) || resourceMatching.startsWith("*") || resourceResponse.toLowerCase().startsWith(resourceMatching.toLowerCase())) {
                LOG.debug("filterResourceFromResponse(): Adding elasticsearch resource {}", resourceResponse);

                resources.add(resourceResponse);
            }
        }

        return resources;
    }
}
