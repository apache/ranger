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

package org.apache.ranger.plugin.contextenricher.externalretrievers;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.hadoop.util.StringUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpStatus;
import org.apache.http.NameValuePair;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.methods.RequestBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;
import org.apache.ranger.authorization.utils.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GetFromURL {
    private static final Logger LOG = LoggerFactory.getLogger(GetFromURL.class);

    public Map<String, Map<String, String>> getFromURL(String url, String configFile) throws Exception {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> getFromURL(url={}, configFile={})", url, configFile);
        }

        String         token   = getBearerToken(configFile);
        HttpUriRequest request = RequestBuilder.get().setUri(url)
                                                     .setHeader(HttpHeaders.AUTHORIZATION, token)
                                                     .setHeader(HttpHeaders.CONTENT_TYPE, "text/plain")
                                                     .build();
        Map<String, Map<String, String>> ret;

        try (CloseableHttpClient   httpClient = HttpClients.createDefault();
             CloseableHttpResponse response   = httpClient.execute(request)) {
            if (response == null) {
                throw new IOException("getFromURL(" + url + ") failed: null response");
            }

            int statusCode = response.getStatusLine().getStatusCode();

            if (statusCode != HttpStatus.SC_OK) {
                throw new IOException("getFromURL(" + url + ") failed: http status=" + response.getStatusLine());
            }

            HttpEntity                             httpEntity     = response.getEntity();
            String                                 stringResult   = EntityUtils.toString(httpEntity);
            Map                                    resultMap      = JsonUtils.jsonToObject(stringResult, Map.class);
            Map<String, Map<String, List<String>>> userAttrValues = (Map<String, Map<String, List<String>>>) resultMap.get("body");

            ret = toUserAttributes(userAttrValues);

            // and ensure response body is fully consumed
            EntityUtils.consume(httpEntity);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== getFromURL(url={}, configFile={}): ret={}", url, configFile, ret);
        }

        return ret;
    }

    private String getBearerToken(String configFile) throws Exception {
        String   secrets    = getSecretsFromFile(configFile);
        JsonNode jsonObject = JsonUtils.getMapper().readTree(secrets);
        String   tokenURL   = jsonObject.get("tokenUrl").asText();
        List<Map<String, String>> headers = JsonUtils.getMapper().convertValue(jsonObject.get("headers"), new TypeReference<List<Map<String, String>>>(){});
        List<Map<String, String>> params = JsonUtils.getMapper().convertValue(jsonObject.get("params"), new TypeReference<List<Map<String, String>>>(){});

        List<NameValuePair>       nvPairs    = new ArrayList<>();
        HttpPost                  httpPost   = new HttpPost(tokenURL);

        // add headers to httpPost object:
        for (Map<String, String> header : headers) {
            for (Map.Entry<String, String> e : header.entrySet()) {
                httpPost.setHeader(e.getKey(), e.getValue());
            }
        }

        // add params to httpPost entity:
        for (Map<String, String> param : params) {
            for (Map.Entry<String, String> e : param.entrySet()) {
                nvPairs.add(new BasicNameValuePair(e.getKey(), e.getValue()));
            }
        }

        httpPost.setEntity(new UrlEncodedFormEntity(nvPairs, StandardCharsets.UTF_8));

        String ret;

        // execute httpPost:
        try (CloseableHttpClient   httpClient = HttpClients.createDefault();
             CloseableHttpResponse response   = httpClient.execute(httpPost)) {
            if (response == null) {
                throw new IOException("getBearerToken(" + configFile + ") failed: null response");
            }

            int statusCode = response.getStatusLine().getStatusCode();

            if (statusCode != HttpStatus.SC_OK) {
                throw new IOException("getBearerToken(" + configFile + ") failed: http status=" + response.getStatusLine());
            }

            HttpEntity          httpEntity   = response.getEntity();
            String              stringResult = EntityUtils.toString(httpEntity);
            Map<String, Object> resultMap    = JsonUtils.jsonToObject(stringResult, Map.class);
            String              token        = resultMap.get("access_token").toString();

            ret = "Bearer " + token;

            // and ensure response body is fully consumed
            EntityUtils.consume(httpEntity);
        }

        return ret;
    }

    private Map<String, Map<String, String>> toUserAttributes(Map<String, Map<String, List<String>>> userAttrValues){
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> toUserAttributes(userAttrValues={})", userAttrValues);
        }

        Map<String, Map<String, String>> ret = new HashMap<>();

        for (Map.Entry<String, Map<String, List<String>>> userEntry : userAttrValues.entrySet()) {
            String                    user       = userEntry.getKey();
            Map<String, List<String>> attrValues = userEntry.getValue();
            Map<String, String>       userAttrs  = new HashMap<>();

            for (Map.Entry<String, List<String>> attrEntry : attrValues.entrySet()) {
                String       attrName = attrEntry.getKey();
                List<String> values   = attrEntry.getValue();

                userAttrs.put(attrName, String.join(",", values));
            }

            ret.put(user, userAttrs);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== toUserAttributes(userAttrValues={}): ret={}", userAttrValues, ret);
        }

        return ret;
    }

    private String getSecretsFromFile(String configFile) throws IOException {
        String ret = decodeSecrets(new String(Files.readAllBytes(Paths.get(configFile))));

        verifyToken(ret);

        return ret;
    }

    private String decodeSecrets(String encodedSecrets) {
        return new String(Base64.getDecoder().decode(encodedSecrets));
    }

    private void verifyToken(String secrets) throws IOException {
        String     errorMessage = "";
        JsonNode jsonObject = JsonUtils.getMapper().readTree(secrets);

        // verify all necessary items are there
        if (jsonObject.get("tokenUrl") == null) {
            errorMessage += "tokenUrl must be specified in the config file; ";
        }

        if (jsonObject.get("headers") == null) {
            errorMessage += "headers must be specified in the config file; ";
        } else { // verify that Content-type, if included, is application/x-www-form-urlencoded
            JsonNode headers = jsonObject.get("headers");

            for (JsonNode header : headers) {
                if (header.has("Content-Type") && !StringUtils.equalsIgnoreCase(header.get("Content-Type").textValue(), "application/x-www-form-urlencoded")) {
                    errorMessage += "Content-Type, if specified, must be \"application/x-www-form-urlencoded\"; ";
                }
            }
        }

        if (jsonObject.get("params") == null) {
            errorMessage += "params must be specified in the config file; ";
        }

        if (!errorMessage.equals("")) {
            throw new IOException(errorMessage);
        }
    }
}


