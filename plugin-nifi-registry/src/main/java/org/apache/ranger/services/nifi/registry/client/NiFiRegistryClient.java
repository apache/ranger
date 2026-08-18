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
package org.apache.ranger.services.nifi.registry.client;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.ranger.plugin.client.BaseClient;
import org.apache.ranger.plugin.service.ResourceLookupContext;
import org.apache.ranger.plugin.util.RangerDefaultHostnameVerifier;
import org.apache.ranger.plugin.util.RangerJersey2ClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;
import javax.ws.rs.ProcessingException;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;

/**
 * Client to communicate with NiFi Registry and retrieve available resources.
 */
public class NiFiRegistryClient {
    private static final Logger LOG = LoggerFactory.getLogger(NiFiRegistryClient.class);
    static final String SUCCESS_MSG = "ConnectionTest Successful";
    static final String FAILURE_MSG = "Unable to retrieve any resources using given parameters. ";
    private final String           url;
    private final SSLContext       sslContext;
    private final HostnameVerifier hostnameVerifier;
    private final ObjectMapper     mapper = new ObjectMapper();
    private final Client client;

    public NiFiRegistryClient(final String url, final SSLContext sslContext) {
        this.url              = url;
        this.sslContext       = sslContext;
        this.hostnameVerifier = new RangerDefaultHostnameVerifier();
        this.client = buildClient();
    }

    protected Client buildClient() {
        // Use RangerJersey2ClientBuilder instead of unsafe ClientBuilder.newBuilder() to prevent MOXy usage
        if (sslContext != null) {
            return RangerJersey2ClientBuilder.createSecureClient(sslContext, hostnameVerifier, 30000, 30000);
        } else {
            return RangerJersey2ClientBuilder.createStandardClient();
        }
    }

    public HashMap<String, Object> connectionTest() {
        String                  errMsg       = "";
        boolean                 connectivityStatus;
        HashMap<String, Object> responseData = new HashMap<>();

        try {
            Response response = getResponse();
            LOG.debug("Got response from NiFi with status code {}", response.getStatus());

            if (Response.Status.OK.getStatusCode() == response.getStatus()) {
                connectivityStatus = true;
            } else {
                connectivityStatus = false;
                errMsg = "Status Code = " + response.getStatus();
                // Read the error message from the response entity
                try (InputStream is = response.readEntity(InputStream.class)) {
                    errMsg += ": " + IOUtils.toString(is, "UTF-8");
                }
            }
        } catch (ProcessingException | WebApplicationException e) {
            LOG.error("Connection to NiFi failed due to {}", e.getMessage(), e);
            connectivityStatus = false;
            errMsg = Optional.ofNullable(e.getMessage()).orElse("Unknown error");
        } catch (Exception e) {
            LOG.error("Connection to NiFi failed due to {}", e.getMessage(), e);
            connectivityStatus = false;
            errMsg             = e.getMessage();
        }

        if (connectivityStatus) {
            BaseClient.generateResponseDataMap(connectivityStatus, SUCCESS_MSG, SUCCESS_MSG, null, null, responseData);
        } else {
            String errorMsg = FAILURE_MSG + errMsg;
            BaseClient.generateResponseDataMap(connectivityStatus, FAILURE_MSG, errorMsg, null, null, responseData);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("Response Data - {}", responseData);
        }

        return responseData;
    }

    public List<String> getResources(ResourceLookupContext context) throws Exception {
        Response response = getResponse();

        if (Response.Status.OK.getStatusCode() != response.getStatus()) {
            String errorMsg = response.readEntity(String.class);
            response.close();
            throw new Exception("Unable to retrieve resources from NiFi Registry due to: " + errorMsg);
        }

        JsonNode rootNode = mapper.readTree(response.readEntity(InputStream.class));
        response.close();

        if (rootNode == null) {
            throw new Exception("Unable to retrieve resources from NiFi Registry");
        }

        List<String> identifiers = rootNode.findValuesAsText("identifier");

        final String userInput = context.getUserInput();
        if (StringUtils.isBlank(userInput)) {
            return identifiers;
        } else {
            List<String> filteredIdentifiers = new ArrayList<>();

            for (String identifier : identifiers) {
                if (identifier.contains(userInput)) {
                    filteredIdentifiers.add(identifier);
                }
            }

            return filteredIdentifiers;
        }
    }

    public String getUrl() {
        return url;
    }

    public SSLContext getSslContext() {
        return sslContext;
    }

    public HostnameVerifier getHostnameVerifier() {
        return hostnameVerifier;
    }

    protected WebTarget getWebTarget() {
        return client.target(url);
    }

    protected Response getResponse() {
        WebTarget webTarget = getWebTarget();
        return webTarget.request(MediaType.APPLICATION_JSON).get();
    }
}
