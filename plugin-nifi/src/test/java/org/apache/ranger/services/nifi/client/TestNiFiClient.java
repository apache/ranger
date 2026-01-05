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
package org.apache.ranger.services.nifi.client;

import org.apache.ranger.plugin.service.ResourceLookupContext;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.mockito.stubbing.OngoingStubbing;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Response;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

public class TestNiFiClient {
    private static final String RESOURCES_RESPONSE = "{\n" +
            "  \"revision\": {\n" +
            "    \"clientId\": \"0daac173-025c-4aa7-b644-97f7b10435d2\"\n" +
            "  },\n" +
            "  \"resources\": [\n" +
            "    {\n" +
            "      \"identifier\": \"/system\",\n" +
            "      \"name\": \"System\"\n" +
            "    },\n" +
            "    {\n" +
            "      \"identifier\": \"/controller\",\n" +
            "      \"name\": \"Controller\"\n" +
            "    },\n" +
            "    {\n" +
            "      \"identifier\": \"/flow\",\n" +
            "      \"name\": \"NiFi Flow\"\n" +
            "    },\n" +
            "    {\n" +
            "      \"identifier\": \"/provenance\",\n" +
            "      \"name\": \"Provenance\"\n" +
            "    },\n" +
            "    {\n" +
            "      \"identifier\": \"/proxy\",\n" +
            "      \"name\": \"Proxy User Requests\"\n" +
            "    },\n" +
            "    {\n" +
            "      \"identifier\": \"/resources\",\n" +
            "      \"name\": \"NiFi Resources\"\n" +
            "    }\n" +
            "  ]\n" +
            "}";

    private NiFiClient niFiClient;

    @BeforeEach
    public void setup() {
        niFiClient = new NiFiClient("http://localhost:8080/nifi-api/resources", null) {
            @Override
            protected Client buildClient() {
                return new MockNiFiClient(RESOURCES_RESPONSE, 200).buildClient();
            }
        };
    }

    @Test
    public void testGetResourcesNoUserInput() throws Exception {
        ResourceLookupContext resourceLookupContext = Mockito.mock(ResourceLookupContext.class);
        when(resourceLookupContext.getUserInput()).thenReturn("");

        final List<String> expectedResources = new ArrayList<>();
        expectedResources.add("/system");
        expectedResources.add("/controller");
        expectedResources.add("/flow");
        expectedResources.add("/provenance");
        expectedResources.add("/proxy");
        expectedResources.add("/resources");

        List<String> resources = niFiClient.getResources(resourceLookupContext);
        Assertions.assertNotNull(resources);
        Assertions.assertEquals(expectedResources.size(), resources.size());

        resources.removeAll(expectedResources);
        Assertions.assertEquals(0, resources.size());
    }

    @Test
    public void testGetResourcesWithUserInputBeginning() throws Exception {
        ResourceLookupContext resourceLookupContext = Mockito.mock(ResourceLookupContext.class);
        when(resourceLookupContext.getUserInput()).thenReturn("/pr");

        final List<String> expectedResources = new ArrayList<>();
        expectedResources.add("/provenance");
        expectedResources.add("/proxy");

        List<String> resources = niFiClient.getResources(resourceLookupContext);
        Assertions.assertNotNull(resources);
        Assertions.assertEquals(expectedResources.size(), resources.size());

        resources.removeAll(expectedResources);
        Assertions.assertEquals(0, resources.size());
    }

    @Test
    public void testGetResourcesWithUserInputAnywhere() throws Exception {
        ResourceLookupContext resourceLookupContext = Mockito.mock(ResourceLookupContext.class);
        when(resourceLookupContext.getUserInput()).thenReturn("trol");

        final List<String> expectedResources = new ArrayList<>();
        expectedResources.add("/controller");

        List<String> resources = niFiClient.getResources(resourceLookupContext);
        Assertions.assertNotNull(resources);
        Assertions.assertEquals(expectedResources.size(), resources.size());

        resources.removeAll(expectedResources);
        Assertions.assertEquals(0, resources.size());
    }

    @Test
    public void testGetResourcesErrorResponse() throws NoSuchAlgorithmException, KeyManagementException {
        final String errorMsg = "unknown error";

        niFiClient = new NiFiClient("http://localhost:8080/nifi-api/resources", null) {
            @Override
            protected Client buildClient() {
                return new MockNiFiClient(errorMsg, Response.Status.BAD_REQUEST.getStatusCode()).buildClient();
            }
        };

        ResourceLookupContext resourceLookupContext = Mockito.mock(ResourceLookupContext.class);
        when(resourceLookupContext.getUserInput()).thenReturn("");

        try {
            niFiClient.getResources(resourceLookupContext);
            Assertions.fail("should have thrown exception");
        } catch (Exception e) {
            Assertions.assertTrue(e.getMessage().contains(errorMsg));
        }
    }

    @Test
    public void testConnectionTestSuccess() {
        HashMap<String, Object> ret = niFiClient.connectionTest();
        Assertions.assertNotNull(ret);
        Assertions.assertEquals(NiFiClient.SUCCESS_MSG, ret.get("message"));
    }

    @Test
    public void testConnectionTestFailure() {
        final String errorMsg = "unknown error";

        niFiClient = new NiFiClient("http://localhost:8080/nifi-api/resources", null) {
            @Override
            protected Client buildClient() {
                return new MockNiFiClient(errorMsg, Response.Status.BAD_REQUEST.getStatusCode()).buildClient();
            }
        };

        HashMap<String, Object> ret = niFiClient.connectionTest();
        Assertions.assertNotNull(ret);
        Assertions.assertEquals(NiFiClient.FAILURE_MSG, ret.get("message"));
    }

    /**
     * Extend NiFiClient to return mock responses.
     */
    private static final class MockNiFiClient {
        private final int    statusCode;
        private final String responseEntity;

        public MockNiFiClient(String responseEntity, int statusCode) {
            this.statusCode     = statusCode;
            this.responseEntity = responseEntity;
        }

        public Client buildClient() {
            Client mockClient = Mockito.mock(Client.class);
            WebTarget mockWebTarget = Mockito.mock(WebTarget.class);
            Invocation.Builder mockBuilder = Mockito.mock(Invocation.Builder.class);
            Response mockResponse = Mockito.mock(Response.class);

            when(mockClient.target(anyString())).thenReturn(mockWebTarget);

            when(mockWebTarget.request(any(String.class))).thenReturn(mockBuilder);

            OngoingStubbing<Response> ongoingStubbing = when(mockBuilder.get());
            try {
                if (statusCode == 200) {
                    ongoingStubbing.thenReturn(mockResponse);
                    when(mockResponse.getStatus()).thenReturn(statusCode);
                    when(mockResponse.readEntity(any(Class.class))).thenReturn(new ByteArrayInputStream(responseEntity.getBytes(StandardCharsets.UTF_8)));
                } else {
                    ongoingStubbing.thenReturn(mockResponse);
                    when(mockResponse.getStatus()).thenReturn(statusCode);
                    when(mockResponse.readEntity(String.class)).thenReturn(responseEntity);
                }
            } catch (Exception e) {
            }
            return mockClient;
        }
    }
}
