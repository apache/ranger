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

import org.apache.hadoop.thirdparty.com.google.common.io.Resources;
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
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

public class TestNiFiRegistryClient {
    private NiFiRegistryClient registryClient;

    @BeforeEach
    public void setup() throws IOException {
        final URL    responseFile      = TestNiFiRegistryClient.class.getResource("/resources-response.json");
        final String resourcesResponse = Resources.toString(responseFile, StandardCharsets.UTF_8);

        registryClient = new NiFiRegistryClient("http://localhost:18080/nifi-registry-api/policiesresources", null) {
            @Override
            protected Client buildClient() {
                return new MockNiFiRegistryClient(resourcesResponse, 200).buildClient();
            }
        };
    }

    @Test
    public void testGetResourcesNoUserInput() throws Exception {
        ResourceLookupContext resourceLookupContext = Mockito.mock(ResourceLookupContext.class);
        when(resourceLookupContext.getUserInput()).thenReturn("");

        final List<String> expectedResources = new ArrayList<>();
        expectedResources.add("/policies");
        expectedResources.add("/tenants");
        expectedResources.add("/proxy");
        expectedResources.add("/actuator");
        expectedResources.add("/swagger");
        expectedResources.add("/buckets");
        expectedResources.add("/buckets/fc0625e4-a9ae-4277-bab7-a2bc984f6c4f");
        expectedResources.add("/buckets/0b5edba5-da83-4839-b64a-adf5f21abaf4");

        List<String> resources = registryClient.getResources(resourceLookupContext);
        Assertions.assertNotNull(resources);
        Assertions.assertEquals(expectedResources.size(), resources.size());

        Assertions.assertTrue(resources.containsAll(expectedResources));
    }

    @Test
    public void testGetResourcesWithUserInputBeginning() throws Exception {
        ResourceLookupContext resourceLookupContext = Mockito.mock(ResourceLookupContext.class);
        when(resourceLookupContext.getUserInput()).thenReturn("/p");

        final List<String> expectedResources = new ArrayList<>();
        expectedResources.add("/policies");
        expectedResources.add("/proxy");

        List<String> resources = registryClient.getResources(resourceLookupContext);
        Assertions.assertNotNull(resources);
        Assertions.assertEquals(expectedResources.size(), resources.size());

        Assertions.assertTrue(resources.containsAll(expectedResources));
    }

    @Test
    public void testGetResourcesWithUserInputAnywhere() throws Exception {
        ResourceLookupContext resourceLookupContext = Mockito.mock(ResourceLookupContext.class);
        when(resourceLookupContext.getUserInput()).thenReturn("ant");

        final List<String> expectedResources = new ArrayList<>();
        expectedResources.add("/tenants");

        List<String> resources = registryClient.getResources(resourceLookupContext);
        Assertions.assertNotNull(resources);
        Assertions.assertEquals(expectedResources.size(), resources.size());

        Assertions.assertTrue(resources.containsAll(expectedResources));
    }

    @Test
    public void testGetResourcesErrorResponse() throws Exception {
        final String errorMsg = "unknown error";

        registryClient = new NiFiRegistryClient("http://localhost:18080/nifi-registry-api/policiesresources", null) {
            @Override
            protected Client buildClient() {
                return new MockNiFiRegistryClient(errorMsg, Response.Status.BAD_REQUEST.getStatusCode()).buildClient();
            }
        };

        ResourceLookupContext resourceLookupContext = Mockito.mock(ResourceLookupContext.class);
        when(resourceLookupContext.getUserInput()).thenReturn("");

        try {
            registryClient.getResources(resourceLookupContext);
            Assertions.fail("should have thrown exception");
        } catch (Exception e) {
            Assertions.assertTrue(e.getMessage().contains(errorMsg));
        }
    }

    @Test
    public void testConnectionTestSuccess() {
        HashMap<String, Object> ret = registryClient.connectionTest();
        Assertions.assertNotNull(ret);
        Assertions.assertEquals(NiFiRegistryClient.SUCCESS_MSG, ret.get("message"));
    }

    @Test
    public void testConnectionTestFailure() {
        final String errorMsg = "unknown error";

        registryClient = new NiFiRegistryClient("http://localhost:18080/nifi-registry-api/policiesresources", null) {
            @Override
            protected Client buildClient() {
                return new MockNiFiRegistryClient(errorMsg, Response.Status.BAD_REQUEST.getStatusCode()).buildClient();
            }
        };

        HashMap<String, Object> ret = registryClient.connectionTest();
        Assertions.assertNotNull(ret);
        Assertions.assertEquals(NiFiRegistryClient.FAILURE_MSG, ret.get("message"));
    }

    /**
     * Extend NiFiRegistryClient to return mock responses.
     */
    private static final class MockNiFiRegistryClient {
        private final int    statusCode;
        private final String responseEntity;

        private MockNiFiRegistryClient(String responseEntity, int statusCode) {
            this.statusCode = statusCode;
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
                    when(mockResponse.readEntity(InputStream.class)).thenReturn(new ByteArrayInputStream(responseEntity.getBytes(StandardCharsets.UTF_8)));
                }
            } catch (Exception e) {
            }

            return mockClient;
        }
    }
}
