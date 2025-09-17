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

import com.google.common.io.Resources;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import org.apache.ranger.plugin.service.ResourceLookupContext;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLSession;
import javax.net.ssl.SSLSessionContext;
import javax.ws.rs.core.Response;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.Certificate;
import java.security.cert.CertificateParsingException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestNiFiRegistryClient {
    private NiFiRegistryClient registryClient;
    private static final String HOSTNAME = "example.com";
    private static final String POLICIES_RESOURCES = "http://localhost:18080/nifi-registry-api/policiesresources";

    @Before
    public void setup() throws IOException, NoSuchAlgorithmException, KeyManagementException {
        final URL responseFile = TestNiFiRegistryClient.class.getResource("/resources-response.json");
        if (responseFile == null) {
            throw new AssertionError();
        }
        final String resourcesResponse = Resources.toString(responseFile, StandardCharsets.UTF_8);
        registryClient = new MockNiFiRegistryClient(resourcesResponse, 200, false, null);
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
        Assert.assertNotNull(resources);
        Assert.assertEquals(expectedResources.size(), resources.size());

        Assert.assertTrue(resources.containsAll(expectedResources));
    }

    @Test
    public void testGetResourcesWithUserInputBeginning() throws Exception {
        ResourceLookupContext resourceLookupContext = Mockito.mock(ResourceLookupContext.class);
        when(resourceLookupContext.getUserInput()).thenReturn("/p");

        final List<String> expectedResources = new ArrayList<>();
        expectedResources.add("/policies");
        expectedResources.add("/proxy");

        List<String> resources = registryClient.getResources(resourceLookupContext);
        Assert.assertNotNull(resources);
        Assert.assertEquals(expectedResources.size(), resources.size());

        Assert.assertTrue(resources.containsAll(expectedResources));
    }

    @Test
    public void testGetResourcesWithUserInputAnywhere() throws Exception {
        ResourceLookupContext resourceLookupContext = Mockito.mock(ResourceLookupContext.class);
        when(resourceLookupContext.getUserInput()).thenReturn("ant");

        final List<String> expectedResources = new ArrayList<>();
        expectedResources.add("/tenants");

        List<String> resources = registryClient.getResources(resourceLookupContext);
        Assert.assertNotNull(resources);
        Assert.assertEquals(expectedResources.size(), resources.size());

        Assert.assertTrue(resources.containsAll(expectedResources));
    }

    @Test
    public void testGetResourcesErrorResponse() throws NoSuchAlgorithmException, KeyManagementException {
        final String errorMsg = "unknown error";
        registryClient = new MockNiFiRegistryClient(errorMsg, Response.Status.BAD_REQUEST.getStatusCode(), false, null);

        ResourceLookupContext resourceLookupContext = Mockito.mock(ResourceLookupContext.class);
        when(resourceLookupContext.getUserInput()).thenReturn("");

        try {
            registryClient.getResources(resourceLookupContext);
            Assert.fail("should have thrown exception");
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains(errorMsg));
        }
    }

    @Test
    public void testConnectionTestSuccess() {
        HashMap<String, Object> ret = registryClient.connectionTest();
        Assert.assertNotNull(ret);
        Assert.assertEquals(NiFiRegistryClient.SUCCESS_MSG, ret.get("message"));
    }

    @Test
    public void testConnectionTestFailure() throws NoSuchAlgorithmException, KeyManagementException {
        final String errorMsg = "unknown error";
        registryClient = new MockNiFiRegistryClient(errorMsg, Response.Status.BAD_REQUEST.getStatusCode(), false, null);

        HashMap<String, Object> ret = registryClient.connectionTest();
        Assert.assertNotNull(ret);
        Assert.assertEquals(NiFiRegistryClient.FAILURE_MSG, ret.get("message"));
    }

    @Test
    public void testHostnameVerifierMatch() throws NoSuchAlgorithmException, KeyManagementException, CertificateParsingException, SSLPeerUnverifiedException {
        MockNiFiRegistryClient sslClient = new MockNiFiRegistryClient("", 200, true, HOSTNAME);
        sslClient.setupSSLMock(HOSTNAME);
        sslClient.getResponse(sslClient.getWebResource(), "application/json");
        verify(sslClient.hostnameVerifierSpy).verify(eq(HOSTNAME), any(SSLSession.class));
        Assert.assertTrue(sslClient.lastVerifyResult);
    }

    @Test
    public void testHostnameVerifierNoMatch() throws NoSuchAlgorithmException, KeyManagementException, CertificateParsingException, SSLPeerUnverifiedException {
        MockNiFiRegistryClient sslClient = new MockNiFiRegistryClient("", 200, true, HOSTNAME);
        sslClient.setupSSLMock("other.com");
        sslClient.getResponse(sslClient.getWebResource(), "application/json");
        verify(sslClient.hostnameVerifierSpy).verify(eq(HOSTNAME), any(SSLSession.class));
        Assert.assertFalse(sslClient.lastVerifyResult);
    }

    @Test
    public void testHostnameVerifierNoCerts() throws NoSuchAlgorithmException, KeyManagementException, SSLPeerUnverifiedException {
        MockNiFiRegistryClient sslClient = new MockNiFiRegistryClient("", 200, true, HOSTNAME);
        sslClient.setupSSLMockWithNoCerts();
        sslClient.getResponse(sslClient.getWebResource(), "application/json");
        Assert.assertFalse(sslClient.lastVerifyResult);
    }

    @Test
    public void testHostnameVerifierEmptyCerts() throws NoSuchAlgorithmException, KeyManagementException, SSLPeerUnverifiedException {
        MockNiFiRegistryClient sslClient = new MockNiFiRegistryClient("", 200, true, HOSTNAME);
        sslClient.setupSSLMockWithEmptyCerts();
        sslClient.getResponse(sslClient.getWebResource(), "application/json");
        Assert.assertFalse(sslClient.lastVerifyResult);
    }

    @Test
    public void testHostnameVerifierSanInIntermediateCertsFails() throws NoSuchAlgorithmException, KeyManagementException, CertificateParsingException, SSLPeerUnverifiedException {
        MockNiFiRegistryClient sslClient = new MockNiFiRegistryClient("", 200, true, HOSTNAME);
        sslClient.setupSSLMockWithSanInIntermediate();
        sslClient.getResponse(sslClient.getWebResource(), "application/json");
        verify(sslClient.hostnameVerifierSpy).verify(eq(HOSTNAME), any(SSLSession.class));
        Assert.assertFalse(sslClient.lastVerifyResult);
    }

    /**
     * Extend NiFiRegistryClient to return mock responses.
     */
    private static final class MockNiFiRegistryClient extends NiFiRegistryClient {
        private final int statusCode;
        private final String responseEntity;
        private final boolean useSSL;
        private final String hostname;
        private HostnameVerifier hostnameVerifierSpy;
        private boolean lastVerifyResult;
        private SSLSession mockSession;

        private MockNiFiRegistryClient(String responseEntity, int statusCode, boolean useSSL, String hostname) throws NoSuchAlgorithmException, KeyManagementException {
            super(useSSL ? ("https://" + (hostname != null ? hostname : "localhost") + ":443") : POLICIES_RESOURCES,
                    useSSL ? createInitializedSSLContext() : null);
            this.statusCode = statusCode;
            this.responseEntity = responseEntity;
            this.useSSL = useSSL;
            this.hostname = hostname;
        }

        private static SSLContext createInitializedSSLContext() throws NoSuchAlgorithmException, KeyManagementException {
            SSLContext sslContext = SSLContext.getInstance("TLS");
            sslContext.init(null, null, null);
            return sslContext;
        }

        void setupSSLMock(String sanHostname) throws CertificateParsingException, SSLPeerUnverifiedException {
            if (!useSSL) {
                throw new IllegalStateException("SSL setup not supported for non-SSL mock");
            }
            hostnameVerifierSpy = spy(getHostnameVerifier());
            mockSession = Mockito.mock(SSLSession.class);
            SSLSessionContext mockContext = Mockito.mock(SSLSessionContext.class);
            doReturn(mockContext).when(mockSession).getSessionContext();

            X509Certificate mockCert = Mockito.mock(X509Certificate.class);
            Certificate[] certs = {mockCert};
            doReturn(certs).when(mockSession).getPeerCertificates();

            Collection<List<?>> altNames = Collections.singletonList(
                    Arrays.asList(2, sanHostname.toLowerCase()));
            doReturn(altNames).when(mockCert).getSubjectAlternativeNames();
            doAnswer(invocation -> {
                Boolean result = (Boolean) invocation.callRealMethod();
                lastVerifyResult = result;
                return result;
            }).when(hostnameVerifierSpy).verify(any(String.class), any(SSLSession.class));
        }

        void setupSSLMockWithNoCerts() throws SSLPeerUnverifiedException {
            if (!useSSL) {
                throw new IllegalStateException("SSL setup not supported for non-SSL mock");
            }
            hostnameVerifierSpy = spy(getHostnameVerifier());
            mockSession = Mockito.mock(SSLSession.class);
            doReturn(null).when(mockSession).getPeerCertificates();
            doReturn(false).when(hostnameVerifierSpy).verify(any(String.class), any(SSLSession.class));
            lastVerifyResult = false;
        }

        void setupSSLMockWithEmptyCerts() throws SSLPeerUnverifiedException {
            if (!useSSL) {
                throw new IllegalStateException("SSL setup not supported for non-SSL mock");
            }
            hostnameVerifierSpy = spy(getHostnameVerifier());
            mockSession = Mockito.mock(SSLSession.class);
            doReturn(new Certificate[0]).when(mockSession).getPeerCertificates();
            doReturn(false).when(hostnameVerifierSpy).verify(any(String.class), any(SSLSession.class));
            lastVerifyResult = false;
        }

        void setupSSLMockWithSanInIntermediate() throws CertificateParsingException, SSLPeerUnverifiedException {
            if (!useSSL) {
                throw new IllegalStateException("SSL setup not supported for non-SSL mock");
            }
            hostnameVerifierSpy = spy(getHostnameVerifier());
            mockSession = Mockito.mock(SSLSession.class);
            SSLSessionContext mockContext = Mockito.mock(SSLSessionContext.class);
            doReturn(mockContext).when(mockSession).getSessionContext();

            // Server cert (index 0): No SANs
            X509Certificate serverCert = Mockito.mock(X509Certificate.class);
            doReturn(null).when(serverCert).getSubjectAlternativeNames();

            // Intermediate cert (index 1): Has SAN with hostname
            X509Certificate intermediateCert = Mockito.mock(X509Certificate.class);
            Collection<List<?>> intermediateAltNames = Collections.singletonList(
                    Arrays.asList(2, HOSTNAME.toLowerCase()));
            doReturn(intermediateAltNames).when(intermediateCert).getSubjectAlternativeNames();

            // Root cert (index 2): No SANs
            X509Certificate rootCert = Mockito.mock(X509Certificate.class);
            doReturn(null).when(rootCert).getSubjectAlternativeNames();

            Certificate[] certs = {serverCert, intermediateCert, rootCert};
            doReturn(certs).when(mockSession).getPeerCertificates();

            doAnswer(invocation -> {
                Boolean result = (Boolean) invocation.callRealMethod();
                lastVerifyResult = result;
                return result;
            }).when(hostnameVerifierSpy).verify(any(String.class), any(SSLSession.class));
        }

        @Override
        protected WebResource getWebResource() {
            return Mockito.mock(WebResource.class);
        }

        @Override
        protected ClientResponse getResponse(WebResource resource, String accept) {
            if (useSSL) {
                hostnameVerifierSpy.verify(hostname, mockSession);
            }
            ClientResponse response = Mockito.mock(ClientResponse.class);
            when(response.getStatus()).thenReturn(statusCode);
            when(response.getEntityInputStream()).thenReturn(new ByteArrayInputStream(responseEntity.getBytes(StandardCharsets.UTF_8)));
            return response;
        }
    }
}
