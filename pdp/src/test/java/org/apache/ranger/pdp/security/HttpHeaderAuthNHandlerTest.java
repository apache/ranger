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

package org.apache.ranger.pdp.security;

import org.apache.ranger.pdp.config.RangerPdpConstants;
import org.junit.jupiter.api.Test;

import javax.servlet.http.HttpServletRequest;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class HttpHeaderAuthNHandlerTest {
    @Test
    public void testAuthenticate_usesNoHeaderName() {
        HttpHeaderAuthNHandler handler = new HttpHeaderAuthNHandler();
        Properties             config  = new Properties();

        handler.init(config);

        HttpServletRequest     request = requestWithHeader("X-Forwarded-User", "alice");
        PdpAuthNHandler.Result result  = handler.authenticate(request, null);

        assertEquals(PdpAuthNHandler.Result.Status.SKIP, result.getStatus());
        assertNull(result.getUserName());
        assertNull(result.getAuthType());
    }

    @Test
    public void testAuthenticate_usesConfiguredHeaderName() {
        HttpHeaderAuthNHandler handler = new HttpHeaderAuthNHandler();
        Properties             config  = new Properties();

        config.setProperty(RangerPdpConstants.PROP_AUTHN_HEADER_USERNAME, "X-Authenticated-User");

        handler.init(config);

        HttpServletRequest     request = requestWithHeader("X-Authenticated-User", "bob");
        PdpAuthNHandler.Result result  = handler.authenticate(request, null);

        assertEquals(PdpAuthNHandler.Result.Status.AUTHENTICATED, result.getStatus());
        assertEquals("bob", result.getUserName());
    }

    @Test
    void testAuthenticate_usesSpiffeHeaderWhenUsernameAbsent() {
        HttpHeaderAuthNHandler handler = new HttpHeaderAuthNHandler();
        Properties             config  = new Properties();

        config.setProperty(RangerPdpConstants.PROP_AUTHN_HEADER_USERNAME, "X-Authenticated-User");
        config.setProperty(RangerPdpConstants.PROP_AUTHN_HEADER_SPIFFE, "x-awc-source-workload-id");

        handler.init(config);

        HttpServletRequest     request = requestWithHeader("x-awc-source-workload-id", "spiffe://my-cluster/ns/service-namespace/sa/service-sa");
        PdpAuthNHandler.Result result  = handler.authenticate(request, null);

        assertEquals(PdpAuthNHandler.Result.Status.AUTHENTICATED, result.getStatus());
        assertEquals("service-sa", result.getUserName());
        assertEquals(HttpHeaderAuthNHandler.AUTH_TYPE_SPIFFE, result.getAuthType());
    }

    @Test
    void testAuthenticate_usernameTakesPrecedenceOverSpiffe() {
        HttpHeaderAuthNHandler handler = new HttpHeaderAuthNHandler();
        Properties             config  = new Properties();

        config.setProperty(RangerPdpConstants.PROP_AUTHN_HEADER_USERNAME, "X-Authenticated-User");
        config.setProperty(RangerPdpConstants.PROP_AUTHN_HEADER_SPIFFE, "x-awc-source-workload-id");

        handler.init(config);

        Map<String, String> headers = new HashMap<>();

        headers.put("X-Authenticated-User", "bob");
        headers.put("x-awc-source-workload-id", "spiffe://my-cluster/ns/service-namespace/sa/service-sa");

        PdpAuthNHandler.Result result = handler.authenticate(requestWithHeaders(headers), null);

        assertEquals(PdpAuthNHandler.Result.Status.AUTHENTICATED, result.getStatus());
        assertEquals("bob", result.getUserName());
        assertEquals(HttpHeaderAuthNHandler.AUTH_TYPE, result.getAuthType());
    }

    @Test
    void testAuthenticate_multipleSpiffeHeadersUsesFirstValid() {
        HttpHeaderAuthNHandler handler = new HttpHeaderAuthNHandler();
        Properties             config  = new Properties();

        config.setProperty(RangerPdpConstants.PROP_AUTHN_HEADER_SPIFFE, "x-awc-source-workload-id, x-awc-upstream-workload-id");

        handler.init(config);

        Map<String, String> headers = new HashMap<>();

        headers.put("x-awc-source-workload-id", "not-a-spiffe-id");
        headers.put("x-awc-upstream-workload-id", "spiffe://my-cluster/ns/service-namespace/sa/service-sa");

        PdpAuthNHandler.Result result = handler.authenticate(requestWithHeaders(headers), null);

        assertEquals(PdpAuthNHandler.Result.Status.AUTHENTICATED, result.getStatus());
        assertEquals("service-sa", result.getUserName());
        assertEquals(HttpHeaderAuthNHandler.AUTH_TYPE_SPIFFE, result.getAuthType());
    }

    @Test
    void testAuthenticate_malformedSpiffeHeaderSkips() {
        HttpHeaderAuthNHandler handler = new HttpHeaderAuthNHandler();
        Properties             config  = new Properties();

        config.setProperty(RangerPdpConstants.PROP_AUTHN_HEADER_SPIFFE, "x-awc-source-workload-id");

        handler.init(config);

        HttpServletRequest     request = requestWithHeader("x-awc-source-workload-id", "not-a-spiffe-id");
        PdpAuthNHandler.Result result  = handler.authenticate(request, null);

        assertEquals(PdpAuthNHandler.Result.Status.SKIP, result.getStatus());
        assertNull(result.getUserName());
    }

    @Test
    void testAuthenticate_specValidButNonConformingSpiffeHeaderSkips() {
        HttpHeaderAuthNHandler handler = new HttpHeaderAuthNHandler();
        Properties             config  = new Properties();

        config.setProperty(RangerPdpConstants.PROP_AUTHN_HEADER_SPIFFE, "x-awc-source-workload-id");

        handler.init(config);

        // Valid SPIFFE ID per the SPIFFE spec, but not in the expected /ns/<ns>/sa/<sa> layout.
        HttpServletRequest     request = requestWithHeader("x-awc-source-workload-id", "spiffe://example.org/workload/frontend");
        PdpAuthNHandler.Result result  = handler.authenticate(request, null);

        assertEquals(PdpAuthNHandler.Result.Status.SKIP, result.getStatus());
        assertNull(result.getUserName());
    }

    private static HttpServletRequest requestWithHeader(String expectedHeader, String headerValue) {
        return requestWithHeaders(Collections.singletonMap(expectedHeader, headerValue));
    }

    private static HttpServletRequest requestWithHeaders(Map<String, String> headers) {
        InvocationHandler invocationHandler = (proxy, method, args) -> {
            String methodName = method.getName();

            if ("getHeader".equals(methodName)) {
                return headers.get(args[0]);
            } else if ("getHeaders".equals(methodName) || "getHeaderNames".equals(methodName)) {
                return java.util.Collections.emptyEnumeration();
            } else if ("getMethod".equals(methodName)) {
                return "POST";
            } else if ("toString".equals(methodName)) {
                return "HttpServletRequest(test)";
            }

            return null;
        };

        return (HttpServletRequest) Proxy.newProxyInstance(HttpServletRequest.class.getClassLoader(), new Class<?>[] {HttpServletRequest.class}, invocationHandler);
    }
}
