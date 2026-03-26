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

import org.junit.jupiter.api.Test;

import javax.servlet.http.HttpServletRequest;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class HttpHeaderAuthHandlerTest {
    @Test
    public void testAuthenticate_usesDefaultHeaderName() {
        HttpHeaderAuthHandler handler = new HttpHeaderAuthHandler();
        Properties            config  = new Properties();

        handler.init(config);

        HttpServletRequest    request = requestWithHeader("X-Forwarded-User", "alice");
        PdpAuthHandler.Result result  = handler.authenticate(request, null);

        assertEquals(PdpAuthHandler.Result.Status.AUTHENTICATED, result.getStatus());
        assertEquals("alice", result.getUserName());
        assertEquals(HttpHeaderAuthHandler.AUTH_TYPE, result.getAuthType());
    }

    @Test
    public void testAuthenticate_usesConfiguredHeaderName() {
        HttpHeaderAuthHandler handler = new HttpHeaderAuthHandler();
        Properties            config  = new Properties();

        config.setProperty(RangerPdpAuthFilter.PARAM_HEADER_AUTHN_USERNAME, "X-Authenticated-User");

        handler.init(config);

        HttpServletRequest    request = requestWithHeader("X-Authenticated-User", "bob");
        PdpAuthHandler.Result result  = handler.authenticate(request, null);

        assertEquals(PdpAuthHandler.Result.Status.AUTHENTICATED, result.getStatus());
        assertEquals("bob", result.getUserName());
    }

    private static HttpServletRequest requestWithHeader(String expectedHeader, String headerValue) {
        InvocationHandler invocationHandler = (proxy, method, args) -> {
            String methodName = method.getName();

            if ("getHeader".equals(methodName)) {
                return expectedHeader.equals(args[0]) ? headerValue : null;
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
