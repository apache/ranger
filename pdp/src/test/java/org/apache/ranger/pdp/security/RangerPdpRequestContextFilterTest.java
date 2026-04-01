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
import org.slf4j.MDC;

import javax.servlet.FilterChain;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class RangerPdpRequestContextFilterTest {
    @Test
    public void testUsesIncomingRequestIdAndClearsMdc() throws Exception {
        RangerPdpRequestContextFilter filter               = new RangerPdpRequestContextFilter();
        Map<String, Object>           requestAttrs         = new HashMap<>();
        Map<String, String>           responseHeaders      = new HashMap<>();
        final String[]                requestIdSeenInChain = new String[1];
        String                        requestId            = "abc-123";

        HttpServletRequest  req  = requestProxy(requestId, requestAttrs);
        HttpServletResponse resp  = responseProxy(responseHeaders);
        FilterChain         chain = chainProxy((request, response) -> requestIdSeenInChain[0] = MDC.get(RangerPdpRequestContextFilter.MDC_REQUEST_ID));

        filter.doFilter(req, resp, chain);

        assertEquals(requestId, requestAttrs.get(RangerPdpRequestContextFilter.MDC_REQUEST_ID));
        assertEquals(requestId, responseHeaders.get(RangerPdpRequestContextFilter.RES_HEADER_REQUEST_ID));
        assertEquals(requestId, requestIdSeenInChain[0]);
        assertNull(MDC.get(RangerPdpRequestContextFilter.MDC_REQUEST_ID));
    }

    @Test
    public void testGeneratesRequestIdWhenMissing() throws Exception {
        RangerPdpRequestContextFilter filter          = new RangerPdpRequestContextFilter();
        Map<String, Object>           requestAttrs    = new HashMap<>();
        Map<String, String>           responseHeaders = new HashMap<>();

        HttpServletRequest  req   = requestProxy(null, requestAttrs);
        HttpServletResponse resp  = responseProxy(responseHeaders);
        FilterChain         chain = chainProxy((request, response) -> {});

        filter.doFilter(req, resp, chain);

        Object requestId = requestAttrs.get(RangerPdpRequestContextFilter.MDC_REQUEST_ID);

        assertNotNull(requestId);
        assertTrue(requestId.toString().length() > 10);
        assertEquals(requestId.toString(), responseHeaders.get(RangerPdpRequestContextFilter.RES_HEADER_REQUEST_ID));
    }

    private static HttpServletRequest requestProxy(String incomingRequestId, Map<String, Object> attrs) {
        InvocationHandler handler = (proxy, method, args) -> {
            switch (method.getName()) {
                case "getHeader":
                    return RangerPdpRequestContextFilter.REQ_HEADER_REQUEST_ID.equals(args[0]) ? incomingRequestId : null;
                case "setAttribute":
                    attrs.put((String) args[0], args[1]);
                    return null;
                case "getAttribute":
                    return attrs.get((String) args[0]);
                default:
                    return null;
            }
        };

        return (HttpServletRequest) Proxy.newProxyInstance(
                HttpServletRequest.class.getClassLoader(),
                new Class<?>[] {HttpServletRequest.class},
                handler);
    }

    private static HttpServletResponse responseProxy(Map<String, String> headers) {
        InvocationHandler handler = (proxy, method, args) -> {
            if ("setHeader".equals(method.getName())) {
                headers.put((String) args[0], (String) args[1]);
            }
            return null;
        };

        return (HttpServletResponse) Proxy.newProxyInstance(
                HttpServletResponse.class.getClassLoader(),
                new Class<?>[] {HttpServletResponse.class},
                handler);
    }

    private static FilterChain chainProxy(ChainAction action) {
        InvocationHandler handler = (proxy, method, args) -> {
            if ("doFilter".equals(method.getName())) {
                action.apply(args[0], args[1]);
            }
            return null;
        };

        return (FilterChain) Proxy.newProxyInstance(
                FilterChain.class.getClassLoader(),
                new Class<?>[] {FilterChain.class},
                handler);
    }

    @FunctionalInterface
    private interface ChainAction {
        void apply(Object request, Object response) throws Exception;
    }
}
