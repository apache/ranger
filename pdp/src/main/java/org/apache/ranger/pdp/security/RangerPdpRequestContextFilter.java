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

import org.apache.commons.lang3.StringUtils;
import org.slf4j.MDC;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.io.IOException;
import java.util.UUID;

public class RangerPdpRequestContextFilter implements Filter {
    public static final String REQ_HEADER_REQUEST_ID = "X-Request-Id";
    public static final String RES_HEADER_REQUEST_ID = "X-Request-Id";
    public static final String MDC_REQUEST_ID        = "requestId";

    @Override
    public void init(FilterConfig filterConfig) {
        // no-op
    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain) throws IOException, ServletException {
        HttpServletRequest  req = (HttpServletRequest) request;
        HttpServletResponse res = (HttpServletResponse) response;

        String requestId = req.getHeader(REQ_HEADER_REQUEST_ID);

        if (StringUtils.isBlank(requestId)) {
            requestId = UUID.randomUUID().toString();
        }

        MDC.put(MDC_REQUEST_ID, requestId);
        req.setAttribute(MDC_REQUEST_ID, requestId);
        res.setHeader(RES_HEADER_REQUEST_ID, requestId);

        try {
            chain.doFilter(request, response);
        } finally {
            MDC.remove(MDC_REQUEST_ID);
        }
    }

    @Override
    public void destroy() {
        // no-op
    }
}
