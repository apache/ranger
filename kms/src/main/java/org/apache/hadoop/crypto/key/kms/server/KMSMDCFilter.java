/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.crypto.key.kms.server;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.delegation.web.HttpUserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;

/**
 * Servlet filter that captures context of the HTTP request to be use in the
 * scope of KMS calls on the server side.
 */
@InterfaceAudience.Private
public class KMSMDCFilter implements Filter {
    static final Logger logger = LoggerFactory.getLogger(KMSMDCFilter.class);

    static final String RANGER_KMS_REST_API_PATH = "/kms/api/status";

    private static final String EEK_OP_CODE = "eek_op";

    private static final ThreadLocal<Data> DATA_TL = new ThreadLocal<>();

    public static UserGroupInformation getUgi() {
        return DATA_TL.get().ugi;
    }

    public static String getMethod() {
        return DATA_TL.get().method;
    }

    public static String getURL() {
        return DATA_TL.get().url;
    }

    public static String getOperation() {
        return DATA_TL.get().operation;
    }

    @Override
    public void init(FilterConfig config) throws ServletException {
    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain) throws IOException, ServletException {
        try {
            String              path = ((HttpServletRequest) request).getRequestURI();
            HttpServletRequest  req  = (HttpServletRequest) request;
            HttpServletResponse resp = (HttpServletResponse) response;

            if (path.startsWith(RANGER_KMS_REST_API_PATH)) {
                chain.doFilter(request, resp);
            } else {
                DATA_TL.remove();

                UserGroupInformation ugi         = HttpUserGroupInformation.get();
                String               method      = req.getMethod();
                StringBuffer         requestURL  = req.getRequestURL();
                String               queryString = req.getQueryString();

                // Extract operation from query parameters if present
                String operation = null;
                if (path.contains("/_eek") && queryString != null) {
                    for (String param : queryString.split("&")) {
                        String[] kv = param.split("=", 2);
                        if (kv.length == 2 && "eek_op".equals(kv[0])) {
                            try {
                                operation = URLDecoder.decode(kv[1], StandardCharsets.UTF_8.name());
                            } catch (UnsupportedEncodingException | IllegalArgumentException e) {
                                logger.error("Failed to decode eek_op parameter value using UTF-8 encoding: {}", kv[1], e);
                                throw new ServletException("Failed to decode eek_op parameter: '" + kv[1] + "'. " + e.getClass().getSimpleName() + ": " + e.getMessage(), e);
                            }
                            break;
                        }
                    }
                }

                if (queryString != null) {
                    requestURL.append("?").append(queryString);
                }

                // Store opCode in request attribute for Tomcat access logs
                if (operation != null) {
                    req.setAttribute(EEK_OP_CODE, operation);
                }

                DATA_TL.set(new Data(ugi, method, requestURL.toString(), operation));

                chain.doFilter(request, resp);
            }
        } finally {
            DATA_TL.remove();
        }
    }

    @Override
    public void destroy() {
    }

    private static class Data {
        private final UserGroupInformation ugi;
        private final String               method;
        private final String               url;
        private final String               operation;

        private Data(UserGroupInformation ugi, String method, String url, String operation) {
            this.ugi       = ugi;
            this.method    = method;
            this.url       = url;
            this.operation = operation;
        }
    }
}
