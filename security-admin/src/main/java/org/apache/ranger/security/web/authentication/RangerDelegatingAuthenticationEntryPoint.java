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
package org.apache.ranger.security.web.authentication;

import org.apache.commons.lang3.StringUtils;
import org.apache.ranger.common.PropertiesUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.core.AuthenticationException;
import org.springframework.security.web.AuthenticationEntryPoint;
import org.springframework.security.web.authentication.LoginUrlAuthenticationEntryPoint;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.io.IOException;

import static java.util.Objects.requireNonNull;

public class RangerDelegatingAuthenticationEntryPoint implements AuthenticationEntryPoint {
    private static final Logger LOG = LoggerFactory.getLogger(RangerDelegatingAuthenticationEntryPoint.class);
    private static final String AUTH_METHOD_SAML = "SAML";
    private final LoginUrlAuthenticationEntryPoint samlEntryPoint;
    private final AuthenticationEntryPoint defaultEntryPoint;
    private final String authenticationMethod;

    public RangerDelegatingAuthenticationEntryPoint(String samlLoginUrl, AuthenticationEntryPoint defaultEntryPoint) {
        this.samlEntryPoint = new LoginUrlAuthenticationEntryPoint(samlLoginUrl);
        this.defaultEntryPoint = requireNonNull(defaultEntryPoint, "defaultEntryPoint cannot be null");
        // Cache auth method at startup — auth method changes require Ranger Admin restart to take effect
        this.authenticationMethod = PropertiesUtil.getProperty("ranger.authentication.method", "NONE").trim();
    }

    @Override
    public void commence(HttpServletRequest request, HttpServletResponse response, AuthenticationException authException) throws IOException, ServletException {
        if (!AUTH_METHOD_SAML.equalsIgnoreCase(authenticationMethod) || isApiOrAjaxRequest(request)) {
            LOG.debug("Delegating to defaultEntryPoint (API or non-SAML auth)");
            defaultEntryPoint.commence(request, response, authException);
        } else {
            LOG.debug("Delegating to SAML EntryPoint (Browser login redirect)");
            samlEntryPoint.commence(request, response, authException);
        }
    }

    private boolean isApiOrAjaxRequest(HttpServletRequest request) {
        String uri = request.getRequestURI();
        String acceptHeader = request.getHeader("Accept");
        String xRequestedWith = request.getHeader("X-Requested-With");
        String contentType = request.getHeader("Content-Type");
        if (StringUtils.isNotBlank(uri) && uri.contains("/service/")) {
            LOG.debug("REST API detected via URI: {}", uri);
            return true;
        }
        // Check 2 — XMLHttpRequest (AJAX)
        if ("XMLHttpRequest".equalsIgnoreCase(xRequestedWith)) {
            LOG.debug("AJAX detected via X-Requested-With header");
            return true;
        }
        // Check 3 — Accept header prefers JSON/XML
        if (StringUtils.isNotBlank(acceptHeader) && (acceptHeader.contains("application/json") || acceptHeader.contains("application/xml")) && !acceptHeader.contains("text/html")) {
            LOG.debug("REST API detected via " + "Accept header: {}", acceptHeader);
            return true;
        }
        // Check 4 — Content-Type is JSON
        if (StringUtils.isNotBlank(contentType) && contentType.contains("application/json")) {
            LOG.debug("REST API detected via Content-Type header: {}", contentType);
            return true;
        }
        return false;
    }
}
