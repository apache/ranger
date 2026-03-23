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

public class RangerDelegatingAuthenticationEntryPoint implements AuthenticationEntryPoint {
    private static final Logger logger = LoggerFactory.getLogger(RangerDelegatingAuthenticationEntryPoint.class);

    private static final String AUTH_METHOD_SAML = "SAML";

    private final LoginUrlAuthenticationEntryPoint samlEntryPoint;
    private final AuthenticationEntryPoint         defaultEntryPoint;

    public RangerDelegatingAuthenticationEntryPoint(String samlLoginUrl, AuthenticationEntryPoint defaultEntryPoint) {
        this.samlEntryPoint    = new LoginUrlAuthenticationEntryPoint(samlLoginUrl);
        this.defaultEntryPoint = defaultEntryPoint;
    }

    @Override
    public void commence(HttpServletRequest request, HttpServletResponse response,
                         AuthenticationException authException) throws IOException, ServletException {
        String authMethod = PropertiesUtil.getProperty("ranger.authentication.method", "NONE");
        logger.debug("RangerDelegatingAuthenticationEntryPoint.commence() authMethod={}", authMethod);
        if (AUTH_METHOD_SAML.equalsIgnoreCase(authMethod)) {
            samlEntryPoint.commence(request, response, authException);
        } else {
            defaultEntryPoint.commence(request, response, authException);
        }
    }
}
