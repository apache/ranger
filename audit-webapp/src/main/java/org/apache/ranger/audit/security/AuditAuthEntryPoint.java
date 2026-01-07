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
package org.apache.ranger.audit.security;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.core.AuthenticationException;
import org.springframework.security.web.authentication.LoginUrlAuthenticationEntryPoint;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.io.IOException;

public class AuditAuthEntryPoint extends LoginUrlAuthenticationEntryPoint {
    private static final Logger LOG = LoggerFactory.getLogger(AuditAuthEntryPoint.class);

    public AuditAuthEntryPoint(String loginFormUrl) {
        super(loginFormUrl);
    }

    @Override
    public void commence(HttpServletRequest request, HttpServletResponse response, AuthenticationException authException) throws IOException, ServletException {
        //any task for entry filter
        response.setHeader("X-Frame-Options", "DENY");
        response.setHeader("Strict-Transport-Security", "max-age=31536000; includeSubDomains; preload");

        LOG.debug("commence() X-Requested-With=[{}]", request.getHeader("X-Requested-With"));
        LOG.debug("requestURL : [{}]", (request.getRequestURL() != null) ? request.getRequestURL().toString() : "");

        /* This is invoked when user tries to access a secured REST resource without supplying any credentials
        We should just send a 401 Unauthorized response because there is no 'login page' to redirect to */
        response.sendError(HttpServletResponse.SC_UNAUTHORIZED, "JWT Authorization Error");
    }
}
