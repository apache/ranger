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
import org.springframework.security.core.Authentication;
import org.springframework.security.saml2.provider.service.web.authentication.logout.Saml2RelyingPartyInitiatedLogoutSuccessHandler;
import org.springframework.security.web.authentication.logout.LogoutSuccessHandler;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.io.IOException;

public class RangerDelegatingLogoutSuccessHandler implements LogoutSuccessHandler {
    private static final Logger logger = LoggerFactory.getLogger(RangerDelegatingLogoutSuccessHandler.class);
    private static final String AUTH_METHOD_SAML = "SAML";

    private final Saml2RelyingPartyInitiatedLogoutSuccessHandler samlLogoutSuccessHandler;
    private final LogoutSuccessHandler defaultLogoutSuccessHandler;

    public RangerDelegatingLogoutSuccessHandler(Saml2RelyingPartyInitiatedLogoutSuccessHandler samlLogoutSuccessHandler,
                                                LogoutSuccessHandler defaultLogoutSuccessHandler) {
        this.samlLogoutSuccessHandler = samlLogoutSuccessHandler;
        this.defaultLogoutSuccessHandler = defaultLogoutSuccessHandler;
    }

    @Override
    public void onLogoutSuccess(HttpServletRequest request, HttpServletResponse response,
                                Authentication authentication) throws IOException, ServletException {
        String authMethod = PropertiesUtil.getProperty("ranger.authentication.method", "NONE");
        logger.debug("RangerDelegatingLogoutSuccessHandler.onLogoutSuccess() authMethod={}", authMethod);
        if (AUTH_METHOD_SAML.equalsIgnoreCase(authMethod)) {
            samlLogoutSuccessHandler.onLogoutSuccess(request, response, authentication);
        } else {
            defaultLogoutSuccessHandler.onLogoutSuccess(request, response, authentication);
        }
    }
}
