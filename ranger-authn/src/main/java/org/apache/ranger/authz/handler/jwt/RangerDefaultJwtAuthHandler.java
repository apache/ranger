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
package org.apache.ranger.authz.handler.jwt;

import javax.servlet.ServletRequest;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.security.authentication.server.AuthenticationToken;
import org.apache.ranger.authz.handler.RangerAuth;

import com.nimbusds.jose.proc.JWSKeySelector;
import com.nimbusds.jose.proc.SecurityContext;
import com.nimbusds.jwt.proc.ConfigurableJWTProcessor;
import com.nimbusds.jwt.proc.DefaultJWTClaimsVerifier;
import com.nimbusds.jwt.proc.DefaultJWTProcessor;
import com.nimbusds.jwt.proc.JWTClaimsSetVerifier;

/**
 * Default implementation of Ranger JWT authentication
 *
 */
public class RangerDefaultJwtAuthHandler extends RangerJwtAuthHandler {

    protected static final String AUTHORIZATION_HEADER = "Authorization";
    protected static final String DO_AS_PARAMETER      = "doAs";

    @Override
    public ConfigurableJWTProcessor<SecurityContext> getJwtProcessor(JWSKeySelector<SecurityContext> keySelector) {
        ConfigurableJWTProcessor<SecurityContext> jwtProcessor   = new DefaultJWTProcessor<>();
        JWTClaimsSetVerifier<SecurityContext>     claimsVerifier = new DefaultJWTClaimsVerifier<>();

        jwtProcessor.setJWSKeySelector(keySelector);
        jwtProcessor.setJWTClaimsSetVerifier(claimsVerifier);

        return jwtProcessor;
    }

    @Override
    public RangerAuth authenticate(HttpServletRequest httpServletRequest) {
        RangerAuth rangerAuth       = null;
        String     jwtAuthHeaderStr = getJwtAuthHeader(httpServletRequest);
        String     jwtCookieStr     = StringUtils.isBlank(jwtAuthHeaderStr) ? getJwtCookie(httpServletRequest) : null;
        String     doAsUser         = httpServletRequest.getParameter(DO_AS_PARAMETER);

        AuthenticationToken authenticationToken = authenticate(jwtAuthHeaderStr, jwtCookieStr, doAsUser);

        if (authenticationToken != null) {
            rangerAuth = new RangerAuth(authenticationToken, RangerAuth.AUTH_TYPE.JWT_JWKS);
        }

        return rangerAuth;
    }

    public static boolean canAuthenticateRequest(final ServletRequest request) {
        HttpServletRequest httpServletRequest = (HttpServletRequest) request;
        String     jwtAuthHeaderStr = getJwtAuthHeader(httpServletRequest);
        String     jwtCookieStr     = StringUtils.isBlank(jwtAuthHeaderStr) ? getJwtCookie(httpServletRequest) : null;

        return shouldProceedAuth(jwtAuthHeaderStr, jwtCookieStr);
    }

    public static String getJwtAuthHeader(final HttpServletRequest httpServletRequest) {
        return httpServletRequest.getHeader(AUTHORIZATION_HEADER);
    }

    public static String getJwtCookie(final HttpServletRequest httpServletRequest) {
        String jwtCookieStr = null;
        Cookie[] cookies = httpServletRequest.getCookies();
        if (cookies != null) {
            for (Cookie cookie : cookies) {
                if (cookieName.equals(cookie.getName())) {
                    jwtCookieStr = cookie.getName() + "=" + cookie.getValue();
                    break;
                }
            }
        }
        return jwtCookieStr;
    }
}
