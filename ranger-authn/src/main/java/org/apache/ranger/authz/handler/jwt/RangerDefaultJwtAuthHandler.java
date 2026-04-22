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

import com.nimbusds.jose.proc.JWSKeySelector;
import com.nimbusds.jose.proc.SecurityContext;
import com.nimbusds.jwt.proc.ConfigurableJWTProcessor;
import com.nimbusds.jwt.proc.DefaultJWTClaimsVerifier;
import com.nimbusds.jwt.proc.DefaultJWTProcessor;
import com.nimbusds.jwt.proc.JWTClaimsSetVerifier;
import org.apache.commons.lang3.StringUtils;
import org.apache.ranger.authz.handler.RangerAuth;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletRequest;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;

/**
 * Default implementation of Ranger JWT authentication
 */
public class RangerDefaultJwtAuthHandler extends RangerJwtAuthHandler {
    private static final Logger LOG = LoggerFactory.getLogger(RangerDefaultJwtAuthHandler.class);
    protected static final String AUTHORIZATION_HEADER = "Authorization";
    protected static final String DO_AS_PARAMETER      = "doAs";

    public static boolean canAuthenticateRequest(final ServletRequest request) {
        HttpServletRequest httpServletRequest = (HttpServletRequest) request;
        String             jwtAuthHeaderStr   = getJwtAuthHeader(httpServletRequest);
        String             jwtCookieStr       = StringUtils.isBlank(jwtAuthHeaderStr) ? getJwtCookie(httpServletRequest) : null;

        return shouldProceedAuth(jwtAuthHeaderStr, jwtCookieStr);
    }

    public static String getJwtAuthHeader(final HttpServletRequest httpServletRequest) {
        return httpServletRequest.getHeader(AUTHORIZATION_HEADER);
    }

    public static String getJwtCookie(final HttpServletRequest httpServletRequest) {
        String   jwtCookieStr = null;
        Cookie[] cookies      = httpServletRequest.getCookies();
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
        // authenticate against the JWT first to get the real (token-verified) user
        String realUser = authenticate(jwtAuthHeaderStr, jwtCookieStr);

        if (realUser != null) {
            String effectiveUser = realUser;

            if (StringUtils.isNotBlank(doAsUser)) {
                LOG.debug("RangerDefaultJwtAuthHandler.authenticate(): doAs=[{}] requested. isProxyEnabled=[{}]", doAsUser, isProxyEnabled());

                if (!isProxyEnabled()) {
                    LOG.warn("doAs [{}] requested but trusted proxy is not enabled. Ignoring doAs, proceeding with real user [{}].",
                            doAsUser, effectiveUser);
                } else {
                    LOG.debug("RangerDefaultJwtAuthHandler.authenticate(): Calling authorizeProxyUser: realUser=[{}], doAs=[{}], remoteAddr=[{}]",
                            realUser, doAsUser, httpServletRequest.getRemoteAddr());
                    // Check: is realUser authorized to impersonate doAsUser
                    if (!authorizeProxyUser(realUser, doAsUser, httpServletRequest.getRemoteAddr())) {
                        LOG.warn("RangerDefaultJwtAuthHandler.authenticate(): doAs=[{}] not authorized for realUser=[{}]. Rejecting.", doAsUser, realUser);
                        return null;
                    }
                    //Checks passed → switch to doAs user
                    effectiveUser = doAsUser.trim();
                    LOG.info("JWT doAs authorized: effectiveUser=[{}], realUser=[{}]", effectiveUser, realUser);
                }
            }

            rangerAuth = new RangerAuth(effectiveUser, RangerAuth.AuthType.JWT_JWKS);
        }
        return rangerAuth;
    }

    protected boolean isProxyEnabled() {
        return false;
    }

    protected boolean authorizeProxyUser(String realUser, String doAsUser, String remoteAddr) {
        return false;
    }
}
