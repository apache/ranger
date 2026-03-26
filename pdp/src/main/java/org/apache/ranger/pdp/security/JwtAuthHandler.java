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

import org.apache.ranger.authz.handler.RangerAuth;
import org.apache.ranger.authz.handler.jwt.RangerDefaultJwtAuthHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.io.IOException;
import java.util.Properties;

/**
 * Authenticates requests using a JWT bearer token.
 *
 * <p>Checks for the token in the {@code Authorization: Bearer <token>} header first,
 * then in the configured JWT cookie.  Delegates signature verification and expiry/audience
 * checks to {@link RangerDefaultJwtAuthHandler} from the {@code ranger-authn} module.
 *
 * <p>Configuration keys (all prefixed with {@code ranger.pdp.auth.jwt.}):
 * <ul>
 *   <li>{@code provider.url}  – JWKS endpoint URL (optional if public key is set)
 *   <li>{@code public.key}    – PEM-encoded public key (optional if provider URL is set)
 *   <li>{@code cookie.name}   – JWT cookie name (default: {@code hadoop-jwt})
 *   <li>{@code audiences}     – comma-separated list of accepted audiences (optional)
 * </ul>
 */
public class JwtAuthHandler implements PdpAuthHandler {
    private static final Logger LOG = LoggerFactory.getLogger(JwtAuthHandler.class);

    public static final String AUTH_TYPE = "JWT";

    private RangerDefaultJwtAuthHandler delegate;

    @Override
    public void init(Properties config) throws Exception {
        Properties jwtConfig = new Properties();

        copyIfPresent(config, RangerPdpAuthFilter.PARAM_JWT_PROVIDER_URL, jwtConfig, RangerDefaultJwtAuthHandler.KEY_PROVIDER_URL);
        copyIfPresent(config, RangerPdpAuthFilter.PARAM_JWT_PUBLIC_KEY, jwtConfig, RangerDefaultJwtAuthHandler.KEY_JWT_PUBLIC_KEY);
        copyIfPresent(config, RangerPdpAuthFilter.PARAM_JWT_COOKIE_NAME, jwtConfig, RangerDefaultJwtAuthHandler.KEY_JWT_COOKIE_NAME);
        copyIfPresent(config, RangerPdpAuthFilter.PARAM_JWT_AUDIENCES, jwtConfig, RangerDefaultJwtAuthHandler.KEY_JWT_AUDIENCES);

        delegate = new RangerDefaultJwtAuthHandler();

        delegate.initialize(jwtConfig);

        LOG.info("JwtAuthHandler initialized");
    }

    @Override
    public Result authenticate(HttpServletRequest request, HttpServletResponse response) throws IOException {
        if (!RangerDefaultJwtAuthHandler.canAuthenticateRequest(request)) {
            return Result.skip();
        }

        RangerAuth rangerAuth = delegate.authenticate(request);

        if (rangerAuth != null && rangerAuth.isAuthenticated()) {
            LOG.debug("authenticate(): user={}", rangerAuth.getUserName());

            return Result.authenticated(rangerAuth.getUserName(), AUTH_TYPE);
        }

        LOG.warn("authenticate(): JWT validation failed");

        response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
        response.addHeader("WWW-Authenticate", getChallengeHeader() + ", error=\"invalid_token\"");

        return Result.challenge();
    }

    @Override
    public String getChallengeHeader() {
        return "Bearer realm=\"Ranger PDP\"";
    }

    private void copyIfPresent(Properties src, String srcKey, Properties dst, String dstKey) {
        String val = src.getProperty(srcKey);

        if (val != null && !val.isEmpty()) {
            dst.setProperty(dstKey, val);
        }
    }
}
