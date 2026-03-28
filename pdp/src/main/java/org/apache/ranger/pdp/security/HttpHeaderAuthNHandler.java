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
import org.apache.ranger.pdp.config.RangerPdpConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.util.Properties;

/**
 * Authenticates requests by extracting the username from a trusted HTTP request header.
 *
 * <p>This handler is intended for deployments where a trusted reverse proxy / API gateway
 * has already authenticated the caller and propagates the identity via a header
 * (e.g., {@code X-Remote-User}).  The header value is accepted as-is; no further
 * validation is performed.
 *
 * <p><strong>Security note:</strong> Only enable this handler when the Ranger PDP server
 * is reachable exclusively through the trusted proxy.  Direct client access would allow
 * unauthenticated identity spoofing.
 */
public class HttpHeaderAuthNHandler implements PdpAuthNHandler {
    private static final Logger LOG = LoggerFactory.getLogger(HttpHeaderAuthNHandler.class);

    public static final String AUTH_TYPE = "HEADER";

    private String usernameHeader;

    @Override
    public void init(Properties config) {
        usernameHeader = config.getProperty(RangerPdpConstants.PROP_AUTHN_HEADER_USERNAME, "X-Forwarded-User");

        LOG.info("HttpHeaderAuthHandler initialized; username header={}", usernameHeader);
    }

    @Override
    public Result authenticate(HttpServletRequest request, HttpServletResponse response) {
        String userName = request.getHeader(usernameHeader);

        LOG.debug("authenticate(): user={} (from header {})", userName, usernameHeader);

        return StringUtils.isBlank(userName) ? Result.skip() : Result.authenticated(userName.trim(), AUTH_TYPE);
    }

    @Override
    public String getChallengeHeader() {
        return null;
    }
}
