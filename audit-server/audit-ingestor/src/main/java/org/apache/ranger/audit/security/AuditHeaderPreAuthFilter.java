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

import org.apache.commons.lang3.StringUtils;
import org.apache.ranger.audit.server.AuditServerConfig;
import org.apache.ranger.audit.server.AuditServerConstants;
import org.apache.ranger.plugin.util.SpiffeIdUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.core.userdetails.User;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.web.authentication.WebAuthenticationDetails;
import org.springframework.web.filter.GenericFilterBean;

import javax.annotation.PostConstruct;
import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

/**
 * Authenticates audit REST requests using identity headers set by a trusted proxy. The resolved
 * principal is still subject to the per-service allowed users check in the audit REST API.
 */
public class AuditHeaderPreAuthFilter extends GenericFilterBean {
    private static final Logger LOG = LoggerFactory.getLogger(AuditHeaderPreAuthFilter.class);

    public static final String PROP_HEADER_AUTH_ENABLED  = AuditServerConstants.PROP_PREFIX_AUDIT_SERVER + "authn.header.enabled";
    public static final String PROP_USERNAME_HEADER_NAME = AuditServerConstants.PROP_PREFIX_AUDIT_SERVER + "authn.header.username";
    public static final String PROP_SPIFFE_HEADER_NAME   = AuditServerConstants.PROP_PREFIX_AUDIT_SERVER + "authn.header.spiffe";

    private static final String DEFAULT_AUDIT_ROLE = "ROLE_USER";

    private boolean      headerAuthEnabled;
    private String       userNameHeaderName;
    private List<String> spiffeHeaderNames;

    @PostConstruct
    protected void initialize() {
        AuditServerConfig auditConfig = AuditServerConfig.getInstance();

        headerAuthEnabled = auditConfig.getBoolean(PROP_HEADER_AUTH_ENABLED, false);

        if (headerAuthEnabled) {
            userNameHeaderName = StringUtils.trimToNull(auditConfig.get(PROP_USERNAME_HEADER_NAME));
            spiffeHeaderNames  = SpiffeIdUtil.parseHeaderNames(auditConfig.get(PROP_SPIFFE_HEADER_NAME));

            if (userNameHeaderName == null && spiffeHeaderNames.isEmpty()) {
                LOG.warn("Disabling header-based authentication, as neither {} nor {} is set", PROP_USERNAME_HEADER_NAME, PROP_SPIFFE_HEADER_NAME);

                headerAuthEnabled = false;
            } else {
                LOG.info("Header-based authentication is enabled: usernameHeader={}, spiffeHeaders={}", userNameHeaderName, spiffeHeaderNames);
            }
        }
    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain) throws IOException, ServletException {
        if (headerAuthEnabled) {
            Authentication existingAuthn = SecurityContextHolder.getContext().getAuthentication();

            if (existingAuthn == null || !existingAuthn.isAuthenticated()) {
                HttpServletRequest httpRequest = (HttpServletRequest) request;
                String             username    = resolvePrincipal(httpRequest);

                if (username != null) {
                    List<GrantedAuthority>              grantedAuths = Collections.singletonList(new SimpleGrantedAuthority(DEFAULT_AUDIT_ROLE));
                    UserDetails                         principal    = new User(username, "", grantedAuths);
                    UsernamePasswordAuthenticationToken authToken    = new UsernamePasswordAuthenticationToken(principal, "", grantedAuths);

                    authToken.setDetails(new WebAuthenticationDetails(httpRequest));

                    SecurityContextHolder.getContext().setAuthentication(authToken);

                    LOG.debug("Authenticated request using trusted headers for user={}", username);
                } else {
                    LOG.debug("No trusted identity header found in the request!");
                }
            }
        } else {
            LOG.debug("Header-based authentication is disabled!");
        }

        chain.doFilter(request, response);
    }

    /**
     * Resolves the principal from trusted headers. The username header (user identity) takes
     * precedence; when it is absent, the SPIFFE header (service identity) is used and the
     * full SPIFFE ID becomes the principal (SPIFFE IDs are used as usernames in Ranger).
     */
    private String resolvePrincipal(HttpServletRequest httpRequest) {
        String ret = userNameHeaderName != null ? StringUtils.trimToNull(httpRequest.getHeader(userNameHeaderName)) : null;

        if (ret == null) {
            for (String spiffeHeaderName : spiffeHeaderNames) {
                String spiffeId = StringUtils.trimToNull(httpRequest.getHeader(spiffeHeaderName));

                if (SpiffeIdUtil.isValidSpiffeId(spiffeId)) {
                    LOG.debug("Resolved SPIFFE ID '{}' from header '{}'", spiffeId, spiffeHeaderName);

                    ret = spiffeId;

                    break;
                } else if (spiffeId != null) {
                    LOG.warn("SPIFFE header '{}' value is not a well-formed SPIFFE ID", spiffeHeaderName);
                }
            }
        }

        return ret;
    }
}
