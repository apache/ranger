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

import org.apache.ranger.audit.server.AuditConfig;
import org.apache.ranger.authz.handler.RangerAuth;
import org.apache.ranger.authz.handler.jwt.RangerDefaultJwtAuthHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.authentication.AbstractAuthenticationToken;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.core.userdetails.User;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.web.authentication.WebAuthenticationDetails;

import javax.annotation.PostConstruct;
import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class AuditJwtAuthFilter extends RangerDefaultJwtAuthHandler implements Filter {
    private static final Logger  LOG                 = LoggerFactory.getLogger(AuditJwtAuthFilter.class);
    private static final String  DEFAULT_AUDIT_ROLE  = "ROLE_USER";
    private static final String  CONFIG_PREFIX       = "config.prefix";
    private static final String  DEFAULT_JWT_PREFIX  = "ranger.audit.jwt.auth";

    @PostConstruct
    public void initialize() {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> AuditJwtAuthFilter.initialize()");
        }

        AuditConfig auditConfig = AuditConfig.getInstance();

        // Check if JWT authentication is enabled
        String  configPrefix = auditConfig.get(CONFIG_PREFIX, DEFAULT_JWT_PREFIX).concat(".");
        boolean jwtEnabled   = auditConfig.getBoolean(configPrefix + "enabled", false);

        if (!jwtEnabled) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("JWT authentication is disabled, skipping initialization");
            }
            if (LOG.isDebugEnabled()) {
                LOG.debug("<<<=== AuditJwtAuthFilter.initialize()");
            }
            return;
        }

        /**
         * If this filter is configured in spring security. The
         * {@link org.springframework.web.filter.DelegatingFilterProxy
         * DelegatingFilterProxy} does not invoke init method (like Servlet container).
         */
        try {
            Properties config   = new Properties();
            Properties allProps = auditConfig.getProperties();

            for (String key : allProps.stringPropertyNames()) {
                if (key.startsWith(configPrefix)) {
                    String mappedKey = key.substring(configPrefix.length());
                    // Map audit-specific property names to RangerJwtAuthHandler expected names
                    if ("provider-url".equals(mappedKey)) {
                        mappedKey = "jwks.provider-url";
                    } else if ("public-key".equals(mappedKey)) {
                        mappedKey = "jwt.public-key";
                    } else if ("cookie.name".equals(mappedKey)) {
                        mappedKey = "jwt.cookie-name";
                    } else if ("audiences".equals(mappedKey)) {
                        mappedKey = "jwt.audiences";
                    }
                    config.put(mappedKey, auditConfig.get(key));
                }
            }

            if (LOG.isDebugEnabled()) {
                LOG.debug("JWT Auth configs : {}", config.toString());
            }

            super.initialize(config);
        } catch (Exception e) {
            LOG.error("Failed to initialize Audit JWT Auth Filter.", e);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== AuditJwtAuthFilter.initialize()");
        }
    }

    @Override
    public void init(FilterConfig filterConfig) throws ServletException {
    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
            throws IOException, ServletException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> AuditJwtAuthFilter.doFilter({}, {}, {})", request, response, chain);
        }

        // Check if JWT authentication is enabled
        AuditConfig auditConfig  = AuditConfig.getInstance();
        String      configPrefix = auditConfig.get(CONFIG_PREFIX, DEFAULT_JWT_PREFIX).concat(".");
        boolean     jwtEnabled   = auditConfig.getBoolean(configPrefix + "enabled", false);

        if (!jwtEnabled) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("JWT authentication is disabled, passing request to next filter");
            }
            chain.doFilter(request, response);
            return;
        }

        if (request != null) {
            if (canAuthenticateRequest(request)) {
                Authentication previousAuth = SecurityContextHolder.getContext().getAuthentication();

                // check if not already authenticated
                if (previousAuth == null || !previousAuth.isAuthenticated()) {
                    HttpServletRequest httpServletRequest = (HttpServletRequest) request;

                    RangerAuth rangerAuth = authenticate(httpServletRequest);

                    if (rangerAuth != null) {
                        final List<GrantedAuthority>  grantedAuths         = Arrays.asList(new SimpleGrantedAuthority(DEFAULT_AUDIT_ROLE));
                        final UserDetails              principal           = new User(rangerAuth.getUserName(), "", grantedAuths);
                        final Authentication           finalAuthentication = new UsernamePasswordAuthenticationToken(principal, "", grantedAuths);
                        final WebAuthenticationDetails webDetails          = new WebAuthenticationDetails(httpServletRequest);

                        ((AbstractAuthenticationToken) finalAuthentication).setDetails(webDetails);
                        SecurityContextHolder.getContext().setAuthentication(finalAuthentication);
                    }
                } else {
                    // Already authenticated
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("User [{}] is already authenticated, proceeding with filter chain.", previousAuth.getPrincipal());
                    }
                }

                // Log final status of request.
                Authentication finalAuth = SecurityContextHolder.getContext().getAuthentication();
                if (finalAuth != null) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("AuditJwtAuthFilter.doFilter() - user=[{}], isUserAuthenticated? [{}]", finalAuth.getPrincipal(), finalAuth.isAuthenticated());
                    }
                } else {
                    LOG.warn("AuditJwtAuthFilter.doFilter() - Failed to authenticate request using Audit JWT authentication framework.");
                }
            } else {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Skipping JWT Audit auth for request.");
                }
            }
        }

        chain.doFilter(request, response); // proceed with filter chain

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== AuditJwtAuthFilter.doFilter()");
        }
    }

    @Override
    public void destroy() {
        // Auto-generated method stub
    }
}
