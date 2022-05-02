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
package org.apache.ranger.security.web.filter;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import javax.annotation.PostConstruct;
import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;

import org.apache.log4j.Logger;
import org.apache.ranger.authz.handler.RangerAuth;
import org.apache.ranger.authz.handler.jwt.RangerDefaultJwtAuthHandler;
import org.apache.ranger.authz.handler.jwt.RangerJwtAuthHandler;
import org.apache.ranger.common.PropertiesUtil;
import org.springframework.context.annotation.Lazy;
import org.springframework.security.authentication.AbstractAuthenticationToken;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.core.userdetails.User;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.web.authentication.WebAuthenticationDetails;
import org.springframework.stereotype.Component;

@Lazy(true)
@Component
public class RangerJwtAuthFilter extends RangerDefaultJwtAuthHandler implements Filter {
    private static final Logger LOG                 = Logger.getLogger(RangerJwtAuthFilter.class);
    private static final String DEFAULT_RANGER_ROLE = "ROLE_USER";

    @PostConstruct
    public void initialize() {
        if (LOG.isDebugEnabled()) {
            LOG.debug("===>>> RangerJwtAuthFilter.initialize()");
        }

        /**
         * If this filter is configured in spring security. The
         * {@link org.springframework.web.filter.DelegatingFilterProxy
         * DelegatingFilterProxy} does not invoke init method (like Servlet container).
         */
        try {
            Properties config       = new Properties();

            config.setProperty(RangerJwtAuthHandler.KEY_PROVIDER_URL, PropertiesUtil.getProperty(RangerSSOAuthenticationFilter.JWT_AUTH_PROVIDER_URL));
            config.setProperty(RangerJwtAuthHandler.KEY_JWT_PUBLIC_KEY, PropertiesUtil.getProperty(RangerSSOAuthenticationFilter.JWT_PUBLIC_KEY, ""));
            config.setProperty(RangerJwtAuthHandler.KEY_JWT_COOKIE_NAME,
                               PropertiesUtil.getProperty(RangerSSOAuthenticationFilter.JWT_COOKIE_NAME, RangerSSOAuthenticationFilter.JWT_COOKIE_NAME_DEFAULT));
            config.setProperty(RangerJwtAuthHandler.KEY_JWT_AUDIENCES, PropertiesUtil.getProperty(RangerSSOAuthenticationFilter.JWT_AUDIENCES, ""));

            super.initialize(config);
        } catch (Exception e) {
            LOG.error("Failed to initialize Ranger Admin JWT Auth Filter.", e);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<<<=== RangerJwtAuthFilter.initialize()");
        }
    }

    @Override
    public void init(FilterConfig filterConfig) throws ServletException {
        // Empty method
    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain) throws IOException, ServletException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("===>>> RangerJwtAuthFilter.doFilter()");
        }

        HttpServletRequest httpServletRequest = (HttpServletRequest) request;

        RangerAuth rangerAuth = authenticate(httpServletRequest);

        if (rangerAuth != null) {
            final List<GrantedAuthority>   grantedAuths        = Arrays.asList(new SimpleGrantedAuthority(DEFAULT_RANGER_ROLE));
            final UserDetails              principal           = new User(rangerAuth.getUserName(), "", grantedAuths);
            final Authentication           finalAuthentication = new UsernamePasswordAuthenticationToken(principal, "", grantedAuths);
            final WebAuthenticationDetails webDetails          = new WebAuthenticationDetails(httpServletRequest);
            ((AbstractAuthenticationToken) finalAuthentication).setDetails(webDetails);
            SecurityContextHolder.getContext().setAuthentication(finalAuthentication);
        }

        // Log final status of request.
        Authentication auth = SecurityContextHolder.getContext().getAuthentication();
        if (auth != null) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("<<<=== RangerJwtAuthFilter.doFilter() - user=[" + auth.getPrincipal() + "], isUserAuthenticated? [" + auth.isAuthenticated() + "]");
            }
        } else {
            LOG.warn("<<<=== RangerJwtAuthFilter.doFilter() - Failed to authenticate request using Ranger JWT authentication framework.");
        }
    }

    @Override
    public void destroy() {
        // Empty method
    }
}
