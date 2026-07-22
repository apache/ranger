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

import org.apache.commons.lang3.StringUtils;
import org.apache.ranger.biz.UserMgr;
import org.apache.ranger.common.PropertiesUtil;
import org.apache.ranger.entity.XXAuthSession;
import org.apache.ranger.plugin.util.SpiffeIdUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
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
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class RangerHeaderPreAuthFilter extends GenericFilterBean {
    private static final Logger LOG = LoggerFactory.getLogger(RangerHeaderPreAuthFilter.class);

    public static final String PROP_HEADER_AUTH_ENABLED    = "ranger.admin.authn.header.enabled";
    public static final String PROP_USERNAME_HEADER_NAME   = "ranger.admin.authn.header.username";
    public static final String PROP_SPIFFE_HEADER_NAME     = "ranger.admin.authn.header.spiffe";
    public static final String PROP_REQUEST_ID_HEADER_NAME = "ranger.admin.authn.header.requestid";

    private boolean      headerAuthEnabled;
    private String       userNameHeaderName;
    private List<String> spiffeHeaderNames;

    @Autowired
    UserMgr userMgr;

    @PostConstruct
    protected void initialize() {
        headerAuthEnabled = PropertiesUtil.getBooleanProperty(PROP_HEADER_AUTH_ENABLED, false);

        if (headerAuthEnabled) {
            userNameHeaderName = PropertiesUtil.getProperty(PROP_USERNAME_HEADER_NAME);
            spiffeHeaderNames  = SpiffeIdUtil.parseHeaderNames(PropertiesUtil.getProperty(PROP_SPIFFE_HEADER_NAME));

            if (StringUtils.isBlank(userNameHeaderName) && spiffeHeaderNames.isEmpty()) {
                LOG.warn("Disabling header-based authentication, as neither {} nor {} is set", PROP_USERNAME_HEADER_NAME, PROP_SPIFFE_HEADER_NAME);

                headerAuthEnabled = false;
            }
        }
    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain) throws IOException, ServletException {
        if (headerAuthEnabled) {
            Authentication existingAuthn = SecurityContextHolder.getContext().getAuthentication();

            if (existingAuthn == null || !existingAuthn.isAuthenticated()) {
                HttpServletRequest  httpRequest = (HttpServletRequest) request;
                String              username    = resolvePrincipal(httpRequest);

                if (StringUtils.isNotBlank(username)) {
                    List<GrantedAuthority>    grantedAuthorities = getAuthoritiesFromRanger(username);
                    final UserDetails         principal          = new User(username, "", grantedAuthorities);
                    RangerAuthenticationToken authToken          = new RangerAuthenticationToken(principal, grantedAuthorities, XXAuthSession.AUTH_TYPE_TRUSTED_PROXY);

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
     * precedence; when it is absent, the SPIFFE header (service-to-service identity) is used and
     * the trailing service-account segment of the SPIFFE ID becomes the principal.
     */
    private String resolvePrincipal(HttpServletRequest httpRequest) {
        String username = StringUtils.isNotBlank(userNameHeaderName) ? StringUtils.trimToNull(httpRequest.getHeader(userNameHeaderName)) : null;

        if (StringUtils.isNotBlank(username)) {
            return username;
        }

        for (String spiffeHeaderName : spiffeHeaderNames) {
            String spiffeId       = StringUtils.trimToNull(httpRequest.getHeader(spiffeHeaderName));
            String serviceAccount = SpiffeIdUtil.extractServiceAccount(spiffeId);

            if (StringUtils.isNotBlank(serviceAccount)) {
                LOG.debug("Resolved service-account '{}' from SPIFFE header '{}'", serviceAccount, spiffeHeaderName);

                return serviceAccount;
            } else if (StringUtils.isNotBlank(spiffeId)) {
                LOG.warn("SPIFFE header '{}' value is not a well-formed SPIFFE ID", spiffeHeaderName);
            }
        }

        return null;
    }

    /**
     * Loads authorities from Ranger DB
     */
    private List<GrantedAuthority> getAuthoritiesFromRanger(String username) {
        List<GrantedAuthority> ret      = new ArrayList<>();
        Collection<String>     roleList = userMgr.getRolesByLoginId(username);

        if (roleList != null) {
            for (String role : roleList) {
                if (StringUtils.isNotBlank(role)) {
                    ret.add(new SimpleGrantedAuthority(role));
                }
            }
        }

        return ret;
    }
}
