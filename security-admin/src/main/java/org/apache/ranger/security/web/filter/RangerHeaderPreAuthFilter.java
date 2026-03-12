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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.core.userdetails.User;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.web.authentication.WebAuthenticationDetails;
import org.springframework.web.filter.GenericFilterBean;

import javax.annotation.PostConstruct;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
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

    public static final String PROP_HEADER_AUTH_ENABLED        = "ranger.authn.header.enabled";
    public static final String PROP_USERNAME_HEADER_NAME       = "ranger.authn.header.username";
    public static final String PROP_REQUEST_ID_HEADER_NAME     = "ranger.authn.header.requestid";

    private static boolean headerAuthEnabled;
    private static String  userNameHeader;

    @Autowired
    UserMgr userMgr;

    @PostConstruct
    public void initialize(FilterConfig filterConfig) throws ServletException {
        headerAuthEnabled = PropertiesUtil.getBooleanProperty(PROP_HEADER_AUTH_ENABLED, false);
        userNameHeader    = PropertiesUtil.getProperty(PROP_USERNAME_HEADER_NAME);
    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain) throws IOException, ServletException {
        HttpServletRequest  httpRequest  = (HttpServletRequest) request;
        String username                  = StringUtils.trimToNull(httpRequest.getHeader(userNameHeader));

        if (!headerAuthEnabled || StringUtils.isBlank(username)) {
            LOG.debug("Header-based authentication is disabled or username header is missing/empty!");
        } else {
            List<GrantedAuthority>      grantedAuthorities = getAuthoritiesFromRanger(username);
            final UserDetails           principal          = new User(username, "", grantedAuthorities);
            UsernamePasswordAuthenticationToken authToken  = new UsernamePasswordAuthenticationToken(principal, "", grantedAuthorities);

            authToken.setDetails(new WebAuthenticationDetails(httpRequest));

            SecurityContextHolder.getContext().setAuthentication(authToken);

            LOG.debug("Authenticated request using trusted headers for user={}", username);
        }

        chain.doFilter(request, response);
    }

    /**
     * Loads authorities from Ranger DB
     */
    private List<GrantedAuthority> getAuthoritiesFromRanger(String username) {
        List<GrantedAuthority> ret = new ArrayList<>();
        Collection<String>    roleList = userMgr.getRolesByLoginId(username);

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
