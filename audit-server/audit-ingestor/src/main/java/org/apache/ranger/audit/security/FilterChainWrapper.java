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
import org.apache.hadoop.security.authentication.client.AuthenticatedURL;
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

import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

/**
 * Ranger Audit specific {@link javax.servlet.FilterChain filter-chain} wrapper.
 * This wrapper class will be used to perform additional activities after
 * {@link javax.servlet.Filter filter} based authentication.
 */
public class FilterChainWrapper implements FilterChain {
    private static final Logger LOG = LoggerFactory.getLogger(FilterChainWrapper.class);

    private static final String      DEFAULT_AUDIT_ROLE = "ROLE_USER";
    private static final String      COOKIE_PARAM       = "Set-Cookie";
    private              FilterChain filterChain;

    // Constructor
    public FilterChainWrapper(final FilterChain filterChain) {
        this.filterChain = filterChain;
    }

    /**
     * Read user from cookie.
     *
     * @param httpResponse
     * @return
     */
    private static String readUserFromCookie(HttpServletResponse httpResponse) {
        String userName = null;
        boolean isCookieSet = httpResponse.containsHeader(COOKIE_PARAM);

        if (isCookieSet) {
            Collection<String> cookies = httpResponse.getHeaders(COOKIE_PARAM);

            if (cookies != null) {
                for (String cookie : cookies) {
                    if (!StringUtils.startsWithIgnoreCase(cookie, AuthenticatedURL.AUTH_COOKIE)
                            || !cookie.contains("u=")) {
                        continue;
                    }

                    String[] split = cookie.split(";");

                    for (String s : split) {
                        if (StringUtils.startsWithIgnoreCase(s, AuthenticatedURL.AUTH_COOKIE)) {
                            int idxUEq = s.indexOf("u=");

                            if (idxUEq == -1) {
                                continue;
                            }

                            int idxAmp = s.indexOf("&", idxUEq);

                            if (idxAmp == -1) {
                                continue;
                            }

                            try {
                                userName = s.substring(idxUEq + 2, idxAmp);
                                break;
                            } catch (Exception e) {
                                userName = null;
                            }
                        }
                    }
                }
            }
        }

        return userName;
    }

    /**
     * This method sets spring security {@link org.springframework.security.core.context.SecurityContextHolder context}
     * with {@link org.springframework.security.core.Authentication authentication},
     * if not already authenticated by other filter. And carried on with {@link javax.servlet.FilterChain filter-chain}.
     * This method will be invoked by {@link org.apache.hadoop.security.authentication.server.AuthenticationFilter AuthenticationFilter},
     * only when user is authenticated.
     */
    @Override
    public void doFilter(ServletRequest servletRequest, ServletResponse servletResponse) throws IOException, ServletException {
        final HttpServletRequest httpRequest = (HttpServletRequest) servletRequest;
        final HttpServletResponse httpResponse = (HttpServletResponse) servletResponse;
        final Authentication existingAuth = SecurityContextHolder.getContext().getAuthentication();
        String userName = readUserFromCookie(httpResponse);

        if (StringUtils.isEmpty(userName) && StringUtils.isNotEmpty(httpRequest.getRemoteUser())) {
            userName = httpRequest.getRemoteUser();
        }

        if ((existingAuth == null || !existingAuth.isAuthenticated()) && StringUtils.isNotEmpty(userName)) {
            String doAsUser = httpRequest.getParameter("doAs");
            if (StringUtils.isNotEmpty(doAsUser)) {
                userName = doAsUser;
            }

            final List<GrantedAuthority> grantedAuths        = Arrays.asList(new SimpleGrantedAuthority(DEFAULT_AUDIT_ROLE));
            final UserDetails             principal           = new User(userName, "", grantedAuths);
            final Authentication          finalAuthentication = new UsernamePasswordAuthenticationToken(principal, "", grantedAuths);
            final WebAuthenticationDetails webDetails         = new WebAuthenticationDetails(httpRequest);

            ((AbstractAuthenticationToken) finalAuthentication).setDetails(webDetails);

            SecurityContextHolder.getContext().setAuthentication(finalAuthentication);

            if (LOG.isDebugEnabled()) {
                LOG.debug("User [{}] is authenticated via AuditDelegationTokenFilter.", finalAuthentication.getPrincipal());
            }
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("As user [{}] is authenticated, proceeding with filter chain.", userName);
        }
        filterChain.doFilter(httpRequest, httpResponse);
    }
}
