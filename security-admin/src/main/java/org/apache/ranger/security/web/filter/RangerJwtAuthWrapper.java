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

import javax.annotation.PostConstruct;
import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.apache.ranger.common.PropertiesUtil;
import org.apache.ranger.common.UserSessionBase;
import org.apache.ranger.security.context.RangerContextHolder;
import org.apache.ranger.security.context.RangerSecurityContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.GenericFilterBean;

@Lazy(true)
@Component
public class RangerJwtAuthWrapper extends GenericFilterBean {
    private static final Logger LOG = Logger.getLogger(RangerJwtAuthWrapper.class);

    private String[] browserUserAgents = new String[] {""}; //Initialize with empty

    @Lazy(true)
    @Autowired
    RangerJwtAuthFilter rangerJwtAuthFilter;

    @PostConstruct
    public void initialize() {
        //FIXME: Browser agents should be common across ALL filters.
        String defaultUserAgent = PropertiesUtil.getProperty(RangerSSOAuthenticationFilter.DEFAULT_BROWSER_USERAGENT);
        String userAgent        = PropertiesUtil.getProperty(RangerSSOAuthenticationFilter.BROWSER_USERAGENT);

        if (StringUtils.isBlank(userAgent) && StringUtils.isNotBlank(defaultUserAgent)) {
            userAgent = defaultUserAgent;
        }

        if (StringUtils.isNotBlank(userAgent)) {
            browserUserAgents = userAgent.split(",");
        }
    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain filterChain) throws IOException, ServletException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("===>>> RangerJwtAuthWrapper.doFilter(" + request + ", " + response + ", " + filterChain + ")");
        }

        RangerSecurityContext context             = RangerContextHolder.getSecurityContext();
        UserSessionBase       session             = context != null ? context.getUserSession() : null;
        boolean               ssoEnabled          = session != null ? session.isSSOEnabled() : PropertiesUtil.getBooleanProperty("ranger.sso.enabled", false);
        boolean               useJwtAuthMechanism = request != null && !isRequestAuthenticated() && RangerJwtAuthFilter.canAuthenticateRequest(request);

        if (!ssoEnabled && useJwtAuthMechanism) {
            rangerJwtAuthFilter.doFilter(request, response, filterChain);

            if (!isRequestAuthenticated()) {
                String userAgent = ((HttpServletRequest) request).getHeader("User-Agent");
                if (isBrowserAgent(userAgent)) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Redirecting to login page as request does not have valid JWT auth details.");
                    }
                    ((HttpServletResponse) response).sendRedirect("/login.jsp");
                }
            }
        } else {
            if (LOG.isDebugEnabled()) {
                LOG.debug("===>> RangerJwtAuthWrapper.doFilter() - Skipping JWT auth.");
            }
        }

        filterChain.doFilter(request, response); // proceed with filter chain

        if (LOG.isDebugEnabled()) {
            LOG.debug("<<<=== RangerJwtAuthWrapper.doFilter()");
        }
    }

    private boolean isRequestAuthenticated() {
        Authentication auth = SecurityContextHolder.getContext().getAuthentication();
        return auth != null && auth.isAuthenticated();
    }

    protected boolean isBrowserAgent(String userAgent) {
        boolean isBrowserAgent = false;

        if (browserUserAgents.length > 0 && StringUtils.isNotBlank(userAgent)) {
            for (String ua : browserUserAgents) {
                if (userAgent.toLowerCase().startsWith(ua.toLowerCase())) {
                    isBrowserAgent = true;
                    break;
                }
            }
        }

        return isBrowserAgent;
    }
}
