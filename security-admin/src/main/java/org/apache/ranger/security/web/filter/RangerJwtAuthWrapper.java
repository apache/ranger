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
import org.apache.ranger.common.PropertiesUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.GenericFilterBean;

import javax.annotation.PostConstruct;
import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.io.IOException;

@Lazy(true)
@Component
public class RangerJwtAuthWrapper extends GenericFilterBean {
    private static final Logger LOG = LoggerFactory.getLogger(RangerJwtAuthWrapper.class);

    @Lazy(true)
    @Autowired
    RangerJwtAuthFilter rangerJwtAuthFilter;

    private String[] browserUserAgents = new String[] {""}; //Initialize with empty

    @PostConstruct
    public void initialize() {
        //FIXME: Browser agents should be common across ALL filters.
        String defaultUserAgent = PropertiesUtil.getProperty(RangerJwtAuthConfig.DEFAULT_BROWSER_USERAGENT);
        String userAgent        = RangerJwtAuthConfig.getBrowserUserAgent();

        if (StringUtils.isBlank(userAgent) && StringUtils.isNotBlank(defaultUserAgent)) {
            userAgent = defaultUserAgent;
        }

        if (StringUtils.isNotBlank(userAgent)) {
            browserUserAgents = userAgent.split(",");
        }
    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain filterChain) throws IOException, ServletException {
        LOG.debug("===>>> RangerJwtAuthWrapper.doFilter({}, {}, {})", request, response, filterChain);

        HttpServletRequest httpRequest         = (HttpServletRequest) request;
        boolean            useJwtAuthMechanism = request != null && !isRequestAuthenticated() && RangerJwtAuthFilter.canAuthenticateRequest(request);

        // Skip JWT processing for health probes so that /actuator/health(/liveness|/readiness) never gets
        // redirected to the login page and works regardless of JWT configuration.
        if (useJwtAuthMechanism && !isHealthCheckRequest(httpRequest)) {
            rangerJwtAuthFilter.doFilter(request, response, filterChain);

            if (!isRequestAuthenticated()) {
                String userAgent = httpRequest.getHeader("User-Agent");

                if (isBrowserAgent(userAgent)) {
                    LOG.debug("Redirecting to login page as request does not have valid JWT auth details.");

                    ((HttpServletResponse) response).sendRedirect("/login.jsp");
                }
            }
        } else {
            LOG.debug("===>> RangerJwtAuthWrapper.doFilter() - Skipping JWT auth.");
        }

        filterChain.doFilter(request, response); // proceed with filter chain

        LOG.debug("<<<=== RangerJwtAuthWrapper.doFilter()");
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

    private boolean isHealthCheckRequest(HttpServletRequest httpRequest) {
        String requestUri = httpRequest.getRequestURI();

        return StringUtils.isNotBlank(requestUri) && requestUri.contains(RangerJwtAuthConfig.HEALTH_CHECK_URI);
    }

    private boolean isRequestAuthenticated() {
        Authentication auth = SecurityContextHolder.getContext().getAuthentication();

        return auth != null && auth.isAuthenticated();
    }
}
