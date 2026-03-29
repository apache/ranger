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

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.apache.ranger.pdp.config.RangerPdpConstants.PROP_AUTHN_TYPES;

/**
 * Servlet filter that enforces authentication for all PDP REST endpoints.
 *
 * <p>Handlers are configured via the {@code ranger.pdp.authn.types} filter init parameter
 * (comma-separated list of {@code header}, {@code jwt}, {@code kerberos}).  Handlers are
 * tried in the listed order; the first successful match wins.
 *
 * <p>On success the authenticated username is stored in the request attribute
 * {@link RangerPdpConstants#ATTR_AUTHENTICATED_USER} so that REST resources can read it.
 *
 * <p>If all handlers return {@code SKIP} (no recognisable credentials found), the filter
 * sends a {@code 401} response with {@code WWW-Authenticate} headers for every
 * configured handler that provides a challenge.
 */
public class RangerPdpAuthNFilter implements Filter {
    private static final Logger LOG = LoggerFactory.getLogger(RangerPdpAuthNFilter.class);

    private final List<PdpAuthNHandler> handlers = new ArrayList<>();

    @Override
    public void init(FilterConfig filterConfig) throws ServletException {
        Properties config     = toProperties(filterConfig);
        String     authnTypes = filterConfig.getInitParameter(PROP_AUTHN_TYPES);

        if (StringUtils.isNotBlank(authnTypes)) {
            for (String authnType : authnTypes.split(",")) {
                PdpAuthNHandler handler = createHandler(authnType.trim().toLowerCase(), filterConfig);

                if (handler == null) {
                    continue;
                }

                try {
                    handler.init(config);

                    handlers.add(handler);

                    LOG.info("{}: successfully registered authentication handler", authnType);
                } catch (Exception excp) {
                    LOG.error("{}: failed to initialize authentication handler. Handler disabled", authnType, excp);
                }
            }
        }

        if (handlers.isEmpty()) {
            throw new ServletException("No valid authentication handlers configured");
        }
    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain) throws IOException, ServletException {
        HttpServletRequest  httpReq  = (HttpServletRequest) request;
        HttpServletResponse httpResp = (HttpServletResponse) response;

        for (PdpAuthNHandler handler : handlers) {
            PdpAuthNHandler.Result result = handler.authenticate(httpReq, httpResp);

            switch (result.getStatus()) {
                case AUTHENTICATED:
                    httpReq.setAttribute(RangerPdpConstants.ATTR_AUTHENTICATED_USER, result.getUserName());
                    httpReq.setAttribute(RangerPdpConstants.ATTR_AUTHN_TYPE, result.getAuthType());

                    LOG.debug("doFilter(): authenticated user={}, type={}", result.getUserName(), result.getAuthType());

                    chain.doFilter(request, response);
                    return;

                case CHALLENGE:
                    // handler has already written the 401; stop processing
                    return;

                case SKIP:
                default:
                    // try the next handler
                    break;
            }
        }

        // No handler could authenticate the request; send a 401 with all challenge headers
        LOG.debug("doFilter(): no handler authenticated request from {}", httpReq.getRemoteAddr());

        sendUnauthenticated(httpResp);
    }

    @Override
    public void destroy() {
        handlers.clear();
    }

    private void sendUnauthenticated(HttpServletResponse response) throws IOException {
        for (PdpAuthNHandler handler : handlers) {
            String challenge = handler.getChallengeHeader();

            if (StringUtils.isNotBlank(challenge)) {
                response.addHeader("WWW-Authenticate", challenge);
            }
        }

        response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
        response.setContentType("application/json");
        response.getWriter().write("{\"code\":\"UNAUTHENTICATED\",\"message\":\"Authentication required\"}");
    }

    private PdpAuthNHandler createHandler(String type, FilterConfig filterConfig) {
        final PdpAuthNHandler ret;

        switch (type) {
            case "header":
                ret = getBoolean(filterConfig, RangerPdpConstants.PROP_AUTHN_HEADER_ENABLED) ? new HttpHeaderAuthNHandler() : null;
                break;
            case "jwt":
                ret = getBoolean(filterConfig, RangerPdpConstants.PROP_AUTHN_JWT_ENABLED) ? new JwtAuthNHandler() : null;
                break;
            case "kerberos":
                ret = getBoolean(filterConfig, RangerPdpConstants.PROP_AUTHN_KERBEROS_ENABLED) ? new KerberosAuthNHandler() : null;
                break;
            default:
                ret = null;
                break;
        }

        if (ret == null) {
            LOG.warn("{}: authentication type unknown or disabled. Ignored", type);
        }

        return ret;
    }

    private Properties toProperties(FilterConfig filterConfig) {
        Properties                    props = new Properties();
        java.util.Enumeration<String> names = filterConfig.getInitParameterNames();

        while (names.hasMoreElements()) {
            String name = names.nextElement();

            props.setProperty(name, filterConfig.getInitParameter(name));
        }

        return props;
    }

    private boolean getBoolean(FilterConfig config, String name) {
        return Boolean.parseBoolean(config.getInitParameter(name));
    }
}
