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

/**
 * Servlet filter that enforces authentication for all PDP REST endpoints.
 *
 * <p>Handlers are configured via the {@code ranger.pdp.auth.types} filter init parameter
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
public class RangerPdpAuthFilter implements Filter {
    private static final Logger LOG = LoggerFactory.getLogger(RangerPdpAuthFilter.class);

    // Auth type list — pdp-specific
    public static final String PARAM_AUTH_TYPES = RangerPdpConstants.PROP_AUTH_TYPES;

    // HTTP Header auth — consistent with ranger.admin.authn.header.* in security-admin
    public static final String PARAM_HEADER_AUTHN_ENABLED  = RangerPdpConstants.PROP_AUTHN_HEADER_ENABLED;
    public static final String PARAM_HEADER_AUTHN_USERNAME = RangerPdpConstants.PROP_AUTHN_HEADER_USERNAME;

    // JWT bearer token auth
    public static final String PARAM_JWT_PROVIDER_URL = RangerPdpConstants.PROP_AUTHN_JWT_PROVIDER_URL;
    public static final String PARAM_JWT_PUBLIC_KEY   = RangerPdpConstants.PROP_AUTHN_JWT_PUBLIC_KEY;
    public static final String PARAM_JWT_COOKIE_NAME  = RangerPdpConstants.PROP_AUTHN_JWT_COOKIE_NAME;
    public static final String PARAM_JWT_AUDIENCES    = RangerPdpConstants.PROP_AUTHN_JWT_AUDIENCES;

    // Kerberos / SPNEGO
    public static final String PARAM_SPNEGO_PRINCIPAL   = RangerPdpConstants.PROP_AUTHN_KERBEROS_SPNEGO_PRINCIPAL;
    public static final String PARAM_SPNEGO_KEYTAB      = RangerPdpConstants.PROP_AUTHN_KERBEROS_SPNEGO_KEYTAB;
    public static final String PARAM_KRB_NAME_RULES     = RangerPdpConstants.PROP_KRB_NAME_RULES;
    public static final String PARAM_KRB_TOKEN_VALIDITY = RangerPdpConstants.PROP_AUTHN_KERBEROS_KRB_TOKEN_VALIDITY;
    public static final String PARAM_KRB_COOKIE_DOMAIN  = RangerPdpConstants.PROP_AUTHN_KERBEROS_KRB_COOKIE_DOMAIN;
    public static final String PARAM_KRB_COOKIE_PATH    = RangerPdpConstants.PROP_AUTHN_KERBEROS_KRB_COOKIE_PATH;

    private final List<PdpAuthHandler> handlers = new ArrayList<>();

    @Override
    public void init(FilterConfig filterConfig) throws ServletException {
        Properties config            = toProperties(filterConfig);
        String     types             = filterConfig.getInitParameter(PARAM_AUTH_TYPES);
        boolean    headerAuthEnabled = Boolean.parseBoolean(StringUtils.defaultIfBlank(filterConfig.getInitParameter(PARAM_HEADER_AUTHN_ENABLED), "false"));

        if (StringUtils.isBlank(types)) {
            types = "jwt,kerberos";
        }

        for (String type : types.split(",")) {
            String normalizedType = type.trim().toLowerCase();

            if ("header".equals(normalizedType) && !headerAuthEnabled) {
                LOG.info("Header auth type is configured but disabled via {}=false; skipping handler registration", PARAM_HEADER_AUTHN_ENABLED);

                continue;
            }

            PdpAuthHandler handler = createHandler(normalizedType);

            if (handler == null) {
                LOG.warn("Unknown auth type ignored: {}", type);

                continue;
            }

            try {
                handler.init(config);
                handlers.add(handler);

                LOG.info("Registered auth handler: {}", normalizedType);
            } catch (Exception e) {
                LOG.error("Failed to initialize auth handler: {}", type, e);
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

        for (PdpAuthHandler handler : handlers) {
            PdpAuthHandler.Result result = handler.authenticate(httpReq, httpResp);

            switch (result.getStatus()) {
                case AUTHENTICATED:
                    httpReq.setAttribute(RangerPdpConstants.ATTR_AUTHENTICATED_USER, result.getUserName());
                    httpReq.setAttribute(RangerPdpConstants.ATTR_AUTH_TYPE, result.getAuthType());

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
        for (PdpAuthHandler handler : handlers) {
            String challenge = handler.getChallengeHeader();

            if (StringUtils.isNotBlank(challenge)) {
                response.addHeader("WWW-Authenticate", challenge);
            }
        }

        response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
        response.setContentType("application/json");
        response.getWriter().write("{\"code\":\"UNAUTHENTICATED\",\"message\":\"Authentication required\"}");
    }

    private PdpAuthHandler createHandler(String type) {
        switch (type) {
            case "header":    return new HttpHeaderAuthHandler();
            case "jwt":       return new JwtAuthHandler();
            case "kerberos":  return new KerberosAuthHandler();
            default:          return null;
        }
    }

    private Properties toProperties(FilterConfig filterConfig) {
        Properties props = new Properties();

        java.util.Enumeration<String> names = filterConfig.getInitParameterNames();

        while (names.hasMoreElements()) {
            String name = names.nextElement();

            props.setProperty(name, filterConfig.getInitParameter(name));
        }

        return props;
    }
}
