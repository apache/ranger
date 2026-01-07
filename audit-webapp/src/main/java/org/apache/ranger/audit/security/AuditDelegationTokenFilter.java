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

import org.apache.commons.collections.iterators.IteratorEnumeration;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.authentication.server.KerberosAuthenticationHandler;
import org.apache.hadoop.security.authentication.server.PseudoAuthenticationHandler;
import org.apache.hadoop.security.token.delegation.web.DelegationTokenAuthenticationFilter;
import org.apache.hadoop.security.token.delegation.web.DelegationTokenAuthenticationHandler;
import org.apache.hadoop.security.token.delegation.web.KerberosDelegationTokenAuthenticationHandler;
import org.apache.hadoop.security.token.delegation.web.PseudoDelegationTokenAuthenticationHandler;
import org.apache.ranger.audit.server.AuditConfig;
import org.apache.ranger.audit.utils.AuditServerLogFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;

import javax.annotation.PostConstruct;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

public class AuditDelegationTokenFilter extends DelegationTokenAuthenticationFilter {
    private static final Logger LOG = LoggerFactory.getLogger(AuditDelegationTokenFilter.class);

    private final ServletContext nullContext = new NullServletContext();

    protected AuditConfig auditConfig;

    private String tokenKindStr;

    private static final String AUDIT_DELEGATION_TOKEN_KIND_DEFAULT = "AUDIT_DELEGATION_TOKEN";

    // Uses "ranger.audit" prefix for KerberosAuthenticationHandler
    private static final String DEFAULT_CONFIG_PREFIX = "ranger.audit";
    private static final String PROP_CONFIG_PREFIX    = "config.prefix";

    /**
     * Initialized this filter. Only PROP_CONFIG_PREFIX needs to be set.
     * Rest other configurations will be fetched from {@link java.util.Properties config}
     * return by {@link #getConfiguration(String, FilterConfig)}.
     */
    @PostConstruct
    public void initialize() {
        LOG.debug("==> AuditDelegationTokenFilter.initialize()");

        this.auditConfig = AuditConfig.getInstance();
        LOG.debug("AuditConfig instance obtained: [{}]", auditConfig);

        try {
            // Currently setting tokenKindStr to default value. Later we can configure as per requirement.
            tokenKindStr = AUDIT_DELEGATION_TOKEN_KIND_DEFAULT;
            LOG.debug("Token kind set to: [{}]", tokenKindStr);

            final Map<String, String> params = new HashMap<>();
            params.put(PROP_CONFIG_PREFIX, DEFAULT_CONFIG_PREFIX);
            LOG.debug("Config Prefix used for AuditDelegationTokenFilter is [{}].", params.get(PROP_CONFIG_PREFIX));

            // Log all audit config properties for debugging
            AuditServerLogFormatter.LogBuilder logBuilder = AuditServerLogFormatter.builder("===> All audit config properties:");
            Properties allProps = auditConfig.getProperties();
            for (String key : allProps.stringPropertyNames()) {
                logBuilder.add(key, allProps.getProperty(key));
            }
            logBuilder.logDebug(LOG);

            FilterConfig filterConfigNew = new FilterConfig() {
                @Override
                public ServletContext getServletContext() {
                    return nullContext;
                }

                @SuppressWarnings("unchecked")
                @Override
                public Enumeration<String> getInitParameterNames() {
                    return new IteratorEnumeration(params.keySet().iterator());
                }

                @Override
                public String getInitParameter(String param) {
                    String value = params.get(param);
                    LOG.debug("FilterConfig.getInitParameter({}) = [{}]", param, value);
                    return value;
                }

                @Override
                public String getFilterName() {
                    return "AuditDelegationTokenFilter";
                }
            };

            LOG.debug("Initializing parent DelegationTokenAuthenticationFilter");
            super.init(filterConfigNew);
            LOG.debug("Parent DelegationTokenAuthenticationFilter initialized successfully");
        } catch (ServletException e) {
            LOG.error("AuditDelegationTokenFilter(): initialization failure", e);
            throw new RuntimeException("Failed to initialize AuditDelegationTokenFilter", e);
        }

        LOG.debug("<== AuditDelegationTokenFilter.initialize()");
    }

    /**
     * Return {@link java.util.Properties configuration} required for this filter to work properly.
     */
    @Override
    protected Properties getConfiguration(String configPrefix, FilterConfig filterConfig) throws ServletException {
        LOG.debug("==> getConfiguration() - configPrefix: [{}]", configPrefix);

        Properties props    = new Properties();
        Properties allProps = auditConfig.getProperties();

        // Log all available properties for debugging
        AuditServerLogFormatter.LogBuilder confLogBuilder = AuditServerLogFormatter.builder("====> All available audit config properties:");
        for (String key : allProps.stringPropertyNames()) {
            if (key.startsWith(configPrefix)) {
                confLogBuilder.add(key, auditConfig.get(key));
            }
        }
        confLogBuilder.logDebug(LOG);

        // Get all properties with the given prefix
        // configPrefix comes with trailing dot (e.g., "ranger.audit.")
        for (String key : allProps.stringPropertyNames()) {
            if (key.startsWith(configPrefix)) {
                // Remove the prefix - configPrefix already includes the trailing dot
                String shortKey = key.substring(configPrefix.length());
                String value = auditConfig.get(key);
                props.put(shortKey, value);
                LOG.debug("Added property: {} = {}", shortKey, value);
            }
        }

        // Map kerberos.type to type (Hadoop expects "type" property)
        String kerberosType = props.getProperty("kerberos.type");
        if (kerberosType != null) {
            props.setProperty(AUTH_TYPE, kerberosType);
            LOG.debug("Mapped kerberos.type [{}] to AUTH_TYPE", kerberosType);
        }

        String authType = props.getProperty(AUTH_TYPE, "simple");
        LOG.debug("Authentication type: [{}]", authType);

        if (authType.equals(PseudoAuthenticationHandler.TYPE)) {
            props.setProperty(AUTH_TYPE, PseudoDelegationTokenAuthenticationHandler.class.getName());
            LOG.debug("Using PseudoDelegationTokenAuthenticationHandler");
        } else if (authType.equals(KerberosAuthenticationHandler.TYPE)) {
            props.setProperty(AUTH_TYPE, KerberosDelegationTokenAuthenticationHandler.class.getName());
            LOG.debug("Using KerberosDelegationTokenAuthenticationHandler");
        }

        props.setProperty(DelegationTokenAuthenticationHandler.TOKEN_KIND, tokenKindStr);

        // Resolve _HOST into bind address
        String bindAddress = auditConfig.get("ranger.audit.server.bind.address");
        LOG.debug("Bind address from config: [{}]", bindAddress);

        if (Objects.isNull(bindAddress)) {
            LOG.warn("No host name configured.  Defaulting to local host name.");

            try {
                bindAddress = InetAddress.getLocalHost().getHostName();
                LOG.debug("Resolved bind address to: [{}]", bindAddress);
            } catch (UnknownHostException e) {
                LOG.error("Unable to obtain host name", e);
                throw new ServletException("Unable to obtain host name", e);
            }
        }

        // Resolve principle using latest bindAddress
        String principal = props.getProperty(KerberosAuthenticationHandler.PRINCIPAL);
        LOG.debug("Original Kerberos principal: [{}]", principal);

        if (Objects.nonNull(principal)) {
            try {
                String resolvedPrincipal = SecurityUtil.getServerPrincipal(principal, bindAddress);
                LOG.debug("Resolved Kerberos principal: [{}] -> [{}]", principal, resolvedPrincipal);
                props.put(KerberosAuthenticationHandler.PRINCIPAL, resolvedPrincipal);
            } catch (IOException ex) {
                LOG.error("Could not resolve Kerberos principal name: [{}] with bind address: [{}]", principal, bindAddress, ex);
                throw new RuntimeException("Could not resolve Kerberos principal name: " + ex.toString(), ex);
            }
        }

        // Log final authentication configuration
        AuditServerLogFormatter.LogBuilder finalAuthnConfLogBuilder = AuditServerLogFormatter.builder("====> Final authentication configuration:");
        for (String key : props.stringPropertyNames()) {
            finalAuthnConfLogBuilder.add(key, props.getProperty(key));
        }
        finalAuthnConfLogBuilder.logDebug(LOG);

        LOG.debug("<== getConfiguration()");

        return props;
    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain filterChain) throws IOException, ServletException {
        HttpServletRequest httpRequest = (HttpServletRequest) request;
        HttpServletResponse httpResponse = (HttpServletResponse) response;
        String requestURI = httpRequest.getRequestURI();

        // Log request details and authentication headers
        AuditServerLogFormatter.builder("===>>> AuditDelegationTokenFilter.doFilter()")
                .add("Request URI", requestURI)
                .add("Request method", httpRequest.getMethod())
                .add("Remote address", httpRequest.getRemoteAddr())
                .add("Remote host", httpRequest.getRemoteHost())
                .add("User-Agent", httpRequest.getHeader("User-Agent"))
                .add("Authorization header", httpRequest.getHeader("Authorization"))
                .add("WWW-Authenticate header", httpRequest.getHeader("WWW-Authenticate"))
                .logDebug(LOG);

        // Skip authentication for anonymous/public endpoints (health check, status)
        // These are marked as permitAll() in Spring Security config
        if (requestURI != null && (requestURI.endsWith("/api/audit/health") || requestURI.endsWith("/api/audit/status"))) {
            LOG.debug("Skipping authentication for public endpoint: {}", requestURI);
            filterChain.doFilter(request, response);
            return;
        }

        // Log all headers for debugging
        AuditServerLogFormatter.LogBuilder logBuilder = AuditServerLogFormatter.builder("===>>> All request headers:");
        Enumeration<String> headerNames = httpRequest.getHeaderNames();
        while (headerNames.hasMoreElements()) {
            String headerName = headerNames.nextElement();
            String headerValue = httpRequest.getHeader(headerName);
            logBuilder.add(headerName, headerValue);
        }
        logBuilder.logDebug(LOG);

        Authentication previousAuth = SecurityContextHolder.getContext().getAuthentication();
        LOG.debug("Previous authentication: [{}]", previousAuth);

        if (previousAuth == null || !previousAuth.isAuthenticated()) {
            FilterChainWrapper filterChainWrapper = new FilterChainWrapper(filterChain);

            LOG.debug("No previous authentication found, attempting authentication via AuditDelegationTokenFilter");

            try {
                super.doFilter(request, response, filterChainWrapper);
                LOG.debug("Authentication attempt completed");
            } catch (Exception e) {
                LOG.error("Authentication failed with exception", e);
                throw e;
            }
        } else {
            // Already authenticated
            LOG.debug("User [{}] is already authenticated, proceeding with filter chain.", previousAuth.getPrincipal());
            LOG.debug("Authentication details: name=[{}], authorities=[{}], authenticated=[{}]", previousAuth.getName(), previousAuth.getAuthorities(), previousAuth.isAuthenticated());
            filterChain.doFilter(request, response);
        }

        Authentication authentication  = SecurityContextHolder.getContext().getAuthentication();
        boolean        isAuthenticated = (authentication != null && authentication.isAuthenticated());

        //LOG Final Authentication status
        AuditServerLogFormatter.LogBuilder authnStatusLogBuilder = AuditServerLogFormatter.builder("===>>> Final Authentication ");
        authnStatusLogBuilder.add("status", isAuthenticated);
        if (authentication != null) {
            authnStatusLogBuilder.add("Authenticated user", authentication.getName());
            authnStatusLogBuilder.add("Authentication class", authentication.getClass().getSimpleName());
            authnStatusLogBuilder.add("Authentication authorities", authentication.getAuthorities());
            authnStatusLogBuilder.logDebug(LOG);
        } else {
            LOG.debug("No authentication found in SecurityContext");
        }

        // Log response status and headers
        LOG.debug("Response status: [{}]", httpResponse.getStatus());
        LOG.debug("Response WWW-Authenticate header: [{}]", httpResponse.getHeader("WWW-Authenticate"));
        LOG.debug("Response Set-Cookie header: [{}]", httpResponse.getHeader("Set-Cookie"));

        // Log all response headers for debugging
        AuditServerLogFormatter.LogBuilder responseHeadersLog = AuditServerLogFormatter.builder("===>>> Response headers:");
        for (String headerName : httpResponse.getHeaderNames()) {
            responseHeadersLog.add(headerName, httpResponse.getHeader(headerName));
        }
        responseHeadersLog.logDebug(LOG);

        LOG.debug("<== AuditDelegationTokenFilter.doFilter()");
    }
}
