/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

package org.apache.ranger.plugin.util;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Outbound trusted-header auth for audit-server and other REST clients.
 *
 * <p>Properties are read under a caller-supplied prefix (audit destination example):
 * <pre>
 * xasecure.audit.destination.auditserver.authn.header.enabled=true
 * xasecure.audit.destination.auditserver.authn.header.spiffe=X-Spiffe-Id
 * </pre>
 * SPIFFE ID value is resolved via {@link SpiffeIdentityResolver} under the same
 * prefix (explicit value, identity file, or {@code SPIFFE_ID} environment variable).
 */
public final class PluginHeaderAuthConfig {
    public static final String RANGER_CONFIG_PREFIX       = "ranger.";
    public static final String PROP_HEADER_AUTH_ENABLED   = "authn.header.enabled";
    public static final String PROP_HEADER_SPIFFE         = "authn.header.spiffe";
    public static final String DEFAULT_SPIFFE_HEADER_NAME = "X-Spiffe-Id";

    private static final Logger LOG =
            LoggerFactory.getLogger(PluginHeaderAuthConfig.class);

    private PluginHeaderAuthConfig() {
        // to block instantiation
    }

    /**
     * Builds the {@code ranger.<serviceType>} config prefix for a service type.
     *
     * @param serviceType Ranger service type (e.g. {@code hive})
     * @return the config prefix, or {@code null} when {@code serviceType} is blank
     */
    public static String configPrefixForServiceType(final String serviceType) {
        if (StringUtils.isBlank(serviceType)) {
            return null;
        }

        return RANGER_CONFIG_PREFIX + serviceType.trim();
    }

    /**
     * Finds the first {@code ranger.<serviceType>.authn.header.enabled=true}
     * prefix in {@code props}.
     *
     * @param props plugin or site configuration properties
     * @return the matching config prefix, or {@code null} when none is enabled
     */
    public static String resolveEnabledConfigPrefix(final Properties props) {
        if (props == null || props.isEmpty()) {
            return null;
        }

        String suffix = "." + PROP_HEADER_AUTH_ENABLED;

        for (String key : props.stringPropertyNames()) {
            if (!key.startsWith(RANGER_CONFIG_PREFIX) || !key.endsWith(suffix)) {
                continue;
            }

            String prefix = key.substring(0, key.length() - suffix.length());

            if (isHeaderAuthEnabled(props, prefix)) {
                return prefix;
            }
        }

        return null;
    }

    /**
     * Returns whether trusted header auth is enabled for the given config prefix.
     *
     * @param props        plugin or site configuration properties
     * @param configPrefix prefix such as {@code ranger.hive}
     * @return {@code true} when header auth is enabled
     */
    public static boolean isHeaderAuthEnabled(final Properties props,
            final String configPrefix) {
        if (props == null || StringUtils.isBlank(configPrefix)) {
            return false;
        }

        return Boolean.parseBoolean(
                props.getProperty(configPrefix + "." + PROP_HEADER_AUTH_ENABLED,
                        "false"));
    }

    /**
     * Builds SPIFFE header(s) for outbound REST calls when header auth is enabled.
     *
     * @param props        plugin or site configuration properties
     * @param configPrefix prefix such as {@code xasecure.audit.destination.auditserver}
     * @return immutable header map; empty when auth is disabled or misconfigured
     */
    public static Map<String, String> buildSpiffeAuthHeaders(final Properties props,
            final String configPrefix) {
        if (!isHeaderAuthEnabled(props, configPrefix)) {
            return Collections.emptyMap();
        }

        List<String> headerNames = SpiffeIdUtil.parseHeaderNames(
                resolveSpiffeHeaderName(props, configPrefix));
        String spiffeId = SpiffeIdentityResolver.resolve(props, configPrefix);

        if (headerNames.isEmpty()) {
            LOG.warn("Plugin header auth enabled for {} but no SPIFFE header "
                    + "name is configured", configPrefix);
            return Collections.emptyMap();
        }

        if (StringUtils.isBlank(spiffeId)) {
            LOG.warn("Plugin header auth enabled for {} but no SPIFFE ID could "
                    + "be resolved", configPrefix);
            return Collections.emptyMap();
        }

        if (!SpiffeIdUtil.isValidSpiffeId(spiffeId)) {
            LOG.warn("Resolved SPIFFE ID for {} is not well-formed", configPrefix);
            return Collections.emptyMap();
        }

        Map<String, String> headers = new LinkedHashMap<>();

        for (String headerName : headerNames) {
            headers.put(headerName, spiffeId.trim());
        }

        return Collections.unmodifiableMap(headers);
    }

    private static String resolveSpiffeHeaderName(final Properties props,
            final String configPrefix) {
        String headerName = props != null
                ? StringUtils.trimToNull(
                        props.getProperty(configPrefix + "." + PROP_HEADER_SPIFFE))
                : null;

        return headerName != null ? headerName : DEFAULT_SPIFFE_HEADER_NAME;
    }
}
