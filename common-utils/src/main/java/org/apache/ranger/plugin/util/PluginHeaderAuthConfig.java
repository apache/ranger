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

package org.apache.ranger.plugin.util;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Outbound trusted-header auth for audit-server and other REST clients.
 *
 * <p>Legacy SPIFFE configuration (audit destination example):
 * <pre>
 * xasecure.audit.destination.auditserver.authn.header.enabled=true
 * xasecure.audit.destination.auditserver.authn.header.spiffe=X-Spiffe-Id
 * xasecure.audit.destination.auditserver.authn.spiffe.value=spiffe://...
 * </pre>
 *
 * <p>Generic slot-based configuration:
 * <pre>
 * authn.header.enabled=true
 * authn.header.headers=spiffe,value
 * authn.header.spiffe=X-Spiffe-Id
 * authn.header.value=file:/path/to/spiffe-id.file
 * </pre>
 * Value specs support {@code file:}, {@code env:}, or a literal string. Multiple
 * headers can be configured with {@code authn.header.{slot}} for the HTTP header
 * name and {@code authn.header.{slot}.value} for the value spec.
 */
public final class PluginHeaderAuthConfig {
    public static final String PROP_HEADER_AUTH_ENABLED   = "authn.header.enabled";
    public static final String PROP_HEADER_HEADERS        = "authn.header.headers";
    public static final String PROP_HEADER_SPIFFE         = "authn.header.spiffe";
    public static final String DEFAULT_SPIFFE_HEADER_NAME = "X-Spiffe-Id";

    private static final String SLOT_SPIFFE     = "spiffe";
    private static final String SLOT_VALUE      = "value";
    private static final String VALUE_PREFIX_FILE = "file:";
    private static final String VALUE_PREFIX_ENV  = "env:";

    private static final Logger LOG =
            LoggerFactory.getLogger(PluginHeaderAuthConfig.class);

    private PluginHeaderAuthConfig() {
        // to block instantiation
    }

    /**
     * Returns whether trusted header auth is enabled for the given config prefix.
     *
     * @param props        plugin or site configuration properties
     * @param configPrefix property prefix for header-auth settings
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
     * Builds trusted HTTP headers for outbound REST calls when header auth is enabled.
     *
     * @param props        plugin or site configuration properties
     * @param configPrefix prefix such as {@code xasecure.audit.destination.auditserver}
     * @return immutable header map; empty when auth is disabled or misconfigured
     */
    public static Map<String, String> buildTrustedAuthHeaders(final Properties props,
            final String configPrefix) {
        if (!isHeaderAuthEnabled(props, configPrefix)) {
            return Collections.emptyMap();
        }

        String headersConfig = getProperty(props, configPrefix, PROP_HEADER_HEADERS);

        if (StringUtils.isBlank(headersConfig)) {
            return buildLegacyTrustedHeaders(props, configPrefix);
        }

        return buildConfiguredTrustedHeaders(props, configPrefix, headersConfig);
    }

    private static Map<String, String> buildLegacyTrustedHeaders(final Properties props,
            final String configPrefix) {
        List<String> headerNames = SpiffeIdUtil.parseHeaderNames(
                resolveSpiffeHeaderName(props, configPrefix));
        String headerValue = SpiffeIdentityResolver.resolve(props, configPrefix);

        return buildSingleTrustedHeader(configPrefix, headerNames, headerValue);
    }

    private static Map<String, String> buildConfiguredTrustedHeaders(final Properties props,
            final String configPrefix, final String headersConfig) {
        List<String> slots = parseHeaderSlots(headersConfig);

        if (slots.isEmpty()) {
            LOG.warn("Plugin header auth enabled for {} but {} is empty",
                    configPrefix, PROP_HEADER_HEADERS);
            return Collections.emptyMap();
        }

        Map<String, String> headers = new LinkedHashMap<>();

        if (slots.contains(SLOT_SPIFFE) && slots.contains(SLOT_VALUE)) {
            addConfiguredHeader(headers, configPrefix,
                    getSlotConfig(props, configPrefix, SLOT_SPIFFE),
                    resolveConfiguredValue(getSlotConfig(props, configPrefix, SLOT_VALUE),
                            props, configPrefix));
        } else {
            for (String slot : slots) {
                if (SLOT_VALUE.equals(slot)) {
                    continue;
                }

                String headerName = getSlotConfig(props, configPrefix, slot);
                String valueSpec  = getSlotConfig(props, configPrefix, slot + ".value");

                if (valueSpec == null) {
                    valueSpec = getSlotConfig(props, configPrefix, SLOT_VALUE);
                }

                addConfiguredHeader(headers, configPrefix, headerName,
                        resolveConfiguredValue(valueSpec, props, configPrefix));
            }
        }

        return Collections.unmodifiableMap(headers);
    }

    private static Map<String, String> buildSingleTrustedHeader(final String configPrefix,
            final List<String> headerNames, final String headerValue) {
        if (headerNames.isEmpty()) {
            LOG.warn("Plugin header auth enabled for {} but no trusted header "
                    + "name is configured", configPrefix);
            return Collections.emptyMap();
        }

        if (StringUtils.isBlank(headerValue)) {
            LOG.warn("Plugin header auth enabled for {} but no trusted header "
                    + "value could be resolved", configPrefix);
            return Collections.emptyMap();
        }

        if (!SpiffeIdUtil.isValidSpiffeId(headerValue)) {
            LOG.warn("Resolved trusted header value for {} is not a well-formed "
                    + "SPIFFE ID", configPrefix);
            return Collections.emptyMap();
        }

        Map<String, String> headers = new LinkedHashMap<>();

        for (String headerName : headerNames) {
            headers.put(headerName, headerValue);
        }

        return Collections.unmodifiableMap(headers);
    }

    private static void addConfiguredHeader(final Map<String, String> headers,
            final String configPrefix, final String headerName,
            final String headerValue) {
        if (StringUtils.isBlank(headerName)) {
            LOG.warn("Plugin header auth enabled for {} but a trusted header "
                    + "name slot is not configured", configPrefix);
            return;
        }

        if (StringUtils.isBlank(headerValue)) {
            LOG.warn("Plugin header auth enabled for {} but trusted header {} "
                    + "has no resolvable value", configPrefix, headerName);
            return;
        }

        if (SpiffeIdUtil.isValidSpiffeId(headerValue)
                || headerValue.startsWith("spiffe://")) {
            if (!SpiffeIdUtil.isValidSpiffeId(headerValue)) {
                LOG.warn("Resolved trusted header value for {} is not a "
                        + "well-formed SPIFFE ID", configPrefix);
                return;
            }
        }

        headers.put(headerName, headerValue);
    }

    private static String resolveConfiguredValue(final String valueSpec,
            final Properties props, final String configPrefix) {
        String ret = null;

        if (StringUtils.isBlank(valueSpec)) {
            ret = SpiffeIdentityResolver.resolve(props, configPrefix);
        } else if (valueSpec.startsWith(VALUE_PREFIX_FILE)) {
            ret = SpiffeIdentityResolver.readFirstLine(
                    valueSpec.substring(VALUE_PREFIX_FILE.length()));
        } else if (valueSpec.startsWith(VALUE_PREFIX_ENV)) {
            ret = StringUtils.trimToNull(
                    System.getenv(valueSpec.substring(VALUE_PREFIX_ENV.length())));
        } else {
            ret = StringUtils.trimToNull(valueSpec);
        }

        return ret;
    }

    private static List<String> parseHeaderSlots(final String headersConfig) {
        List<String> ret = new ArrayList<>();

        for (String slot : headersConfig.split(",")) {
            String trimmed = StringUtils.trimToNull(slot);

            if (trimmed != null) {
                ret.add(trimmed);
            }
        }

        return ret;
    }

    private static String getSlotConfig(final Properties props,
            final String configPrefix, final String slot) {
        return getProperty(props, configPrefix, "authn.header." + slot);
    }

    private static String getProperty(final Properties props,
            final String configPrefix, final String propertySuffix) {
        if (props == null || StringUtils.isBlank(configPrefix)) {
            return null;
        }

        return StringUtils.trimToNull(
                props.getProperty(configPrefix + "." + propertySuffix));
    }

    private static String resolveSpiffeHeaderName(final Properties props,
            final String configPrefix) {
        String headerName = getProperty(props, configPrefix, PROP_HEADER_SPIFFE);

        return headerName != null ? headerName : DEFAULT_SPIFFE_HEADER_NAME;
    }
}
