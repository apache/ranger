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
 * <p>When {@code authn.header.enabled=true}, trusted HTTP headers are added to every
 * outbound REST request. Each header is configured as a property whose name is the
 * HTTP header name under {@code authn.header} (audit destination example):
 * <pre>
 * xasecure.audit.destination.auditserver.authn.header.enabled=true
 * xasecure.audit.destination.auditserver.authn.header.X-Spiffe-Id=file:/path/to/spiffe-id.file
 * # valid value specs:
 * #   file:/path/to/spiffe-id.file
 * #   env:SPIFFE_ID
 * #   spiffe://trust-domain/ns/.../sa/...  (literal string)
 * </pre>
 */
public final class PluginHeaderAuthConfig {
    private static final Logger LOG = LoggerFactory.getLogger(PluginHeaderAuthConfig.class);

    public static final String PROP_HEADER_PREFIX       = "authn.header.";
    public static final String PROP_HEADER_AUTH_ENABLED = PROP_HEADER_PREFIX + "enabled";

    private static final String VALUE_PREFIX_FILE = "file:";
    private static final String VALUE_PREFIX_ENV  = "env:";

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
    public static boolean isHeaderAuthEnabled(final Properties props, final String configPrefix) {
        return props != null && StringUtils.isNotBlank(configPrefix) && Boolean.parseBoolean(props.getProperty(configPrefix + "." + PROP_HEADER_AUTH_ENABLED, "false"));
    }

    /**
     * Builds trusted HTTP headers for outbound REST calls when header auth is enabled.
     *
     * @param props        plugin or site configuration properties
     * @param configPrefix prefix such as {@code xasecure.audit.destination.auditserver}
     * @return immutable header map; empty when auth is disabled or misconfigured
     */
    public static Map<String, String> buildTrustedAuthHeaders(final Properties props, final String configPrefix) {
        if (!isHeaderAuthEnabled(props, configPrefix)) {
            return Collections.emptyMap();
        }

        String              propertyPrefix = configPrefix + "." + PROP_HEADER_PREFIX;
        Map<String, String> headers        = new LinkedHashMap<>();

        for (String propertyName : sortedPropertyNames(props)) {
            if (!propertyName.startsWith(propertyPrefix)) {
                continue;
            }

            String headerName = propertyName.substring(propertyPrefix.length());

            if (StringUtils.isBlank(headerName) || "enabled".equals(headerName)) {
                continue;
            }

            String headerValue = resolveConfiguredValue(props.getProperty(propertyName));

            addConfiguredHeader(headers, configPrefix, headerName, headerValue);
        }

        if (headers.isEmpty()) {
            LOG.warn("Plugin header auth enabled for {} but no trusted headers could be resolved", configPrefix);
        }

        return Collections.unmodifiableMap(headers);
    }

    private static void addConfiguredHeader(final Map<String, String> headers, final String configPrefix, final String headerName, final String headerValue) {
        if (StringUtils.isBlank(headerValue)) {
            LOG.warn("Plugin header auth enabled for {} but trusted header {} has no resolvable value", configPrefix, headerName);
        } else if (headerValue.startsWith("spiffe://") && !SpiffeIdUtil.isValidSpiffeId(headerValue)) {
            LOG.warn("Resolved trusted header value for {} is not a well-formed SPIFFE ID", configPrefix);
        } else {
            headers.put(headerName, headerValue);
        }
    }

    private static String resolveConfiguredValue(final String valueSpec) {
        final String ret;

        if (StringUtils.isBlank(valueSpec)) {
            ret = null;
        } else if (valueSpec.startsWith(VALUE_PREFIX_FILE)) {
            ret = SpiffeIdentityResolver.readFirstLine(valueSpec.substring(VALUE_PREFIX_FILE.length()));
        } else if (valueSpec.startsWith(VALUE_PREFIX_ENV)) {
            ret = StringUtils.trimToNull(System.getenv(valueSpec.substring(VALUE_PREFIX_ENV.length())));
        } else {
            ret = StringUtils.trimToNull(valueSpec);
        }

        return ret;
    }

    private static List<String> sortedPropertyNames(final Properties props) {
        List<String> names = new ArrayList<>();

        if (props != null) {
            names.addAll(props.stringPropertyNames());
            Collections.sort(names);
        }

        return names;
    }
}
