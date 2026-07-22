/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ranger.entraid.graph;

import org.apache.commons.lang3.StringUtils;
import org.apache.ranger.credentialapi.CredentialReader;
import org.apache.ranger.entraid.graph.EntraIdGraphConfig.AuthMode;
import org.apache.ranger.ugsyncutil.model.graph.MembershipMode;
import org.apache.ranger.unixusersync.config.UserGroupSyncConfig;

import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.function.Consumer;
import java.util.function.IntConsumer;
import java.util.function.LongConsumer;

public final class EntraIdGraphConfigLoader {
    static final String ENTRAID_TENANT_ID = "ranger.usersync.entraid.tenant.id";
    static final String ENTRAID_CLIENT_ID = "ranger.usersync.entraid.client.id";
    static final String ENTRAID_AUTH_TYPE = "ranger.usersync.entraid.auth.type";
    static final String ENTRAID_AUTHORITY_HOST = "ranger.usersync.entraid.authority.host";
    static final String ENTRAID_GRAPH_BASE_URL = "ranger.usersync.entraid.graph.base.url";

    static final String ENTRAID_KEYSTORE_FILE = "ranger.usersync.entraid.keystore.file";
    static final String ENTRAID_KEYSTORE_PASSWORD = "ranger.usersync.entraid.keystore.password";
    static final String ENTRAID_KEYSTORE_ALIAS = "ranger.usersync.entraid.keystore.credential.alias";
    static final String ENTRAID_CERT_ALIAS = "ranger.usersync.entraid.cert.alias";

    static final String ENTRAID_CLIENT_SECRET = "ranger.usersync.entraid.client.secret";
    static final String ENTRAID_CLIENT_SECRET_ALIAS = "ranger.usersync.entraid.client.secret.credential.alias";

    static final String ENTRAID_CREDSTORE_FILE = "ranger.usersync.credstore.filename";
    static final String RANGER_KEYSTORE_TYPE = "ranger.keystore.file.type";

    static final String ENTRAID_MEMBERSHIP_MODE = "ranger.usersync.entraid.membership.mode";
    static final String ENTRAID_USER_SELECT_ATTRS = "ranger.usersync.entraid.user.select.attributes";
    static final String ENTRAID_GROUP_SELECT_ATTRS = "ranger.usersync.entraid.group.select.attributes";
    static final String ENTRAID_GROUP_FILTER = "ranger.usersync.entraid.group.filter";
    static final String ENTRAID_PAGE_SIZE = "ranger.usersync.entraid.page.size";

    static final String ENTRAID_MAX_RETRIES = "ranger.usersync.entraid.max.retries";
    static final String ENTRAID_RETRY_BASE_BACKOFF = "ranger.usersync.entraid.retry.base.backoff.ms";
    static final String ENTRAID_CONNECT_TIMEOUT_MS = "ranger.usersync.entraid.connect.timeout.ms";
    static final String ENTRAID_READ_TIMEOUT_MS = "ranger.usersync.entraid.read.timeout.ms";

    private final UserGroupSyncConfig config;

    public EntraIdGraphConfigLoader(UserGroupSyncConfig config) {
        if (config == null) {
            throw new IllegalArgumentException("UserGroupSyncConfig must not be null");
        }
        this.config = config;
    }

    public EntraIdGraphConfig load() throws GraphClientException {
        EntraIdGraphConfig.Builder builder = new EntraIdGraphConfig.Builder()
                .tenantId(trimmed(ENTRAID_TENANT_ID))
                .clientId(trimmed(ENTRAID_CLIENT_ID))
                .membershipMode(parseMembershipMode(trimmed(ENTRAID_MEMBERSHIP_MODE)))
                .userSelectAttrs(parseCsv(getProperty(ENTRAID_USER_SELECT_ATTRS)))
                .groupSelectAttrs(parseCsv(getProperty(ENTRAID_GROUP_SELECT_ATTRS)))
                .groupFilter(trimmed(ENTRAID_GROUP_FILTER));

        applyIfPresent(ENTRAID_AUTHORITY_HOST, builder::authorityHost);
        applyIfPresent(ENTRAID_GRAPH_BASE_URL, builder::graphBaseUrl);
        applyIntIfPresent(ENTRAID_PAGE_SIZE, builder::pageSize);
        applyIntIfPresent(ENTRAID_MAX_RETRIES, builder::maxRetries);
        applyLongIfPresent(ENTRAID_RETRY_BASE_BACKOFF, builder::retryBaseBackoffMs);
        applyIntIfPresent(ENTRAID_CONNECT_TIMEOUT_MS, builder::connectTimeoutMs);
        applyIntIfPresent(ENTRAID_READ_TIMEOUT_MS, builder::readTimeoutMs);
        AuthMode authMode = parseAuthMode(trimmed(ENTRAID_AUTH_TYPE));
        builder.authMode(authMode);
        if (authMode == AuthMode.CERTIFICATE) {
            builder.keystorePath(trimmed(ENTRAID_KEYSTORE_FILE)).certAlias(trimmed(ENTRAID_CERT_ALIAS));
            char[] keystorePassword = resolveSecret(ENTRAID_KEYSTORE_PASSWORD, ENTRAID_KEYSTORE_ALIAS);
            if (keystorePassword != null) {
                builder.keystorePassword(keystorePassword);
                Arrays.fill(keystorePassword, '\0');
            }
        } else { // CLIENT_SECRET
            char[] clientSecret = resolveSecret(ENTRAID_CLIENT_SECRET, ENTRAID_CLIENT_SECRET_ALIAS);
            if (clientSecret != null) {
                builder.clientSecret(clientSecret);
                Arrays.fill(clientSecret, '\0');
            }
        }
        try {
            return builder.build();
        } catch (RuntimeException e) {
            throw new GraphClientException("Invalid EntraID UserSync configuration: " + e.getMessage(), e);
        }
    }

    private char[] resolveSecret(String directProp, String aliasProp) {
        String direct = getProperty(directProp);
        if (StringUtils.isNotBlank(direct)) {
            return direct.trim().toCharArray();
        }
        String credStorePath = getProperty(ENTRAID_CREDSTORE_FILE);
        String alias = getProperty(aliasProp);
        if (StringUtils.isBlank(credStorePath) || StringUtils.isBlank(alias)) {
            return null;
        }
        String storeType = config.getProperty(RANGER_KEYSTORE_TYPE);
        String decrypted = CredentialReader.getDecryptedString(credStorePath.trim(), alias.trim(), storeType);
        if (StringUtils.isBlank(decrypted) || "none".equalsIgnoreCase(decrypted.trim())) {
            return null;
        }
        return decrypted.toCharArray();
    }

    private AuthMode parseAuthMode(String value) throws GraphClientException {
        if (StringUtils.isBlank(value)) {
            return AuthMode.CERTIFICATE;
        }
        try {
            return AuthMode.valueOf(value.trim().toUpperCase());
        } catch (IllegalArgumentException e) {
            throw new GraphClientException(ENTRAID_AUTH_TYPE + " must be CERTIFICATE or CLIENT_SECRET, got: " + value);
        }
    }

    private MembershipMode parseMembershipMode(String value) throws GraphClientException {
        if (StringUtils.isBlank(value)) {
            return MembershipMode.DIRECT;
        }
        try {
            return MembershipMode.valueOf(value.trim().toUpperCase());
        } catch (IllegalArgumentException e) {
            throw new GraphClientException(ENTRAID_MEMBERSHIP_MODE + " must be DIRECT or TRANSITIVE, got: " + value);
        }
    }

    private static Set<String> parseCsv(String value) {
        Set<String> result = new LinkedHashSet<>();
        if (StringUtils.isNotBlank(value)) {
            StringTokenizer st = new StringTokenizer(value, ",");
            while (st.hasMoreTokens()) {
                String token = st.nextToken().trim();
                if (!token.isEmpty()) {
                    result.add(token);
                }
            }
        }
        return result;
    }

    private String getProperty(String name) {
        return config.getProperty(name);
    }

    private String trimmed(String name) {
        String value = config.getProperty(name);
        return value == null ? null : value.trim();
    }

    private void applyIfPresent(String name, Consumer<String> setter) {
        String value = trimmed(name);
        if (StringUtils.isNotBlank(value)) {
            setter.accept(value);
        }
    }

    private void applyIntIfPresent(String name, IntConsumer setter) throws GraphClientException {
        String value = trimmed(name);
        if (StringUtils.isNotBlank(value)) {
            try {
                setter.accept(Integer.parseInt(value));
            } catch (NumberFormatException e) {
                throw new GraphClientException(name + " must be an integer, got: " + value);
            }
        }
    }

    private void applyLongIfPresent(String name, LongConsumer setter) throws GraphClientException {
        String value = trimmed(name);
        if (StringUtils.isNotBlank(value)) {
            try {
                setter.accept(Long.parseLong(value));
            } catch (NumberFormatException e) {
                throw new GraphClientException(EntraIdGraphConfigLoader.ENTRAID_RETRY_BASE_BACKOFF + " must be a long, got: " + value);
            }
        }
    }
}
