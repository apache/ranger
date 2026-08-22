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

import org.apache.ranger.ugsyncutil.model.graph.MembershipMode;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;

public final class EntraIdGraphConfig {
    public enum AuthMode { CERTIFICATE, CLIENT_SECRET }

    private static final String OAUTH2_TOKEN_PATH_SUFFIX = "/oauth2/v2.0/token";
    private static final String DEFAULT_SCOPE_SUFFIX = "/.default";
    private final String authorityHost;
    private final String graphBaseUrl;
    private final String tenantId;
    private final String clientId;

    private final AuthMode authMode;
    private final String keystorePath;
    private final char[] keystorePassword;
    private final String certAlias;
    private final char[] clientSecret;

    private final MembershipMode membershipMode;
    private final Set<String> userSelectAttrs;
    private final Set<String> groupSelectAttrs;
    private final String groupFilter;
    private final int pageSize;

    private final int maxRetries;
    private final long retryBaseBackoffMs;
    private final int connectTimeoutMs;
    private final int readTimeoutMs;

    private EntraIdGraphConfig(Builder b) {
        this.authorityHost = stripTrailingSlashes(b.authorityHost);
        this.graphBaseUrl = stripTrailingSlashes(b.graphBaseUrl);
        this.tenantId = b.tenantId;
        this.clientId = b.clientId;
        this.authMode = b.authMode;
        this.keystorePath = b.keystorePath;
        this.keystorePassword = b.keystorePassword == null ? null : b.keystorePassword.clone();
        this.certAlias = b.certAlias;
        this.clientSecret = b.clientSecret == null ? null : b.clientSecret.clone();
        this.membershipMode = b.membershipMode;
        this.userSelectAttrs = unmodifiableCopy(b.userSelectAttrs);
        this.groupSelectAttrs = unmodifiableCopy(b.groupSelectAttrs);
        this.groupFilter = b.groupFilter;
        this.pageSize = b.pageSize;
        this.maxRetries = b.maxRetries;
        this.retryBaseBackoffMs = b.retryBaseBackoffMs;
        this.connectTimeoutMs = b.connectTimeoutMs;
        this.readTimeoutMs = b.readTimeoutMs;
    }

    private static Set<String> unmodifiableCopy(Set<String> in) {
        if (in == null || in.isEmpty()) {
            return Collections.emptySet();
        }
        return Collections.unmodifiableSet(new LinkedHashSet<>(in));
    }

    private static String stripTrailingSlashes(String s) {
        return s == null ? null : s.replaceAll("/+$", "");
    }

    public String getTokenEndpoint() {
        return authorityHost + "/" + tenantId + OAUTH2_TOKEN_PATH_SUFFIX;
    }

    public String getDefaultScope() {
        return graphBaseUrl + DEFAULT_SCOPE_SUFFIX;
    }

    public String getAuthorityHost() {
        return authorityHost;
    }

    public String getGraphBaseUrl() {
        return graphBaseUrl;
    }

    public String getTenantId() {
        return tenantId;
    }

    public String getClientId() {
        return clientId;
    }

    public AuthMode getAuthMode() {
        return authMode;
    }

    public String getKeystorePath() {
        return keystorePath;
    }

    public char[] getKeystorePassword() {
        return keystorePassword == null ? null : keystorePassword.clone();
    }

    public String getCertAlias() {
        return certAlias;
    }

    public char[] getClientSecret() {
        return clientSecret == null ? null : clientSecret.clone();
    }

    public MembershipMode getMembershipMode() {
        return membershipMode;
    }

    public Set<String> getUserSelectAttrs() {
        return userSelectAttrs;
    }

    public Set<String> getGroupSelectAttrs() {
        return groupSelectAttrs;
    }

    public String getGroupFilter() {
        return groupFilter;
    }

    public int getPageSize() {
        return pageSize;
    }

    public int getMaxRetries() {
        return maxRetries;
    }

    public long getRetryBaseBackoffMs() {
        return retryBaseBackoffMs;
    }

    public int getConnectTimeoutMs() {
        return connectTimeoutMs;
    }

    public int getReadTimeoutMs() {
        return readTimeoutMs;
    }

    @Override
    public String toString() {
        return "EntraIdGraphConfig{authorityHost=" + authorityHost
                + ", graphBaseUrl=" + graphBaseUrl
                + ", tenantId=" + tenantId
                + ", clientId=" + clientId
                + ", authMode=" + authMode
                + ", membershipMode=" + membershipMode
                + ", userSelectAttrs=" + userSelectAttrs
                + ", groupSelectAttrs=" + groupSelectAttrs
                + ", groupFilter=" + (groupFilter == null ? "none" : groupFilter)
                + ", pageSize=" + pageSize
                + ", maxRetries=" + maxRetries
                + ", retryBaseBackoffMs=" + retryBaseBackoffMs
                + ", connectTimeoutMs=" + connectTimeoutMs
                + ", readTimeoutMs=" + readTimeoutMs
                + '}';
    }

    public static final class Builder {
        // endpoints — defaults = global Azure commercial cloud
        private String authorityHost = "https://login.microsoftonline.com";
        private String graphBaseUrl = "https://graph.microsoft.com";

        private String tenantId;
        private String clientId;

        private AuthMode authMode;
        private String keystorePath;
        private char[] keystorePassword;
        private String certAlias;
        private char[] clientSecret;

        private MembershipMode membershipMode = MembershipMode.DIRECT;
        private Set<String> userSelectAttrs;
        private Set<String> groupSelectAttrs;
        private String groupFilter;
        private int pageSize = 999;

        private int maxRetries = 5;
        private long retryBaseBackoffMs = 1000L;
        private int connectTimeoutMs = 30_000;
        private int readTimeoutMs = 60_000;

        public Builder authorityHost(String v) {
            this.authorityHost = v;
            return this;
        }

        public Builder graphBaseUrl(String v) {
            this.graphBaseUrl = v;
            return this;
        }

        public Builder tenantId(String v) {
            this.tenantId = v;
            return this;
        }

        public Builder clientId(String v) {
            this.clientId = v;
            return this;
        }

        public Builder authMode(AuthMode v) {
            this.authMode = v;
            return this;
        }

        public Builder keystorePath(String v) {
            this.keystorePath = v;
            return this;
        }

        public Builder keystorePassword(char[] v) {
            this.keystorePassword = v == null ? null : v.clone();
            return this;
        }

        public Builder certAlias(String v) {
            this.certAlias = v;
            return this;
        }

        public Builder clientSecret(char[] v) {
            this.clientSecret = v == null ? null : v.clone();
            return this;
        }

        public Builder membershipMode(MembershipMode v) {
            this.membershipMode = v == null ? MembershipMode.DIRECT : v;
            return this;
        }

        public Builder userSelectAttrs(Set<String> v) {
            this.userSelectAttrs = v;
            return this;
        }

        public Builder groupSelectAttrs(Set<String> v) {
            this.groupSelectAttrs = v;
            return this;
        }

        public Builder groupFilter(String v) {
            this.groupFilter = v;
            return this;
        }

        public Builder pageSize(int v) {
            this.pageSize = v;
            return this;
        }

        public Builder maxRetries(int v) {
            this.maxRetries = v;
            return this;
        }

        public Builder retryBaseBackoffMs(long v) {
            this.retryBaseBackoffMs = v;
            return this;
        }

        public Builder connectTimeoutMs(int v) {
            this.connectTimeoutMs = v;
            return this;
        }

        public Builder readTimeoutMs(int v) {
            this.readTimeoutMs = v;
            return this;
        }

        public EntraIdGraphConfig build() {
            requireText(tenantId, "tenantId");
            requireText(clientId, "clientId");
            requireText(authorityHost, "authorityHost");
            requireText(graphBaseUrl, "graphBaseUrl");

            if (authMode == null) {
                throw new IllegalStateException("authMode must be set (CERTIFICATE or CLIENT_SECRET)");
            }

            if (authMode == AuthMode.CERTIFICATE) {
                requireText(keystorePath, "keystorePath (required for CERTIFICATE auth)");
                requireText(certAlias, "certAlias (required for CERTIFICATE auth)");
                if (clientSecret != null) {
                    throw new IllegalStateException("clientSecret must not be set when authMode=CERTIFICATE");
                }
            } else { // CLIENT_SECRET
                if (clientSecret == null || clientSecret.length == 0) {
                    throw new IllegalStateException("clientSecret is required for CLIENT_SECRET auth");
                }
                if (keystorePath != null) {
                    throw new IllegalStateException("keystorePath must not be set when authMode=CLIENT_SECRET");
                }
            }

            if (pageSize < 1) {
                throw new IllegalStateException("pageSize must be >= 1");
            }
            if (maxRetries < 0) {
                throw new IllegalStateException("maxRetries must be >= 0");
            }
            if (retryBaseBackoffMs < 0) {
                throw new IllegalStateException("retryBaseBackoffMs must be >= 0");
            }
            if (connectTimeoutMs < 0 || readTimeoutMs < 0) {
                throw new IllegalStateException("timeouts must be >= 0");
            }

            return new EntraIdGraphConfig(this);
        }

        private static void requireText(String v, String name) {
            if (v == null || v.trim().isEmpty()) {
                throw new IllegalStateException(name + " must not be blank");
            }
        }
    }
}
