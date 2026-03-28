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

package org.apache.ranger.pdp.config;

public final class RangerPdpConstants {
    private RangerPdpConstants() {
        // no instances
    }

    // Servlet context attributes
    public static final String SERVLET_CTX_ATTR_CONFIG        = "ranger.pdp.config";
    public static final String SERVLET_CTX_ATTR_AUTHORIZER    = "ranger.pdp.authorizer";
    public static final String SERVLET_CTX_ATTR_RUNTIME_STATE = "ranger.pdp.runtime.state";

    // Request attributes set by auth filter
    public static final String ATTR_AUTHENTICATED_USER = "ranger.pdp.authenticated.user";
    public static final String ATTR_AUTH_TYPE          = "ranger.pdp.auth.type";

    // Server
    public static final String PROP_CONF_DIR = "ranger.pdp.conf.dir";
    public static final String PROP_PORT    = "ranger.pdp.port";
    public static final String PROP_LOG_DIR = "ranger.pdp.log.dir";

    // SSL/TLS
    public static final String PROP_SSL_ENABLED             = "ranger.pdp.ssl.enabled";
    public static final String PROP_SSL_KEYSTORE_FILE       = "ranger.pdp.ssl.keystore.file";
    public static final String PROP_SSL_KEYSTORE_PASSWORD   = "ranger.pdp.ssl.keystore.password";
    public static final String PROP_SSL_KEYSTORE_TYPE       = "ranger.pdp.ssl.keystore.type";
    public static final String PROP_SSL_TRUSTSTORE_ENABLED  = "ranger.pdp.ssl.truststore.enabled";
    public static final String PROP_SSL_TRUSTSTORE_FILE     = "ranger.pdp.ssl.truststore.file";
    public static final String PROP_SSL_TRUSTSTORE_PASSWORD = "ranger.pdp.ssl.truststore.password";
    public static final String PROP_SSL_TRUSTSTORE_TYPE     = "ranger.pdp.ssl.truststore.type";

    // HTTP/2
    public static final String PROP_HTTP2_ENABLED = "ranger.pdp.http2.enabled";

    // HTTP connector limits
    public static final String PROP_HTTP_CONNECTOR_MAX_THREADS       = "ranger.pdp.http.connector.maxThreads";
    public static final String PROP_HTTP_CONNECTOR_MIN_SPARE_THREADS = "ranger.pdp.http.connector.minSpareThreads";
    public static final String PROP_HTTP_CONNECTOR_ACCEPT_COUNT      = "ranger.pdp.http.connector.acceptCount";
    public static final String PROP_HTTP_CONNECTOR_MAX_CONNECTIONS   = "ranger.pdp.http.connector.maxConnections";

    // Authentication types
    public static final String PROP_AUTH_TYPES = "ranger.pdp.auth.types";

    // HTTP header auth
    public static final String PROP_AUTHN_HEADER_ENABLED  = "ranger.pdp.authn.header.enabled";
    public static final String PROP_AUTHN_HEADER_USERNAME = "ranger.pdp.authn.header.username";

    // JWT auth
    public static final String PROP_AUTHN_JWT_PROVIDER_URL = "ranger.pdp.authn.jwt.provider.url";
    public static final String PROP_AUTHN_JWT_PUBLIC_KEY   = "ranger.pdp.authn.jwt.public.key";
    public static final String PROP_AUTHN_JWT_COOKIE_NAME  = "ranger.pdp.authn.jwt.cookie.name";
    public static final String PROP_AUTHN_JWT_AUDIENCES    = "ranger.pdp.authn.jwt.audiences";

    // Kerberos/SPNEGO auth
    public static final String PROP_AUTHN_KERBEROS_SPNEGO_PRINCIPAL   = "ranger.pdp.authn.kerberos.spnego.principal";
    public static final String PROP_AUTHN_KERBEROS_SPNEGO_KEYTAB      = "ranger.pdp.authn.kerberos.spnego.keytab";
    public static final String PROP_AUTHN_KERBEROS_KRB_TOKEN_VALIDITY = "ranger.pdp.authn.kerberos.token.valid.seconds";
    public static final String PROP_AUTHN_KERBEROS_KRB_COOKIE_DOMAIN  = "ranger.pdp.authn.kerberos.cookie.domain";
    public static final String PROP_AUTHN_KERBEROS_KRB_COOKIE_PATH    = "ranger.pdp.authn.kerberos.cookie.path";
    public static final String PROP_KRB_NAME_RULES                    = "hadoop.security.auth_to_local";

    // Authorizer/audit properties referenced by PDP code
    public static final String PROP_AUTHZ_POLICY_CACHE_DIR = "ranger.authz.default.policy.cache.dir";
    public static final String PROP_AUTHZ_PREFIX           = "ranger.authz.";
    public static final String PROP_PDP_PREFIX             = "ranger.pdp.";
    public static final String PROP_PDP_SERVICE_PREFIX     = PROP_PDP_PREFIX + "service.";
    public static final String PROP_SPNEGO_PREFIX          = "ranger.spnego.";
    public static final String PROP_HADOOP_SECURITY_PREFIX = "hadoop.security.";

    // delegation users
    public static final String PROP_SUFFIX_DELEGATION_USERS = ".delegation.users";
    public static final String WILDCARD_SERVICE_NAME        = "*";
}
