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

package org.apache.ranger.authz.remote;

import org.apache.commons.lang3.StringUtils;
import org.apache.ranger.authz.api.RangerAuthzException;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

import static org.apache.ranger.authz.remote.RangerRemoteAuthzErrorCode.INVALID_PROPERTY_VALUE;
import static org.apache.ranger.authz.remote.RangerRemoteAuthzErrorCode.MISSING_AUTH_CONFIG;
import static org.apache.ranger.authz.remote.RangerRemoteAuthzErrorCode.MISSING_PDP_URL;
import static org.apache.ranger.authz.remote.RangerRemoteAuthzErrorCode.UNSUPPORTED_AUTH_TYPE;

public class RangerRemoteAuthzConfig {
    public static final String PROP_REMOTE_URL                               = "ranger.authz.remote.pdp.url";
    public static final String PROP_REMOTE_CONNECT_TIMEOUT_MS                = "ranger.authz.remote.pdp.connect.timeout.ms";
    public static final String PROP_REMOTE_READ_TIMEOUT_MS                   = "ranger.authz.remote.pdp.read.timeout.ms";
    public static final String PROP_REMOTE_HEADER_PREFIX                     = "ranger.authz.remote.header.";
    public static final String PROP_REMOTE_SSL_KEYSTORE_FILE                 = "ranger.authz.remote.ssl.keystore.file";
    public static final String PROP_REMOTE_SSL_KEYSTORE_PASSWORD             = "ranger.authz.remote.ssl.keystore.password";
    public static final String PROP_REMOTE_SSL_KEYSTORE_TYPE                 = "ranger.authz.remote.ssl.keystore.type";
    public static final String PROP_REMOTE_SSL_TRUSTSTORE_FILE               = "ranger.authz.remote.ssl.truststore.file";
    public static final String PROP_REMOTE_SSL_TRUSTSTORE_PASSWORD           = "ranger.authz.remote.ssl.truststore.password";
    public static final String PROP_REMOTE_SSL_TRUSTSTORE_TYPE               = "ranger.authz.remote.ssl.truststore.type";
    public static final String PROP_REMOTE_SSL_DISABLE_HOSTNAME_VERIFICATION = "ranger.authz.remote.ssl.disable.hostname.verification";
    public static final String PROP_REMOTE_AUTH_TYPE                         = "ranger.authz.remote.auth.type";
    public static final String PROP_REMOTE_AUTH_KERBEROS_PRINCIPAL           = "ranger.authz.remote.auth.kerberos.principal";
    public static final String PROP_REMOTE_AUTH_KERBEROS_KEYTAB              = "ranger.authz.remote.auth.kerberos.keytab";
    public static final String PROP_REMOTE_AUTH_KERBEROS_DEBUG               = "ranger.authz.remote.auth.kerberos.debug";

    public static final String PROP_PREFIX_SERVICE                           = "ranger.authz.service.";
    public static final String PROP_PREFIX_SERVICE_TYPE                      = "ranger.authz.servicetype.";

    private static final String AUTHZ_PATH_PREFIX = "/authz/v1";

    private static final int DEFAULT_CONNECT_TIMEOUT_MS = 5_000;
    private static final int DEFAULT_READ_TIMEOUT_MS    = 30_000;
    private static final String DEFAULT_STORE_TYPE      = "PKCS12";

    private final Properties properties;
    private final Map<String, String> headers;

    public RangerRemoteAuthzConfig(Properties properties) {
        this.properties = properties != null ? properties : new Properties();
        Map<String, String> ret = new LinkedHashMap<>();

        for (String propName : this.properties.stringPropertyNames()) {
            if (!propName.startsWith(PROP_REMOTE_HEADER_PREFIX)) {
                continue;
            }

            String headerName  = propName.substring(PROP_REMOTE_HEADER_PREFIX.length());
            String headerValue = this.properties.getProperty(propName);

            if (StringUtils.isNotBlank(headerName) && headerValue != null) {
                ret.put(headerName, headerValue);
            }
        }

        this.headers = Collections.unmodifiableMap(ret);
    }

    public String getPdpUrl() throws RangerAuthzException {
        String value = normalizeBaseUrl(properties.getProperty(PROP_REMOTE_URL));

        if (StringUtils.isBlank(value)) {
            throw new RangerAuthzException(MISSING_PDP_URL, PROP_REMOTE_URL);
        }

        return value;
    }

    public String getEndpointUrl(String path) throws RangerAuthzException {
        String baseUrl = getPdpUrl();

        if (baseUrl.endsWith(AUTHZ_PATH_PREFIX)) {
            return baseUrl + path;
        }

        return baseUrl + AUTHZ_PATH_PREFIX + path;
    }

    public int getConnectTimeoutMs() throws RangerAuthzException {
        return getIntProperty(PROP_REMOTE_CONNECT_TIMEOUT_MS, DEFAULT_CONNECT_TIMEOUT_MS);
    }

    public int getReadTimeoutMs() throws RangerAuthzException {
        return getIntProperty(PROP_REMOTE_READ_TIMEOUT_MS, DEFAULT_READ_TIMEOUT_MS);
    }

    public RangerRemoteAuthType getAuthType() throws RangerAuthzException {
        String value = StringUtils.defaultIfBlank(properties.getProperty(PROP_REMOTE_AUTH_TYPE), RangerRemoteAuthType.NONE.name());

        try {
            return RangerRemoteAuthType.valueOf(value.trim().toUpperCase());
        } catch (IllegalArgumentException e) {
            throw new RangerAuthzException(UNSUPPORTED_AUTH_TYPE, e, value);
        }
    }

    public String getKerberosPrincipal() throws RangerAuthzException {
        String ret = trimToNull(properties.getProperty(PROP_REMOTE_AUTH_KERBEROS_PRINCIPAL));

        if (ret == null) {
            throw new RangerAuthzException(MISSING_AUTH_CONFIG, PROP_REMOTE_AUTH_KERBEROS_PRINCIPAL);
        }

        return ret;
    }

    public String getKerberosKeytab() throws RangerAuthzException {
        String ret = trimToNull(properties.getProperty(PROP_REMOTE_AUTH_KERBEROS_KEYTAB));

        if (ret == null) {
            throw new RangerAuthzException(MISSING_AUTH_CONFIG, PROP_REMOTE_AUTH_KERBEROS_KEYTAB);
        }

        return ret;
    }

    public boolean isKerberosDebugEnabled() throws RangerAuthzException {
        return getBooleanProperty(PROP_REMOTE_AUTH_KERBEROS_DEBUG, false);
    }

    public String getSslKeyStoreFile() {
        return trimToNull(properties.getProperty(PROP_REMOTE_SSL_KEYSTORE_FILE));
    }

    public String getSslKeyStorePassword() {
        return properties.getProperty(PROP_REMOTE_SSL_KEYSTORE_PASSWORD);
    }

    public String getSslKeyStoreType() {
        return StringUtils.defaultIfBlank(properties.getProperty(PROP_REMOTE_SSL_KEYSTORE_TYPE), DEFAULT_STORE_TYPE).trim();
    }

    public String getSslTrustStoreFile() {
        return trimToNull(properties.getProperty(PROP_REMOTE_SSL_TRUSTSTORE_FILE));
    }

    public String getSslTrustStorePassword() {
        return properties.getProperty(PROP_REMOTE_SSL_TRUSTSTORE_PASSWORD);
    }

    public String getSslTrustStoreType() {
        return StringUtils.defaultIfBlank(properties.getProperty(PROP_REMOTE_SSL_TRUSTSTORE_TYPE), DEFAULT_STORE_TYPE).trim();
    }

    public boolean isHostnameVerificationDisabled() throws RangerAuthzException {
        return getBooleanProperty(PROP_REMOTE_SSL_DISABLE_HOSTNAME_VERIFICATION, false);
    }

    public Map<String, String> getHeaders() {
        return headers;
    }

    public String getServiceTypeForService(String serviceName) {
        return properties.getProperty(PROP_PREFIX_SERVICE + serviceName + ".servicetype");
    }

    public String getDefaultServiceNameForServiceType(String serviceType) {
        return properties.getProperty(PROP_PREFIX_SERVICE_TYPE + serviceType + ".default.service");
    }

    private boolean getBooleanProperty(String propertyName, boolean defaultValue) throws RangerAuthzException {
        String propertyValue = properties.getProperty(propertyName);

        if (StringUtils.isBlank(propertyValue)) {
            return defaultValue;
        }

        if ("true".equalsIgnoreCase(propertyValue.trim())) {
            return true;
        }

        if ("false".equalsIgnoreCase(propertyValue.trim())) {
            return false;
        }

        throw new RangerAuthzException(INVALID_PROPERTY_VALUE, propertyName, propertyValue);
    }

    private int getIntProperty(String propertyName, int defaultValue) throws RangerAuthzException {
        String propertyValue = properties.getProperty(propertyName);

        if (StringUtils.isBlank(propertyValue)) {
            return defaultValue;
        }

        try {
            int ret = Integer.parseInt(propertyValue.trim());

            if (ret < 0) {
                throw new RangerAuthzException(INVALID_PROPERTY_VALUE, propertyName, propertyValue);
            }

            return ret;
        } catch (NumberFormatException e) {
            throw new RangerAuthzException(INVALID_PROPERTY_VALUE, e, propertyName, propertyValue);
        }
    }

    private static String normalizeBaseUrl(String url) {
        String ret = StringUtils.trimToEmpty(url);

        while (ret.endsWith("/")) {
            ret = ret.substring(0, ret.length() - 1);
        }

        return ret;
    }

    private static String trimToNull(String value) {
        return StringUtils.trimToNull(value);
    }
}
