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

package org.apache.ranger.kafka.auth;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginCallbackHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.URI;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import java.util.stream.Collectors;

/**
 * Builds the security portion of a Kafka client configuration from Ranger-style properties.
 * <p>
 * All keys are read relative to a caller-supplied prefix such as {@code ranger.audit.ingestor}:
 * <ul>
 *   <li>{@code <prefix>.kafka.security.protocol}: PLAINTEXT (default), SSL, SASL_PLAINTEXT or SASL_SSL</li>
 *   <li>{@code <prefix>.kafka.sasl.mechanism}: OAUTHBEARER (default), GSSAPI or any other Kafka mechanism</li>
 *   <li>{@code <prefix>.kafka.sasl.oauthbearer.token.endpoint.url}: OAUTHBEARER token, file:// or http(s)://</li>
 *   <li>{@code <prefix>.service.kerberos.principal}: GSSAPI principal, _HOST is resolved</li>
 *   <li>{@code <prefix>.service.kerberos.keytab}: GSSAPI keytab path</li>
 *   <li>{@code <prefix>.kafka.sasl.*} and {@code <prefix>.kafka.ssl.*}: passed through, overriding derived values</li>
 * </ul>
 * A file:// token URL selects {@link RangerKafkaOAuthBearerLoginCallbackHandler}, an http(s):// URL Kafka's own
 * client-credentials handler. Producers, consumers and admin clients all accept the resulting keys.
 */
public final class RangerKafkaClientSecurityConfig {
    private static final Logger LOG                            = LoggerFactory.getLogger(RangerKafkaClientSecurityConfig.class);

    public static final String PROP_SECURITY_PROTOCOL          = "kafka.security.protocol";
    public static final String PROP_SASL_MECHANISM             = "kafka.sasl.mechanism";
    public static final String PROP_SASL_KERBEROS_SERVICE_NAME = "kafka.sasl.kerberos.service.name";
    public static final String PROP_OAUTH_TOKEN_ENDPOINT_URL   = "kafka.sasl.oauthbearer.token.endpoint.url";
    public static final String PROP_KERBEROS_PRINCIPAL         = "service.kerberos.principal";
    public static final String PROP_KERBEROS_KEYTAB            = "service.kerberos.keytab";
    public static final String PROP_HOST                       = "host";

    public static final String SECURITY_PROTOCOL_PLAINTEXT     = "PLAINTEXT";
    public static final String SASL_MECHANISM_GSSAPI           = "GSSAPI";
    public static final String SASL_MECHANISM_OAUTHBEARER      = "OAUTHBEARER";
    public static final String DEFAULT_SASL_MECHANISM          = SASL_MECHANISM_OAUTHBEARER;
    public static final String DEFAULT_KERBEROS_SERVICE_NAME   = "kafka";
    public static final String HOSTNAME_PATTERN                = "_HOST";

    public static final String OAUTHBEARER_LOGIN_MODULE        = "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule";
    public static final String KRB5_LOGIN_MODULE               = "com.sun.security.auth.module.Krb5LoginModule";

    private static final String KAFKA_PREFIX                   = "kafka.";
    private static final String SASL_PASSTHROUGH               = "kafka.sasl.";
    private static final String SSL_PASSTHROUGH                = "kafka.ssl.";
    private static final String SENSITIVE_KEY_PART             = "password";
    private static final String MASKED_VALUE                   = "***";
    private static final String PROPERTY_NAME_FORMAT           = "%s.%s";
    private static final String KAFKA_PROPERTY_NAME_FORMAT     = "%s.kafka.%s";
    private static final String PRINCIPAL_FORMAT               = "%s/%s@%s";
    private static final String KRB5_JAAS_CONFIG_FORMAT        = "%s required useKeyTab=true keyTab=\"%s\" storeKey=true useTicketCache=false serviceName=%s principal=\"%s\";";
    private static final String OAUTHBEARER_JAAS_CONFIG_FORMAT = "%s required;";

    private RangerKafkaClientSecurityConfig() {
    }

    /** Returns a fresh map holding only the security keys derived from {@code props}. */
    public static Map<String, Object> build(Properties props, String propPrefix) {
        Map<String, Object> ret = new HashMap<>();

        apply(props, propPrefix, ret);

        return ret;
    }

    /** Adds the security keys derived from {@code props} to an existing client configuration. */
    public static void apply(Properties props, String propPrefix, Map<? super String, ? super Object> target) {
        Map<String, Object> security = new TreeMap<>();
        String              protocol = getProperty(props, propPrefix, PROP_SECURITY_PROTOCOL, SECURITY_PROTOCOL_PLAINTEXT);

        security.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, protocol);

        if (isSasl(protocol)) {
            String mechanism = getProperty(props, propPrefix, PROP_SASL_MECHANISM, DEFAULT_SASL_MECHANISM);

            security.put(SaslConfigs.SASL_MECHANISM, mechanism);

            if (SASL_MECHANISM_GSSAPI.equalsIgnoreCase(mechanism)) {
                applyKerberos(props, propPrefix, security);
            } else if (SASL_MECHANISM_OAUTHBEARER.equalsIgnoreCase(mechanism)) {
                applyOAuthBearer(props, propPrefix, security);
            }
        }

        applyPassThrough(props, propPrefix, security);

        if (isSasl(protocol) && !security.containsKey(SaslConfigs.SASL_JAAS_CONFIG)) {
            throw new IllegalStateException(String.format("Kafka security protocol %s with mechanism %s requires %s", protocol, security.get(SaslConfigs.SASL_MECHANISM), String.format(KAFKA_PROPERTY_NAME_FORMAT, propPrefix, SaslConfigs.SASL_JAAS_CONFIG)));
        }

        target.putAll(security);

        logSecurityConfig(propPrefix, security);
    }

    public static boolean isSasl(String securityProtocol) {
        return securityProtocol != null && securityProtocol.trim().toUpperCase(Locale.ROOT).startsWith("SASL");
    }

    /** Resolves {@code _HOST} in a Kerberos principal, falling back to the local canonical host name. */
    public static String resolvePrincipal(String principal, String hostName) {
        String[] components = principal.split("[/@]");

        if (components.length != 3 || !HOSTNAME_PATTERN.equals(components[1])) {
            return principal;
        }

        String fqdn = hostName;

        if (fqdn == null || fqdn.trim().isEmpty() || "0.0.0.0".equals(fqdn)) {
            try {
                fqdn = InetAddress.getLocalHost().getCanonicalHostName();
            } catch (IOException e) {
                throw new IllegalStateException(String.format("Unable to resolve %s in principal %s", HOSTNAME_PATTERN, principal), e);
            }
        }

        return String.format(PRINCIPAL_FORMAT, components[0], fqdn.trim().toLowerCase(Locale.ROOT), components[2]);
    }

    public static String buildKerberosJaasConfig(String principal, String keytab, String serviceName) {
        return String.format(KRB5_JAAS_CONFIG_FORMAT, KRB5_LOGIN_MODULE, keytab, serviceName, principal);
    }

    public static String buildOAuthBearerJaasConfig() {
        return String.format(OAUTHBEARER_JAAS_CONFIG_FORMAT, OAUTHBEARER_LOGIN_MODULE);
    }

    private static void applyKerberos(Properties props, String propPrefix, Map<String, Object> security) {
        String principal   = getProperty(props, propPrefix, PROP_KERBEROS_PRINCIPAL, null);
        String keytab      = getProperty(props, propPrefix, PROP_KERBEROS_KEYTAB, null);
        String hostName    = getProperty(props, propPrefix, PROP_HOST, null);
        String serviceName = getProperty(props, propPrefix, PROP_SASL_KERBEROS_SERVICE_NAME, DEFAULT_KERBEROS_SERVICE_NAME);

        if (principal == null || keytab == null) {
            throw new IllegalStateException(String.format("Kafka SASL/GSSAPI requires both %s and %s", propertyName(propPrefix, PROP_KERBEROS_PRINCIPAL), propertyName(propPrefix, PROP_KERBEROS_KEYTAB)));
        }

        File keytabFile = new File(keytab);

        if (!keytabFile.isFile()) {
            throw new IllegalStateException(String.format("Keytab file not found: %s", keytab));
        }

        if (!keytabFile.canRead()) {
            throw new IllegalStateException(String.format("Keytab file not readable: %s", keytab));
        }

        security.put(SaslConfigs.SASL_KERBEROS_SERVICE_NAME, serviceName);
        security.put(SaslConfigs.SASL_JAAS_CONFIG, buildKerberosJaasConfig(resolvePrincipal(principal, hostName), keytab, serviceName));
    }

    private static void applyOAuthBearer(Properties props, String propPrefix, Map<String, Object> security) {
        String url = getProperty(props, propPrefix, PROP_OAUTH_TOKEN_ENDPOINT_URL, null);

        if (url == null) {
            throw new IllegalStateException(String.format("Kafka SASL/OAUTHBEARER (the default mechanism) requires %s (file:// token or http(s):// token endpoint); set %s=%s with %s and %s to use Kerberos instead",
                    propertyName(propPrefix, PROP_OAUTH_TOKEN_ENDPOINT_URL), propertyName(propPrefix, PROP_SASL_MECHANISM), SASL_MECHANISM_GSSAPI,
                    propertyName(propPrefix, PROP_KERBEROS_PRINCIPAL), propertyName(propPrefix, PROP_KERBEROS_KEYTAB)));
        }

        /* file:// tokens rotate in place, so use the Ranger handler that re-reads them;
         * http(s):// endpoints go through Kafka's own client-credentials handler. */
        String handler = isFileUrl(url) ? RangerKafkaOAuthBearerLoginCallbackHandler.class.getName() : OAuthBearerLoginCallbackHandler.class.getName();

        security.put(SaslConfigs.SASL_JAAS_CONFIG, buildOAuthBearerJaasConfig());
        security.put(SaslConfigs.SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL, url);
        security.put(SaslConfigs.SASL_LOGIN_CALLBACK_HANDLER_CLASS, handler);
    }

    private static void applyPassThrough(Properties props, String propPrefix, Map<String, Object> security) {
        String saslPrefix = propertyName(propPrefix, SASL_PASSTHROUGH);
        String sslPrefix  = propertyName(propPrefix, SSL_PASSTHROUGH);

        for (String name : props.stringPropertyNames()) {
            if (name.startsWith(saslPrefix) || name.startsWith(sslPrefix)) {
                String value = props.getProperty(name);

                if (value != null && !value.trim().isEmpty()) {
                    security.put(name.substring(propPrefix.length() + 1 + KAFKA_PREFIX.length()), value.trim());
                }
            }
        }
    }

    private static boolean isFileUrl(String url) {
        try {
            return "file".equalsIgnoreCase(URI.create(url).getScheme());
        } catch (IllegalArgumentException e) {
            return false;
        }
    }

    private static String getProperty(Properties props, String propPrefix, String name, String defaultValue) {
        String value = props.getProperty(propertyName(propPrefix, name));

        return value == null || value.trim().isEmpty() ? defaultValue : value.trim();
    }

    private static String propertyName(String propPrefix, String name) {
        return String.format(PROPERTY_NAME_FORMAT, propPrefix, name);
    }

    private static void logSecurityConfig(String propPrefix, Map<String, Object> security) {
        String summary = security.entrySet().stream()
                .map(entry -> String.format("%s=%s", entry.getKey(), isSensitive(entry.getKey()) ? MASKED_VALUE : entry.getValue()))
                .collect(Collectors.joining(" "));

        LOG.info("Kafka client security for {}: {}", propPrefix, summary);
    }

    private static boolean isSensitive(String key) {
        return SaslConfigs.SASL_JAAS_CONFIG.equals(key) || key.toLowerCase(Locale.ROOT).contains(SENSITIVE_KEY_PART);
    }
}
