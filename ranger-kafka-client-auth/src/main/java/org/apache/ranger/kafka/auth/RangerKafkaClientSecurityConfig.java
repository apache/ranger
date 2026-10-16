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

import org.apache.commons.lang3.StringUtils;
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
import java.util.LinkedHashMap;
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
 *   <li>{@code <prefix>.kafka.sasl.mechanism}: GSSAPI (default), OAUTHBEARER or any other Kafka mechanism</li>
 *   <li>{@code <prefix>.service.kerberos.principal}: GSSAPI principal, _HOST is resolved</li>
 *   <li>{@code <prefix>.service.kerberos.keytab}: GSSAPI keytab path</li>
 *   <li>{@code <prefix>.kafka.sasl.oauthbearer.token.endpoint.url}: OAUTHBEARER token, file:// or http(s)://</li>
 *   <li>{@code <prefix>.kafka.sasl.jaas.loginModuleName}: JAAS login module, defaults per mechanism</li>
 *   <li>{@code <prefix>.kafka.sasl.jaas.loginModuleControlFlag}: JAAS control flag, default required</li>
 *   <li>{@code <prefix>.kafka.sasl.jaas.option.<name>}: any JAAS login module option (for GSSAPI the shipped configuration sets useKeyTab, storeKey, useTicketCache)</li>
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
    public static final String PROP_JAAS_LOGIN_MODULE_NAME     = "kafka.sasl.jaas.loginModuleName";
    public static final String PROP_JAAS_CONTROL_FLAG          = "kafka.sasl.jaas.loginModuleControlFlag";
    public static final String PROP_JAAS_OPTION_PREFIX         = "kafka.sasl.jaas.option.";
    public static final String PROP_KERBEROS_PRINCIPAL         = "service.kerberos.principal";
    public static final String PROP_KERBEROS_KEYTAB            = "service.kerberos.keytab";
    public static final String PROP_HOST                       = "host";

    public static final String SECURITY_PROTOCOL_PLAINTEXT     = "PLAINTEXT";
    public static final String SASL_MECHANISM_GSSAPI           = "GSSAPI";
    public static final String SASL_MECHANISM_OAUTHBEARER      = "OAUTHBEARER";
    public static final String DEFAULT_SASL_MECHANISM          = SASL_MECHANISM_GSSAPI;
    public static final String DEFAULT_JAAS_CONTROL_FLAG       = "required";
    public static final String DEFAULT_KERBEROS_SERVICE_NAME   = "kafka";
    public static final String HOSTNAME_MARKER                 = "_HOST";

    public static final String OAUTHBEARER_LOGIN_MODULE        = "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule";
    public static final String KRB5_LOGIN_MODULE               = "com.sun.security.auth.module.Krb5LoginModule";

    public static final String JAAS_OPTION_USE_KEYTAB          = "useKeyTab";
    public static final String JAAS_OPTION_KEYTAB              = "keyTab";
    public static final String JAAS_OPTION_SERVICE_NAME        = "serviceName";
    public static final String JAAS_OPTION_PRINCIPAL           = "principal";

    private static final String SASL_PROTOCOL_MARKER           = "SASL";
    private static final String SASL_PASSTHROUGH               = "kafka.sasl.";
    private static final String SSL_PASSTHROUGH                = "kafka.ssl.";
    private static final String JAAS_RANGER_PREFIX             = "kafka.sasl.jaas.";
    private static final String KAFKA_PREFIX                   = "kafka.";
    private static final String SENSITIVE_KEY_PART             = "password";
    private static final String MASKED_VALUE                   = "***";
    private static final String PROPERTY_NAME_FORMAT           = "%s.%s";
    private static final String KAFKA_PROPERTY_NAME_FORMAT     = "%s.kafka.%s";
    private static final String PRINCIPAL_FORMAT               = "%s/%s@%s";

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
        Map<String, Object> security = new HashMap<>();
        String              protocol = getProperty(props, propPrefix, PROP_SECURITY_PROTOCOL, SECURITY_PROTOCOL_PLAINTEXT);

        security.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, protocol);

        if (isSasl(protocol)) {
            String mechanism = getProperty(props, propPrefix, PROP_SASL_MECHANISM, DEFAULT_SASL_MECHANISM);

            security.put(SaslConfigs.SASL_MECHANISM, mechanism);

            if (SASL_MECHANISM_GSSAPI.equalsIgnoreCase(mechanism)) {
                applyKerberos(props, propPrefix, security);
            } else if (SASL_MECHANISM_OAUTHBEARER.equalsIgnoreCase(mechanism)) {
                applyOAuthBearer(props, propPrefix, security);
            } else {
                applyConfiguredJaas(props, propPrefix, security, null);
            }
        }

        applyPassThrough(props, propPrefix, security);

        if (isSasl(protocol) && !security.containsKey(SaslConfigs.SASL_JAAS_CONFIG)) {
            throw new IllegalStateException(String.format("Kafka security protocol %s with mechanism %s requires %s or %s", protocol, security.get(SaslConfigs.SASL_MECHANISM),
                    propertyName(propPrefix, PROP_JAAS_LOGIN_MODULE_NAME), String.format(KAFKA_PROPERTY_NAME_FORMAT, propPrefix, SaslConfigs.SASL_JAAS_CONFIG)));
        }

        target.putAll(security);

        logSecurityConfig(propPrefix, security);
    }

    public static boolean isSasl(String securityProtocol) {
        return StringUtils.startsWithIgnoreCase(securityProtocol, SASL_PROTOCOL_MARKER);
    }

    /* Resolves _HOST in a Kerberos principal, falling back to the local canonical host name. */
    static String resolvePrincipal(String principal, String hostName) {
        String[] components = principal.split("[/@]");

        if (components.length != 3 || !HOSTNAME_MARKER.equals(components[1])) {
            return principal;
        }

        String fqdn = hostName;

        if (StringUtils.isBlank(fqdn) || "0.0.0.0".equals(fqdn)) {
            try {
                fqdn = InetAddress.getLocalHost().getCanonicalHostName();
            } catch (IOException e) {
                throw new IllegalStateException(String.format("Unable to resolve %s in principal %s", HOSTNAME_MARKER, principal), e);
            }
        }

        return String.format(PRINCIPAL_FORMAT, components[0], fqdn.toLowerCase(Locale.ROOT), components[2]);
    }

    /* Krb5LoginModule whose options are all configuration driven; the shipped site XML carries
     * useKeyTab/storeKey/useTicketCache, so ticket-cache or other setups need no code change. */
    private static void applyKerberos(Properties props, String propPrefix, Map<String, Object> security) {
        String              principal   = getProperty(props, propPrefix, PROP_KERBEROS_PRINCIPAL, null);
        String              keytab      = getProperty(props, propPrefix, PROP_KERBEROS_KEYTAB, null);
        String              hostName    = getProperty(props, propPrefix, PROP_HOST, null);
        String              serviceName = getProperty(props, propPrefix, PROP_SASL_KERBEROS_SERVICE_NAME, DEFAULT_KERBEROS_SERVICE_NAME);
        Map<String, String> options     = new LinkedHashMap<>();

        /* keyTab, serviceName and principal come from the service properties; every other Krb5LoginModule option (useKeyTab, storeKey, useTicketCache, ...) comes from <prefix>.kafka.sasl.jaas.option.* */
        if (keytab != null) {
            options.put(JAAS_OPTION_KEYTAB, keytab);
        }

        options.put(JAAS_OPTION_SERVICE_NAME, serviceName);

        if (principal != null) {
            options.put(JAAS_OPTION_PRINCIPAL, principal);
        }

        options.putAll(configuredJaasOptions(props, propPrefix));

        if (options.containsKey(JAAS_OPTION_PRINCIPAL)) {
            options.put(JAAS_OPTION_PRINCIPAL, resolvePrincipal(options.get(JAAS_OPTION_PRINCIPAL), hostName));
        }

        if (Boolean.parseBoolean(options.get(JAAS_OPTION_USE_KEYTAB))) {
            /* keytab login was asked for by configuration, so fail early on a missing or unreadable keytab */
            String keytabPath = options.get(JAAS_OPTION_KEYTAB);

            if (keytabPath == null || !options.containsKey(JAAS_OPTION_PRINCIPAL)) {
                throw new IllegalStateException(String.format("Kafka SASL/GSSAPI with %s=true requires both %s and %s", JAAS_OPTION_USE_KEYTAB, propertyName(propPrefix, PROP_KERBEROS_PRINCIPAL), propertyName(propPrefix, PROP_KERBEROS_KEYTAB)));
            }

            File keytabFile = new File(keytabPath);

            if (!keytabFile.isFile()) {
                throw new IllegalStateException(String.format("Keytab file not found: %s", keytabPath));
            }

            if (!keytabFile.canRead()) {
                throw new IllegalStateException(String.format("Keytab file not readable: %s", keytabPath));
            }
        }

        security.put(SaslConfigs.SASL_KERBEROS_SERVICE_NAME, serviceName);
        security.put(SaslConfigs.SASL_JAAS_CONFIG, buildJaasConfig(props, propPrefix, KRB5_LOGIN_MODULE, options));
    }

    private static void applyOAuthBearer(Properties props, String propPrefix, Map<String, Object> security) {
        String url = getProperty(props, propPrefix, PROP_OAUTH_TOKEN_ENDPOINT_URL, null);

        if (url == null) {
            throw new IllegalStateException(String.format("Kafka SASL/OAUTHBEARER requires %s (file:// token or http(s):// token endpoint)", propertyName(propPrefix, PROP_OAUTH_TOKEN_ENDPOINT_URL)));
        }

        /* file:// tokens rotate in place, so use the Ranger handler that re-reads them; http(s):// endpoints go through Kafka's own client-credentials handler. */
        String handler = isFileUrl(url) ? RangerKafkaOAuthBearerLoginCallbackHandler.class.getName() : OAuthBearerLoginCallbackHandler.class.getName();

        security.put(SaslConfigs.SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL, url);
        security.put(SaslConfigs.SASL_LOGIN_CALLBACK_HANDLER_CLASS, handler);

        applyConfiguredJaas(props, propPrefix, security, OAUTHBEARER_LOGIN_MODULE);
    }

    /* JAAS from <prefix>.kafka.sasl.jaas.* with an optional default login module; nothing is set when neither is available, leaving the mechanism to a passed-through sasl.jaas.config. */
    private static void applyConfiguredJaas(Properties props, String propPrefix, Map<String, Object> security, String defaultLoginModule) {
        String loginModule = getProperty(props, propPrefix, PROP_JAAS_LOGIN_MODULE_NAME, defaultLoginModule);

        if (loginModule != null) {
            security.put(SaslConfigs.SASL_JAAS_CONFIG, buildJaasConfig(props, propPrefix, loginModule, configuredJaasOptions(props, propPrefix)));
        }
    }

    private static String buildJaasConfig(Properties props, String propPrefix, String defaultLoginModule, Map<String, String> options) {
        String loginModule = getProperty(props, propPrefix, PROP_JAAS_LOGIN_MODULE_NAME, defaultLoginModule);
        String controlFlag = getProperty(props, propPrefix, PROP_JAAS_CONTROL_FLAG, DEFAULT_JAAS_CONTROL_FLAG);
        String optionText  = options.entrySet().stream()
                .map(option -> String.format(" %s=\"%s\"", option.getKey(), option.getValue()))
                .collect(Collectors.joining());

        return String.format("%s %s%s;", loginModule, controlFlag, optionText);
    }

    /* sorted so the generated JAAS string is stable across runs */
    private static Map<String, String> configuredJaasOptions(Properties props, String propPrefix) {
        Map<String, String> ret          = new TreeMap<>();
        String              optionPrefix = propertyName(propPrefix, PROP_JAAS_OPTION_PREFIX);

        for (String name : props.stringPropertyNames()) {
            if (name.startsWith(optionPrefix)) {
                String value = props.getProperty(name);

                if (StringUtils.isNotBlank(value)) {
                    ret.put(name.substring(optionPrefix.length()), value);
                }
            }
        }

        return ret;
    }

    private static void applyPassThrough(Properties props, String propPrefix, Map<String, Object> security) {
        String saslPrefix = propertyName(propPrefix, SASL_PASSTHROUGH);
        String sslPrefix  = propertyName(propPrefix, SSL_PASSTHROUGH);
        String jaasPrefix = propertyName(propPrefix, JAAS_RANGER_PREFIX);
        String jaasConfig = String.format(KAFKA_PROPERTY_NAME_FORMAT, propPrefix, SaslConfigs.SASL_JAAS_CONFIG);

        for (String name : props.stringPropertyNames()) {
            boolean rangerOnlyJaasKey = name.startsWith(jaasPrefix) && !name.equals(jaasConfig);

            if ((name.startsWith(saslPrefix) || name.startsWith(sslPrefix)) && !rangerOnlyJaasKey) {
                String value = props.getProperty(name);

                if (StringUtils.isNotBlank(value)) {
                    security.put(name.substring(propPrefix.length() + 1 + KAFKA_PREFIX.length()), value);
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

        return StringUtils.isBlank(value) ? defaultValue : value;
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
        return SaslConfigs.SASL_JAAS_CONFIG.equals(key) || StringUtils.containsIgnoreCase(key, SENSITIVE_KEY_PART);
    }
}
