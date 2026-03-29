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

import org.apache.commons.lang3.StringUtils;
import org.apache.ranger.plugin.util.XMLUtils;
import org.ietf.jgss.GSSCredential;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.Properties;

/**
 * Reads Ranger PDP configuration from {@code ranger-pdp-default.xml} (classpath)
 * overridden by {@code ranger-pdp-site.xml} (classpath or filesystem).
 *
 * <p>Both files use the Hadoop {@code <configuration>} XML format, consistent
 * with other Ranger server modules (tagsync, kms, etc.).
 * The format is parsed directly using the JDK DOM API to avoid an early
 * class-load dependency on Hadoop's {@code Configuration} class.
 *
 * <p>Authentication property names:
 * <ul>
 *   <li>Kerberos/SPNEGO:    {@code ranger.pdp.authn.kerberos.*}
 *   <li>JWT bearer token:   {@code ranger.pdp.authn.jwt.*}
 *   <li>HTTP header:        {@code ranger.pdp.authn.header.*}
 * </ul>
 */
public class RangerPdpConfig {
    private static final Logger LOG = LoggerFactory.getLogger(RangerPdpConfig.class);

    private static final String DEFAULT_CONFIG_FILE = "ranger-pdp-default.xml";
    private static final String SITE_CONFIG_FILE    = "ranger-pdp-site.xml";

    private final Properties props = new Properties();

    public RangerPdpConfig() {
        loadFromClasspath(DEFAULT_CONFIG_FILE);
        loadFromClasspath(SITE_CONFIG_FILE);

        String confDir = System.getProperty(RangerPdpConstants.PROP_CONF_DIR, "");

        if (StringUtils.isNotBlank(confDir)) {
            loadFromFile(new File(confDir, SITE_CONFIG_FILE));
        }

        applySystemPropertyOverrides();

        LOG.info("RangerPdpConfig initialized (conf.dir={})", confDir);
    }

    public int getPort() {
        return getInt(RangerPdpConstants.PROP_PORT, 6500);
    }

    public String getLogDir() {
        return get(RangerPdpConstants.PROP_LOG_DIR, "/var/log/ranger/pdp");
    }

    public boolean isSslEnabled() {
        return getBoolean(RangerPdpConstants.PROP_SSL_ENABLED, false);
    }

    public String getKeystoreFile() {
        return get(RangerPdpConstants.PROP_SSL_KEYSTORE_FILE, "");
    }

    public String getKeystorePassword() {
        return get(RangerPdpConstants.PROP_SSL_KEYSTORE_PASSWORD, "");
    }

    public String getKeystoreType() {
        return get(RangerPdpConstants.PROP_SSL_KEYSTORE_TYPE, "JKS");
    }

    public boolean isTruststoreEnabled() {
        return getBoolean(RangerPdpConstants.PROP_SSL_TRUSTSTORE_ENABLED, false);
    }

    public String getTruststoreFile() {
        return get(RangerPdpConstants.PROP_SSL_TRUSTSTORE_FILE, "");
    }

    public String getTruststorePassword() {
        return get(RangerPdpConstants.PROP_SSL_TRUSTSTORE_PASSWORD, "");
    }

    public String getTruststoreType() {
        return get(RangerPdpConstants.PROP_SSL_TRUSTSTORE_TYPE, "JKS");
    }

    public boolean isHttp2Enabled() {
        return getBoolean(RangerPdpConstants.PROP_HTTP2_ENABLED, true);
    }

    public int getHttpConnectorMaxThreads() {
        return getInt(RangerPdpConstants.PROP_HTTP_CONNECTOR_MAX_THREADS, 200);
    }

    public int getHttpConnectorMinSpareThreads() {
        return getInt(RangerPdpConstants.PROP_HTTP_CONNECTOR_MIN_SPARE_THREADS, 20);
    }

    public int getHttpConnectorAcceptCount() {
        return getInt(RangerPdpConstants.PROP_HTTP_CONNECTOR_ACCEPT_COUNT, 100);
    }

    public int getHttpConnectorMaxConnections() {
        return getInt(RangerPdpConstants.PROP_HTTP_CONNECTOR_MAX_CONNECTIONS, 10000);
    }

    public String getAuthnTypes() {
        return get(RangerPdpConstants.PROP_AUTHN_TYPES, "header,jwt,kerberos");
    }

    // --- HTTP Header auth ---
    public boolean isHeaderAuthnEnabled() {
        return getBoolean(RangerPdpConstants.PROP_AUTHN_HEADER_ENABLED, false);
    }

    public String getHeaderAuthnUsername() {
        return get(RangerPdpConstants.PROP_AUTHN_HEADER_USERNAME, "X-Forwarded-User");
    }

    // --- JWT bearer token auth ---
    public boolean isJwtAuthnEnabled() {
        return getBoolean(RangerPdpConstants.PROP_AUTHN_JWT_ENABLED, false);
    }

    public String getJwtProviderUrl() {
        return get(RangerPdpConstants.PROP_AUTHN_JWT_PROVIDER_URL, "");
    }

    public String getJwtPublicKey() {
        return get(RangerPdpConstants.PROP_AUTHN_JWT_PUBLIC_KEY, "");
    }

    public String getJwtCookieName() {
        return get(RangerPdpConstants.PROP_AUTHN_JWT_COOKIE_NAME, "hadoop-jwt");
    }

    public String getJwtAudiences() {
        return get(RangerPdpConstants.PROP_AUTHN_JWT_AUDIENCES, "");
    }

    // --- Kerberos / SPNEGO ---
    public boolean isKerberosAuthnEnabled() {
        return getBoolean(RangerPdpConstants.PROP_AUTHN_KERBEROS_ENABLED, false);
    }

    public String getSpnegoPrincipal() {
        return get(RangerPdpConstants.PROP_AUTHN_KERBEROS_SPNEGO_PRINCIPAL, "");
    }

    public String getSpnegoKeytab() {
        return get(RangerPdpConstants.PROP_AUTHN_KERBEROS_SPNEGO_KEYTAB, "");
    }

    public int getKerberosTokenValiditySeconds() {
        return getInt(RangerPdpConstants.PROP_AUTHN_KERBEROS_KRB_TOKEN_VALIDITY, GSSCredential.INDEFINITE_LIFETIME);
    }

    public String getKerberosNameRules() {
        return get(RangerPdpConstants.PROP_AUTHN_KERBEROS_NAME_RULES, "DEFAULT");
    }

    /**
     * Returns all properties for forwarding to {@code RangerEmbeddedAuthorizer}.
     */
    public Properties getAuthzProperties() {
        return new Properties(props);
    }

    public String get(String key, String defaultValue) {
        String val = props.getProperty(key);

        return StringUtils.isNotBlank(val) ? val.trim() : defaultValue;
    }

    public int getInt(String key, int defaultValue) {
        String val = props.getProperty(key);

        if (StringUtils.isNotBlank(val)) {
            try {
                return Integer.parseInt(val.trim());
            } catch (NumberFormatException e) {
                LOG.warn("Invalid integer for {}: '{}'; using default {}", key, val, defaultValue);
            }
        }

        return defaultValue;
    }

    public boolean getBoolean(String key, boolean defaultValue) {
        String val = props.getProperty(key);

        return StringUtils.isNotBlank(val) ? Boolean.parseBoolean(val.trim()) : defaultValue;
    }

    private void loadFromClasspath(String resourceName) {
        try (InputStream in = getClass().getClassLoader().getResourceAsStream(resourceName)) {
            if (in != null) {
                parseHadoopXml(in, resourceName);
            } else {
                LOG.debug("Config resource not found on classpath: {}", resourceName);
            }
        } catch (IOException e) {
            LOG.warn("Failed to close stream for classpath resource: {}", resourceName, e);
        }
    }

    private void loadFromFile(File file) {
        if (!file.exists() || !file.isFile()) {
            LOG.debug("Config file not found: {}", file);
            return;
        }

        try (InputStream in = Files.newInputStream(file.toPath())) {
            parseHadoopXml(in, file.getAbsolutePath());
        } catch (IOException e) {
            LOG.warn("Failed to read config file: {}", file, e);
        }
    }

    /**
     * Parses a Hadoop-style {@code <configuration>} XML document and merges all
     * {@code <property>} entries into {@link #props}.  Later entries override earlier
     * ones, matching Hadoop's own override semantics.
     *
     * <pre>
     * {@code
     * <configuration>
     *   <property>
     *     <name>some.key</name>
     *     <value>some-value</value>
     *   </property>
     * </configuration>
     * }
     * </pre>
     */
    private void parseHadoopXml(InputStream in, String source) {
        LOG.info("Loading from {}. Properties count {}", source, props.size());

        XMLUtils.loadConfig(in, props);

        LOG.info("Loaded from {}. Properties count {}", source, props.size());
    }

    /**
     * Apply JVM system-property overrides for operationally sensitive keys so Kubernetes
     * (or any orchestrator) can drive runtime config with JAVA_OPTS/-D flags.
     */
    private void applySystemPropertyOverrides() {
        for (String key : System.getProperties().stringPropertyNames()) {
            if (key.startsWith(RangerPdpConstants.PROP_PDP_PREFIX) || key.startsWith(RangerPdpConstants.PROP_AUTHZ_PREFIX)) {
                props.setProperty(key, System.getProperty(key));
            }
        }
    }
}
