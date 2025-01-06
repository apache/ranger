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

/**
 *
 */
package org.apache.ranger.common;

import org.apache.commons.lang.StringUtils;
import org.apache.ranger.biz.RangerBizUtil;
import org.apache.ranger.credentialapi.CredentialReader;
import org.apache.ranger.plugin.util.RangerCommonConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.beans.factory.config.PropertyPlaceholderConfigurer;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyStore;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

public class PropertiesUtil extends PropertyPlaceholderConfigurer {
    private static final Logger LOG = LoggerFactory.getLogger(PropertiesUtil.class);

    private static final Map<String, String> propertiesMap         = new HashMap<>();
    protected            List<String>        xmlPropertyConfigurer = new ArrayList<>();

    private PropertiesUtil() {
    }

    public static String getProperty(String key, String defaultValue) {
        if (key == null) {
            return null;
        }

        String rtrnVal = propertiesMap.get(key);

        if (rtrnVal == null) {
            rtrnVal = defaultValue;
        }

        return rtrnVal;
    }

    public static String getProperty(String key) {
        if (key == null) {
            return null;
        }

        return propertiesMap.get(key);
    }

    public static String[] getPropertyStringList(String key) {
        if (key == null) {
            return null;
        }

        String value = propertiesMap.get(key);

        if (value != null) {
            String[] splitValues  = value.split(",");
            String[] returnValues = new String[splitValues.length];

            for (int i = 0; i < splitValues.length; i++) {
                returnValues[i] = splitValues[i].trim();
            }

            return returnValues;
        } else {
            return new String[0];
        }
    }

    public static Integer getIntProperty(String key, int defaultValue) {
        if (key == null) {
            return defaultValue;
        }

        String rtrnVal = propertiesMap.get(key);

        if (rtrnVal == null) {
            return defaultValue;
        }

        return Integer.valueOf(rtrnVal);
    }

    public static long getLongProperty(String key, long defaultValue) {
        if (key == null) {
            return defaultValue;
        }

        String rtrnVal = propertiesMap.get(key);

        if (rtrnVal == null) {
            return defaultValue;
        }

        return Long.parseLong(rtrnVal);
    }

    public static Integer getIntProperty(String key) {
        if (key == null) {
            return null;
        }

        String rtrnVal = propertiesMap.get(key);

        if (rtrnVal == null) {
            return null;
        }

        return Integer.valueOf(rtrnVal);
    }

    public static boolean getBooleanProperty(String key, boolean defaultValue) {
        if (key == null) {
            return defaultValue;
        }

        String value = getProperty(key);

        if (value == null) {
            return defaultValue;
        }

        return Boolean.parseBoolean(value);
    }

    public static Map<String, String> getPropertiesMap() {
        return propertiesMap;
    }

    public static Properties getProps() {
        Properties ret = new Properties();

        ret.putAll(propertiesMap);

        return ret;
    }

    @Override
    protected void processProperties(ConfigurableListableBeanFactory beanFactory, Properties props) throws BeansException {
        // First let's add the system properties
        Set<Object> keySet = System.getProperties().keySet();

        for (Object key : keySet) {
            String keyStr = key.toString();

            propertiesMap.put(keyStr, System.getProperties().getProperty(keyStr).trim());
        }

        // Let's add our properties now
        keySet = props.keySet();

        for (Object key : keySet) {
            String keyStr = key.toString();

            propertiesMap.put(keyStr, props.getProperty(keyStr).trim());
        }

        String storeType = propertiesMap.get("ranger.keystore.file.type");

        // update system trust store path with custom trust store.
        if (propertiesMap.containsKey("ranger.truststore.file")) {
            if (!StringUtils.isEmpty(propertiesMap.get("ranger.truststore.file"))) {
                System.setProperty("javax.net.ssl.trustStore", propertiesMap.get("ranger.truststore.file"));
                System.setProperty("javax.net.ssl.trustStoreType", KeyStore.getDefaultType());

                Path trustStoreFile = Paths.get(propertiesMap.get("ranger.truststore.file"));

                if (!Files.exists(trustStoreFile) || !Files.isReadable(trustStoreFile)) {
                    LOG.debug("Could not find or read truststore file '{}'", propertiesMap.get("ranger.truststore.file"));
                } else {
                    if (propertiesMap.containsKey("ranger.credential.provider.path")) {
                        String path            = propertiesMap.get("ranger.credential.provider.path");
                        String trustStoreAlias = getProperty("ranger.truststore.alias", "trustStoreAlias");

                        if (path != null && trustStoreAlias != null) {
                            String trustStorePassword = CredentialReader.getDecryptedString(path.trim(), trustStoreAlias.trim(), storeType);

                            if (trustStorePassword != null && !trustStorePassword.trim().isEmpty() && !trustStorePassword.trim().equalsIgnoreCase("none")) {
                                propertiesMap.put("ranger.truststore.password", trustStorePassword);
                                props.put("ranger.truststore.password", trustStorePassword);
                            } else {
                                LOG.info("trustStorePassword password not applied; clear text password shall be applicable");
                            }
                        }
                    }
                }
            }

            System.setProperty("javax.net.ssl.trustStorePassword", propertiesMap.get("ranger.truststore.password"));
        }

        // update system key store path with custom key store.
        if (propertiesMap.containsKey("ranger.keystore.file")) {
            if (!StringUtils.isEmpty(propertiesMap.get("ranger.keystore.file"))) {
                System.setProperty("javax.net.ssl.keyStore", propertiesMap.get("ranger.keystore.file"));
                System.setProperty("javax.net.ssl.keyStoreType", KeyStore.getDefaultType());

                Path keyStoreFile = Paths.get(propertiesMap.get("ranger.keystore.file"));

                if (!Files.exists(keyStoreFile) || !Files.isReadable(keyStoreFile)) {
                    LOG.debug("Could not find or read keystore file '{}'", propertiesMap.get("ranger.keystore.file"));
                } else {
                    if (propertiesMap.containsKey("ranger.credential.provider.path")) {
                        String path          = propertiesMap.get("ranger.credential.provider.path");
                        String keyStoreAlias = getProperty("ranger.keystore.alias", "keyStoreAlias");

                        if (path != null && keyStoreAlias != null) {
                            String keyStorePassword = CredentialReader.getDecryptedString(path.trim(), keyStoreAlias.trim(), storeType);

                            if (keyStorePassword != null && !keyStorePassword.trim().isEmpty() && !keyStorePassword.trim().equalsIgnoreCase("none")) {
                                propertiesMap.put("ranger.keystore.password", keyStorePassword);
                                props.put("ranger.keystore.password", keyStorePassword);
                            } else {
                                LOG.info("keyStorePassword password not applied; clear text password shall be applicable");
                            }
                        }
                    }
                }
            }

            System.setProperty("javax.net.ssl.keyStorePassword", propertiesMap.get("ranger.keystore.password"));
        }

        //update unixauth keystore and truststore credentials
        if (propertiesMap.containsKey("ranger.credential.provider.path")) {
            String path = propertiesMap.get("ranger.credential.provider.path");

            if (path != null) {
                String unixAuthKeyStoreAlias = getProperty("ranger.unixauth.keystore.alias", "unixAuthKeyStoreAlias");

                if (unixAuthKeyStoreAlias != null) {
                    String unixAuthKeyStorePass = CredentialReader.getDecryptedString(path.trim(), unixAuthKeyStoreAlias.trim(), storeType);

                    if (unixAuthKeyStorePass != null && !unixAuthKeyStorePass.trim().isEmpty() && !unixAuthKeyStorePass.trim().equalsIgnoreCase("none")) {
                        propertiesMap.put("ranger.unixauth.keystore.password", unixAuthKeyStorePass);
                        props.put("ranger.unixauth.keystore.password", unixAuthKeyStorePass);
                    } else {
                        LOG.info("unixauth keystore password not applied; clear text password shall be applicable");
                    }
                }

                //
                String unixAuthTrustStoreAlias = getProperty("ranger.unixauth.truststore.alias", "unixAuthTrustStoreAlias");

                if (unixAuthTrustStoreAlias != null) {
                    String unixAuthTrustStorePass = CredentialReader.getDecryptedString(path.trim(), unixAuthTrustStoreAlias.trim(), storeType);

                    if (unixAuthTrustStorePass != null && !unixAuthTrustStorePass.trim().isEmpty() && !unixAuthTrustStorePass.trim().equalsIgnoreCase("none")) {
                        propertiesMap.put("ranger.unixauth.truststore.password", unixAuthTrustStorePass);
                        props.put("ranger.unixauth.truststore.password", unixAuthTrustStorePass);
                    } else {
                        LOG.info("unixauth truststore password not applied; clear text password shall be applicable");
                    }
                }
            }
        }

        //update credential from keystore
        if (propertiesMap.containsKey("ranger.credential.provider.path") && propertiesMap.containsKey("ranger.jpa.jdbc.credential.alias")) {
            String path  = propertiesMap.get("ranger.credential.provider.path");
            String alias = propertiesMap.get("ranger.jpa.jdbc.credential.alias");

            if (path != null && alias != null) {
                String xaDBPassword = CredentialReader.getDecryptedString(path.trim(), alias.trim(), storeType);

                if (xaDBPassword != null && !xaDBPassword.trim().isEmpty() && !"none".equalsIgnoreCase(xaDBPassword.trim())) {
                    propertiesMap.put("ranger.jpa.jdbc.password", xaDBPassword);
                    props.put("ranger.jpa.jdbc.password", xaDBPassword);
                } else {
                    LOG.info("Credential keystore password not applied for Ranger DB; clear text password shall be applicable");
                }
            }
        }

        if (propertiesMap.containsKey("ranger.credential.provider.path") && propertiesMap.containsKey("ranger.jpa.audit.jdbc.credential.alias")) {
            String path  = propertiesMap.get("ranger.credential.provider.path");
            String alias = propertiesMap.get("ranger.jpa.audit.jdbc.credential.alias");

            if (path != null && alias != null) {
                String auditDBPassword = CredentialReader.getDecryptedString(path.trim(), alias.trim(), storeType);

                if (auditDBPassword != null && !auditDBPassword.trim().isEmpty() && !"none".equalsIgnoreCase(auditDBPassword.trim())) {
                    propertiesMap.put("ranger.jpa.audit.jdbc.password", auditDBPassword);
                    props.put("ranger.jpa.audit.jdbc.password", auditDBPassword);
                } else {
                    LOG.info("Credential keystore password not applied for Audit DB; clear text password shall be applicable");
                }
            }
        }

        if (propertiesMap.containsKey("ranger.authentication.method")) {
            String authenticationMethod = propertiesMap.get("ranger.authentication.method");

            if (("ACTIVE_DIRECTORY".equalsIgnoreCase(authenticationMethod) || "AD".equalsIgnoreCase(authenticationMethod))) {
                if (propertiesMap.containsKey("ranger.credential.provider.path") && propertiesMap.containsKey("ranger.ldap.ad.binddn.credential.alias")) {
                    String path  = propertiesMap.get("ranger.credential.provider.path");
                    String alias = propertiesMap.get("ranger.ldap.ad.binddn.credential.alias");

                    if (path != null && alias != null) {
                        String bindDNPassword = CredentialReader.getDecryptedString(path.trim(), alias.trim(), storeType);

                        if (bindDNPassword != null && !bindDNPassword.trim().isEmpty() && !"none".equalsIgnoreCase(bindDNPassword.trim())) {
                            propertiesMap.put("ranger.ldap.ad.bind.password", bindDNPassword);
                            props.put("ranger.ldap.ad.bind.password", bindDNPassword);
                        } else {
                            LOG.info("Credential keystore password not applied for AD Bind DN; clear text password shall be applicable");
                        }
                    }
                }
            }
        }

        if (propertiesMap.containsKey("ranger.authentication.method")) {
            String authenticationMethod = propertiesMap.get("ranger.authentication.method");

            if (("LDAP".equalsIgnoreCase(authenticationMethod))) {
                if (propertiesMap.containsKey("ranger.credential.provider.path") && propertiesMap.containsKey("ranger.ldap.binddn.credential.alias")) {
                    String path  = propertiesMap.get("ranger.credential.provider.path");
                    String alias = propertiesMap.get("ranger.ldap.binddn.credential.alias");

                    if (path != null && alias != null) {
                        String bindDNPassword = CredentialReader.getDecryptedString(path.trim(), alias.trim(), storeType);

                        if (bindDNPassword != null && !bindDNPassword.trim().isEmpty() && !"none".equalsIgnoreCase(bindDNPassword.trim())) {
                            propertiesMap.put("ranger.ldap.bind.password", bindDNPassword);
                            props.put("ranger.ldap.bind.password", bindDNPassword);
                        } else {
                            LOG.info("Credential keystore password not applied for LDAP Bind DN; clear text password shall be applicable");
                        }
                    }
                }
            }
        }

        if (propertiesMap.containsKey("ranger.audit.source.type")) {
            String auditStore = propertiesMap.get("ranger.audit.source.type");

            if (("solr".equalsIgnoreCase(auditStore))) {
                if (propertiesMap.containsKey("ranger.credential.provider.path") && propertiesMap.containsKey("ranger.solr.audit.credential.alias")) {
                    String path  = propertiesMap.get("ranger.credential.provider.path");
                    String alias = propertiesMap.get("ranger.solr.audit.credential.alias");

                    if (path != null && alias != null) {
                        String solrAuditPassword = CredentialReader.getDecryptedString(path.trim(), alias.trim(), storeType);

                        if (solrAuditPassword != null && !solrAuditPassword.trim().isEmpty() && !"none".equalsIgnoreCase(solrAuditPassword.trim())) {
                            propertiesMap.put("ranger.solr.audit.user.password", solrAuditPassword);
                            props.put("ranger.solr.audit.user.password", solrAuditPassword);
                        } else {
                            LOG.info("Credential keystore password not applied for Solr; clear text password shall be applicable");
                        }
                    }
                }
            }
        }

        String sha256PasswordUpdateDisable = "false";

        if (propertiesMap.containsKey("ranger.sha256Password.update.disable")) {
            sha256PasswordUpdateDisable = propertiesMap.get("ranger.sha256Password.update.disable");

            if (sha256PasswordUpdateDisable == null || sha256PasswordUpdateDisable.trim().isEmpty() || !"true".equalsIgnoreCase(sha256PasswordUpdateDisable)) {
                sha256PasswordUpdateDisable = "false";
            }
        }

        propertiesMap.put("ranger.sha256Password.update.disable", sha256PasswordUpdateDisable);
        props.put("ranger.sha256Password.update.disable", sha256PasswordUpdateDisable);

        if (RangerBizUtil.getDBFlavor() == AppConstants.DB_FLAVOR_MYSQL || RangerBizUtil.getDBFlavor() == AppConstants.DB_FLAVOR_POSTGRES) {
            if (propertiesMap.containsKey("ranger.db.ssl.enabled")) {
                String dbSslEnabled = propertiesMap.get("ranger.db.ssl.enabled");

                if (StringUtils.isEmpty(dbSslEnabled) || !"true".equalsIgnoreCase(dbSslEnabled)) {
                    dbSslEnabled = "false";
                }

                dbSslEnabled = dbSslEnabled.toLowerCase();

                String rangerJpaJdbcUrl = propertiesMap.get("ranger.jpa.jdbc.url");

                if ("true".equalsIgnoreCase(dbSslEnabled)) {
                    String dbSslRequired = propertiesMap.get("ranger.db.ssl.required");

                    if (StringUtils.isEmpty(dbSslRequired) || !"true".equalsIgnoreCase(dbSslRequired)) {
                        dbSslRequired = "false";
                    }

                    dbSslRequired = dbSslRequired.toLowerCase();

                    String dbSslVerifyServerCertificate = propertiesMap.get("ranger.db.ssl.verifyServerCertificate");

                    if (StringUtils.isEmpty(dbSslVerifyServerCertificate) || !"true".equalsIgnoreCase(dbSslVerifyServerCertificate)) {
                        dbSslVerifyServerCertificate = "false";
                    }

                    dbSslVerifyServerCertificate = dbSslVerifyServerCertificate.toLowerCase();

                    String dbSslAuthType = propertiesMap.get("ranger.db.ssl.auth.type");

                    if (StringUtils.isEmpty(dbSslAuthType) || !"1-way".equalsIgnoreCase(dbSslAuthType)) {
                        dbSslAuthType = "2-way";
                    }

                    propertiesMap.put("ranger.db.ssl.enabled", dbSslEnabled);
                    props.put("ranger.db.ssl.enabled", dbSslEnabled);
                    propertiesMap.put("ranger.db.ssl.required", dbSslRequired);
                    props.put("ranger.db.ssl.required", dbSslRequired);
                    propertiesMap.put("ranger.db.ssl.verifyServerCertificate", dbSslVerifyServerCertificate);
                    props.put("ranger.db.ssl.verifyServerCertificate", dbSslVerifyServerCertificate);
                    propertiesMap.put("ranger.db.ssl.auth.type", dbSslAuthType);
                    props.put("ranger.db.ssl.auth.type", dbSslAuthType);

                    if (StringUtils.isNotEmpty(rangerJpaJdbcUrl) && !rangerJpaJdbcUrl.contains("?")) {
                        StringBuilder rangerJpaJdbcUrlSsl = new StringBuilder(rangerJpaJdbcUrl);

                        if (RangerBizUtil.getDBFlavor() == AppConstants.DB_FLAVOR_MYSQL) {
                            rangerJpaJdbcUrlSsl.append("?useSSL=").append(dbSslEnabled).append("&requireSSL=").append(dbSslRequired).append("&verifyServerCertificate=").append(dbSslVerifyServerCertificate);
                        } else if (RangerBizUtil.getDBFlavor() == AppConstants.DB_FLAVOR_POSTGRES) {
                            String dbSslCertificateFile = propertiesMap.get("ranger.db.ssl.certificateFile");

                            if (StringUtils.isNotEmpty(dbSslCertificateFile)) {
                                rangerJpaJdbcUrlSsl.append("?ssl=").append(dbSslEnabled).append("&sslmode=verify-full").append("&sslrootcert=").append(dbSslCertificateFile);
                            } else if ("true".equalsIgnoreCase(dbSslVerifyServerCertificate) || "true".equalsIgnoreCase(dbSslRequired)) {
                                rangerJpaJdbcUrlSsl.append("?ssl=").append(dbSslEnabled).append("&sslmode=verify-full").append("&sslfactory=org.postgresql.ssl.DefaultJavaSSLFactory");
                            } else {
                                rangerJpaJdbcUrlSsl.append("?ssl=").append(dbSslEnabled);
                            }
                        }

                        propertiesMap.put("ranger.jpa.jdbc.url", rangerJpaJdbcUrlSsl.toString());
                    }

                    rangerJpaJdbcUrl = propertiesMap.get("ranger.jpa.jdbc.url");

                    if (StringUtils.isNotEmpty(rangerJpaJdbcUrl)) {
                        props.put("ranger.jpa.jdbc.url", rangerJpaJdbcUrl);
                    }

                    LOG.info("ranger.jpa.jdbc.url={}", rangerJpaJdbcUrl);
                } else {
                    String rangerJpaJdbcUrlExtraArgs = "";

                    if (!StringUtils.isEmpty(rangerJpaJdbcUrl)) {
                        if (rangerJpaJdbcUrl.contains("?")) {
                            rangerJpaJdbcUrlExtraArgs = rangerJpaJdbcUrl.substring(rangerJpaJdbcUrl.indexOf("?") + 1);
                            rangerJpaJdbcUrl            = rangerJpaJdbcUrl.substring(0, rangerJpaJdbcUrl.indexOf("?"));
                        }

                        if (RangerBizUtil.getDBFlavor() == AppConstants.DB_FLAVOR_MYSQL) {
                            StringBuilder rangerJpaJdbcUrlNoSsl = new StringBuilder(rangerJpaJdbcUrl);

                            if (!rangerJpaJdbcUrlExtraArgs.contains("useSSL")) {
                                rangerJpaJdbcUrlNoSsl.append("?useSSL=false");
                            }

                            if (!StringUtils.isEmpty(rangerJpaJdbcUrlExtraArgs) && rangerJpaJdbcUrlNoSsl.toString().contains("useSSL")) {
                                rangerJpaJdbcUrlNoSsl.append("&").append(rangerJpaJdbcUrlExtraArgs);
                            } else if (!StringUtils.isEmpty(rangerJpaJdbcUrlExtraArgs) && !rangerJpaJdbcUrlNoSsl.toString().contains("useSSL")) {
                                rangerJpaJdbcUrlNoSsl.append("?").append(rangerJpaJdbcUrlExtraArgs);
                            }

                            propertiesMap.put("ranger.jpa.jdbc.url", rangerJpaJdbcUrlNoSsl.toString());
                        }

                        rangerJpaJdbcUrl = propertiesMap.get("ranger.jpa.jdbc.url");

                        if (StringUtils.isNotEmpty(rangerJpaJdbcUrl)) {
                            props.put("ranger.jpa.jdbc.url", rangerJpaJdbcUrl);
                        }

                        LOG.info("ranger.jpa.jdbc.url={}", rangerJpaJdbcUrl);
                    }
                }
            }
        }

        if (propertiesMap.containsKey(RangerCommonConstants.PROP_COOKIE_NAME)) {
            String cookieName = propertiesMap.get(RangerCommonConstants.PROP_COOKIE_NAME);

            if (StringUtils.isBlank(cookieName)) {
                cookieName = RangerCommonConstants.DEFAULT_COOKIE_NAME;
            }

            propertiesMap.put(RangerCommonConstants.PROP_COOKIE_NAME, cookieName);
            props.put(RangerCommonConstants.PROP_COOKIE_NAME, cookieName);
        }

        keySet = props.keySet();

        for (Object key : keySet) {
            String keyStr = key.toString();

            if (LOG.isDebugEnabled()) {
                LOG.debug("PropertiesUtil:[{}][{}]", keyStr, keyStr.toLowerCase().contains("pass") ? "********]" : props.get(keyStr));
            }
        }

        super.processProperties(beanFactory, props);
    }
}
