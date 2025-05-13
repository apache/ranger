/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.crypto.key;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.ranger.kms.dao.DaoManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyStore;
import java.util.HashMap;
import java.util.Map;

public class RangerKMSDB {
    public static final int DB_FLAVOR_UNKNOWN     = 0;
    public static final int DB_FLAVOR_MYSQL       = 1;
    public static final int DB_FLAVOR_ORACLE      = 2;
    public static final int DB_FLAVOR_POSTGRES    = 3;
    public static final int DB_FLAVOR_SQLSERVER   = 4;
    public static final int DB_FLAVOR_SQLANYWHERE = 5;
    private static final Logger logger = LoggerFactory.getLogger(RangerKMSDB.class);
    private static final String PROPERTY_PREFIX                = "ranger.ks.";
    private static final String DB_DIALECT                     = "jpa.jdbc.dialect";
    private static final String DB_DRIVER                      = "jpa.jdbc.driver";
    private static final String DB_URL                         = "jpa.jdbc.url";
    private static final String DB_USER                        = "jpa.jdbc.user";
    private static final String DB_PASSWORD                    = "jpa.jdbc.password";
    private static final String JPA_DB_DIALECT                 = "javax.persistence.jdbc.dialect";
    private static final String JPA_DB_DRIVER                  = "javax.persistence.jdbc.driver";
    private static final String JPA_DB_URL                     = "javax.persistence.jdbc.url";
    private static final String JPA_DB_USER                    = "javax.persistence.jdbc.user";
    private static final String JPA_DB_PASSWORD                = "javax.persistence.jdbc.password";
    private static final String DB_SSL_ENABLED                 = "db.ssl.enabled";
    private static final String DB_SSL_REQUIRED                = "db.ssl.required";
    private static final String DB_SSL_VerifyServerCertificate = "db.ssl.verifyServerCertificate";
    private static final String DB_SSL_AUTH_TYPE               = "db.ssl.auth.type";
    private static final String DB_SSL_KEYSTORE                = "keystore.file";
    private static final String DB_SSL_KEYSTORE_PASSWORD       = "keystore.password";
    private static final String DB_SSL_TRUSTSTORE              = "truststore.file";
    private static final String DB_SSL_TRUSTSTORE_PASSWORD     = "truststore.password";
    private static final String DB_SSL_CERTIFICATE_FILE        = "db.ssl.certificateFile";

    private final Configuration       conf;
    private final Map<String, String> jpaProperties = new HashMap<>();
    private final DaoManager          daoManager;

    public RangerKMSDB(Configuration conf) {
        this.conf = conf;

        DaoManager daoManager = null;

        try {
            jpaProperties.put(JPA_DB_DIALECT, conf.get(PROPERTY_PREFIX + DB_DIALECT));
            jpaProperties.put(JPA_DB_DRIVER, conf.get(PROPERTY_PREFIX + DB_DRIVER));
            jpaProperties.put(JPA_DB_URL, conf.get(PROPERTY_PREFIX + DB_URL));
            jpaProperties.put(JPA_DB_USER, conf.get(PROPERTY_PREFIX + DB_USER));
            jpaProperties.put(JPA_DB_PASSWORD, conf.get(PROPERTY_PREFIX + DB_PASSWORD));

            int dbFlavor = getDBFlavor(conf);

            if (dbFlavor == DB_FLAVOR_MYSQL || dbFlavor == DB_FLAVOR_POSTGRES) {
                updateDBSSLURL();
            }

            EntityManagerFactory entityManagerFactory = Persistence.createEntityManagerFactory("persistence_ranger_server", jpaProperties);

            daoManager = new DaoManager(entityManagerFactory);

            daoManager.getEntityManager(); // this forces the connection to be made to DB

            logger.info("Connected to DB : {}", isDbConnected());
        } catch (Exception excp) {
            logger.error("initDBConnectivity() failed", excp);
        } finally {
            this.daoManager = daoManager;
        }
    }

    public DaoManager getDaoManager() {
        return daoManager;
    }

    private boolean isDbConnected() {
        EntityManager em = getEntityManager();

        return em != null && em.isOpen();
    }

    private EntityManager getEntityManager() {
        DaoManager daoMgr = daoManager;

        if (daoMgr != null) {
            try {
                return daoMgr.getEntityManager();
            } catch (Exception excp) {
                logger.error("getEntityManager() failed", excp);
            }
        }

        return null;
    }

    private int getDBFlavor(Configuration newConfig) {
        String[] propertyNames = new String[] {PROPERTY_PREFIX + DB_DIALECT, PROPERTY_PREFIX + DB_DRIVER, PROPERTY_PREFIX + DB_URL};

        for (String propertyName : propertyNames) {
            String propertyValue = newConfig.get(propertyName);

            if (StringUtils.isBlank(propertyValue)) {
                continue;
            }

            if (StringUtils.containsIgnoreCase(propertyValue, "mysql")) {
                return DB_FLAVOR_MYSQL;
            } else if (StringUtils.containsIgnoreCase(propertyValue, "oracle")) {
                return DB_FLAVOR_ORACLE;
            } else if (StringUtils.containsIgnoreCase(propertyValue, "postgresql")) {
                return DB_FLAVOR_POSTGRES;
            } else if (StringUtils.containsIgnoreCase(propertyValue, "sqlserver")) {
                return DB_FLAVOR_SQLSERVER;
            } else if (StringUtils.containsIgnoreCase(propertyValue, "mssql")) {
                return DB_FLAVOR_SQLSERVER;
            } else if (StringUtils.containsIgnoreCase(propertyValue, "sqlanywhere")) {
                return DB_FLAVOR_SQLANYWHERE;
            } else if (StringUtils.containsIgnoreCase(propertyValue, "sqla")) {
                return DB_FLAVOR_SQLANYWHERE;
            } else {
                logger.debug("DB Flavor could not be determined from property - {} = {}", propertyName, propertyValue);
            }
        }

        logger.error("DB Flavor could not be determined");

        return DB_FLAVOR_UNKNOWN;
    }

    private void updateDBSSLURL() {
        if (conf != null && conf.get(PROPERTY_PREFIX + DB_SSL_ENABLED) != null) {
            final String dbSslEnabled     = normalize(conf.get(PROPERTY_PREFIX + DB_SSL_ENABLED));
            String       rangerJpaJdbcUrl = conf.get(PROPERTY_PREFIX + DB_URL);
            int          dbFlavor         = getDBFlavor(conf);
            if ("true".equalsIgnoreCase(dbSslEnabled)) {
                final String dbSslRequired                = normalize(conf.get(PROPERTY_PREFIX + DB_SSL_REQUIRED));
                final String dbSslVerifyServerCertificate = normalize(conf.get(PROPERTY_PREFIX + DB_SSL_VerifyServerCertificate));
                final String dbSslAuthType                = conf.get(PROPERTY_PREFIX + DB_SSL_AUTH_TYPE, "2-way");

                conf.set(PROPERTY_PREFIX + DB_SSL_ENABLED, dbSslEnabled);
                conf.set(PROPERTY_PREFIX + DB_SSL_REQUIRED, dbSslRequired);
                conf.set(PROPERTY_PREFIX + DB_SSL_VerifyServerCertificate, dbSslVerifyServerCertificate);
                conf.set(PROPERTY_PREFIX + DB_SSL_AUTH_TYPE, dbSslAuthType);

                if (StringUtils.isNotEmpty(rangerJpaJdbcUrl) && !rangerJpaJdbcUrl.contains("?")) {
                    StringBuilder rangerJpaJdbcUrlSsl = new StringBuilder(rangerJpaJdbcUrl);

                    if (dbFlavor == DB_FLAVOR_MYSQL) {
                        rangerJpaJdbcUrlSsl.append("?useSSL=").append(dbSslEnabled)
                                .append("&requireSSL=").append(dbSslRequired)
                                .append("&verifyServerCertificate=").append(dbSslVerifyServerCertificate);
                    } else if (dbFlavor == DB_FLAVOR_POSTGRES) {
                        String dbSslCertificateFile = conf.get(PROPERTY_PREFIX + DB_SSL_CERTIFICATE_FILE);

                        if (StringUtils.isNotEmpty(dbSslCertificateFile)) {
                            rangerJpaJdbcUrlSsl.append("?ssl=").append(dbSslEnabled)
                                    .append("&sslmode=verify-full")
                                    .append("&sslrootcert=").append(dbSslCertificateFile);
                        } else if ("true".equalsIgnoreCase(dbSslVerifyServerCertificate) || "true".equalsIgnoreCase(dbSslRequired)) {
                            rangerJpaJdbcUrlSsl.append("?ssl=").append(dbSslEnabled)
                                    .append("&sslmode=verify-full")
                                    .append("&sslfactory=org.postgresql.ssl.DefaultJavaSSLFactory");
                        } else {
                            rangerJpaJdbcUrlSsl.append("?ssl=").append(dbSslEnabled);
                        }
                    }

                    conf.set(PROPERTY_PREFIX + DB_URL, rangerJpaJdbcUrlSsl.toString());
                }

                jpaProperties.put(JPA_DB_URL, conf.get(PROPERTY_PREFIX + DB_URL));

                logger.info("{}{}={}", PROPERTY_PREFIX, DB_URL, conf.get(PROPERTY_PREFIX + DB_URL));

                if ("true".equalsIgnoreCase(dbSslVerifyServerCertificate) || "true".equalsIgnoreCase(dbSslRequired)) {
                    if (!"1-way".equalsIgnoreCase((dbSslAuthType))) {
                        // update system key store path with custom key store.
                        String keystore = conf.get(PROPERTY_PREFIX + DB_SSL_KEYSTORE);

                        if (!StringUtils.isEmpty(keystore)) {
                            Path path = Paths.get(keystore);

                            if (Files.exists(path) && Files.isReadable(path)) {
                                System.setProperty("javax.net.ssl.keyStore", conf.get(PROPERTY_PREFIX + DB_SSL_KEYSTORE));
                                System.setProperty("javax.net.ssl.keyStorePassword", conf.get(PROPERTY_PREFIX + DB_SSL_KEYSTORE_PASSWORD));
                                System.setProperty("javax.net.ssl.keyStoreType", KeyStore.getDefaultType());
                            } else {
                                logger.debug("Could not find or read keystore file '{}'", keystore);
                            }
                        } else {
                            logger.debug("keystore property '{}' value not found!", PROPERTY_PREFIX + DB_SSL_KEYSTORE);
                        }
                    }

                    // update system trust store path with custom trust store.
                    String truststore = conf.get(PROPERTY_PREFIX + DB_SSL_TRUSTSTORE);

                    if (!StringUtils.isEmpty(truststore)) {
                        Path path = Paths.get(truststore);

                        if (Files.exists(path) && Files.isReadable(path)) {
                            System.setProperty("javax.net.ssl.trustStore", conf.get(PROPERTY_PREFIX + DB_SSL_TRUSTSTORE));
                            System.setProperty("javax.net.ssl.trustStorePassword", conf.get(PROPERTY_PREFIX + DB_SSL_TRUSTSTORE_PASSWORD));
                            System.setProperty("javax.net.ssl.trustStoreType", KeyStore.getDefaultType());
                        } else {
                            logger.debug("Could not find or read truststore file '{}'", truststore);
                        }
                    } else {
                        logger.debug("truststore property '{}' value not found!", PROPERTY_PREFIX + DB_SSL_TRUSTSTORE);
                    }
                }
            } else {
                if (dbFlavor == DB_FLAVOR_MYSQL) {
                    if (StringUtils.isNotEmpty(rangerJpaJdbcUrl) && !rangerJpaJdbcUrl.contains("?")) {
                        rangerJpaJdbcUrl = rangerJpaJdbcUrl + "?useSSL=" + dbSslEnabled;
                        conf.set(PROPERTY_PREFIX + DB_URL, rangerJpaJdbcUrl);
                        jpaProperties.put(JPA_DB_URL, conf.get(PROPERTY_PREFIX + DB_URL));
                    }
                }
                logger.info(PROPERTY_PREFIX + DB_URL + "=" + conf.get(PROPERTY_PREFIX + DB_URL));
            }
        }
    }

    private String normalize(String booleanFlag) {
        if (StringUtils.isEmpty(booleanFlag) || !"true".equalsIgnoreCase(booleanFlag)) {
            return "false";
        } else {
            return booleanFlag.toLowerCase();
        }
    }
}
