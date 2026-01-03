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

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(MockitoExtension.class)
@Disabled
public class RangerKMSDBTest {
    private static final String PROPERTY_PREFIX                = "ranger.ks.";
    private static final String DB_DIALECT                     = "jpa.jdbc.dialect";
    private static final String DB_DRIVER                      = "jpa.jdbc.driver";
    private static final String DB_URL                         = "jpa.jdbc.url";
    private static final String DB_USER                        = "jpa.jdbc.user";
    private static final String DB_PASSWORD                    = "jpa.jdbc.password";
    private static final String DB_SSL_ENABLED                 = "db.ssl.enabled";
    private static final String DB_SSL_REQUIRED                = "db.ssl.required";
    private static final String DB_SSL_VerifyServerCertificate = "db.ssl.verifyServerCertificate";
    private static final String DB_SSL_AUTH_TYPE               = "db.ssl.auth.type";
    private static final String DB_SSL_KEYSTORE                = "keystore.file";
    private static final String DB_SSL_KEYSTORE_PASSWORD       = "keystore.password";
    private static final String DB_SSL_TRUSTSTORE              = "truststore.file";
    private static final String DB_SSL_TRUSTSTORE_PASSWORD     = "truststore.password";
    private static final String DB_SSL_CERTIFICATE_FILE        = "db.ssl.certificateFile";
    private static final String JPA_DB_URL                     = "javax.persistence.jdbc.url";

    private RangerKMSDB   rangerKMSDB;
    private Configuration conf;
    private Method        updateDBSSLURLMethod;
    private Field         jpaPropertiesField;
    private File          tempKeystore;
    private File          tempTruststore;
    private File          tempCertificate;
    private Properties    originalSystemProperties;

    @BeforeEach
    public void setUp() throws Exception {
        conf = new Configuration();

        // Set basic database properties required for RangerKMSDB constructor
        conf.set(PROPERTY_PREFIX + DB_DIALECT, "org.eclipse.persistence.platform.database.H2Platform");
        conf.set(PROPERTY_PREFIX + DB_DRIVER, "org.h2.Driver");
        conf.set(PROPERTY_PREFIX + DB_URL, "jdbc:h2:mem:testdb");
        conf.set(PROPERTY_PREFIX + DB_USER, "test");
        conf.set(PROPERTY_PREFIX + DB_PASSWORD, "test");

        // Save original system properties
        originalSystemProperties = new Properties();
        originalSystemProperties.putAll(System.getProperties());

        // Create temporary files for testing
        createTempFiles();

        // Get private method and field using reflection
        updateDBSSLURLMethod = RangerKMSDB.class.getDeclaredMethod("updateDBSSLURL");
        updateDBSSLURLMethod.setAccessible(true);

        jpaPropertiesField = RangerKMSDB.class.getDeclaredField("jpaProperties");
        jpaPropertiesField.setAccessible(true);
    }

    @AfterEach
    public void tearDown() {
        // Restore original system properties
        System.setProperties(originalSystemProperties);

        // Clean up temporary files
        cleanupTempFiles();

        if (rangerKMSDB != null) {
            // Clean up any database connections if needed
        }
    }

    @Test
    public void testUpdateDBSSLURL_NullConfiguration() throws Exception {
        Configuration nullConf = null;
        rangerKMSDB = new RangerKMSDB(nullConf) {
            @Override
            public org.apache.ranger.kms.dao.DaoManager getDaoManager() {
                return null;
            }
        };

        // Should not throw exception
        assertDoesNotThrow(() -> updateDBSSLURLMethod.invoke(rangerKMSDB));
    }

    @Test
    public void testUpdateDBSSLURL_NoSSLEnabledProperty() throws Exception {
        // Don't set DB_SSL_ENABLED property
        createRangerKMSDBWithoutSSL();

        String originalUrl = conf.get(PROPERTY_PREFIX + DB_URL);
        updateDBSSLURLMethod.invoke(rangerKMSDB);

        // URL should remain unchanged
        assertEquals(originalUrl, conf.get(PROPERTY_PREFIX + DB_URL));
    }

    @Test
    public void testUpdateDBSSLURL_MySQLSSLEnabled_NoQueryParams() throws Exception {
        // Setup MySQL configuration
        conf.set(PROPERTY_PREFIX + DB_DIALECT, "mysql");
        conf.set(PROPERTY_PREFIX + DB_URL, "jdbc:mysql://localhost:3306/ranger");
        conf.set(PROPERTY_PREFIX + DB_SSL_ENABLED, "true");
        conf.set(PROPERTY_PREFIX + DB_SSL_REQUIRED, "true");
        conf.set(PROPERTY_PREFIX + DB_SSL_VerifyServerCertificate, "true");

        createRangerKMSDBWithoutSSL();
        updateDBSSLURLMethod.invoke(rangerKMSDB);

        String updatedUrl = conf.get(PROPERTY_PREFIX + DB_URL);
        assertTrue(updatedUrl.contains("useSSL=true"));
        assertTrue(updatedUrl.contains("requireSSL=true"));
        assertTrue(updatedUrl.contains("verifyServerCertificate=true"));

        @SuppressWarnings("unchecked")
        Map<String, String> jpaProperties = (Map<String, String>) jpaPropertiesField.get(rangerKMSDB);
        assertEquals(updatedUrl, jpaProperties.get(JPA_DB_URL));
    }

    @Test
    public void testUpdateDBSSLURL_MySQLSSLEnabled_WithQueryParams() throws Exception {
        // Setup MySQL configuration with existing query parameters
        conf.set(PROPERTY_PREFIX + DB_DIALECT, "mysql");
        conf.set(PROPERTY_PREFIX + DB_URL, "jdbc:mysql://localhost:3306/ranger?charset=utf8");
        conf.set(PROPERTY_PREFIX + DB_SSL_ENABLED, "true");

        createRangerKMSDBWithoutSSL();
        updateDBSSLURLMethod.invoke(rangerKMSDB);

        String updatedUrl = conf.get(PROPERTY_PREFIX + DB_URL);
        // Should not modify URL if it already has query parameters
        assertEquals("jdbc:mysql://localhost:3306/ranger?charset=utf8", updatedUrl);
    }

    @Test
    public void testUpdateDBSSLURL_MySQLSSLDisabled() throws Exception {
        // Setup MySQL configuration with SSL disabled
        conf.set(PROPERTY_PREFIX + DB_DIALECT, "mysql");
        conf.set(PROPERTY_PREFIX + DB_URL, "jdbc:mysql://localhost:3306/ranger");
        conf.set(PROPERTY_PREFIX + DB_SSL_ENABLED, "false");

        createRangerKMSDBWithoutSSL();
        updateDBSSLURLMethod.invoke(rangerKMSDB);

        String updatedUrl = conf.get(PROPERTY_PREFIX + DB_URL);
        assertTrue(updatedUrl.contains("useSSL=false"));
    }

    @Test
    public void testUpdateDBSSLURL_PostgreSQLSSLEnabled_WithCertificateFile() throws Exception {
        // Setup PostgreSQL configuration
        conf.set(PROPERTY_PREFIX + DB_DIALECT, "postgresql");
        conf.set(PROPERTY_PREFIX + DB_URL, "jdbc:postgresql://localhost:5432/ranger");
        conf.set(PROPERTY_PREFIX + DB_SSL_ENABLED, "true");
        conf.set(PROPERTY_PREFIX + DB_SSL_CERTIFICATE_FILE, tempCertificate.getAbsolutePath());

        createRangerKMSDBWithoutSSL();
        updateDBSSLURLMethod.invoke(rangerKMSDB);

        String updatedUrl = conf.get(PROPERTY_PREFIX + DB_URL);
        assertTrue(updatedUrl.contains("ssl=true"));
        assertTrue(updatedUrl.contains("sslmode=verify-full"));
        assertTrue(updatedUrl.contains("sslrootcert=" + tempCertificate.getAbsolutePath()));
    }

    @Test
    public void testUpdateDBSSLURL_PostgreSQLSSLEnabled_WithVerification_NoCertFile() throws Exception {
        // Setup PostgreSQL configuration
        conf.set(PROPERTY_PREFIX + DB_DIALECT, "postgresql");
        conf.set(PROPERTY_PREFIX + DB_URL, "jdbc:postgresql://localhost:5432/ranger");
        conf.set(PROPERTY_PREFIX + DB_SSL_ENABLED, "true");
        conf.set(PROPERTY_PREFIX + DB_SSL_VerifyServerCertificate, "true");

        createRangerKMSDBWithoutSSL();
        updateDBSSLURLMethod.invoke(rangerKMSDB);

        String updatedUrl = conf.get(PROPERTY_PREFIX + DB_URL);
        assertTrue(updatedUrl.contains("ssl=true"));
        assertTrue(updatedUrl.contains("sslmode=verify-full"));
        assertTrue(updatedUrl.contains("sslfactory=org.postgresql.ssl.DefaultJavaSSLFactory"));
    }

    @Test
    public void testUpdateDBSSLURL_PostgreSQLSSLEnabled_NoVerification() throws Exception {
        // Setup PostgreSQL configuration
        conf.set(PROPERTY_PREFIX + DB_DIALECT, "postgresql");
        conf.set(PROPERTY_PREFIX + DB_URL, "jdbc:postgresql://localhost:5432/ranger");
        conf.set(PROPERTY_PREFIX + DB_SSL_ENABLED, "true");
        conf.set(PROPERTY_PREFIX + DB_SSL_VerifyServerCertificate, "false");
        conf.set(PROPERTY_PREFIX + DB_SSL_REQUIRED, "false");

        createRangerKMSDBWithoutSSL();
        updateDBSSLURLMethod.invoke(rangerKMSDB);

        String updatedUrl = conf.get(PROPERTY_PREFIX + DB_URL);
        assertTrue(updatedUrl.contains("ssl=true"));
        assertFalse(updatedUrl.contains("sslmode=verify-full"));
    }

    @Test
    public void testUpdateDBSSLURL_PostgreSQLSSLDisabled() throws Exception {
        // Setup PostgreSQL configuration with SSL disabled
        conf.set(PROPERTY_PREFIX + DB_DIALECT, "postgresql");
        conf.set(PROPERTY_PREFIX + DB_URL, "jdbc:postgresql://localhost:5432/ranger");
        conf.set(PROPERTY_PREFIX + DB_SSL_ENABLED, "false");

        createRangerKMSDBWithoutSSL();
        String originalUrl = conf.get(PROPERTY_PREFIX + DB_URL);
        updateDBSSLURLMethod.invoke(rangerKMSDB);

        String updatedUrl = conf.get(PROPERTY_PREFIX + DB_URL);
        // PostgreSQL URL should not be modified when SSL is disabled
        assertEquals(originalUrl, updatedUrl);
    }

    @Test
    public void testUpdateDBSSLURL_OracleDatabase() throws Exception {
        // Setup Oracle configuration (should not modify URL)
        conf.set(PROPERTY_PREFIX + DB_DIALECT, "oracle");
        conf.set(PROPERTY_PREFIX + DB_URL, "jdbc:oracle:thin:@localhost:1521:ranger");
        conf.set(PROPERTY_PREFIX + DB_SSL_ENABLED, "true");

        createRangerKMSDBWithoutSSL();
        String originalUrl = conf.get(PROPERTY_PREFIX + DB_URL);
        updateDBSSLURLMethod.invoke(rangerKMSDB);

        String updatedUrl = conf.get(PROPERTY_PREFIX + DB_URL);
        // Oracle URL should not be modified
        assertEquals(originalUrl, updatedUrl);
    }

    @Test
    public void testUpdateDBSSLURL_KeystoreAndTruststoreSetup() throws Exception {
        // Setup MySQL configuration with SSL verification and keystore/truststore
        conf.set(PROPERTY_PREFIX + DB_DIALECT, "mysql");
        conf.set(PROPERTY_PREFIX + DB_URL, "jdbc:mysql://localhost:3306/ranger");
        conf.set(PROPERTY_PREFIX + DB_SSL_ENABLED, "true");
        conf.set(PROPERTY_PREFIX + DB_SSL_REQUIRED, "true");
        conf.set(PROPERTY_PREFIX + DB_SSL_VerifyServerCertificate, "true");
        conf.set(PROPERTY_PREFIX + DB_SSL_AUTH_TYPE, "2-way");
        conf.set(PROPERTY_PREFIX + DB_SSL_KEYSTORE, tempKeystore.getAbsolutePath());
        conf.set(PROPERTY_PREFIX + DB_SSL_KEYSTORE_PASSWORD, "keystore-password");
        conf.set(PROPERTY_PREFIX + DB_SSL_TRUSTSTORE, tempTruststore.getAbsolutePath());
        conf.set(PROPERTY_PREFIX + DB_SSL_TRUSTSTORE_PASSWORD, "truststore-password");

        createRangerKMSDBWithoutSSL();
        updateDBSSLURLMethod.invoke(rangerKMSDB);

        // Verify system properties are set
        assertEquals(tempKeystore.getAbsolutePath(), System.getProperty("javax.net.ssl.keyStore"));
        assertEquals("keystore-password", System.getProperty("javax.net.ssl.keyStorePassword"));
        assertEquals(tempTruststore.getAbsolutePath(), System.getProperty("javax.net.ssl.trustStore"));
        assertEquals("truststore-password", System.getProperty("javax.net.ssl.trustStorePassword"));
    }

    @Test
    public void testUpdateDBSSLURL_OneWaySSL() throws Exception {
        // Setup MySQL configuration with 1-way SSL (should not set keystore)
        conf.set(PROPERTY_PREFIX + DB_DIALECT, "mysql");
        conf.set(PROPERTY_PREFIX + DB_URL, "jdbc:mysql://localhost:3306/ranger");
        conf.set(PROPERTY_PREFIX + DB_SSL_ENABLED, "true");
        conf.set(PROPERTY_PREFIX + DB_SSL_REQUIRED, "true");
        conf.set(PROPERTY_PREFIX + DB_SSL_VerifyServerCertificate, "true");
        conf.set(PROPERTY_PREFIX + DB_SSL_AUTH_TYPE, "1-way");
        conf.set(PROPERTY_PREFIX + DB_SSL_KEYSTORE, tempKeystore.getAbsolutePath());
        conf.set(PROPERTY_PREFIX + DB_SSL_TRUSTSTORE, tempTruststore.getAbsolutePath());
        conf.set(PROPERTY_PREFIX + DB_SSL_TRUSTSTORE_PASSWORD, "truststore-password");

        createRangerKMSDBWithoutSSL();
        updateDBSSLURLMethod.invoke(rangerKMSDB);

        // Verify keystore is not set for 1-way SSL
        assertNull(System.getProperty("javax.net.ssl.keyStore"));
        // But truststore should still be set
        assertEquals(tempTruststore.getAbsolutePath(), System.getProperty("javax.net.ssl.trustStore"));
    }

    @Test
    public void testUpdateDBSSLURL_NonExistentKeystoreFile() throws Exception {
        // Setup configuration with non-existent keystore file
        conf.set(PROPERTY_PREFIX + DB_DIALECT, "mysql");
        conf.set(PROPERTY_PREFIX + DB_URL, "jdbc:mysql://localhost:3306/ranger");
        conf.set(PROPERTY_PREFIX + DB_SSL_ENABLED, "true");
        conf.set(PROPERTY_PREFIX + DB_SSL_REQUIRED, "true");
        conf.set(PROPERTY_PREFIX + DB_SSL_VerifyServerCertificate, "true");
        conf.set(PROPERTY_PREFIX + DB_SSL_KEYSTORE, "/non/existent/keystore.jks");
        conf.set(PROPERTY_PREFIX + DB_SSL_KEYSTORE_PASSWORD, "password");

        createRangerKMSDBWithoutSSL();
        updateDBSSLURLMethod.invoke(rangerKMSDB);

        // Should not set system property for non-existent file
        assertNull(System.getProperty("javax.net.ssl.keyStore"));
    }

    @Test
    public void testUpdateDBSSLURL_EmptyKeystoreProperty() throws Exception {
        // Setup configuration with empty keystore property
        conf.set(PROPERTY_PREFIX + DB_DIALECT, "mysql");
        conf.set(PROPERTY_PREFIX + DB_URL, "jdbc:mysql://localhost:3306/ranger");
        conf.set(PROPERTY_PREFIX + DB_SSL_ENABLED, "true");
        conf.set(PROPERTY_PREFIX + DB_SSL_REQUIRED, "true");
        conf.set(PROPERTY_PREFIX + DB_SSL_VerifyServerCertificate, "true");
        conf.set(PROPERTY_PREFIX + DB_SSL_KEYSTORE, "");

        createRangerKMSDBWithoutSSL();
        updateDBSSLURLMethod.invoke(rangerKMSDB);

        // Should not set system property for empty keystore
        assertNull(System.getProperty("javax.net.ssl.keyStore"));
    }

    @Test
    public void testUpdateDBSSLURL_VariousBooleanValues() throws Exception {
        // Test various boolean value formats
        String[] trueValues  = {"true", "TRUE", "True"};
        String[] falseValues = {"false", "FALSE", "False", "", null, "invalid"};

        for (String trueValue : trueValues) {
            conf = new Configuration();
            conf.set(PROPERTY_PREFIX + DB_DIALECT, "mysql");
            conf.set(PROPERTY_PREFIX + DB_URL, "jdbc:mysql://localhost:3306/ranger");
            conf.set(PROPERTY_PREFIX + DB_SSL_ENABLED, trueValue);

            createRangerKMSDBWithoutSSL();
            updateDBSSLURLMethod.invoke(rangerKMSDB);

            String updatedUrl = conf.get(PROPERTY_PREFIX + DB_URL);
            assertTrue(updatedUrl.contains("useSSL=true"),
                    "Failed for SSL enabled value: " + trueValue);
        }

        for (String falseValue : falseValues) {
            conf = new Configuration();
            conf.set(PROPERTY_PREFIX + DB_DIALECT, "mysql");
            conf.set(PROPERTY_PREFIX + DB_URL, "jdbc:mysql://localhost:3306/ranger");
            if (falseValue != null) {
                conf.set(PROPERTY_PREFIX + DB_SSL_ENABLED, falseValue);
            }

            createRangerKMSDBWithoutSSL();
            updateDBSSLURLMethod.invoke(rangerKMSDB);

            String updatedUrl = conf.get(PROPERTY_PREFIX + DB_URL);
            if (falseValue == null) {
                assertFalse(updatedUrl.contains("useSSL"),
                        "URL should not be modified when SSL property is null");
            } else {
                assertTrue(updatedUrl.contains("useSSL=false"),
                        "Failed for SSL enabled value: " + falseValue);
            }
        }
    }

    @Test
    public void testUpdateDBSSLURL_SQLServerDatabase() throws Exception {
        // Test SQL Server (should not modify URL for SSL)
        conf.set(PROPERTY_PREFIX + DB_DIALECT, "sqlserver");
        conf.set(PROPERTY_PREFIX + DB_URL, "jdbc:sqlserver://localhost:1433;database=ranger");
        conf.set(PROPERTY_PREFIX + DB_SSL_ENABLED, "true");

        createRangerKMSDBWithoutSSL();
        String originalUrl = conf.get(PROPERTY_PREFIX + DB_URL);
        updateDBSSLURLMethod.invoke(rangerKMSDB);

        String updatedUrl = conf.get(PROPERTY_PREFIX + DB_URL);
        // SQL Server URL should not be modified
        assertEquals(originalUrl, updatedUrl);
    }

    @Test
    public void testUpdateDBSSLURL_PostgreSQLSSLRequired() throws Exception {
        // Setup PostgreSQL configuration with SSL required
        conf.set(PROPERTY_PREFIX + DB_DIALECT, "postgresql");
        conf.set(PROPERTY_PREFIX + DB_URL, "jdbc:postgresql://localhost:5432/ranger");
        conf.set(PROPERTY_PREFIX + DB_SSL_ENABLED, "true");
        conf.set(PROPERTY_PREFIX + DB_SSL_REQUIRED, "true");

        createRangerKMSDBWithoutSSL();
        updateDBSSLURLMethod.invoke(rangerKMSDB);

        String updatedUrl = conf.get(PROPERTY_PREFIX + DB_URL);
        assertTrue(updatedUrl.contains("ssl=true"));
        assertTrue(updatedUrl.contains("sslmode=verify-full"));
        assertTrue(updatedUrl.contains("sslfactory=org.postgresql.ssl.DefaultJavaSSLFactory"));
    }

    @Test
    public void testUpdateDBSSLURL_ComplexScenario() throws Exception {
        // Test complex scenario with multiple properties set
        conf.set(PROPERTY_PREFIX + DB_DIALECT, "mysql");
        conf.set(PROPERTY_PREFIX + DB_URL, "jdbc:mysql://localhost:3306/ranger");
        conf.set(PROPERTY_PREFIX + DB_SSL_ENABLED, "true");
        conf.set(PROPERTY_PREFIX + DB_SSL_REQUIRED, "false");
        conf.set(PROPERTY_PREFIX + DB_SSL_VerifyServerCertificate, "false");
        conf.set(PROPERTY_PREFIX + DB_SSL_AUTH_TYPE, "2-way");
        conf.set(PROPERTY_PREFIX + DB_SSL_KEYSTORE, tempKeystore.getAbsolutePath());
        conf.set(PROPERTY_PREFIX + DB_SSL_KEYSTORE_PASSWORD, "keystore-pass");
        conf.set(PROPERTY_PREFIX + DB_SSL_TRUSTSTORE, tempTruststore.getAbsolutePath());
        conf.set(PROPERTY_PREFIX + DB_SSL_TRUSTSTORE_PASSWORD, "truststore-pass");

        createRangerKMSDBWithoutSSL();
        updateDBSSLURLMethod.invoke(rangerKMSDB);

        String updatedUrl = conf.get(PROPERTY_PREFIX + DB_URL);
        assertTrue(updatedUrl.contains("useSSL=true"));
        assertTrue(updatedUrl.contains("requireSSL=false"));
        assertTrue(updatedUrl.contains("verifyServerCertificate=false"));

        // System properties should not be set when verification is false
        assertNull(System.getProperty("javax.net.ssl.keyStore"));
        assertNull(System.getProperty("javax.net.ssl.trustStore"));
    }

    private void createTempFiles() throws IOException {
        tempKeystore    = File.createTempFile("test-keystore", ".jks");
        tempTruststore  = File.createTempFile("test-truststore", ".jks");
        tempCertificate = File.createTempFile("test-cert", ".pem");

        // Write some dummy content to make files readable
        Files.write(tempKeystore.toPath(), "dummy content".getBytes());
        Files.write(tempTruststore.toPath(), "dummy content".getBytes());
        Files.write(tempCertificate.toPath(), "dummy content".getBytes());
    }

    private void cleanupTempFiles() {
        if (tempKeystore != null && tempKeystore.exists()) {
            tempKeystore.delete();
        }
        if (tempTruststore != null && tempTruststore.exists()) {
            tempTruststore.delete();
        }
        if (tempCertificate != null && tempCertificate.exists()) {
            tempCertificate.delete();
        }
    }

    private void createRangerKMSDBWithoutSSL() {
        try {
            rangerKMSDB = new RangerKMSDB(conf) {
                // Override to prevent actual DB connection
                @Override
                public org.apache.ranger.kms.dao.DaoManager getDaoManager() {
                    return null;
                }
            };
        } catch (Exception e) {
            // Expected for some tests where DB connection fails
        }
    }
}
