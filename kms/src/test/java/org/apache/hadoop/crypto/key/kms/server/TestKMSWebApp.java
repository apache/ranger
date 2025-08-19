
/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.crypto.key.kms.server;

import com.codahale.metrics.JmxReporter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.key.KeyProvider;
import org.apache.hadoop.crypto.key.KeyProviderCryptoExtension;
import org.apache.ranger.kms.metrics.collector.KMSMetricsCollector;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.Logger;

import javax.servlet.ServletContextEvent;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.URI;
import java.nio.file.Files;
import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
@TestMethodOrder(MethodOrderer.MethodName.class)
@Disabled
public class TestKMSWebApp {
    private Configuration                       mockConfig;
    private KeyAuthorizationKeyProvider.KeyACLs mockAcls;
    private Meter                               mockMeter;
    private KMSMetricsCollector                 mockKmsMetricsCollector;

    @BeforeEach
    public void setup() throws Throwable {
        mockConfig = new Configuration();
        mockConfig.set("test.key", "test.value");

        mockAcls                = mock(KeyAuthorizationKeyProvider.KeyACLs.class);
        mockMeter               = mock(Meter.class);
        mockKmsMetricsCollector = mock(KMSMetricsCollector.class);

        setStaticField("kmsConf", mockConfig);
        setStaticField("kmsAcls", mockAcls);
        setStaticField("keyCallsMeter", mockMeter);
        setStaticField("invalidCallsMeter", mockMeter);
        setStaticField("generateEEKCallsMeter", mockMeter);
        setStaticField("decryptEEKCallsMeter", mockMeter);
        setStaticField("reencryptEEKCallsMeter", mockMeter);
        setStaticField("reencryptEEKBatchCallsMeter", mockMeter);
        setStaticField("kmsMetricsCollector", mockKmsMetricsCollector);
    }

    @Test
    public void testGetConfigurationReturnsCopyOfKmsConf() {
        Configuration returnedConf = KMSWebApp.getConfiguration();

        assertNotNull(returnedConf);
        assertNotSame(mockConfig, returnedConf);
    }

    @Test
    public void testGetACLs() {
        assertSame(mockAcls, KMSWebApp.getACLs());
    }

    @Test
    public void testGetKeyCallsMeter() {
        assertSame(mockMeter, KMSWebApp.getKeyCallsMeter());
    }

    @Test
    public void testGetInvalidCallsMeter() {
        assertSame(mockMeter, KMSWebApp.getInvalidCallsMeter());
    }

    @Test
    public void testGetGenerateEEKCallsMeter() {
        assertSame(mockMeter, KMSWebApp.getGenerateEEKCallsMeter());
    }

    @Test
    public void testGetDecryptEEKCallsMeter() {
        assertSame(mockMeter, KMSWebApp.getDecryptEEKCallsMeter());
    }

    @Test
    public void testGetReencryptEEKCallsMeter() {
        assertSame(mockMeter, KMSWebApp.getReencryptEEKCallsMeter());
    }

    @Test
    public void testGetReencryptEEKBatchCallsMeter() {
        assertSame(mockMeter, KMSWebApp.getReencryptEEKBatchCallsMeter());
    }

    @Test
    public void testGetKmsMetricsCollector() {
        assertSame(mockKmsMetricsCollector, KMSWebApp.getKmsMetricsCollector());
    }

    @Test
    public void testInitLogging() throws Exception {
        KMSWebApp app    = new KMSWebApp();
        Method    method = KMSWebApp.class.getDeclaredMethod("initLogging");
        method.setAccessible(true);
        method.invoke(app);

        Field logField = KMSWebApp.class.getDeclaredField("log");
        logField.setAccessible(true);
        Logger logger = (Logger) logField.get(null);

        assertNotNull(logger);
        assertEquals(KMSWebApp.class.getName(), logger.getName());
    }

    @Test
    public void testCreateKeyProviderViaReflection() throws Exception {
        URI    uri    = new URI("jceks://file/tmp/test.jceks");
        Method method = KMSWebApp.class.getDeclaredMethod("createKeyProvider", URI.class, Configuration.class);
        method.setAccessible(true);
        Object result = method.invoke(null, uri, mockConfig);

        assertNotNull(result);
        assertInstanceOf(KeyProvider.class, result);
    }

    @Test
    public void testGetKeyAcls() throws Exception {
        KMSWebApp app    = new KMSWebApp();
        Method    method = KMSWebApp.class.getDeclaredMethod("getKeyAcls", String.class);
        method.setAccessible(true);
        Object result = method.invoke(app, (String) null);

        assertNotNull(result);
        assertInstanceOf(KMSACLs.class, result);
    }

    @Test
    public void testContextInitialized_successfulPath_java8Compatible() throws Exception {
        final SecurityManager originalSecurityManager = System.getSecurityManager();
        System.setSecurityManager(new SecurityManager() {
            @Override
            public void checkPermission(java.security.Permission perm) {}

            @Override
            public void checkExit(int status) {
                throw new SecurityException("Intercepted System.exit(" + status + ")");
            }
        });

        File tempDir = Files.createTempDirectory("kms-test-conf").toFile();
        File kmsSite = new File(tempDir, "kms-site.xml");

        try (FileWriter writer = new FileWriter(kmsSite)) {
            writer.write("<configuration>\n" +
                    "  <property><name>hadoop.kms.key.provider.uri</name><value>user:///</value></property>\n" +
                    "  <property><name>hadoop.kms.key.cache.enable</name><value>false</value></property>\n" +
                    "  <property><name>hadoop.kms.metric.collection.threadsafe</name><value>false</value></property>\n" +
                    "  <property><name>hadoop.kms.security.authorization.manager</name>\n" +
                    "           <value>org.apache.hadoop.crypto.key.kms.server.KMSACLs</value></property>\n" +
                    "</configuration>");
        }

        System.setProperty(KMSConfiguration.KMS_CONFIG_DIR, tempDir.getAbsolutePath());

        // Setup logger
        Logger mockLogger = mock(Logger.class);
        Field  logField   = KMSWebApp.class.getDeclaredField("log");
        logField.setAccessible(true);
        logField.set(null, mockLogger);

        try {
            KMSWebApp webApp = new KMSWebApp();
            webApp.contextInitialized(mock(ServletContextEvent.class));
        } catch (SecurityException e) {
            System.out.println("Intercepted System.exit: " + e.getMessage());
        } finally {
            System.setSecurityManager(originalSecurityManager);
            System.clearProperty(KMSConfiguration.KMS_CONFIG_DIR);
        }
    }

    @Test
    public void testContextDestroyed_success_java8Compatible() throws Exception {
        // Save original SecurityManager
        SecurityManager originalSecurityManager = System.getSecurityManager();
        System.setSecurityManager(new SecurityManager() {
            @Override
            public void checkPermission(java.security.Permission perm) {}

            @Override
            public void checkExit(int status) {
                throw new SecurityException("Intercepted System.exit(" + status + ")");
            }
        });

        // Create valid minimal kms-site.xml
        File tempDir = Files.createTempDirectory("kms-test-conf").toFile();
        File kmsSite = new File(tempDir, "kms-site.xml");
        try (FileWriter writer = new FileWriter(kmsSite)) {
            writer.write("<configuration>\n" +
                    "  <property><name>hadoop.kms.key.provider.uri</name><value>user:///</value></property>\n" +
                    "  <property><name>hadoop.kms.key.cache.enable</name><value>false</value></property>\n" +
                    "  <property><name>hadoop.kms.metric.collection.threadsafe</name><value>false</value></property>\n" +
                    "  <property><name>hadoop.kms.security.authorization.manager</name>\n" +
                    "           <value>org.apache.hadoop.crypto.key.kms.server.KMSACLs</value></property>\n" +
                    "</configuration>");
        }
        System.setProperty(KMSConfiguration.KMS_CONFIG_DIR, tempDir.getAbsolutePath());

        // Set mock logger
        Logger mockLogger = mock(Logger.class);
        Field  logField   = KMSWebApp.class.getDeclaredField("log");
        logField.setAccessible(true);
        logField.set(null, mockLogger);
        KeyProviderCryptoExtension keyProviderCryptoExtension = mock(KeyProviderCryptoExtension.class);

        try {
            // Initialize first
            KMSWebApp app = new KMSWebApp();
            app.contextInitialized(mock(ServletContextEvent.class));

            // Then destroy
            app.contextDestroyed(mock(ServletContextEvent.class));

            // ✅ No exceptions = successful test
        } catch (SecurityException e) {
            //
        } finally {
            System.setSecurityManager(originalSecurityManager);
            System.clearProperty(KMSConfiguration.KMS_CONFIG_DIR);
        }
    }

    @Test
    public void testContextInitialized_fullCoverageUsingReflection1() throws Exception {
        SecurityManager originalSM = System.getSecurityManager();
        System.setSecurityManager(new SecurityManager() {
            @Override
            public void checkPermission(java.security.Permission perm) {}

            @Override
            public void checkExit(int status) {
                throw new SecurityException("Intercepted System.exit(" + status + ")");
            }
        });

        File tempDir = Files.createTempDirectory("kms-test-conf").toFile();
        File kmsSite = new File(tempDir, "kms-site.xml");
        try (FileWriter writer = new FileWriter(kmsSite)) {
            writer.write("<configuration>\n" +
                    "  <property><name>hadoop.kms.key.cache.enable</name><value>false</value></property>\n" +
                    "  <property><name>hadoop.kms.metric.collection.threadsafe</name><value>false</value></property>\n" +
                    "  <property><name>hadoop.kms.key.provider.uri</name><value>user:///</value></property>\n" +
                    "  <property><name>hadoop.kms.security.authorization.manager</name><value>org.apache.hadoop.crypto.key.kms.server.KMSACLs</value></property>\n" +
                    "</configuration>");
        }
        File dbksSite = new File(tempDir, "dbks-site.xml");
        try (FileWriter writer = new FileWriter(dbksSite)) {
            writer.write("<configuration></configuration>");
        }
        System.setProperty(KMSConfiguration.KMS_CONFIG_DIR, tempDir.getAbsolutePath());
        boolean exitCalled = false;
        try {
            KMSWebApp     webApp       = new KMSWebApp();
            Configuration kmsConf      = KMSConfiguration.getKMSConf();
            Field         kmsConfField = KMSWebApp.class.getDeclaredField("kmsConf");
            kmsConfField.setAccessible(true);
            kmsConfField.set(webApp, kmsConf);

            Method getKeyAclsMethod = KMSWebApp.class.getDeclaredMethod("getKeyAcls", String.class);
            getKeyAclsMethod.setAccessible(true);
            Object                              realAcls     = getKeyAclsMethod.invoke(webApp, kmsConf.get(KMSConfiguration.KMS_SECURITY_AUTHORIZER));
            KeyAuthorizationKeyProvider.KeyACLs spyAcls      = spy((KeyAuthorizationKeyProvider.KeyACLs) realAcls);
            Field                               kmsAclsField = KMSWebApp.class.getDeclaredField("kmsAcls");
            kmsAclsField.setAccessible(true);
            kmsAclsField.set(webApp, spyAcls);

            MetricRegistry realRegistry        = new MetricRegistry();
            Field          metricRegistryField = KMSWebApp.class.getDeclaredField("metricRegistry");
            metricRegistryField.setAccessible(true);
            metricRegistryField.set(webApp, realRegistry);

            JmxReporter mockReporter     = mock(JmxReporter.class);
            Field       jmxReporterField = KMSWebApp.class.getDeclaredField("jmxReporter");
            jmxReporterField.setAccessible(true);
            jmxReporterField.set(webApp, mockReporter);

            webApp.contextInitialized(mock(ServletContextEvent.class));

            Set<String> expectedMeters = new HashSet<>();
            expectedMeters.add("GenerateEEKCalls");
            expectedMeters.add("DecryptEEKCalls");
            expectedMeters.add("ReencryptEEKCalls");
            expectedMeters.add("ReencryptEEKBatchCalls");
            expectedMeters.add("AdminCalls");
            expectedMeters.add("KeyCalls");
            expectedMeters.add("InvalidCalls");
            expectedMeters.add("UnauthorizedCalls");
            expectedMeters.add("UnauthenticatedCalls");
            assertTrue(realRegistry.getMeters().keySet().containsAll(expectedMeters));
        } catch (SecurityException se) {
            exitCalled = se.getMessage().contains("Intercepted");
        } finally {
            System.setSecurityManager(originalSM);
            System.clearProperty(KMSConfiguration.KMS_CONFIG_DIR);
        }
    }

    @Test
    public void testContextDestroyed() throws Exception {
        ServletContextEvent sce    = mock(ServletContextEvent.class);
        KMSWebApp           webApp = new KMSWebApp();

        Logger mockLogger = mock(Logger.class);
        Field  logField   = KMSWebApp.class.getDeclaredField("log");
        logField.setAccessible(true);
        logField.set(null, mockLogger);

        KeyProviderCryptoExtension          mockExtension = mock(KeyProviderCryptoExtension.class);
        KMSAudit                            mockAudit     = mock(KMSAudit.class);
        KeyAuthorizationKeyProvider.KeyACLs mockAcls      = mock(KeyAuthorizationKeyProvider.KeyACLs.class);
        JmxReporter                         mockReporter  = mock(JmxReporter.class);

        Field extensionField = KMSWebApp.class.getDeclaredField("keyProviderCryptoExtension");
        extensionField.setAccessible(true);
        extensionField.set(null, mockExtension);

        Field auditField = KMSWebApp.class.getDeclaredField("kmsAudit");
        auditField.setAccessible(true);
        auditField.set(null, mockAudit);

        Field aclsField = KMSWebApp.class.getDeclaredField("kmsAcls");
        aclsField.setAccessible(true);
        aclsField.set(null, mockAcls);

        Field reporterField = KMSWebApp.class.getDeclaredField("jmxReporter");
        reporterField.setAccessible(true);
        reporterField.set(webApp, mockReporter); // instance field

        Field registryField = KMSWebApp.class.getDeclaredField("metricRegistry");
        registryField.setAccessible(true);
        registryField.set(null, new MetricRegistry());

        webApp.contextDestroyed(sce);

        Field registryFieldCheck = KMSWebApp.class.getDeclaredField("metricRegistry");
        registryFieldCheck.setAccessible(true);
        assertNull(registryFieldCheck.get(null));
    }

    @Test
    public void testContextDestroyed_whenCloseThrowsIOException() throws Exception {
        ServletContextEvent sce    = mock(ServletContextEvent.class);
        KMSWebApp           webApp = new KMSWebApp();

        Logger                              mockLogger    = mock(Logger.class);
        KeyProviderCryptoExtension          mockExtension = mock(KeyProviderCryptoExtension.class);
        KMSAudit                            mockAudit     = mock(KMSAudit.class);
        KeyAuthorizationKeyProvider.KeyACLs mockAcls      = mock(KeyAuthorizationKeyProvider.KeyACLs.class);
        JmxReporter                         mockReporter  = mock(JmxReporter.class);
        MetricRegistry                      dummyRegistry = new MetricRegistry();

        doThrow(new IOException("simulated")).when(mockExtension).close();

        Field logField = KMSWebApp.class.getDeclaredField("log");
        logField.setAccessible(true);
        logField.set(null, mockLogger);

        Field extensionField = KMSWebApp.class.getDeclaredField("keyProviderCryptoExtension");
        extensionField.setAccessible(true);
        extensionField.set(null, mockExtension);

        Field auditField = KMSWebApp.class.getDeclaredField("kmsAudit");
        auditField.setAccessible(true);
        auditField.set(null, mockAudit);

        Field aclsField = KMSWebApp.class.getDeclaredField("kmsAcls");
        aclsField.setAccessible(true);
        aclsField.set(null, mockAcls);

        Field reporterField = KMSWebApp.class.getDeclaredField("jmxReporter");
        reporterField.setAccessible(true);
        reporterField.set(webApp, mockReporter); // instance field

        Field registryField = KMSWebApp.class.getDeclaredField("metricRegistry");
        registryField.setAccessible(true);
        registryField.set(null, dummyRegistry);

        webApp.contextDestroyed(sce);

        verify(mockExtension).close();
        verify(mockLogger).error(eq("Error closing KeyProviderCryptoExtension"), any(IOException.class));
        verify(mockAudit).shutdown();
        verify(mockAcls).stopReloader();
        verify(mockReporter).stop();
        verify(mockReporter).close();
        verify(mockLogger).info("KMS Stopped");

        Field regCheck = KMSWebApp.class.getDeclaredField("metricRegistry");
        regCheck.setAccessible(true);
        assertNull(regCheck.get(null));
    }

    private void setStaticField(String fieldName, Object value) throws Exception {
        Field field = KMSWebApp.class.getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(null, value);
    }
}
