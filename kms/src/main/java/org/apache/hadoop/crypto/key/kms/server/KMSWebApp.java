/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
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
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.key.CachingKeyProvider;
import org.apache.hadoop.crypto.key.KeyProvider;
import org.apache.hadoop.crypto.key.KeyProviderCryptoExtension;
import org.apache.hadoop.crypto.key.KeyProviderFactory;
import org.apache.hadoop.crypto.key.kms.server.KeyAuthorizationKeyProvider.KeyACLs;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.ShutdownHookManager;
import org.apache.hadoop.util.VersionInfo;
import org.apache.ranger.kms.metrics.KMSMetricWrapper;
import org.apache.ranger.kms.metrics.collector.KMSMetricsCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.bridge.SLF4JBridgeHandler;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

import java.io.IOException;
import java.net.URI;
import java.util.ServiceLoader;

@InterfaceAudience.Private
public class KMSWebApp implements ServletContextListener {
    private static Logger log;

    public static final String HADOOP_KMS_METRIC_COLLECTION_THREADSAFE = "hadoop.kms.metric.collection.threadsafe";

    private static final String METRICS_PREFIX              = "hadoop.kms.";
    private static final String ADMIN_CALLS_METER           = METRICS_PREFIX + "admin.calls.meter";
    private static final String KEY_CALLS_METER             = METRICS_PREFIX + "key.calls.meter";
    private static final String INVALID_CALLS_METER         = METRICS_PREFIX + "invalid.calls.meter";
    private static final String UNAUTHORIZED_CALLS_METER    = METRICS_PREFIX + "unauthorized.calls.meter";
    private static final String UNAUTHENTICATED_CALLS_METER = METRICS_PREFIX + "unauthenticated.calls.meter";
    private static final String GENERATE_EEK_METER          = METRICS_PREFIX + "generate_eek.calls.meter";
    private static final String DECRYPT_EEK_METER           = METRICS_PREFIX + "decrypt_eek.calls.meter";
    private static final String REENCRYPT_EEK_METER         = METRICS_PREFIX + "reencrypt_eek.calls.meter";
    private static final String REENCRYPT_EEK_BATCH_METER   = METRICS_PREFIX + "reencrypt_eek_batch.calls.meter";

    private static MetricRegistry             metricRegistry;
    private static Configuration              kmsConf;
    private static KeyACLs                    kmsAcls;
    private static Meter                      adminCallsMeter;
    private static Meter                      keyCallsMeter;
    private static Meter                      unauthorizedCallsMeter;
    private static Meter                      unauthenticatedCallsMeter;
    private static Meter                      decryptEEKCallsMeter;
    private static Meter                      reencryptEEKCallsMeter;
    private static Meter                      reencryptEEKBatchCallsMeter;
    private static Meter                      generateEEKCallsMeter;
    private static Meter                      invalidCallsMeter;
    private static KMSAudit                   kmsAudit;
    private static KeyProviderCryptoExtension keyProviderCryptoExtension;
    private static KMSMetricsCollector        kmsMetricsCollector;

    private static boolean isMetricsCollectionThreadsafe;
    private JmxReporter jmxReporter;

    public static Configuration getConfiguration() {
        return new Configuration(kmsConf);
    }

    public static KeyACLs getACLs() {
        return kmsAcls;
    }

    public static Meter getAdminCallsMeter() {
        return adminCallsMeter;
    }

    public static Meter getKeyCallsMeter() {
        return keyCallsMeter;
    }

    public static Meter getInvalidCallsMeter() {
        return invalidCallsMeter;
    }

    public static Meter getGenerateEEKCallsMeter() {
        return generateEEKCallsMeter;
    }

    public static Meter getDecryptEEKCallsMeter() {
        return decryptEEKCallsMeter;
    }

    public static Meter getReencryptEEKCallsMeter() {
        return reencryptEEKCallsMeter;
    }

    public static Meter getReencryptEEKBatchCallsMeter() {
        return reencryptEEKBatchCallsMeter;
    }

    public static Meter getUnauthorizedCallsMeter() {
        return unauthorizedCallsMeter;
    }

    public static Meter getUnauthenticatedCallsMeter() {
        return unauthenticatedCallsMeter;
    }

    public static KeyProviderCryptoExtension getKeyProvider() {
        return keyProviderCryptoExtension;
    }

    public static KMSAudit getKMSAudit() {
        return kmsAudit;
    }

    public static boolean isMetricCollectionThreadSafe() {
        return isMetricsCollectionThreadsafe;
    }

    public static KMSMetricsCollector getKmsMetricsCollector() {
        return kmsMetricsCollector;
    }

    @Override
    public void contextInitialized(ServletContextEvent sce) {
        try {
            String confDir = System.getProperty(KMSConfiguration.KMS_CONFIG_DIR);

            if (confDir == null) {
                throw new RuntimeException("System property '" + KMSConfiguration.KMS_CONFIG_DIR + "' not defined");
            }

            kmsConf = KMSConfiguration.getKMSConf();

            initLogging();

            UserGroupInformation.setConfiguration(kmsConf);

            log.info("-------------------------------------------------------------");
            log.info("  Java runtime version : {}", System.getProperty("java.runtime.version"));
            log.info("  KMS Hadoop Version: {}", VersionInfo.getVersion());
            log.info("-------------------------------------------------------------");

            kmsAcls = getKeyAcls(kmsConf.get(KMSConfiguration.KMS_SECURITY_AUTHORIZER));

            kmsAcls.startReloader();

            metricRegistry = new MetricRegistry();
            jmxReporter    = JmxReporter.forRegistry(metricRegistry).build();

            jmxReporter.start();

            generateEEKCallsMeter       = metricRegistry.register(GENERATE_EEK_METER, new Meter());
            decryptEEKCallsMeter        = metricRegistry.register(DECRYPT_EEK_METER, new Meter());
            reencryptEEKCallsMeter      = metricRegistry.register(REENCRYPT_EEK_METER, new Meter());
            reencryptEEKBatchCallsMeter = metricRegistry.register(REENCRYPT_EEK_BATCH_METER, new Meter());
            adminCallsMeter             = metricRegistry.register(ADMIN_CALLS_METER, new Meter());
            keyCallsMeter               = metricRegistry.register(KEY_CALLS_METER, new Meter());
            invalidCallsMeter           = metricRegistry.register(INVALID_CALLS_METER, new Meter());
            unauthorizedCallsMeter      = metricRegistry.register(UNAUTHORIZED_CALLS_METER, new Meter());
            unauthenticatedCallsMeter   = metricRegistry.register(UNAUTHENTICATED_CALLS_METER, new Meter());

            kmsAudit = new KMSAudit(kmsConf);

            isMetricsCollectionThreadsafe = Boolean.valueOf(kmsConf.get(HADOOP_KMS_METRIC_COLLECTION_THREADSAFE, "false"));
            KMSMetricWrapper kmsMetricWrapper = KMSMetricWrapper.getInstance(isMetricCollectionThreadSafe());

            kmsMetricsCollector = kmsMetricWrapper.getKmsMetricsCollector();

            // intializing the KeyProvider
            String providerString = kmsConf.get(KMSConfiguration.KEY_PROVIDER_URI);

            if (providerString == null) {
                throw new IllegalStateException("No KeyProvider has been defined");
            }

            log.info("------------------ Ranger KMSWebApp---------------------");
            log.info("provider string = {}", providerString);
            log.info("URI = {} scheme = {}", new URI(providerString), new URI(providerString).getScheme());
            log.info("kmsconf size= {} kms classname={}", kmsConf.size(), kmsConf.getClass().getName());
            log.info("----------------Instantiating key provider ---------------");

            KeyProvider keyProvider = createKeyProvider(new URI(providerString), kmsConf);

            Preconditions.checkNotNull(keyProvider, String.format("No KeyProvider has been initialized, please check whether %s '%s' is configured correctly in kms-site.xml.", KMSConfiguration.KEY_PROVIDER_URI, providerString));

            log.info("keyProvider = {}", keyProvider);

            if (kmsConf.getBoolean(KMSConfiguration.KEY_CACHE_ENABLE, KMSConfiguration.KEY_CACHE_ENABLE_DEFAULT)) {
                long keyTimeOutMillis     = kmsConf.getLong(KMSConfiguration.KEY_CACHE_TIMEOUT_KEY, KMSConfiguration.KEY_CACHE_TIMEOUT_DEFAULT);
                long currKeyTimeOutMillis = kmsConf.getLong(KMSConfiguration.CURR_KEY_CACHE_TIMEOUT_KEY, KMSConfiguration.CURR_KEY_CACHE_TIMEOUT_DEFAULT);

                keyProvider = new CachingKeyProvider(keyProvider, keyTimeOutMillis, currKeyTimeOutMillis);
            }

            log.info("Initialized KeyProvider {}", keyProvider);

            keyProviderCryptoExtension = KeyProviderCryptoExtension.createKeyProviderCryptoExtension(keyProvider);
            keyProviderCryptoExtension = new EagerKeyGeneratorKeyProviderCryptoExtension(kmsConf, keyProviderCryptoExtension);

            if (kmsConf.getBoolean(KMSConfiguration.KEY_AUTHORIZATION_ENABLE, KMSConfiguration.KEY_AUTHORIZATION_ENABLE_DEFAULT)) {
                keyProviderCryptoExtension = new KeyAuthorizationKeyProvider(keyProviderCryptoExtension, kmsAcls);
            }

            log.info("Initialized KeyProviderCryptoExtension {}", keyProviderCryptoExtension);
            log.info("Default key bitlength is {}", kmsConf.getInt(KeyProvider.DEFAULT_BITLENGTH_NAME, KeyProvider.DEFAULT_BITLENGTH));
            log.info("Ranger KMS Started");

            // Adding shutdown hook to flush in-memory metric to a file.
            ShutdownHookManager.get().addShutdownHook(() -> KMSMetricWrapper.getInstance(isMetricCollectionThreadSafe()).writeJsonMetricsToFile(), 10);
        } catch (Throwable ex) {
            System.out.println();
            System.out.println("ERROR: Hadoop KMS could not be started");
            System.out.println();
            System.out.println("REASON: " + ex);
            System.out.println();
            System.out.println("Stacktrace:");
            System.out.println("---------------------------------------------------");
            ex.printStackTrace(System.out);
            System.out.println("---------------------------------------------------");
            System.out.println();

            System.exit(1);
        }
    }

    @Override
    public void contextDestroyed(ServletContextEvent sce) {
        try {
            keyProviderCryptoExtension.close();
        } catch (IOException ioe) {
            log.error("Error closing KeyProviderCryptoExtension", ioe);
        }

        kmsAudit.shutdown();
        kmsAcls.stopReloader();
        jmxReporter.stop();
        jmxReporter.close();

        metricRegistry = null;

        log.info("KMS Stopped");
    }

    private void initLogging() {
        log = LoggerFactory.getLogger(KMSWebApp.class);
    }

    /**
     * @see org.apache.hadoop.crypto.key.KeyProviderFactory
     *
     * Code here to ensure KeyProvideFactory subclasses in ews/webapp/ can be loaded.
     * The hadoop-common.jar in ews/lib can only load subclasses in ews/lib.
     * This is due to the limitation of ClassLoader mechanism of java/tomcat.
     */
    private static KeyProvider createKeyProvider(URI uri, Configuration conf) throws IOException {
        ServiceLoader<KeyProviderFactory> serviceLoader = ServiceLoader.load(KeyProviderFactory.class);
        KeyProvider                       kp            = null;

        for (KeyProviderFactory factory : serviceLoader) {
            kp = factory.createProvider(uri, conf);

            if (kp != null) {
                break;
            }
        }

        return kp;
    }

    @SuppressWarnings("unchecked")
    private KeyACLs getKeyAcls(String clsStr) throws IOException {
        KeyACLs keyAcl = null;

        try {
            Class<? extends KeyACLs> cls = null;

            if (clsStr == null || clsStr.trim().isEmpty()) {
                cls = KMSACLs.class;
            } else {
                Class<?> configClass = Class.forName(clsStr);

                if (!KeyACLs.class.isAssignableFrom(configClass)) {
                    throw new RuntimeException(clsStr + " should implement KeyACLs");
                }

                cls = (Class<? extends KeyACLs>) configClass;
            }

            if (cls != null) {
                keyAcl = ReflectionUtils.newInstance(cls, kmsConf);
            }
        } catch (Exception e) {
            log.error("Unable to getAcls with an exception", e);

            throw new IOException(e.getMessage());
        }

        return keyAcl;
    }

    static {
        SLF4JBridgeHandler.removeHandlersForRootLogger();
        SLF4JBridgeHandler.install();
    }
}
