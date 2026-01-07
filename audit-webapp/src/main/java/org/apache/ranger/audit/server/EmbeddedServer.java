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

package org.apache.ranger.audit.server;

import org.apache.catalina.Context;
import org.apache.catalina.LifecycleException;
import org.apache.catalina.WebResourceRoot;
import org.apache.catalina.connector.Connector;
import org.apache.catalina.core.StandardContext;
import org.apache.catalina.startup.Tomcat;
import org.apache.catalina.valves.AccessLogValve;
import org.apache.catalina.webresources.StandardRoot;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.SecureClientLogin;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.alias.BouncyCastleFipsKeyStoreProvider;
import org.apache.hadoop.security.alias.CredentialProvider;
import org.apache.hadoop.security.alias.CredentialProviderFactory;
import org.apache.hadoop.security.alias.JavaKeyStoreProvider;
import org.apache.hadoop.security.alias.LocalBouncyCastleFipsKeyStoreProvider;
import org.apache.ranger.audit.provider.MiscUtil;
import org.apache.ranger.audit.utils.AuditServerLogFormatter;
import org.apache.ranger.authorization.hadoop.utils.RangerCredentialProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.util.Date;
import java.util.List;
import java.util.Map;

public class EmbeddedServer {
    private static final Logger LOG                                    = LoggerFactory.getLogger(EmbeddedServer.class);

    private static final String ACCESS_LOG_PREFIX                      = "accesslog.prefix";
    private static final String ACCESS_LOG_DATE_FORMAT                 = "accesslog.dateformat";
    private static final String ACCESS_LOG_PATTERN                     = "accesslog.pattern";
    private static final String ACCESS_LOG_ROTATE_MAX_DAYS             = "accesslog.rotate.max.days";
    private static final String DEFAULT_LOG_DIR                        = "/tmp";
    private static final int    DEFAULT_HTTP_PORT                      = 6081;
    private static final int    DEFAULT_HTTPS_PORT                     = -1;
    private static final int    DEFAULT_SHUTDOWN_PORT                  = 6185;
    private static final String DEFAULT_SHUTDOWN_COMMAND               = "SHUTDOWN";
    private static final String DEFAULT_WEBAPPS_ROOT_FOLDER            = "webapps";
    private static final String DEFAULT_ENABLED_PROTOCOLS              = "TLSv1.2";
    public  static final String DEFAULT_NAME_RULE                      = "DEFAULT";
    private static final String RANGER_KEYSTORE_FILE_TYPE_DEFAULT      = "jks";
    private static final String RANGER_TRUSTSTORE_FILE_TYPE_DEFAULT    = "jks";
    private static final String RANGER_SSL_CONTEXT_ALGO_TYPE           = "TLSv1.2";
    private static final String RANGER_SSL_KEYMANAGER_ALGO_TYPE        = KeyManagerFactory.getDefaultAlgorithm();
    private static final String RANGER_SSL_TRUSTMANAGER_ALGO_TYPE      = TrustManagerFactory.getDefaultAlgorithm();
    public static final  String KEYSTORE_FILE_TYPE_DEFAULT             = KeyStore.getDefaultType();
    public static final  String TRUSTSTORE_FILE_TYPE_DEFAULT           = KeyStore.getDefaultType();

    private final Configuration configuration;
    private final String        appName;
    private final String        configPrefix;
    private volatile Tomcat     server;
    private volatile Context    webappContext;

    public EmbeddedServer(Configuration configuration, String appName, String configPrefix) {
        LOG.info("==> EmbeddedServer(appName={}, configPrefix={})", appName, configPrefix);

        this.configuration = new Configuration(configuration);
        this.appName       = appName;
        this.configPrefix  = configPrefix;

        LOG.info("<== EmbeddedServer(appName={}, configPrefix={}", appName, configPrefix);
    }

    public static void main(String[] args) {
        Configuration config       = AuditConfig.getInstance();
        String        appName      = AuditServerConstants.AUDIT_SERVER_APPNAME;
        String        configPrefix = AuditServerConstants.AUDIT_SERVER_PROP_PREFIX;

        new EmbeddedServer(config, appName, configPrefix).start();
    }

    @SuppressWarnings("deprecation")
    public void start() {
        LOG.info("==> EmbeddedServer.start(appName={})", appName);

        SSLContext sslContext = getSSLContext();

        if (sslContext != null) {
            SSLContext.setDefault(sslContext);
        }

        this.server = new Tomcat();

        // Add shutdown hook for graceful cleanup
        addShutdownHook();

        String  logDir          = getConfig("log.dir", DEFAULT_LOG_DIR);
        String  hostName        = getConfig("host");
        int     serverPort      = getIntConfig("http.port", DEFAULT_HTTP_PORT);
        int     sslPort         = getIntConfig("https.port", DEFAULT_HTTPS_PORT);
        int     shutdownPort    = getIntConfig("shutdown.port", DEFAULT_SHUTDOWN_PORT);
        String  shutdownCommand = getConfig("shutdown.command", DEFAULT_SHUTDOWN_COMMAND);
        boolean isHttpsEnabled  = getBooleanConfig("https.attrib.ssl.enabled", false);
        boolean ajpEnabled      = getBooleanConfig("ajp.enabled", false);

        server.setHostname(hostName);
        server.setPort(serverPort);
        server.getServer().setPort(shutdownPort);
        server.getServer().setShutdown(shutdownCommand);

        if (ajpEnabled) {
            Connector ajpConnector = new Connector("org.apache.coyote.ajp.AjpNioProtocol");

            ajpConnector.setPort(serverPort);
            ajpConnector.setProperty("protocol", "AJP/1.3");

            server.getService().addConnector(ajpConnector);

            // Making this as a default connector
            server.setConnector(ajpConnector);

            LOG.info("Created AJP Connector");
        } else if (isHttpsEnabled && sslPort > 0) {
            String clientAuth       = getConfig("https.attrib.clientAuth", "false");
            String providerPath     = getConfig("credential.provider.path");
            String keyAlias         = getConfig("https.attrib.keystore.credential.alias", "keyStoreCredentialAlias");
            String keystorePass     = null;
            String enabledProtocols = getConfig("https.attrib.ssl.enabled.protocols", DEFAULT_ENABLED_PROTOCOLS);
            String ciphers          = getConfig("tomcat.ciphers");

            if (StringUtils.equalsIgnoreCase(clientAuth, "false")) {
                clientAuth = getConfig("https.attrib.client.auth", "want");
            }

            if (providerPath != null && keyAlias != null) {
                keystorePass = getDecryptedString(providerPath.trim(), keyAlias.trim(), getConfig("keystore.file.type", KEYSTORE_FILE_TYPE_DEFAULT));

                if (StringUtils.isBlank(keystorePass) || StringUtils.equalsIgnoreCase(keystorePass.trim(), "none")) {
                    keystorePass = getConfig("https.attrib.keystore.pass");
                }
            }

            Connector ssl                 = new Connector();
            String    sslKeystoreKeyAlias = getConfig("https.attrib.keystore.keyalias", "");
            ssl.setPort(sslPort);
            ssl.setSecure(true);
            ssl.setScheme("https");
            ssl.setAttribute("SSLEnabled", "true");
            ssl.setAttribute("sslProtocol", getConfig("https.attrib.ssl.protocol", "TLSv1.2"));
            ssl.setAttribute("clientAuth", clientAuth);
            if (StringUtils.isNotBlank(sslKeystoreKeyAlias)) {
                ssl.setAttribute("keyAlias", sslKeystoreKeyAlias);
            }
            ssl.setAttribute("keystorePass", keystorePass);
            ssl.setAttribute("keystoreFile", getKeystoreFile());
            ssl.setAttribute("sslEnabledProtocols", enabledProtocols);
            ssl.setAttribute("keystoreType", getConfig("audit.keystore.file.type", KEYSTORE_FILE_TYPE_DEFAULT));
            ssl.setAttribute("truststoreType", getConfig("audit.truststore.file.type", TRUSTSTORE_FILE_TYPE_DEFAULT));

            if (StringUtils.isNotBlank(ciphers)) {
                ssl.setAttribute("ciphers", ciphers);
            }

            server.getService().addConnector(ssl);

            //
            // Making this as a default connector
            //
            server.setConnector(ssl);
        }

        updateHttpConnectorAttribConfig(server);

        File logDirectory = new File(logDir);

        if (!logDirectory.exists()) {
            boolean created = logDirectory.mkdirs();
            if (!created) {
                LOG.error("Failed to create log directory: {}", logDir);
                throw new RuntimeException("Failed to create log directory: " + logDir);
            }
        }

        String logPattern = getConfig(ACCESS_LOG_PATTERN, "%h %l %u %t \"%r\" %s %b");

        AccessLogValve valve = new AccessLogValve();

        valve.setRotatable(true);
        valve.setAsyncSupported(true);
        valve.setBuffered(false);
        valve.setEnabled(true);
        valve.setPrefix(getConfig(ACCESS_LOG_PREFIX, "access_log-" + hostName + "-"));
        valve.setFileDateFormat(getConfig(ACCESS_LOG_DATE_FORMAT, "yyyy-MM-dd.HH"));
        valve.setDirectory(logDirectory.getAbsolutePath());
        valve.setSuffix(".log");
        valve.setPattern(logPattern);
        valve.setMaxDays(getIntConfig(ACCESS_LOG_ROTATE_MAX_DAYS, 15));

        server.getHost().getPipeline().addValve(valve);

        try {
            String webappDir = getConfig("webapp.dir");

            if (StringUtils.isBlank(webappDir)) {
                LOG.error("Tomcat Server failed to start: {}.webapp.dir is not set", configPrefix);

                System.exit(1);
            }

            String webContextName = getConfig("contextName", "");

            if (StringUtils.isBlank(webContextName)) {
                webContextName = "";
            } else if (!webContextName.startsWith("/")) {
                LOG.info("Context Name [{}] is being loaded as [ /{}]", webContextName, webContextName);

                webContextName = "/" + webContextName;
            }

            File wad = new File(webappDir);

            if (wad.isDirectory()) {
                LOG.info("Webapp dir={}, webAppName={}", webappDir, webContextName);
            } else if (wad.isFile()) {
                File webAppDir = new File(DEFAULT_WEBAPPS_ROOT_FOLDER);

                if (!webAppDir.exists()) {
                    boolean created = webAppDir.mkdirs();
                    if (!created) {
                        LOG.error("Failed to create webapp directory: {}", DEFAULT_WEBAPPS_ROOT_FOLDER);
                        throw new RuntimeException("Failed to create webapp directory: " + DEFAULT_WEBAPPS_ROOT_FOLDER);
                    }
                }

                LOG.info("Webapp file={}, webAppName={}", webappDir, webContextName);
            }

            LOG.info("Adding webapp [{}] = path [{}] .....", webContextName, webappDir);

            this.webappContext = server.addWebapp(webContextName, new File(webappDir).getAbsolutePath());
            if (webappContext instanceof StandardContext) {
                boolean         allowLinking    = getBooleanConfig("allow.linking", true);
                StandardContext standardContext = (StandardContext) webappContext;
                String          workDirPath     = getConfig("tomcat.work.dir", "");
                if (!workDirPath.isEmpty() && new File(workDirPath).exists()) {
                    standardContext.setWorkDir(workDirPath);
                } else {
                    LOG.debug("Skipping to set tomcat server work directory {} as it is blank or directory does not exist.", workDirPath);
                }
                WebResourceRoot resRoot = new StandardRoot(standardContext);
                webappContext.setResources(resRoot);
                webappContext.getResources().setAllowLinking(allowLinking);
                LOG.debug("Tomcat Configuration - allowLinking=[{}]", allowLinking);
            } else {
                LOG.error("Tomcat Context [{}] is either NULL OR it's NOT instanceof StandardContext", webappContext);
            }

            webappContext.init();

            LOG.info("Finished init of webapp [{}] = path [{}].", webContextName, webappDir);
        } catch (LifecycleException lce) {
            LOG.error("Tomcat Server failed to start webapp", lce);

            lce.printStackTrace();
        }

        String authenticationType = getConfig(AuditServerConstants.PROP_HADOOP_AUTHENTICATION_TYPE);

        if (StringUtils.equalsIgnoreCase(authenticationType, AuditServerConstants.PROP_HADOOP_AUTH_TYPE_KERBEROS)) {
            String keyTab    = getConfig(AuditServerConstants.PROP_AUDIT_SERVICE_KEYTAB);
            String principal = null;
            String nameRules = getConfig(AuditServerConstants.PROP_HADOOP_KERBEROS_NAME_RULES, DEFAULT_NAME_RULE);

            try {
                principal = SecureClientLogin.getPrincipal(getConfig(AuditServerConstants.PROP_AUDIT_SERVICE_PRINCIPAL), hostName);
            } catch (IOException excp) {
                LOG.warn("Failed to get principal" + excp);
            }

            if (SecureClientLogin.isKerberosCredentialExists(principal, keyTab)) {
                try {
                    AuditServerLogFormatter.builder("Kerberos Login Attempt")
                            .add("Principal", principal)
                            .add("Keytab", keyTab)
                            .add("Name rules", nameRules)
                            .logInfo(LOG);

                    Configuration hadoopConf = new Configuration();
                    hadoopConf.set(AuditServerConstants.PROP_HADOOP_AUTHENTICATION_TYPE, AuditServerConstants.PROP_HADOOP_AUTH_TYPE_KERBEROS);
                    hadoopConf.set(AuditServerConstants.PROP_HADOOP_KERBEROS_NAME_RULES, nameRules);
                    UserGroupInformation.setConfiguration(hadoopConf);
                    UserGroupInformation.loginUserFromKeytab(principal, keyTab);
                    UserGroupInformation currentUGI = UserGroupInformation.getLoginUser();

                    AuditServerLogFormatter.builder("Kerberos Login Successful")
                            .add("UGI Username", currentUGI.getUserName())
                            .add("UGI Real User", currentUGI.getUserName())
                            .add("Authentication Method", currentUGI.getAuthenticationMethod().toString())
                            .add("Has Kerberos Credentials", currentUGI.hasKerberosCredentials())
                            .logInfo(LOG);

                    MiscUtil.setUGILoginUser(currentUGI, null);

                    LOG.info("Starting Server using kerberos credential");
                    startServer(server);
                } catch (Exception excp) {
                    LOG.error("Tomcat Server failed to start", excp);
                }
            } else {
                LOG.warn("Kerberos principal={} not found in keytab={}. Starting server in non-kerberos mode", principal, keyTab);

                startServer(server);
            }
        } else {
            LOG.info("Starting server in non-kerberos mode");

            startServer(server);
        }

        LOG.info("<== EmbeddedServer.start(appName={})", appName);
    }

    public String getDecryptedString(String crendentialProviderPath, String alias, String storeType) {
        String ret = null;

        if (StringUtils.isNotBlank(crendentialProviderPath) && StringUtils.isNotBlank(alias)) {
            try {
                Configuration conf             = new Configuration();
                String        prefixJceks      = JavaKeyStoreProvider.SCHEME_NAME + "://file";
                String        prefixLocalJceks = "localjceks://file";
                String        prefixBcfks      = BouncyCastleFipsKeyStoreProvider.SCHEME_NAME + "://file";
                String        prefixLocalBcfks = LocalBouncyCastleFipsKeyStoreProvider.SCHEME_NAME + "://file";

                String lowerPath = crendentialProviderPath.toLowerCase();
                if (lowerPath.startsWith(prefixJceks.toLowerCase()) || lowerPath.startsWith(prefixLocalJceks.toLowerCase()) || lowerPath.startsWith(prefixBcfks.toLowerCase()) || lowerPath.startsWith(prefixLocalBcfks.toLowerCase())) {
                    conf.set(CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH, crendentialProviderPath);
                } else {
                    if (crendentialProviderPath.startsWith("/")) {
                        if ("bcfks".equalsIgnoreCase(storeType)) {
                            conf.set(CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH, crendentialProviderPath);
                        } else {
                            conf.set(CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH, prefixJceks + crendentialProviderPath);
                        }
                    } else {
                        conf.set(CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH, prefixJceks + "/" + crendentialProviderPath);
                    }
                }

                List<CredentialProvider> providers = CredentialProviderFactory.getProviders(conf);

                LOG.debug("crendentialProviderPath={} alias={} storeType={}", crendentialProviderPath, alias, storeType);
                LOG.debug("List of CredentialProvider = {}", providers.toString());

                for (CredentialProvider provider : providers) {
                    //System.out.println("Credential Provider :" + provider);
                    List<String> aliasesList = provider.getAliases();

                    if (CollectionUtils.isNotEmpty(aliasesList) && aliasesList.contains(alias)) {
                        CredentialProvider.CredentialEntry credEntry = provider.getCredentialEntry(alias);
                        char[]                             pass      = credEntry.getCredential();

                        if (pass != null && pass.length > 0) {
                            ret = String.valueOf(pass);

                            break;
                        }
                    }
                }
            } catch (Exception ex) {
                LOG.error("CredentialReader failed while decrypting provided string. Reason: {}", ex.toString());

                ret = null;
            }
        }

        LOG.debug("getDecryptedString() : ret = {}", ret);

        return ret;
    }

    protected long getLongConfig(String key, long defaultValue) {
        long   ret    = defaultValue;
        String retStr = getConfig(key);

        try {
            if (retStr != null) {
                ret = Long.parseLong(retStr);
            }
        } catch (Exception err) {
            LOG.warn(retStr + " can't be parsed to long. Reason: {}", err.toString());
        }

        return ret;
    }

    private void startServer(final Tomcat server) {
        LOG.info("==> EmbeddedServer.startServer(appName={})", appName);

        try {
            server.start();

            server.getServer().await();

            shutdownServer();
        } catch (LifecycleException e) {
            LOG.error("Tomcat Server failed to start", e);

            e.printStackTrace();
        } catch (Exception e) {
            LOG.error("Tomcat Server failed to start", e);

            e.printStackTrace();
        }

        LOG.info("<== EmbeddedServer.startServer(appName={})", appName);
    }

    private void addShutdownHook() {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOG.info("==> EmbeddedServer shutdown hook triggered");
            try {
                gracefulShutdown();
            } catch (Exception e) {
                LOG.error("Error during graceful shutdown", e);
            }
            LOG.info("<== EmbeddedServer shutdown hook completed");
        }, "EmbeddedServer-ShutdownHook"));
    }

    private void gracefulShutdown() {
        LOG.info("==> EmbeddedServer.gracefulShutdown()");

        // Instead of trying to manually shutdown Spring context, let's rely on Tomcat's
        // normal webapp lifecycle which will trigger ContextLoaderListener.contextDestroyed()
        // and call @PreDestroy methods automatically

        if (server != null) {
            try {
                LOG.info("Initiating Tomcat server stop to trigger webapp lifecycle shutdown");
                server.stop();
                LOG.info("Tomcat server stop completed - Spring @PreDestroy methods should have been called");
            } catch (Exception e) {
                LOG.error("Error stopping Tomcat server during graceful shutdown", e);

                // Fallback: try to stop the webapp context directly
                if (webappContext != null) {
                    try {
                        LOG.info("Fallback: stopping webapp context directly");
                        webappContext.stop();
                        LOG.info("Webapp context stopped");
                    } catch (Exception contextE) {
                        LOG.error("Error stopping webapp context", contextE);
                    }
                }
            }
        }

        // Give some time for Spring components to shutdown through normal lifecycle
        try {
            Thread.sleep(3000); // Increased to 3 seconds to allow for proper cleanup
        } catch (InterruptedException e) {
            LOG.warn("Interrupted while waiting for Spring shutdown", e);
            Thread.currentThread().interrupt();
        }

        LOG.info("<== EmbeddedServer.gracefulShutdown()");
    }

    private void shutdownServer() {
        LOG.info("==> EmbeddedServer.shutdownServer(appName={})", appName);

        int timeWaitForShutdownInSeconds = getIntConfig("waitTimeForForceShutdownInSeconds", 0);

        if (timeWaitForShutdownInSeconds > 0) {
            long endTime = System.currentTimeMillis() + (timeWaitForShutdownInSeconds * 1000L);

            LOG.info("Will wait for all threads to shutdown gracefully. Final shutdown Time: {}", new Date(endTime));

            while (System.currentTimeMillis() < endTime) {
                int activeCount = Thread.activeCount();

                if (activeCount == 0) {
                    LOG.info("Number of active threads = {}", activeCount);

                    break;
                } else {
                    LOG.info("Number of active threads = {}. Waiting for all threads to shutdown ...", activeCount);

                    try {
                        Thread.sleep(5000L);
                    } catch (InterruptedException e) {
                        LOG.warn("shutdownServer process is interrupted with exception", e);

                        break;
                    }
                }
            }
        }

        LOG.info("Shutting down the Server.");

        LOG.info("<== EmbeddedServer.shutdownServer(appName={})", appName);

        System.exit(0);
    }

    @SuppressWarnings("deprecation")
    private void updateHttpConnectorAttribConfig(Tomcat server) {
        server.getConnector().setAllowTrace(getBooleanConfig("http.connector.attrib.allowTrace", false));
        server.getConnector().setAsyncTimeout(getLongConfig("http.connector.attrib.asyncTimeout", 10000));
        server.getConnector().setEnableLookups(getBooleanConfig("http.connector.attrib.enableLookups", false));
        server.getConnector().setMaxParameterCount(getIntConfig("http.connector.attrib.maxParameterCount", 10000));
        server.getConnector().setMaxPostSize(getIntConfig("http.connector.attrib.maxPostSize", 2097152));
        server.getConnector().setMaxSavePostSize(getIntConfig("http.connector.attrib.maxSavePostSize", 4096));
        server.getConnector().setParseBodyMethods(getConfig("http.connector.attrib.methods", "POST"));
        server.getConnector().setURIEncoding(getConfig("http.connector.attrib.URIEncoding", "UTF-8"));
        server.getConnector().setProperty("acceptCount", getConfig("http.connector.attrib.acceptCount", "1024"));
        server.getConnector().setXpoweredBy(false);
        server.getConnector().setAttribute("server", getConfig("servername", "Audit Server"));

        int     maxThreads              = getIntConfig("http.connector.attrib.maxThreads", 2000);
        int     maxKeepAliveRequests    = getIntConfig("http.connector.attrib.maxKeepAliveRequests", 1000);
        int     minSpareThreads         = getIntConfig("http.connector.attrib.minSpareThreads", (int) (maxThreads * 0.8));  // (default 80% of maxThreads)
        boolean prestartminSpareThreads = getBooleanConfig("http.connector.attrib.prestartminSpareThreads", true);

        server.getConnector().setAttribute("maxThreads", maxThreads);
        server.getConnector().setAttribute("maxKeepAliveRequests", maxKeepAliveRequests);
        server.getConnector().setAttribute("minSpareThreads", minSpareThreads);
        server.getConnector().setAttribute("prestartminSpareThreads", prestartminSpareThreads);
        server.getConnector().setProperty("sendReasonPhrase", getConfig(configPrefix + "http.connector.property.sendReasonPhrase", "true"));

        for (Map.Entry<String, String> entry : configuration) {
            String key   = entry.getKey();
            String value = entry.getValue();

            if (key != null && key.startsWith(configPrefix + "http.connector.property.")) {
                String property = key.replace(configPrefix + "http.connector.property.", "");

                server.getConnector().setProperty(property, value);

                LOG.info("{}:{}", property, server.getConnector().getProperty(property));
            }
        }
    }

    private SSLContext getSSLContext() {
        SSLContext     ret    = null;
        KeyManager[]   kmList = getKeyManagers();
        TrustManager[] tmList = getTrustManagers();

        if (tmList != null) {
            try {
                ret = SSLContext.getInstance(RANGER_SSL_CONTEXT_ALGO_TYPE);

                ret.init(kmList, tmList, new SecureRandom());
            } catch (NoSuchAlgorithmException e) {
                LOG.error("SSL algorithm is not available in the environment", e);
            } catch (KeyManagementException e) {
                LOG.error("Unable to initials the SSLContext", e);
            }
        }

        return ret;
    }

    private KeyManager[] getKeyManagers() {
        KeyManager[] ret                    = null;
        String       keyStoreFile           = getConfig("keystore.file");
        String       keyStoreAlias          = getConfig("keystore.alias");
        String       credentialProviderPath = getConfig("credential.provider.path");
        String       keyStoreFilepwd        = getCredential(credentialProviderPath, keyStoreAlias);

        if (StringUtils.isNotBlank(keyStoreFile) && StringUtils.isNotBlank(keyStoreFilepwd)) {
            InputStream in = null;

            try {
                in = getFileInputStream(keyStoreFile);

                if (in != null) {
                    KeyStore keyStore = KeyStore.getInstance(RANGER_KEYSTORE_FILE_TYPE_DEFAULT);

                    keyStore.load(in, keyStoreFilepwd.toCharArray());

                    KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance(RANGER_SSL_KEYMANAGER_ALGO_TYPE);

                    keyManagerFactory.init(keyStore, keyStoreFilepwd.toCharArray());

                    ret = keyManagerFactory.getKeyManagers();
                } else {
                    LOG.error("Unable to obtain keystore from file [{}]", keyStoreFile);
                }
            } catch (KeyStoreException e) {
                LOG.error("Unable to obtain from KeyStore", e);
            } catch (NoSuchAlgorithmException e) {
                LOG.error("SSL algorithm is NOT available in the environment", e);
            } catch (CertificateException e) {
                LOG.error("Unable to obtain the requested certification", e);
            } catch (FileNotFoundException e) {
                LOG.error("Unable to find the necessary SSL Keystore Files", e);
            } catch (IOException e) {
                LOG.error("Unable to read the necessary SSL Keystore Files", e);
            } catch (UnrecoverableKeyException e) {
                LOG.error("Unable to recover the key from keystore", e);
            } finally {
                close(in, keyStoreFile);
            }
        }

        return ret;
    }

    private TrustManager[] getTrustManagers() {
        TrustManager[] ret                    = null;
        String         truststoreFile         = getConfig("truststore.file");
        String         truststoreAlias        = getConfig("truststore.alias");
        String         credentialProviderPath = getConfig("credential.provider.path");
        String         trustStoreFilepwd      = getCredential(credentialProviderPath, truststoreAlias);

        if (StringUtils.isNotBlank(truststoreFile) && StringUtils.isNotBlank(trustStoreFilepwd)) {
            InputStream in = null;

            try {
                in = getFileInputStream(truststoreFile);

                if (in != null) {
                    KeyStore trustStore = KeyStore.getInstance(RANGER_TRUSTSTORE_FILE_TYPE_DEFAULT);

                    trustStore.load(in, trustStoreFilepwd.toCharArray());

                    TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(RANGER_SSL_TRUSTMANAGER_ALGO_TYPE);

                    trustManagerFactory.init(trustStore);

                    ret = trustManagerFactory.getTrustManagers();
                } else {
                    LOG.error("Unable to obtain truststore from file [{}]", truststoreFile);
                }
            } catch (KeyStoreException e) {
                LOG.error("Unable to obtain from KeyStore", e);
            } catch (NoSuchAlgorithmException e) {
                LOG.error("SSL algorithm is NOT available in the environment", e);
            } catch (CertificateException e) {
                LOG.error("Unable to obtain the requested certification", e);
            } catch (FileNotFoundException e) {
                LOG.error("Unable to find the necessary SSL TrustStore File:{}", truststoreFile, e);
            } catch (IOException e) {
                LOG.error("Unable to read the necessary SSL TrustStore Files:{}", truststoreFile, e);
            } finally {
                close(in, truststoreFile);
            }
        }

        return ret;
    }

    private String getKeystoreFile() {
        String ret = getConfig("https.attrib.keystore.file");

        if (StringUtils.isBlank(ret)) {
            // new property not configured, lets use the old property
            ret = getConfig("https.attrib.keystore.file");
        }

        return ret;
    }

    private String getCredential(String url, String alias) {
        return RangerCredentialProvider.getInstance().getCredentialString(url, alias);
    }

    private String getConfig(String key) {
        String propertyWithPrefix = configPrefix + key;
        String value              = configuration.get(propertyWithPrefix);
        if (value == null) { // config-with-prefix not found; look at System properties
            value = System.getProperty(propertyWithPrefix);
            if (value == null) { // config-with-prefix not found in System properties; look for config-without-prefix
                value = configuration.get(key);
                if (value == null) { // config-without-prefix not found; look at System properties
                    value = System.getProperty(key);
                }
            }
        }

        return value;
    }

    private String getConfig(String key, String defaultValue) {
        String ret = getConfig(key);

        if (ret == null) {
            ret = defaultValue;
        }

        return ret;
    }

    private int getIntConfig(String key, int defaultValue) {
        int    ret      = defaultValue;
        String strValue = getConfig(key);

        try {
            if (strValue != null) {
                ret = Integer.parseInt(strValue);
            }
        } catch (Exception err) {
            LOG.warn(strValue + " can't be parsed to int. Reason: {}", err.toString());
        }

        return ret;
    }

    private boolean getBooleanConfig(String key, boolean defaultValue) {
        boolean ret      = defaultValue;
        String  strValue = getConfig(key);

        if (StringUtils.isNotBlank(strValue)) {
            ret = Boolean.valueOf(strValue);
        }

        return ret;
    }

    private InputStream getFileInputStream(String fileName) throws IOException {
        InputStream ret = null;

        if (StringUtils.isNotEmpty(fileName)) {
            File f = new File(fileName);

            if (f.exists()) {
                ret = new FileInputStream(f);
            } else {
                ret = ClassLoader.getSystemResourceAsStream(fileName);
            }
        }

        return ret;
    }

    private void close(InputStream str, String filename) {
        if (str != null) {
            try {
                str.close();
            } catch (IOException excp) {
                LOG.error("Error while closing file: [{}]", filename, excp);
            }
        }
    }
}
