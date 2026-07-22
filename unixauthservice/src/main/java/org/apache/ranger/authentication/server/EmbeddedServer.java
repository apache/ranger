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

package org.apache.ranger.authentication.server;

import org.apache.catalina.Context;
import org.apache.catalina.LifecycleException;
import org.apache.catalina.WebResourceRoot;
import org.apache.catalina.connector.Connector;
import org.apache.catalina.core.StandardContext;
import org.apache.catalina.startup.Tomcat;
import org.apache.catalina.valves.AccessLogValve;
import org.apache.catalina.valves.ErrorReportValve;
import org.apache.catalina.webresources.StandardRoot;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.SecureClientLogin;
import org.apache.hadoop.security.alias.BouncyCastleFipsKeyStoreProvider;
import org.apache.hadoop.security.alias.CredentialProvider;
import org.apache.hadoop.security.alias.CredentialProviderFactory;
import org.apache.hadoop.security.alias.JavaKeyStoreProvider;
import org.apache.hadoop.security.alias.LocalBouncyCastleFipsKeyStoreProvider;
import org.apache.ranger.authorization.hadoop.utils.RangerCredentialProvider;
import org.apache.ranger.unixusersync.config.UserGroupSyncConfig;
import org.apache.tomcat.util.net.SSLHostConfig;
import org.apache.tomcat.util.scan.StandardJarScanner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.security.auth.Subject;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.PrivilegedAction;
import java.security.SecureRandom;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class EmbeddedServer {
    public static final  String RANGER_KERBEROS_KEYTAB            = "kerberos.keytab";
    public static final  String RANGER_KERBEROS_PRINCIPAL         = "kerberos.principal";
    public static final  String HADOOP_KERBEROS_NAME_RULES        = "hadoop.security.auth_to_local";
    public static final  String HADOOP_AUTHENTICATION_TYPE        = "hadoop.security.authentication";
    public static final  String HADOOP_AUTH_TYPE_KERBEROS         = "kerberos";
    public static final  String DEFAULT_NAME_RULE                 = "DEFAULT";
    public static final  String KEYSTORE_FILE_TYPE_DEFAULT        = KeyStore.getDefaultType();
    public static final  String TRUSTSTORE_FILE_TYPE_DEFAULT      = KeyStore.getDefaultType();
    private static final Logger LOG = LoggerFactory.getLogger(EmbeddedServer.class);
    private static final String RANGER_SSL_CONTEXT_ALGO_TYPE      = "TLS";
    private static final String RANGER_SSL_KEYMANAGER_ALGO_TYPE   = KeyManagerFactory.getDefaultAlgorithm();
    private static final String RANGER_SSL_TRUSTMANAGER_ALGO_TYPE = TrustManagerFactory.getDefaultAlgorithm();
    private static final String ACCESS_LOG_PREFIX                 = "accesslog.prefix";
    private static final String ACCESS_LOG_DATE_FORMAT            = "accesslog.dateformat";
    private static final String ACCESS_LOG_PATTERN                = "accesslog.pattern";
    private static final String ACCESS_LOG_ROTATE_MAX_DAYS        = "accesslog.rotate.max.days";
    private static final int    DEFAULT_HTTP_PORT                 = 8280;
    private static final int    DEFAULT_HTTPS_PORT                = 8283;
    private static final int    DEFAULT_SHUTDOWN_PORT             = 8285;
    private static final String DEFAULT_SHUTDOWN_COMMAND          = "SHUTDOWN";
    private static final String DEFAULT_WEBAPPS_ROOT_FOLDER       = "webapps";
    private static final String DEFAULT_ENABLED_PROTOCOLS         = "TLSv1.2";
    private static final String DEFAULT_SSL_PROTOCOL              = "TLS";
    private final Configuration configuration;
    private final String        appName;
    private final String        configPrefix;
    private       String        keyStoreFileType;
    private       String        trustStoreFileType;

    private final UserGroupSyncConfig config = UserGroupSyncConfig.getInstance();
    private       Properties          prop   = new Properties();

    public EmbeddedServer(String appName, String configPrefix) {
        LOG.info("==> EmbeddedServer(appName={}, configPrefix={})", appName, configPrefix);

        this.configuration = config.getConfig();
        this.appName       = appName;
        this.configPrefix  = configPrefix;
        prop               = config.getProperties();

        LOG.info("<== EmbeddedServer(appName={}, configPrefix={}", appName, configPrefix);
    }

    public static void main(String[] args) {
        String appName      = "ranger-usersync";
        String configPrefix = "ranger.usersync.";

        new EmbeddedServer(appName, configPrefix).start();
    }

    @SuppressWarnings("deprecation")
    public void start() {
        LOG.info("==> EmbeddedServer.start(appName={})", appName);

        keyStoreFileType   = getProperty("ranger.usersync.keystore.file.type", KEYSTORE_FILE_TYPE_DEFAULT);
        trustStoreFileType = getProperty("ranger.usersync.truststore.file.type", TRUSTSTORE_FILE_TYPE_DEFAULT);

        SSLContext sslContext = getSSLContext();

        if (sslContext != null) {
            SSLContext.setDefault(sslContext);
        }

        final Tomcat server = new Tomcat();

        String  logDir          = getProperty("log.dir");
        String  hostName        = getProperty("service.host");
        int     serverPort      = getIntProperty("service.http.port", DEFAULT_HTTP_PORT);
        int     sslPort         = getIntProperty("service.https.port", DEFAULT_HTTPS_PORT);
        int     shutdownPort    = getIntProperty("service.shutdown.port", DEFAULT_SHUTDOWN_PORT);
        String  shutdownCommand = getProperty("service.shutdown.command", DEFAULT_SHUTDOWN_COMMAND);
        boolean isHttpsEnabled  = getBooleanProperty("service.https.attrib.ssl.enabled", false);
        boolean ajpEnabled      = getBooleanProperty("ajp.enabled", false);

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
            String clientAuth       = getProperty("service.https.attrib.clientAuth", "false");
            String providerPath     = getProperty("credential.provider.path");
            String keyAlias         = getProperty("service.https.attrib.keystore.credential.alias", "keyStoreCredentialAlias");
            String keystorePass     = null;
            String enabledProtocols = getProperty("service.https.attrib.ssl.enabled.protocols", DEFAULT_ENABLED_PROTOCOLS);
            String ciphers          = getProperty("tomcat.ciphers");

            if (StringUtils.equalsIgnoreCase(clientAuth, "false")) {
                clientAuth = getProperty("service.https.attrib.client.auth", "want");
            }

            if (providerPath != null && keyAlias != null) {
                keystorePass = getDecryptedString(providerPath.trim(), keyAlias.trim(), keyStoreFileType);
                if (StringUtils.isBlank(keystorePass) || StringUtils.equalsIgnoreCase(keystorePass.trim(), "none")) {
                    keystorePass = getProperty("service.https.attrib.keystore.pass");
                }
            }

            Connector ssl                 = new Connector();
            String    sslKeystoreKeyAlias = getProperty("service.https.attrib.keystore.keyalias", "");
            ssl.setPort(sslPort);
            ssl.setSecure(true);
            ssl.setScheme("https");
            ssl.setAttribute("SSLEnabled", "true");
            ssl.setAttribute("sslProtocol", getProperty("service.https.attrib.ssl.protocol", DEFAULT_SSL_PROTOCOL));
            ssl.setAttribute("clientAuth", clientAuth);
            if (StringUtils.isNotBlank(sslKeystoreKeyAlias)) {
                ssl.setAttribute("keyAlias", sslKeystoreKeyAlias);
            }
            ssl.setAttribute("keystorePass", keystorePass);
            ssl.setAttribute("keystoreFile", getKeystoreFile());
            ssl.setAttribute("sslEnabledProtocols", enabledProtocols);
            ssl.setAttribute("keystoreType", keyStoreFileType);
            ssl.setAttribute("truststoreType", trustStoreFileType);

            if (StringUtils.isNotBlank(ciphers)) {
                ssl.setAttribute("ciphers", ciphers);
                SSLHostConfig[] configs = ssl.findSslHostConfigs();
                if (configs != null) {
                    for (SSLHostConfig hostConfig : configs) {
                        if (hostConfig != null) {
                            hostConfig.setCipherSuites(ciphers);
                        }
                    }
                }
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
            logDirectory.mkdirs();
        }

        String logPattern = getProperty(ACCESS_LOG_PATTERN, "%h %l %u %t \"%r\" %s %b");

        AccessLogValve valve = new AccessLogValve();

        valve.setRotatable(true);
        valve.setAsyncSupported(true);
        valve.setBuffered(false);
        valve.setEnabled(true);
        valve.setPrefix(getProperty(ACCESS_LOG_PREFIX, "access_log-" + hostName + "-"));
        valve.setFileDateFormat(getProperty(ACCESS_LOG_DATE_FORMAT, "yyyy-MM-dd.HH"));
        valve.setDirectory(logDirectory.getAbsolutePath());
        valve.setSuffix(".log");
        valve.setPattern(logPattern);
        valve.setMaxDays(getIntProperty(ACCESS_LOG_ROTATE_MAX_DAYS, 15));

        server.getHost().getPipeline().addValve(valve);

        ErrorReportValve errorReportValve = new ErrorReportValve();
        boolean          showServerinfo   = getBooleanProperty("ranger.valve.errorreportvalve.showserverinfo", false);
        boolean          showReport       = getBooleanProperty("ranger.valve.errorreportvalve.showreport", false);

        errorReportValve.setShowServerInfo(showServerinfo);
        errorReportValve.setShowReport(showReport);

        server.getHost().getPipeline().addValve(errorReportValve);

        try {
            String webappDir = getProperty("webapp.dir");
            LOG.info("==> EmbeddedServer.start(webappDir={})", webappDir);
            if (StringUtils.isBlank(webappDir)) {
                LOG.error("Tomcat Server failed to start: {}.webapp.dir is not set", configPrefix);

                System.exit(1);
            }

            String webContextName = getProperty("contextName", "");

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
                    webAppDir.mkdirs();
                }

                LOG.info("Webapp file={}, webAppName={}", webappDir, webContextName);
            }

            LOG.info("Adding webapp [{}] = path [{}] .....", webContextName, webappDir);

            Context webappCtx = server.addWebapp(webContextName, new File(webappDir).getAbsolutePath());
            if (webappCtx instanceof StandardContext) {
                boolean         allowLinking    = getBooleanProperty("allow.linking", true);
                StandardContext standardContext = (StandardContext) webappCtx;
                String          workDirPath     = getProperty("tomcat.work.dir", "");
                if (!workDirPath.isEmpty() && new File(workDirPath).exists()) {
                    standardContext.setWorkDir(workDirPath);
                } else {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Skipping to set tomcat server work directory, '" + workDirPath +
                                "', as it is blank or directory does not exist.");
                    }
                }
                WebResourceRoot resRoot = new StandardRoot(standardContext);
                webappCtx.setResources(resRoot);
                webappCtx.getResources().setAllowLinking(allowLinking);
                StandardJarScanner scanner = new StandardJarScanner();
                scanner.setScanManifest(false);
                webappCtx.setJarScanner(scanner);
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Tomcat Configuration - allowLinking=[{}]", allowLinking);
                }
            } else {
                LOG.error("Tomcat Context [{}] is either NULL OR it's NOT instanceof StandardContext", webappCtx);
            }

            webappCtx.init();

            LOG.info("Finished init of webapp [{}] = path [{}].", webContextName + webappDir);
        } catch (LifecycleException lce) {
            LOG.error("Tomcat Server failed to start webapp", lce);

            lce.printStackTrace();
        }

        String authenticationType = getProperty(HADOOP_AUTHENTICATION_TYPE);

        if (StringUtils.equalsIgnoreCase(authenticationType, HADOOP_AUTH_TYPE_KERBEROS)) {
            String keyTab    = getProperty(RANGER_KERBEROS_KEYTAB);
            String principal = null;
            String nameRules = getProperty(HADOOP_KERBEROS_NAME_RULES, DEFAULT_NAME_RULE);

            try {
                principal = SecureClientLogin.getPrincipal(getProperty(RANGER_KERBEROS_PRINCIPAL), hostName);
            } catch (IOException excp) {
                LOG.warn("Failed to get principal" + excp);
            }
            if (SecureClientLogin.isKerberosCredentialExists(principal, keyTab)) {
                try {
                    LOG.info("Logging in with Kerberos: principal={}, keytab={}, nameRules={}", principal, keyTab, nameRules);

                    Subject sub = SecureClientLogin.loginUserFromKeytab(principal, keyTab, nameRules);

                    Subject.doAs(sub, new PrivilegedAction<Void>() {
                        @Override
                        public Void run() {
                            LOG.info("Starting Server using kerberos credential");

                            startServer(server);

                            return null;
                        }
                    });
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

                if (StringUtils.startsWithIgnoreCase(crendentialProviderPath, prefixJceks) ||
                        StringUtils.startsWithIgnoreCase(crendentialProviderPath, prefixLocalJceks) ||
                        StringUtils.startsWithIgnoreCase(crendentialProviderPath, prefixBcfks) ||
                        StringUtils.startsWithIgnoreCase(crendentialProviderPath, prefixLocalBcfks)) {
                    conf.set(CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH, crendentialProviderPath);
                } else {
                    if (crendentialProviderPath.startsWith("/")) {
                        if (StringUtils.equalsIgnoreCase(storeType, "bcfks")) {
                            conf.set(CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH, prefixLocalBcfks + crendentialProviderPath);
                        } else {
                            conf.set(CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH, prefixJceks + crendentialProviderPath);
                        }
                    } else {
                        conf.set(CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH, prefixJceks + "/" + crendentialProviderPath);
                    }
                }

                List<CredentialProvider> providers = CredentialProviderFactory.getProviders(conf);
                if (LOG.isDebugEnabled()) {
                    LOG.debug("crendentialProviderPath=" + crendentialProviderPath + " alias=" + alias + " storeType=" + storeType);
                    LOG.debug("List of CredentialProvider = " + providers);
                }

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
                LOG.error("CredentialReader failed while decrypting provided string. Reason: " + ex);

                ret = null;
            }
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("getDecryptedString() : ret = " + ret);
        }

        return ret;
    }

    public String getProperty(String aPropertyName) {
        String propertyWithPrefix = configPrefix + aPropertyName;
        String value              = prop.getProperty(propertyWithPrefix);

        if (value == null) { // config-with-prefix not found; look at System properties
            value = System.getProperty(propertyWithPrefix);

            if (value == null) { // config-with-prefix not found in System properties; look for config-without-prefix
                value = prop.getProperty(aPropertyName);

                if (value == null) { // config-without-prefix not found; look at System properties
                    value = System.getProperty(aPropertyName);
                }
            }
        }
        return value;
    }

    public String getProperty(String aPropertyName, String aDefaultValue) {
        String ret = getProperty(aPropertyName);
        if (ret == null) {
            ret = aDefaultValue;
        }
        return ret;
    }

    public boolean getBooleanProperty(String aPropertyName, boolean aDefaultValue) {
        boolean ret = aDefaultValue;
        String  val = getProperty(aPropertyName);

        if (StringUtils.isNotBlank(val)) {
            ret = Boolean.valueOf(val);
        }

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
            LOG.warn(retStr + " can't be parsed to long. Reason: " + err);
        }

        return ret;
    }

    protected long getLongProperty(String key, long defaultValue) {
        long   ret    = defaultValue;
        String retStr = getProperty(key);

        try {
            if (retStr != null) {
                ret = Long.parseLong(retStr);
            }
        } catch (Exception err) {
            LOG.warn(retStr + " can't be parsed to long. Reason: " + err);
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

    private void shutdownServer() {
        LOG.info("==> EmbeddedServer.shutdownServer(appName={})", appName);

        int timeWaitForShutdownInSeconds = getIntProperty("service.waitTimeForForceShutdownInSeconds", 0);

        if (timeWaitForShutdownInSeconds > 0) {
            long endTime = System.currentTimeMillis() + (timeWaitForShutdownInSeconds * 1000L);

            LOG.info("Will wait for all threads to shutdown gracefully. Final shutdown Time: " + new Date(endTime));

            while (System.currentTimeMillis() < endTime) {
                int activeCount = Thread.activeCount();

                if (activeCount == 0) {
                    LOG.info("Number of active threads = " + activeCount + ".");

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

        LOG.info("Shuting down the Server.");

        LOG.info("<== EmbeddedServer.shutdownServer(appName={})", appName);

        System.exit(0);
    }

    @SuppressWarnings("deprecation")
    private void updateHttpConnectorAttribConfig(Tomcat server) {
        server.getConnector().setAllowTrace(getBooleanProperty("service.http.connector.attrib.allowTrace", false));
        server.getConnector().setAsyncTimeout(getLongProperty("service.http.connector.attrib.asyncTimeout", 10000));
        server.getConnector().setEnableLookups(getBooleanProperty("service.http.connector.attrib.enableLookups", false));
        server.getConnector().setMaxParameterCount(getIntProperty("service.http.connector.attrib.maxParameterCount", 10000));
        server.getConnector().setMaxPostSize(getIntProperty("service.http.connector.attrib.maxPostSize", 2097152));
        server.getConnector().setMaxSavePostSize(getIntProperty("service.http.connector.attrib.maxSavePostSize", 4096));
        server.getConnector().setParseBodyMethods(getProperty("service.http.connector.attrib.methods", "POST"));
        server.getConnector().setURIEncoding(getProperty("service.http.connector.attrib.URIEncoding", "UTF-8"));
        server.getConnector().setProperty("acceptCount", getProperty("service.http.connector.attrib.acceptCount", "1024"));
        server.getConnector().setXpoweredBy(false);
        server.getConnector().setAttribute("server", getProperty("servername", "Apache Ranger USERSYNC"));
        server.getConnector().setProperty("sendReasonPhrase", getProperty(configPrefix + "service.http.connector.property.sendReasonPhrase", "true"));

        for (Iterator<Map.Entry<String, String>> iterator = configuration.iterator(); iterator.hasNext(); ) {
            Map.Entry<String, String> entry = iterator.next();
            String                    key   = entry.getKey();
            String                    value = entry.getValue();

            if (key != null && key.startsWith(configPrefix + "service.http.connector.property.")) {
                String property = key.replace(configPrefix + "service.http.connector.property.", "");

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
        String       keyStoreFile           = getProperty("keystore.file");
        String       keyStoreAlias          = getProperty("keystore.alias");
        String       credentialProviderPath = getProperty("credential.provider.path");
        String       keyStoreFilepwd        = getCredential(credentialProviderPath, keyStoreAlias);

        if (StringUtils.isNotBlank(keyStoreFile) && StringUtils.isNotBlank(keyStoreFilepwd)) {
            InputStream in = null;

            try {
                in = getFileInputStream(keyStoreFile);

                if (in != null) {
                    KeyStore keyStore = KeyStore.getInstance(keyStoreFileType);

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
        String         truststoreFile         = getProperty("truststore.file");
        String         truststoreAlias        = getProperty("truststore.alias");
        String         credentialProviderPath = getProperty("credential.provider.path");
        String         trustStoreFilepwd      = getCredential(credentialProviderPath, truststoreAlias);

        if (StringUtils.isNotBlank(truststoreFile) && StringUtils.isNotBlank(trustStoreFilepwd)) {
            InputStream in = null;

            try {
                in = getFileInputStream(truststoreFile);

                if (in != null) {
                    KeyStore trustStore = KeyStore.getInstance(trustStoreFileType);

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
                LOG.error("Unable to find the necessary SSL TrustStore File:{}", truststoreFile + e);
            } catch (IOException e) {
                LOG.error("Unable to read the necessary SSL TrustStore Files:{}", truststoreFile + e);
            } finally {
                close(in, truststoreFile);
            }
        }

        return ret;
    }

    private String getKeystoreFile() {
        String ret = getProperty("service.https.attrib.keystore.file");

        if (StringUtils.isBlank(ret)) {
            // new property not configured, lets use the old property
            ret = getProperty("https.attrib.keystore.file");
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

    private int getIntProperty(String key, int defaultValue) {
        int    ret = defaultValue;
        String val = getProperty(key);

        try {
            if (val != null) {
                ret = Integer.parseInt(val);
            }
        } catch (Exception err) {
            LOG.warn(val + " can't be parsed to int. Reason: " + err);
        }

        return ret;
    }
}
