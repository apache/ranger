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

package org.apache.ranger.server.tomcat;

import org.apache.catalina.LifecycleException;
import org.apache.catalina.WebResourceRoot;
import org.apache.catalina.connector.Connector;
import org.apache.catalina.core.StandardContext;
import org.apache.catalina.startup.Tomcat;
import org.apache.catalina.valves.AccessLogValve;
import org.apache.catalina.valves.ErrorReportValve;
import org.apache.catalina.webresources.StandardRoot;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.SecureClientLogin;
import org.apache.ranger.credentialapi.CredentialReader;
import org.apache.tomcat.util.net.SSLHostConfig;
import org.apache.tomcat.util.scan.StandardJarScanner;

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
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

public class EmbeddedServer {
    private static final Logger LOG = Logger.getLogger(EmbeddedServer.class.getName());

    public  static final String RANGER_KEYSTORE_FILE_TYPE_DEFAULT   = KeyStore.getDefaultType();
    public  static final String RANGER_TRUSTSTORE_FILE_TYPE_DEFAULT = KeyStore.getDefaultType();
    public  static final String RANGER_SSL_CONTEXT_ALGO_TYPE        = "TLS";
    public  static final String RANGER_SSL_KEYMANAGER_ALGO_TYPE     = KeyManagerFactory.getDefaultAlgorithm();
    public  static final String RANGER_SSL_TRUSTMANAGER_ALGO_TYPE   = TrustManagerFactory.getDefaultAlgorithm();
    private static final String DEFAULT_NAME_RULE                   = "DEFAULT";
    private static final String DEFAULT_ENABLED_PROTOCOLS           = "TLSv1.2";
    private static final String DEFAULT_SSL_PROTOCOL                = "TLS";
    private static final String DEFAULT_WEBAPPS_ROOT_FOLDER         = "webapps";
    private static final String AUTH_TYPE_KERBEROS                  = "kerberos";
    private static final String AUTHENTICATION_TYPE                 = "hadoop.security.authentication";
    private static final String ADMIN_USER_PRINCIPAL                = "ranger.admin.kerberos.principal";
    private static final String AUDIT_SOURCE_TYPE                   = "ranger.audit.source.type";
    private static final String AUDIT_SOURCE_SOLR                   = "solr";
    private static final String AUDIT_SOURCE_ES                     = "elasticsearch";
    private static final String AUDIT_SOURCE_OPENSEARCH             = "opensearch";
    private static final String SOLR_BOOTSTRAP_ENABLED              = "ranger.audit.solr.bootstrap.enabled";
    private static final String ES_BOOTSTRAP_ENABLED                = "ranger.audit.elasticsearch.bootstrap.enabled";
    private static final String OS_BOOTSTRAP_ENABLED                = "ranger.audit.opensearch.bootstrap.enabled";
    private static final String ADMIN_USER_KEYTAB                   = "ranger.admin.kerberos.keytab";
    private static final String ADMIN_NAME_RULES                    = "hadoop.security.auth_to_local";
    private static final String ADMIN_SERVER_NAME                   = "rangeradmin";
    private static final String KMS_SERVER_NAME                     = "rangerkms";
    private static final String ACCESS_LOG_ENABLED                  = "ranger.accesslog.enabled";
    private static final String ACCESS_LOG_PREFIX                   = "ranger.accesslog.prefix";
    private static final String ACCESS_LOG_DATE_FORMAT              = "ranger.accesslog.dateformat";
    private static final String ACCESS_LOG_PATTERN                  = "ranger.accesslog.pattern";
    private static final String ACCESS_LOG_ROTATE_ENABLED           = "ranger.accesslog.rotate.enabled";
    private static final String ACCESS_LOG_ROTATE_MAX_DAYS          = "ranger.accesslog.rotate.max_days";
    private static final String ACCESS_LOG_ROTATE_RENAME_ON_ROTATE  = "ranger.accesslog.rotate.rename_on_rotate";

    public  static String                         defaultShutdownCommand  = "SHUTDOWN";
    private static String                         configFile              = "ranger-admin-site.xml";
    public  static int                            defaultShutdownPort     = 6185;
    private static EmbeddedServerMetricsCollector serverMetricsCollector;

    private final EmbeddedServerConfigUtil prefixedConfig;
    private final String                   configPrefix;

    public EmbeddedServer(String[] args) {
        if (args.length > 0) {
            configFile = args[0];
        }

        EmbeddedServerUtil.loadRangerConfigProperties(configFile);
        this.prefixedConfig = null;
        this.configPrefix   = null;
    }

    protected EmbeddedServer(Configuration configuration, String configPrefix) {
        this.prefixedConfig = new EmbeddedServerConfigUtil(configuration, configPrefix);
        this.configPrefix   = configPrefix;
    }

    public static void main(String[] args) {
        new EmbeddedServer(args).start();
    }

    public static EmbeddedServerMetricsCollector getServerMetricsCollector() {
        EmbeddedServerMetricsCollector embeddedServerMetricsCollector = EmbeddedServer.serverMetricsCollector;

        if (null != embeddedServerMetricsCollector) {
            LOG.info("Selected Tomcat protocolHandler: " + embeddedServerMetricsCollector.getProtocolHandlerName());
        }

        return embeddedServerMetricsCollector;
    }

    protected String getConnectorServerBanner() {
        return "Apache Ranger";
    }

    protected String getDefaultAccessLogPattern(String servername) {
        if (servername != null && servername.equalsIgnoreCase(KMS_SERVER_NAME)) {
            return "%h %l %u %t \"%m %U\" %s %b %D %{eek_op}r";
        }

        return "%h %l %u %t \"%r\" %s %b %D";
    }

    public void start() {
        SSLContext sslContext = getSSLContext();

        if (sslContext != null) {
            SSLContext.setDefault(sslContext);
        }

        final Tomcat server = new Tomcat();

        String logDir = getServiceConfig("logdir", "log.dir");

        if (logDir == null) {
            logDir = getServiceConfig("kms.log.dir", "logdir");
        }

        String servername      = getServiceConfig("servername", "servername");
        String hostName        = getServiceConfig("ranger.service.host", "service.host");
        int    serverPort      = getServiceIntConfig("ranger.service.http.port", "service.http.port", isPrefixedConfigMode() ? 0 : 6181);
        int    sslPort         = getServiceIntConfig("ranger.service.https.port", "service.https.port", isPrefixedConfigMode() ? 0 : -1);
        int    shutdownPort    = getServiceIntConfig("ranger.service.shutdown.port", "service.shutdown.port", defaultShutdownPort);
        String shutdownCommand = getServiceConfig("ranger.service.shutdown.command", "service.shutdown.command", defaultShutdownCommand);

        if (isPrefixedConfigMode() && serverPort == 0 && sslPort == 0) {
            LOG.severe("Tomcat Server failed to start: http and https ports are not set");
            System.exit(1);
        }

        server.setHostname(hostName);
        server.setPort(serverPort);
        server.getServer().setPort(shutdownPort);
        server.getServer().setShutdown(shutdownCommand);

        boolean isHttpsEnabled = getServiceBooleanConfig("ranger.service.https.attrib.ssl.enabled", "service.https.attrib.ssl.enabled", false);
        boolean ajpEnabled     = getServiceBooleanConfig("ajp.enabled", "ajp.enabled", false);

        if (ajpEnabled) {
            Connector ajpConnector = new Connector("org.apache.coyote.ajp.AjpNioProtocol");

            ajpConnector.setPort(serverPort);
            ajpConnector.setProperty("protocol", "AJP/1.3");

            server.getService().addConnector(ajpConnector);

            // Making this as a default connector
            server.setConnector(ajpConnector);

            LOG.info("Created AJP Connector");
        } else if ((sslPort > 0) && isHttpsEnabled) {
            Connector ssl = new Connector();

            ssl.setPort(sslPort);
            ssl.setSecure(true);
            ssl.setScheme("https");

            String clientAuth = getServiceConfig("ranger.service.https.attrib.clientAuth", "service.https.attrib.clientAuth", "false");

            if ("false".equalsIgnoreCase(clientAuth)) {
                clientAuth = getServiceConfig("ranger.service.https.attrib.client.auth", "service.https.attrib.client.auth", "want");
            }

            String providerPath = getServiceConfig("ranger.credential.provider.path", "credential.provider.path");
            String keyAlias     = getServiceConfig("ranger.service.https.attrib.keystore.credential.alias", "service.https.attrib.keystore.credential.alias", "keyStoreCredentialAlias");
            String keystorePass = null;

            if (providerPath != null && keyAlias != null) {
                keystorePass = CredentialReader.getDecryptedString(
                        providerPath.trim(),
                        keyAlias.trim(),
                        getServiceConfig("ranger.keystore.file.type", "keystore.file.type", RANGER_KEYSTORE_FILE_TYPE_DEFAULT));

                if (StringUtils.isBlank(keystorePass) || "none".equalsIgnoreCase(keystorePass.trim())) {
                    keystorePass = getServiceConfig("ranger.service.https.attrib.keystore.pass", "service.https.attrib.keystore.pass");
                }
            }

            String keystoreKeyAlias = getServiceConfig("ranger.service.https.attrib.keystore.keyalias", "service.https.attrib.keystore.keyalias", "rangeradmin");
            String enabledProtocols = getServiceConfig("ranger.service.https.attrib.ssl.enabled.protocols", "service.https.attrib.ssl.enabled.protocols", DEFAULT_ENABLED_PROTOCOLS);
            String ciphers          = getServiceConfig("ranger.tomcat.ciphers", "tomcat.ciphers");

            if (isPrefixedConfigMode()) {
                ssl.setProperty("SSLEnabled", "true");
                ssl.setProperty("sslProtocol", getServiceConfig("ranger.service.https.attrib.ssl.protocol", "service.https.attrib.ssl.protocol", DEFAULT_SSL_PROTOCOL));
                ssl.setProperty("keystoreType", getServiceConfig("ranger.keystore.file.type", "keystore.file.type", RANGER_KEYSTORE_FILE_TYPE_DEFAULT));
                ssl.setProperty("truststoreType", getServiceConfig("ranger.truststore.file.type", "truststore.file.type", RANGER_TRUSTSTORE_FILE_TYPE_DEFAULT));
                ssl.setProperty("clientAuth", clientAuth);

                if (StringUtils.isNotBlank(keystoreKeyAlias)) {
                    ssl.setProperty("keyAlias", keystoreKeyAlias);
                }

                ssl.setProperty("keystorePass", keystorePass);
                ssl.setProperty("keystoreFile", getKeystoreFile());
                ssl.setProperty("sslEnabledProtocols", enabledProtocols);

                if (StringUtils.isNotBlank(ciphers)) {
                    ssl.setProperty("ciphers", ciphers);
                }
            } else {
                ssl.setAttribute("SSLEnabled", "true");
                ssl.setAttribute("sslProtocol", getServiceConfig("ranger.service.https.attrib.ssl.protocol", "service.https.attrib.ssl.protocol", DEFAULT_SSL_PROTOCOL));
                ssl.setAttribute("keystoreType", getServiceConfig("ranger.keystore.file.type", "keystore.file.type", RANGER_KEYSTORE_FILE_TYPE_DEFAULT));
                ssl.setAttribute("truststoreType", getServiceConfig("ranger.truststore.file.type", "truststore.file.type", RANGER_TRUSTSTORE_FILE_TYPE_DEFAULT));
                ssl.setAttribute("clientAuth", clientAuth);
                ssl.setAttribute("keyAlias", keystoreKeyAlias);
                ssl.setAttribute("keystorePass", keystorePass);
                ssl.setAttribute("keystoreFile", getKeystoreFile());
                ssl.setAttribute("sslEnabledProtocols", enabledProtocols);

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

        AccessLogValve valve = new AccessLogValve();

        valve.setRotatable(true);
        valve.setAsyncSupported(true);
        valve.setBuffered(false);

        if (isPrefixedConfigMode()) {
            valve.setEnabled(true);
            valve.setPrefix(getServiceConfig("ranger.accesslog.prefix", "accesslog.prefix", "access_log-" + hostName + "-"));
            valve.setFileDateFormat(getServiceConfig("ranger.accesslog.dateformat", "accesslog.dateformat", "yyyy-MM-dd.HH"));
            valve.setMaxDays(getServiceIntConfig("ranger.accesslog.rotate.max_days", "accesslog.rotate.max.days", 15));
        } else {
            valve.setEnabled(getServiceBooleanConfig(ACCESS_LOG_ENABLED, ACCESS_LOG_ENABLED, true));
            valve.setPrefix(getServiceConfig(ACCESS_LOG_PREFIX, ACCESS_LOG_PREFIX, "access-" + hostName));
            valve.setFileDateFormat(getServiceConfig(ACCESS_LOG_DATE_FORMAT, ACCESS_LOG_DATE_FORMAT, "-yyyy-MM-dd.HH"));
            valve.setRotatable(getServiceBooleanConfig(ACCESS_LOG_ROTATE_ENABLED, ACCESS_LOG_ROTATE_ENABLED, true));
            valve.setMaxDays(getServiceIntConfig(ACCESS_LOG_ROTATE_MAX_DAYS, ACCESS_LOG_ROTATE_MAX_DAYS, 15));
            valve.setRenameOnRotate(getServiceBooleanConfig(ACCESS_LOG_ROTATE_RENAME_ON_ROTATE, ACCESS_LOG_ROTATE_RENAME_ON_ROTATE, false));
        }

        valve.setDirectory(logDirectory.getAbsolutePath());
        valve.setSuffix(".log");

        String defaultAccessLogPattern = getDefaultAccessLogPattern(servername);
        String logPattern              = getServiceConfig(ACCESS_LOG_PATTERN, "accesslog.pattern", defaultAccessLogPattern);

        valve.setPattern(logPattern);
        server.getHost().getPipeline().addValve(valve);

        ErrorReportValve errorReportValve = new ErrorReportValve();

        errorReportValve.setShowServerInfo(getServiceBooleanConfig("ranger.valve.errorreportvalve.showserverinfo", "valve.errorreportvalve.showserverinfo", true));
        errorReportValve.setShowReport(getServiceBooleanConfig("ranger.valve.errorreportvalve.showreport", "valve.errorreportvalve.showreport", true));
        server.getHost().getPipeline().addValve(errorReportValve);

        try {
            String webappDir = getServiceConfig("xa.webapp.dir", "webapp.dir");

            if (StringUtils.isBlank(webappDir)) {
                String catalinaBaseDir = getServiceConfig("catalina.base", "catalina.base");

                if (StringUtils.isBlank(catalinaBaseDir)) {
                    LOG.severe("Tomcat Server failed to start: catalina.base and/or webapp dir is not set");
                    System.exit(1);
                }

                webappDir = catalinaBaseDir + File.separator + "webapp";
                LOG.info("Deriving webapp folder from catalina.base property. folder=" + webappDir);
            }

            String webContextName = getServiceConfig("ranger.contextName", "contextName", "/");

            if (webContextName == null) {
                webContextName = "/";
            } else if (!webContextName.startsWith("/")) {
                LOG.info("Context Name [" + webContextName + "] is being loaded as [ /" + webContextName + "]");
                webContextName = "/" + webContextName;
            }

            File wad = new File(webappDir);

            if (wad.isDirectory()) {
                LOG.info("Webapp file =" + webappDir + ", webAppName = " + webContextName);
            } else if (wad.isFile()) {
                File webAppDir = new File(DEFAULT_WEBAPPS_ROOT_FOLDER);

                if (!webAppDir.exists()) {
                    webAppDir.mkdirs();
                }

                LOG.info("Webapp file =" + webappDir + ", webAppName = " + webContextName);
            }

            LOG.info("Adding webapp [" + webContextName + "] = path [" + webappDir + "] .....");

            StandardContext webappCtx   = (StandardContext) server.addWebapp(webContextName, new File(webappDir).getAbsolutePath());
            String          workDirPath = getServiceConfig("ranger.tomcat.work.dir", "tomcat.work.dir", "");

            if (!workDirPath.isEmpty() && new File(workDirPath).exists()) {
                webappCtx.setWorkDir(workDirPath);
            } else {
                if (LOG.isLoggable(Level.FINE)) {
                    LOG.fine("Skipping to set tomcat server work directory, '" + workDirPath + "', as it is blank or directory does not exist.");
                }
            }

            if (isPrefixedConfigMode()) {
                boolean allowLinking = getServiceBooleanConfig("", "allow.linking", true);
                boolean scanManifest = getServiceBooleanConfig("", "tomcat.scan.manifest", false);

                WebResourceRoot resRoot = new StandardRoot(webappCtx);
                webappCtx.setResources(resRoot);
                webappCtx.getResources().setAllowLinking(allowLinking);

                StandardJarScanner scanner = new StandardJarScanner();
                scanner.setScanManifest(scanManifest);
                webappCtx.setJarScanner(scanner);
            }

            webappCtx.init();
            LOG.info("Finished init of webapp [" + webContextName + "] = path [" + webappDir + "].");
        } catch (LifecycleException lce) {
            LOG.severe("Tomcat Server failed to start webapp:" + lce);
            lce.printStackTrace();
        }

        if (shouldStartWithKerberos(servername)) {
            startWithKerberos(server, hostName);
        } else {
            startServer(server);
        }
    }

    public void shutdownServer() {
        int timeWaitForShutdownInSeconds = getServiceIntConfig("service.waitTimeForForceShutdownInSeconds", "service.waitTimeForForceShutdownInSeconds", 0);

        if (timeWaitForShutdownInSeconds > 0) {
            long endTime = System.currentTimeMillis() + (timeWaitForShutdownInSeconds * 1000L);

            LOG.info("Will wait for all threads to shutdown gracefully. Final shutdown Time: " + new Date(endTime));

            while (System.currentTimeMillis() < endTime) {
                int activeCount = Thread.activeCount();

                if (activeCount == 0) {
                    LOG.info("Number of active threads = " + activeCount + ".");
                    break;
                }

                LOG.info("Number of active threads = " + activeCount + ". Waiting for all threads to shutdown ...");

                try {
                    Thread.sleep(5000L);
                } catch (InterruptedException e) {
                    LOG.warning("shutdownServer process is interrupted with exception: " + e);
                    break;
                }
            }
        }

        LOG.info("Shuting down the Server.");
        System.exit(0);
    }

    public void updateHttpConnectorAttribConfig(Tomcat server) {
        server.getConnector().setAllowTrace(getServiceBooleanConfig("ranger.service.http.connector.attrib.allowTrace", "service.http.connector.attrib.allowTrace", false));
        server.getConnector().setAsyncTimeout(getServiceLongConfig("ranger.service.http.connector.attrib.asyncTimeout", "service.http.connector.attrib.asyncTimeout", 10000L));
        server.getConnector().setEnableLookups(getServiceBooleanConfig("ranger.service.http.connector.attrib.enableLookups", "service.http.connector.attrib.enableLookups", false));
        server.getConnector().setMaxParameterCount(getServiceIntConfig("ranger.service.http.connector.attrib.maxParameterCount", "service.http.connector.attrib.maxParameterCount", 10000));
        server.getConnector().setMaxPostSize(getServiceIntConfig("ranger.service.http.connector.attrib.maxPostSize", "service.http.connector.attrib.maxPostSize", 2097152));
        server.getConnector().setMaxSavePostSize(getServiceIntConfig("ranger.service.http.connector.attrib.maxSavePostSize", "service.http.connector.attrib.maxSavePostSize", 4096));
        server.getConnector().setParseBodyMethods(getServiceConfig("ranger.service.http.connector.attrib.methods", "service.http.connector.attrib.methods", "POST"));
        server.getConnector().setURIEncoding(getServiceConfig("ranger.service.http.connector.attrib.URIEncoding", "service.http.connector.attrib.URIEncoding", "UTF-8"));
        server.getConnector().setXpoweredBy(false);
        server.getConnector().setAttribute("server", getConnectorServerBanner());
        server.getConnector().setProperty("sendReasonPhrase",
                getServiceConfig("ranger.service.http.connector.property.sendReasonPhrase", "service.http.connector.property.sendReasonPhrase", "true"));

        if (isPrefixedConfigMode()) {
            String connectorPropertyPrefix = configPrefix + "service.http.connector.property.";
            Iterator<Map.Entry<String, String>> iterator = prefixedConfig.iterator();

            while (iterator.hasNext()) {
                Map.Entry<String, String> entry = iterator.next();

                if (entry != null && entry.getKey() != null && entry.getKey().startsWith(connectorPropertyPrefix)) {
                    String property    = entry.getKey().replace(connectorPropertyPrefix, "");
                    String relativeKey = "service.http.connector.property." + property;
                    server.getConnector().setProperty(property, prefixedConfig.getConfig(relativeKey));
                }
            }
        } else {
            for (Object o : EmbeddedServerUtil.getRangerConfigProperties().keySet()) {
                String key = o != null ? o.toString() : null;

                if (key != null && key.startsWith("ranger.service.http.connector.property.")) {
                    String property = key.replace("ranger.service.http.connector.property.", "");
                    server.getConnector().setProperty(property, EmbeddedServerUtil.getConfig(key));
                    LOG.info(property + ":" + server.getConnector().getProperty(property));
                }
            }
        }
    }

    private boolean shouldStartWithKerberos(String servername) {
        String authenticationType = getServiceConfig(AUTHENTICATION_TYPE, AUTHENTICATION_TYPE);

        if (!AUTH_TYPE_KERBEROS.equalsIgnoreCase(StringUtils.trimToEmpty(authenticationType))) {
            return false;
        }

        if (isPrefixedConfigMode()) {
            return true;
        }

        return ADMIN_SERVER_NAME.equalsIgnoreCase(servername);
    }

    private void startWithKerberos(final Tomcat server, String hostName) {
        String keytab    = getServiceConfig(ADMIN_USER_KEYTAB, "kerberos.keytab");
        String principal = null;

        try {
            principal = SecureClientLogin.getPrincipal(getServiceConfig(ADMIN_USER_PRINCIPAL, "kerberos.principal"), hostName);
        } catch (IOException ignored) {
            LOG.warning("Failed to get kerberos principal. Reason: " + ignored);
        }

        String nameRules = getServiceConfig(ADMIN_NAME_RULES, ADMIN_NAME_RULES);

        if (StringUtils.isBlank(nameRules)) {
            LOG.info("Name is empty. Setting Name Rule as 'DEFAULT'");
            nameRules = DEFAULT_NAME_RULE;
        }

        if (SecureClientLogin.isKerberosCredentialExists(principal, keytab)) {
            try {
                LOG.info("Provided Kerberos Credential : Principal = " + principal + " and Keytab = " + keytab);

                Subject sub = SecureClientLogin.loginUserFromKeytab(principal, keytab, nameRules);

                Subject.doAs(sub, (PrivilegedAction<Void>) () -> {
                    LOG.info("Starting Server using kerberos credential");
                    startServer(server);
                    return null;
                });
            } catch (Exception e) {
                LOG.severe("Tomcat Server failed to start:" + e);
                e.printStackTrace();
            }
        } else {
            startServer(server);
        }
    }

    private void startServer(final Tomcat server) {
        try {
            String servername = getServiceConfig("servername", "servername");

            LOG.info("Server Name : " + servername);

            if (!isPrefixedConfigMode() && ADMIN_SERVER_NAME.equalsIgnoreCase(servername)) {
                runAuditBootstrap();
            }

            serverMetricsCollector = new EmbeddedServerMetricsCollector(server);
            server.start();
            server.getServer().await();
            shutdownServer();
        } catch (LifecycleException e) {
            LOG.severe("Tomcat Server failed to start:" + e);
            e.printStackTrace();
        } catch (Exception e) {
            LOG.severe("Tomcat Server failed to start:" + e);
            e.printStackTrace();
        }
    }

    private void runAuditBootstrap() {
        String auditSourceType = EmbeddedServerUtil.getConfig(AUDIT_SOURCE_TYPE, "db");

        if (AUDIT_SOURCE_SOLR.equalsIgnoreCase(auditSourceType)) {
            if (EmbeddedServerUtil.getBooleanConfig(SOLR_BOOTSTRAP_ENABLED, true)) {
                try {
                    new SolrCollectionBootstrapper().start();
                } catch (Exception e) {
                    LOG.severe("Error while setting solr " + e);
                }
            }
        } else if (AUDIT_SOURCE_ES.equalsIgnoreCase(auditSourceType)) {
            if (EmbeddedServerUtil.getBooleanConfig(ES_BOOTSTRAP_ENABLED, true)) {
                try {
                    new ElasticSearchIndexBootStrapper().start();
                } catch (Exception e) {
                    LOG.severe("Error while setting elasticsearch " + e);
                }
            }
        } else if (AUDIT_SOURCE_OPENSEARCH.equalsIgnoreCase(auditSourceType)) {
            if (EmbeddedServerUtil.getBooleanConfig(OS_BOOTSTRAP_ENABLED, true)) {
                try {
                    new OpenSearchIndexBootStrapper().start();
                } catch (Exception e) {
                    LOG.severe("Error while setting opensearch " + e);
                }
            }
        }
    }

    private String getKeystoreFile() {
        String keystoreFile = getServiceConfig("ranger.service.https.attrib.keystore.file", "service.https.attrib.keystore.file");

        if (StringUtils.isBlank(keystoreFile)) {
            keystoreFile = getServiceConfig("ranger.https.attrib.keystore.file", "https.attrib.keystore.file");
        }

        return keystoreFile;
    }

    private SSLContext getSSLContext() {
        KeyManager[]   kmList     = getKeyManagers();
        TrustManager[] tmList     = getTrustManagers();
        SSLContext     sslContext = null;

        if (tmList != null) {
            try {
                sslContext = SSLContext.getInstance(RANGER_SSL_CONTEXT_ALGO_TYPE);
                sslContext.init(kmList, tmList, new SecureRandom());
            } catch (NoSuchAlgorithmException e) {
                LOG.severe("SSL algorithm is not available in the environment. Reason: " + e);
            } catch (KeyManagementException e) {
                LOG.severe("Unable to initials the SSLContext. Reason: " + e);
            }
        }

        return sslContext;
    }

    private KeyManager[] getKeyManagers() {
        KeyManager[] kmList        = null;
        String       keyStoreFile  = getServiceConfig("ranger.keystore.file", "keystore.file");
        String       keyStoreAlias = getServiceConfig("ranger.keystore.alias", "keystore.alias", "keyStoreCredentialAlias");

        if (StringUtils.isBlank(keyStoreFile)) {
            keyStoreFile  = getKeystoreFile();
            keyStoreAlias = getServiceConfig("ranger.service.https.attrib.keystore.credential.alias", "service.https.attrib.keystore.credential.alias", "keyStoreCredentialAlias");
        }

        String keyStoreFileType       = getServiceConfig("ranger.keystore.file.type", "keystore.file.type", RANGER_KEYSTORE_FILE_TYPE_DEFAULT);
        String credentialProviderPath = getServiceConfig("ranger.credential.provider.path", "credential.provider.path");
        String keyStoreFilepwd        = CredentialReader.getDecryptedString(credentialProviderPath, keyStoreAlias, keyStoreFileType);

        if (StringUtils.isNotEmpty(keyStoreFile) && StringUtils.isNotEmpty(keyStoreFilepwd)) {
            InputStream in = null;

            try {
                in = getFileInputStream(keyStoreFile);

                if (in != null) {
                    KeyStore keyStore = KeyStore.getInstance(keyStoreFileType);

                    keyStore.load(in, keyStoreFilepwd.toCharArray());

                    KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());

                    keyManagerFactory.init(keyStore, keyStoreFilepwd.toCharArray());

                    kmList = keyManagerFactory.getKeyManagers();
                } else {
                    LOG.severe("Unable to obtain keystore from file [" + keyStoreFile + "]");
                }
            } catch (KeyStoreException e) {
                LOG.log(Level.SEVERE, "Unable to obtain from KeyStore :" + e.getMessage(), e);
            } catch (NoSuchAlgorithmException e) {
                LOG.log(Level.SEVERE, "SSL algorithm is NOT available in the environment", e);
            } catch (CertificateException e) {
                LOG.log(Level.SEVERE, "Unable to obtain the requested certification ", e);
            } catch (FileNotFoundException e) {
                LOG.log(Level.SEVERE, "Unable to find the necessary SSL Keystore Files", e);
            } catch (IOException e) {
                LOG.log(Level.SEVERE, "Unable to read the necessary SSL Keystore Files", e);
            } catch (UnrecoverableKeyException e) {
                LOG.log(Level.SEVERE, "Unable to recover the key from keystore", e);
            } finally {
                close(in, keyStoreFile);
            }
        } else {
            if (StringUtils.isBlank(keyStoreFile)) {
                LOG.warning("Config 'ranger.keystore.file' or 'ranger.service.https.attrib.keystore.file' is not found or contains blank value");
            } else if (StringUtils.isBlank(keyStoreAlias)) {
                LOG.warning("Config 'ranger.keystore.alias' or 'ranger.service.https.attrib.keystore.credential.alias' is not found or contains blank value");
            } else if (StringUtils.isBlank(credentialProviderPath)) {
                LOG.warning("Config 'ranger.credential.provider.path' is not found or contains blank value");
            } else if (StringUtils.isBlank(keyStoreFilepwd)) {
                LOG.warning("Unable to read credential from credential store file [" + credentialProviderPath + "] for given alias:" + keyStoreAlias);
            }
        }

        return kmList;
    }

    private TrustManager[] getTrustManagers() {
        TrustManager[] tmList                 = null;
        String         truststoreFile         = getServiceConfig("ranger.truststore.file", "truststore.file");
        String         truststoreAlias        = getServiceConfig("ranger.truststore.alias", "truststore.alias");
        String         credentialProviderPath = getServiceConfig("ranger.credential.provider.path", "credential.provider.path");
        String         truststoreFileType     = getServiceConfig("ranger.truststore.file.type", "truststore.file.type", RANGER_TRUSTSTORE_FILE_TYPE_DEFAULT);
        String         trustStoreFilepwd      = CredentialReader.getDecryptedString(credentialProviderPath, truststoreAlias, truststoreFileType);

        if (StringUtils.isNotEmpty(truststoreFile) && StringUtils.isNotEmpty(trustStoreFilepwd)) {
            InputStream in = null;

            try {
                in = getFileInputStream(truststoreFile);

                if (in != null) {
                    KeyStore trustStore = KeyStore.getInstance(truststoreFileType);

                    trustStore.load(in, trustStoreFilepwd.toCharArray());

                    TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(RANGER_SSL_TRUSTMANAGER_ALGO_TYPE);

                    trustManagerFactory.init(trustStore);

                    tmList = trustManagerFactory.getTrustManagers();
                } else {
                    LOG.log(Level.SEVERE, "Unable to obtain truststore from file [" + truststoreFile + "]");
                }
            } catch (KeyStoreException e) {
                LOG.log(Level.SEVERE, "Unable to obtain from KeyStore", e);
            } catch (NoSuchAlgorithmException e) {
                LOG.log(Level.SEVERE, "SSL algorithm is NOT available in the environment :" + e.getMessage(), e);
            } catch (CertificateException e) {
                LOG.log(Level.SEVERE, "Unable to obtain the requested certification :" + e.getMessage(), e);
            } catch (FileNotFoundException e) {
                LOG.log(Level.SEVERE, "Unable to find the necessary SSL TrustStore File:" + truststoreFile, e);
            } catch (IOException e) {
                LOG.log(Level.SEVERE, "Unable to read the necessary SSL TrustStore Files :" + truststoreFile, e);
            } finally {
                close(in, truststoreFile);
            }
        } else {
            if (StringUtils.isBlank(truststoreFile)) {
                LOG.warning("Config 'ranger.truststore.file' is not found or contains blank value!");
            } else if (StringUtils.isBlank(truststoreAlias)) {
                LOG.warning("Config 'ranger.truststore.alias' is not found or contains blank value!");
            } else if (StringUtils.isBlank(credentialProviderPath)) {
                LOG.warning("Config 'ranger.credential.provider.path' is not found or contains blank value!");
            } else if (StringUtils.isBlank(trustStoreFilepwd)) {
                LOG.warning("Unable to read credential from credential store file [" + credentialProviderPath + "] for given alias:" + truststoreAlias);
            }
        }

        return tmList;
    }

    private boolean isPrefixedConfigMode() {
        return prefixedConfig != null;
    }

    /**
     * Reads a config property using Admin/KMS full keys or a prefixed service relative key.
     */
    private String getServiceConfig(String adminPropertyKey, String prefixedPropertyKey) {
        if (isPrefixedConfigMode()) {
            return prefixedConfig.getConfig(prefixedPropertyKey);
        }

        return EmbeddedServerUtil.getConfig(adminPropertyKey);
    }

    private String getServiceConfig(String adminPropertyKey, String prefixedPropertyKey, String defaultValue) {
        String value = getServiceConfig(adminPropertyKey, prefixedPropertyKey);

        return value != null ? value : defaultValue;
    }

    private int getServiceIntConfig(String adminPropertyKey, String prefixedPropertyKey, int defaultValue) {
        if (isPrefixedConfigMode()) {
            return prefixedConfig.getIntConfig(prefixedPropertyKey, defaultValue);
        }

        return EmbeddedServerUtil.getIntConfig(adminPropertyKey, defaultValue);
    }

    private long getServiceLongConfig(String adminPropertyKey, String prefixedPropertyKey, long defaultValue) {
        if (isPrefixedConfigMode()) {
            return prefixedConfig.getLongConfig(prefixedPropertyKey, defaultValue);
        }

        return EmbeddedServerUtil.getLongConfig(adminPropertyKey, defaultValue);
    }

    private boolean getServiceBooleanConfig(String adminPropertyKey, String prefixedPropertyKey, boolean defaultValue) {
        if (isPrefixedConfigMode()) {
            return prefixedConfig.getBooleanConfig(prefixedPropertyKey, defaultValue);
        }

        return EmbeddedServerUtil.getBooleanConfig(adminPropertyKey, defaultValue);
    }

    private InputStream getFileInputStream(String fileName) throws IOException {
        InputStream in = null;

        if (StringUtils.isNotEmpty(fileName)) {
            File f = new File(fileName);

            if (f.exists()) {
                in = new FileInputStream(f);
            } else {
                in = ClassLoader.getSystemResourceAsStream(fileName);
            }
        }

        return in;
    }

    private void close(InputStream str, String filename) {
        if (str != null) {
            try {
                str.close();
            } catch (IOException excp) {
                LOG.log(Level.SEVERE, "Error while closing file: [" + filename + "]", excp);
            }
        }
    }
}
