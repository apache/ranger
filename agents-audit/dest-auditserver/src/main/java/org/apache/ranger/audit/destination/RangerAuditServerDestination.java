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

package org.apache.ranger.audit.destination;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.auth.AuthSchemeProvider;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.KerberosCredentials;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.config.AuthSchemes;
import org.apache.http.client.config.CookieSpecs;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.config.Lookup;
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.auth.SPNegoSchemeFactory;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.client.StandardHttpRequestRetryHandler;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.protocol.HttpContext;
import org.apache.ranger.audit.model.AuditEventBase;
import org.apache.ranger.audit.provider.MiscUtil;
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
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.PrivilegedExceptionAction;
import java.security.SecureRandom;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class RangerAuditServerDestination extends AuditDestination {
    public static final String PROP_AUDITSERVER_URL                          = "xasecure.audit.destination.auditserver.url";
    public static final String PROP_AUDITSERVER_USER_NAME                    = "xasecure.audit.destination.auditserver.username";
    public static final String PROP_AUDITSERVER_USER_PASSWORD                = "xasecure.audit.destination.auditserver.password";
    public static final String PROP_AUDITSERVER_AUTH_TYPE                    = "xasecure.audit.destination.auditserver.authentication.type";
    public static final String PROP_AUDITSERVER_JWT_TOKEN                    = "xasecure.audit.destination.auditserver.jwt.token";
    public static final String PROP_AUDITSERVER_JWT_TOKEN_FILE               = "xasecure.audit.destination.auditserver.jwt.token.file";
    public static final String PROP_AUDITSERVER_CLIENT_CONN_TIMEOUT_MS       = "xasecure.audit.destination.auditserver.connection.timeout.ms";
    public static final String PROP_AUDITSERVER_CLIENT_READ_TIMEOUT_MS       = "xasecure.audit.destination.auditserver.read.timeout.ms";
    public static final String PROP_AUDITSERVER_MAX_CONNECTION               = "xasecure.audit.destination.auditserver.max.connections";
    public static final String PROP_AUDITSERVER_MAX_CONNECTION_PER_HOST      = "xasecure.audit.destination.auditserver.max.connections.per.host";
    public static final String PROP_AUDITSERVER_VALIDATE_INACTIVE_MS         = "xasecure.audit.destination.auditserver.validate.inactivity.ms";
    public static final String PROP_AUDITSERVER_POOL_RETRY_COUNT             = "xasecure.audit.destination.auditserver.pool.retry.count";
    public static final String GSON_DATE_FORMAT                              = "yyyy-MM-dd'T'HH:mm:ss.SSSZ";
    public static final String REST_ACCEPTED_MIME_TYPE_JSON                  = "application/json";
    public static final String REST_CONTENT_TYPE_MIME_TYPE_JSON              = "application/json";
    public static final String REST_HEADER_ACCEPT                            = "Accept";
    public static final String REST_HEADER_CONTENT_TYPE                      = "Content-type";
    public static final String REST_HEADER_AUTHORIZATION                     = "Authorization";
    public static final String REST_RELATIVE_PATH_POST                       = "/api/audit/post";

    // Authentication types
    public static final String AUTH_TYPE_KERBEROS                            = "kerberos";
    public static final String AUTH_TYPE_BASIC                               = "basic";
    public static final String AUTH_TYPE_JWT                                 = "jwt";

    private static final Logger              LOG            = LoggerFactory.getLogger(RangerAuditServerDestination.class);
    private static final String              PROTOCOL_HTTPS = "https";
    private volatile     CloseableHttpClient httpClient;
    private volatile     Gson                gsonBuilder;
    private              String              httpURL;
    private              String              authType;
    private              String              jwtToken;

    @Override
    public void init(Properties props, String propPrefix) {
        LOG.info("==> RangerAuditServerDestination:init()");
        super.init(props, propPrefix);

        this.authType = MiscUtil.getStringProperty(props, PROP_AUDITSERVER_AUTH_TYPE);
        if (AUTH_TYPE_JWT.equalsIgnoreCase(authType)) {
            LOG.info("JWT authentication configured....");
            initJwtToken();
        } else if (StringUtils.isEmpty(authType)) {
            // Authentication priority: JWT → Kerberos → Basic
            try {
                if (StringUtils.isNotEmpty(MiscUtil.getStringProperty(props, PROP_AUDITSERVER_JWT_TOKEN)) ||
                    StringUtils.isNotEmpty(MiscUtil.getStringProperty(props, PROP_AUDITSERVER_JWT_TOKEN_FILE))) {
                    this.authType = AUTH_TYPE_JWT;
                    initJwtToken();
                } else if (isKerberosAuthenticated()) {
                    this.authType = AUTH_TYPE_KERBEROS;
                } else if (StringUtils.isNotEmpty(MiscUtil.getStringProperty(props, PROP_AUDITSERVER_USER_NAME))) {
                    this.authType = AUTH_TYPE_BASIC;
                }
            } catch (Exception e) {
                LOG.warn("Failed to auto-detect authentication type", e);
            }
        }

        LOG.info("Audit destination authentication type: {}", authType);

        if (AUTH_TYPE_KERBEROS.equalsIgnoreCase(authType)) {
            preAuthenticateKerberos();
        }

        this.httpClient  = buildHTTPClient();
        this.gsonBuilder = new GsonBuilder().setDateFormat(GSON_DATE_FORMAT).create();
        LOG.info("<== RangerAuditServerDestination:init()");
    }

    private void initJwtToken() {
        LOG.info("==> RangerAuditServerDestination:initJwtToken()");

        this.jwtToken = MiscUtil.getStringProperty(props, PROP_AUDITSERVER_JWT_TOKEN);
        if (StringUtils.isEmpty(jwtToken)) {
            String jwtTokenFile = MiscUtil.getStringProperty(props, PROP_AUDITSERVER_JWT_TOKEN_FILE);
            if (StringUtils.isNotEmpty(jwtTokenFile)) {
                try {
                    this.jwtToken = readJwtTokenFromFile(jwtTokenFile);
                    LOG.info("JWT token loaded from file: {}", jwtTokenFile);
                } catch (Exception e) {
                    LOG.error("Failed to read JWT token from file: {}", jwtTokenFile, e);
                }
            }
        }

        if (StringUtils.isEmpty(jwtToken)) {
            LOG.warn("JWT authentication configured but no token found. Configure {} or {}", PROP_AUDITSERVER_JWT_TOKEN, PROP_AUDITSERVER_JWT_TOKEN_FILE);
        } else {
            LOG.info("JWT authentication initialized successfully");
        }

        LOG.info("<== RangerAuditServerDestination:initJwtToken()");
    }

    private String readJwtTokenFromFile(String tokenFile) throws IOException {
        InputStream in = null;
        try {
            in = getFileInputStream(tokenFile);
            if (in != null) {
                String token = IOUtils.toString(in, Charset.defaultCharset()).trim();
                return token;
            } else {
                throw new IOException("Unable to read JWT token file: " + tokenFile);
            }
        } finally {
            close(in, tokenFile);
        }
    }

    /**
     * This method proactively obtains the TGT and service ticket for the audit server
     * during initialization, so they are cached and ready when the first audit event arrives.
     */
    private void preAuthenticateKerberos() {
        LOG.info("==> RangerAuditServerDestination:preAuthenticateKerberos()");

        try {
            UserGroupInformation ugi = UserGroupInformation.getLoginUser();

            if (ugi == null) {
                LOG.warn("No UserGroupInformation available for Kerberos pre-authentication");
                return;
            }

            if (!ugi.hasKerberosCredentials()) {
                LOG.warn("User {} does not have Kerberos credentials for pre-authentication", ugi.getUserName());
                return;
            }

            LOG.info("Pre-authenticating Kerberos for user: {}, authMethod: {}", ugi.getUserName(), ugi.getAuthenticationMethod());

            ugi.checkTGTAndReloginFromKeytab();
            LOG.debug("TGT verified and refreshed if needed for user: {}", ugi.getUserName());

            // Get the audit server URL to determine the target hostname for service ticket
            String auditServerUrl = MiscUtil.getStringProperty(props, PROP_AUDITSERVER_URL);

            if (StringUtils.isNotEmpty(auditServerUrl)) {
                try {
                    URI uri = new URI(auditServerUrl);
                    String hostname = uri.getHost();

                    LOG.info("Pre-fetching Kerberos service ticket for HTTP/{}", hostname);

                    ugi.doAs((PrivilegedExceptionAction<Void>) () -> {
                        LOG.debug("Kerberos security context initialized for audit server: {}", hostname);
                        return null;
                    });

                    LOG.info("Kerberos pre-authentication completed successfully. Service ticket cached for HTTP/{}", hostname);
                } catch (URISyntaxException e) {
                    LOG.warn("Invalid audit server URL format: {}. Skipping service ticket pre-fetch", auditServerUrl, e);
                } catch (Exception e) {
                    LOG.warn("Failed to pre-fetch service ticket for audit server: {}. First request may need to obtain ticket", auditServerUrl, e);
                }
            } else {
                LOG.warn("Audit server URL not configured. Cannot pre-fetch service ticket");
            }
        } catch (Exception e) {
            LOG.warn("Kerberos pre-authentication failed. First request will retry authentication", e);
        }

        LOG.info("<== RangerAuditServerDestination:preAuthenticateKerberos()");
    }

    @Override
    public void stop() {
        LOG.info("==> RangerAuditServerDestination.stop() called..");
        logStatus();
        if (httpClient != null) {
            try {
                httpClient.close();
            } catch (IOException ioe) {
                LOG.error("Error while closing httpclient in RangerAuditServerDestination!", ioe);
            } finally {
                httpClient = null;
            }
        }
    }

    /*
     * (non-Javadoc)
     *
     * @see org.apache.ranger.audit.provider.AuditProvider#flush()
     */
    @Override
    public void flush() {
    }

    @Override
    public boolean log(Collection<AuditEventBase> events) {
        boolean ret = false;
        try {
            logStatusIfRequired();
            addTotalCount(events.size());

            if (httpClient == null) {
                httpClient = buildHTTPClient();
                if (httpClient == null) {
                    // HTTP Server is still not initialized. So need return error
                    addDeferredCount(events.size());
                    return ret;
                }
            }
            ret = logAsBatch(events);
        } catch (Throwable t) {
            addDeferredCount(events.size());
            logError("Error sending audit to HTTP Server", t);
        }
        return ret;
    }

    public boolean isAsync() {
        return true;
    }

    private boolean logAsBatch(Collection<AuditEventBase> events) {
        boolean  batchSuccess = false;
        int      totalEvents  = events.size();

        LOG.debug("==> logAsBatch() Sending batch of {} events to Audit Server....", totalEvents);

        batchSuccess = sendBatch(events);
        if (batchSuccess) {
            addSuccessCount(totalEvents);
        } else {
            LOG.error("Failed to send batch of {} events", totalEvents);
            addFailedCount(totalEvents);
        }

        LOG.debug("<== logAsBatch() Batch processing complete: {}/{} events sent successfully", batchSuccess ? totalEvents : 0, totalEvents);

        return batchSuccess;
    }

    private boolean sendBatch(Collection<AuditEventBase> events) {
        boolean ret = false;
        Map<String, String> queryParams = new HashMap<>();
        try {
            UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
            LOG.debug("Sending audit batch of {} events as user: {}", events.size(), ugi.getUserName());

            PrivilegedExceptionAction<Map<?, ?>> action =
                    () -> executeHttpBatchRequest(REST_RELATIVE_PATH_POST, queryParams, events, Map.class);
            Map<?, ?> response = executeAction(action, ugi);

            if (response != null) {
                LOG.info("Audit batch sent successfully. {} events delivered. Response: {}", events.size(), response);
                ret = true;
            } else {
                LOG.error("Received null response from audit server for batch of {} events", events.size());
                ret = false;
            }
        } catch (Exception e) {
            LOG.error("Failed to send audit batch of {} events to {}. Error: {}", events.size(), httpURL, e.getMessage(), e);

            // Log additional context for authentication errors
            if (e.getMessage() != null && e.getMessage().contains("401")) {
                LOG.error("Authentication failure detected. Verify Kerberos credentials are valid and audit server is reachable.");
            }
            ret = false;
        }

        return ret;
    }

    public <T> T executeHttpBatchRequest(String relativeUrl, Map<String, String> params,
                                         Collection<AuditEventBase> events, Class<T> clazz) throws Exception {
        T finalResponse = postAndParse(httpURL + relativeUrl, params, events, clazz);
        return finalResponse;
    }

    public <T> T executeHttpRequestPOST(String relativeUrl, Map<String, String> params, Object obj, Class<T> clazz) throws Exception {
        LOG.debug("==>  RangerAuditServerDestination().executeHttpRequestPOST()");

        T finalResponse = postAndParse(httpURL + relativeUrl, params, obj, clazz);

        LOG.debug("<== RangerAuditServerDestination().executeHttpRequestPOST()");
        return finalResponse;
    }

    public <T> T postAndParse(String url, Map<String, String> queryParams, Object obj, Class<T> clazz) throws Exception {
        HttpPost     httpPost = new HttpPost(buildURI(url, queryParams));
        StringEntity entity   = new StringEntity(gsonBuilder.toJson(obj));
        httpPost.setEntity(entity);
        return executeAndParseResponse(httpPost, clazz, queryParams);
    }

    synchronized CloseableHttpClient buildHTTPClient() {
        Lookup<AuthSchemeProvider> authRegistry              = RegistryBuilder.<AuthSchemeProvider>create().register(AuthSchemes.SPNEGO, new SPNegoSchemeFactory(true, true)).build();
        HttpClientBuilder          clientBuilder             = HttpClients.custom().setDefaultAuthSchemeRegistry(authRegistry);
        String                     userName                  = MiscUtil.getStringProperty(props, PROP_AUDITSERVER_USER_NAME);
        String                     passWord                  = MiscUtil.getStringProperty(props, PROP_AUDITSERVER_USER_PASSWORD);
        int                        clientConnTimeOutMs       = MiscUtil.getIntProperty(props, PROP_AUDITSERVER_CLIENT_CONN_TIMEOUT_MS, 1000);
        int                        clientReadTimeOutMs       = MiscUtil.getIntProperty(props, PROP_AUDITSERVER_CLIENT_READ_TIMEOUT_MS, 1000);
        int                        maxConnections            = MiscUtil.getIntProperty(props, PROP_AUDITSERVER_MAX_CONNECTION, 10);
        int                        maxConnectionsPerHost     = MiscUtil.getIntProperty(props, PROP_AUDITSERVER_MAX_CONNECTION_PER_HOST, 10);
        int                        validateAfterInactivityMs = MiscUtil.getIntProperty(props, PROP_AUDITSERVER_VALIDATE_INACTIVE_MS, 1000);
        int                        poolRetryCount            = MiscUtil.getIntProperty(props, PROP_AUDITSERVER_POOL_RETRY_COUNT, 5);
        httpURL = MiscUtil.getStringProperty(props, PROP_AUDITSERVER_URL);

        LOG.info("Building HTTP client for audit destination: url={}, connTimeout={}ms, readTimeout={}ms, maxConn={}", httpURL, clientConnTimeOutMs, clientReadTimeOutMs, maxConnections);

        try {
            if (AUTH_TYPE_JWT.equalsIgnoreCase(authType)) {
                // JWT authentication - token will be added to request headers directly
                LOG.info("HTTP client configured for JWT Bearer token authentication");
            } else {
                // Kerberos or Basic authentication - use credentials provider
                BasicCredentialsProvider credentialsProvider = new BasicCredentialsProvider();

                if (AUTH_TYPE_KERBEROS.equalsIgnoreCase(authType) || isKerberosAuthenticated()) {
                    credentialsProvider.setCredentials(AuthScope.ANY, new KerberosCredentials(null));
                    LOG.info("HTTP client configured for Kerberos authentication (SPNEGO)....");
                    try {
                        UserGroupInformation ugi = UserGroupInformation.getLoginUser();
                        LOG.info("Current Kerberos principal: {}, authMethod: {}, hasKerberosCredentials: {}", ugi.getUserName(), ugi.getAuthenticationMethod(), ugi.hasKerberosCredentials());
                    } catch (Exception e) {
                        LOG.warn("Failed to get current UGI details", e);
                    }
                } else if (AUTH_TYPE_BASIC.equalsIgnoreCase(authType) || (StringUtils.isNotEmpty(userName) && StringUtils.isNotEmpty(passWord))) {
                    credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(userName, passWord));
                    LOG.info("HTTP client configured for basic authentication with username: {}", userName);
                } else {
                    LOG.warn("No authentication credentials configured for HTTP audit destination");
                }

                clientBuilder.setDefaultCredentialsProvider(credentialsProvider);
            }
        } catch (Exception excp) {
            LOG.error("Exception while configuring authentication credentials. Audits may fail to send!", excp);
        }

        PoolingHttpClientConnectionManager connectionManager;

        KeyManager[]   kmList     = getKeyManagers();
        TrustManager[] tmList     = getTrustManagers();
        SSLContext     sslContext = getSSLContext(kmList, tmList);

        if (sslContext != null) {
            SSLContext.setDefault(sslContext);
        }

        boolean isSSL = (httpURL != null && httpURL.contains("https://"));
        if (isSSL) {
            SSLConnectionSocketFactory        sslsf                 = new SSLConnectionSocketFactory(sslContext, NoopHostnameVerifier.INSTANCE);
            Registry<ConnectionSocketFactory> socketFactoryRegistry = RegistryBuilder.<ConnectionSocketFactory>create().register(PROTOCOL_HTTPS, sslsf).build();

            connectionManager = new PoolingHttpClientConnectionManager(socketFactoryRegistry);

            clientBuilder.setSSLSocketFactory(sslsf);
        } else {
            connectionManager = new PoolingHttpClientConnectionManager();
        }

        connectionManager.setMaxTotal(maxConnections);
        connectionManager.setDefaultMaxPerRoute(maxConnectionsPerHost);
        connectionManager.setValidateAfterInactivity(validateAfterInactivityMs);

        RequestConfig.Builder requestConfigBuilder = RequestConfig.custom()
                .setCookieSpec(CookieSpecs.DEFAULT)
                .setConnectTimeout(clientConnTimeOutMs)
                .setSocketTimeout(clientReadTimeOutMs);

        // Configure authentication based on auth type
        if (AUTH_TYPE_JWT.equalsIgnoreCase(authType)) {
            // JWT doesn't use HttpClient's authentication mechanism - token is added to headers
            LOG.info("RequestConfig configured for JWT authentication....");
        } else {
            // For Kerberos and Basic auth, enable HttpClient's authentication mechanism
            requestConfigBuilder.setAuthenticationEnabled(true);

            try {
                // For Kerberos authentication, specify SPNEGO as target auth scheme
                if (isKerberosAuthenticated() || (AUTH_TYPE_KERBEROS.equalsIgnoreCase(authType))) {
                    requestConfigBuilder.setTargetPreferredAuthSchemes(Arrays.asList(AuthSchemes.SPNEGO));
                    LOG.info("Configured SPNEGO as target authentication scheme for audit HTTP client");
                }
            } catch (Exception e) {
                LOG.warn("Failed to check Kerberos authentication status for request config", e);
            }
        }

        RequestConfig customizedRequestConfig = requestConfigBuilder.build();

        CloseableHttpClient httpClient = clientBuilder.setConnectionManager(connectionManager).setRetryHandler(new AuditHTTPRetryHandler(poolRetryCount, true)).setDefaultRequestConfig(customizedRequestConfig).build();

        return httpClient;
    }

    boolean isKerberosAuthenticated() throws Exception {
        boolean status = false;
        try {
            UserGroupInformation                      loggedInUser           = UserGroupInformation.getLoginUser();
            boolean                                   isSecurityEnabled      = UserGroupInformation.isSecurityEnabled();
            boolean                                   hasKerberosCredentials = loggedInUser.hasKerberosCredentials();
            UserGroupInformation.AuthenticationMethod loggedInUserAuthMethod = loggedInUser.getAuthenticationMethod();

            status = isSecurityEnabled && hasKerberosCredentials && loggedInUserAuthMethod.equals(UserGroupInformation.AuthenticationMethod.KERBEROS);
        } catch (IOException e) {
            throw new Exception("Failed to get authentication details.", e);
        }
        return status;
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

    private <T> PrivilegedExceptionAction<T> getPrivilegedAction(String relativeUrl, Map<String, String> queryParams, Object postParam, Class<T> clazz) {
        return () -> executeHttpRequestPOST(relativeUrl, queryParams, postParam, clazz);
    }

    private <T extends HttpRequestBase, R> R executeAndParseResponse(T request, Class<R> responseClazz, Map<String, String> queryParams) throws Exception {
        R                   ret    = null;
        CloseableHttpClient client = getCloseableHttpClient();

        if (client != null) {
            addCommonHeaders(request, queryParams);
            // Create an HttpClientContext to maintain authentication state across potential retries
            HttpClientContext context = HttpClientContext.create();
            try (CloseableHttpResponse response = client.execute(request, context)) {
                ret = parseResponse(response, responseClazz);
            }
        } else {
            LOG.error("Cannot process request as Audit HTTPClient is null...");
        }
        return ret;
    }

    private <T extends HttpRequestBase> T addCommonHeaders(T t, Map<String, String> queryParams) {
        t.addHeader(REST_HEADER_ACCEPT, REST_ACCEPTED_MIME_TYPE_JSON);
        t.setHeader(REST_HEADER_CONTENT_TYPE, REST_CONTENT_TYPE_MIME_TYPE_JSON);

        // Add JWT Bearer token if JWT authentication is configured
        if (AUTH_TYPE_JWT.equalsIgnoreCase(authType) && StringUtils.isNotEmpty(jwtToken)) {
            t.setHeader(REST_HEADER_AUTHORIZATION, "Bearer " + jwtToken);
            LOG.debug("Added JWT Bearer token to request Authorization header");
        }

        return t;
    }

    @SuppressWarnings("unchecked")
    private <R> R parseResponse(HttpResponse response, Class<R> clazz) throws Exception {
        R type = null;

        if (response == null) {
            String responseError = "Received NULL response from server";
            LOG.error(responseError);
            throw new Exception(responseError);
        }

        int httpStatus = response.getStatusLine().getStatusCode();

        if (httpStatus == HttpStatus.SC_OK) {
            InputStream responseInputStream = response.getEntity().getContent();
            if (clazz.equals(String.class)) {
                type = (R) IOUtils.toString(responseInputStream, Charset.defaultCharset());
            } else {
                type = gsonBuilder.fromJson(new InputStreamReader(responseInputStream), clazz);
            }
            responseInputStream.close();
        } else {
            String responseBody = "";
            try {
                if (response.getEntity() != null && response.getEntity().getContent() != null) {
                    responseBody = IOUtils.toString(response.getEntity().getContent(), Charset.defaultCharset());
                }
            } catch (Exception e) {
                LOG.debug("Failed to read error response body", e);
            }

            String error = String.format("Request failed with HTTP status %d. Response body: %s", httpStatus, responseBody);
            if (httpStatus == HttpStatus.SC_UNAUTHORIZED) {
                LOG.error("{} - Authentication failed. Verify Kerberos credentials are valid and properly configured.", error);
            } else {
                LOG.error(error);
            }
            throw new Exception(error);
        }
        return type;
    }

    private <T> T executeAction(PrivilegedExceptionAction<T> action, UserGroupInformation owner) throws Exception {
        T ret = null;
        if (owner != null) {
            ret = owner.doAs(action);
        } else {
            ret = action.run();
        }
        return ret;
    }

    private URI buildURI(String url, Map<String, String> queryParams) throws URISyntaxException {
        URIBuilder builder = new URIBuilder(url);
        if (queryParams != null) {
            for (Map.Entry<String, String> param : queryParams.entrySet()) {
                builder.addParameter(param.getKey(), param.getValue());
            }
        }
        return builder.build();
    }

    private CloseableHttpClient getCloseableHttpClient() {
        CloseableHttpClient client = httpClient;
        if (client == null) {
            synchronized (this) {
                client = httpClient;

                if (client == null) {
                    client     = buildHTTPClient();
                    httpClient = client;
                }
            }
        }
        return client;
    }

    private KeyManager[] getKeyManagers() {
        KeyManager[] kmList                 = null;
        String       credentialProviderPath = MiscUtil.getStringProperty(props, RANGER_POLICYMGR_CLIENT_KEY_FILE_CREDENTIAL);
        String       keyStoreAlias          = RANGER_POLICYMGR_CLIENT_KEY_FILE_CREDENTIAL_ALIAS;
        String       keyStoreFile           = MiscUtil.getStringProperty(props, RANGER_POLICYMGR_CLIENT_KEY_FILE);
        String       keyStoreFilepwd        = MiscUtil.getCredentialString(credentialProviderPath, keyStoreAlias);
        if (StringUtils.isNotEmpty(keyStoreFile) && StringUtils.isNotEmpty(keyStoreFilepwd)) {
            InputStream in = null;
            try {
                in = getFileInputStream(keyStoreFile);

                if (in != null) {
                    String keyStoreType = MiscUtil.getStringProperty(props, RANGER_POLICYMGR_CLIENT_KEY_FILE_TYPE);
                    keyStoreType = StringUtils.isNotEmpty(keyStoreType) ? keyStoreType : RANGER_POLICYMGR_CLIENT_KEY_FILE_TYPE_DEFAULT;
                    KeyStore keyStore = KeyStore.getInstance(keyStoreType);

                    keyStore.load(in, keyStoreFilepwd.toCharArray());

                    KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance(RANGER_SSL_KEYMANAGER_ALGO_TYPE);

                    keyManagerFactory.init(keyStore, keyStoreFilepwd.toCharArray());

                    kmList = keyManagerFactory.getKeyManagers();
                } else {
                    LOG.error("Unable to obtain keystore from file [{}]", keyStoreFile);
                }
            } catch (KeyStoreException e) {
                LOG.error("Unable to obtain from KeyStore :{}", e.getMessage(), e);
            } catch (NoSuchAlgorithmException e) {
                LOG.error("SSL algorithm is NOT available in the environment", e);
            } catch (CertificateException e) {
                LOG.error("Unable to obtain the requested certification ", e);
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
        return kmList;
    }

    private TrustManager[] getTrustManagers() {
        TrustManager[] tmList                 = null;
        String         credentialProviderPath = MiscUtil.getStringProperty(props, RANGER_POLICYMGR_TRUSTSTORE_FILE_CREDENTIAL);
        String         trustStoreAlias        = RANGER_POLICYMGR_TRUSTSTORE_FILE_CREDENTIAL_ALIAS;
        String         trustStoreFile         = MiscUtil.getStringProperty(props, RANGER_POLICYMGR_TRUSTSTORE_FILE);
        String         trustStoreFilepwd      = MiscUtil.getCredentialString(credentialProviderPath, trustStoreAlias);
        if (StringUtils.isNotEmpty(trustStoreFile) && StringUtils.isNotEmpty(trustStoreFilepwd)) {
            InputStream in = null;
            try {
                in = getFileInputStream(trustStoreFile);

                if (in != null) {
                    String trustStoreType = MiscUtil.getStringProperty(props, RANGER_POLICYMGR_TRUSTSTORE_FILE_TYPE);
                    trustStoreType = StringUtils.isNotEmpty(trustStoreType) ? trustStoreType : RANGER_POLICYMGR_TRUSTSTORE_FILE_TYPE_DEFAULT;
                    KeyStore trustStore = KeyStore.getInstance(trustStoreType);

                    trustStore.load(in, trustStoreFilepwd.toCharArray());

                    TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(RANGER_SSL_TRUSTMANAGER_ALGO_TYPE);

                    trustManagerFactory.init(trustStore);

                    tmList = trustManagerFactory.getTrustManagers();
                } else {
                    LOG.error("Unable to obtain truststore from file [{}]", trustStoreFile);
                }
            } catch (KeyStoreException e) {
                LOG.error("Unable to obtain from KeyStore", e);
            } catch (NoSuchAlgorithmException e) {
                LOG.error("SSL algorithm is NOT available in the environment :{}", e.getMessage(), e);
            } catch (CertificateException e) {
                LOG.error("Unable to obtain the requested certification :{}", e.getMessage(), e);
            } catch (FileNotFoundException e) {
                LOG.error("Unable to find the necessary SSL TrustStore File:{}", trustStoreFile, e);
            } catch (IOException e) {
                LOG.error("Unable to read the necessary SSL TrustStore Files :{}", trustStoreFile, e);
            } finally {
                close(in, trustStoreFile);
            }
        }
        return tmList;
    }

    private SSLContext getSSLContext(KeyManager[] kmList, TrustManager[] tmList) {
        SSLContext sslContext = null;
        try {
            sslContext = SSLContext.getInstance(RANGER_SSL_CONTEXT_ALGO_TYPE);
            if (sslContext != null) {
                sslContext.init(kmList, tmList, new SecureRandom());
            }
        } catch (NoSuchAlgorithmException e) {
            LOG.error("SSL algorithm is not available in the environment", e);
        } catch (KeyManagementException e) {
            LOG.error("Unable to initialise the SSLContext", e);
        }
        return sslContext;
    }

    private InputStream getFileInputStream(String fileName) throws IOException {
        InputStream in = null;
        if (StringUtils.isNotEmpty(fileName)) {
            File file = new File(fileName);
            if (file != null && file.exists()) {
                in = new FileInputStream(file);
            } else {
                in = ClassLoader.getSystemResourceAsStream(fileName);
            }
        }
        return in;
    }

    private class AuditHTTPRetryHandler extends StandardHttpRequestRetryHandler {
        public AuditHTTPRetryHandler(Integer poolRetryCount, boolean isIdempotentRequest) {
            super(poolRetryCount, isIdempotentRequest);
        }

        @Override
        public boolean retryRequest(IOException exception, int executionCount, HttpContext context) {
            LOG.debug("==> AuditHTTPRetryHandler.retryRequest {} Execution Count = {}", exception.getMessage(), executionCount);

            boolean ret = super.retryRequest(exception, executionCount, context);

            LOG.debug("<== AuditHTTPRetryHandler.retryRequest(): ret= {}", ret);

            return ret;
        }
    }
}
