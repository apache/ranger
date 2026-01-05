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

package org.apache.ranger.plugin.util;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.apache.hadoop.conf.Configuration;
import org.apache.ranger.authorization.hadoop.config.RangerPluginConfig;
import org.apache.ranger.authorization.hadoop.utils.RangerCredentialProvider;
import org.apache.ranger.authorization.utils.JsonUtils;
import org.apache.ranger.authorization.utils.StringUtil;
import org.apache.ranger.plugin.authn.JwtProvider;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.client.ClientProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSession;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.ws.rs.ProcessingException;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Cookie;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

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
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class RangerRESTClient {
    private static final Logger LOG = LoggerFactory.getLogger(RangerRESTClient.class);

    public static final String RANGER_PROP_POLICYMGR_URL                     = "ranger.service.store.rest.url";
    public static final String RANGER_PROP_POLICYMGR_SSLCONFIG_FILENAME      = "ranger.service.store.rest.ssl.config.file";
    public static final String RANGER_POLICYMGR_CLIENT_KEY_FILE                  = "xasecure.policymgr.clientssl.keystore";
    public static final String RANGER_POLICYMGR_CLIENT_KEY_FILE_TYPE             = "xasecure.policymgr.clientssl.keystore.type";
    public static final String RANGER_POLICYMGR_CLIENT_KEY_FILE_CREDENTIAL       = "xasecure.policymgr.clientssl.keystore.credential.file";
    public static final String RANGER_POLICYMGR_CLIENT_KEY_FILE_CREDENTIAL_ALIAS = "sslKeyStore";
    public static final String RANGER_POLICYMGR_CLIENT_KEY_FILE_TYPE_DEFAULT     = "jks";
    public static final String RANGER_POLICYMGR_TRUSTSTORE_FILE                  = "xasecure.policymgr.clientssl.truststore";
    public static final String RANGER_POLICYMGR_TRUSTSTORE_FILE_TYPE             = "xasecure.policymgr.clientssl.truststore.type";
    public static final String RANGER_POLICYMGR_TRUSTSTORE_FILE_CREDENTIAL       = "xasecure.policymgr.clientssl.truststore.credential.file";
    public static final String RANGER_POLICYMGR_TRUSTSTORE_FILE_CREDENTIAL_ALIAS = "sslTrustStore";
    public static final String RANGER_POLICYMGR_TRUSTSTORE_FILE_TYPE_DEFAULT     = "jks";
    public static final String RANGER_SSL_KEYMANAGER_ALGO_TYPE                   = KeyManagerFactory.getDefaultAlgorithm();
    public static final String RANGER_SSL_TRUSTMANAGER_ALGO_TYPE                 = TrustManagerFactory.getDefaultAlgorithm();
    public static final String RANGER_SSL_CONTEXT_ALGO_TYPE                      = "TLSv1.2";
    public static final String JWT_HEADER_PREFIX                                 = "Bearer ";

    public static final String RANGER_PROP_JWT_TOKEN_RETRIEVER_CLASS         = "ranger.common.auth.jwt.retriever.class";
    public static final String RANGER_PROP_JWT_TOKEN_RETRIEVER_CLASS_DEFAULT = "org.apache.ranger.plugin.token.JwTokenRetrieverEnv";
    public static final String RANGER_PROP_JWT_SERVER_COOKIE_NAME            = "ranger.common.auth.jwt.server.cookie.name";
    public static final String RANGER_PROP_JWT_IGNOREIF_OTHER_AUTH_EXISTS    = "ranger.common.auth.jwt.ignoreif.other.auth.exists";
    public static final String RANGER_PROP_JWT_ENABLED                       = "ranger.common.auth.jwt.enabled";

    public static final String JWT_COOKIE_NAME_DEFAULT                           = "hadoop-jwt";

    public static final String RANGER_PROP_AUTH_TYPE           = "AUTH_TYPE";
    public static final String RANGER_AUTH_TYPE_KERBEROS       = "KERBEROS";
    public static final String RANGER_AUTH_TYPE_RAZ_DT         = "RAZ-DT";
    public static final String RANGER_PROP_DT_OPERATION_TYPE   = "DT_OPERATION_TYPE";
    public static final String RANGER_DT_OPERATION_TYPE_GET    = "GETDELEGATIONTOKEN";
    public static final String RANGER_DT_OPERATION_TYPE_RENEW  = "RENEWDELEGATIONTOKEN";
    public static final String RANGER_DT_OPERATION_TYPE_CANCEL = "CANCELDELEGATIONTOKEN";

    private final    List<String> configuredURLs;
    private final    String       propertyPrefix;
    private          String       mUrl;
    private final    String       mSslConfigFileName;
    private          String       mUsername;
    private          String       mPassword;
    private          boolean      mIsSSL;
    private          String       mKeyStoreURL;
    private          String       mKeyStoreAlias;
    private          String       mKeyStoreFile;
    private          String       mKeyStoreType;
    private          String       mTrustStoreURL;
    private          String       mTrustStoreAlias;
    private          String       mTrustStoreFile;
    private          String       mTrustStoreType;
    private          int          mRestClientConnTimeOutMs;
    private          int          mRestClientReadTimeOutMs;
    private          int          maxRetryAttempts;
    private          int          retryIntervalMs;
    private          int          lastKnownActiveUrlIndex;
    private volatile Client       client;
    private volatile Client       cookieAuthClient;
    private          JwtProvider  jwtProvider;
    private volatile String       authHeader;

    public RangerRESTClient(String url, String sslConfigFileName, Configuration config) {
        this(url, sslConfigFileName, config, getPropertyPrefix(config));
    }

    public RangerRESTClient(String url, String sslConfigFileName, Configuration config, String propertyPrefix) {
        mUrl                = url;
        mSslConfigFileName  = sslConfigFileName;
        configuredURLs      = StringUtil.getURLs(mUrl);
        this.propertyPrefix = propertyPrefix;

        if (StringUtil.isEmpty(url)) {
            throw new IllegalArgumentException("Ranger URL is null or empty. Likely caused by incorrect configuration");
        } else {
            setLastKnownActiveUrlIndex((new Random()).nextInt(getConfiguredURLs().size()));
        }

        init(config);
    }

    public String getUrl() {
        return mUrl;
    }

    public void setUrl(String url) {
        this.mUrl = url;
    }

    public String getUsername() {
        return mUsername;
    }

    public String getPassword() {
        return mPassword;
    }

    public boolean isAuthFilterPresent() {
        return jwtProvider != null || hasBasicAuth();
    }

    public int getRestClientConnTimeOutMs() {
        return mRestClientConnTimeOutMs;
    }

    public void setRestClientConnTimeOutMs(int mRestClientConnTimeOutMs) {
        this.mRestClientConnTimeOutMs = mRestClientConnTimeOutMs;
    }

    public int getRestClientReadTimeOutMs() {
        return mRestClientReadTimeOutMs;
    }

    public void setRestClientReadTimeOutMs(int mRestClientReadTimeOutMs) {
        this.mRestClientReadTimeOutMs = mRestClientReadTimeOutMs;
    }

    public int getMaxRetryAttempts() {
        return maxRetryAttempts;
    }

    public void setMaxRetryAttempts(int maxRetryAttempts) {
        this.maxRetryAttempts = maxRetryAttempts;
    }

    public int getRetryIntervalMs() {
        return retryIntervalMs;
    }

    public void setRetryIntervalMs(int retryIntervalMs) {
        this.retryIntervalMs = retryIntervalMs;
    }

    public void setBasicAuthInfo(String username, String password) {
        setBasicAuthFilter(username, password);
    }

    public void setJwtProvider(JwtProvider jwtProvider) {
        this.jwtProvider = jwtProvider;
        resetClient();
    }

    public WebTarget getResource(String relativeUrl) {
        return getClient().target(getUrl() + relativeUrl);
    }

    public String toJson(Object obj) {
        return JsonUtils.objectToJson(obj);
    }

    public <T> T fromJson(String json, Class<T> cls) {
        return JsonUtils.jsonToObject(json, cls);
    }

    public Client getClient() {
        // result saves on access time when client is built at the time of the call
        Client result = client;

        if (result == null) {
            synchronized (this) {
                result = client;

                if (result == null) {
                    result = buildClient();
                    client = result;
                }
            }
        }

        return result;
    }

    protected void setClient(Client client) {
        this.client = client;
    }

    protected void setCookieAuthClient(Client cookieAuthClient) {
        this.cookieAuthClient = cookieAuthClient;
    }

    public void resetClient() {
        if (client != null) {
            client.close();
            client = null;
        }

        if (cookieAuthClient != null) {
            cookieAuthClient.close();
            cookieAuthClient = null;
        }
    }

    public KeyManager[] getKeyManagers(String keyStoreFile, String keyStoreFilePwd) {
        KeyManager[] kmList = null;

        if (StringUtils.isNotEmpty(keyStoreFile) && StringUtils.isNotEmpty(keyStoreFilePwd)) {
            InputStream in = null;

            try {
                in = getFileInputStream(keyStoreFile);

                if (in != null) {
                    KeyStore keyStore = KeyStore.getInstance(mKeyStoreType);

                    keyStore.load(in, keyStoreFilePwd.toCharArray());

                    KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance(RANGER_SSL_KEYMANAGER_ALGO_TYPE);

                    keyManagerFactory.init(keyStore, keyStoreFilePwd.toCharArray());

                    kmList = keyManagerFactory.getKeyManagers();
                } else {
                    LOG.error("Unable to obtain keystore from file [{}]", keyStoreFile);

                    throw new IllegalStateException("Unable to find keystore file :" + keyStoreFile);
                }
            } catch (KeyStoreException e) {
                LOG.error("Unable to obtain from KeyStore :{}", e.getMessage(), e);

                throw new IllegalStateException("Unable to init keystore:" + e.getMessage(), e);
            } catch (NoSuchAlgorithmException e) {
                LOG.error("SSL algorithm is NOT available in the environment", e);

                throw new IllegalStateException("SSL algorithm is NOT available in the environment :" + e.getMessage(), e);
            } catch (CertificateException e) {
                LOG.error("Unable to obtain the requested certification ", e);

                throw new IllegalStateException("Unable to obtain the requested certification :" + e.getMessage(), e);
            } catch (FileNotFoundException e) {
                LOG.error("Unable to find the necessary SSL Keystore Files", e);

                throw new IllegalStateException("Unable to find keystore file :" + keyStoreFile + ", error :" + e.getMessage(), e);
            } catch (IOException e) {
                LOG.error("Unable to read the necessary SSL Keystore Files", e);

                throw new IllegalStateException("Unable to read keystore file :" + keyStoreFile + ", error :" + e.getMessage(), e);
            } catch (UnrecoverableKeyException e) {
                LOG.error("Unable to recover the key from keystore", e);

                throw new IllegalStateException("Unable to recover the key from keystore :" + keyStoreFile + ", error :" + e.getMessage(), e);
            } finally {
                close(in, keyStoreFile);
            }
        }

        return kmList;
    }

    public TrustManager[] getTrustManagers(String trustStoreFile, String trustStoreFilepwd) {
        TrustManager[] tmList = null;

        if (StringUtils.isNotEmpty(trustStoreFile) && StringUtils.isNotEmpty(trustStoreFilepwd)) {
            InputStream in = null;

            try {
                in = getFileInputStream(trustStoreFile);

                if (in != null) {
                    KeyStore trustStore = KeyStore.getInstance(mTrustStoreType);

                    trustStore.load(in, trustStoreFilepwd.toCharArray());

                    TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(RANGER_SSL_TRUSTMANAGER_ALGO_TYPE);

                    trustManagerFactory.init(trustStore);

                    tmList = trustManagerFactory.getTrustManagers();
                } else {
                    LOG.error("Unable to obtain truststore from file [{}]", trustStoreFile);

                    throw new IllegalStateException("Unable to find truststore file :" + trustStoreFile);
                }
            } catch (KeyStoreException e) {
                LOG.error("Unable to obtain from KeyStore", e);

                throw new IllegalStateException("Unable to init keystore:" + e.getMessage(), e);
            } catch (NoSuchAlgorithmException e) {
                LOG.error("SSL algorithm is NOT available in the environment :{}", e.getMessage(), e);

                throw new IllegalStateException("SSL algorithm is NOT available in the environment :" + e.getMessage(), e);
            } catch (CertificateException e) {
                LOG.error("Unable to obtain the requested certification :{}", e.getMessage(), e);

                throw new IllegalStateException("Unable to obtain the requested certification :" + e.getMessage(), e);
            } catch (FileNotFoundException e) {
                LOG.error("Unable to find the necessary SSL TrustStore File:{}", trustStoreFile, e);

                throw new IllegalStateException("Unable to find trust store file :" + trustStoreFile + ", error :" + e.getMessage(), e);
            } catch (IOException e) {
                LOG.error("Unable to read the necessary SSL TrustStore Files :{}", trustStoreFile, e);

                throw new IllegalStateException("Unable to read the trust store file :" + trustStoreFile + ", error :" + e.getMessage(), e);
            } finally {
                close(in, trustStoreFile);
            }
        }

        return tmList;
    }

    public Response get(String relativeUrl, Map<String, String> params) throws Exception {
        return performRequest("GET", relativeUrl, params, null, null);
    }

    public Response get(String relativeUrl, Map<String, String> params, Cookie sessionId) throws Exception {
        return performRequest("GET", relativeUrl, params, null, sessionId);
    }

    public Response post(String relativeUrl, Map<String, String> params, Object obj) throws Exception {
        return performRequest("POST", relativeUrl, params, obj, null);
    }

    public Response post(String relativeURL, Map<String, String> params, Object obj, Cookie sessionId) throws Exception {
        return performRequest("POST", relativeURL, params, obj, sessionId);
    }

    public Response delete(String relativeUrl, Map<String, String> params) throws Exception {
        return performRequest("DELETE", relativeUrl, params, null, null);
    }

    public Response delete(String relativeURL, Map<String, String> params, Cookie sessionId) throws Exception {
        return performRequest("DELETE", relativeURL, params, null, sessionId);
    }

    public Response put(String relativeUrl, Map<String, String> params, Object obj) throws Exception {
        return performRequest("PUT", relativeUrl, params, obj, null);
    }

    public Response put(String relativeURL, Object request, Cookie sessionId) throws Exception {
        return performRequest("PUT", relativeURL, null, request, sessionId);
    }

    public int getLastKnownActiveUrlIndex() {
        return lastKnownActiveUrlIndex;
    }

    protected void setLastKnownActiveUrlIndex(int lastKnownActiveUrlIndex) {
        this.lastKnownActiveUrlIndex = lastKnownActiveUrlIndex % configuredURLs.size();
    }

    public List<String> getConfiguredURLs() {
        return configuredURLs;
    }

    public boolean isSSL() {
        return mIsSSL;
    }

    public void setSSL(boolean mIsSSL) {
        this.mIsSSL = mIsSSL;
    }

    protected SSLContext getSSLContext(KeyManager[] kmList, TrustManager[] tmList) {
        if (tmList == null) {
            try {
                String              algo = TrustManagerFactory.getDefaultAlgorithm();
                TrustManagerFactory tmf  = TrustManagerFactory.getInstance(algo);

                tmf.init((KeyStore) null);

                tmList = tmf.getTrustManagers();
            } catch (NoSuchAlgorithmException | KeyStoreException | IllegalStateException e) {
                LOG.error("Unable to get the default SSL TrustStore for the JVM", e);
                tmList = null;
            }
        }

        Validate.notNull(tmList, "TrustManager is not specified");

        try {
            SSLContext sslContext = SSLContext.getInstance(RANGER_SSL_CONTEXT_ALGO_TYPE);

            sslContext.init(kmList, tmList, new SecureRandom());

            return sslContext;
        } catch (NoSuchAlgorithmException e) {
            LOG.error("SSL algorithm is not available in the environment", e);

            throw new IllegalStateException("SSL algorithm is not available in the environment: " + e.getMessage(), e);
        } catch (KeyManagementException e) {
            LOG.error("Unable to initials the SSLContext", e);

            throw new IllegalStateException("Unable to initials the SSLContext: " + e.getMessage(), e);
        }
    }

    protected WebTarget setQueryParams(WebTarget webTarget, Map<String, String> params) {
        WebTarget ret = webTarget;

        if (params != null) {
            Set<Map.Entry<String, String>> entrySet = params.entrySet();

            for (Map.Entry<String, String> entry : entrySet) {
                ret = ret.queryParam(entry.getKey(), entry.getValue());
            }
        }

        return ret;
    }

    private Invocation.Builder createInvocationBuilder(int currentIndex, String relativeURL, Map<String, String> params, Cookie sessionId) {
        Client             clientToUse = sessionId != null ? getCookieAuthClient() : getClient();
        WebTarget          webTarget   = clientToUse.target(configuredURLs.get(currentIndex) + relativeURL);

        webTarget = setQueryParams(webTarget, params);

        Invocation.Builder builder = webTarget.request(MediaType.APPLICATION_JSON);

        if (sessionId != null) {
            builder = builder.cookie(sessionId);
        }

        return builder;
    }

    private Response performRequest(String method, String relativeUrl, Map<String, String> params, Object requestBody, Cookie sessionId) throws Exception {
        Response finalResponse = null;
        int      startIndex    = this.lastKnownActiveUrlIndex;
        int      retryAttempt  = 0;

        for (int index = 0; index < configuredURLs.size(); index++) {
            int currentIndex = (startIndex + index) % configuredURLs.size();

            try {
                Invocation.Builder builder = createInvocationBuilder(currentIndex, relativeUrl, params, sessionId);

                if ("POST".equalsIgnoreCase(method)) {
                    finalResponse = builder.post(Entity.entity(requestBody, MediaType.APPLICATION_JSON));
                } else if ("PUT".equalsIgnoreCase(method)) {
                    finalResponse = builder.put(Entity.entity(requestBody, MediaType.APPLICATION_JSON));
                } else if ("DELETE".equalsIgnoreCase(method)) {
                    finalResponse = builder.delete();
                } else {
                    finalResponse = builder.get();
                }

                if (finalResponse != null) {
                    int status = finalResponse.getStatus();

                    boolean success = status == Response.Status.OK.getStatusCode()
                            || status == Response.Status.NO_CONTENT.getStatusCode()
                            || status == Response.Status.NOT_MODIFIED.getStatusCode();

                    if (success) {
                        setLastKnownActiveUrlIndex(currentIndex);

                        break;
                    } else if (finalResponse.getStatusInfo().getFamily() == Response.Status.Family.SERVER_ERROR) {
                        throw new ProcessingException("Response status: " + finalResponse.getStatus());
                    } else {
                        LOG.error("Request to {} failed with HTTP status {}", configuredURLs.get(currentIndex), finalResponse.getStatus());

                        break;
                    }
                }
            } catch (ProcessingException | WebApplicationException ex) {
                if (shouldRetry(configuredURLs.get(currentIndex), index, retryAttempt, ex)) {
                    retryAttempt++;

                    index = -1; // start from first url
                }
            }
        }

        return finalResponse;
    }

    protected boolean shouldRetry(String currentUrl, int index, int retryAttemptCount, Exception ex) throws Exception {
        LOG.warn("Failed to communicate with Ranger Admin. URL: {}. Error: {}", currentUrl, ex.getMessage());

        boolean isLastUrl = index == (configuredURLs.size() - 1);

        // attempt retry after failure on the last url
        boolean ret = isLastUrl && (retryAttemptCount < maxRetryAttempts);

        if (ret) {
            LOG.warn("Waiting for {}ms before retry attempt #{}", retryIntervalMs, (retryAttemptCount + 1));

            try {
                TimeUnit.MILLISECONDS.sleep(retryIntervalMs);
            } catch (InterruptedException excp) {
                LOG.error("Failed while waiting to retry", excp);
                Thread.currentThread().interrupt();
                return false;
            }
        } else if (isLastUrl) {
            LOG.error("Failed to communicate with all Ranger Admin's URL's : [ {} ]", configuredURLs);

            throw ex;
        }

        return ret;
    }

    protected void setKeyStoreType(String mKeyStoreType) {
        this.mKeyStoreType = mKeyStoreType;
    }

    protected void setTrustStoreType(String mTrustStoreType) {
        this.mTrustStoreType = mTrustStoreType;
    }

    private static String getPropertyPrefix(Configuration config) {
        return (config instanceof RangerPluginConfig) ? ((RangerPluginConfig) config).getPropertyPrefix() : "ranger.plugin";
    }

    private Client getCookieAuthClient() {
        Client ret = cookieAuthClient;

        if (ret == null) {
            synchronized (this) {
                ret = cookieAuthClient;

                if (ret == null) {
                    cookieAuthClient = buildClient();
                    //Pending : need to remove basic auth filter from client.
                    ret = cookieAuthClient;
                }
            }
        }

        return ret;
    }

    private Client buildClient() {
        RangerJersey2ClientBuilder.SafeClientBuilder clientBuilder;
        ClientConfig config = new ClientConfig();

        if (mIsSSL) {
            try {
                KeyManager[]     kmList     = getKeyManagers();
                TrustManager[]   tmList     = getTrustManagers();
                SSLContext       sslContext = getSSLContext(kmList, tmList);
                HostnameVerifier hv = new HostnameVerifier() {
                    public boolean verify(String urlHostName, SSLSession session) {
                        return session.getPeerHost().equals(urlHostName);
                    }
                };

                // Use RangerJersey2ClientBuilder instead of unsafe ClientBuilder.newBuilder() to prevent MOXy usage
                clientBuilder = RangerJersey2ClientBuilder.newBuilder()
                        .sslContext(sslContext)
                        .hostnameVerifier(hv);
            } catch (Exception e) {
                LOG.error("Failed to build SSL client. Falling back to HTTP.");
                throw new IllegalStateException("Failed to build SSL client.", e);
            }
        } else {
            // Use RangerJersey2ClientBuilder instead of unsafe ClientBuilder.newBuilder() to prevent MOXy usage
            clientBuilder = RangerJersey2ClientBuilder.newBuilder();
        }

        // Apply centralized anti-MOXy configuration using the utility
        RangerJersey2ClientBuilder.applyAntiMoxyConfiguration(config);

        // Set timeouts
        config.property(ClientProperties.CONNECT_TIMEOUT, mRestClientConnTimeOutMs);
        config.property(ClientProperties.READ_TIMEOUT, mRestClientReadTimeOutMs);

        // Validate that MOXy prevention is properly configured
        RangerJersey2ClientBuilder.validateAntiMoxyConfiguration(config);

        if (jwtProvider != null) {
            config.register(new javax.ws.rs.client.ClientRequestFilter() {
                @Override
                public void filter(javax.ws.rs.client.ClientRequestContext requestContext) {
                    String header = getAuthHeader();

                    if (StringUtils.isNotEmpty(header)) {
                        authHeader = header;
                        requestContext.getHeaders().putSingle("Authorization", header);
                    }
                }
            });
        } else if (hasBasicAuth()) {
            authHeader = getBasicAuthHeader();
            config.register(new javax.ws.rs.client.ClientRequestFilter() {
                @Override
                public void filter(javax.ws.rs.client.ClientRequestContext requestContext) {
                    requestContext.getHeaders().add("Authorization", authHeader);
                }
            });
            LOG.info("Registering Basic auth header in REST client");
        } else {
            authHeader = null;
        }

        Client client = clientBuilder.withConfig(config).build();

        return client;
    }

    private void setJWTFilter() {
        JwtProvider provider = jwtProvider;

        if (provider != null) {
            LOG.info("Registering JWT auth header in REST client");
        }
    }

    private void setBasicAuthFilter(String username, String password) {
        if (StringUtils.isNotEmpty(username) && StringUtils.isNotEmpty(password)) {
            mUsername = username;
            mPassword = password;
        } else {
            mUsername = null;
            mPassword = null;
        }

        resetClient();
    }

    private void init(Configuration config) {
        mIsSSL = isSslEnabled(mUrl);

        if (mIsSSL) {
            InputStream in = null;

            try {
                in = getFileInputStream(mSslConfigFileName);

                if (in != null) {
                    config.addResource(in);
                }

                mKeyStoreURL   = config.get(RANGER_POLICYMGR_CLIENT_KEY_FILE_CREDENTIAL);
                mKeyStoreAlias = RANGER_POLICYMGR_CLIENT_KEY_FILE_CREDENTIAL_ALIAS;
                mKeyStoreType  = config.get(RANGER_POLICYMGR_CLIENT_KEY_FILE_TYPE, RANGER_POLICYMGR_CLIENT_KEY_FILE_TYPE_DEFAULT);
                mKeyStoreFile  = config.get(RANGER_POLICYMGR_CLIENT_KEY_FILE);

                mTrustStoreURL   = config.get(RANGER_POLICYMGR_TRUSTSTORE_FILE_CREDENTIAL);
                mTrustStoreAlias = RANGER_POLICYMGR_TRUSTSTORE_FILE_CREDENTIAL_ALIAS;
                mTrustStoreType  = config.get(RANGER_POLICYMGR_TRUSTSTORE_FILE_TYPE, RANGER_POLICYMGR_TRUSTSTORE_FILE_TYPE_DEFAULT);
                mTrustStoreFile  = config.get(RANGER_POLICYMGR_TRUSTSTORE_FILE);
            } catch (IOException ioe) {
                LOG.error("Unable to load SSL Config FileName: [{}]", mSslConfigFileName, ioe);
            } finally {
                close(in, mSslConfigFileName);
            }
        }

        String username = config.get(propertyPrefix + ".policy.rest.client.username");
        String password = config.get(propertyPrefix + ".policy.rest.client.password");

        setJWTFilter();

        if (StringUtils.isNotBlank(username) && StringUtils.isNotBlank(password)) {
            setBasicAuthFilter(username, password);
        }
    }

    private boolean isSslEnabled(String url) {
        return !StringUtils.isEmpty(url) && url.toLowerCase().startsWith("https");
    }

    private KeyManager[] getKeyManagers() {
        KeyManager[] kmList = null;

        String keyStoreFilepwd = getCredential(mKeyStoreURL, mKeyStoreAlias);

        kmList = getKeyManagers(mKeyStoreFile, keyStoreFilepwd);
        return kmList;
    }

    private TrustManager[] getTrustManagers() {
        TrustManager[] tmList = null;

        if (StringUtils.isNotEmpty(mTrustStoreURL) && StringUtils.isNotEmpty(mTrustStoreAlias)) {
            String trustStoreFilepwd = getCredential(mTrustStoreURL, mTrustStoreAlias);

            if (StringUtils.isNotEmpty(trustStoreFilepwd)) {
                tmList = getTrustManagers(mTrustStoreFile, trustStoreFilepwd);
            }
        }

        return tmList;
    }

    private String getCredential(String url, String alias) {
        return RangerCredentialProvider.getInstance().getCredentialString(url, alias);
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
                LOG.error("Error while closing file: [{}]", filename, excp);
            }
        }
    }

    private String getAuthHeader() {
        String currentJwt = getCurrentJwt();

        if (currentJwt != null) {
            return JWT_HEADER_PREFIX + currentJwt;
        }

        return hasBasicAuth() ? getBasicAuthHeader() : null;
    }

    private String getCurrentJwt() {
        JwtProvider provider = jwtProvider;

        return provider != null ? StringUtils.trimToNull(provider.getJwt()) : null;
    }

    private boolean hasBasicAuth() {
        return StringUtils.isNotEmpty(mUsername) && StringUtils.isNotEmpty(mPassword);
    }

    private String getBasicAuthHeader() {
        return "Basic " + java.util.Base64.getEncoder().encodeToString((mUsername + ":" + mPassword).getBytes());
    }
}
