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

package org.apache.ranger.audit.client;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.ranger.audit.model.RangerAuditMetrics;
import org.apache.ranger.audit.provider.MiscUtil;
import org.apache.ranger.audit.token.TokenRetriever;
import org.apache.ranger.audit.utils.StringUtil;
import org.apache.ranger.authorization.hadoop.utils.RangerCredentialProvider;
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
import java.security.PrivilegedAction;
import java.security.SecureRandom;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class RangerAuditMetricRESTClient extends AbstractRangerAuditMetricRESTClient {
    public static final  String  RANGER_PROP_JWT_TOKEN_RETRIEVER_CLASS             = "ranger.audit.auth.jwt.retriever.class";
    public static final  String  RANGER_PROP_JWT_TOKEN_RETRIEVER_CLASS_DEFAULT     = "org.apache.ranger.audit.token.JwTokenRetrieverEnv";
    public static final  String  RANGER_PROP_JWT_SERVER_COOKIE_NAME                = "ranger.audit.auth.jwt.server.cookie.name";
    public static final  String  RANGER_PROP_JWT_IGNOREIF_OTHER_AUTH_EXISTS        = "ranger.audit.auth.jwt.ignoreif.other.auth.exists";
    public static final  String  RANGER_PROP_JWT_ENABLED                           = "ranger.audit.auth.jwt.enabled";
    public static final  String  RANGER_PROP_AUTH_TYPE                             = "AUTH_TYPE";
    public static final  String  RANGER_AUTH_TYPE_KERBEROS                         = "KERBEROS";
    public static final  String  RANGER_AUTH_TYPE_RAZ_DT                           = "RAZ-DT";
    public static final  String  RANGER_PROP_DT_OPERATION_TYPE                     = "DT_OPERATION_TYPE";
    public static final  String  RANGER_DT_OPERATION_TYPE_GET                      = "GETDELEGATIONTOKEN";
    public static final  String  RANGER_DT_OPERATION_TYPE_RENEW                    = "RENEWDELEGATIONTOKEN";
    public static final  String  RANGER_DT_OPERATION_TYPE_CANCEL                   = "CANCELDELEGATIONTOKEN";
    public static final  String  RANGER_POLICYMGR_CLIENT_KEY_FILE                  = "xasecure.policymgr.clientssl.keystore";
    public static final  String  RANGER_POLICYMGR_CLIENT_KEY_FILE_TYPE             = "xasecure.policymgr.clientssl.keystore.type";
    public static final  String  RANGER_POLICYMGR_CLIENT_KEY_FILE_CREDENTIAL       = "xasecure.policymgr.clientssl.keystore.credential.file";
    public static final  String  RANGER_POLICYMGR_CLIENT_KEY_FILE_CREDENTIAL_ALIAS = "sslKeyStore";
    public static final  String  RANGER_POLICYMGR_CLIENT_KEY_FILE_TYPE_DEFAULT     = "jks";
    public static final  String  RANGER_POLICYMGR_TRUSTSTORE_FILE                  = "xasecure.policymgr.clientssl.truststore";
    public static final  String  RANGER_POLICYMGR_TRUSTSTORE_FILE_TYPE             = "xasecure.policymgr.clientssl.truststore.type";
    public static final  String  RANGER_POLICYMGR_TRUSTSTORE_FILE_CREDENTIAL       = "xasecure.policymgr.clientssl.truststore.credential.file";
    public static final  String  RANGER_POLICYMGR_TRUSTSTORE_FILE_CREDENTIAL_ALIAS = "sslTrustStore";
    public static final  String  RANGER_POLICYMGR_TRUSTSTORE_FILE_TYPE_DEFAULT     = "jks";
    public static final  String  JWT_COOKIE_NAME_DEFAULT                           = "hadoop-jwt";
    public static final  String  REST_URL_CREATE_AUDIT_METRICS                     = "/service/audit/metrics";
    public static final  String  RANGER_PROP_SERVICE_TYPE                          = "ranger.plugin.serviceType";
    public static final  String  RANGER_PROP_CLIENT_CONNECTION_TIMEOUT_MS          = "ranger.plugin.%s.policy.rest.client.connection.timeoutMs";
    public static final  String  RANGER_PROP_CLIENT_READ_TIMEOUT_MS                = "ranger.plugin.%s.policy.rest.client.read.timeoutMs";
    public static final  String  RANGER_PROP_CLIENT_MAX_RETRY_ATTEMPTS             = "ranger.plugin.%s.policy.rest.client.max.retry.attempts";
    public static final  String  RANGER_PROP_CLIENT_RETRY_INTERVAL_MS              = "ranger.plugin.%s.policy.rest.client.retry.interval.ms";
    public static final  String  RANGER_SSL_KEYMANAGER_ALGO_TYPE                   = KeyManagerFactory.getDefaultAlgorithm();
    public static final  String  RANGER_SSL_TRUSTMANAGER_ALGO_TYPE                 = TrustManagerFactory.getDefaultAlgorithm();
    public static final  String  RANGER_SSL_CONTEXT_ALGO_TYPE                      = "TLS";
    public static final  String  REST_EXPECTED_MIME_TYPE                           = "application/json";
    public static final  String  REST_MIME_TYPE_JSON                               = "application/json";
    private static final Logger  LOG                                               = LoggerFactory.getLogger(RangerAuditMetricRESTClient.class);
    private              String  mUrl;
    private              String  mSslConfigFileName;
    private              String  mUsername;
    private              String  mPassword;
    private              boolean mIsSSL;

    private String mKeyStoreURL;
    private String mKeyStoreAlias;
    private String mKeyStoreFile;
    private String mKeyStoreType;
    private String mTrustStoreURL;
    private String mTrustStoreAlias;
    private String mTrustStoreFile;
    private String mTrustStoreType;
    private String serviceType;

    private Gson gsonBuilder;
    private int  mRestClientConnTimeOutMs;
    private int  mRestClientReadTimeOutMs;
    private int  maxRetryAttempts;
    private int  retryIntervalMs;
    private int  lastKnownActiveUrlIndex;

    private          List<String>           configuredURLs;
    private volatile Client                 client;
    private          TokenRetriever<String> tokenRetriever;
    private          String                 jwtServerCookieName;
    private          boolean                ignoreJwtIfAuthExists;

    protected WebTarget setQueryParams(WebTarget webTarget, Map<String, String> params) {
        WebTarget ret = webTarget;

        if (webTarget != null && params != null) {
            Set<Map.Entry<String, String>> entrySet = params.entrySet();
            for (Map.Entry<String, String> entry : entrySet) {
                ret = ret.queryParam(entry.getKey(), entry.getValue());
            }
        }

        return ret;
    }

    @Override
    public void init(String url, String sslConfigFileName, Configuration config) {
        mUrl               = url;
        mSslConfigFileName = sslConfigFileName;
        configuredURLs     = StringUtil.getURLs(mUrl);
        setLastKnownActiveUrlIndex((new Random()).nextInt(getConfiguredURLs().size()));

        serviceType              = config.get(RANGER_PROP_SERVICE_TYPE);
        mRestClientConnTimeOutMs = config.getInt(String.format(RANGER_PROP_CLIENT_CONNECTION_TIMEOUT_MS, serviceType), 120 * 1000);
        mRestClientReadTimeOutMs = config.getInt(String.format(RANGER_PROP_CLIENT_READ_TIMEOUT_MS, serviceType), 30 * 1000);
        maxRetryAttempts         = config.getInt(String.format(RANGER_PROP_CLIENT_MAX_RETRY_ATTEMPTS, serviceType), 3);
        retryIntervalMs          = config.getInt(String.format(RANGER_PROP_CLIENT_RETRY_INTERVAL_MS, serviceType), 1000);

        init(config);
    }

    @Override
    public RangerAuditMetrics createAuditMetrics(RangerAuditMetrics request) throws Exception {
        LOG.debug("==> RangerAdminRESTClient.createAuditMetrics({})", request);

        RangerAuditMetrics ret = null;

        Response             response     = null;
        UserGroupInformation user         = MiscUtil.getUGILoginUser();
        boolean              isSecureMode = user != null && UserGroupInformation.isSecurityEnabled();
        String               relativeURL  = REST_URL_CREATE_AUDIT_METRICS;
        Map<String, String>  queryParams  = new HashMap<>();

        if (isSecureMode) {
            PrivilegedAction<Response> action = new PrivilegedAction<Response>() {
                public Response run() {
                    Response clientResp = null;
                    try {
                        clientResp = post(relativeURL, queryParams, request);
                    } catch (Exception e) {
                        LOG.error("Failed to get response, Error is : {}", e.getMessage());
                    }
                    return clientResp;
                }
            };

            LOG.debug("create createAuditMetrics as user {}", user);
            response = user.doAs(action);
        } else {
            response = post(relativeURL, queryParams, request);
        }

        if (response != null) {
            if (response.getStatus() != Response.Status.OK.getStatusCode()) {
                RESTResponse resp = RESTResponse.fromClientResponse(response);
                LOG.error("createAuditMetrics() failed: HTTP status={}, message={}, isSecure={}", response.getStatus(), resp.getMessage(), isSecureMode + (isSecureMode ? (", user=" + user) : ""));

                if (response.getStatus() == Response.Status.UNAUTHORIZED.getStatusCode()) {
                    throw new AccessControlException("Unauthorized access.");
                }

                throw new Exception("HTTP " + response.getStatus() + " Error: " + resp.getMessage());
            } else {
                ret = response.readEntity(RangerAuditMetrics.class);
            }
        } else {
            throw new Exception("unknown error during createAuditMetrics. AuditMetrics = " + request);
        }

        LOG.debug("<== RangerAdminRESTClient.createAuditMetrics({})", request);

        return ret;
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
        mUsername = username;
        mPassword = password;
    }

    public String toJson(Object obj) {
        return gsonBuilder.toJson(obj);
    }

    public <T> T fromJson(String json, Class<T> cls) {
        return gsonBuilder.fromJson(json, cls);
    }

    public Client getClient() {
        // result saves on access time when client is built at the time of the call
        Client result = client;
        if (result == null) {
            synchronized (this) {
                result = client;
                if (result == null) {
                    client = buildClient();
                    result = client;
                }
            }
        }

        return result;
    }

    protected void setClient(Client client) {
        this.client = client;
    }

    public void resetClient() {
        client = null;
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
        return performRequest("GET", relativeUrl, params, null);
    }

    public Response get(String relativeUrl, Map<String, String> params, Cookie sessionId) throws Exception {
        return performRequest("GET", relativeUrl, params, sessionId);
    }

    public Response post(String relativeUrl, Map<String, String> params, Object obj) throws Exception {
        return performRequest("POST", relativeUrl, params, obj);
    }

    private Response performRequest(String method, String relativeUrl, Map<String, String> params, Object requestBody) throws Exception {
        Response finalResponse = null;
        int      startIndex    = this.lastKnownActiveUrlIndex;
        int      retryAttempt  = 0;

        for (int index = 0; index < configuredURLs.size(); index++) {
            int currentIndex = (startIndex + index) % configuredURLs.size();
            try {
                Client clientToUse = getClient();

                WebTarget webTarget = clientToUse.target(configuredURLs.get(currentIndex) + relativeUrl);
                webTarget = setQueryParams(webTarget, params);

                Invocation.Builder builder = webTarget.request(MediaType.APPLICATION_JSON);

                Invocation.Builder finalBuilder = handleJwt(builder, params);

                if ("POST".equalsIgnoreCase(method)) {
                    finalResponse = finalBuilder.post(Entity.entity(requestBody, MediaType.APPLICATION_JSON));
                } else {
                    finalResponse = finalBuilder.get();
                }

                if (finalResponse != null) {
                    setLastKnownActiveUrlIndex(currentIndex);
                    break;
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

    public int getLastKnownActiveUrlIndex() {
        return lastKnownActiveUrlIndex;
    }

    protected void setLastKnownActiveUrlIndex(int lastKnownActiveUrlIndex) {
        this.lastKnownActiveUrlIndex = lastKnownActiveUrlIndex;
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
            throw new IllegalStateException("Unable to initials the SSLContex: " + e.getMessage(), e);
        }
    }

    protected boolean shouldRetry(String currentUrl, int index, int retryAttemptCount, Exception ex) throws Exception {
        LOG.warn("Failed to communicate with Ranger Admin URL: {} Error: {}", currentUrl, ex.getMessage());

        boolean isLastUrl = index == (configuredURLs.size() - 1);

        // attempt retry after failure on the last url
        boolean ret = isLastUrl && (retryAttemptCount < maxRetryAttempts);

        if (ret) {
            LOG.warn("Waiting for {} ms before retry attempt #{}", retryIntervalMs, (retryAttemptCount + 1));

            try {
                TimeUnit.MILLISECONDS.sleep(retryIntervalMs);
            } catch (InterruptedException excp) {
                LOG.error("Failed while waiting to retry", excp);
                Thread.currentThread().interrupt();
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

    private Client buildClient() {
        Client client;

        // Use RangerJersey2ClientBuilder to create clients with MOXy prevention
        if (mIsSSL) {
            try {
                KeyManager[]   kmList     = getKeyManagers();
                TrustManager[] tmList     = getTrustManagers();
                SSLContext     sslContext = getSSLContext(kmList, tmList);

                HostnameVerifier hv = new HostnameVerifier() {
                    public boolean verify(String urlHostName, SSLSession session) {
                        return session.getPeerHost().equals(urlHostName);
                    }
                };

                // Use RangerJersey2ClientBuilder to create secure client with MOXy prevention
                client = RangerJersey2ClientBuilder.createSecureClient(sslContext, hv, mRestClientConnTimeOutMs, mRestClientReadTimeOutMs);
            } catch (Exception e) {
                LOG.error("Failed to build SSL client. Falling back to HTTP.");
                throw new IllegalStateException("Failed to build SSL client.", e);
            }
        } else {
            // Use RangerJersey2ClientBuilder to create standard client with MOXy prevention
            client = RangerJersey2ClientBuilder.createClient(mRestClientConnTimeOutMs, mRestClientReadTimeOutMs);
        }

        // Register basic authentication filter if credentials are provided
        if (StringUtils.isNotEmpty(mUsername) && StringUtils.isNotEmpty(mPassword)) {
            client.register(new javax.ws.rs.client.ClientRequestFilter() {
                @Override
                public void filter(javax.ws.rs.client.ClientRequestContext requestContext) {
                    String authHeader = "Basic " + java.util.Base64.getEncoder().encodeToString((mUsername + ":" + mPassword).getBytes());
                    requestContext.getHeaders().add("Authorization", authHeader);
                }
            });
        }

        return client;
    }

    private void init(Configuration config) {
        try {
            gsonBuilder = new GsonBuilder().setDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ").create();
        } catch (Throwable excp) {
            LOG.error("RangerRESTClient.init(): failed to create GsonBuilder object", excp);
        }

        mIsSSL = isSsl(mUrl);

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

        boolean isJwtFetcherEnabled = config.getBoolean(RANGER_PROP_JWT_ENABLED, true);
        if (isJwtFetcherEnabled) {
            tokenRetriever      = getJwtTokenRetriever(config);
            jwtServerCookieName = config.get(RANGER_PROP_JWT_SERVER_COOKIE_NAME, JWT_COOKIE_NAME_DEFAULT);
        } else {
            LOG.warn("RangerAdminJersey2RESTClient.init(): Skipping JWT fetcher, use property {} to enable.", RANGER_PROP_JWT_ENABLED);
        }

        ignoreJwtIfAuthExists = config.getBoolean(RANGER_PROP_JWT_IGNOREIF_OTHER_AUTH_EXISTS, ignoreJwtIfAuthExists);
    }

    @SuppressWarnings("unchecked")
    private TokenRetriever<String> getJwtTokenRetriever(Configuration config) {
        String                        clsName   = config.get(RANGER_PROP_JWT_TOKEN_RETRIEVER_CLASS, RANGER_PROP_JWT_TOKEN_RETRIEVER_CLASS_DEFAULT);
        ClassLoader                   clsLoader = Thread.currentThread().getContextClassLoader();
        TokenRetriever<String>        ret       = null;
        Class<TokenRetriever<String>> cls;

        try {
            cls = (Class<TokenRetriever<String>>) clsLoader.loadClass(clsName.trim());
            ret = cls.getConstructor(Configuration.class).newInstance(config);
        } catch (Exception e) {
            LOG.error("RangerAdminJersey2RESTClient.getJwtTokenRetriever(): Failed to initialize JWT token retriever.", e);
        }

        return ret;
    }

    private boolean isSsl(String url) {
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

    private Invocation.Builder handleJwt(Invocation.Builder builder, Map<String, String> queryParams) {
        if (!isDtOperation(queryParams) && !(ignoreJwtIfAuthExists && otherAuthCredExists(queryParams))) {
            if (tokenRetriever != null) {
                Optional<String> jwtOptional = tokenRetriever.retrieve();

                if (jwtOptional.isPresent()) {
                    builder.cookie(new Cookie(jwtServerCookieName, jwtOptional.get()));
                }
            } else {
                LOG.warn("RangerAdminRESTClient.handleJwt(): Since JWTokenRetriever init failed, skipping JWT auth.");
            }
        } else {
            LOG.debug("RangerAdminRESTClient.handleJwt(): Skipping JWT as required condition does not meet.");
            LOG.debug("RangerAdminRESTClient.handleJwt(): [isDtOperation(queryParams)={}], [ignoreJwtIfAuthExists={}], [otherAuthCredExists(queryParams)={}]", isDtOperation(queryParams), ignoreJwtIfAuthExists, otherAuthCredExists(queryParams));
        }

        return builder;
    }

    private boolean otherAuthCredExists(Map<String, String> queryParams) {
        boolean ret = false;

        if (queryParams != null && (!queryParams.isEmpty())) {
            String authType = queryParams.get(RANGER_PROP_AUTH_TYPE);
            if (StringUtils.isNotBlank(authType)) {
                ret = true;
            }
        }

        return ret;
    }

    private boolean isDtOperation(Map<String, String> queryParams) {
        boolean ret = false;

        if (queryParams != null && (!queryParams.isEmpty())) {
            String authType = queryParams.get(RANGER_PROP_DT_OPERATION_TYPE);
            if (StringUtils.isNotBlank(authType)) {
                ret = true;
            }
        }

        return ret;
    }
}
