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

import com.fasterxml.jackson.jaxrs.json.JacksonJsonProvider;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientHandlerException;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import com.sun.jersey.api.client.filter.ClientFilter;
import com.sun.jersey.api.client.filter.HTTPBasicAuthFilter;
import com.sun.jersey.client.urlconnection.HTTPSProperties;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.Validate;
import org.apache.hadoop.conf.Configuration;
import org.apache.ranger.authorization.hadoop.config.RangerPluginConfig;
import org.apache.ranger.authorization.hadoop.utils.RangerCredentialProvider;
import org.apache.ranger.authorization.utils.JsonUtils;
import org.apache.ranger.authorization.utils.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.ws.rs.core.Cookie;
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

public class RangerRESTClient {
    private static final Logger LOG = LoggerFactory.getLogger(RangerRESTClient.class);

    public static final String RANGER_PROP_POLICYMGR_URL                         = "ranger.service.store.rest.url";
    public static final String RANGER_PROP_POLICYMGR_SSLCONFIG_FILENAME          = "ranger.service.store.rest.ssl.config.file";
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

    private final    List<String> configuredURLs;
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
    private          ClientFilter basicAuthFilter;

    public RangerRESTClient(String url, String sslConfigFileName, Configuration config) {
        mUrl               = url;
        mSslConfigFileName = sslConfigFileName;
        configuredURLs     = StringUtil.getURLs(mUrl);

        setLastKnownActiveUrlIndex((new Random()).nextInt(getConfiguredURLs().size()));

        init(config);
    }

    protected static WebResource setQueryParams(WebResource webResource, Map<String, String> params) {
        WebResource ret = webResource;

        if (webResource != null && params != null) {
            for (Map.Entry<String, String> entry : params.entrySet()) {
                ret = ret.queryParam(entry.getKey(), entry.getValue());
            }
        }

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

        setBasicAuthFilter(username, password);
    }

    public WebResource getResource(String relativeUrl) {
        return getClient().resource(getUrl() + relativeUrl);
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

    public ClientResponse get(String relativeUrl, Map<String, String> params) throws Exception {
        ClientResponse finalResponse = null;
        int            startIndex    = this.lastKnownActiveUrlIndex;
        int            retryAttempt  = 0;

        for (int index = 0; index < configuredURLs.size(); index++) {
            int currentIndex = (startIndex + index) % configuredURLs.size();

            try {
                WebResource.Builder webResource = createWebResource(currentIndex, relativeUrl, params);

                finalResponse = webResource.accept(RangerRESTUtils.REST_EXPECTED_MIME_TYPE).type(RangerRESTUtils.REST_MIME_TYPE_JSON).get(ClientResponse.class);

                if (finalResponse != null) {
                    if (finalResponse.getStatusInfo().getFamily() == Response.Status.Family.SERVER_ERROR) {
                        throw new ClientHandlerException("Response status: " + finalResponse.getStatus());
                    }

                    setLastKnownActiveUrlIndex(currentIndex);

                    break;
                }
            } catch (ClientHandlerException ex) {
                if (shouldRetry(configuredURLs.get(currentIndex), index, retryAttempt, ex)) {
                    retryAttempt++;

                    index = -1; // start from first url
                }
            }
        }

        return finalResponse;
    }

    public ClientResponse get(String relativeUrl, Map<String, String> params, Cookie sessionId) throws Exception {
        ClientResponse finalResponse = null;
        int            startIndex    = this.lastKnownActiveUrlIndex;
        int            retryAttempt  = 0;

        for (int index = 0; index < configuredURLs.size(); index++) {
            int currentIndex = (startIndex + index) % configuredURLs.size();

            try {
                WebResource.Builder br = createWebResource(currentIndex, relativeUrl, params, sessionId);

                finalResponse = br.accept(RangerRESTUtils.REST_EXPECTED_MIME_TYPE).type(RangerRESTUtils.REST_MIME_TYPE_JSON).get(ClientResponse.class);

                if (finalResponse != null) {
                    if (finalResponse.getStatusInfo().getFamily() == Response.Status.Family.SERVER_ERROR) {
                        throw new ClientHandlerException("Response status: " + finalResponse.getStatus());
                    }

                    setLastKnownActiveUrlIndex(currentIndex);

                    break;
                }
            } catch (ClientHandlerException ex) {
                if (shouldRetry(configuredURLs.get(currentIndex), index, retryAttempt, ex)) {
                    retryAttempt++;

                    index = -1; // start from first url
                }
            }
        }

        return finalResponse;
    }

    public ClientResponse post(String relativeUrl, Map<String, String> params, Object obj) throws Exception {
        ClientResponse finalResponse = null;
        int            startIndex    = this.lastKnownActiveUrlIndex;
        int            retryAttempt  = 0;

        for (int index = 0; index < configuredURLs.size(); index++) {
            int currentIndex = (startIndex + index) % configuredURLs.size();

            try {
                WebResource.Builder webResource = createWebResource(currentIndex, relativeUrl, params);

                finalResponse = webResource.accept(RangerRESTUtils.REST_EXPECTED_MIME_TYPE).type(RangerRESTUtils.REST_MIME_TYPE_JSON).post(ClientResponse.class, toJson(obj));

                if (finalResponse != null) {
                    setLastKnownActiveUrlIndex(currentIndex);

                    break;
                }
            } catch (ClientHandlerException ex) {
                if (shouldRetry(configuredURLs.get(currentIndex), index, retryAttempt, ex)) {
                    retryAttempt++;

                    index = -1; // start from first url
                }
            }
        }

        return finalResponse;
    }

    public ClientResponse post(String relativeURL, Map<String, String> params, Object obj, Cookie sessionId) throws Exception {
        ClientResponse response     = null;
        int            startIndex   = this.lastKnownActiveUrlIndex;
        int            retryAttempt = 0;

        for (int index = 0; index < configuredURLs.size(); index++) {
            int currentIndex = (startIndex + index) % configuredURLs.size();

            try {
                WebResource.Builder br = createWebResource(currentIndex, relativeURL, params, sessionId);

                response = br.accept(RangerRESTUtils.REST_EXPECTED_MIME_TYPE).type(RangerRESTUtils.REST_MIME_TYPE_JSON).post(ClientResponse.class, toJson(obj));

                if (response != null) {
                    setLastKnownActiveUrlIndex(currentIndex);

                    break;
                }
            } catch (ClientHandlerException ex) {
                if (shouldRetry(configuredURLs.get(currentIndex), index, retryAttempt, ex)) {
                    retryAttempt++;

                    index = -1; // start from first url
                }
            }
        }

        return response;
    }

    public ClientResponse delete(String relativeUrl, Map<String, String> params) throws Exception {
        ClientResponse finalResponse = null;
        int            startIndex    = this.lastKnownActiveUrlIndex;
        int            retryAttempt  = 0;

        for (int index = 0; index < configuredURLs.size(); index++) {
            int currentIndex = (startIndex + index) % configuredURLs.size();

            try {
                WebResource.Builder webResource = createWebResource(currentIndex, relativeUrl, params);

                finalResponse = webResource.accept(RangerRESTUtils.REST_EXPECTED_MIME_TYPE).type(RangerRESTUtils.REST_MIME_TYPE_JSON).delete(ClientResponse.class);

                if (finalResponse != null) {
                    setLastKnownActiveUrlIndex(currentIndex);

                    break;
                }
            } catch (ClientHandlerException ex) {
                if (shouldRetry(configuredURLs.get(currentIndex), index, retryAttempt, ex)) {
                    retryAttempt++;

                    index = -1; // start from first url
                }
            }
        }

        return finalResponse;
    }

    public ClientResponse delete(String relativeURL, Map<String, String> params, Cookie sessionId) throws Exception {
        ClientResponse response     = null;
        int            startIndex   = this.lastKnownActiveUrlIndex;
        int            retryAttempt = 0;

        for (int index = 0; index < configuredURLs.size(); index++) {
            int currentIndex = (startIndex + index) % configuredURLs.size();

            try {
                WebResource.Builder br = createWebResource(currentIndex, relativeURL, params, sessionId);

                response = br.delete(ClientResponse.class);

                if (response != null) {
                    setLastKnownActiveUrlIndex(currentIndex);

                    break;
                }
            } catch (ClientHandlerException ex) {
                if (shouldRetry(configuredURLs.get(currentIndex), index, retryAttempt, ex)) {
                    retryAttempt++;

                    index = -1; // start from first url
                }
            }
        }

        return response;
    }

    public ClientResponse put(String relativeUrl, Map<String, String> params, Object obj) throws Exception {
        ClientResponse finalResponse = null;
        int            startIndex    = this.lastKnownActiveUrlIndex;
        int            retryAttempt  = 0;

        for (int index = 0; index < configuredURLs.size(); index++) {
            int currentIndex = (startIndex + index) % configuredURLs.size();

            try {
                WebResource.Builder webResource = createWebResource(currentIndex, relativeUrl, params);

                finalResponse = webResource.accept(RangerRESTUtils.REST_EXPECTED_MIME_TYPE).type(RangerRESTUtils.REST_MIME_TYPE_JSON).put(ClientResponse.class, toJson(obj));

                if (finalResponse != null) {
                    setLastKnownActiveUrlIndex(currentIndex);

                    break;
                }
            } catch (ClientHandlerException ex) {
                if (shouldRetry(configuredURLs.get(currentIndex), index, retryAttempt, ex)) {
                    retryAttempt++;

                    index = -1; // start from first url
                }
            }
        }

        return finalResponse;
    }

    public ClientResponse put(String relativeURL, Object request, Cookie sessionId) throws Exception {
        ClientResponse response     = null;
        int            startIndex   = this.lastKnownActiveUrlIndex;
        int            retryAttempt = 0;

        for (int index = 0; index < configuredURLs.size(); index++) {
            int currentIndex = (startIndex + index) % configuredURLs.size();

            try {
                WebResource.Builder br = createWebResource(currentIndex, relativeURL, null, sessionId);

                response = br.accept(RangerRESTUtils.REST_EXPECTED_MIME_TYPE).type(RangerRESTUtils.REST_MIME_TYPE_JSON).put(ClientResponse.class, toJson(request));

                if (response != null) {
                    setLastKnownActiveUrlIndex(currentIndex);

                    break;
                }
            } catch (ClientHandlerException ex) {
                if (shouldRetry(configuredURLs.get(currentIndex), index, retryAttempt, ex)) {
                    retryAttempt++;

                    index = -1; // start from first url
                }
            }
        }
        return response;
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

    protected WebResource.Builder createWebResource(int currentIndex, String relativeURL, Map<String, String> params) {
        WebResource webResource = getClient().resource(configuredURLs.get(currentIndex) + relativeURL);

        webResource = setQueryParams(webResource, params);

        return webResource.getRequestBuilder();
    }

    protected WebResource.Builder createWebResource(int currentIndex, String relativeURL, Map<String, String> params, Cookie sessionId) {
        if (sessionId == null) {
            return createWebResource(currentIndex, relativeURL, params);
        } else {
            WebResource webResource = getCookieAuthClient().resource(configuredURLs.get(currentIndex) + relativeURL);

            webResource = setQueryParams(webResource, params);

            return webResource.getRequestBuilder().cookie(sessionId);
        }
    }

    protected boolean shouldRetry(String currentUrl, int index, int retryAttemptCount, Exception ex) throws Exception {
        LOG.warn("Failed to communicate with Ranger Admin. URL: {}. Error: {}", currentUrl, ex.getMessage());

        boolean isLastUrl = index == (configuredURLs.size() - 1);

        // attempt retry after failure on the last url
        boolean ret = isLastUrl && (retryAttemptCount < maxRetryAttempts);

        if (ret) {
            LOG.warn("Waiting for {}ms before retry attempt #{}", retryIntervalMs, (retryAttemptCount + 1));

            try {
                Thread.sleep(retryIntervalMs);
            } catch (InterruptedException excp) {
                LOG.error("Failed while waiting to retry", excp);
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

    private Client getCookieAuthClient() {
        Client ret = cookieAuthClient;

        if (ret == null) {
            synchronized (this) {
                ret = cookieAuthClient;

                if (ret == null) {
                    cookieAuthClient = buildClient();

                    if (basicAuthFilter != null) {
                        cookieAuthClient.removeFilter(basicAuthFilter);
                    }

                    ret = cookieAuthClient;
                }
            }
        }

        return ret;
    }

    private Client buildClient() {
        Client client = null;

        if (mIsSSL) {
            KeyManager[]   kmList     = getKeyManagers();
            TrustManager[] tmList     = getTrustManagers();
            SSLContext     sslContext = getSSLContext(kmList, tmList);
            ClientConfig   config     = new DefaultClientConfig();

            config.getClasses().add(JacksonJsonProvider.class); // to handle List<> unmarshalling

            HostnameVerifier hv = (urlHostName, session) -> session.getPeerHost().equals(urlHostName);

            config.getProperties().put(HTTPSProperties.PROPERTY_HTTPS_PROPERTIES, new HTTPSProperties(hv, sslContext));

            client = Client.create(config);
        }

        if (client == null) {
            ClientConfig config = new DefaultClientConfig();

            config.getClasses().add(JacksonJsonProvider.class); // to handle List<> unmarshalling

            client = Client.create(config);
        }

        if (basicAuthFilter != null && !client.isFilterPresent(basicAuthFilter)) {
            client.addFilter(basicAuthFilter);
        }

        // Set Connection Timeout and ReadTime for the PolicyRefresh
        client.setConnectTimeout(mRestClientConnTimeOutMs);
        client.setReadTimeout(mRestClientReadTimeOutMs);

        return client;
    }

    private void setBasicAuthFilter(String username, String password) {
        if (StringUtils.isNotEmpty(username) && StringUtils.isNotEmpty(password)) {
            basicAuthFilter = new HTTPBasicAuthFilter(username, password);
        } else {
            basicAuthFilter = null;
        }
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

        final String pluginPropertyPrefix;

        if (config instanceof RangerPluginConfig) {
            pluginPropertyPrefix = ((RangerPluginConfig) config).getPropertyPrefix();
        } else {
            pluginPropertyPrefix = "ranger.plugin";
        }

        String username = config.get(pluginPropertyPrefix + ".policy.rest.client.username");
        String password = config.get(pluginPropertyPrefix + ".policy.rest.client.password");

        if (StringUtils.isNotBlank(username) && StringUtils.isNotBlank(password)) {
            setBasicAuthFilter(username, password);
        }
    }

    private boolean isSslEnabled(String url) {
        return !StringUtils.isEmpty(url) && url.toLowerCase().startsWith("https");
    }

    private KeyManager[] getKeyManagers() {
        String keyStoreFilepwd = getCredential(mKeyStoreURL, mKeyStoreAlias);

        return getKeyManagers(mKeyStoreFile, keyStoreFilepwd);
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
}
