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

package org.apache.ranger.authz.remote;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.HttpEntity;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.socket.PlainConnectionSocketFactory;
import org.apache.http.conn.ssl.DefaultHostnameVerifier;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.http.util.EntityUtils;
import org.apache.ranger.authz.api.RangerAuthzException;
import org.apache.ranger.authz.model.RangerAuthzRequest;
import org.apache.ranger.authz.model.RangerAuthzResult;
import org.apache.ranger.authz.model.RangerMultiAuthzRequest;
import org.apache.ranger.authz.model.RangerMultiAuthzResult;
import org.apache.ranger.authz.model.RangerResourcePermissions;
import org.apache.ranger.authz.model.RangerResourcePermissionsRequest;
import org.apache.ranger.authz.remote.authn.RangerRemoteKerberosContext;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;

import java.io.Closeable;
import java.io.FileInputStream;
import java.io.IOException;
import java.security.KeyStore;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.Map;

import static org.apache.ranger.authz.remote.RangerRemoteAuthType.KERBEROS;
import static org.apache.ranger.authz.remote.RangerRemoteAuthzErrorCode.REMOTE_CALL_UNSUCCESSFUL;
import static org.apache.ranger.authz.remote.RangerRemoteAuthzErrorCode.REMOTE_REQUEST_FAILED;
import static org.apache.ranger.authz.remote.RangerRemoteAuthzErrorCode.REMOTE_RESPONSE_INVALID;
import static org.apache.ranger.authz.remote.RangerRemoteAuthzErrorCode.TLS_CONFIGURATION_FAILED;

class RangerPdpClient implements Closeable {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    private static final String PATH_AUTHORIZE            = "/authorize";
    private static final String PATH_AUTHORIZE_MULTI      = "/authorizeMulti";
    private static final String PATH_RESOURCE_PERMISSIONS = "/permissions";

    private final RangerRemoteAuthzConfig config;
    private final CloseableHttpClient     httpClient;
    private final RangerRemoteAuthType    authType;
    private final RangerRemoteKerberosContext kerberosContext;

    RangerPdpClient(RangerRemoteAuthzConfig config) throws RangerAuthzException {
        this.config          = config;
        this.authType        = config.getAuthType();
        this.kerberosContext = authType == KERBEROS ? RangerRemoteKerberosContext.create(config) : null;
        this.httpClient      = createHttpClient(config, kerberosContext);
    }

    RangerAuthzResult authorize(RangerAuthzRequest request) throws RangerAuthzException {
        String endpoint = config.getEndpointUrl(PATH_AUTHORIZE);

        return post(endpoint, request, RangerAuthzResult.class);
    }

    RangerMultiAuthzResult authorize(RangerMultiAuthzRequest request) throws RangerAuthzException {
        String endpoint = config.getEndpointUrl(PATH_AUTHORIZE_MULTI);

        return post(endpoint, request, RangerMultiAuthzResult.class);
    }

    RangerResourcePermissions getResourcePermissions(RangerResourcePermissionsRequest request) throws RangerAuthzException {
        String endpoint = config.getEndpointUrl(PATH_RESOURCE_PERMISSIONS);

        return post(endpoint, request, RangerResourcePermissions.class);
    }

    private <T> T post(String endpoint, Object payload, Class<T> responseType) throws RangerAuthzException {
        final String requestBody;

        try {
            requestBody = OBJECT_MAPPER.writeValueAsString(payload);
        } catch (IOException e) {
            throw new RangerAuthzException(REMOTE_RESPONSE_INVALID, e, "request-body");
        }

        HttpPost request = new HttpPost(endpoint);

        try {
            request.setHeader("Content-Type", ContentType.APPLICATION_JSON.getMimeType());
            request.setHeader("Accept", ContentType.APPLICATION_JSON.getMimeType());
            request.setEntity(new StringEntity(requestBody, ContentType.APPLICATION_JSON));

            for (Map.Entry<String, String> header : config.getHeaders().entrySet()) {
                request.setHeader(header.getKey(), header.getValue());
            }

            String responseBody;

            if (authType == KERBEROS) {
                responseBody = kerberosContext.doAs((PrivilegedExceptionAction<String>) () -> execute(request, endpoint));
            } else {
                responseBody = execute(request, endpoint);
            }

            try {
                return OBJECT_MAPPER.readValue(responseBody, responseType);
            } catch (IOException e) {
                throw new RangerAuthzException(REMOTE_RESPONSE_INVALID, e, endpoint);
            }
        } catch (RangerAuthzException e) {
            throw e;
        } catch (PrivilegedActionException e) {
            Throwable cause = e.getException();

            if (cause instanceof RangerAuthzException) {
                throw (RangerAuthzException) cause;
            }

            throw new RangerAuthzException(REMOTE_REQUEST_FAILED, cause, endpoint);
        } catch (IOException e) {
            throw new RangerAuthzException(REMOTE_REQUEST_FAILED, e, endpoint);
        }
    }

    @Override
    public void close() throws IOException {
        httpClient.close();
    }

    private String execute(HttpPost request, String endpoint) throws IOException, RangerAuthzException {
        try (CloseableHttpResponse response = httpClient.execute(request)) {
            int        status       = response.getStatusLine().getStatusCode();
            HttpEntity entity       = response.getEntity();
            String     responseBody = entity != null ? EntityUtils.toString(entity) : "";

            if (status >= 200 && status < 300) {
                return responseBody;
            }

            String errorMessage = readErrorMessage(responseBody);

            if (errorMessage == null || errorMessage.trim().isEmpty()) {
                errorMessage = response.getStatusLine().getReasonPhrase();
            }

            throw new RangerAuthzException(REMOTE_CALL_UNSUCCESSFUL, endpoint, String.valueOf(status), errorMessage);
        }
    }

    private static CloseableHttpClient createHttpClient(RangerRemoteAuthzConfig config, RangerRemoteKerberosContext kerberosContext) throws RangerAuthzException {
        RequestConfig requestConfig = RequestConfig.custom()
                .setConnectTimeout(config.getConnectTimeoutMs())
                .setConnectionRequestTimeout(config.getConnectTimeoutMs())
                .setSocketTimeout(config.getReadTimeoutMs())
                .build();

        PoolingHttpClientConnectionManager connectionManager = new PoolingHttpClientConnectionManager(createSocketFactoryRegistry(config));
        connectionManager.setDefaultMaxPerRoute(20);
        connectionManager.setMaxTotal(100);

        HttpClientBuilder builder = HttpClients.custom()
                .setConnectionManager(connectionManager)
                .setDefaultRequestConfig(requestConfig)
                .disableAutomaticRetries();

        if (kerberosContext != null) {
            kerberosContext.applyToHttpClientBuilder(builder);
        }

        return builder.build();
    }

    private static Registry<ConnectionSocketFactory> createSocketFactoryRegistry(RangerRemoteAuthzConfig config) throws RangerAuthzException {
        RegistryBuilder<ConnectionSocketFactory> builder = RegistryBuilder.<ConnectionSocketFactory>create()
                .register("http", PlainConnectionSocketFactory.getSocketFactory());

        builder.register("https", createSslSocketFactory(config));

        return builder.build();
    }

    private static SSLConnectionSocketFactory createSslSocketFactory(RangerRemoteAuthzConfig config) throws RangerAuthzException {
        try {
            SSLContextBuilder sslContextBuilder = SSLContextBuilder.create();

            if (config.getSslTrustStoreFile() != null) {
                sslContextBuilder.loadTrustMaterial(
                        loadKeyStore(config.getSslTrustStoreFile(), config.getSslTrustStorePassword(), config.getSslTrustStoreType()),
                        null);
            }

            if (config.getSslKeyStoreFile() != null) {
                sslContextBuilder.loadKeyMaterial(
                        loadKeyStore(config.getSslKeyStoreFile(), config.getSslKeyStorePassword(), config.getSslKeyStoreType()),
                        toPasswordChars(config.getSslKeyStorePassword()));
            }

            SSLContext sslContext = sslContextBuilder.build();
            HostnameVerifier hostnameVerifier = config.isHostnameVerificationDisabled()
                    ? NoopHostnameVerifier.INSTANCE
                    : new DefaultHostnameVerifier();

            return new SSLConnectionSocketFactory(sslContext, hostnameVerifier);
        } catch (Exception e) {
            throw new RangerAuthzException(TLS_CONFIGURATION_FAILED, e, config.getPdpUrl());
        }
    }

    private static KeyStore loadKeyStore(String location, String password, String type) throws Exception {
        KeyStore keyStore = KeyStore.getInstance(type);

        try (FileInputStream input = new FileInputStream(location)) {
            keyStore.load(input, toPasswordChars(password));
        }

        return keyStore;
    }

    private static char[] toPasswordChars(String password) {
        return password != null ? password.toCharArray() : null;
    }

    private static String readErrorMessage(String json) {
        if (json == null || json.trim().isEmpty()) {
            return null;
        }

        try {
            JsonNode root    = OBJECT_MAPPER.readTree(json);
            JsonNode message = root != null ? root.get("message") : null;

            return message != null && !message.isNull() ? message.asText() : null;
        } catch (IOException e) {
            return null;
        }
    }
}
