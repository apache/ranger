/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ranger.entraid.graph;

import com.fasterxml.jackson.databind.ObjectMapper;

import javax.ws.rs.client.Client;
import javax.ws.rs.core.Form;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.Key;
import java.security.KeyStore;
import java.security.MessageDigest;
import java.security.PrivateKey;
import java.security.Signature;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.time.Instant;
import java.util.Arrays;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

final class CertificateTokenProvider extends OAuthTokenProvider {
    private static final String JWT_BEARER_ASSERTION_TYPE = "urn:ietf:params:oauth:client-assertion-type:jwt-bearer";
    private static final long ASSERTION_LIFETIME_SECONDS = 300L;

    private final ObjectMapper objectMapper = new ObjectMapper();
    private PrivateKey privateKey;
    private String x5tThumbprint;

    @Override
    public void init(EntraIdGraphConfig config, Client httpClient) throws GraphClientException {
        super.init(config, httpClient);
        loadKeyMaterial(config);
    }

    @Override
    protected void addCredential(Form form) throws GraphClientException {
        form.param("client_assertion_type", JWT_BEARER_ASSERTION_TYPE);
        form.param("client_assertion", buildSignedAssertion());
    }

    private void loadKeyMaterial(EntraIdGraphConfig config) throws GraphClientException {
        String path = config.getKeystorePath();
        char[] password = config.getKeystorePassword();
        String alias = config.getCertAlias();

        try (InputStream in = Files.newInputStream(Paths.get(path))) {
            KeyStore keyStore = KeyStore.getInstance(inferKeystoreType(path));
            keyStore.load(in, password);
            Key key = keyStore.getKey(alias, password);
            if (!(key instanceof PrivateKey)) {
                throw new GraphClientException("Alias '" + alias + "' in keystore " + path + " does not resolve to a private key");
            }
            this.privateKey = (PrivateKey) key;
            Certificate cert = keyStore.getCertificate(alias);
            if (!(cert instanceof X509Certificate)) {
                throw new GraphClientException("Alias '" + alias + "' in keystore " + path + " does not have an X.509 certificate");
            }
            this.x5tThumbprint = computeX5t((X509Certificate) cert);
        } catch (GraphClientException e) {
            throw e;
        } catch (Exception e) {
            throw new GraphClientException("Failed to load certificate key material from keystore " + path, e);
        } finally {
            if (password != null) {
                Arrays.fill(password, '\0');
            }
        }
    }

    private String buildSignedAssertion() throws GraphClientException {
        try {
            long now = Instant.now().getEpochSecond();
            Map<String, Object> header = new LinkedHashMap<>();
            header.put("alg", "RS256");
            header.put("typ", "JWT");
            header.put("x5t", x5tThumbprint);
            Map<String, Object> claims = new LinkedHashMap<>();
            claims.put("aud", getTokenEndpoint());
            claims.put("iss", getClientId());
            claims.put("sub", getClientId());
            claims.put("jti", UUID.randomUUID().toString());
            claims.put("nbf", now);
            claims.put("exp", now + ASSERTION_LIFETIME_SECONDS);
            claims.put("iat", now);
            String encodedHeader = base64Url(objectMapper.writeValueAsBytes(header));
            String encodedClaims = base64Url(objectMapper.writeValueAsBytes(claims));
            String signingInput = encodedHeader + "." + encodedClaims;
            Signature signature = Signature.getInstance("SHA256withRSA");
            signature.initSign(privateKey);
            signature.update(signingInput.getBytes(StandardCharsets.UTF_8));
            String encodedSignature = base64Url(signature.sign());
            return signingInput + "." + encodedSignature;
        } catch (Exception e) {
            throw new GraphClientException("Failed to build signed client assertion", e);
        }
    }

    private static String computeX5t(X509Certificate cert) throws GraphClientException {
        try {
            MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
            return base64Url(sha1.digest(cert.getEncoded()));
        } catch (Exception e) {
            throw new GraphClientException("Failed to compute certificate thumbprint (x5t)", e);
        }
    }

    private static String inferKeystoreType(String path) {
        String lower = path.toLowerCase();
        if (lower.endsWith(".jks")) {
            return "JKS";
        }
        if (lower.endsWith(".bcfks")) {
            return "BCFKS";
        }
        return "PKCS12";
    }

    private static String base64Url(byte[] data) {
        return Base64.getUrlEncoder().withoutPadding().encodeToString(data);
    }
}
