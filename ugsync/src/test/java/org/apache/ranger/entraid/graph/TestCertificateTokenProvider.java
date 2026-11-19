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

package org.apache.ranger.entraid.graph;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.ranger.entraid.graph.EntraIdGraphConfig.AuthMode;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import javax.ws.rs.core.Form;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.KeyStore;
import java.security.MessageDigest;
import java.security.PublicKey;
import java.security.Signature;
import java.security.cert.X509Certificate;
import java.util.Base64;
import java.util.List;

public class TestCertificateTokenProvider {
    private static final String KEYSTORE_PATH     = "src/test/resources/entraid-test.p12";
    private static final String KEYSTORE_PASSWORD = "changeit";
    private static final String CERT_ALIAS        = "entraid-test";
    private static final String TENANT            = "11111111-1111-1111-1111-111111111111";
    private static final String CLIENT            = "22222222-2222-2222-2222-222222222222";

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static X509Certificate certificate;

    @BeforeAll
    public static void loadKeystore() throws Exception {
        try (InputStream in = Files.newInputStream(Paths.get(KEYSTORE_PATH))) {
            KeyStore ks = KeyStore.getInstance("PKCS12");
            ks.load(in, KEYSTORE_PASSWORD.toCharArray());
            certificate = (X509Certificate) ks.getCertificate(CERT_ALIAS);
        }
    }

    private EntraIdGraphConfig certConfig() {
        return new EntraIdGraphConfig.Builder()
                .tenantId(TENANT)
                .clientId(CLIENT)
                .authMode(AuthMode.CERTIFICATE)
                .keystorePath(KEYSTORE_PATH)
                .certAlias(CERT_ALIAS)
                .keystorePassword(KEYSTORE_PASSWORD.toCharArray())
                .authorityHost("https://login.microsoftonline.com")
                .build();
    }

    private String buildAssertion() throws Exception {
        CertificateTokenProvider provider = new CertificateTokenProvider();
        provider.init(certConfig(), null);
        Form form = new Form();
        provider.addCredential(form);
        List<String> values = form.asMap().get("client_assertion");
        return values.get(0);
    }

    private static String b64UrlDecodeToString(String segment) {
        return new String(Base64.getUrlDecoder().decode(segment), StandardCharsets.UTF_8);
    }

    @Test
    public void test01_init_loadsKeystoreWithoutError() {
        Assertions.assertDoesNotThrow(() -> {
            CertificateTokenProvider provider = new CertificateTokenProvider();
            provider.init(certConfig(), null);
        });
    }

    @Test
    public void test02_addCredential_setsAssertionType() throws Exception {
        CertificateTokenProvider provider = new CertificateTokenProvider();
        provider.init(certConfig(), null);
        Form form = new Form();
        provider.addCredential(form);
        Assertions.assertEquals("urn:ietf:params:oauth:client-assertion-type:jwt-bearer", form.asMap().get("client_assertion_type").get(0));
    }

    @Test
    public void test03_assertion_hasThreeSegments() throws Exception {
        String assertion = buildAssertion();
        Assertions.assertEquals(3, assertion.split("\\.").length);
    }

    @Test
    public void test04_header_algIsRs256() throws Exception {
        String[] parts  = buildAssertion().split("\\.");
        JsonNode header = MAPPER.readTree(b64UrlDecodeToString(parts[0]));
        Assertions.assertEquals("RS256", header.get("alg").asText());
        Assertions.assertEquals("JWT", header.get("typ").asText());
    }

    @Test
    public void test05_header_x5tMatchesCertSha1Thumbprint() throws Exception {
        String[] parts  = buildAssertion().split("\\.");
        JsonNode header = MAPPER.readTree(b64UrlDecodeToString(parts[0]));
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        String  expected = Base64.getUrlEncoder().withoutPadding().encodeToString(sha1.digest(certificate.getEncoded()));
        Assertions.assertEquals(expected, header.get("x5t").asText());
    }

    @Test
    public void test06_claims_audIsTokenEndpoint() throws Exception {
        String[] parts  = buildAssertion().split("\\.");
        JsonNode claims = MAPPER.readTree(b64UrlDecodeToString(parts[1]));
        Assertions.assertEquals("https://login.microsoftonline.com/" + TENANT + "/oauth2/v2.0/token", claims.get("aud").asText());
    }

    @Test
    public void test07_claims_issAndSubAreClientId() throws Exception {
        String[] parts  = buildAssertion().split("\\.");
        JsonNode claims = MAPPER.readTree(b64UrlDecodeToString(parts[1]));
        Assertions.assertEquals(CLIENT, claims.get("iss").asText());
        Assertions.assertEquals(CLIENT, claims.get("sub").asText());
    }

    @Test
    public void test08_claims_expIsAfterNbf() throws Exception {
        String[] parts  = buildAssertion().split("\\.");
        JsonNode claims = MAPPER.readTree(b64UrlDecodeToString(parts[1]));
        Assertions.assertTrue(claims.get("exp").asLong() > claims.get("nbf").asLong());
    }

    @Test
    public void test09_claims_jtiIsPresent() throws Exception {
        String[] parts  = buildAssertion().split("\\.");
        JsonNode claims = MAPPER.readTree(b64UrlDecodeToString(parts[1]));
        Assertions.assertTrue(claims.hasNonNull("jti"));
        Assertions.assertFalse(claims.get("jti").asText().isEmpty());
    }

    @Test
    public void test10_signature_verifiesAgainstCertPublicKey() throws Exception {
        String   assertion    = buildAssertion();
        String[] parts        = assertion.split("\\.");
        String   signingInput = parts[0] + "." + parts[1];
        byte[]   sigBytes     = Base64.getUrlDecoder().decode(parts[2]);
        PublicKey publicKey = certificate.getPublicKey();
        Signature verifier  = Signature.getInstance("SHA256withRSA");
        verifier.initVerify(publicKey);
        verifier.update(signingInput.getBytes(StandardCharsets.UTF_8));
        Assertions.assertTrue(verifier.verify(sigBytes), "client_assertion signature must verify against the certificate's public key");
    }

    @Test
    public void test11_eachAssertion_hasUniqueJti() throws Exception {
        JsonNode c1 = MAPPER.readTree(b64UrlDecodeToString(buildAssertion().split("\\.")[1]));
        JsonNode c2 = MAPPER.readTree(b64UrlDecodeToString(buildAssertion().split("\\.")[1]));
        Assertions.assertNotEquals(c1.get("jti").asText(), c2.get("jti").asText());
    }

    @Test
    public void test12_wrongAlias_throws() {
        EntraIdGraphConfig badAlias = new EntraIdGraphConfig.Builder()
                .tenantId(TENANT)
                .clientId(CLIENT)
                .authMode(AuthMode.CERTIFICATE)
                .keystorePath(KEYSTORE_PATH)
                .certAlias("no-such-alias")
                .keystorePassword(KEYSTORE_PASSWORD.toCharArray())
                .build();
        CertificateTokenProvider provider = new CertificateTokenProvider();
        Assertions.assertThrows(GraphClientException.class, () -> provider.init(badAlias, null));
    }

    @Test
    public void test13_wrongKeystorePassword_throws() {
        EntraIdGraphConfig badPwd = new EntraIdGraphConfig.Builder()
                .tenantId(TENANT)
                .clientId(CLIENT)
                .authMode(AuthMode.CERTIFICATE)
                .keystorePath(KEYSTORE_PATH)
                .certAlias(CERT_ALIAS)
                .keystorePassword("wrong-password".toCharArray())
                .build();
        CertificateTokenProvider provider = new CertificateTokenProvider();
        Assertions.assertThrows(GraphClientException.class, () -> provider.init(badPwd, null));
    }

    @Test
    public void test14_missingKeystoreFile_throwsWrappedException() {
        EntraIdGraphConfig badPath = new EntraIdGraphConfig.Builder()
                .tenantId(TENANT)
                .clientId(CLIENT)
                .authMode(AuthMode.CERTIFICATE)
                .keystorePath("src/test/resources/non-existent.p12")
                .certAlias(CERT_ALIAS)
                .keystorePassword(KEYSTORE_PASSWORD.toCharArray())
                .build();
        CertificateTokenProvider provider = new CertificateTokenProvider();
        Assertions.assertThrows(GraphClientException.class, () -> provider.init(badPath, null));
    }
}
