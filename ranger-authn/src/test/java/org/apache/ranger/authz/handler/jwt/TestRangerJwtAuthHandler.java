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
package org.apache.ranger.authz.handler.jwt;

import com.nimbusds.jose.JOSEObjectType;
import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.JWSSigner;
import com.nimbusds.jose.crypto.MACSigner;
import com.nimbusds.jose.crypto.RSASSASigner;
import com.nimbusds.jose.jwk.JWKSet;
import com.nimbusds.jose.jwk.RSAKey;
import com.nimbusds.jose.jwk.gen.RSAKeyGenerator;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.security.KeyFactory;
import java.security.PrivateKey;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.Arrays;
import java.util.Base64;
import java.util.Date;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestRangerJwtAuthHandler {
    private static final String ISSUER   = "https://idp.example.com/realms/ranger";
    private static final String AUDIENCE = "ranger-admin";

    /* throwaway self-signed RSA-2048 key pair generated for these tests only; never used anywhere else */
    private static final String TEST_CERT_PEM = readResource("jwt-test-only-cert.pem");
    private static final String TEST_KEY_PEM  = readResource("jwt-test-only-key.pem");

    private static RSAKey     jwksKey;
    private static RSAKey     rogueKey;
    private static PrivateKey pemPrivateKey;
    private static HttpServer jwksServer;
    private static String     jwksUrl;

    @BeforeAll
    static void startJwksServer() throws Exception {
        jwksKey  = new RSAKeyGenerator(2048).keyID("kid-1").generate();
        rogueKey = new RSAKeyGenerator(2048).keyID("kid-1").generate();

        byte[] jwks = new JWKSet(jwksKey.toPublicJWK()).toString().getBytes(StandardCharsets.UTF_8);

        jwksServer = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        jwksServer.createContext("/jwks", exchange -> {
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, jwks.length);

            try (OutputStream os = exchange.getResponseBody()) {
                os.write(jwks);
            }
        });
        jwksServer.start();

        jwksUrl = "http://127.0.0.1:" + jwksServer.getAddress().getPort() + "/jwks";

        String base64 = TEST_KEY_PEM.replace("-----BEGIN PRIVATE KEY-----", "").replace("-----END PRIVATE KEY-----", "").replaceAll("\\s", "");

        pemPrivateKey = KeyFactory.getInstance("RSA").generatePrivate(new PKCS8EncodedKeySpec(Base64.getDecoder().decode(base64)));
    }

    @AfterAll
    static void stopJwksServer() {
        jwksServer.stop(0);
    }

    private static String readResource(String name) {
        try (InputStream in = TestRangerJwtAuthHandler.class.getClassLoader().getResourceAsStream(name)) {
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static Properties jwksConfig() {
        Properties config = new Properties();

        config.setProperty(RangerJwtAuthHandler.KEY_PROVIDER_URL, jwksUrl);
        config.setProperty(RangerJwtAuthHandler.KEY_JWT_AUDIENCES, AUDIENCE);
        config.setProperty(RangerJwtAuthHandler.KEY_JWT_ISS, ISSUER);

        return config;
    }

    private static Properties pemConfig() {
        Properties config = new Properties();

        config.setProperty(RangerJwtAuthHandler.KEY_JWT_PUBLIC_KEY, TEST_CERT_PEM);
        config.setProperty(RangerJwtAuthHandler.KEY_JWT_AUDIENCES, AUDIENCE);
        config.setProperty(RangerJwtAuthHandler.KEY_JWT_ISS, ISSUER);

        return config;
    }

    private static RangerDefaultJwtAuthHandler handler(Properties config) throws Exception {
        RangerDefaultJwtAuthHandler handler = new RangerDefaultJwtAuthHandler();

        handler.initialize(config);

        return handler;
    }

    private static JWTClaimsSet.Builder validClaims() {
        return new JWTClaimsSet.Builder()
                .issuer(ISSUER)
                .audience(AUDIENCE)
                .subject("alice")
                .issueTime(new Date())
                .expirationTime(new Date(System.currentTimeMillis() + 60_000));
    }

    private static String sign(JWSHeader header, JWTClaimsSet claims, JWSSigner signer) throws Exception {
        SignedJWT jwt = new SignedJWT(header, claims);

        jwt.sign(signer);

        return jwt.serialize();
    }

    private static String bearerFromJwks(JWTClaimsSet claims) throws Exception {
        return "Bearer " + sign(new JWSHeader.Builder(JWSAlgorithm.RS256).keyID(jwksKey.getKeyID()).build(), claims, new RSASSASigner(jwksKey));
    }

    private static String bearerFromPem(JWTClaimsSet claims) throws Exception {
        return "Bearer " + sign(new JWSHeader.Builder(JWSAlgorithm.RS256).keyID("some-kid-the-cert-does-not-have").build(), claims, new RSASSASigner(pemPrivateKey));
    }

    @Test
    void initialize_failsWithoutKeySource() {
        Properties config = jwksConfig();

        config.remove(RangerJwtAuthHandler.KEY_PROVIDER_URL);

        Exception e = assertThrows(Exception.class, () -> handler(config));

        assertTrue(e.getMessage().contains(RangerJwtAuthHandler.KEY_PROVIDER_URL));
    }

    @Test
    void initialize_failsWithoutAudiences() {
        for (String value : new String[] {null, "", " , "}) {
            Properties config = jwksConfig();

            if (value == null) {
                config.remove(RangerJwtAuthHandler.KEY_JWT_AUDIENCES);
            } else {
                config.setProperty(RangerJwtAuthHandler.KEY_JWT_AUDIENCES, value);
            }

            Exception e = assertThrows(Exception.class, () -> handler(config));

            assertTrue(e.getMessage().contains(RangerJwtAuthHandler.KEY_JWT_AUDIENCES));
        }
    }

    @Test
    void initialize_failsWithoutIssuer() {
        for (String value : new String[] {null, "", "  "}) {
            Properties config = jwksConfig();

            if (value == null) {
                config.remove(RangerJwtAuthHandler.KEY_JWT_ISS);
            } else {
                config.setProperty(RangerJwtAuthHandler.KEY_JWT_ISS, value);
            }

            Exception e = assertThrows(Exception.class, () -> handler(config));

            assertTrue(e.getMessage().contains(RangerJwtAuthHandler.KEY_JWT_ISS));
        }
    }

    @Test
    void initialize_trimsAudiencesAndIssuer() throws Exception {
        Properties config = jwksConfig();

        config.setProperty(RangerJwtAuthHandler.KEY_JWT_AUDIENCES, " other , ," + AUDIENCE + " ");
        config.setProperty(RangerJwtAuthHandler.KEY_JWT_ISS, " " + ISSUER + " ");

        RangerDefaultJwtAuthHandler handler = handler(config);

        assertEquals(Arrays.asList("other", AUDIENCE), handler.audiences);
        assertEquals(ISSUER, handler.issuer);
        assertEquals("alice", handler.authenticate(bearerFromJwks(validClaims().build())));
    }

    @Test
    void authenticate_acceptsValidTokenViaJwks() throws Exception {
        RangerDefaultJwtAuthHandler handler = handler(jwksConfig());

        assertEquals("alice", handler.authenticate(bearerFromJwks(validClaims().build())));
        assertEquals("alice", handler.authenticate(bearerFromJwks(validClaims().audience(Arrays.asList("other", AUDIENCE)).build())));
    }

    @Test
    void authenticate_acceptsValidTokenViaPinnedPublicKeyIgnoringKid() throws Exception {
        RangerDefaultJwtAuthHandler handler = handler(pemConfig());

        assertEquals("alice", handler.authenticate(bearerFromPem(validClaims().build())));
    }

    @Test
    void authenticate_rejectsTokenSignedByWrongKey() throws Exception {
        RangerDefaultJwtAuthHandler jwksHandler = handler(jwksConfig());
        RangerDefaultJwtAuthHandler pemHandler  = handler(pemConfig());

        String rogueViaJwks = "Bearer " + sign(new JWSHeader.Builder(JWSAlgorithm.RS256).keyID("kid-1").build(), validClaims().build(), new RSASSASigner(rogueKey));

        assertNull(jwksHandler.authenticate(rogueViaJwks));
        assertNull(pemHandler.authenticate(rogueViaJwks));
        assertNull(jwksHandler.authenticate(bearerFromPem(validClaims().build())));
    }

    @Test
    void authenticate_rejectsUnknownKid() throws Exception {
        RangerDefaultJwtAuthHandler handler = handler(jwksConfig());

        String unknownKid = "Bearer " + sign(new JWSHeader.Builder(JWSAlgorithm.RS256).keyID("kid-2").build(), validClaims().build(), new RSASSASigner(jwksKey));

        assertNull(handler.authenticate(unknownKid));
    }

    @Test
    void authenticate_rejectsHmacAlgorithm() throws Exception {
        RangerDefaultJwtAuthHandler handler = handler(jwksConfig());

        /* an HS256 token must never be verified against public key material */
        byte[] secret = new byte[32];
        String hmac   = "Bearer " + sign(new JWSHeader.Builder(JWSAlgorithm.HS256).keyID("kid-1").build(), validClaims().build(), new MACSigner(secret));

        assertNull(handler.authenticate(hmac));
    }

    @Test
    void authenticate_rejectsMissingOrPastExpiry() throws Exception {
        RangerDefaultJwtAuthHandler handler = handler(jwksConfig());

        assertNull(handler.authenticate(bearerFromJwks(validClaims().expirationTime(null).build())));
        assertNull(handler.authenticate(bearerFromJwks(validClaims().expirationTime(new Date(System.currentTimeMillis() - 5 * 60_000)).build())));
    }

    @Test
    void authenticate_rejectsTokenNotYetValid() throws Exception {
        RangerDefaultJwtAuthHandler handler = handler(jwksConfig());

        assertNull(handler.authenticate(bearerFromJwks(validClaims().notBeforeTime(new Date(System.currentTimeMillis() + 5 * 60_000)).build())));
    }

    @Test
    void authenticate_rejectsTokenForOtherAudience() throws Exception {
        RangerDefaultJwtAuthHandler handler = handler(jwksConfig());

        assertNull(handler.authenticate(bearerFromJwks(validClaims().audience("other-service").build())));
        assertNull(handler.authenticate(bearerFromJwks(validClaims().audience((String) null).build())));
    }

    @Test
    void authenticate_rejectsTokenFromOtherIssuer() throws Exception {
        RangerDefaultJwtAuthHandler handler = handler(jwksConfig());

        assertNull(handler.authenticate(bearerFromJwks(validClaims().issuer("https://idp.example.com/realms/other").build())));
        assertNull(handler.authenticate(bearerFromJwks(validClaims().issuer(null).build())));
    }

    @Test
    void authenticate_rejectsMissingOrBlankSubject() throws Exception {
        RangerDefaultJwtAuthHandler handler = handler(jwksConfig());

        assertNull(handler.authenticate(bearerFromJwks(validClaims().subject(null).build())));
        assertNull(handler.authenticate(bearerFromJwks(validClaims().subject("  ").build())));
    }

    @Test
    void authenticate_acceptsJwtAndAccessTokenTypesOnly() throws Exception {
        RangerDefaultJwtAuthHandler handler = handler(jwksConfig());

        for (String typ : new String[] {"JWT", "at+jwt"}) {
            String token = "Bearer " + sign(new JWSHeader.Builder(JWSAlgorithm.RS256).keyID("kid-1").type(new JOSEObjectType(typ)).build(), validClaims().build(), new RSASSASigner(jwksKey));

            assertEquals("alice", handler.authenticate(token), typ);
        }

        String wrongType = "Bearer " + sign(new JWSHeader.Builder(JWSAlgorithm.RS256).keyID("kid-1").type(new JOSEObjectType("secevent+jwt")).build(), validClaims().build(), new RSASSASigner(jwksKey));

        assertNull(handler.authenticate(wrongType));
    }

    @Test
    void authenticate_returnsNullForMissingOrMalformedHeader() throws Exception {
        RangerDefaultJwtAuthHandler handler = handler(jwksConfig());

        assertNull(handler.authenticate((String) null));
        assertNull(handler.authenticate(""));
        assertNull(handler.authenticate("Basic abc"));
        assertNull(handler.authenticate("Bearer "));
        assertNull(handler.authenticate("Bearer not-a-jwt"));
        assertNull(handler.authenticate("Bearer eyJhbGciOiJIUzI1NiJ9.buyevwv678.abcd"));
    }

    @Test
    void validateToken_rejectsWhenHandlerNotInitialized() throws Exception {
        RangerDefaultJwtAuthHandler handler = new RangerDefaultJwtAuthHandler();

        assertFalse(handler.validateToken(SignedJWT.parse(bearerFromJwks(validClaims().build()).substring("Bearer ".length()))));
        assertNull(handler.authenticate(bearerFromJwks(validClaims().build())));
    }

    @Test
    void safeJwtLogContext_includesMetadataWithoutRawToken() throws Exception {
        RangerDefaultJwtAuthHandler handler = new RangerDefaultJwtAuthHandler();
        String header = "eyJhbGciOiJSUzI1NiIsImtpZCI6ImtpZC1hYmMifQ";
        String payload = "eyJzdWIiOiJmMDE1X3JlcGxheV91c2VyIiwiYXVkIjoic2VydmljZS1hIiwiaXNzIjoidGVzdC1pc3N1ZXIiLCJqdGkiOiJqdGktMTIzIiwiZXhwIjoxOTk5OTk5OTk5fQ";
        String signature = "abcd";
        String serialized = header + "." + payload + "." + signature;

        String context = handler.safeJwtLogContext(SignedJWT.parse(serialized));

        assertNotNull(context);
        assertTrue(context.contains("subject=f015_replay_user"));
        assertTrue(context.contains("audience=service-a"));
        assertTrue(context.contains("issuer=test-issuer"));
        assertTrue(context.contains("keyId=kid-abc"));
        assertTrue(context.contains("jwtId=jti-123"));
        assertFalse(context.contains(serialized));
        assertFalse(context.contains(header));
        assertFalse(context.contains(payload));
        assertFalse(context.contains(signature));
    }
}
