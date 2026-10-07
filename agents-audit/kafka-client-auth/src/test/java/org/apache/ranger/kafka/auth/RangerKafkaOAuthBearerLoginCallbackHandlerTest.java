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

package org.apache.ranger.kafka.auth;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.security.auth.SaslExtensionsCallback;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerToken;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerTokenCallback;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.UnsupportedCallbackException;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class RangerKafkaOAuthBearerLoginCallbackHandlerTest {
    private static final String SUBJECT = "system:serviceaccount:ranger:ranger-audit";

    @TempDir
    Path tempDir;

    @Test
    public void testTokenFromFileWithoutScopeClaim() throws Exception {
        Path   tokenFile = writeToken("token", buildJwt(SUBJECT, 1_700_000_000L, 1_700_003_600L));
        String jwt       = Files.readString(tokenFile);

        try (RangerKafkaOAuthBearerLoginCallbackHandler handler = configuredHandler(tokenFile)) {
            OAuthBearerToken token = login(handler);

            assertEquals(jwt, token.value());
            assertEquals(SUBJECT, token.principalName());
            assertEquals(1_700_003_600_000L, token.lifetimeMs());
            assertEquals(Long.valueOf(1_700_000_000_000L), token.startTimeMs());
            assertTrue(token.scope().isEmpty());
        }
    }

    @Test
    public void testRotatedTokenIsReadOnNextLogin() throws Exception {
        String jwtV1      = buildJwt(SUBJECT, 1_700_000_000L, 1_700_003_600L);
        String jwtV2      = buildJwt(SUBJECT, 1_700_003_601L, 1_700_007_201L);
        Path   tokenFile  = writeToken("token", jwtV1);

        try (RangerKafkaOAuthBearerLoginCallbackHandler handler = configuredHandler(tokenFile)) {
            assertEquals(jwtV1, login(handler).value());

            Files.write(tokenFile, jwtV2.getBytes(StandardCharsets.UTF_8));

            OAuthBearerToken refreshed = login(handler);

            assertEquals(jwtV2, refreshed.value());
            assertEquals(1_700_007_201_000L, refreshed.lifetimeMs());
        }
    }

    @Test
    public void testTokenWithoutIssuedAtHasNoStartTime() throws Exception {
        Path tokenFile = writeToken("token", buildJwt(SUBJECT, null, 1_700_003_600L));

        try (RangerKafkaOAuthBearerLoginCallbackHandler handler = configuredHandler(tokenFile)) {
            assertNull(login(handler).startTimeMs());
        }
    }

    @Test
    public void testTokenWithoutExpirationIsRejected() throws Exception {
        String payload   = base64Url("{\"sub\":\"" + SUBJECT + "\"}");
        Path   tokenFile = writeToken("token", base64Url("{\"alg\":\"RS256\"}") + "." + payload + ".sig");

        try (RangerKafkaOAuthBearerLoginCallbackHandler handler = configuredHandler(tokenFile)) {
            IOException e = assertThrows(IOException.class, () -> login(handler));

            assertTrue(e.getCause().getMessage().contains("exp"), e.getCause().getMessage());
        }
    }

    @Test
    public void testEmptyTokenFileIsRejected() throws Exception {
        Path tokenFile = writeToken("token", "");

        try (RangerKafkaOAuthBearerLoginCallbackHandler handler = configuredHandler(tokenFile)) {
            assertThrows(IOException.class, () -> login(handler));
        }
    }

    @Test
    public void testNonFileUrlIsRejected() {
        Map<String, Object> configs = new HashMap<>();

        configs.put(SaslConfigs.SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL, "https://idp.example.com/token");

        assertThrows(ConfigException.class, () -> new RangerKafkaOAuthBearerLoginCallbackHandler().configure(configs, "OAUTHBEARER", Collections.emptyList()));
    }

    @Test
    public void testMissingTokenFileIsRejectedAtConfigure() {
        Map<String, Object> configs = new HashMap<>();

        configs.put(SaslConfigs.SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL, tempDir.resolve("missing").toUri().toString());

        assertThrows(ConfigException.class, () -> new RangerKafkaOAuthBearerLoginCallbackHandler().configure(configs, "OAUTHBEARER", Collections.emptyList()));
    }

    @Test
    public void testMissingUrlIsRejectedAtConfigure() {
        assertThrows(ConfigException.class, () -> new RangerKafkaOAuthBearerLoginCallbackHandler().configure(Collections.emptyMap(), "OAUTHBEARER", Collections.emptyList()));
    }

    @Test
    public void testAllowedUrlsSystemPropertyIsEnforced() throws Exception {
        Path   tokenFile = writeToken("token", buildJwt(SUBJECT, 1_700_000_000L, 1_700_003_600L));
        String url       = tokenFile.toUri().toString();

        System.setProperty(RangerKafkaOAuthBearerLoginCallbackHandler.ALLOWED_URLS_SYSTEM_PROPERTY, "file:///somewhere/else");

        try {
            assertThrows(ConfigException.class, () -> configuredHandler(tokenFile));

            System.setProperty(RangerKafkaOAuthBearerLoginCallbackHandler.ALLOWED_URLS_SYSTEM_PROPERTY, "file:///somewhere/else, " + url);

            try (RangerKafkaOAuthBearerLoginCallbackHandler handler = configuredHandler(tokenFile)) {
                assertEquals(SUBJECT, login(handler).principalName());
            }
        } finally {
            System.clearProperty(RangerKafkaOAuthBearerLoginCallbackHandler.ALLOWED_URLS_SYSTEM_PROPERTY);
        }
    }

    @Test
    public void testExtensionsCallbackGetsEmptyExtensions() throws Exception {
        Path tokenFile = writeToken("token", buildJwt(SUBJECT, 1_700_000_000L, 1_700_003_600L));

        try (RangerKafkaOAuthBearerLoginCallbackHandler handler = configuredHandler(tokenFile)) {
            SaslExtensionsCallback callback = new SaslExtensionsCallback();

            handler.handle(new Callback[] {callback});

            assertTrue(callback.extensions().map().isEmpty());
        }
    }

    @Test
    public void testUnsupportedCallbackIsRejected() throws Exception {
        Path tokenFile = writeToken("token", buildJwt(SUBJECT, 1_700_000_000L, 1_700_003_600L));

        try (RangerKafkaOAuthBearerLoginCallbackHandler handler = configuredHandler(tokenFile)) {
            assertThrows(UnsupportedCallbackException.class, () -> handler.handle(new Callback[] {new NameCallback("name")}));
        }
    }

    @Test
    public void testHandleBeforeConfigureFails() {
        assertThrows(IllegalStateException.class, () -> new RangerKafkaOAuthBearerLoginCallbackHandler().handle(new Callback[] {new OAuthBearerTokenCallback()}));
    }

    private RangerKafkaOAuthBearerLoginCallbackHandler configuredHandler(Path tokenFile) {
        Map<String, Object> configs = new HashMap<>();

        configs.put(SaslConfigs.SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL, tokenFile.toUri().toString());

        RangerKafkaOAuthBearerLoginCallbackHandler handler = new RangerKafkaOAuthBearerLoginCallbackHandler();

        handler.configure(configs, "OAUTHBEARER", Collections.emptyList());

        return handler;
    }

    private static OAuthBearerToken login(RangerKafkaOAuthBearerLoginCallbackHandler handler) throws Exception {
        OAuthBearerTokenCallback callback = new OAuthBearerTokenCallback();

        handler.handle(new Callback[] {callback});

        return callback.token();
    }

    private Path writeToken(String name, String content) throws IOException {
        Path file = tempDir.resolve(name);

        Files.write(file, content.getBytes(StandardCharsets.UTF_8));

        return file;
    }

    static String buildJwt(String subject, Long issuedAtSeconds, long expirationSeconds) {
        StringBuilder payload = new StringBuilder("{\"sub\":\"").append(subject).append("\",\"exp\":").append(expirationSeconds);

        if (issuedAtSeconds != null) {
            payload.append(",\"iat\":").append(issuedAtSeconds);
        }

        payload.append(",\"iss\":\"https://kubernetes.default.svc.cluster.local\"}");

        return base64Url("{\"alg\":\"RS256\",\"typ\":\"JWT\"}") + "." + base64Url(payload.toString()) + ".signature";
    }

    private static String base64Url(String value) {
        return Base64.getUrlEncoder().withoutPadding().encodeToString(value.getBytes(StandardCharsets.UTF_8));
    }
}
