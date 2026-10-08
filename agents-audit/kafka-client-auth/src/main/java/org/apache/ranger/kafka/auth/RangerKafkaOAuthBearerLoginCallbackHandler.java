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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.security.auth.AuthenticateCallbackHandler;
import org.apache.kafka.common.security.auth.SaslExtensions;
import org.apache.kafka.common.security.auth.SaslExtensionsCallback;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerTokenCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.AppConfigurationEntry;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * SASL/OAUTHBEARER login callback handler that presents a JWT read from a local file, such as a
 * Kubernetes projected service-account token, to the Kafka broker.
 * <p>
 * Kafka's bundled {@code OAuthBearerLoginCallbackHandler} also accepts a {@code file://} value for
 * {@code sasl.oauthbearer.token.endpoint.url}, but it reads the file once and caches the token, so a
 * client fails to re-authenticate after the platform rotates the token in place. This handler never
 * caches: every login and every refresh re-reads the file and re-derives {@code sub}, {@code exp}
 * and {@code iat}, so Kafka's refresh scheduling always follows the current token. Signature and
 * audience validation are the broker's responsibility.
 */
public class RangerKafkaOAuthBearerLoginCallbackHandler implements AuthenticateCallbackHandler, AutoCloseable {
    private static final Logger       LOG                          = LoggerFactory.getLogger(RangerKafkaOAuthBearerLoginCallbackHandler.class);

    /* same system property Kafka's bundled handler consults; when set, the token URL must be listed */
    public static final String        ALLOWED_URLS_SYSTEM_PROPERTY = "org.apache.kafka.sasl.oauthbearer.allowed.urls";
    public static final int           TOKEN_READ_MAX_ATTEMPTS      = 5;
    public static final long          TOKEN_READ_RETRY_SLEEP_MS    = 50L;

    private static final String       CLAIM_SUBJECT                = "sub";
    private static final String       CLAIM_EXPIRATION             = "exp";
    private static final String       CLAIM_ISSUED_AT              = "iat";
    private static final ObjectMapper MAPPER                       = new ObjectMapper();

    private volatile Path tokenFile;

    @Override
    public void configure(Map<String, ?> configs, String saslMechanism, List<AppConfigurationEntry> jaasConfigEntries) {
        Object urlValue = configs.get(SaslConfigs.SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL);
        String url      = urlValue != null ? urlValue.toString().trim() : "";

        if (url.isEmpty()) {
            throw new ConfigException(String.format("%s must be set to a file:// URL for %s", SaslConfigs.SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL, getClass().getSimpleName()));
        }

        throwIfUrlNotAllowed(url);

        tokenFile = resolveTokenFile(url);

        LOG.info("{}: reading SASL/OAUTHBEARER token from {}", getClass().getSimpleName(), tokenFile);
    }

    @Override
    public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
        Path file = tokenFile;

        if (file == null) {
            throw new IllegalStateException(String.format("%s is not configured; call configure() first", getClass().getSimpleName()));
        }

        for (Callback callback : callbacks) {
            if (callback instanceof OAuthBearerTokenCallback) {
                RangerOAuthBearerToken token = readToken(file);

                ((OAuthBearerTokenCallback) callback).token(token);

                LOG.info("{}: token loaded from {} for principal={}, expiresAtMs={}", getClass().getSimpleName(), file, token.principalName(), token.lifetimeMs());
            } else if (callback instanceof SaslExtensionsCallback) {
                ((SaslExtensionsCallback) callback).extensions(SaslExtensions.empty());
            } else {
                throw new UnsupportedCallbackException(callback);
            }
        }
    }

    @Override
    public void close() {
        tokenFile = null;
    }

    static void throwIfUrlNotAllowed(String url) {
        String allowed = System.getProperty(ALLOWED_URLS_SYSTEM_PROPERTY);

        if (allowed == null) {
            return;
        }

        Set<String> allowedUrls = Arrays.stream(allowed.split(",")).map(String::trim).collect(Collectors.toSet());

        if (!allowedUrls.contains(url)) {
            throw new ConfigException(String.format("%s is not allowed. Update system property '%s' to allow it", url, ALLOWED_URLS_SYSTEM_PROPERTY));
        }
    }

    static Path resolveTokenFile(String url) {
        URI uri;

        try {
            uri = URI.create(url);
        } catch (IllegalArgumentException e) {
            throw new ConfigException(String.format("%s is not a valid URL: %s", SaslConfigs.SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL, url), e);
        }

        if (!"file".equalsIgnoreCase(uri.getScheme())) {
            throw new ConfigException(String.format("%s supports only file:// token URLs, got: %s", RangerKafkaOAuthBearerLoginCallbackHandler.class.getSimpleName(), url));
        }

        Path path = Paths.get(uri);

        if (!Files.isRegularFile(path)) {
            throw new ConfigException(String.format("SASL/OAUTHBEARER token file does not exist: %s", path));
        }

        return path;
    }

    /* Re-reads and re-parses on every call. Retries briefly to ride out the moment where kubelet
     * atomically replaces the projected token file. */
    static RangerOAuthBearerToken readToken(Path file) throws IOException {
        IOException lastFailure = null;

        for (int attempt = 1; attempt <= TOKEN_READ_MAX_ATTEMPTS; attempt++) {
            try {
                String jwt = new String(Files.readAllBytes(file), StandardCharsets.UTF_8).trim();

                if (jwt.isEmpty()) {
                    throw new IOException(String.format("SASL/OAUTHBEARER token file is empty: %s", file));
                }

                return parseToken(jwt);
            } catch (IOException e) {
                lastFailure = e;

                LOG.debug("{}: token read/parse failed (attempt {}/{}): {}", RangerKafkaOAuthBearerLoginCallbackHandler.class.getSimpleName(), attempt, TOKEN_READ_MAX_ATTEMPTS, e.getMessage());

                if (attempt < TOKEN_READ_MAX_ATTEMPTS) {
                    sleepBeforeRetry();
                }
            }
        }

        throw new IOException(String.format("Failed to read SASL/OAUTHBEARER token from %s after %d attempts", file, TOKEN_READ_MAX_ATTEMPTS), lastFailure);
    }

    static RangerOAuthBearerToken parseToken(String jwt) throws IOException {
        String[] parts = jwt.split("\\.");

        if (parts.length < 2) {
            throw new IOException("Malformed JWT: expected header.payload[.signature]");
        }

        JsonNode claims;

        try {
            claims = MAPPER.readTree(Base64.getUrlDecoder().decode(parts[1]));
        } catch (IllegalArgumentException e) {
            throw new IOException("Malformed JWT: payload is not base64url", e);
        }

        JsonNode sub = claims.get(CLAIM_SUBJECT);
        JsonNode exp = claims.get(CLAIM_EXPIRATION);
        JsonNode iat = claims.get(CLAIM_ISSUED_AT);

        if (sub == null || !sub.isTextual() || sub.asText().trim().isEmpty()) {
            throw new IOException(String.format("JWT is missing required '%s' claim", CLAIM_SUBJECT));
        }

        if (exp == null || !exp.isNumber()) {
            throw new IOException(String.format("JWT is missing required numeric '%s' claim", CLAIM_EXPIRATION));
        }

        Long startTimeMs = iat != null && iat.isNumber() ? toEpochMillis(iat) : null;

        return new RangerOAuthBearerToken(jwt, sub.asText(), toEpochMillis(exp), startTimeMs);
    }

    private static long toEpochMillis(JsonNode epochSeconds) {
        return Math.round(epochSeconds.asDouble() * 1000);
    }

    private static void sleepBeforeRetry() throws IOException {
        try {
            Thread.sleep(TOKEN_READ_RETRY_SLEEP_MS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();

            throw new IOException("Interrupted while waiting to re-read SASL/OAUTHBEARER token", e);
        }
    }
}
