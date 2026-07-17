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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Form;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import java.time.Instant;

abstract class OAuthTokenProvider implements TokenProvider {
    private static final Logger LOG = LoggerFactory.getLogger(OAuthTokenProvider.class);
    /** Renew this many seconds before the server-stated expiry to avoid races. */
    private static final long EXPIRY_SAFETY_SECONDS = 120L;
    private final ObjectMapper objectMapper = new ObjectMapper();

    private Client client;
    private String tokenEndpoint;
    private String scope;
    private String clientId;

    private String  cachedToken;
    private Instant cachedTokenExpiry;

    @Override
    public void init(EntraIdGraphConfig config, Client httpClient) throws GraphClientException {
        this.client = httpClient;
        this.tokenEndpoint = config.getTokenEndpoint();
        this.scope = config.getDefaultScope();
        this.clientId = config.getClientId();
    }

    @Override
    public String getAccessToken(boolean forceRefresh) throws GraphClientException {
        if (!forceRefresh && cachedToken != null && cachedTokenExpiry != null && Instant.now().isBefore(cachedTokenExpiry)) {
            return cachedToken;
        }
        return acquireNewToken();
    }

    protected String getTokenEndpoint() {
        return tokenEndpoint;
    }

    protected String getClientId() {
        return clientId;
    }

    /**
     * Add the credential-specific parameters to the token request form.
     * For a client secret this is {@code client_secret}; for a certificate this
     * is {@code client_assertion_type} + {@code client_assertion}.
     */
    protected abstract void addCredential(Form form) throws GraphClientException;

    private synchronized String acquireNewToken() throws GraphClientException {
        Form form = new Form();
        form.param("client_id", clientId);
        form.param("scope", scope);
        form.param("grant_type", "client_credentials");
        addCredential(form);
        try (Response response = client.target(tokenEndpoint).request(MediaType.APPLICATION_JSON).post(Entity.form(form))) {
            int status = response.getStatus();
            String body = response.hasEntity() ? response.readEntity(String.class) : "";
            if (status != 200) {
                throw new GraphClientException("Token request to " + tokenEndpoint + " failed: " + summarizeError(body), status);
            }
            JsonNode root = objectMapper.readTree(body);
            JsonNode accessToken = root.get("access_token");
            JsonNode expiresIn = root.get("expires_in");
            if (accessToken == null || accessToken.asText().isEmpty()) {
                throw new GraphClientException("Token response did not contain access_token");
            }
            long ttlSeconds = expiresIn == null ? 3600L : expiresIn.asLong(3600L);
            long effective = Math.max(ttlSeconds - EXPIRY_SAFETY_SECONDS, 1L);
            cachedToken = accessToken.asText();
            cachedTokenExpiry = Instant.now().plusSeconds(effective);
            LOG.debug("Acquired Entra access token; expires in {}s (effective {}s)", ttlSeconds, effective);
            return cachedToken;
        } catch (GraphClientException e) {
            throw e;
        } catch (Exception e) {
            throw new GraphClientException("Failed to acquire access token from " + tokenEndpoint, e);
        }
    }

    /** Surface the OAuth error code without leaking the full token-endpoint payload. */
    private String summarizeError(String body) {
        if (body == null || body.isEmpty()) {
            return "empty response";
        }
        try {
            JsonNode root = objectMapper.readTree(body);
            JsonNode err = root.get("error");
            JsonNode desc = root.get("error_description");
            if (err != null) {
                String code = err.asText();
                // error_description can be verbose; keep only its first line.
                String detail = desc == null ? "" : desc.asText();
                int nl = detail.indexOf('\n');
                if (nl > 0) {
                    detail = detail.substring(0, nl);
                }
                return detail.isEmpty() ? code : (code + ": " + detail);
            }
        } catch (JsonProcessingException ignored) {
            // fall through
        }
        return "unparseable error response";
    }
}
