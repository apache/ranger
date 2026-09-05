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

package org.apache.ranger.services;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import org.apache.ranger.plugin.client.HadoopException;
import org.apache.ranger.plugin.service.ResourceLookupContext;
import org.apache.ranger.plugin.util.PasswordUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link RangerServiceDrill}.
 *
 * <p>Uses JDK's built-in {@link HttpServer} to stand in for the Drill REST
 * endpoint ({@code POST <url>/query.json}). The tests assert that
 * <ul>
 *   <li>the password stored by Ranger Admin (encrypted 5-segment value) is
 *       decrypted before it is used for HTTP Basic auth, and</li>
 *   <li>a plain-text password falls back to the raw value instead of failing
 *       the connection test ({@code PasswordUtils.getDecryptPassword}), and</li>
 *   <li>expected lookup behaviours (parent-gated resources, empty result
 *       handling) hold while talking to a real HTTP stack.</li>
 * </ul>
 */
public class RangerServiceDrillTest {

    private static final String CONFIG_DRILL_URL   = "drill.connection.url";
    private static final String CONFIG_USERNAME    = "username";
    private static final String CONFIG_PASSWORD    = "password";

    private static final String TEST_USERNAME      = "rangerlookup";
    private static final String TEST_PLAIN_PASSWORD = "s3cretDri11!";

    private HttpServer server;
    private volatile String capturedAuth;
    private volatile String capturedBody;
    private volatile int    responseCode  = 200;
    private volatile String responseBody  = "{\"rows\":[]}";

    @BeforeEach
    void startFakeDrillServer() throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", this::handle);
        server.start();
    }

    @AfterEach
    void stopFakeDrillServer() {
        if (server != null) {
            server.stop(0);
        }
    }

    private void handle(HttpExchange exchange) throws IOException {
        capturedAuth  = exchange.getRequestHeaders().getFirst("Authorization");
        capturedBody  = readRequestBody(exchange.getRequestBody());
        byte[] resp   = responseBody.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "application/json");
        exchange.sendResponseHeaders(responseCode, resp.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(resp);
        }
    }

    /** Reads the whole request body. JDK 8 compatible replacement for {@code InputStream.readAllBytes()} (Java 9+). */
    private static String readRequestBody(InputStream in) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        byte[] buf = new byte[4096];
        int n;
        while ((n = in.read(buf)) != -1) {
            out.write(buf, 0, n);
        }
        return new String(out.toByteArray(), StandardCharsets.UTF_8);
    }

    private String baseUrl() {
        return "http://127.0.0.1:" + server.getAddress().getPort();
    }

    private RangerServiceDrill newService(String username, String password) {
        Map<String, String> configs = new HashMap<>();
        configs.put(CONFIG_DRILL_URL, baseUrl());
        configs.put(CONFIG_USERNAME, username);
        configs.put(CONFIG_PASSWORD, password);
        RangerServiceDrill service = new RangerServiceDrill();
        service.setConfigs(configs);
        return service;
    }

    /**
     * Builds the 5-segment encrypted password value exactly like Ranger Admin's
     * {@code ServiceDBStore} does for the default PBEWithMD5AndDES algorithm:
     * {@code algo,key,salt,iterations,encryptedPayload}. The payload itself is
     * the plain text encrypted with the very same default parameters.
     */
    private static String rangerStoredPassword(String plainPassword) throws Exception {
        String head = PasswordUtils.DEFAULT_CRYPT_ALGO + "," + PasswordUtils.DEFAULT_ENCRYPT_KEY + ","
                + PasswordUtils.DEFAULT_SALT + "," + PasswordUtils.DEFAULT_ITERATION_COUNT;
        return head + "," + PasswordUtils.encryptPassword(head + "," + plainPassword);
    }

    private static String expectedBasicAuth(String username, String password) {
        String creds = username + ":" + password;
        return "Basic " + Base64.getEncoder().encodeToString(creds.getBytes(StandardCharsets.UTF_8));
    }

    // ---------------------------------------------------------------------
    // validateConfig
    // ---------------------------------------------------------------------

    @Test
    void validateConfig_withEncryptedPassword_decryptsBeforeBasicAuth() throws Exception {
        String stored = rangerStoredPassword(TEST_PLAIN_PASSWORD);

        Map<String, Object> result = newService(TEST_USERNAME, stored).validateConfig();

        assertEquals(Boolean.TRUE, result.get("connectivityStatus"));
        assertEquals(expectedBasicAuth(TEST_USERNAME, TEST_PLAIN_PASSWORD), capturedAuth,
                "Basic auth must use the decrypted plain-text password");
    }

    @Test
    void validateConfig_withPlainTextPassword_fallsBackToRawValue() throws Exception {
        Map<String, Object> result = newService(TEST_USERNAME, TEST_PLAIN_PASSWORD).validateConfig();

        assertEquals(Boolean.TRUE, result.get("connectivityStatus"));
        assertEquals(expectedBasicAuth(TEST_USERNAME, TEST_PLAIN_PASSWORD), capturedAuth,
                "Plain-text password must be used as-is (no decrypt failure)");
    }

    @Test
    void validateConfig_withoutUsername_throwsHadoopExceptionWithFieldName() {
        assertThrows(HadoopException.class,
                () -> newService("", TEST_PLAIN_PASSWORD).validateConfig());
    }

    @Test
    void validateConfig_withNonJsonBody_returnsConnectivityFalse() throws Exception {
        responseBody = "{\"foo\":\"bar\"}";

        Map<String, Object> result = newService(TEST_USERNAME, TEST_PLAIN_PASSWORD).validateConfig();

        assertEquals(Boolean.FALSE, result.get("connectivityStatus"));
    }

    @Test
    void validateConfig_withHttpError_returnsConnectivityFalse() throws Exception {
        responseCode = 500;
        responseBody = "{\"error\":\"boom\"}";

        Map<String, Object> result = newService(TEST_USERNAME, TEST_PLAIN_PASSWORD).validateConfig();

        assertEquals(Boolean.FALSE, result.get("connectivityStatus"));
    }

    // ---------------------------------------------------------------------
    // lookupResource
    // ---------------------------------------------------------------------

    @Test
    void lookupResource_datasource_withEncryptedPassword_returnsRowsAndDecryptedAuth() throws Exception {
        responseBody = "{\"queryState\":\"COMPLETED\",\"columns\":[\"DATASOURCE\"],"
                + "\"rows\":[{\"DATASOURCE\":\"dfs\"},{\"DATASOURCE\":\"dfs.default\"}]}";
        ResourceLookupContext ctx = new ResourceLookupContext();
        ctx.setResourceName("datasource");
        ctx.setUserInput("");

        List<String> result = newService(TEST_USERNAME, rangerStoredPassword(TEST_PLAIN_PASSWORD))
                .lookupResource(ctx);

        assertEquals(Arrays.asList("dfs", "dfs.default"), result);
        assertEquals(expectedBasicAuth(TEST_USERNAME, TEST_PLAIN_PASSWORD), capturedAuth,
                "lookup must decrypt the stored password before Basic auth");
    }

    @Test
    void lookupResource_schema_withoutParentDatasource_returnsEmptyWithoutQuery() throws Exception {
        ResourceLookupContext ctx = new ResourceLookupContext();
        ctx.setResourceName("schema");
        ctx.setUserInput("");
        ctx.setResources(new HashMap<>());

        List<String> result = newService(TEST_USERNAME, TEST_PLAIN_PASSWORD).lookupResource(ctx);

        assertEquals(Collections.emptyList(), result);
        assertNull(capturedBody, "No HTTP call must be made when the parent datasource is missing");
    }

    @Test
    void lookupResource_withBlankResourceName_returnsEmpty() throws Exception {
        ResourceLookupContext ctx = new ResourceLookupContext();
        ctx.setResourceName("  ");

        List<String> result = newService(TEST_USERNAME, TEST_PLAIN_PASSWORD).lookupResource(ctx);

        assertEquals(Collections.emptyList(), result);
        assertNull(capturedBody);
    }
}