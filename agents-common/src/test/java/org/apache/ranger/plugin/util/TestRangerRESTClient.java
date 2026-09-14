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

import com.sun.jersey.api.client.ClientResponse;
import com.sun.net.httpserver.HttpServer;
import org.apache.hadoop.conf.Configuration;
import org.apache.ranger.authorization.hadoop.config.RangerPluginConfig;
import org.apache.ranger.plugin.policyengine.RangerPolicyEngineOptions;
import org.apache.ranger.plugin.service.RangerBasePlugin;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestRangerRESTClient {
    private static final String SERVICE_TYPE = "hive";
    private static final String SERVICE_NAME = "test-service";
    private static final String APP_ID = "test-app";
    private static final String ERR_MESSAGE = "Ranger URL is null or empty.";

    @Test
    public void testPluginInit_WithNoUrl_ThrowsException() {
        RangerBasePlugin plugin = new RangerBasePlugin(SERVICE_TYPE, SERVICE_NAME, APP_ID);
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, plugin::init);
        assertTrue(exception.getMessage().contains(ERR_MESSAGE));
    }

    @Test
    public void testPluginInit_WithValidUrl_Succeeds() {
        RangerPolicyEngineOptions peOptions = new RangerPolicyEngineOptions();
        RangerPluginConfig pluginConfig = new RangerPluginConfig(SERVICE_TYPE, SERVICE_NAME, APP_ID, "cl1", "on-perm", peOptions);
        pluginConfig.set("ranger.plugin.hive.policy.rest.url", "http://dummy:1234");
        RangerBasePlugin plugin = new RangerBasePlugin(pluginConfig);
        plugin.init();
        assertNotNull(plugin, "RangerBasePlugin should be initialized successfully");
    }

    @Test
    public void jwtProviderInjectsBearerAuthorizationHeader() throws Exception {
        assertEquals("Bearer test-jwt-token", captureAuthHeader(() -> "test-jwt-token", null, null));
    }

    @Test
    public void tokenSupplierWithoutTokenFallsBackToBasicAuth() throws Exception {
        assertEquals("Basic dXNlcjE6cGFzc3dvcmQx", captureAuthHeader(() -> null, "user1", "password1"));
    }

    private String captureAuthHeader(Supplier<String> tokenSupplier, String username, String password) throws Exception {
        AtomicReference<String> capturedAuthHeader = new AtomicReference<>();
        HttpServer              httpServer         = HttpServer.create(new InetSocketAddress(0), 0);

        httpServer.createContext("/", exchange -> {
            List<String> values = exchange.getRequestHeaders().get("Authorization");

            if (values != null && !values.isEmpty()) {
                capturedAuthHeader.set(values.get(0));
            }

            exchange.sendResponseHeaders(200, -1);
            exchange.close();
        });

        httpServer.start();

        try {
            String           serverUrl = "http://localhost:" + httpServer.getAddress().getPort();
            Configuration    conf      = new Configuration();
            RangerRESTClient client    = new RangerRESTClient(serverUrl, null, conf);

            if (username != null) {
                client.setBasicAuthInfo(username, password);
            }

            // custom token provider is now a plain java.util.function.Supplier<String> (no dependency on Ranger interfaces)
            client.setTokenSupplier(tokenSupplier);

            assertTrue(client.isAuthFilterPresent(), "JWT auth filter should be present when a token supplier is registered");

            ClientResponse response = client.get("/test", Collections.emptyMap());

            assertEquals(200, response.getStatus());
            response.close();

            return capturedAuthHeader.get();
        } finally {
            httpServer.stop(0);
        }
    }
}
