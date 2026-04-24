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

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.HttpsConfigurator;
import com.sun.net.httpserver.HttpsServer;
import org.apache.ranger.authz.api.RangerAuthzException;
import org.apache.ranger.authz.model.RangerAccessContext;
import org.apache.ranger.authz.model.RangerAccessInfo;
import org.apache.ranger.authz.model.RangerAuthzRequest;
import org.apache.ranger.authz.model.RangerAuthzResult;
import org.apache.ranger.authz.model.RangerAuthzResult.AccessDecision;
import org.apache.ranger.authz.model.RangerAuthzResult.AccessResult;
import org.apache.ranger.authz.model.RangerAuthzResult.PermissionResult;
import org.apache.ranger.authz.model.RangerAuthzResult.PolicyInfo;
import org.apache.ranger.authz.model.RangerMultiAuthzRequest;
import org.apache.ranger.authz.model.RangerMultiAuthzResult;
import org.apache.ranger.authz.model.RangerResourceInfo;
import org.apache.ranger.authz.model.RangerResourcePermissions;
import org.apache.ranger.authz.model.RangerResourcePermissionsRequest;
import org.apache.ranger.authz.model.RangerUserInfo;
import org.junit.jupiter.api.Test;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class TestRemoteAuthorizer {
    @Test
    public void testAuthorizeOverTlsUsesConfiguredHeadersAndServiceLookup() throws Exception {
        try (StubPdpServer server = StubPdpServer.createHttps()) {
            server.respond("/authz/v1/authorize", 200,
                    "{\"requestId\":\"req-1\",\"decision\":\"ALLOW\",\"permissions\":{\"select\":{\"permission\":\"select\",\"access\":{\"decision\":\"ALLOW\",\"policy\":{\"id\":7,\"version\":3}},\"additionalInfo\":{\"source\":\"pdp\"}}}}");

            Path trustStore = server.writeClientTrustStore();
            RangerRemoteAuthorizer authorizer = new RangerRemoteAuthorizer(createTlsNoAuthProperties(server.getBaseUrl(), trustStore));

            try {
                authorizer.init();

                RangerAccessContext accessContext = new RangerAccessContext("hive", null, 123L, "10.0.0.1",
                        Collections.singletonList("10.0.0.2"), stringMap("requestData", "show tables"));
                RangerAuthzRequest request = new RangerAuthzRequest("req-1", new RangerUserInfo("alice"),
                        new RangerAccessInfo(resource("table:default/sales", "column:id", "column:name"), "QUERY", linkedSet("select")),
                        accessContext);
                RangerAuthzResult result = authorizer.authorize(request);

                assertEquals(expectedAuthorizeResult(), result);
                assertNull(server.getLastHeader("Authorization"));
                assertEquals("integration-test", server.getLastHeader("X-Request-Source"));
                assertTrue(server.getLastRequestBody().contains("\"serviceName\":\"dev_hive\""));
                assertTrue(server.getLastRequestBody().contains("\"serviceType\":\"hive\""));
            } finally {
                authorizer.close();
            }
        }
    }

    @Test
    public void testAuthorizeMultiResolvesServiceTypeFromServiceName() throws Exception {
        try (StubPdpServer server = StubPdpServer.createHttp()) {
            server.respond("/authz/v1/authorizeMulti", 200,
                    "{\"requestId\":\"req-2\",\"decision\":\"PARTIAL\",\"accesses\":["
                            + "{\"requestId\":\"sub-1\",\"decision\":\"ALLOW\",\"permissions\":{\"select\":{\"permission\":\"select\",\"access\":{\"decision\":\"ALLOW\"}}}},"
                            + "{\"requestId\":\"sub-2\",\"decision\":\"DENY\",\"permissions\":{\"update\":{\"permission\":\"update\",\"access\":{\"decision\":\"DENY\"}}}}"
                            + "]}");

            RangerRemoteAuthorizer authorizer = new RangerRemoteAuthorizer(createNoAuthProperties(server.getBaseUrl()));

            try {
                authorizer.init();

                RangerAccessContext accessContext = new RangerAccessContext(null, "dev_hive", 456L, null, null, null);
                RangerMultiAuthzRequest multiRequest = new RangerMultiAuthzRequest("req-2", new RangerUserInfo("alice"),
                        Arrays.asList(
                                new RangerAccessInfo(resource("table:default/sales"), "QUERY", linkedSet("select")),
                                new RangerAccessInfo(resource("table:default/sales"), "UPDATE", linkedSet("update"))),
                        accessContext);
                RangerMultiAuthzResult result = authorizer.authorize(multiRequest);

                assertEquals(expectedMultiResult(), result);
                assertTrue(server.getLastRequestBody().contains("\"serviceName\":\"dev_hive\""));
                assertTrue(server.getLastRequestBody().contains("\"serviceType\":\"hive\""));
            } finally {
                authorizer.close();
            }
        }
    }

    @Test
    public void testGetResourcePermissions() throws Exception {
        try (StubPdpServer server = StubPdpServer.createHttp()) {
            server.respond("/authz/v1/permissions", 200,
                    "{\"resource\":{\"name\":\"table:default/sales\",\"subResources\":[\"column:id\"]},"
                            + "\"users\":{\"alice\":{\"select\":{\"permission\":\"select\",\"access\":{\"decision\":\"ALLOW\"}}}},"
                            + "\"groups\":{\"analysts\":{\"select\":{\"permission\":\"select\",\"access\":{\"decision\":\"ALLOW\"}}}},"
                            + "\"roles\":{\"finance\":{\"update\":{\"permission\":\"update\",\"access\":{\"decision\":\"DENY\"}}}}}");

            RangerRemoteAuthorizer authorizer = new RangerRemoteAuthorizer(createNoAuthProperties(server.getBaseUrl()));

            try {
                authorizer.init();

                RangerResourcePermissionsRequest permissionsRequest = new RangerResourcePermissionsRequest("req-3",
                        resource("table:default/sales", "column:id"),
                        new RangerAccessContext("hive", "dev_hive", 789L, null, null, null));
                RangerResourcePermissions result = authorizer.getResourcePermissions(permissionsRequest);

                assertEquals(expectedResourcePermissions(), result);
            } finally {
                authorizer.close();
            }
        }
    }

    @Test
    public void testRemoteErrorIncludesPdpMessage() throws Exception {
        try (StubPdpServer server = StubPdpServer.createHttp()) {
            server.respond("/authz/v1/authorize", 403, "{\"code\":\"FORBIDDEN\",\"message\":\"alice is not authorized\"}");

            RangerRemoteAuthorizer authorizer = new RangerRemoteAuthorizer(createNoAuthProperties(server.getBaseUrl()));

            try {
                authorizer.init();

                RangerAccessContext accessContext = new RangerAccessContext("hive", "dev_hive");
                authorizer.authorize(new RangerAuthzRequest("req-4", new RangerUserInfo("alice"),
                        new RangerAccessInfo(resource("table:default/sales"), "QUERY", linkedSet("select")),
                        accessContext));

                fail("Expected authorize() to fail");
            } catch (RangerAuthzException e) {
                assertTrue(e.getMessage().contains("403"));
                assertTrue(e.getMessage().contains("alice is not authorized"));
            } finally {
                authorizer.close();
            }
        }
    }

    @Test
    public void testKerberosConfigRequiresPrincipalAndKeytab() {
        Properties props = createNoAuthProperties("https://localhost:6500");
        props.setProperty(RangerRemoteAuthzConfig.PROP_REMOTE_AUTH_TYPE, "kerberos");

        RangerRemoteAuthorizer authorizer = new RangerRemoteAuthorizer(props);

        RangerAuthzException exception = assertThrows(RangerAuthzException.class, authorizer::init);

        assertTrue(exception.getMessage().contains(RangerRemoteAuthzConfig.PROP_REMOTE_AUTH_KERBEROS_PRINCIPAL));
    }

    private static Properties createTlsNoAuthProperties(String baseUrl, Path trustStore) {
        Properties props = new Properties();

        props.setProperty(RangerRemoteAuthzConfig.PROP_REMOTE_URL, baseUrl);
        props.setProperty(RangerRemoteAuthzConfig.PROP_REMOTE_AUTH_TYPE, "none");
        props.setProperty(RangerRemoteAuthzConfig.PROP_REMOTE_SSL_TRUSTSTORE_FILE, trustStore.toAbsolutePath().toString());
        props.setProperty(RangerRemoteAuthzConfig.PROP_REMOTE_SSL_TRUSTSTORE_PASSWORD, "changeit");
        props.setProperty(RangerRemoteAuthzConfig.PROP_REMOTE_SSL_TRUSTSTORE_TYPE, "PKCS12");
        props.setProperty(RangerRemoteAuthzConfig.PROP_REMOTE_HEADER_PREFIX + "X-Request-Source", "integration-test");
        props.setProperty(RangerRemoteAuthzConfig.PROP_PREFIX_SERVICE_TYPE + "hive.default.service", "dev_hive");
        props.setProperty(RangerRemoteAuthzConfig.PROP_PREFIX_SERVICE + "dev_hive.servicetype", "hive");

        return props;
    }

    private static Properties createNoAuthProperties(String baseUrl) {
        Properties props = new Properties();

        props.setProperty(RangerRemoteAuthzConfig.PROP_REMOTE_URL, baseUrl);
        props.setProperty(RangerRemoteAuthzConfig.PROP_PREFIX_SERVICE_TYPE + "hive.default.service", "dev_hive");
        props.setProperty(RangerRemoteAuthzConfig.PROP_PREFIX_SERVICE + "dev_hive.servicetype", "hive");

        return props;
    }

    private static RangerAuthzResult expectedAuthorizeResult() {
        PermissionResult permission = new PermissionResult();
        AccessResult     access     = new AccessResult();

        access.setDecision(AccessDecision.ALLOW);
        access.setPolicy(new PolicyInfo(7L, 3L));

        permission.setPermission("select");
        permission.setAccess(access);
        permission.setAdditionalInfo(stringMap("source", "pdp"));

        RangerAuthzResult ret = new RangerAuthzResult();

        ret.setRequestId("req-1");
        ret.setDecision(AccessDecision.ALLOW);
        ret.setPermissions(Collections.singletonMap("select", permission));

        return ret;
    }

    private static RangerMultiAuthzResult expectedMultiResult() {
        RangerAuthzResult first = new RangerAuthzResult();
        first.setRequestId("sub-1");
        first.setDecision(AccessDecision.ALLOW);
        first.setPermissions(Collections.singletonMap("select", permission("select", AccessDecision.ALLOW)));

        RangerAuthzResult second = new RangerAuthzResult();
        second.setRequestId("sub-2");
        second.setDecision(AccessDecision.DENY);
        second.setPermissions(Collections.singletonMap("update", permission("update", AccessDecision.DENY)));

        RangerMultiAuthzResult ret = new RangerMultiAuthzResult();
        ret.setRequestId("req-2");
        ret.setDecision(AccessDecision.PARTIAL);
        ret.setAccesses(Arrays.asList(first, second));

        return ret;
    }

    private static RangerResourcePermissions expectedResourcePermissions() {
        RangerResourcePermissions ret = new RangerResourcePermissions();

        ret.setResource(resource("table:default/sales", "column:id"));
        ret.setUsers(Collections.singletonMap("alice", Collections.singletonMap("select", permission("select", AccessDecision.ALLOW))));
        ret.setGroups(Collections.singletonMap("analysts", Collections.singletonMap("select", permission("select", AccessDecision.ALLOW))));
        ret.setRoles(Collections.singletonMap("finance", Collections.singletonMap("update", permission("update", AccessDecision.DENY))));

        return ret;
    }

    private static PermissionResult permission(String permission, AccessDecision decision) {
        PermissionResult ret = new PermissionResult();
        AccessResult     access = new AccessResult();

        access.setDecision(decision);

        ret.setPermission(permission);
        ret.setAccess(access);

        return ret;
    }

    private static RangerResourceInfo resource(String name, String... subResources) {
        RangerResourceInfo ret = new RangerResourceInfo();

        ret.setName(name);
        if (subResources != null && subResources.length > 0) {
            ret.setSubResources(linkedSet(subResources));
        }

        return ret;
    }

    private static LinkedHashSet<String> linkedSet(String... values) {
        LinkedHashSet<String> ret = new LinkedHashSet<>();

        if (values != null) {
            ret.addAll(Arrays.asList(values));
        }

        return ret;
    }

    private static Map<String, Object> stringMap(String key, String value) {
        Map<String, Object> ret = new LinkedHashMap<>();
        ret.put(key, value);
        return ret;
    }

    private static final class StubPdpServer implements AutoCloseable {
        private static final String SERVER_KEYSTORE_RESOURCE = "/tls/server-keystore.p12.base64";
        private static final String CLIENT_TRUSTSTORE_RESOURCE = "/tls/client-truststore.p12.base64";

        private final HttpServer server;

        private volatile String lastRequestBody;
        private volatile Map<String, String> lastHeaders = new LinkedHashMap<>();

        private StubPdpServer(HttpServer server) {
            this.server = server;
            this.server.start();
        }

        private static StubPdpServer createHttp() throws IOException {
            return new StubPdpServer(HttpServer.create(new InetSocketAddress(0), 0));
        }

        private static StubPdpServer createHttps() throws Exception {
            HttpsServer server = HttpsServer.create(new InetSocketAddress(0), 0);
            server.setHttpsConfigurator(new HttpsConfigurator(createServerSslContext()));

            return new StubPdpServer(server);
        }

        private void respond(String path, int status, String body) {
            server.createContext(path, exchange -> handle(exchange, status, body));
        }

        private String getBaseUrl() {
            String scheme = server instanceof HttpsServer ? "https" : "http";

            return scheme + "://localhost:" + server.getAddress().getPort();
        }

        private String getLastRequestBody() {
            return lastRequestBody;
        }

        private String getLastHeader(String name) {
            for (Map.Entry<String, String> entry : lastHeaders.entrySet()) {
                if (entry.getKey().equalsIgnoreCase(name)) {
                    return entry.getValue();
                }
            }

            return null;
        }

        private Path writeClientTrustStore() throws IOException {
            return writeDecodedResourceToTempFile(CLIENT_TRUSTSTORE_RESOURCE, "client-truststore", ".p12");
        }

        private void handle(HttpExchange exchange, int status, String body) throws IOException {
            lastRequestBody = readFully(exchange.getRequestBody());
            lastHeaders     = new LinkedHashMap<>();

            for (Map.Entry<String, java.util.List<String>> entry : exchange.getRequestHeaders().entrySet()) {
                if (!entry.getValue().isEmpty()) {
                    lastHeaders.put(entry.getKey(), entry.getValue().get(0));
                }
            }

            byte[] responseBytes = body.getBytes(StandardCharsets.UTF_8);

            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.sendResponseHeaders(status, responseBytes.length);

            try (OutputStream out = exchange.getResponseBody()) {
                out.write(responseBytes);
            } finally {
                exchange.close();
            }
        }

        @Override
        public void close() {
            server.stop(0);
        }

        private static String readFully(InputStream input) throws IOException {
            assertNotNull(input);

            try (InputStream in = input; ByteArrayOutputStream out = new ByteArrayOutputStream()) {
                byte[] buffer = new byte[2048];
                int    bytesRead;

                while ((bytesRead = in.read(buffer)) != -1) {
                    out.write(buffer, 0, bytesRead);
                }

                return out.toString(StandardCharsets.UTF_8.name());
            }
        }

        private static SSLContext createServerSslContext() throws Exception {
            KeyStore keyStore = loadKeyStore(SERVER_KEYSTORE_RESOURCE, "PKCS12", "changeit");

            KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
            keyManagerFactory.init(keyStore, "changeit".toCharArray());

            TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            trustManagerFactory.init(keyStore);

            SSLContext sslContext = SSLContext.getInstance("TLS");
            sslContext.init(keyManagerFactory.getKeyManagers(), trustManagerFactory.getTrustManagers(), new SecureRandom());

            return sslContext;
        }

        private static KeyStore loadKeyStore(String resourcePath, String type, String password) throws Exception {
            KeyStore keyStore = KeyStore.getInstance(type);

            try (InputStream input = TestRemoteAuthorizer.class.getResourceAsStream(resourcePath)) {
                assertNotNull(input);

                byte[] decoded = Base64.getDecoder().decode(readFully(input).trim());

                try (InputStream decodedInput = new java.io.ByteArrayInputStream(decoded)) {
                    keyStore.load(decodedInput, password.toCharArray());
                }
            }

            return keyStore;
        }

        private static Path writeDecodedResourceToTempFile(String resourcePath, String prefix, String suffix) throws IOException {
            try (InputStream input = TestRemoteAuthorizer.class.getResourceAsStream(resourcePath)) {
                assertNotNull(input);

                byte[] decoded = Base64.getDecoder().decode(readFully(input).trim());
                Path tempFile = Files.createTempFile(prefix, suffix);

                Files.write(tempFile, decoded);
                tempFile.toFile().deleteOnExit();

                return tempFile;
            }
        }
    }
}
