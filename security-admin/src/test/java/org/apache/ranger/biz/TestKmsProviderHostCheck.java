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
package org.apache.ranger.biz;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.net.httpserver.HttpServer;
import org.apache.ranger.common.ContextUtil;
import org.apache.ranger.common.MessageEnums;
import org.apache.ranger.common.PropertiesUtil;
import org.apache.ranger.common.RESTErrorUtil;
import org.apache.ranger.common.TimedExecutor;
import org.apache.ranger.plugin.model.RangerService;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.service.RangerDefaultService;
import org.apache.ranger.plugin.service.ResourceLookupContext;
import org.apache.ranger.plugin.store.ServiceStore;
import org.apache.ranger.service.RangerServiceService;
import org.apache.ranger.view.VXKmsKey;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.WebApplicationException;

import java.lang.reflect.Field;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestKmsProviderHostCheck {
    private static final String AUTH_PROP = "hadoop.security.authentication";
    private static final String SERVICE = "kms-test";

    /**
     * Providers the KMS client would resolve to a blocked host.
     */
    private static final String[] BLOCKED_PROVIDERS = {
            "kms://http@127.0.0.1:9/kms",
            // percent-encoded nested provider: Hadoop's unnestUri() decodes the authority
            "kms://http%3A%2F%2F127.0.0.1%3A9%2Fkms",
            "kms://http%3a%2f%2f127.0.0.1%3a9%2fkms",
            "kms://http%3A%2F%2F127%2E0%2E0%2E1%3A9%2Fkms",
            "kms://http%253A%252F%252F127.0.0.1%253A9%252Fkms",
            "kms://https%3A%2F%2Flocalhost%3A9%2Fkms",
            // KMS HA host lists: the client connects to every ';' separated host
            "kms://http@kms.example;127.0.0.1:9/kms",
            "kms://http@127.0.0.1;kms.example:9/kms",
            "kms://http@kms.example;localhost:9/kms",
            "kms://http@kms.example;localhost.:9/kms",
            "kms://http@kms.example;0.0.0.0:9/kms",
            "kms://http@kms.example;127.1:9/kms",
            "kms://http@kms.example;2130706433:9/kms",
            "kms://http@kms.example;169.254.169.254:9/kms",
            "kms://http@kms.example;[::1]:9/kms",
            "kms://http@localhost.:9/kms"
    };

    /**
     * Providers that must keep working.
     */
    private static final String[] ALLOWED_PROVIDERS = {
            "kms://http@kms1.example.com:9292/kms",
            "kms://https@kms1.example.com;kms2.example.com:9494/kms"
    };

    private String previousAuth;
    private String previousRequireSaved;

    @BeforeEach
    public void saveProperties() {
        previousAuth = PropertiesUtil.getProperty(AUTH_PROP);
        previousRequireSaved = PropertiesUtil.getProperty(ServiceMgr.PROP_SERVICE_CONFIG_VALIDATE_REQUIRE_SAVED_SERVICE);

        PropertiesUtil.getPropertiesMap().put(AUTH_PROP, "simple");
        PropertiesUtil.getPropertiesMap().put(ServiceMgr.PROP_SERVICE_CONFIG_VALIDATE_REQUIRE_SAVED_SERVICE, "false");
    }

    @AfterEach
    public void restoreState() throws Exception {
        restoreProperty(AUTH_PROP, previousAuth);
        restoreProperty(ServiceMgr.PROP_SERVICE_CONFIG_VALIDATE_REQUIRE_SAVED_SERVICE, previousRequireSaved);

        Field providerListField = KmsKeyMgr.class.getDeclaredField("providerList");
        providerListField.setAccessible(true);

        @SuppressWarnings("unchecked")
        Map<String, String> providerList = (Map<String, String>) providerListField.get(null);
        providerList.clear();

        Field nextProviderField = KmsKeyMgr.class.getDeclaredField("nextProvider");
        nextProviderField.setAccessible(true);
        nextProviderField.setInt(null, 0);
    }

    @ParameterizedTest
    @MethodSource("blockedProviders")
    public void validateOutboundServiceConfigs_rejectsBlockedProvider(String provider) {
        Exception thrown = Assertions.assertThrows(Exception.class, () -> ServiceMgr.validateOutboundServiceConfigs(Collections.singletonMap("provider", provider)),
                "accepted provider " + provider + ", dial hosts " + ServiceMgr.extractDialHosts(provider));

        assertBlockedHost(thrown, provider);
    }

    @ParameterizedTest
    @MethodSource("allowedProviders")
    public void validateOutboundServiceConfigs_allowsLegitimateProvider(String provider) {
        Assertions.assertDoesNotThrow(() -> ServiceMgr.validateOutboundServiceConfigs(Collections.singletonMap("provider", provider)));
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "hive,impala",                         // download auth users
            "hive;impala",
            "zk1.example:2181,zk2.example:2181",   // quorum lists
            "hdfs://namenode.example.com:8020",
            "jdbc:hive2://hs2.example.com:10000/default",
            "https://ranger.example.com:6182/path?a=1,2",
            "p%40ss%word",                         // values with '%' that are not URLs
            "simple"
    })
    public void validateOutboundServiceConfigs_allowsOrdinaryValues(String value) {
        Assertions.assertDoesNotThrow(() -> ServiceMgr.validateOutboundServiceConfigs(Collections.singletonMap("config", value)));
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "thrift://zk1.example:2181,127.0.0.1:2181",
            "zk1.example:2181,localhost:2181"
    })
    public void validateOutboundServiceConfigs_checksEveryHostInAList(String value) {
        Assertions.assertThrows(Exception.class, () -> ServiceMgr.validateOutboundServiceConfigs(Collections.singletonMap("config", value)));
    }

    @ParameterizedTest
    @MethodSource("blockedProviders")
    public void validateConfig_rejectsBlockedProviderBeforeConnect(String provider) throws Exception {
        ServiceMgr mgr = newServiceMgr(provider);
        TimedExecutor exec = (TimedExecutor) getField(mgr, "timedExecutor");

        Exception thrown = Assertions.assertThrows(Exception.class, () -> mgr.validateConfig(kmsService(provider), kmsServiceStore()),
                "validateConfig accepted provider " + provider);

        verify(exec, never()).timedTask(any(), anyLong(), any());
        assertBlockedHost(thrown, provider);
    }

    @ParameterizedTest
    @MethodSource("allowedProviders")
    public void validateConfig_allowsLegitimateProvider(String provider) throws Exception {
        ServiceMgr mgr = newServiceMgr(provider);
        TimedExecutor exec = (TimedExecutor) getField(mgr, "timedExecutor");

        mgr.validateConfig(kmsService(provider), kmsServiceStore());

        verify(exec, times(1)).timedTask(any(), anyLong(), any());
    }

    @ParameterizedTest
    @MethodSource("blockedProviders")
    public void lookupResource_rejectsBlockedProviderBeforeConnect(String provider) throws Exception {
        ServiceMgr mgr = newServiceMgr(provider);
        TimedExecutor exec = (TimedExecutor) getField(mgr, "timedExecutor");
        ServiceDBStore dbStore = mock(ServiceDBStore.class);
        setField(mgr, "svcDBStore", dbStore);
        when(dbStore.getServiceByName(SERVICE)).thenReturn(kmsService(provider));

        Exception thrown = Assertions.assertThrows(Exception.class, () -> mgr.lookupResource(SERVICE, new ResourceLookupContext(), kmsServiceStore()),
                "lookupResource accepted provider " + provider);

        verify(exec, never()).timedTask(any(), anyLong(), any());
        assertBlockedHost(thrown, provider);
    }

    @ParameterizedTest(name = "{0} rejects {2}")
    @MethodSource("keyOperationsWithBlockedProviders")
    public void keyOperation_rejectsSavedBlockedProviderBeforeConnect(String opName, KeyOperation op, String provider) throws Exception {
        KmsKeyMgr mgr = newKmsKeyMgr(provider);
        Client client = okClient();

        try (MockedStatic<ContextUtil> context = Mockito.mockStatic(ContextUtil.class);
             MockedStatic<Client> clients = Mockito.mockStatic(Client.class)) {
            context.when(ContextUtil::getCurrentUserLoginId).thenReturn("keyadmin");
            clients.when(() -> Client.create(any(ClientConfig.class))).thenReturn(client);

            Exception rejected = Assertions.assertThrows(Exception.class, () -> op.run(mgr), opName + " accepted provider " + provider);

            // The security property: no client was created, so nothing was dialed
            clients.verify(() -> Client.create(any(ClientConfig.class)), never());
            Assertions.assertInstanceOf(WebApplicationException.class, rejected, opName + " should surface the rejection, not a 'service not found'");
            assertBlockedHost(rejected, provider);
        }
    }

    @ParameterizedTest
    @MethodSource("allowedProviders")
    public void getKey_allowsLegitimateProvider(String provider) throws Exception {
        KmsKeyMgr mgr = newKmsKeyMgr(provider);
        Client client = okClient();

        try (MockedStatic<ContextUtil> context = Mockito.mockStatic(ContextUtil.class);
             MockedStatic<Client> clients = Mockito.mockStatic(Client.class)) {
            context.when(ContextUtil::getCurrentUserLoginId).thenReturn("keyadmin");
            clients.when(() -> Client.create(any(ClientConfig.class))).thenReturn(client);

            VXKmsKey key = mgr.getKey(SERVICE, "k1");

            Assertions.assertNotNull(key, "getKey rejected legitimate provider " + provider);
            Assertions.assertEquals("k1", key.getName());
        }
    }

    @Test
    public void getKey_doesNotFollowRedirectToLoopback() throws Exception {
        InetAddress localAddress = localNonLoopbackAddress();
        Assumptions.assumeTrue(localAddress != null, "needs a non-loopback local address to show redirect following");

        AtomicInteger redirectHits = new AtomicInteger();
        AtomicInteger loopbackHits = new AtomicInteger();
        HttpServer loopback = loopbackServer(loopbackHits);
        HttpServer redirector = null;

        try {
            // NOTE: if Ranger later also blocks the Admin host's own interface addresses, this provider is
            // rejected before any request and redirectHits stays 0; switch to another reachable address then.
            redirector = redirectServer(localAddress, loopback.getAddress().getPort(), redirectHits);

            String provider = "kms://http@" + localAddress.getHostAddress() + ":" + redirector.getAddress().getPort() + "/kms";
            KmsKeyMgr mgr = newKmsKeyMgr(provider);

            try (MockedStatic<ContextUtil> context = Mockito.mockStatic(ContextUtil.class)) {
                context.when(ContextUtil::getCurrentUserLoginId).thenReturn("keyadmin");

                try {
                    mgr.getKey(SERVICE, "k1");
                } catch (Exception ignored) {
                    // The 302 is not a key. The assertion is which server was contacted.
                }
            }

            Assertions.assertTrue(redirectHits.get() > 0, "getKey did not contact the configured provider " + provider);
            Assertions.assertEquals(0, loopbackHits.get(), "getKey followed a redirect to loopback");
        } finally {
            loopback.stop(0);
            if (redirector != null) {
                redirector.stop(0);
            }
        }
    }

    @FunctionalInterface
    interface KeyOperation {
        void run(KmsKeyMgr mgr) throws Exception;
    }

    static Stream<String> blockedProviders() {
        return Stream.of(BLOCKED_PROVIDERS);
    }

    static Stream<String> allowedProviders() {
        return Stream.of(ALLOWED_PROVIDERS);
    }

    static Stream<Arguments> keyOperationsWithBlockedProviders() {
        Map<String, KeyOperation> ops = new HashMap<>();
        ops.put("getKey", mgr -> mgr.getKey(SERVICE, "k1"));
        ops.put("searchKeys", mgr -> mgr.searchKeys(mock(HttpServletRequest.class), SERVICE));
        ops.put("createKey", mgr -> mgr.createKey(SERVICE, newKey()));
        ops.put("rolloverKey", mgr -> mgr.rolloverKey(SERVICE, newKey()));
        ops.put("deleteKey", mgr -> mgr.deleteKey(SERVICE, "k1"));

        List<Arguments> ret = new ArrayList<>();
        ops.forEach((name, op) -> {
            for (String provider : BLOCKED_PROVIDERS) {
                ret.add(Arguments.of(name, op, provider));
            }
        });

        return ret.stream();
    }

    private static VXKmsKey newKey() {
        VXKmsKey key = new VXKmsKey();
        key.setName("k1");
        return key;
    }

    private static ServiceMgr newServiceMgr(String provider) throws Exception {
        ServiceMgr mgr = new ServiceMgr();
        RangerServiceService svcService = mock(RangerServiceService.class);
        TimedExecutor exec = mock(TimedExecutor.class);
        setField(mgr, "rangerSvcService", svcService);
        setField(mgr, "timedExecutor", exec);

        when(svcService.getConfigsWithDecryptedPassword(any(RangerService.class))).thenAnswer(inv -> configs(provider));

        return mgr;
    }

    private static ServiceStore kmsServiceStore() throws Exception {
        ServiceStore store = mock(ServiceStore.class);
        RangerServiceDef def = new RangerServiceDef();
        def.setName("kms");
        def.setImplClass(RangerDefaultService.class.getName());
        when(store.getServiceDefByName("kms")).thenReturn(def);
        return store;
    }

    private static KmsKeyMgr newKmsKeyMgr(String provider) throws Exception {
        KmsKeyMgr mgr = new KmsKeyMgr();
        ServiceDBStore store = mock(ServiceDBStore.class);
        RESTErrorUtil errorUtil = mock(RESTErrorUtil.class);

        setField(mgr, "svcStore", store);
        setField(mgr, "restErrorUtil", errorUtil);
        setField(mgr, "rangerBizUtil", mock(RangerBizUtil.class));

        when(store.getServiceByName(SERVICE)).thenReturn(kmsService(provider));
        when(errorUtil.createRESTException(anyString(), any(MessageEnums.class)))
                .thenAnswer(inv -> new WebApplicationException(new Exception(inv.getArgument(0, String.class)), 400));

        return mgr;
    }

    private static Client okClient() {
        Client client = mock(Client.class);
        // WebResource and its Builder are fluent: every call returns the builder, and get(String.class) returns the key JSON
        WebResource.Builder builder = mock(WebResource.Builder.class, inv ->
                "get".equals(inv.getMethod().getName()) ? "{\"name\":\"k1\"}" : inv.getMock());
        WebResource resource = mock(WebResource.class, inv ->
                WebResource.Builder.class.equals(inv.getMethod().getReturnType()) ? builder : null);

        when(client.resource(anyString())).thenReturn(resource);

        return client;
    }

    private static void assertBlockedHost(Exception thrown, String provider) {
        String message = thrown.getMessage() == null ? "" : thrown.getMessage();
        Assertions.assertTrue(message.contains("blocked host detected") || message.contains("could not be parsed securely"),
                "provider " + provider + " was not rejected as a blocked host: " + message);
    }

    private static Map<String, String> configs(String provider) {
        Map<String, String> configs = new HashMap<>();
        configs.put("provider", provider);
        return configs;
    }

    private static RangerService kmsService(String provider) {
        RangerService service = new RangerService();
        service.setName(SERVICE);
        service.setType("kms");
        service.setConfigs(configs(provider));
        return service;
    }

    static HttpServer loopbackServer(AtomicInteger hits) throws Exception {
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange -> {
            hits.incrementAndGet();
            byte[] body = "marker".getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(418, body.length);
            exchange.getResponseBody().write(body);
            exchange.close();
        });
        server.start();
        return server;
    }

    static HttpServer redirectServer(InetAddress address, int loopbackPort, AtomicInteger hits) throws Exception {
        HttpServer server = HttpServer.create(new InetSocketAddress(address, 0), 0);
        server.createContext("/", exchange -> {
            hits.incrementAndGet();
            exchange.getResponseHeaders().set("Location", "http://127.0.0.1:" + loopbackPort + "/kms/v1/key/k1/_metadata");
            exchange.sendResponseHeaders(302, -1);
            exchange.close();
        });
        server.start();
        return server;
    }

    static InetAddress localNonLoopbackAddress() throws Exception {
        Enumeration<NetworkInterface> ifaces = NetworkInterface.getNetworkInterfaces();
        while (ifaces.hasMoreElements()) {
            NetworkInterface iface = ifaces.nextElement();
            if (!iface.isUp() || iface.isLoopback()) {
                continue;
            }

            Enumeration<InetAddress> addresses = iface.getInetAddresses();
            while (addresses.hasMoreElements()) {
                InetAddress address = addresses.nextElement();
                if (address instanceof Inet4Address && !address.isLoopbackAddress() && !address.isLinkLocalAddress()) {
                    return address;
                }
            }
        }

        return null;
    }

    private static void restoreProperty(String name, String previous) {
        if (previous == null) {
            PropertiesUtil.getPropertiesMap().remove(name);
        } else {
            PropertiesUtil.getPropertiesMap().put(name, previous);
        }
    }

    private static Object getField(Object target, String fieldName) throws Exception {
        Field field = target.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        return field.get(target);
    }

    private static void setField(Object target, String fieldName, Object value) throws Exception {
        Field field = target.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(target, value);
    }
}
