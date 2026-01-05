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
package org.apache.ranger.services.nifi.client;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertThrows;

public class TestNiFiConnectionMgr {
    @Test
    public void testValidURLWithWrongEndPoint() throws Exception {
        final String nifiUrl = "http://localhost:8080/nifi";

        Map<String, String> configs = new HashMap<>();
        configs.put(NiFiConfigs.NIFI_URL, nifiUrl);
        configs.put(NiFiConfigs.NIFI_AUTHENTICATION_TYPE, NiFiAuthType.NONE.name());

        assertThrows(IllegalArgumentException.class, () -> NiFiConnectionMgr.getNiFiClient("nifi", configs));
    }

    @Test
    public void testInvalidURL() throws Exception {
        final String nifiUrl = "not a url";

        Map<String, String> configs = new HashMap<>();
        configs.put(NiFiConfigs.NIFI_URL, nifiUrl);
        configs.put(NiFiConfigs.NIFI_AUTHENTICATION_TYPE, NiFiAuthType.NONE.name());

        assertThrows(IllegalArgumentException.class, () -> NiFiConnectionMgr.getNiFiClient("nifi", configs));
    }

    @Test
    public void testAuthTypeNone() throws Exception {
        final String nifiUrl = "http://localhost:8080/nifi-api/resources";

        Map<String, String> configs = new HashMap<>();
        configs.put(NiFiConfigs.NIFI_URL, nifiUrl);
        configs.put(NiFiConfigs.NIFI_AUTHENTICATION_TYPE, NiFiAuthType.NONE.name());

        NiFiClient client = NiFiConnectionMgr.getNiFiClient("nifi", configs);
        Assertions.assertNotNull(client);
        Assertions.assertEquals(nifiUrl, client.getUrl());
        Assertions.assertNull(client.getSslContext());
    }

    @Test
    public void testAuthTypeNoneMissingURL() throws Exception {
        Map<String, String> configs = new HashMap<>();
        configs.put(NiFiConfigs.NIFI_URL, null);
        configs.put(NiFiConfigs.NIFI_AUTHENTICATION_TYPE, NiFiAuthType.NONE.name());

        assertThrows(IllegalArgumentException.class, () -> NiFiConnectionMgr.getNiFiClient("nifi", configs));
    }

    @Test
    public void testAuthTypeSSL() throws Exception {
        final String nifiUrl = "https://localhost:8080/nifi-api/resources";

        Map<String, String> configs = new HashMap<>();
        configs.put(NiFiConfigs.NIFI_URL, nifiUrl);
        configs.put(NiFiConfigs.NIFI_AUTHENTICATION_TYPE, NiFiAuthType.SSL.name());

        configs.put(NiFiConfigs.NIFI_SSL_KEYSTORE, "src/test/resources/missing.jks");
        configs.put(NiFiConfigs.NIFI_SSL_KEYSTORE_PASSWORD, "password");
        configs.put(NiFiConfigs.NIFI_SSL_KEYSTORE_TYPE, "JKS");

        configs.put(NiFiConfigs.NIFI_SSL_TRUSTSTORE, "src/test/resources/missing.jks");
        configs.put(NiFiConfigs.NIFI_SSL_TRUSTSTORE_PASSWORD, "password");
        configs.put(NiFiConfigs.NIFI_SSL_TRUSTSTORE_TYPE, "JKS");

        assertThrows(FileNotFoundException.class, () -> NiFiConnectionMgr.getNiFiClient("nifi", configs));
    }

    @Test
    public void testAuthTypeSSLWithNonHttpsUrl() throws Exception {
        final String nifiUrl = "http://localhost:8080/nifi-api/resources";

        Map<String, String> configs = new HashMap<>();
        configs.put(NiFiConfigs.NIFI_URL, nifiUrl);
        configs.put(NiFiConfigs.NIFI_AUTHENTICATION_TYPE, NiFiAuthType.SSL.name());

        configs.put(NiFiConfigs.NIFI_SSL_KEYSTORE, "src/test/resources/missing.jks");
        configs.put(NiFiConfigs.NIFI_SSL_KEYSTORE_PASSWORD, "password");
        configs.put(NiFiConfigs.NIFI_SSL_KEYSTORE_TYPE, "JKS");

        configs.put(NiFiConfigs.NIFI_SSL_TRUSTSTORE, "src/test/resources/missing.jks");
        configs.put(NiFiConfigs.NIFI_SSL_TRUSTSTORE_PASSWORD, "password");
        configs.put(NiFiConfigs.NIFI_SSL_TRUSTSTORE_TYPE, "JKS");

        assertThrows(IllegalArgumentException.class, () -> NiFiConnectionMgr.getNiFiClient("nifi", configs));
    }

    @Test
    public void testAuthTypeSSLMissingConfigs() throws Exception {
        final String nifiUrl = "http://localhost:8080/nifi";

        Map<String, String> configs = new HashMap<>();
        configs.put(NiFiConfigs.NIFI_URL, nifiUrl);
        configs.put(NiFiConfigs.NIFI_AUTHENTICATION_TYPE, NiFiAuthType.SSL.name());

        assertThrows(IllegalArgumentException.class, () -> NiFiConnectionMgr.getNiFiClient("nifi", configs));
    }
}
