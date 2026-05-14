/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ranger.services.schema.registry.client;

import org.apache.ranger.services.schema.registry.client.connection.DefaultSchemaRegistryClient;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static com.hortonworks.registries.schemaregistry.client.SchemaRegistryClient.Configuration.SCHEMA_REGISTRY_URL;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class UrlSelectorTest {
    @Test
    void testCreateUrlSelectorWithUnauthorizedClass() {
        Map<String, Object> conf = new HashMap<>();
        conf.put(SCHEMA_REGISTRY_URL.name(), "http://localhost:8060/api/v1");
        conf.put("schema.registry.client.url.selector", "org.springframework.context.support.ClassPathXmlApplicationContext");
        RuntimeException ex = assertThrows(RuntimeException.class, () -> new DefaultSchemaRegistryClient(conf));
        assertTrue(ex.getMessage().contains(DefaultSchemaRegistryClient.ERR_CLASS_NOT_IMPLEMENTING_URL_SELECTOR));
    }

    @Test
    void testCreateUrlSelectorWithValidClass() {
        Map<String, Object> conf = new HashMap<>();
        conf.put(SCHEMA_REGISTRY_URL.name(), "http://localhost:8060/api/v1");
        conf.put("schema.registry.client.url.selector", "com.hortonworks.registries.schemaregistry.client.LoadBalancedFailoverUrlSelector");
        assertDoesNotThrow(() -> new DefaultSchemaRegistryClient(conf));
    }

    @Test
    void testCreateUrlSelectorDefault() {
        Map<String, Object> conf = new HashMap<>();
        conf.put(SCHEMA_REGISTRY_URL.name(), "http://localhost:8060/api/v1");
        assertDoesNotThrow(() -> new DefaultSchemaRegistryClient(conf));
    }

    @Test
    void testCreateUrlSelectorWithNonExistentClass() {
        Map<String, Object> conf = new HashMap<>();
        conf.put(SCHEMA_REGISTRY_URL.name(), "http://localhost:8060/api/v1");
        conf.put("schema.registry.client.url.selector", "com.nonexistent.FakeSelector");
        RuntimeException ex = assertThrows(RuntimeException.class, () -> new DefaultSchemaRegistryClient(conf));
        assertInstanceOf(ClassNotFoundException.class, ex.getCause());
    }

    @Test
    void testCreateUrlSelectorWithNullSelector() {
        Map<String, Object> conf = new HashMap<>();
        conf.put(SCHEMA_REGISTRY_URL.name(), "http://localhost:8060/api/v1");
        conf.put("schema.registry.client.url.selector", null);
        assertDoesNotThrow(() -> new DefaultSchemaRegistryClient(conf));
    }

    @Test
    void testCreateUrlSelectorWithEmptySelector() {
        Map<String, Object> conf = new HashMap<>();
        conf.put(SCHEMA_REGISTRY_URL.name(), "http://localhost:8060/api/v1");
        conf.put("schema.registry.client.url.selector", "");
        assertThrows(IllegalArgumentException.class, () -> new DefaultSchemaRegistryClient(conf));
    }
}
