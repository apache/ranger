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

package org.apache.ranger.services.elasticsearch.client;

import org.apache.ranger.plugin.client.HadoopException;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(MockitoExtension.class)
@TestMethodOrder(MethodOrderer.MethodName.class)
public class TestElasticsearchClient {
    @Test
    public void test01_validateUrlResourceName_rejectsPathTraversal() throws Exception {
        Map<String, String> configs = new HashMap<>();
        configs.put("elasticsearch.url", "http://localhost:9200");
        configs.put("username", "test");
        ElasticsearchClient client = new ElasticsearchClient("svc", configs);

        List<String> pathTraversalInputs = Arrays.asList(
                "../etc/passwd",
                "../../sensitive",
                "test/../admin",
                "index//malicious",
                "test\\windows\\path",
                "..\\..\\config");

        for (String input : pathTraversalInputs) {
            HadoopException ex = assertThrows(HadoopException.class,
                    () -> invokeValidateUrlResourceName(client, input, "index pattern"),
                    "Path traversal should be rejected: " + input);
            assertTrue(ex.getMessage().contains("Path traversal"),
                    "Error should indicate path traversal for: " + input);
        }
    }

    @Test
    public void test02_validateUrlResourceName_rejectsSpecialCharacters() throws Exception {
        Map<String, String> configs = new HashMap<>();
        configs.put("elasticsearch.url", "http://localhost:9200");
        configs.put("username", "test");
        ElasticsearchClient client = new ElasticsearchClient("svc", configs);

        List<String> invalidInputs = Arrays.asList(
                "'; DROP TABLE users; --",
                "index<script>alert(1)</script>",
                "test@index",
                "index#name",
                "test!index",
                "index&name",
                "index(with)parens",
                "index{with}braces",
                "index[with]brackets",
                "index$name",
                "index%encoded",
                "index name",
                "index\ttab",
                "index\nnewline",
                "index;rm -rf /",
                "index|cat /etc/passwd",
                "index`whoami`",
                "index$(whoami)");

        for (String input : invalidInputs) {
            HadoopException ex = assertThrows(HadoopException.class,
                    () -> invokeValidateUrlResourceName(client, input, "index pattern"),
                    "Special characters should be rejected: " + input);
            assertTrue(ex.getMessage().contains("Invalid"),
                    "Error should indicate invalid input for: " + input);
        }
    }

    @Test
    public void test03_validateUrlResourceName_acceptsValidNames() throws Exception {
        Map<String, String> configs = new HashMap<>();
        configs.put("elasticsearch.url", "http://localhost:9200");
        configs.put("username", "test");
        ElasticsearchClient client = new ElasticsearchClient("svc", configs);

        List<String> validInputs = Arrays.asList(
                "index",
                "index_name",
                "index123",
                "INDEX",
                "Index_Name_123",
                "_index",
                "index_",
                "index.name",
                "index-name",
                "index*",
                "idx*");

        for (String input : validInputs) {
            try {
                invokeValidateUrlResourceName(client, input, "index pattern");
            } catch (Exception e) {
                throw new AssertionError("Valid index name should not throw exception: " + input, e);
            }
        }
    }

    @Test
    public void test04_validateUrlResourceName_rejectsNullByteInjection() throws Exception {
        Map<String, String> configs = new HashMap<>();
        configs.put("elasticsearch.url", "http://localhost:9200");
        configs.put("username", "test");
        ElasticsearchClient client = new ElasticsearchClient("svc", configs);

        HadoopException ex = assertThrows(HadoopException.class,
                () -> invokeValidateUrlResourceName(client, "index\0null", "index pattern"));
        assertTrue(ex.getMessage().contains("Invalid"));
    }

    @Test
    public void test05_validateUrlResourceName_rejectsUrlEncoded() throws Exception {
        Map<String, String> configs = new HashMap<>();
        configs.put("elasticsearch.url", "http://localhost:9200");
        configs.put("username", "test");
        ElasticsearchClient client = new ElasticsearchClient("svc", configs);

        List<String> encodedInputs = Arrays.asList(
                "%2e%2e%2f",
                "index%00",
                "test%20space");

        for (String input : encodedInputs) {
            HadoopException ex = assertThrows(HadoopException.class,
                    () -> invokeValidateUrlResourceName(client, input, "index pattern"),
                    "URL encoded attack should be rejected: " + input);
            assertTrue(ex.getMessage().contains("Invalid"),
                    "Error should indicate invalid input for: " + input);
        }
    }

    private void invokeValidateUrlResourceName(ElasticsearchClient client, String resourceName, String resourceType) throws Exception {
        Method method = ElasticsearchClient.class.getSuperclass().getDeclaredMethod("validateUrlResourceName", String.class, String.class);
        method.setAccessible(true);
        try {
            method.invoke(client, resourceName, resourceType);
        } catch (java.lang.reflect.InvocationTargetException e) {
            Throwable cause = e.getCause();
            if (cause instanceof HadoopException) {
                throw (HadoopException) cause;
            }
            throw e;
        }
    }
}
