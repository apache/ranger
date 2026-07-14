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

package org.apache.ranger.services.solr.client;

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
public class TestServiceSolrClient {
    @Test
    public void test01_validateResourceName_rejectsPathTraversal() throws Exception {
        Map<String, String> configs = new HashMap<>();
        configs.put("username", "test");
        configs.put("password", "test");
        NoopServiceSolrClient client = new NoopServiceSolrClient("svc", configs, "http://localhost:8983/solr", false);

        List<String> pathTraversalInputs = Arrays.asList("../etc/passwd", "../../sensitive", "test/../admin", "collection//malicious", "test\\windows\\path", "..\\..\\config");

        for (String input : pathTraversalInputs) {
            HadoopException ex = assertThrows(HadoopException.class,
                    () -> invokeGetFieldList(client, input, null),
                    "Path traversal should be rejected: " + input);
            assertTrue(ex.getMessage().contains("Path traversal"),
                    "Error should indicate path traversal for: " + input);
        }
    }

    @Test
    public void test02_validateResourceName_rejectsSpecialCharacters() throws Exception {
        Map<String, String> configs = new HashMap<>();
        configs.put("username", "test");
        configs.put("password", "test");
        NoopServiceSolrClient client = new NoopServiceSolrClient("svc", configs, "http://localhost:8983/solr", false);

        List<String> invalidInputs = Arrays.asList("'; DROP TABLE users; --", "collection<script>alert(1)</script>", "test@collection", "collection#name", "test!collection", "collection&name", "collection(with)parens", "collection{with}braces", "collection[with]brackets", "collection$name", "collection%encoded", "collection name", "collection\ttab", "collection\nnewline");

        for (String input : invalidInputs) {
            HadoopException ex = assertThrows(HadoopException.class,
                    () -> invokeGetFieldList(client, input, null),
                    "Special characters should be rejected: " + input);
            assertTrue(ex.getMessage().contains("Invalid"),
                    "Error should indicate invalid input for: " + input);
        }
    }

    @Test
    public void test03_validateResourceName_acceptsValidNames() throws Exception {
        Map<String, String> configs = new HashMap<>();
        configs.put("username", "test");
        configs.put("password", "test");
        NoopServiceSolrClient client = new NoopServiceSolrClient("svc", configs, "http://localhost:8983/solr", false);

        List<String> validInputs = Arrays.asList("collection", "collection_name", "collection123", "COLLECTION", "Collection_Name_123", "_collection", "collection_", "collection.name", "collection-name", "collection*", "coll*");

        Method validateMethod = ServiceSolrClient.class.getDeclaredMethod("validateResourceName", String.class, String.class);
        validateMethod.setAccessible(true);

        for (String input : validInputs) {
            try {
                validateMethod.invoke(client, input, "collection name");
            } catch (Exception e) {
                throw new AssertionError("Valid collection name should not throw exception: " + input, e);
            }
        }
    }

    @Test
    public void test04_validateResourceName_rejectsNullByteInjection() throws Exception {
        Map<String, String> configs = new HashMap<>();
        configs.put("username", "test");
        configs.put("password", "test");
        NoopServiceSolrClient client = new NoopServiceSolrClient("svc", configs, "http://localhost:8983/solr", false);

        HadoopException ex = assertThrows(HadoopException.class,
                () -> invokeGetFieldList(client, "collection\0null", null));
        assertTrue(ex.getMessage().contains("Invalid"));
    }

    @Test
    public void test05_validateResourceName_rejectsCommandInjection() throws Exception {
        Map<String, String> configs = new HashMap<>();
        configs.put("username", "test");
        configs.put("password", "test");
        NoopServiceSolrClient client = new NoopServiceSolrClient("svc", configs, "http://localhost:8983/solr", false);

        List<String> commandInjectionInputs = Arrays.asList("collection;rm -rf /", "collection|cat /etc/passwd", "collection`whoami`", "collection$(whoami)", "collection&&ls");

        for (String input : commandInjectionInputs) {
            HadoopException ex = assertThrows(HadoopException.class,
                    () -> invokeGetFieldList(client, input, null),
                    "Command injection should be rejected: " + input);
            assertTrue(ex.getMessage().contains("Invalid"),
                    "Error should indicate invalid input for: " + input);
        }
    }

    @Test
    public void test06_validateResourceName_rejectsUrlEncoded() throws Exception {
        Map<String, String> configs = new HashMap<>();
        configs.put("username", "test");
        configs.put("password", "test");
        NoopServiceSolrClient client = new NoopServiceSolrClient("svc", configs, "http://localhost:8983/solr", false);

        List<String> encodedInputs = Arrays.asList("%2e%2e%2f", "collection%00", "test%20space");

        for (String input : encodedInputs) {
            HadoopException ex = assertThrows(HadoopException.class,
                    () -> invokeGetFieldList(client, input, null),
                    "URL encoded attack should be rejected: " + input);
            assertTrue(ex.getMessage().contains("Invalid"),
                    "Error should indicate invalid input for: " + input);
        }
    }

    private List<String> invokeGetFieldList(ServiceSolrClient client, String collection, List<String> ignoreList) throws Exception {
        Method method = ServiceSolrClient.class.getDeclaredMethod("getFieldList", String.class, List.class);
        method.setAccessible(true);
        try {
            return (List<String>) method.invoke(client, collection, ignoreList);
        } catch (java.lang.reflect.InvocationTargetException e) {
            Throwable cause = e.getCause();
            if (cause instanceof HadoopException) {
                throw (HadoopException) cause;
            }
            throw e;
        }
    }

    private static class NoopServiceSolrClient extends ServiceSolrClient {
        public NoopServiceSolrClient(String serviceName, Map<String, String> configs, String url, boolean isSolrCloud) {
            super(serviceName, configs, url, isSolrCloud);
        }
    }
}
