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
 * software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ranger.audit.rest;

import org.apache.ranger.admin.client.RangerAdminRESTClient;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AuditRESTTest {
    @Test
    void testMergeAllowedUsersUnionsServiceAccountUsers() {
        Map<String, Set<String>> configuredAllowedUsers = new HashMap<>();

        configuredAllowedUsers.put("dev_hive", new HashSet<>(Collections.singleton("hive")));

        Map<String, Set<String>> serviceAccountUsersByServiceName = new HashMap<>();

        serviceAccountUsersByServiceName.put("dev_hive", Collections.singleton("hive2"));
        serviceAccountUsersByServiceName.put("dev_kafka", Collections.singleton("kafka"));

        Map<String, Set<String>> mergedAllowedUsers = AuditREST.mergeAllowedUsers(configuredAllowedUsers, serviceAccountUsersByServiceName);

        assertEquals(new HashSet<>(Arrays.asList("hive", "hive2")), mergedAllowedUsers.get("dev_hive"));
        assertEquals(Collections.singleton("kafka"), mergedAllowedUsers.get("dev_kafka"));
    }

    @Test
    void testRefreshKeepsAllowedUsersWhenAdminCallFails() throws Exception {
        Map<String, Set<String>> before = allowedUsers();

        AuditREST.refreshAllowedUsers(new RangerAdminRESTClient() {
            @Override
            public Map<String, Set<String>> getServiceAccountUsers() throws Exception {
                throw new Exception("admin down");
            }
        });

        assertSame(before, allowedUsers());
    }

    @Test
    void testRefreshAddsServiceAccountUsers() throws Exception {
        Map<String, Set<String>> before = allowedUsers();

        try {
            AuditREST.refreshAllowedUsers(new RangerAdminRESTClient() {
                @Override
                public Map<String, Set<String>> getServiceAccountUsers() {
                    return Collections.singletonMap("dev_new", Collections.singleton("new-user"));
                }
            });

            assertTrue(allowedUsers().get("dev_new").contains("new-user"));
        } finally {
            AuditREST.refreshAllowedUsers(new RangerAdminRESTClient() {
                @Override
                public Map<String, Set<String>> getServiceAccountUsers() {
                    return Collections.emptyMap();
                }
            });

            assertEquals(before.keySet(), allowedUsers().keySet());
        }
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Set<String>> allowedUsers() throws Exception {
        Field field = AuditREST.class.getDeclaredField("allowedServiceUsers");

        field.setAccessible(true);

        return (Map<String, Set<String>>) field.get(null);
    }
}
