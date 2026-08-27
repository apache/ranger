/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information regarding copyright ownership.
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

package org.apache.ranger.audit.partition;

import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ServiceAllowedUsersUtilTest {
    @Test
    public void testParseUsersFromConfigValue() {
        assertIterableEquals(List.of("hive", "hive2"), ServiceAllowedUsersUtil.parseUsers(" hive, hive2 "));
    }

    @Test
    public void testParseUsersIgnoresWildcard() {
        assertTrue(ServiceAllowedUsersUtil.parseUsers("*").isEmpty());
    }

    @Test
    public void testNormalizeServiceAllowedUsersSkipsEmptyEntries() {
        Map<String, List<String>> input = new LinkedHashMap<>();
        input.put("dev_hive", List.of("hive"));
        input.put("dev_empty", List.of());
        input.put("dev_wildcard", List.of("*"));

        Map<String, List<String>> normalized = ServiceAllowedUsersUtil.normalizeServiceAllowedUsers(input);

        assertEquals(1, normalized.size());
        assertIterableEquals(List.of("hive"), normalized.get("dev_hive"));
    }

    @Test
    public void testNormalizeServiceAllowedUsersSkipsNullListElements() {
        Map<String, List<String>> input = new LinkedHashMap<>();
        input.put("dev_hive", java.util.Arrays.asList("hive", null));

        Map<String, List<String>> normalized = ServiceAllowedUsersUtil.normalizeServiceAllowedUsers(input);

        assertEquals(1, normalized.size());
        assertIterableEquals(List.of("hive"), normalized.get("dev_hive"));
    }
}
