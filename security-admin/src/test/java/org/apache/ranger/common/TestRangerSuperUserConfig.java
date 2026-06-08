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

package org.apache.ranger.common;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class TestRangerSuperUserConfig {
    @BeforeEach
    public void setUp() {
        RangerSuperUserConfig.resetForTests();
    }

    @AfterEach
    public void tearDown() {
        PropertiesUtil.getPropertiesMap().remove(RangerConstants.RANGER_ADMIN_SUPER_USERS);
        PropertiesUtil.getPropertiesMap().remove(RangerConstants.RANGER_ADMIN_SUPER_GROUPS);
        RangerSuperUserConfig.resetForTests();
    }

    @Test
    public void testIsSuperUser_ByConfiguredUser() {
        PropertiesUtil.getPropertiesMap().put(RangerConstants.RANGER_ADMIN_SUPER_USERS, "alice, BOB");

        Assertions.assertTrue(RangerSuperUserConfig.isSuperUser("alice", Collections.emptySet()));
        Assertions.assertTrue(RangerSuperUserConfig.isSuperUser("bob", Collections.emptySet()));
        Assertions.assertFalse(RangerSuperUserConfig.isSuperUser("carol", Collections.emptySet()));
    }

    @Test
    public void testIsSuperUser_ByConfiguredGroup() {
        PropertiesUtil.getPropertiesMap().put(RangerConstants.RANGER_ADMIN_SUPER_GROUPS, "ranger-admins, ops");

        Set<String> groups = new HashSet<>();

        groups.add("ops");

        Assertions.assertTrue(RangerSuperUserConfig.isSuperUser("carol", groups));
        Assertions.assertFalse(RangerSuperUserConfig.isSuperUser("carol", Collections.emptySet()));
    }

    @Test
    public void testIsSuperUser_ByConfiguredGroupCaseInsensitive() {
        PropertiesUtil.getPropertiesMap().put(
                RangerConstants.RANGER_ADMIN_SUPER_GROUPS, "testgroup_3A");

        Set<String> groups = new HashSet<>();

        groups.add("testgroup_3a");

        Assertions.assertTrue(
                RangerSuperUserConfig.isSuperUser("testuser_7", groups));

        PropertiesUtil.getPropertiesMap().put(
                RangerConstants.RANGER_ADMIN_SUPER_GROUPS, "Ranger-Admins");

        groups.clear();
        groups.add("ranger-admins");

        Assertions.assertTrue(
                RangerSuperUserConfig.isSuperUser("carol", groups));
    }

    @Test
    public void testIsSuperUser_WildcardUser() {
        PropertiesUtil.getPropertiesMap().put(RangerConstants.RANGER_ADMIN_SUPER_USERS, "*");

        Assertions.assertTrue(RangerSuperUserConfig.isSuperUser("anyone", Collections.emptySet()));
    }

    @Test
    public void testIsSuperUser_PublicGroup() {
        PropertiesUtil.getPropertiesMap().put(
                RangerConstants.RANGER_ADMIN_SUPER_GROUPS,
                RangerConstants.GROUP_PUBLIC);

        Assertions.assertTrue(RangerSuperUserConfig.isSuperUser("carol", Collections.singleton("other-group")));
    }

    @Test
    public void testIsSuperUser_Unconfigured() {
        Assertions.assertFalse(RangerSuperUserConfig.isSuperUser("alice", Collections.emptySet()));
    }

    @Test
    public void testIsSuperUser_BlankLoginId() {
        PropertiesUtil.getPropertiesMap().put(RangerConstants.RANGER_ADMIN_SUPER_USERS, "alice");

        Assertions.assertFalse(RangerSuperUserConfig.isSuperUser(null, Collections.emptySet()));
        Assertions.assertFalse(RangerSuperUserConfig.isSuperUser("  ", Collections.emptySet()));
    }

    @Test
    public void testMergeConfigSuperUserRoles_ForAuthentication() {
        List<String> merged = RangerSuperUserConfig.mergeConfigSuperUserRoles(
                Collections.singletonList(RangerConstants.ROLE_USER), false);

        Assertions.assertEquals(
                Arrays.asList(
                        RangerConstants.ROLE_SYS_ADMIN,
                        RangerConstants.ROLE_KEY_ADMIN,
                        RangerConstants.ROLE_USER),
                merged);
    }

    @Test
    public void testMergeConfigSuperUserRoles_ForSession() {
        List<String> merged = RangerSuperUserConfig.mergeConfigSuperUserRoles(
                Collections.singletonList(RangerConstants.ROLE_ADMIN_AUDITOR),
                true);

        Assertions.assertEquals(
                Arrays.asList(
                        RangerConstants.ROLE_SYS_ADMIN,
                        RangerConstants.ROLE_KEY_ADMIN,
                        RangerConstants.ROLE_USER,
                        RangerConstants.ROLE_ADMIN_AUDITOR),
                merged);
    }

    @Test
    public void testGetConfigSuperUserProfileRoles() {
        Assertions.assertEquals(
                Arrays.asList(
                        RangerConstants.ROLE_SYS_ADMIN,
                        RangerConstants.ROLE_KEY_ADMIN),
                RangerSuperUserConfig.getConfigSuperUserProfileRoles());
    }

    @Test
    public void testIsEnabled_WhenBothPropertiesAbsent() {
        Assertions.assertFalse(RangerSuperUserConfig.isEnabled());
    }

    @Test
    public void testIsEnabled_WhenSuperUsersSet() {
        PropertiesUtil.getPropertiesMap().put(
                RangerConstants.RANGER_ADMIN_SUPER_USERS, "alice");

        Assertions.assertTrue(RangerSuperUserConfig.isEnabled());
    }

    @Test
    public void testIsEnabled_WhenSuperGroupsSet() {
        PropertiesUtil.getPropertiesMap().put(
                RangerConstants.RANGER_ADMIN_SUPER_GROUPS, "ranger-admins");

        Assertions.assertTrue(RangerSuperUserConfig.isEnabled());
    }

    @Test
    public void testIsEnabled_WhenPropertiesBlankOnly() {
        PropertiesUtil.getPropertiesMap().put(
                RangerConstants.RANGER_ADMIN_SUPER_USERS, "  ,  ");
        PropertiesUtil.getPropertiesMap().put(
                RangerConstants.RANGER_ADMIN_SUPER_GROUPS, "");

        Assertions.assertFalse(RangerSuperUserConfig.isEnabled());
    }
}
