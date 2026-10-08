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

package org.apache.ranger.authorization.sqoop.authorizer;

import org.apache.ranger.plugin.policyengine.RangerAccessRequest;
import org.apache.ranger.plugin.policyengine.RangerAccessResult;
import org.apache.ranger.plugin.service.RangerBasePlugin;
import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.model.MPrincipal;
import org.apache.sqoop.model.MPrivilege;
import org.apache.sqoop.model.MResource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Verifies fail-closed behavior when the Ranger plugin is unavailable or returns no decision.
 */
public class RangerSqoopAuthorizerFailClosedTest {
    private static final String TEST_USER = "test-user";

    private RangerSqoopAuthorizer authorizer;

    private Object sqoopPluginBefore;

    @BeforeEach
    public void setUp() throws Exception {
        sqoopPluginBefore = getSqoopPluginField().get(null);
        authorizer          = new RangerSqoopAuthorizer();
    }

    @AfterEach
    public void tearDown() throws Exception {
        getSqoopPluginField().set(null, sqoopPluginBefore);
    }

    @Test
    public void checkPrivilegesDeniesWhenPluginReturnsNull() throws Exception {
        Object plugin = mockSqoopPlugin();

        when(((RangerBasePlugin) plugin).isAccessAllowed(any(RangerAccessRequest.class))).thenReturn(null);

        setSqoopPlugin(plugin);

        assertThrows(SqoopException.class, () -> authorizer.checkPrivileges(buildUserPrincipal(), buildReadConnectorPrivilege()));
    }

    @Test
    public void checkPrivilegesDeniesWhenPluginIsNull() throws Exception {
        setSqoopPlugin(null);

        assertThrows(SqoopException.class, () -> authorizer.checkPrivileges(buildUserPrincipal(), buildReadConnectorPrivilege()));
    }

    @Test
    public void checkPrivilegesDeniesWhenPluginReturnsNotAllowed() throws Exception {
        Object              plugin = mockSqoopPlugin();
        RangerAccessResult  result = mock(RangerAccessResult.class);

        when(result.getIsAllowed()).thenReturn(false);
        wireIsAccessAllowed(plugin, result);

        setSqoopPlugin(plugin);

        assertThrows(SqoopException.class, () -> authorizer.checkPrivileges(buildUserPrincipal(), buildReadConnectorPrivilege()));
    }

    @Test
    public void checkPrivilegesAllowsWhenPluginReturnsAllowed() throws Exception {
        Object             plugin = mockSqoopPlugin();
        RangerAccessResult result = mock(RangerAccessResult.class);

        when(result.getIsAllowed()).thenReturn(true);
        wireIsAccessAllowed(plugin, result);

        setSqoopPlugin(plugin);

        assertDoesNotThrow(() -> authorizer.checkPrivileges(buildUserPrincipal(), buildReadConnectorPrivilege()));
    }

    private static Field getSqoopPluginField() throws NoSuchFieldException {
        Field field = RangerSqoopAuthorizer.class.getDeclaredField("sqoopPlugin");

        field.setAccessible(true);

        return field;
    }

    private static void setSqoopPlugin(Object plugin) throws Exception {
        getSqoopPluginField().set(null, plugin);
    }

    private static Object mockSqoopPlugin() throws ClassNotFoundException {
        Class<?> pluginClass = Class.forName("org.apache.ranger.authorization.sqoop.authorizer.RangerSqoopAuthorizer$RangerSqoopPlugin");

        return mock(pluginClass);
    }

    private static void wireIsAccessAllowed(Object plugin, RangerAccessResult result) {
        when(((RangerBasePlugin) plugin).isAccessAllowed(any(RangerAccessRequest.class))).thenReturn(result);
    }

    private static MPrincipal buildUserPrincipal() {
        MPrincipal principal = mock(MPrincipal.class);

        when(principal.getType()).thenReturn(MPrincipal.TYPE.USER.name());
        when(principal.getName()).thenReturn(TEST_USER);

        return principal;
    }

    private static List<MPrivilege> buildReadConnectorPrivilege() {
        MResource resource = mock(MResource.class);

        when(resource.getType()).thenReturn(MResource.TYPE.CONNECTOR.name());
        when(resource.getName()).thenReturn("hdfs-connector");

        MPrivilege privilege = mock(MPrivilege.class);

        when(privilege.getResource()).thenReturn(resource);
        when(privilege.getAction()).thenReturn("read");

        return Collections.singletonList(privilege);
    }
}
