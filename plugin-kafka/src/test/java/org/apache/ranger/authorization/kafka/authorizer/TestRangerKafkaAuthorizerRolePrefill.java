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

package org.apache.ranger.authorization.kafka.authorizer;

import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.server.authorizer.Action;
import org.apache.kafka.server.authorizer.AuthorizableRequestContext;
import org.apache.kafka.server.authorizer.AuthorizationResult;
import org.apache.ranger.plugin.policyengine.RangerAccessRequest;
import org.apache.ranger.plugin.policyengine.RangerAccessRequestImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessResult;
import org.apache.ranger.plugin.service.RangerAuthContext;
import org.apache.ranger.plugin.service.RangerBasePlugin;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.eq;

public class TestRangerKafkaAuthorizerRolePrefill {
    @AfterEach
    public void cleanup() throws Exception {
        setStaticPlugin(null);
    }

    @Test
    public void authorizePrefillsRolesWhenAuthContextReturnsRoles() throws Exception {
        RangerBasePlugin pluginMock     = Mockito.mock(RangerBasePlugin.class);
        RangerAuthContext authCtxMock   = Mockito.mock(RangerAuthContext.class);
        RangerAccessResult accessResult = Mockito.mock(RangerAccessResult.class);
        ArgumentCaptor<Collection<RangerAccessRequest>> requestsCaptor = ArgumentCaptor.forClass(Collection.class);
        Set<String> roles = new HashSet<>(Arrays.asList("roleA", "roleB"));

        setStaticPlugin(pluginMock);

        Mockito.when(pluginMock.getCurrentRangerAuthContext()).thenReturn(authCtxMock);
        Mockito.when(authCtxMock.getRolesForUserAndGroups(eq("alice"), anySet())).thenReturn(roles);
        Mockito.when(pluginMock.isAccessAllowed(requestsCaptor.capture())).thenReturn(Collections.singletonList(accessResult));
        Mockito.when(accessResult.getIsAllowed()).thenReturn(true);

        RangerKafkaAuthorizer authorizer = new RangerKafkaAuthorizer();
        List<AuthorizationResult> out = authorizer.authorize(mockContext("alice"), Collections.singletonList(topicAction(AclOperation.READ, "t")));

        assertEquals(Collections.singletonList(AuthorizationResult.ALLOWED), out);

        RangerAccessRequestImpl captured = (RangerAccessRequestImpl) requestsCaptor.getValue().iterator().next();
        assertEquals(roles, captured.getUserRoles());
    }

    @Test
    public void authorizeDoesNotPrefillRolesWhenAuthContextAbsent() throws Exception {
        RangerBasePlugin pluginMock     = Mockito.mock(RangerBasePlugin.class);
        RangerAccessResult accessResult = Mockito.mock(RangerAccessResult.class);
        ArgumentCaptor<Collection<RangerAccessRequest>> requestsCaptor = ArgumentCaptor.forClass(Collection.class);

        setStaticPlugin(pluginMock);

        Mockito.when(pluginMock.getCurrentRangerAuthContext()).thenReturn(null);
        Mockito.when(pluginMock.isAccessAllowed(requestsCaptor.capture())).thenReturn(Collections.singletonList(accessResult));
        Mockito.when(accessResult.getIsAllowed()).thenReturn(true);

        RangerKafkaAuthorizer authorizer = new RangerKafkaAuthorizer();
        List<AuthorizationResult> out = authorizer.authorize(mockContext("bob"), Collections.singletonList(topicAction(AclOperation.READ, "t")));

        assertEquals(Collections.singletonList(AuthorizationResult.ALLOWED), out);

        RangerAccessRequestImpl captured = (RangerAccessRequestImpl) requestsCaptor.getValue().iterator().next();
        assertTrue(captured.getUserRoles() == null || captured.getUserRoles().isEmpty());
    }

    @Test
    public void authorizeDoesNotPrefillRolesWhenResolvedRolesEmptyOrNull() throws Exception {
        RangerBasePlugin pluginMock     = Mockito.mock(RangerBasePlugin.class);
        RangerAuthContext authCtxMock   = Mockito.mock(RangerAuthContext.class);
        RangerAccessResult accessResult = Mockito.mock(RangerAccessResult.class);
        ArgumentCaptor<Collection<RangerAccessRequest>> requestsCaptor = ArgumentCaptor.forClass(Collection.class);

        setStaticPlugin(pluginMock);

        Mockito.when(pluginMock.getCurrentRangerAuthContext()).thenReturn(authCtxMock);
        Mockito.when(authCtxMock.getRolesForUserAndGroups(eq("charlie"), anySet()))
                .thenReturn(Collections.emptySet())
                .thenReturn(null);
        Mockito.when(pluginMock.isAccessAllowed(anyCollection())).thenReturn(Collections.singletonList(accessResult));
        Mockito.when(accessResult.getIsAllowed()).thenReturn(true);

        RangerKafkaAuthorizer authorizer = new RangerKafkaAuthorizer();

        authorizer.authorize(mockContext("charlie"), Collections.singletonList(topicAction(AclOperation.READ, "t")));
        authorizer.authorize(mockContext("charlie"), Collections.singletonList(topicAction(AclOperation.READ, "t")));

        Mockito.verify(pluginMock, Mockito.times(2)).isAccessAllowed(requestsCaptor.capture());

        List<Collection<RangerAccessRequest>> allCalls = requestsCaptor.getAllValues();
        RangerAccessRequestImpl first = (RangerAccessRequestImpl) allCalls.get(0).iterator().next();
        RangerAccessRequestImpl second = (RangerAccessRequestImpl) allCalls.get(1).iterator().next();

        assertTrue(first.getUserRoles() == null || first.getUserRoles().isEmpty());
        assertTrue(second.getUserRoles() == null || second.getUserRoles().isEmpty());
    }

    private static AuthorizableRequestContext mockContext(String user) {
        AuthorizableRequestContext ret = Mockito.mock(AuthorizableRequestContext.class);

        Mockito.when(ret.principal()).thenReturn(new KafkaPrincipal(KafkaPrincipal.USER_TYPE, user));
        Mockito.when(ret.clientAddress()).thenReturn(null);
        Mockito.when(ret.clientId()).thenReturn("cid");

        return ret;
    }

    private static Action topicAction(AclOperation operation, String topic) {
        ResourcePattern resource = new ResourcePattern(ResourceType.TOPIC, topic, PatternType.LITERAL);

        return new Action(operation, resource, 1, true, true);
    }

    private static void setStaticPlugin(RangerBasePlugin plugin) throws Exception {
        Field field = RangerKafkaAuthorizer.class.getDeclaredField("rangerPlugin");

        field.setAccessible(true);
        field.set(null, plugin);
    }
}
