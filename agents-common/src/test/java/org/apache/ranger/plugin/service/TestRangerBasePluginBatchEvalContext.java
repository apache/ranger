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

package org.apache.ranger.plugin.service;

import org.apache.ranger.authorization.hadoop.config.RangerPluginConfig;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerRole;
import org.apache.ranger.plugin.policyengine.RangerAccessRequest;
import org.apache.ranger.plugin.policyengine.RangerAccessRequestImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessResourceImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessResult;
import org.apache.ranger.plugin.policyengine.RangerPolicyEngine;
import org.apache.ranger.plugin.policyengine.RangerPolicyEngineOptions;
import org.apache.ranger.plugin.store.EmbeddedServiceDefsUtil;
import org.apache.ranger.plugin.util.RangerAccessRequestUtil;
import org.apache.ranger.plugin.util.RangerBatchEvalContext;
import org.apache.ranger.plugin.util.RangerRoles;
import org.apache.ranger.plugin.util.ServicePolicies;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestRangerBasePluginBatchEvalContext {
    @Test
    void collectionAccessAttachesOneBatchEvalContextAndRemovesIt() throws Exception {
        RangerBasePlugin plugin = new RangerBasePlugin("hbase", "hbase");
        RangerPolicyEngine policyEngine = Mockito.mock(RangerPolicyEngine.class);
        setPolicyEngine(plugin, policyEngine);

        RangerAccessRequestImpl first = new RangerAccessRequestImpl();
        RangerAccessRequestImpl second = new RangerAccessRequestImpl();
        RangerAccessRequest nullContext = Mockito.mock(RangerAccessRequest.class);
        Mockito.when(nullContext.getContext()).thenReturn(null);

        Mockito.when(policyEngine.evaluatePolicies(Mockito.anyCollection(), Mockito.eq(RangerPolicy.POLICY_TYPE_ACCESS), Mockito.isNull())).thenAnswer(invocation -> {
            Collection<RangerAccessRequest> seen = invocation.getArgument(0);
            RangerBatchEvalContext shared = null;

            for (RangerAccessRequest request : seen) {
                if (request == null || request.getContext() == null) {
                    continue;
                }

                RangerBatchEvalContext batchEvalContext = RangerAccessRequestUtil.getBatchEvalContext(request.getContext());

                assertNotNull(batchEvalContext);

                if (shared == null) {
                    shared = batchEvalContext;
                } else {
                    assertSame(shared, batchEvalContext);
                }
            }

            assertNotNull(shared);

            return Collections.emptyList();
        });

        plugin.isAccessAllowed(Arrays.asList(first, null, second, nullContext), null);

        assertNull(RangerAccessRequestUtil.getBatchEvalContext(first.getContext()));
        assertNull(RangerAccessRequestUtil.getBatchEvalContext(second.getContext()));
    }

    @Test
    void collectionAccessRemovesBatchEvalContextWhenEvaluationThrows() throws Exception {
        RangerBasePlugin plugin = new RangerBasePlugin("hbase", "hbase");
        RangerPolicyEngine policyEngine = Mockito.mock(RangerPolicyEngine.class);
        setPolicyEngine(plugin, policyEngine);

        RangerAccessRequestImpl request = new RangerAccessRequestImpl();
        Mockito.when(policyEngine.evaluatePolicies(Mockito.anyCollection(), Mockito.eq(RangerPolicy.POLICY_TYPE_ACCESS), Mockito.isNull())).thenThrow(new IllegalStateException("evaluation failed"));

        assertThrows(IllegalStateException.class, () -> plugin.isAccessAllowed(Collections.singletonList(request), null));
        assertNull(RangerAccessRequestUtil.getBatchEvalContext(request.getContext()));
    }

    @Test
    void singleAccessDoesNotAttachBatchEvalContext() throws Exception {
        RangerBasePlugin plugin = new RangerBasePlugin("hbase", "hbase");
        RangerPolicyEngine policyEngine = Mockito.mock(RangerPolicyEngine.class);
        setPolicyEngine(plugin, policyEngine);

        RangerAccessRequestImpl request = new RangerAccessRequestImpl();

        plugin.isAccessAllowed(request, null);

        assertNull(RangerAccessRequestUtil.getBatchEvalContext(request.getContext()));
    }

    @Test
    void bulkEvaluationMatchesSingleRequestEvaluation() throws Exception {
        RangerBasePlugin plugin = newPlugin();
        plugin.setPolicies(servicePolicies());
        plugin.setRoles(roles());

        List<RangerAccessRequest> bulkRequests = new ArrayList<>(Arrays.asList(
                request("alice"),
                request("alice"),
                request("bob"),
                request("carol"),
                request("carol"),
                request("dave"),
                request(null)));
        List<RangerAccessRequest> singleRequests = new ArrayList<>(Arrays.asList(
                request("alice"),
                request("alice"),
                request("bob"),
                request("carol"),
                request("carol"),
                request("dave"),
                request(null)));

        Collection<RangerAccessResult> bulkResults = plugin.isAccessAllowed(bulkRequests, null);
        List<Boolean> bulkAllowed = new ArrayList<>();

        for (RangerAccessResult result : bulkResults) {
            bulkAllowed.add(result.getIsAllowed());
        }

        List<Boolean> singleAllowed = new ArrayList<>();

        for (RangerAccessRequest singleRequest : singleRequests) {
            singleAllowed.add(plugin.isAccessAllowed(singleRequest).getIsAllowed());
        }

        assertEquals(Arrays.asList(true, true, false, true, true, false, false), singleAllowed);
        assertEquals(singleAllowed, bulkAllowed);
        assertSame(bulkRequests.get(3).getUserRoles(), bulkRequests.get(4).getUserRoles());
        assertTrue(bulkRequests.get(3).getUserRoles().contains("readers"));
        assertFalse(bulkRequests.get(0).getUserRoles().contains("readers"));

        for (RangerAccessRequest bulkRequest : bulkRequests) {
            assertNull(RangerAccessRequestUtil.getBatchEvalContext(bulkRequest.getContext()));
        }
    }

    private static void setPolicyEngine(RangerBasePlugin plugin, RangerPolicyEngine policyEngine) throws Exception {
        Field field = RangerBasePlugin.class.getDeclaredField("policyEngine");

        field.setAccessible(true);
        field.set(plugin, policyEngine);
    }

    private static RangerBasePlugin newPlugin() {
        RangerPolicyEngineOptions options = new RangerPolicyEngineOptions();
        options.disablePolicyRefresher = true;
        options.disableTagRetriever = true;
        options.disableUserStoreRetriever = true;
        options.disableGdsInfoRetriever = true;

        return new RangerBasePlugin(new RangerPluginConfig("hive", "svc", "app", null, null, options));
    }

    private static ServicePolicies servicePolicies() throws Exception {
        RangerPolicy.RangerPolicyItem allowAlice = policyItem(Collections.singletonList("alice"), Collections.emptyList());
        RangerPolicy.RangerPolicyItem allowReaders = policyItem(Collections.emptyList(), Collections.singletonList("readers"));
        RangerPolicy.RangerPolicyItem denyBob = policyItem(Collections.singletonList("bob"), Collections.emptyList());

        RangerPolicy policy = new RangerPolicy();
        policy.setId(1L);
        policy.setName("db1-select");
        policy.setService("svc");
        policy.setIsEnabled(true);
        policy.setResources(resources());
        policy.setPolicyItems(Arrays.asList(allowAlice, allowReaders));
        policy.setDenyPolicyItems(Collections.singletonList(denyBob));

        ServicePolicies policies = new ServicePolicies();
        policies.setServiceName("svc");
        policies.setPolicyVersion(1L);
        policies.setServiceDef(EmbeddedServiceDefsUtil.instance().getEmbeddedServiceDef(EmbeddedServiceDefsUtil.EMBEDDED_SERVICEDEF_HIVE_NAME));
        policies.setPolicies(Collections.singletonList(policy));

        return policies;
    }

    private static Map<String, RangerPolicy.RangerPolicyResource> resources() {
        Map<String, RangerPolicy.RangerPolicyResource> resources = new HashMap<>();
        resources.put("database", new RangerPolicy.RangerPolicyResource("db1"));
        resources.put("table", new RangerPolicy.RangerPolicyResource("tbl1"));
        resources.put("column", new RangerPolicy.RangerPolicyResource("col1"));

        return resources;
    }

    private static RangerPolicy.RangerPolicyItem policyItem(List<String> users, List<String> roles) {
        RangerPolicy.RangerPolicyItemAccess access = new RangerPolicy.RangerPolicyItemAccess();
        access.setType("select");
        access.setIsAllowed(true);

        RangerPolicy.RangerPolicyItem item = new RangerPolicy.RangerPolicyItem();
        item.setUsers(users);
        item.setRoles(roles);
        item.setAccesses(Collections.singletonList(access));

        return item;
    }

    private static RangerRoles roles() {
        RangerRole readers = new RangerRole();
        readers.setName("readers");
        readers.setUsers(Collections.singletonList(new RangerRole.RoleMember("carol", false)));

        RangerRoles roles = new RangerRoles();
        roles.setRangerRoles(Collections.singleton(readers));

        return roles;
    }

    private static RangerAccessRequestImpl request(String user) {
        RangerAccessResourceImpl resource = new RangerAccessResourceImpl();
        resource.setValue("database", "db1");
        resource.setValue("table", "tbl1");
        resource.setValue("column", "col1");

        RangerAccessRequestImpl request = new RangerAccessRequestImpl();
        request.setResource(resource);
        request.setAccessType("select");
        request.setUser(user);
        request.setUserGroups(new HashSet<>(Collections.singleton("public")));

        return request;
    }
}
