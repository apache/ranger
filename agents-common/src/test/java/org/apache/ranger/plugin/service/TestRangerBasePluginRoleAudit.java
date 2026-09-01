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

import java.lang.reflect.Field;

import org.apache.ranger.admin.client.RangerAdminClient;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.policyengine.RangerAccessRequest;
import org.apache.ranger.plugin.policyengine.RangerAccessResult;
import org.apache.ranger.plugin.policyengine.RangerAccessResultProcessor;
import org.apache.ranger.plugin.util.GrantRevokeRoleRequest;
import org.apache.ranger.plugin.util.PolicyRefresher;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestRangerBasePluginRoleAudit {
    private RangerBasePlugin            plugin;
    private RangerAdminClient            adminClient;
    private RangerAccessResultProcessor resultProcessor;

    @BeforeEach
    public void setUp() throws Exception {
        plugin          = Mockito.spy(new RangerBasePlugin("hive", "test-hive", "test-app"));
        adminClient     = mock(RangerAdminClient.class);
        resultProcessor = mock(RangerAccessResultProcessor.class);

        PolicyRefresher refresher = mock(PolicyRefresher.class);

        when(refresher.getRangerAdminClient()).thenReturn(adminClient);

        Field refresherField = RangerBasePlugin.class.getDeclaredField("refresher");

        refresherField.setAccessible(true);
        refresherField.set(plugin, refresher);

        doAnswer(invocation -> {
            RangerAccessRequest accessRequest = invocation.getArgument(0);
            RangerAccessResult  ret           = new RangerAccessResult(RangerPolicy.POLICY_TYPE_ACCESS, plugin.getServiceName(), plugin.getServiceDef(), accessRequest);

            ret.setIsAudited(true);

            return ret;
        }).when(plugin).isAccessAllowed(any(RangerAccessRequest.class), isNull());
    }

    @Test
    public void testGrantRoleProducesAuditResult() throws Exception {
        GrantRevokeRoleRequest request = createRequest();

        plugin.grantRole(request, resultProcessor);

        verify(adminClient).grantRole(request);

        RangerAccessResult result = getAuditResult();

        assertTrue(result.getIsAllowed());
        assertEquals("GRANT_ROLE", result.getAccessRequest().getAction());
        assertEquals("alter", result.getAccessRequest().getAccessType());
        assertEquals("*", result.getAccessRequest().getResource().getValue("global"));
    }

    @Test
    public void testRevokeRoleFailureProducesDeniedAuditResult() throws Exception {
        GrantRevokeRoleRequest request = createRequest();

        doThrow(new Exception("revoke failed")).when(adminClient).revokeRole(request);

        assertThrows(Exception.class, () -> plugin.revokeRole(request, resultProcessor));

        RangerAccessResult result = getAuditResult();

        assertFalse(result.getIsAllowed());
        assertEquals(-1, result.getPolicyId());
        assertEquals("REVOKE_ROLE", result.getAccessRequest().getAction());
    }

    private GrantRevokeRoleRequest createRequest() {
        GrantRevokeRoleRequest ret = new GrantRevokeRoleRequest();

        ret.setGrantor("admin");
        ret.setClientIPAddress("192.0.2.10");
        ret.setClientType("test-client");
        ret.setRequestData("grant role test_role to user test_user");
        ret.setSessionId("test-session");

        return ret;
    }

    private RangerAccessResult getAuditResult() {
        ArgumentCaptor<RangerAccessResult> captor = ArgumentCaptor.forClass(RangerAccessResult.class);

        verify(resultProcessor).processResult(captor.capture());

        return captor.getValue();
    }
}
