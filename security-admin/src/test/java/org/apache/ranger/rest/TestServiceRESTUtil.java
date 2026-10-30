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

package org.apache.ranger.rest;

import org.apache.ranger.biz.ServiceDBStore;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItem;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItemAccess;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyResource;
import org.apache.ranger.plugin.util.GrantRevokeRequest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@TestMethodOrder(MethodOrderer.MethodName.class)
public class TestServiceRESTUtil {
    @Mock
    private ServiceDBStore mockServiceDBStore;

    private RangerPolicy policy;
    private GrantRevokeRequest grantRequest;
    private GrantRevokeRequest revokeRequest;

    @BeforeEach
    public void setUp() {
        policy = createTestPolicy();
        grantRequest = createTestGrantRequest();
        revokeRequest = createTestRevokeRequest();
    }

    @Test
    public void testProcessGrantRequest_Success() {
        boolean result = ServiceRESTUtil.processGrantRequest(policy, grantRequest);
        assertTrue(result);
        assertNotNull(policy.getPolicyItems());
        assertFalse(policy.getPolicyItems().isEmpty());
    }

    @Test
    public void testProcessGrantRequest_NullPolicy() {
        assertThrows(NullPointerException.class, () -> ServiceRESTUtil.processGrantRequest(null, grantRequest));
    }

    @Test
    public void testProcessGrantRequest_NullGrantRequest() {
        assertThrows(NullPointerException.class, () -> ServiceRESTUtil.processGrantRequest(policy, null));
    }

    @Test
    public void testProcessRevokeRequest_Success() {
        boolean result = ServiceRESTUtil.processRevokeRequest(policy, revokeRequest);
        assertTrue(result);
    }

    @Test
    public void testProcessRevokeRequest_NullPolicy() {
        assertThrows(NullPointerException.class, () -> ServiceRESTUtil.processRevokeRequest(null, revokeRequest));
    }

    @Test
    public void testProcessRevokeRequest_NullRevokeRequest() {
        assertThrows(NullPointerException.class, () -> ServiceRESTUtil.processRevokeRequest(policy, null));
    }

    @Test
    public void testProcessApplyPolicy_Success() {
        RangerPolicy existingPolicy = createTestPolicy();
        RangerPolicy appliedPolicy = createTestPolicy();

        ServiceRESTUtil.processApplyPolicy(existingPolicy, appliedPolicy);

        assertNotNull(existingPolicy.getPolicyItems());
    }

    @Test
    public void testMergeExactMatchPolicyForResource_Success() {
        RangerPolicy existingPolicy = createTestPolicy();
        RangerPolicy appliedPolicy = createTestPolicy();

        ServiceRESTUtil.mergeExactMatchPolicyForResource(existingPolicy, appliedPolicy);

        assertNotNull(existingPolicy.getPolicyItems());
    }

    @Test
    public void testContainsRangerCondition_True() {
        RangerPolicy policyWithCondition = createTestPolicy();
        policyWithCondition.setConditions(Arrays.asList(new RangerPolicy.RangerPolicyItemCondition()));

        boolean result = ServiceRESTUtil.containsRangerCondition(policyWithCondition);
        assertTrue(result);
    }

    @Test
    public void testContainsRangerCondition_False() {
        boolean result = ServiceRESTUtil.containsRangerCondition(policy);
        assertFalse(result);
    }

    @Test
    public void testContainsRangerCondition_NullPolicy() {
        boolean result = ServiceRESTUtil.containsRangerCondition(null);
        assertFalse(result);
    }

    private RangerPolicy createTestPolicy() {
        RangerPolicy policy = new RangerPolicy();
        policy.setId(1L);
        policy.setName("test-policy");
        policy.setService("test-service");

        Map<String, RangerPolicyResource> resources = new HashMap<>();
        resources.put("database", new RangerPolicyResource("test_db"));
        resources.put("table", new RangerPolicyResource("test_table"));
        resources.put("column", new RangerPolicyResource("test_column"));
        policy.setResources(resources);

        List<RangerPolicyItem> policyItems = new ArrayList<>();
        RangerPolicyItem item = new RangerPolicyItem();
        item.setUsers(new ArrayList<>(Arrays.asList("test-user")));
        item.setGroups(new ArrayList<>(Arrays.asList("test-group")));
        item.setRoles(new ArrayList<>(Arrays.asList("test-role")));

        List<RangerPolicyItemAccess> accesses = new ArrayList<>();
        accesses.add(new RangerPolicyItemAccess("read", true));
        accesses.add(new RangerPolicyItemAccess("write", false));
        item.setAccesses(accesses);

        policyItems.add(item);
        policy.setPolicyItems(policyItems);

        return policy;
    }

    private GrantRevokeRequest createTestGrantRequest() {
        GrantRevokeRequest request = new GrantRevokeRequest();
        request.setGrantor("test-grantor");
        request.setUsers(Collections.singleton("test-user"));
        request.setGroups(Collections.singleton("test-group"));
        request.setRoles(Collections.singleton("test-role"));
        request.setAccessTypes(new HashSet<>(Arrays.asList("read", "write")));
        request.setEnableAudit(true);
        request.setReplaceExistingPermissions(false);

        Map<String, String> resources = new HashMap<>();
        resources.put("database", "test_db");
        resources.put("table", "test_table");
        resources.put("column", "test_column");
        request.setResource(resources);

        return request;
    }

    private GrantRevokeRequest createTestRevokeRequest() {
        GrantRevokeRequest request = new GrantRevokeRequest();
        request.setGrantor("test-grantor");
        request.setUsers(Collections.singleton("test-user"));
        request.setGroups(Collections.singleton("test-group"));
        request.setRoles(Collections.singleton("test-role"));
        request.setAccessTypes(new HashSet<>(Arrays.asList("read", "write")));
        request.setEnableAudit(true);
        request.setReplaceExistingPermissions(false);

        Map<String, String> resources = new HashMap<>();
        resources.put("database", "test_db");
        resources.put("table", "test_table");
        resources.put("column", "test_column");
        request.setResource(resources);

        return request;
    }

    @Test
    public void testIsXPolicyUniqueConstraintViolation_NullThrowable() {
        assertFalse(ServiceRESTUtil.isXPolicyUniqueConstraintViolation(null));
        assertFalse(ServiceRESTUtil.isXPolicyServiceSignatureUniqueConstraintViolation(null));
        assertFalse(ServiceRESTUtil.isXPolicyNameServiceZoneUniqueConstraintViolation(null));
    }

    @Test
    public void testIsXPolicyUniqueConstraintViolation_ServiceSignature() {
        Throwable excp = new RuntimeException("Duplicate entry for key 'x_policy.x_policy_UK_service_signature'");

        assertTrue(ServiceRESTUtil.isXPolicyUniqueConstraintViolation(excp));
        assertTrue(ServiceRESTUtil.isXPolicyServiceSignatureUniqueConstraintViolation(excp));
        assertFalse(ServiceRESTUtil.isXPolicyNameServiceZoneUniqueConstraintViolation(excp));
    }

    @Test
    public void testIsXPolicyUniqueConstraintViolation_ServiceSignaturePostgresMessage() {
        Throwable excp = new RuntimeException("duplicate key value violates unique constraint \"x_policy_UK_service_signature\"");

        assertTrue(ServiceRESTUtil.isXPolicyUniqueConstraintViolation(excp));
        assertTrue(ServiceRESTUtil.isXPolicyServiceSignatureUniqueConstraintViolation(excp));
        assertFalse(ServiceRESTUtil.isXPolicyNameServiceZoneUniqueConstraintViolation(excp));
    }

    @Test
    public void testIsXPolicyUniqueConstraintViolation_NameServiceZoneInCause() {
        Throwable cause = new RuntimeException("Duplicate entry for key 'x_policy.x_policy_UK_name_service_zone'");
        Throwable excp  = new RuntimeException("outer exception", cause);

        assertTrue(ServiceRESTUtil.isXPolicyUniqueConstraintViolation(excp));
        assertFalse(ServiceRESTUtil.isXPolicyServiceSignatureUniqueConstraintViolation(excp));
        assertTrue(ServiceRESTUtil.isXPolicyNameServiceZoneUniqueConstraintViolation(excp));
    }

    @Test
    public void testIsXPolicyUniqueConstraintViolation_NameServiceZoneOracleMessage() {
        Throwable excp = new RuntimeException("ORA-00001: unique constraint (RANGER.X_POLICY_UK_NAME_SERVICE_ZONE) violated");

        assertTrue(ServiceRESTUtil.isXPolicyUniqueConstraintViolation(excp));
        assertFalse(ServiceRESTUtil.isXPolicyServiceSignatureUniqueConstraintViolation(excp));
        assertTrue(ServiceRESTUtil.isXPolicyNameServiceZoneUniqueConstraintViolation(excp));
    }

    @Test
    public void testIsXPolicyUniqueConstraintViolation_FalseForOtherErrors() {
        Throwable excp = new RuntimeException("some other SQL failure");

        assertFalse(ServiceRESTUtil.isXPolicyUniqueConstraintViolation(excp));
        assertFalse(ServiceRESTUtil.isXPolicyServiceSignatureUniqueConstraintViolation(excp));
        assertFalse(ServiceRESTUtil.isXPolicyNameServiceZoneUniqueConstraintViolation(excp));
    }

    @Test
    public void testIsOptimisticLockException_TrueFromCauseChain() {
        Throwable cause = new RuntimeException("org.eclipse.persistence.exceptions.OptimisticLockException");
        Throwable excp  = new RuntimeException("outer", cause);

        assertTrue(ServiceRESTUtil.isOptimisticLockException(excp));
    }

    @Test
    public void testIsOptimisticLockException_FalseForOtherErrors() {
        Throwable excp = new RuntimeException("some other exception");

        assertFalse(ServiceRESTUtil.isOptimisticLockException(excp));
    }

    @Test
    public void testPreserveExistingPolicyGuidForGrantUpdate_FillsMissingGuidFromExistingPolicy() throws Exception {
        RangerPolicy policy         = new RangerPolicy();
        RangerPolicy existingPolicy = new RangerPolicy();

        policy.setId(10L);
        policy.setGuid(null);
        existingPolicy.setGuid("existing-guid");

        when(mockServiceDBStore.getPolicy(10L)).thenReturn(existingPolicy);
        assertDoesNotThrow(() -> ServiceRESTUtil.preserveExistingPolicyGuidForGrantUpdate(mockServiceDBStore, policy));

        assertEquals("existing-guid", policy.getGuid());
        verify(mockServiceDBStore).getPolicy(10L);
    }

    @Test
    public void testPreserveExistingPolicyGuidForGrantUpdate_SkipsWhenGuidAlreadySet() throws Exception {
        RangerPolicy policy = new RangerPolicy();

        policy.setId(10L);
        policy.setGuid("already-set");

        assertDoesNotThrow(() -> ServiceRESTUtil.preserveExistingPolicyGuidForGrantUpdate(mockServiceDBStore, policy));

        verify(mockServiceDBStore, never()).getPolicy(anyLong());
    }
}
