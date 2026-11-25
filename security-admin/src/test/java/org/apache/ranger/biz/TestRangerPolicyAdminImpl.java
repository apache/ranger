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

package org.apache.ranger.biz;

import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItem;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItemAccess;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyResource;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.model.RangerServiceDef.RangerAccessTypeDef;
import org.apache.ranger.plugin.policyengine.PolicyEngine;
import org.apache.ranger.plugin.policyengine.RangerPluginContext;
import org.apache.ranger.plugin.policyengine.RangerPolicyEngine;
import org.apache.ranger.plugin.policyengine.RangerPolicyRepository;
import org.apache.ranger.plugin.policyevaluator.RangerPolicyEvaluator;
import org.apache.ranger.plugin.service.RangerAuthContext;
import org.apache.ranger.plugin.util.RangerReadWriteLock.RangerLock;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class TestRangerPolicyAdminImpl {
    @Mock
    PolicyEngine policyEngine;
    @Mock
    RangerPolicyRepository policyRepository;
    @Mock
    RangerPluginContext pluginContext;
    @Mock
    RangerAuthContext authContext;
    @Mock
    ServiceDBStore serviceDBStore;

    @Test
    public void testIsDelegatedAdminAccessAllowedForModify_realDeltaStillCheckedOnlyForChangedAccessTypes() throws Exception {
        RangerPolicyAdminImpl admin = createAdminWithDefaultStubs();
        when(policyEngine.getServiceDef()).thenReturn(serviceDefWithAccessTypes("select", "drop"));

        RangerPolicy storedP = policyWithDenyItem(22L, "victim", "select");
        when(serviceDBStore.getPolicy(22L)).thenReturn(storedP);
        stubDelegatedAdminAccesses("drop");

        RangerPolicy updated = policyWithDenyItem(22L, "victim", "select");
        RangerPolicyItem added = new RangerPolicyItem();
        added.addUser("bob");
        added.addAccess(new RangerPolicyItemAccess("drop"));
        updated.setPolicyItems(Collections.singletonList(added));

        assertTrue(admin.isDelegatedAdminAccessAllowedForModify(updated, "alice", Collections.emptySet(), null, null));
    }

    @Test
    public void testCollectAccessTypes_principalsDoNotShareAccessTypeSet() throws Exception {
        RangerPolicyAdminImpl admin = createAdminWithDefaultStubs();
        Map<String, Collection<String>> expanded = new HashMap<>();
        expanded.put("read", Collections.singletonList("read"));
        expanded.put("write", Collections.singletonList("write"));

        RangerPolicyItem item1 = new RangerPolicyItem();
        item1.addUser("u1");
        item1.addUser("u2");
        item1.addGroup("g1");
        item1.addAccess(new RangerPolicyItemAccess("read"));

        RangerPolicyItem item2 = new RangerPolicyItem();
        item2.addUser("u1");
        item2.addAccess(new RangerPolicyItemAccess("write"));

        Map<List<Object>, Set<String>> userAcc = new HashMap<>();
        Map<List<Object>, Set<String>> grpAcc = new HashMap<>();
        Map<List<Object>, Set<String>> roleAcc = new HashMap<>();

        Method m = RangerPolicyAdminImpl.class.getDeclaredMethod("collectAccessTypes", Map.class, List.class, Map.class, Map.class, Map.class);
        m.setAccessible(true);
        m.invoke(admin, expanded, Arrays.asList(item1, item2), userAcc, grpAcc, roleAcc);

        assertEquals(new HashSet<>(Arrays.asList("read", "write")), accessesOf(userAcc, "u1"));
        assertEquals(Collections.singleton("read"), accessesOf(userAcc, "u2"));
        assertEquals(Collections.singleton("read"), accessesOf(grpAcc, "g1"));
        assertNotSame(accessesOf(userAcc, "u1"), accessesOf(userAcc, "u2"));
        assertNotSame(accessesOf(userAcc, "u2"), accessesOf(grpAcc, "g1"));
    }

    @Test
    public void testGetAllModifiedAccessTypes_emptyWhenUnchangedAndDetectsAliasedChange() throws Exception {
        RangerPolicyAdminImpl admin = createAdminWithDefaultStubs();
        RangerServiceDef sd = serviceDefWithAccessTypes("select", "update");

        Method m = RangerPolicyAdminImpl.class.getDeclaredMethod("getAllModifiedAccessTypes", RangerPolicy.class, RangerPolicy.class, RangerServiceDef.class);
        m.setAccessible(true);

        RangerPolicy p1 = policyWithDenyItem(30L, "victim", "select");
        @SuppressWarnings("unchecked")
        Set<String> unchanged = (Set<String>) m.invoke(admin, p1, policyWithDenyItem(30L, "victim", "select"), sd);
        assertEquals(Collections.singleton("select"), unchanged);
        assertFalse(unchanged.contains(RangerPolicyEngine.ADMIN_ACCESS));

        RangerPolicy oldP = new RangerPolicy();
        oldP.setId(31L);
        RangerPolicyItem shared = new RangerPolicyItem();
        shared.addUser("alice");
        shared.addUser("bob");
        shared.addAccess(new RangerPolicyItemAccess("select"));
        RangerPolicyItem aliceOnly = new RangerPolicyItem();
        aliceOnly.addUser("alice");
        aliceOnly.addAccess(new RangerPolicyItemAccess("update"));
        oldP.setPolicyItems(Arrays.asList(shared, aliceOnly));

        RangerPolicy newP = new RangerPolicy();
        newP.setId(31L);
        RangerPolicyItem shared2 = new RangerPolicyItem();
        shared2.addUser("alice");
        shared2.addUser("bob");
        shared2.addAccess(new RangerPolicyItemAccess("select"));
        RangerPolicyItem both = new RangerPolicyItem();
        both.addUser("alice");
        both.addUser("bob");
        both.addAccess(new RangerPolicyItemAccess("update"));
        newP.setPolicyItems(Arrays.asList(shared2, both));

        @SuppressWarnings("unchecked")
        Set<String> delta = (Set<String>) m.invoke(admin, oldP, newP, sd);
        assertEquals(Collections.singleton("update"), delta);
    }

    @Test
    public void testIsDelegatedAdminAccessAllowedForModify_denyToAllowMoveWithOtherChangeRequiresMovedAccessType() throws Exception {
        RangerPolicyAdminImpl admin = createAdminWithDefaultStubs();
        when(policyEngine.getServiceDef()).thenReturn(serviceDefWithAccessTypes("consume", "delete"));

        RangerPolicy storedP = policyWithItems(60L,
                Arrays.asList(groupItem("analysts", "consume"), userItem("dave", "delete")),
                Collections.singletonList(userItem("victim", "consume")));
        when(serviceDBStore.getPolicy(60L)).thenReturn(storedP);
        stubDelegatedAdminAccesses("delete");

        RangerPolicy inScope = policyWithItems(60L,
                Arrays.asList(groupItem("analysts", "consume"), userItem("dave", "delete"), userItem("bob", "delete")),
                Collections.singletonList(userItem("victim", "consume")));
        assertTrue(admin.isDelegatedAdminAccessAllowedForModify(inScope, "alice", Collections.emptySet(), null, null));

        RangerPolicy moveOnly = policyWithItems(60L,
                Arrays.asList(groupItem("analysts", "consume"), userItem("victim", "consume"), userItem("dave", "delete")),
                Collections.emptyList());
        assertFalse(admin.isDelegatedAdminAccessAllowedForModify(moveOnly, "alice", Collections.emptySet(), null, null));

        RangerPolicy combined = policyWithItems(60L,
                Arrays.asList(groupItem("analysts", "consume"), userItem("victim", "consume"), userItem("dave", "delete"), userItem("bob", "delete")),
                Collections.emptyList());
        assertFalse(admin.isDelegatedAdminAccessAllowedForModify(combined, "alice", Collections.emptySet(), null, null));

        stubDelegatedAdminAccesses("delete", "consume");
        assertTrue(admin.isDelegatedAdminAccessAllowedForModify(combined, "alice", Collections.emptySet(), null, null));
    }

    @Test
    public void testIsDelegatedAdminAccessAllowedForModify_disallowedAccessWithOtherChangeRequiresThatAccessType() throws Exception {
        RangerPolicyAdminImpl admin = createAdminWithDefaultStubs();
        when(policyEngine.getServiceDef()).thenReturn(serviceDefWithAccessTypes("consume", "delete"));

        RangerPolicy storedP = policyWithItems(66L,
                Arrays.asList(groupItem("analysts", "consume"), userItem("dave", "delete")),
                Collections.singletonList(userItem("victim", "consume")));
        when(serviceDBStore.getPolicy(66L)).thenReturn(storedP);
        stubDelegatedAdminAccesses("delete");

        RangerPolicyItem disabledDeny = new RangerPolicyItem();
        disabledDeny.addUser("victim");
        disabledDeny.addAccess(new RangerPolicyItemAccess("consume", Boolean.FALSE));

        RangerPolicy updated = policyWithItems(66L,
                Arrays.asList(groupItem("analysts", "consume"), userItem("dave", "delete"), userItem("bob", "delete")),
                Collections.singletonList(disabledDeny));
        assertFalse(admin.isDelegatedAdminAccessAllowedForModify(updated, "alice", Collections.emptySet(), null, null));

        stubDelegatedAdminAccesses("delete", "consume");
        assertTrue(admin.isDelegatedAdminAccessAllowedForModify(updated, "alice", Collections.emptySet(), null, null));
    }

    @Test
    public void testIsDelegatedAdminAccessAllowedForModify_denyToDenyExceptionMoveWithOtherChangeRequiresMovedAccessType() throws Exception {
        RangerPolicyAdminImpl admin = createAdminWithDefaultStubs();
        when(policyEngine.getServiceDef()).thenReturn(serviceDefWithAccessTypes("select", "drop"));

        RangerPolicy storedP = policyWithItems(61L,
                Arrays.asList(groupItem("contractors", "select"), userItem("dave", "drop")),
                Collections.singletonList(userItem("victim", "select")));
        when(serviceDBStore.getPolicy(61L)).thenReturn(storedP);
        stubDelegatedAdminAccesses("drop");

        RangerPolicy updated = policyWithItems(61L,
                Arrays.asList(groupItem("contractors", "select"), userItem("dave", "drop"), userItem("bob", "drop")),
                Collections.emptyList());
        updated.setDenyExceptions(Collections.singletonList(userItem("victim", "select")));

        assertFalse(admin.isDelegatedAdminAccessAllowedForModify(updated, "alice", Collections.emptySet(), null, null));

        stubDelegatedAdminAccesses("drop", "select");
        assertTrue(admin.isDelegatedAdminAccessAllowedForModify(updated, "alice", Collections.emptySet(), null, null));
    }

    @Test
    public void testIsDelegatedAdminAccessAllowedForModify_groupAndRoleMoveWithOtherChangeRequiresMovedAccessType() throws Exception {
        RangerPolicyAdminImpl admin = createAdminWithDefaultStubs();
        when(policyEngine.getServiceDef()).thenReturn(serviceDefWithAccessTypes("consume", "delete"));

        RangerPolicyItem deniedRole = new RangerPolicyItem();
        deniedRole.addRole("interns");
        deniedRole.addAccess(new RangerPolicyItemAccess("consume"));

        RangerPolicy storedP = policyWithItems(62L,
                Collections.singletonList(userItem("dave", "delete")),
                Arrays.asList(groupItem("guests", "consume"), deniedRole));
        when(serviceDBStore.getPolicy(62L)).thenReturn(storedP);
        stubDelegatedAdminAccesses("delete");

        RangerPolicy groupMoved = policyWithItems(62L,
                Arrays.asList(userItem("dave", "delete"), userItem("bob", "delete"), groupItem("guests", "consume")),
                Collections.singletonList(deniedRole));
        assertFalse(admin.isDelegatedAdminAccessAllowedForModify(groupMoved, "alice", Collections.emptySet(), null, null));

        RangerPolicy roleMoved = policyWithItems(62L,
                Arrays.asList(userItem("dave", "delete"), userItem("bob", "delete"), deniedRole),
                Collections.singletonList(groupItem("guests", "consume")));
        assertFalse(admin.isDelegatedAdminAccessAllowedForModify(roleMoved, "alice", Collections.emptySet(), null, null));
    }

    @Test
    public void testIsDelegatedAdminAccessAllowedForModify_itemConditionRemovalWithOtherChangeRequiresAccessType() throws Exception {
        RangerPolicyAdminImpl admin = createAdminWithDefaultStubs();
        when(policyEngine.getServiceDef()).thenReturn(serviceDefWithAccessTypes("consume", "delete"));

        RangerPolicyItem restricted = groupItem("analysts", "consume");
        restricted.setConditions(Collections.singletonList(new RangerPolicy.RangerPolicyItemCondition("ip-range", Collections.singletonList("10.*"))));

        RangerPolicy storedP = policyWithItems(63L, Arrays.asList(restricted, userItem("dave", "delete")), Collections.emptyList());
        when(serviceDBStore.getPolicy(63L)).thenReturn(storedP);
        stubDelegatedAdminAccesses("delete");

        RangerPolicy updated = policyWithItems(63L,
                Arrays.asList(groupItem("analysts", "consume"), userItem("dave", "delete"), userItem("bob", "delete")),
                Collections.emptyList());
        assertFalse(admin.isDelegatedAdminAccessAllowedForModify(updated, "alice", Collections.emptySet(), null, null));

        RangerPolicyItem storedTwoConds = groupItem("analysts", "consume");
        storedTwoConds.setConditions(Arrays.asList(
                new RangerPolicy.RangerPolicyItemCondition("ip-range", Arrays.asList("10.*", "172.16.*")),
                new RangerPolicy.RangerPolicyItemCondition("hour", Collections.singletonList("9-17"))));
        when(serviceDBStore.getPolicy(64L)).thenReturn(policyWithItems(64L, Arrays.asList(storedTwoConds, userItem("dave", "delete")), Collections.emptyList()));

        RangerPolicyItem reordered = groupItem("analysts", "consume");
        reordered.setConditions(Arrays.asList(
                new RangerPolicy.RangerPolicyItemCondition("hour", Collections.singletonList("9-17")),
                new RangerPolicy.RangerPolicyItemCondition("ip-range", Arrays.asList("172.16.*", "10.*"))));
        RangerPolicy keepsConds = policyWithItems(64L, Arrays.asList(reordered, userItem("dave", "delete"), userItem("bob", "delete")), Collections.emptyList());
        assertTrue(admin.isDelegatedAdminAccessAllowedForModify(keepsConds, "alice", Collections.emptySet(), null, null));

        RangerPolicyItem joinedValue = groupItem("analysts", "consume");
        joinedValue.setConditions(Collections.singletonList(new RangerPolicy.RangerPolicyItemCondition("ip-range", Collections.singletonList("10.*, 192.168.7.7"))));
        when(serviceDBStore.getPolicy(67L)).thenReturn(policyWithItems(67L, Arrays.asList(joinedValue, userItem("dave", "delete")), Collections.emptyList()));

        RangerPolicyItem splitValues = groupItem("analysts", "consume");
        splitValues.setConditions(Collections.singletonList(new RangerPolicy.RangerPolicyItemCondition("ip-range", Arrays.asList("10.*", "192.168.7.7"))));
        RangerPolicy split = policyWithItems(67L, Arrays.asList(splitValues, userItem("dave", "delete"), userItem("bob", "delete")), Collections.emptyList());
        assertFalse(admin.isDelegatedAdminAccessAllowedForModify(split, "alice", Collections.emptySet(), null, null));
    }

    @Test
    public void testIsDelegatedAdminAccessAllowedForModify_auditDisableWithOtherChangeRequiresAllAccessTypes() throws Exception {
        RangerPolicyAdminImpl admin = createAdminWithDefaultStubs();
        when(policyEngine.getServiceDef()).thenReturn(serviceDefWithAccessTypes("select", "drop"));

        RangerPolicy storedP = policyWithDenyItem(65L, "victim", "select");
        when(serviceDBStore.getPolicy(65L)).thenReturn(storedP);
        stubDelegatedAdminAccesses("drop");

        RangerPolicy updated = policyWithDenyItem(65L, "victim", "select");
        updated.setPolicyItems(Collections.singletonList(userItem("bob", "drop")));
        assertTrue(admin.isDelegatedAdminAccessAllowedForModify(updated, "alice", Collections.emptySet(), null, null));

        updated.setIsAuditEnabled(false);
        assertFalse(admin.isDelegatedAdminAccessAllowedForModify(updated, "alice", Collections.emptySet(), null, null));
    }

    @Test
    public void testGetAllModifiedAccessTypes_detectsItemTypeConditionAndItemInfoChanges() throws Exception {
        RangerPolicyAdminImpl admin = createAdminWithDefaultStubs();
        RangerServiceDef sd = serviceDefWithAccessTypes("select", "read");

        Method m = RangerPolicyAdminImpl.class.getDeclaredMethod("getAllModifiedAccessTypes", RangerPolicy.class, RangerPolicy.class, RangerServiceDef.class);
        m.setAccessible(true);

        RangerPolicy oldP = policyWithItems(70L, Collections.singletonList(userItem("dave", "read")), Collections.singletonList(userItem("victim", "select")));
        RangerPolicy newP = policyWithItems(70L, Arrays.asList(userItem("dave", "read"), userItem("bob", "read"), userItem("victim", "select")), Collections.emptyList());
        @SuppressWarnings("unchecked")
        Set<String> moved = (Set<String>) m.invoke(admin, oldP, newP, sd);
        assertEquals(new HashSet<>(Arrays.asList("select", "read")), moved);

        RangerPolicy oldMask = dataMaskPolicy(71L, "MASK");
        RangerPolicy newMask = dataMaskPolicy(71L, "MASK_NONE");
        RangerPolicy.RangerDataMaskPolicyItem bob = new RangerPolicy.RangerDataMaskPolicyItem();
        bob.addUser("bob");
        bob.addAccess(new RangerPolicyItemAccess("read"));
        bob.setDataMaskInfo(new RangerPolicy.RangerPolicyItemDataMaskInfo("MASK", null, null));
        List<RangerPolicy.RangerDataMaskPolicyItem> newMaskItems = new ArrayList<>(newMask.getDataMaskPolicyItems());
        newMaskItems.add(bob);
        newMask.setDataMaskPolicyItems(newMaskItems);
        @SuppressWarnings("unchecked")
        Set<String> masked = (Set<String>) m.invoke(admin, oldMask, newMask, sd);
        assertEquals(new HashSet<>(Arrays.asList("select", "read")), masked);
    }

    @Test
    public void testGetAllModifiedAccessTypes_conditionValuesAreComparedAsList() throws Exception {
        RangerPolicy oldP = policyWithItems(90L, Collections.singletonList(userItem("dave", "read")),
                Collections.singletonList(denyItemWithConditions(new RangerPolicy.RangerPolicyItemCondition("ip-range", Arrays.asList("10.*", "192.168.*")))));
        RangerPolicy newP = policyWithItems(90L, Arrays.asList(userItem("dave", "read"), userItem("bob", "read")),
                Collections.singletonList(denyItemWithConditions(new RangerPolicy.RangerPolicyItemCondition("ip-range", Collections.singletonList("10.*\u0000192.168.*")))));

        assertEquals(new HashSet<>(Arrays.asList("select", "read")), getModifiedAccessTypes(oldP, newP));
    }

    @Test
    public void testGetAllModifiedAccessTypes_conditionsAreComparedAsList() throws Exception {
        RangerPolicy oldP = policyWithItems(91L, Collections.singletonList(userItem("dave", "read")),
                Collections.singletonList(denyItemWithConditions(
                        new RangerPolicy.RangerPolicyItemCondition("ip-range", Collections.singletonList("10.*")),
                        new RangerPolicy.RangerPolicyItemCondition("hour", Collections.singletonList("9-17")))));
        RangerPolicy newP = policyWithItems(91L, Arrays.asList(userItem("dave", "read"), userItem("bob", "read")),
                Collections.singletonList(denyItemWithConditions(
                        new RangerPolicy.RangerPolicyItemCondition("hour", Collections.singletonList("9-17\u0001ip-range\u000010.*")))));

        assertEquals(new HashSet<>(Arrays.asList("select", "read")), getModifiedAccessTypes(oldP, newP));
    }

    @Test
    public void testGetAllModifiedAccessTypes_nullConditionValueIsHandled() throws Exception {
        RangerPolicy oldP = policyWithItems(92L, Collections.singletonList(userItem("dave", "read")),
                Collections.singletonList(denyItemWithConditions(new RangerPolicy.RangerPolicyItemCondition("ip-range", Arrays.asList(null, "10.*")))));
        RangerPolicy newP = policyWithItems(92L, Arrays.asList(userItem("dave", "read"), userItem("bob", "read")),
                Collections.singletonList(denyItemWithConditions(new RangerPolicy.RangerPolicyItemCondition("ip-range", Arrays.asList("10.*", null)))));

        assertEquals(Collections.singleton("read"), getModifiedAccessTypes(oldP, newP));
    }

    @Test
    public void testGetAllModifiedAccessTypes_dataMaskInfoIsComparedByField() throws Exception {
        RangerPolicy oldP = maskPolicy(93L, new RangerPolicy.RangerPolicyItemDataMaskInfo("CUSTOM", "a} valueExpr={b", "c"), false);
        RangerPolicy newP = maskPolicy(93L, new RangerPolicy.RangerPolicyItemDataMaskInfo("CUSTOM", "a", "b} valueExpr={c"), true);

        assertEquals(new HashSet<>(Arrays.asList("select", "read")), getModifiedAccessTypes(oldP, newP));

        RangerPolicy sameP = maskPolicy(93L, new RangerPolicy.RangerPolicyItemDataMaskInfo("CUSTOM", "a} valueExpr={b", "c"), true);

        assertEquals(Collections.singleton("read"), getModifiedAccessTypes(oldP, sameP));
    }

    @Test
    public void testGetAllModifiedAccessTypes_rowFilterInfoIsComparedByField() throws Exception {
        RangerPolicy oldP = rowFilterPolicy(94L, "dept = 'hr'", false);

        assertEquals(new HashSet<>(Arrays.asList("select", "read")), getModifiedAccessTypes(oldP, rowFilterPolicy(94L, "1 = 1", true)));
        assertEquals(Collections.singleton("read"), getModifiedAccessTypes(oldP, rowFilterPolicy(94L, "dept = 'hr'", true)));
    }

    @Test
    public void testGetAllModifiedAccessTypes_unknownAccessTypeIsKeptInDelta() throws Exception {
        RangerPolicy oldP = policyWithItems(95L, Collections.singletonList(userItem("dave", "read")), Collections.emptyList());
        RangerPolicy newP = policyWithItems(95L, Arrays.asList(userItem("dave", "read"), userItem("bob", "unknown")), Collections.emptyList());

        assertEquals(Collections.singleton("unknown"), getModifiedAccessTypes(oldP, newP));
    }

    private static Set<String> accessesOf(Map<List<Object>, Set<String>> accesses, String principal) {
        Set<String> ret = null;

        for (Map.Entry<List<Object>, Set<String>> entry : accesses.entrySet()) {
            if (principal.equals(entry.getKey().get(0))) {
                assertNull(ret, "more than one key for principal " + principal);

                ret = entry.getValue();
            }
        }

        return ret;
    }

    private RangerPolicyAdminImpl createAdminWithDefaultStubs() throws Exception {
        RangerServiceDef sd = new RangerServiceDef();
        sd.setName("svc-def");
        sd.setResources(new ArrayList<>());
        sd.setAccessTypes(new ArrayList<>());

        when(policyEngine.getServiceName()).thenReturn("svc");
        when(policyEngine.getServiceDef()).thenReturn(sd);
        when(policyEngine.getPluginContext()).thenReturn(pluginContext);
        when(pluginContext.getAuthContext()).thenReturn(authContext);
        when(policyEngine.getPolicyRepository()).thenReturn(policyRepository);
        when(policyEngine.getRepositoryForZone(any())).thenAnswer(inv -> policyRepository);
        when(policyEngine.getRepositoryForMatchedZone(any(RangerPolicy.class))).thenReturn(policyRepository);
        when(policyEngine.hasResourcePolicies(any())).thenReturn(true);
        when(policyEngine.hasTagPolicies(any())).thenReturn(false);

        RangerLock noOpLock = mock(RangerLock.class);
        when(noOpLock.isLockingEnabled()).thenReturn(false);
        when(policyEngine.getReadLock()).thenReturn(noOpLock);
        when(policyEngine.getWriteLock()).thenReturn(noOpLock);

        Constructor<RangerPolicyAdminImpl> ctor = RangerPolicyAdminImpl.class.getDeclaredConstructor(PolicyEngine.class);
        ctor.setAccessible(true);
        RangerPolicyAdminImpl newAdmin = ctor.newInstance(policyEngine);

        Field f = RangerPolicyAdminImpl.class.getDeclaredField("serviceDBStore");
        f.setAccessible(true);
        f.set(newAdmin, serviceDBStore);
        return newAdmin;
    }

    private Set<String> getModifiedAccessTypes(RangerPolicy oldP, RangerPolicy newP) throws Exception {
        RangerPolicyAdminImpl admin = createAdminWithDefaultStubs();
        Method m = RangerPolicyAdminImpl.class.getDeclaredMethod("getAllModifiedAccessTypes", RangerPolicy.class, RangerPolicy.class, RangerServiceDef.class);
        m.setAccessible(true);
        @SuppressWarnings("unchecked")
        Set<String> ret = (Set<String>) m.invoke(admin, oldP, newP, serviceDefWithAccessTypes("select", "read"));
        return ret;
    }

    private RangerPolicyItem denyItemWithConditions(RangerPolicy.RangerPolicyItemCondition... conditions) {
        RangerPolicyItem item = userItem("u1", "select");
        item.setConditions(new ArrayList<>(Arrays.asList(conditions)));
        return item;
    }

    private RangerPolicy dataMaskPolicy(long id, String u1MaskType) {
        RangerPolicy p = new RangerPolicy();
        p.setId(id);
        p.setPolicyType(RangerPolicy.POLICY_TYPE_DATAMASK);
        p.setResources(Collections.singletonMap("db", new RangerPolicyResource("hr")));

        RangerPolicy.RangerDataMaskPolicyItem u1 = new RangerPolicy.RangerDataMaskPolicyItem();
        u1.addUser("u1");
        u1.addAccess(new RangerPolicyItemAccess("select"));
        u1.setDataMaskInfo(new RangerPolicy.RangerPolicyItemDataMaskInfo(u1MaskType, null, null));

        RangerPolicy.RangerDataMaskPolicyItem dave = new RangerPolicy.RangerDataMaskPolicyItem();
        dave.addUser("dave");
        dave.addAccess(new RangerPolicyItemAccess("read"));
        dave.setDataMaskInfo(new RangerPolicy.RangerPolicyItemDataMaskInfo("MASK", null, null));

        p.setDataMaskPolicyItems(Arrays.asList(u1, dave));
        return p;
    }

    private RangerPolicy maskPolicy(long id, RangerPolicy.RangerPolicyItemDataMaskInfo u1MaskInfo, boolean withBobRead) {
        RangerPolicy p = new RangerPolicy();
        p.setId(id);
        p.setPolicyType(RangerPolicy.POLICY_TYPE_DATAMASK);
        p.setResources(Collections.singletonMap("db", new RangerPolicyResource("hr")));

        RangerPolicy.RangerDataMaskPolicyItem u1 = new RangerPolicy.RangerDataMaskPolicyItem();
        u1.addUser("u1");
        u1.addAccess(new RangerPolicyItemAccess("select"));
        u1.setDataMaskInfo(u1MaskInfo);

        List<RangerPolicy.RangerDataMaskPolicyItem> items = new ArrayList<>(Collections.singletonList(u1));

        if (withBobRead) {
            RangerPolicy.RangerDataMaskPolicyItem bob = new RangerPolicy.RangerDataMaskPolicyItem();
            bob.addUser("bob");
            bob.addAccess(new RangerPolicyItemAccess("read"));
            bob.setDataMaskInfo(new RangerPolicy.RangerPolicyItemDataMaskInfo("MASK", null, null));
            items.add(bob);
        }

        p.setDataMaskPolicyItems(items);
        return p;
    }

    private RangerPolicy rowFilterPolicy(long id, String u1FilterExpr, boolean withBobRead) {
        RangerPolicy p = new RangerPolicy();
        p.setId(id);
        p.setPolicyType(RangerPolicy.POLICY_TYPE_ROWFILTER);
        p.setResources(Collections.singletonMap("db", new RangerPolicyResource("hr")));

        RangerPolicy.RangerRowFilterPolicyItem u1 = new RangerPolicy.RangerRowFilterPolicyItem();
        u1.addUser("u1");
        u1.addAccess(new RangerPolicyItemAccess("select"));
        u1.setRowFilterInfo(new RangerPolicy.RangerPolicyItemRowFilterInfo(u1FilterExpr));

        List<RangerPolicy.RangerRowFilterPolicyItem> items = new ArrayList<>(Collections.singletonList(u1));

        if (withBobRead) {
            RangerPolicy.RangerRowFilterPolicyItem bob = new RangerPolicy.RangerRowFilterPolicyItem();
            bob.addUser("bob");
            bob.addAccess(new RangerPolicyItemAccess("read"));
            bob.setRowFilterInfo(new RangerPolicy.RangerPolicyItemRowFilterInfo("1 = 0"));
            items.add(bob);
        }

        p.setRowFilterPolicyItems(items);
        return p;
    }

    private RangerPolicy policyWithItems(long id, List<RangerPolicyItem> allowItems, List<RangerPolicyItem> denyItems) {
        RangerPolicy p = new RangerPolicy();
        p.setId(id);
        p.setResources(Collections.singletonMap("db", new RangerPolicyResource("hr")));
        p.setPolicyItems(new ArrayList<>(allowItems));
        p.setDenyPolicyItems(new ArrayList<>(denyItems));
        return p;
    }

    private RangerPolicyItem userItem(String user, String accessType) {
        RangerPolicyItem item = new RangerPolicyItem();
        item.addUser(user);
        item.addAccess(new RangerPolicyItemAccess(accessType));
        return item;
    }

    private RangerPolicyItem groupItem(String group, String accessType) {
        RangerPolicyItem item = new RangerPolicyItem();
        item.addGroup(group);
        item.addAccess(new RangerPolicyItemAccess(accessType));
        return item;
    }

    private RangerServiceDef serviceDefWithAccessTypes(String... accessTypes) {
        RangerServiceDef sd = new RangerServiceDef();
        sd.setName("svc-def");
        sd.setResources(new ArrayList<>());
        List<RangerAccessTypeDef> defs = new ArrayList<>();
        for (String accessType : accessTypes) {
            RangerAccessTypeDef at = new RangerAccessTypeDef();
            at.setName(accessType);
            defs.add(at);
        }
        sd.setAccessTypes(defs);
        return sd;
    }

    private RangerPolicy policyWithDenyItem(long id, String user, String accessType) {
        RangerPolicy p = new RangerPolicy();
        p.setId(id);
        p.setResources(Collections.singletonMap("db", new RangerPolicyResource("hr")));
        RangerPolicyItem item = new RangerPolicyItem();
        item.addUser(user);
        item.addAccess(new RangerPolicyItemAccess(accessType));
        p.setDenyPolicyItems(Collections.singletonList(item));
        return p;
    }

    private void stubDelegatedAdminAccesses(String... heldAccessTypes) {
        Set<String> held = new HashSet<>(Arrays.asList(heldAccessTypes));
        held.add(RangerPolicyEngine.ADMIN_ACCESS);

        RangerPolicyEvaluator eval = mock(RangerPolicyEvaluator.class);
        when(policyRepository.getPolicyEvaluators()).thenReturn(Collections.singletonList(eval));
        when(eval.getAllowedAccesses(any(), any(), any(), any(), any(), any())).thenAnswer(inv -> {
            Set<String> requested = inv.getArgument(4);
            Set<String> ret = new HashSet<>(requested);
            ret.retainAll(held);
            return ret;
        });
    }
}
