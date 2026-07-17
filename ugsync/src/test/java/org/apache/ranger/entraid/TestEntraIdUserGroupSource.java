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

package org.apache.ranger.entraid;

import org.apache.ranger.entraid.graph.EntraIdGraphClient;
import org.apache.ranger.entraid.graph.EntraIdGraphConfig;
import org.apache.ranger.entraid.graph.GraphClientException;
import org.apache.ranger.ugsyncutil.model.graph.DeltaEntry;
import org.apache.ranger.ugsyncutil.model.graph.DeltaPage;
import org.apache.ranger.ugsyncutil.model.graph.GraphGroup;
import org.apache.ranger.ugsyncutil.model.graph.GraphMemberRef;
import org.apache.ranger.ugsyncutil.model.graph.GraphUser;
import org.apache.ranger.ugsyncutil.model.graph.GroupMembershipPage;
import org.apache.ranger.ugsyncutil.model.graph.MemberType;
import org.apache.ranger.ugsyncutil.model.graph.MembershipMode;
import org.apache.ranger.ugsyncutil.util.UgsyncCommonConstants;
import org.apache.ranger.unixusersync.config.UserGroupSyncConfig;
import org.apache.ranger.usergroupsync.UserGroupSink;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class TestEntraIdUserGroupSource {
    private static final String SYNC_SOURCE = "EntraID";
    private static final String USER_GUID   = "aaaaaaaa-0000-0000-0000-000000000001";
    private static final String USER_UPN    = "alice@example.com";
    private static final String GROUP_GUID  = "bbbbbbbb-0000-0000-0000-000000000002";
    private static final String GROUP_NAME  = "Engineering";

    private UserGroupSyncConfig ugSyncConfig;
    private EntraIdGraphConfig  graphConfig;
    private EntraIdGraphClient  graphClient;
    private UserGroupSink       sink;

    @BeforeEach
    public void setUp() throws GraphClientException {
        ugSyncConfig = Mockito.mock(UserGroupSyncConfig.class, Mockito.withSettings().lenient());
        graphConfig  = Mockito.mock(EntraIdGraphConfig.class, Mockito.withSettings().lenient());
        graphClient  = Mockito.mock(EntraIdGraphClient.class, Mockito.withSettings().lenient());
        sink         = Mockito.mock(UserGroupSink.class, Mockito.withSettings().lenient());
        Mockito.when(graphConfig.getMembershipMode()).thenReturn(MembershipMode.DIRECT);
        // Default for full-sync (DIRECT) tests that don't assert on groups: an empty inline
        // members page, so the inline path returns non-null. Group-asserting tests override.
        Mockito.lenient().when(graphClient.getGroupDeltaWithMembers(Mockito.any())).thenReturn(membersPage("gDelta"));
    }

    // Build a GroupMembershipPage (inline-members full-sync return) from group entries.
    @SafeVarargs
    private GroupMembershipPage membersPage(String deltaLink, DeltaEntry<GraphGroup>... entries) {
        List<DeltaEntry<GraphGroup>> list = new ArrayList<>();
        Collections.addAll(list, entries);
        return new GroupMembershipPage(list, new HashMap<>(), deltaLink, false);
    }

    // Build a GroupMembershipPage with an explicit groupId -> member-GUIDs membership map.
    @SafeVarargs
    private GroupMembershipPage membersPage(Map<String, Set<String>> membersByGroupId, DeltaEntry<GraphGroup>... entries) {
        List<DeltaEntry<GraphGroup>> list = new ArrayList<>();
        Collections.addAll(list, entries);
        return new GroupMembershipPage(list, membersByGroupId, "gDelta", false);
    }

    // A GroupMembershipPage flagged resynced (inline-path equivalent of a resynced group page).
    @SafeVarargs
    private GroupMembershipPage resyncedGroupMembersPage(String deltaLink, DeltaEntry<GraphGroup>... entries) {
        List<DeltaEntry<GraphGroup>> list = new ArrayList<>();
        Collections.addAll(list, entries);
        return new GroupMembershipPage(list, new HashMap<>(), deltaLink, true);
    }

    // A resynced DeltaPage<GraphGroup> (used when groups come via the non-inline getGroupDelta path).
    @SafeVarargs
    private DeltaPage<GraphGroup> resyncedGroupPage(String deltaLink, DeltaEntry<GraphGroup>... entries) {
        List<DeltaEntry<GraphGroup>> list = new ArrayList<>();
        Collections.addAll(list, entries);
        return new DeltaPage<>(list, deltaLink, true);
    }

    private EntraIdUserGroupSource newSource() throws Throwable {
        return new EntraIdUserGroupSource(ugSyncConfig, graphConfig, graphClient, SYNC_SOURCE);
    }

    private GraphUser user() {
        GraphUser u = new GraphUser();
        u.setId(TestEntraIdUserGroupSource.USER_GUID);
        u.setUserPrincipalName(TestEntraIdUserGroupSource.USER_UPN);
        return u;
    }

    private GraphGroup group(String groupId, String name) {
        GraphGroup g = new GraphGroup();
        g.setId(groupId);
        g.setDisplayName(name);
        return g;
    }

    @SafeVarargs
    private DeltaPage<GraphUser> userPage(String deltaLink, DeltaEntry<GraphUser>... entries) {
        List<DeltaEntry<GraphUser>> list = new ArrayList<>();
        Collections.addAll(list, entries);
        return new DeltaPage<>(list, deltaLink);
    }

    @SafeVarargs
    private DeltaPage<GraphUser> resyncedUserPage(String deltaLink, DeltaEntry<GraphUser>... entries) {
        List<DeltaEntry<GraphUser>> list = new ArrayList<>();
        Collections.addAll(list, entries);
        return new DeltaPage<>(list, deltaLink, true);
    }

    @SafeVarargs
    private DeltaPage<GraphGroup> groupPage(String deltaLink, DeltaEntry<GraphGroup>... entries) {
        List<DeltaEntry<GraphGroup>> list = new ArrayList<>();
        Collections.addAll(list, entries);
        return new DeltaPage<>(list, deltaLink);
    }

    @SuppressWarnings("unchecked")
    private Map<String, Map<String, String>>[] captureUserAndGroupMaps() throws Throwable {
        ArgumentCaptor<Map<String, Map<String, String>>> groups = ArgumentCaptor.forClass(Map.class);
        ArgumentCaptor<Map<String, Map<String, String>>> users  = ArgumentCaptor.forClass(Map.class);
        ArgumentCaptor<Map<String, Set<String>>>         gusers = ArgumentCaptor.forClass(Map.class);
        ArgumentCaptor<Boolean>                          del    = ArgumentCaptor.forClass(Boolean.class);
        Mockito.verify(sink).addOrUpdateUsersGroups(groups.capture(), users.capture(), gusers.capture(), del.capture());
        return new Map[] {groups.getValue(), users.getValue()};
    }

    private GraphMemberRef userRef(String guid) {
        return GraphMemberRef.added(guid, MemberType.USER);
    }

    @Test
    public void test01_users_keyedByGuid_notUsername() throws Throwable {
        Mockito.when(graphClient.getUserDelta(null)).thenReturn(userPage("uDelta", new DeltaEntry<>(user(), false)));
        Mockito.when(graphClient.getGroupDelta(null)).thenReturn(groupPage("gDelta"));
        EntraIdUserGroupSource source = newSource();
        source.updateSink(sink);
        Map<String, Map<String, String>>[] maps = captureUserAndGroupMaps();
        Map<String, Map<String, String>>   users = maps[1];
        Assertions.assertTrue(users.containsKey(USER_GUID), "user map must be keyed by object-id GUID");
        Assertions.assertFalse(users.containsKey(USER_UPN), "user map must not be keyed by UPN");
    }

    @Test
    public void test02_user_carriesOriginalNameAsUpn() throws Throwable {
        Mockito.when(graphClient.getUserDelta(null)).thenReturn(userPage("uDelta", new DeltaEntry<>(user(), false)));
        Mockito.when(graphClient.getGroupDelta(null)).thenReturn(groupPage("gDelta"));
        EntraIdUserGroupSource source = newSource();
        source.updateSink(sink);
        Map<String, String> attrs = captureUserAndGroupMaps()[1].get(USER_GUID);
        Assertions.assertEquals(USER_UPN, attrs.get(UgsyncCommonConstants.ORIGINAL_NAME));
    }

    @Test
    public void test03_user_fullNameIsGuid() throws Throwable {
        Mockito.when(graphClient.getUserDelta(null)).thenReturn(userPage("uDelta", new DeltaEntry<>(user(), false)));
        Mockito.when(graphClient.getGroupDelta(null)).thenReturn(groupPage("gDelta"));
        EntraIdUserGroupSource source = newSource();
        source.updateSink(sink);
        Map<String, String> attrs = captureUserAndGroupMaps()[1].get(USER_GUID);
        Assertions.assertEquals(USER_GUID, attrs.get(UgsyncCommonConstants.FULL_NAME));
    }

    @Test
    public void test04_user_syncSourceIsSet() throws Throwable {
        Mockito.when(graphClient.getUserDelta(null)).thenReturn(userPage("uDelta", new DeltaEntry<>(user(), false)));
        Mockito.when(graphClient.getGroupDelta(null)).thenReturn(groupPage("gDelta"));
        EntraIdUserGroupSource source = newSource();
        source.updateSink(sink);
        Map<String, String> attrs = captureUserAndGroupMaps()[1].get(USER_GUID);
        Assertions.assertEquals(SYNC_SOURCE, attrs.get(UgsyncCommonConstants.SYNC_SOURCE));
    }

    @Test
    public void test05_user_ldapUrlIsNotSet() throws Throwable {
        // Mirroring Unix: omitting LDAP_URL is what lets the sink's null==null delete gate apply.
        Mockito.when(graphClient.getUserDelta(null)).thenReturn(userPage("uDelta", new DeltaEntry<>(user(), false)));
        Mockito.when(graphClient.getGroupDelta(null)).thenReturn(groupPage("gDelta"));
        EntraIdUserGroupSource source = newSource();
        source.updateSink(sink);
        Map<String, String> attrs = captureUserAndGroupMaps()[1].get(USER_GUID);
        Assertions.assertFalse(attrs.containsKey(UgsyncCommonConstants.LDAP_URL));
    }

    @Test
    public void test06_removedUser_isExcludedFromUpsertSnapshot() throws Throwable {
        // deletes enabled, frequency high so this is a NORMAL (non-sweep) cycle.
        Mockito.when(ugSyncConfig.isUserSyncDeletesEnabled()).thenReturn(true);
        Mockito.when(ugSyncConfig.getUserSyncDeletesFrequency()).thenReturn(100L);
        Mockito.when(graphClient.getUserDelta(null)).thenReturn(userPage("uDelta", new DeltaEntry<>(user(), true)));
        Mockito.when(graphClient.getGroupDelta(null)).thenReturn(groupPage("gDelta"));
        EntraIdUserGroupSource source = newSource();
        source.updateSink(sink);
        Map<String, Map<String, String>> users = captureUserAndGroupMaps()[1];
        Assertions.assertTrue(users.isEmpty(), "removed entries must not appear in the upsert snapshot");
    }

    @Test
    public void test07_group_keyedByGuid() throws Throwable {
        Mockito.when(graphClient.getUserDelta(null)).thenReturn(userPage("uDelta"));
        Mockito.when(graphClient.getGroupDeltaWithMembers(null)).thenReturn(membersPage("gDelta", new DeltaEntry<>(group(GROUP_GUID, GROUP_NAME), false)));
        EntraIdUserGroupSource source = newSource();
        source.updateSink(sink);
        Map<String, Map<String, String>> groups = captureUserAndGroupMaps()[0];
        Assertions.assertTrue(groups.containsKey(GROUP_GUID));
        Assertions.assertEquals(GROUP_NAME, groups.get(GROUP_GUID).get(UgsyncCommonConstants.ORIGINAL_NAME));
    }

    @Test
    public void test08_membership_flowsFromInlinePageToSink() throws Throwable {
        // On the full-sync DIRECT path, membership comes from the inline GroupMembershipPage
        // map (already user-filtered by the client; see client test for type exclusion).
        // This asserts the source passes that membership through to the sink, keyed by group.
        Map<String, Set<String>> membership = new HashMap<>();
        membership.put(GROUP_GUID, new LinkedHashSet<>(List.of(USER_GUID)));
        Mockito.when(graphClient.getUserDelta(null)).thenReturn(userPage("uDelta", new DeltaEntry<>(user(), false)));
        Mockito.when(graphClient.getGroupDeltaWithMembers(null)).thenReturn(membersPage(membership, new DeltaEntry<>(group(GROUP_GUID, GROUP_NAME), false)));
        ArgumentCaptor<Map<String, Set<String>>> gusers = ArgumentCaptor.forClass(Map.class);
        EntraIdUserGroupSource source = newSource();
        source.updateSink(sink);
        Mockito.verify(sink).addOrUpdateUsersGroups(Mockito.any(), Mockito.any(), gusers.capture(), Mockito.anyBoolean());
        Set<String> members = gusers.getValue().get(GROUP_GUID);
        Assertions.assertTrue(members.contains(USER_GUID), "inline membership must flow to the sink keyed by group");
    }

    @Test
    public void test09_firstCycle_passesComputeDeletesFalse() throws Throwable {
        Mockito.when(ugSyncConfig.isUserSyncDeletesEnabled()).thenReturn(false);
        Mockito.when(graphClient.getUserDelta(Mockito.any())).thenReturn(userPage("uDelta"));
        Mockito.when(graphClient.getGroupDelta(Mockito.any())).thenReturn(groupPage("gDelta"));
        EntraIdUserGroupSource source = newSource();
        source.updateSink(sink);
        ArgumentCaptor<Boolean> del = ArgumentCaptor.forClass(Boolean.class);
        Mockito.verify(sink).addOrUpdateUsersGroups(Mockito.any(), Mockito.any(), Mockito.any(), del.capture());
        Assertions.assertFalse(del.getValue(), "deletes disabled -> computeDeletes must be false");
    }

    @Test
    public void test10_reconcileSweep_passesComputeDeletesTrue() throws Throwable {
        // deletes enabled, frequency 1 -> first cycle is already a reconcile-sweep cycle.
        Mockito.when(ugSyncConfig.isUserSyncDeletesEnabled()).thenReturn(true);
        Mockito.when(ugSyncConfig.getUserSyncDeletesFrequency()).thenReturn(1L);
        Mockito.when(graphClient.getUserDelta(Mockito.any())).thenReturn(userPage("uDelta"));
        Mockito.when(graphClient.getGroupDelta(Mockito.any())).thenReturn(groupPage("gDelta"));
        EntraIdUserGroupSource source = newSource();
        source.updateSink(sink);
        ArgumentCaptor<Boolean> del = ArgumentCaptor.forClass(Boolean.class);
        Mockito.verify(sink).addOrUpdateUsersGroups(Mockito.any(), Mockito.any(), Mockito.any(), del.capture());
        Assertions.assertTrue(del.getValue(), "reconcile sweep -> computeDeletes must be true");
    }

    @Test
    public void test11_reconcileSweep_forcesFullPull_nullDeltaToken() throws Throwable {
        Mockito.when(ugSyncConfig.isUserSyncDeletesEnabled()).thenReturn(true);
        Mockito.when(ugSyncConfig.getUserSyncDeletesFrequency()).thenReturn(1L);
        Mockito.when(graphClient.getUserDelta(Mockito.any())).thenReturn(userPage("uDelta"));
        Mockito.when(graphClient.getGroupDeltaWithMembers(Mockito.any())).thenReturn(membersPage("gDelta"));
        EntraIdUserGroupSource source = newSource();
        source.updateSink(sink);
        // A reconcile sweep must request a full snapshot (delta token == null). Groups come
        // via the inline path in DIRECT mode.
        Mockito.verify(graphClient).getUserDelta(null);
        Mockito.verify(graphClient).getGroupDeltaWithMembers(null);
    }

    @Test
    public void test16_normalCycle_removedUser_callsDeleteWithGuid() throws Throwable {
        // Normal cycle (frequency high so no sweep): a @removed user must be sent to
        // sink.deleteUsersAndGroups keyed by GUID, NOT to the upsert path.
        Mockito.when(ugSyncConfig.isUserSyncDeletesEnabled()).thenReturn(true);
        Mockito.when(ugSyncConfig.getUserSyncDeletesFrequency()).thenReturn(100L);
        Mockito.when(graphClient.getUserDelta(null)).thenReturn(userPage("uDelta", new DeltaEntry<>(user(), true)));
        Mockito.when(graphClient.getGroupDelta(null)).thenReturn(groupPage("gDelta"));
        EntraIdUserGroupSource source = newSource();
        source.updateSink(sink);
        ArgumentCaptor<Map<String, Map<String, String>>> delUsers = ArgumentCaptor.forClass(Map.class);
        Mockito.verify(sink).deleteUsersAndGroups(delUsers.capture(), Mockito.any());
        Assertions.assertTrue(delUsers.getValue().containsKey(USER_GUID), "deleted user must be keyed by GUID");
        Assertions.assertEquals(USER_GUID, delUsers.getValue().get(USER_GUID).get(UgsyncCommonConstants.FULL_NAME));
        Assertions.assertEquals(SYNC_SOURCE, delUsers.getValue().get(USER_GUID).get(UgsyncCommonConstants.SYNC_SOURCE));
    }

    @Test
    public void test17_normalCycle_removedGroup_callsDeleteWithGuid() throws Throwable {
        Mockito.when(ugSyncConfig.isUserSyncDeletesEnabled()).thenReturn(true);
        Mockito.when(ugSyncConfig.getUserSyncDeletesFrequency()).thenReturn(100L);
        Mockito.when(graphClient.getUserDelta(null)).thenReturn(userPage("uDelta"));
        Mockito.when(graphClient.getGroupDeltaWithMembers(null)).thenReturn(membersPage("gDelta", new DeltaEntry<>(group(GROUP_GUID, GROUP_NAME), true)));
        EntraIdUserGroupSource source = newSource();
        source.updateSink(sink);
        ArgumentCaptor<Map<String, Map<String, String>>> delGroups = ArgumentCaptor.forClass(Map.class);
        Mockito.verify(sink).deleteUsersAndGroups(Mockito.any(), delGroups.capture());
        Assertions.assertTrue(delGroups.getValue().containsKey(GROUP_GUID), "deleted group must be keyed by GUID");
    }

    @Test
    public void test18_normalCycle_doesNotForceFullPull() throws Throwable {
        // Establish a delta token on cycle 1, then assert cycle 2 (normal) uses it
        // rather than forcing a full pull -- the key scale property for Bosch.
        Mockito.when(ugSyncConfig.isUserSyncDeletesEnabled()).thenReturn(true);
        Mockito.when(ugSyncConfig.getUserSyncDeletesFrequency()).thenReturn(100L);
        Mockito.when(graphClient.getUserDelta(null)).thenReturn(userPage("uDelta-1"));
        Mockito.when(graphClient.getGroupDeltaWithMembers(null)).thenReturn(membersPage("gDelta-1"));
        EntraIdUserGroupSource source = newSource();
        source.updateSink(sink); // cycle 1: first sync, full pull, captures tokens
        Mockito.when(graphClient.getUserDelta("uDelta-1")).thenReturn(userPage("uDelta-2"));
        Mockito.when(graphClient.getGroupDelta("gDelta-1")).thenReturn(groupPage("gDelta-2"));
        source.updateSink(sink); // cycle 2: normal cycle must use the delta token
        Mockito.verify(graphClient).getUserDelta("uDelta-1");
        Mockito.verify(graphClient).getGroupDelta("gDelta-1");
    }

    @Test
    public void test19_reconcileSweep_doesNotCallPerRecordDelete() throws Throwable {
        // On a sweep, deletions are computed by the sink's snapshot-diff (computeDeletes=true),
        // so the per-record delete path must NOT be invoked even if @removed entries arrive.
        Mockito.when(ugSyncConfig.isUserSyncDeletesEnabled()).thenReturn(true);
        Mockito.when(ugSyncConfig.getUserSyncDeletesFrequency()).thenReturn(1L);
        Mockito.when(graphClient.getUserDelta(null)).thenReturn(userPage("uDelta", new DeltaEntry<>(user(), true)));
        Mockito.when(graphClient.getGroupDelta(null)).thenReturn(groupPage("gDelta"));
        EntraIdUserGroupSource source = newSource();
        source.updateSink(sink);
        Mockito.verify(sink, Mockito.never()).deleteUsersAndGroups(Mockito.any(), Mockito.any());
    }

    @Test
    public void test20_normalCycle_noRemovals_doesNotCallDelete() throws Throwable {
        // No @removed entries -> the per-record delete path must not be invoked at all.
        Mockito.when(ugSyncConfig.isUserSyncDeletesEnabled()).thenReturn(true);
        Mockito.when(ugSyncConfig.getUserSyncDeletesFrequency()).thenReturn(100L);
        Mockito.when(graphClient.getUserDelta(null)).thenReturn(userPage("uDelta", new DeltaEntry<>(user(), false)));
        Mockito.when(graphClient.getGroupDelta(null)).thenReturn(groupPage("gDelta"));
        EntraIdUserGroupSource source = newSource();
        source.updateSink(sink);
        Mockito.verify(sink, Mockito.never()).deleteUsersAndGroups(Mockito.any(), Mockito.any());
    }

    @Test
    public void test21_bothResynced_promotesToSweep_computeDeletesTrue() throws Throwable {
        // Both pages came from a forced resync -> cycle must become a reconciliation sweep
        // (computeDeletes=true), and per-record deletes must NOT fire.
        Mockito.when(ugSyncConfig.isUserSyncDeletesEnabled()).thenReturn(true);
        Mockito.when(ugSyncConfig.getUserSyncDeletesFrequency()).thenReturn(100L);
        Mockito.when(graphClient.getUserDelta(null)).thenReturn(resyncedUserPage("uDelta", new DeltaEntry<>(user(), true)));
        Mockito.when(graphClient.getGroupDeltaWithMembers(null)).thenReturn(resyncedGroupMembersPage("gDelta"));
        EntraIdUserGroupSource source = newSource();
        source.updateSink(sink);
        ArgumentCaptor<Boolean> del = ArgumentCaptor.forClass(Boolean.class);
        Mockito.verify(sink).addOrUpdateUsersGroups(Mockito.any(), Mockito.any(), Mockito.any(), del.capture());
        Assertions.assertTrue(del.getValue(), "a resync must force computeDeletes=true (reconcile sweep)");
        // Per-record delete path must be suppressed on a sweep even though @removed was present.
        Mockito.verify(sink, Mockito.never()).deleteUsersAndGroups(Mockito.any(), Mockito.any());
    }

    @Test
    public void test22_onlyUserResynced_forcesFullGroupPull() throws Throwable {
        // DANGEROUS PARTIAL CASE: only the user delta expired. The group page would still be
        // incremental; passing it with computeDeletes=true would mass-delete groups. The source
        // must force a FULL group pull before computing deletes. In DIRECT mode that full pull
        // uses the inline-members path (getGroupDeltaWithMembers).
        Mockito.when(ugSyncConfig.isUserSyncDeletesEnabled()).thenReturn(true);
        Mockito.when(ugSyncConfig.getUserSyncDeletesFrequency()).thenReturn(100L);
        // Cycle 1 (full, DIRECT) establishes tokens via the inline path.
        Mockito.when(graphClient.getUserDelta(null)).thenReturn(userPage("uDelta-1"));
        Mockito.when(graphClient.getGroupDeltaWithMembers(null)).thenReturn(membersPage("gDelta-1"));
        EntraIdUserGroupSource source = newSource();
        source.updateSink(sink); // cycle 1: full pull, tokens captured
        // Cycle 2: user delta link expired -> resynced user page; group delta succeeds
        // incrementally (NOT resynced) off the persisted token.
        Mockito.when(graphClient.getUserDelta("uDelta-1")).thenReturn(resyncedUserPage("uDelta-2"));
        Mockito.when(graphClient.getGroupDelta("gDelta-1")).thenReturn(groupPage("gDelta-2"));
        source.updateSink(sink);
        // getGroupDeltaWithMembers(null): cycle 1 full pull + cycle 2 forced full re-pull = 2.
        Mockito.verify(graphClient, Mockito.times(2)).getGroupDeltaWithMembers(null);
    }

    @Test
    public void test23_onlyGroupResynced_forcesFullUserPull() throws Throwable {
        Mockito.when(ugSyncConfig.isUserSyncDeletesEnabled()).thenReturn(true);
        Mockito.when(ugSyncConfig.getUserSyncDeletesFrequency()).thenReturn(100L);
        Mockito.when(graphClient.getUserDelta(null)).thenReturn(userPage("uDelta-1"));
        Mockito.when(graphClient.getGroupDeltaWithMembers(null)).thenReturn(membersPage("gDelta-1"));
        EntraIdUserGroupSource source = newSource();
        source.updateSink(sink); // cycle 1
        // Cycle 2: group delta expired (resynced), user delta incremental. On an incremental
        // cycle groups come via getGroupDelta (not the inline path), so the resync flag must
        // arrive on that method.
        Mockito.when(graphClient.getUserDelta("uDelta-1")).thenReturn(userPage("uDelta-2"));
        Mockito.when(graphClient.getGroupDelta("gDelta-1")).thenReturn(resyncedGroupPage("gDelta-2"));
        Mockito.when(graphClient.getUserDelta(null)).thenReturn(userPage("uDelta-full"));
        source.updateSink(sink);
        // getUserDelta(null) called on cycle 1 + forced re-pull = 2.
        Mockito.verify(graphClient, Mockito.times(2)).getUserDelta(null);
    }

    @Test
    public void test24_transitiveFullSync_usesPerGroupFetch_notInlineMembers() throws Throwable {
        // In TRANSITIVE mode, $select=members (inline) returns only DIRECT members, so a full
        // sync must NOT use the inline path -- it must fall back to the per-group members fetch
        // (which resolves /transitiveMembers). Otherwise a full sync and an incremental cycle
        // would return different member sets for the same group.
        Mockito.when(graphConfig.getMembershipMode()).thenReturn(MembershipMode.TRANSITIVE);
        Mockito.when(graphClient.getUserDelta(null)).thenReturn(userPage("uDelta"));
        Mockito.when(graphClient.getGroupDelta(null)).thenReturn(groupPage("gDelta", new DeltaEntry<>(group(GROUP_GUID, GROUP_NAME), false)));
        Mockito.when(graphClient.getGroupMembers(Mockito.anyString(), Mockito.any())).thenReturn(Collections.emptyList());
        EntraIdUserGroupSource source = newSource();
        source.updateSink(sink);
        // The inline path must never be taken in TRANSITIVE mode, even on a full sync.
        Mockito.verify(graphClient, Mockito.never()).getGroupDeltaWithMembers(Mockito.any());
        // Membership must be resolved per-group (TRANSITIVE => /transitiveMembers).
        Mockito.verify(graphClient).getGroupMembers(GROUP_GUID, MembershipMode.TRANSITIVE);
    }

    @Test
    public void test25_removedGroupInInlinePage_excludedFromUpsertAndMembership() throws Throwable {
        // A group flagged @removed in an inline getGroupDeltaWithMembers page must be routed
        // to the delete map ONLY: not surfaced in the upsert map or the membership map
        // (no stale group record, no orphaned membership edges).
        Mockito.when(ugSyncConfig.isUserSyncDeletesEnabled()).thenReturn(true);
        Mockito.when(ugSyncConfig.getUserSyncDeletesFrequency()).thenReturn(100L);
        Mockito.when(graphClient.getUserDelta(null)).thenReturn(userPage("uDelta", new DeltaEntry<>(user(), false)));
        String removedGroupGuid = "cccccccc-0000-0000-0000-000000000009";

        // can carry a DIFFERENT guid than the helper's fixed GROUP_GUID.
        GraphGroup liveGroup = group(GROUP_GUID, GROUP_NAME);
        GraphGroup removedGroup = new GraphGroup();
        removedGroup.setId(removedGroupGuid);
        removedGroup.setDisplayName("OldTeam");

        // Membership map carries entries for BOTH groups, to prove the removed group's
        // membership is not surfaced even when present in the page's map.
        Map<String, Set<String>> membership = new HashMap<>();
        membership.put(GROUP_GUID, new LinkedHashSet<>(List.of(USER_GUID)));
        membership.put(removedGroupGuid, new LinkedHashSet<>(List.of(USER_GUID)));

        Mockito.when(graphClient.getGroupDeltaWithMembers(null)).thenReturn(membersPage(membership,
                new DeltaEntry<>(liveGroup, false), new DeltaEntry<>(removedGroup, true)));
        EntraIdUserGroupSource source = newSource();
        source.updateSink(sink);

        // 1. Removed group -> delete map, keyed by GUID.
        ArgumentCaptor<Map<String, Map<String, String>>> delGroups = ArgumentCaptor.forClass(Map.class);
        Mockito.verify(sink).deleteUsersAndGroups(Mockito.any(), delGroups.capture());
        Assertions.assertTrue(delGroups.getValue().containsKey(removedGroupGuid), "removed group must be in the delete map");

        // 2. Removed group NOT in upsert or membership maps; live group IS upserted.
        ArgumentCaptor<Map<String, Map<String, String>>> groupUpserts = ArgumentCaptor.forClass(Map.class);
        ArgumentCaptor<Map<String, Set<String>>> groupMembers = ArgumentCaptor.forClass(Map.class);
        Mockito.verify(sink).addOrUpdateUsersGroups(groupUpserts.capture(), Mockito.any(), groupMembers.capture(), Mockito.anyBoolean());
        Assertions.assertFalse(groupUpserts.getValue().containsKey(removedGroupGuid), "removed group must not be upserted");
        Assertions.assertTrue(groupUpserts.getValue().containsKey(GROUP_GUID), "live group must be upserted");
        Assertions.assertFalse(groupMembers.getValue().containsKey(removedGroupGuid), "removed group must not have membership edges surfaced");
    }

    @Test
    public void test26_transitiveGroupMemberFetch404_skipsGroupNotWholeSync() throws Throwable {
        // TRANSITIVE mode: one group 404s on member fetch. That group's membership is skipped,
        // but the sync completes and OTHER groups still sync.
        Mockito.when(graphConfig.getMembershipMode()).thenReturn(MembershipMode.TRANSITIVE);
        Mockito.when(graphClient.getUserDelta(null)).thenReturn(userPage("uD", new DeltaEntry<>(user(), false)));
        Mockito.when(graphClient.getGroupDelta(null)).thenReturn(groupPage("gD", new DeltaEntry<>(group(GROUP_GUID, "GoodGroup"), false),
                new DeltaEntry<>(group("bad-guid", "GoneGroup"), false)));
        // Good group returns members; bad group 404s.
        Mockito.when(graphClient.getGroupMembers(GROUP_GUID, MembershipMode.TRANSITIVE)).thenReturn(List.of(userRef(USER_GUID)));
        Mockito.when(graphClient.getGroupMembers("bad-guid", MembershipMode.TRANSITIVE)).thenThrow(new GraphClientException("not found", 404));

        EntraIdUserGroupSource source = newSource();
        source.updateSink(sink);   // must NOT throw

        // Both groups upserted; good group has its member, bad group has empty membership.
        Map<String, Map<String, String>>[] maps = captureUserAndGroupMaps();
        Assertions.assertTrue(maps[0].containsKey(GROUP_GUID), "good group synced");
        Assertions.assertTrue(maps[0].containsKey("bad-guid"), "bad group still synced (membership skipped)");
    }

    @Test
    public void test27_transitiveGroupMemberFetch401_failsWholeCycle() throws Throwable {
        // A systemic failure (401, not 404) during member fetch must propagate and fail the
        // cycle -- we do NOT silently sync empty memberships when the connection is broken.
        Mockito.when(graphConfig.getMembershipMode()).thenReturn(MembershipMode.TRANSITIVE);
        Mockito.when(graphClient.getUserDelta(null)).thenReturn(userPage("uD", new DeltaEntry<>(user(), false)));
        Mockito.when(graphClient.getGroupDelta(null)).thenReturn(groupPage("gD", new DeltaEntry<>(group(GROUP_GUID, "G"), false)));
        Mockito.when(graphClient.getGroupMembers(GROUP_GUID, MembershipMode.TRANSITIVE)).thenThrow(new GraphClientException("unauthorized", 401));

        EntraIdUserGroupSource source = newSource();
        Assertions.assertThrows(GraphClientException.class, () -> source.updateSink(sink));
    }

    @Test
    public void test28_nullUserPage_treatedAsEmptyNoNpe() throws Throwable {
        // A null page from the client (contract violation / empty-body parse) must degrade to
        // an empty sync, not NPE. Token must NOT advance (null page = nothing received).
        Mockito.when(graphClient.getUserDelta(null)).thenReturn(null);
        Mockito.when(graphClient.getGroupDeltaWithMembers(null)).thenReturn(membersPage(new HashMap<>()));   // empty groups
        EntraIdUserGroupSource source = newSource();
        source.updateSink(sink);   // must NOT throw
        // No users upserted; sink still called with empty maps.
        Map<String, Map<String, String>>[] maps = captureUserAndGroupMaps();
        Assertions.assertTrue(maps[1].isEmpty(), "null user page yields no users");
    }

    @Test
    public void test29_emptyPageWithDeltaLink_advancesToken() throws Throwable {
        // An empty value array with a valid deltaLink must advance the token, so the next
        // cycle resumes from the new token rather than re-fetching the empty page forever.
        Mockito.when(graphClient.getUserDelta(null)).thenReturn(userPage("USER-TOKEN-2"));      // no entries
        Mockito.when(graphClient.getGroupDeltaWithMembers(null)).thenReturn(membersPage(new HashMap<>()));   // no groups, deltaLink "gDelta"

        EntraIdUserGroupSource source = newSource();
        source.updateSink(sink);

        // Second cycle should resume from the advanced token, not null.
        Mockito.when(graphConfig.getMembershipMode()).thenReturn(MembershipMode.DIRECT);
        // firstSyncDone is now true, so the next getUserDelta is called with the new token:
        Mockito.when(graphClient.getUserDelta("USER-TOKEN-2")).thenReturn(userPage("USER-TOKEN-3"));
        Mockito.when(graphClient.getGroupDelta("gDelta")).thenReturn(groupPage("gDelta2"));
        source.updateSink(sink);

        // Verify the second cycle used the advanced token, proving it was persisted.
        Mockito.verify(graphClient).getUserDelta("USER-TOKEN-2");
    }

    @Test
    public void test30_emptyDisplayName_groupExcludedLikeNull() throws Throwable {
        // group("") must be treated the same as group(null): StringUtils.isBlank catches both,
        // so an empty-name group is not synced (an empty Ranger group name would be invalid).
        Mockito.when(graphClient.getUserDelta(null)).thenReturn(userPage("uD", new DeltaEntry<>(user(), false)));
        Mockito.when(graphClient.getGroupDeltaWithMembers(null)).thenReturn(membersPage(new HashMap<>(), new DeltaEntry<>(group(GROUP_GUID, ""), false)));   // empty display name

        EntraIdUserGroupSource source = newSource();
        source.updateSink(sink);

        Map<String, Map<String, String>>[] maps = captureUserAndGroupMaps();
        Assertions.assertTrue(maps[0].isEmpty(), "group with empty displayName must be excluded");
    }

    @Test
    public void test12_deltaTokens_advanceAfterSuccessfulSink() throws Throwable {
        Mockito.when(ugSyncConfig.isUserSyncDeletesEnabled()).thenReturn(false);
        Mockito.when(graphClient.getUserDelta(null)).thenReturn(userPage("uDelta-1"));
        Mockito.when(graphClient.getGroupDeltaWithMembers(null)).thenReturn(membersPage("gDelta-1"));
        EntraIdUserGroupSource source = newSource();
        source.updateSink(sink); // first cycle: full pull (null), captures uDelta-1/gDelta-1
        // Second cycle should now use the persisted tokens, not null.
        Mockito.when(graphClient.getUserDelta("uDelta-1")).thenReturn(userPage("uDelta-2"));
        Mockito.when(graphClient.getGroupDelta("gDelta-1")).thenReturn(groupPage("gDelta-2"));
        source.updateSink(sink);
        Mockito.verify(graphClient).getUserDelta("uDelta-1");
        Mockito.verify(graphClient).getGroupDelta("gDelta-1");
    }

    @Test
    public void test13_deltaTokens_doNotAdvanceWhenSinkFails() throws Throwable {
        Mockito.when(ugSyncConfig.isUserSyncDeletesEnabled()).thenReturn(false);
        Mockito.when(graphClient.getUserDelta(null)).thenReturn(userPage("uDelta-1"));
        Mockito.when(graphClient.getGroupDelta(null)).thenReturn(groupPage("gDelta-1"));
        Mockito.doThrow(new RuntimeException("admin down")).when(sink).addOrUpdateUsersGroups(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.anyBoolean());
        EntraIdUserGroupSource source = newSource();
        source.updateSink(sink);   // sink throws; tokens must not advance
        source.updateSink(sink);   // second cycle must retry with null, not uDelta-1
        // getUserDelta(null) called on both cycles (token never advanced).
        Mockito.verify(graphClient, Mockito.times(2)).getUserDelta(null);
        Mockito.verify(graphClient, Mockito.never()).getUserDelta("uDelta-1");
    }

    @Test
    public void test14_groupWithoutDisplayName_isSkipped() throws Throwable {
        Mockito.when(graphClient.getUserDelta(null)).thenReturn(userPage("uDelta"));
        Mockito.when(graphClient.getGroupDeltaWithMembers(null)).thenReturn(membersPage("gDelta", new DeltaEntry<>(group(GROUP_GUID, null), false)));
        EntraIdUserGroupSource source = newSource();
        source.updateSink(sink);
        Map<String, Map<String, String>> groups = captureUserAndGroupMaps()[0];
        Assertions.assertTrue(groups.isEmpty(), "a group with no display name has no Ranger name and is skipped");
    }

    @Test
    public void test15_auditInfo_isPosted() throws Throwable {
        Mockito.when(graphClient.getUserDelta(null)).thenReturn(userPage("uDelta"));
        Mockito.when(graphClient.getGroupDelta(null)).thenReturn(groupPage("gDelta"));
        EntraIdUserGroupSource source = newSource();
        source.updateSink(sink);
        Mockito.verify(sink).postUserGroupAuditInfo(Mockito.any());
    }
}
