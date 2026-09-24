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
package org.apache.ranger.unixusersync.process;

import org.apache.ranger.authorization.utils.JsonUtils;
import org.apache.ranger.ugsyncutil.model.XGroupInfo;
import org.apache.ranger.ugsyncutil.model.XUserInfo;
import org.apache.ranger.ugsyncutil.util.UgsyncCommonConstants;
import org.apache.ranger.unixusersync.config.UserGroupSyncConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@TestMethodOrder(MethodOrderer.MethodName.class)
public class PolicyMgrUserGroupBuilderReconcileTest {
    private static final String SYNC_SOURCE_VALUE = "EntraID";
    private static final String OTHER_SYNC_SOURCE = "LDAP";
    private static final String ISVISIBLE = "1";
    private static final String ISHIDDEN = "0";

    private PolicyMgrUserGroupBuilder builder;

    @BeforeEach
    public void setUp() throws Exception {
        UserGroupSyncConfig config = UserGroupSyncConfig.getInstance();
        // Property key constants for case conversion live on UgsyncCommonConstants (see
        // UserGroupSyncConfig.getUserNameCaseConversion()/getGroupNameCaseConversion(),
        // which read exactly these keys); only UGSYNC_NAME_VALIDATION_ENABLED is on
        // UserGroupSyncConfig itself.
        config.setProperty(UgsyncCommonConstants.UGSYNC_GROUPNAME_CASE_CONVERSION_PARAM, UgsyncCommonConstants.UGSYNC_NONE_CASE_CONVERSION_VALUE);
        config.setProperty(UgsyncCommonConstants.UGSYNC_USERNAME_CASE_CONVERSION_PARAM, UgsyncCommonConstants.UGSYNC_NONE_CASE_CONVERSION_VALUE);
        config.setProperty(UserGroupSyncConfig.UGSYNC_NAME_VALIDATION_ENABLED, "false");

        builder = new PolicyMgrUserGroupBuilder();

        setField("groupCache", new HashMap<String, XGroupInfo>());
        setField("userCache", new HashMap<String, XUserInfo>());
        setField("groupNameMap", new HashMap<String, String>());
        setField("userNameMap", new HashMap<String, String>());
        setField("currentSyncSource", SYNC_SOURCE_VALUE);
        setField("ldapUrl", null);
        setField("isStartupFlag", false); // reconcile sweeps always run with isStartupFlag == false
    }

    private void setField(String name, Object value) throws Exception {
        Field f = PolicyMgrUserGroupBuilder.class.getDeclaredField(name);
        f.setAccessible(true);
        f.set(builder, value);
    }

    @SuppressWarnings("unchecked")
    private <T> T getField(String name) throws Exception {
        Field f = PolicyMgrUserGroupBuilder.class.getDeclaredField(name);
        f.setAccessible(true);
        return (T) f.get(builder);
    }

    private void invoke(String name, Class<?> paramType, Object arg) throws Exception {
        Method m = PolicyMgrUserGroupBuilder.class.getDeclaredMethod(name, paramType);
        m.setAccessible(true);
        try {
            m.invoke(builder, arg);
        } catch (InvocationTargetException e) {
            Throwable cause = e.getCause();
            if (cause instanceof RuntimeException) {
                throw (RuntimeException) cause;
            }
            if (cause instanceof Error) {
                throw (Error) cause;
            }
            throw e;
        }
    }

    /**
     * A live (or explicitly hidden) cache entry, sync-source-tagged, keyed by its own GUID.
     */
    private XGroupInfo group(String name, String fullName, String syncSource, String isVisible) {
        XGroupInfo g = new XGroupInfo();
        g.setName(name);
        g.setIsVisible(isVisible);
        g.setSyncSource(syncSource);
        Map<String, String> attrs = new HashMap<>();
        attrs.put(UgsyncCommonConstants.FULL_NAME, fullName);
        attrs.put(UgsyncCommonConstants.SYNC_SOURCE, syncSource);
        g.setOtherAttrsMap(attrs);
        g.setOtherAttributes(JsonUtils.objectToJson(attrs));
        return g;
    }

    private XUserInfo user(String name, String fullName, String syncSource, String isVisible) {
        XUserInfo u = new XUserInfo();
        u.setName(name);
        u.setFirstName(name);
        u.setIsVisible(isVisible);
        u.setSyncSource(syncSource);
        Map<String, String> attrs = new HashMap<>();
        attrs.put(UgsyncCommonConstants.FULL_NAME, fullName);
        attrs.put(UgsyncCommonConstants.SYNC_SOURCE, syncSource);
        u.setOtherAttrsMap(attrs);
        u.setOtherAttributes(JsonUtils.objectToJson(attrs));
        return u;
    }

    private XGroupInfo liveGroup(String name, String guid) {
        return group(name, guid, SYNC_SOURCE_VALUE, ISVISIBLE);
    }

    private XUserInfo liveUser(String name, String guid) {
        return user(name, guid, SYNC_SOURCE_VALUE, ISVISIBLE);
    }

    private Map<String, String> attrs(String guid, String displayName) {
        Map<String, String> attrs = new HashMap<>();
        attrs.put(UgsyncCommonConstants.FULL_NAME, guid);
        attrs.put(UgsyncCommonConstants.SYNC_SOURCE, SYNC_SOURCE_VALUE);
        attrs.put(UgsyncCommonConstants.ORIGINAL_NAME, displayName);
        return attrs;
    }

    @Test
    public void testR1b_reconcile_sameNameDifferentGuid_sameSource_mustNotMerge() throws Exception {
        String cachedGuid = "AAAA0001-0000-0000-0000-000000000001";
        String otherGuid = "AAAA0002-0000-0000-0000-000000000002";
        // Not drifted: full_name correctly holds this entry's own real GUID.
        XGroupInfo cached = liveGroup("group_k", cachedGuid);
        this.<Map<String, XGroupInfo>>getField("groupCache").put("group_k", cached);

        // A second, genuinely different EntraID group that happens to share the same
        // display name right now.
        Map<String, Map<String, String>> sourceGroups = new HashMap<>();
        sourceGroups.put(otherGuid, attrs(otherGuid, "group_k"));

        invoke("computeGroupDelta", Map.class, sourceGroups);

        Map<String, XGroupInfo> deltaGroups = getField("deltaGroups");
        assertFalse(deltaGroups.containsKey("group_k"),
                "A same-name, same-source but genuinely different GUID must not be merged into the "
                        + "existing group_k, but deltaGroups=" + deltaGroups.keySet());
        assertEquals(cachedGuid, cached.getOtherAttrsMap().get(UgsyncCommonConstants.FULL_NAME),
                "The existing group's identity must not be overwritten by an unrelated group's GUID");
    }

    @Test
    public void testR2b_reconcile_sameNameDifferentGuid_sameSource_mustNotMerge() throws Exception {
        String cachedGuid = "BBBB0001-0000-0000-0000-000000000001";
        String otherGuid = "BBBB0002-0000-0000-0000-000000000002";
        // Not drifted: full_name correctly holds this entry's own real GUID.
        XUserInfo cached = liveUser("user_k", cachedGuid);
        this.<Map<String, XUserInfo>>getField("userCache").put("user_k", cached);

        // A second, genuinely different EntraID user that happens to share the same
        // display name right now (e.g. a mail/UPN-fallback collision).
        Map<String, Map<String, String>> sourceUsers = new HashMap<>();
        sourceUsers.put(otherGuid, attrs(otherGuid, "user_k"));

        invoke("computeUserDelta", Map.class, sourceUsers);

        Map<String, XUserInfo> deltaUsers = getField("deltaUsers");
        assertFalse(deltaUsers.containsKey("user_k"),
                "A same-name, same-source but genuinely different GUID must not be merged into the "
                        + "existing user_k, but deltaUsers=" + deltaUsers.keySet());
        assertEquals(cachedGuid, cached.getOtherAttrsMap().get(UgsyncCommonConstants.FULL_NAME),
                "The existing user's identity must not be overwritten by an unrelated user's GUID");
    }

    @Test
    public void testR1c_reconcile_renameCollision_group_keepsOldNameNoCorruption() throws Exception {
        String guid = "CCCC0001-0000-0000-0000-000000000001";
        String otherGuid = "CCCC0002-0000-0000-0000-000000000002";
        XGroupInfo oldGroup = liveGroup("OldName", guid);
        this.<Map<String, XGroupInfo>>getField("groupCache").put("OldName", oldGroup);
        this.<Map<String, String>>getField("groupNameMap").put(guid.toLowerCase(), "OldName");

        // A different, unrelated group already occupies the name this cycle wants to rename into.
        XGroupInfo occupant = liveGroup("NewName", otherGuid);
        this.<Map<String, XGroupInfo>>getField("groupCache").put("NewName", occupant);

        Map<String, Map<String, String>> sourceGroups = new HashMap<>();
        sourceGroups.put(guid, attrs(guid, "NewName"));

        invoke("computeGroupDelta", Map.class, sourceGroups);

        Map<String, XGroupInfo> groupCache = getField("groupCache");
        assertTrue(groupCache.containsKey("OldName"),
                "Rename must be skipped on collision; the group must stay under its old name");
        assertEquals(guid, groupCache.get("OldName").getOtherAttrsMap().get(UgsyncCommonConstants.FULL_NAME));
        assertTrue(groupCache.containsKey("NewName"), "The unrelated occupant of the target name must be untouched");
        assertEquals(otherGuid, groupCache.get("NewName").getOtherAttrsMap().get(UgsyncCommonConstants.FULL_NAME),
                "The pre-existing group's own identity must not be corrupted by the collision");
    }

    @Test
    public void testR2c_reconcile_renameCollision_user_keepsOldNameNoCorruption() throws Exception {
        String guid = "DDDD0001-0000-0000-0000-000000000001";
        String otherGuid = "DDDD0002-0000-0000-0000-000000000002";
        XUserInfo oldUser = liveUser("old_user", guid);
        this.<Map<String, XUserInfo>>getField("userCache").put("old_user", oldUser);
        this.<Map<String, String>>getField("userNameMap").put(guid.toLowerCase(), "old_user");

        // A different, unrelated user already occupies the name this cycle wants to rename into.
        XUserInfo occupant = liveUser("new_user", otherGuid);
        this.<Map<String, XUserInfo>>getField("userCache").put("new_user", occupant);

        Map<String, Map<String, String>> sourceUsers = new HashMap<>();
        sourceUsers.put(guid, attrs(guid, "new_user"));

        invoke("computeUserDelta", Map.class, sourceUsers);

        Map<String, XUserInfo> userCache = getField("userCache");
        assertTrue(userCache.containsKey("old_user"),
                "Rename must be skipped on collision; the user must stay under its old name");
        assertEquals(guid, userCache.get("old_user").getOtherAttrsMap().get(UgsyncCommonConstants.FULL_NAME));
        assertTrue(userCache.containsKey("new_user"), "The unrelated occupant of the target name must be untouched");
        assertEquals(otherGuid, userCache.get("new_user").getOtherAttrsMap().get(UgsyncCommonConstants.FULL_NAME),
                "The pre-existing user's own identity must not be corrupted by the collision");
    }

    @Test
    public void testR3_reconcile_matchingGuid_isNotFalseDeleted() throws Exception {
        String guid = "33333333-3333-3333-3333-333333333333";
        XGroupInfo cached = liveGroup("group_a", guid);
        this.<Map<String, XGroupInfo>>getField("groupCache").put("group_a", cached);

        Map<String, Map<String, String>> sourceGroups = new HashMap<>();
        sourceGroups.put(guid, attrs(guid, "group_a"));

        invoke("computeDeletedGroups", Map.class, sourceGroups);

        Map<String, XGroupInfo> deletedGroups = getField("deletedGroups");
        assertTrue(deletedGroups.isEmpty(),
                "A group present in the same-cycle source snapshot must not be hidden, but it hid: " + deletedGroups.keySet());
        assertEquals(ISVISIBLE, cached.getIsVisible());
    }

    @Test
    public void testR4_reconcile_trulyAbsent_isStillDeleted() throws Exception {
        String guid = "44444444-4444-4444-4444-444444444444";
        XGroupInfo cached = liveGroup("group_d", guid);
        this.<Map<String, XGroupInfo>>getField("groupCache").put("group_d", cached);

        Map<String, Map<String, String>> sourceGroups = new HashMap<>();
        sourceGroups.put("99999999-9999-9999-9999-999999999999", attrs("99999999-9999-9999-9999-999999999999", "some_other_group"));

        invoke("computeDeletedGroups", Map.class, sourceGroups);

        Map<String, XGroupInfo> deletedGroups = getField("deletedGroups");
        assertTrue(deletedGroups.containsKey("group_d"), "A group genuinely absent from the source snapshot must still be marked deleted");
        assertEquals(ISHIDDEN, cached.getIsVisible());
    }

    @Test
    public void testR4b_reconcile_trulyAbsentUser_isStillDeleted() throws Exception {
        String guid = "55555555-5555-5555-5555-555555555555";
        XUserInfo cached = liveUser("user_d", guid);
        this.<Map<String, XUserInfo>>getField("userCache").put("user_d", cached);

        Map<String, Map<String, String>> sourceUsers = new HashMap<>();
        sourceUsers.put("88888888-8888-8888-8888-888888888888", attrs("88888888-8888-8888-8888-888888888888", "some_other_user"));

        invoke("computeDeletedUsers", Map.class, sourceUsers);

        Map<String, XUserInfo> deletedUsers = getField("deletedUsers");
        assertTrue(deletedUsers.containsKey("user_d"), "A user genuinely absent from the source snapshot must still be marked deleted");
        assertEquals(ISHIDDEN, cached.getIsVisible());
    }

    @Test
    public void testR5_reconcile_alreadyHidden_matchingGuid_mustNotAutoRestore() throws Exception {
        String guid = "66666666-6666-6666-6666-666666666666";
        XGroupInfo cached = group("group_e", guid, SYNC_SOURCE_VALUE, ISHIDDEN); // hidden from a prior cycle
        // Match a real production record's stored attrs exactly (including original_name, which a
        // live upsert always sets -- see buildGroupAttributes()), so this exercises the true
        // nothing-changed no-op path rather than the separate attrs-changed self-heal path.
        cached.getOtherAttrsMap().put(UgsyncCommonConstants.ORIGINAL_NAME, "group_e");
        cached.setOtherAttributes(JsonUtils.objectToJson(cached.getOtherAttrsMap()));
        this.<Map<String, XGroupInfo>>getField("groupCache").put("group_e", cached);
        this.<Map<String, String>>getField("groupNameMap").put(guid, "group_e");

        Map<String, Map<String, String>> sourceGroups = new HashMap<>();
        sourceGroups.put(guid, attrs(guid, "group_e"));

        invoke("computeGroupDelta", Map.class, sourceGroups);

        Map<String, XGroupInfo> deltaGroups = getField("deltaGroups");
        assertFalse(deltaGroups.containsKey("group_e"), "An already-hidden group with no actual attribute change must" +
                "not be pushed as an update, but deltaGroups=" + deltaGroups.keySet());
        assertEquals(ISHIDDEN, cached.getIsVisible(),
                "Sync must never auto-restore a hidden group just because the source still reports it "
                        + "present -- visibility changes are admin-owned (e.g. the UI's Set Visibility action) "
                        + "once a row has been hidden");
    }

    @Test
    public void testR6_perRecordDelete_userMatchingGuid_isMarked() throws Exception {
        String guid = "77777777-7777-7777-7777-777777777777";
        XUserInfo cached = liveUser("user_a", guid);
        this.<Map<String, XUserInfo>>getField("userCache").put("user_a", cached);

        Set<String> deletedUserFullNames = new HashSet<>();
        deletedUserFullNames.add(guid);

        invoke("markDeletedUsersByFullName", Set.class, deletedUserFullNames);

        Map<String, XUserInfo> deletedUsers = getField("deletedUsers");
        assertEquals(ISHIDDEN, cached.getIsVisible());
        assertTrue(deletedUsers.containsKey("user_a"), "markDeletedUsersByFullName failed to match user_a by its own full_name GUID");
    }

    @Test
    public void testR7_perRecordDelete_userGuidCaseDrift_doesNotMatch() throws Exception {
        String cachedGuid = "88888888-BBBB-CCCC-DDDD-EEEEEEEEEEEE";
        String deletedGuid = cachedGuid.toLowerCase();

        XUserInfo cached = liveUser("user_b", cachedGuid);
        this.<Map<String, XUserInfo>>getField("userCache").put("user_b", cached);

        Set<String> deletedUserFullNames = new HashSet<>();
        deletedUserFullNames.add(deletedGuid);

        invoke("markDeletedUsersByFullName", Set.class, deletedUserFullNames);

        Map<String, XUserInfo> deletedUsers = getField("deletedUsers");
        assertFalse(deletedUsers.containsKey("user_b"),
                "Per-record delete matches full_name exactly; EntraID lowercases the GUID before calling the sink");
        assertEquals(ISVISIBLE, cached.getIsVisible());
    }

    @Test
    public void testR8_perRecordDelete_groupMatchingGuid_isMarked() throws Exception {
        String guid = "99999999-CCCC-DDDD-EEEE-FFFFFFFFFFFF";
        XGroupInfo cached = liveGroup("group_x", guid);
        this.<Map<String, XGroupInfo>>getField("groupCache").put("group_x", cached);

        Set<String> deletedGroupFullNames = new HashSet<>();
        deletedGroupFullNames.add(guid);

        invoke("markDeletedGroupsByFullName", Set.class, deletedGroupFullNames);

        Map<String, XGroupInfo> deletedGroups = getField("deletedGroups");
        assertEquals(ISHIDDEN, cached.getIsVisible());
        assertTrue(deletedGroups.containsKey("group_x"), "markDeletedGroupsByFullName failed to match group_x by its own full_name GUID");
    }

    @Test
    public void testR9_perRecordDelete_otherSyncSource_isIgnored() throws Exception {
        String guid = "AAAAAAAA-DDDD-EEEE-FFFF-000000000000";
        // Cached under a DIFFERENT sync source than currentSyncSource ("EntraID"); its GUID
        // happens to be in this cycle's @removed set. The sync_source on otherAttributes must veto it.
        XGroupInfo cached = group("group_y", guid, OTHER_SYNC_SOURCE, ISVISIBLE);
        this.<Map<String, XGroupInfo>>getField("groupCache").put("group_y", cached);

        Set<String> deletedGroupFullNames = new HashSet<>();
        deletedGroupFullNames.add(guid);

        invoke("markDeletedGroupsByFullName", Set.class, deletedGroupFullNames);

        Map<String, XGroupInfo> deletedGroups = getField("deletedGroups");
        assertFalse(deletedGroups.containsKey("group_y"), "A record from a different sync source must not be hidden by this source's per-record delete pass");
        assertEquals(ISVISIBLE, cached.getIsVisible());
    }

    @Test
    public void reconcileSweep_hideThenAddOrUpdate_restoresVisibilityInSameCycle() throws Exception {
        String guid = "BBBBBBBB-1111-1111-1111-111111111111";
        XGroupInfo cached = liveGroup("group_b", guid);
        this.<Map<String, XGroupInfo>>getField("groupCache").put("group_b", cached);
        this.<Map<String, String>>getField("groupNameMap").put(guid, "group_b");

        Map<String, Map<String, String>> sourceGroups = new HashMap<>();
        sourceGroups.put(guid, attrs(guid, "group_b"));

        // Reproduce addOrUpdateUsersGroups' production call order: the delete pass (and, in
        // production, its persist-to-Admin POST) runs BEFORE the add/update pass that restores
        // visibility -- all within the SAME cycle (unlike testR1, which starts from stale drift).
        invoke("computeDeletedGroups", Map.class, sourceGroups);
        Map<String, XGroupInfo> deletedGroups = getField("deletedGroups");
        this.<Map<String, XGroupInfo>>getField("groupCache").putAll(deletedGroups);

        invoke("computeGroupDelta", Map.class, sourceGroups);

        Map<String, XGroupInfo> deltaGroups = getField("deltaGroups");
        assertTrue(deltaGroups.containsKey("group_b"),
                "group_b was hidden by computeDeletedGroups() and is present in this cycle's own "
                        + "source snapshot, so computeGroupDelta() must restore it -- deltaGroups was: " + deltaGroups.keySet());
        assertEquals(ISVISIBLE, cached.getIsVisible());
    }

    @Test
    public void reconcileSweep_sameGuidDifferentCase_isTreatedAsAbsent() throws Exception {
        String cachedGuid = "CCCCCCCC-BBBB-CCCC-DDDD-EEEEEEEEEEEE";
        String sourceGuid = cachedGuid.toLowerCase();

        XGroupInfo cached = liveGroup("group_c", cachedGuid);
        this.<Map<String, XGroupInfo>>getField("groupCache").put("group_c", cached);

        Map<String, Map<String, String>> sourceGroups = new HashMap<>();
        sourceGroups.put(sourceGuid, attrs(sourceGuid, "group_c"));

        invoke("computeDeletedGroups", Map.class, sourceGroups);

        Map<String, XGroupInfo> deletedGroups = getField("deletedGroups");
        assertTrue(deletedGroups.containsKey("group_c"),
                "Delete matching stays an exact key compare; a case-only GUID difference is absent from the snapshot");
        assertEquals(ISHIDDEN, cached.getIsVisible());
    }

    @Test
    public void perRecordDelete_groupNotInDeleteSet_isNotMarked() throws Exception {
        String guid = "DDDDDDDD-6666-6666-6666-666666666666";
        XGroupInfo cached = liveGroup("group_e2", guid);
        this.<Map<String, XGroupInfo>>getField("groupCache").put("group_e2", cached);

        Set<String> deletedGroupFullNames = new HashSet<>();
        deletedGroupFullNames.add("EEEEEEEE-7777-7777-7777-777777777777"); // a different group entirely

        invoke("markDeletedGroupsByFullName", Set.class, deletedGroupFullNames);

        Map<String, XGroupInfo> deletedGroups = getField("deletedGroups");
        assertTrue(deletedGroups.isEmpty(), "A group not named in the @removed set must not be marked deleted");
        assertEquals(ISVISIBLE, cached.getIsVisible());
    }

    @Test
    public void reconcileSweep_alreadyHiddenGroup_isNotReMarkedButStaysHidden() throws Exception {
        String guid = "FFFFFFFF-1111-1111-1111-111111111111";
        XGroupInfo cached = group("group_f", guid, SYNC_SOURCE_VALUE, ISHIDDEN); // already hidden from a prior cycle
        this.<Map<String, XGroupInfo>>getField("groupCache").put("group_f", cached);

        Map<String, Map<String, String>> sourceGroups = new HashMap<>(); // still absent from source

        invoke("computeDeletedGroups", Map.class, sourceGroups);

        Map<String, XGroupInfo> deletedGroups = getField("deletedGroups");
        assertTrue(deletedGroups.isEmpty(), "An already-hidden group must not be re-added to deletedGroups (it hits the 'already marked' branch instead)");
        assertEquals(ISHIDDEN, cached.getIsVisible());
    }

    @Test
    public void reconcileSweep_mixedBatch_onlyTrulyAbsentGroupIsMarkedDeleted() throws Exception {
        String presentGuid = "01010101-DDDD-EEEE-FFFF-000000000000";
        String absentGuid = "02020202-EEEE-FFFF-0000-111111111111";

        XGroupInfo presentGroup = liveGroup("group_present", presentGuid);
        XGroupInfo absentGroup = liveGroup("group_absent", absentGuid);
        this.<Map<String, XGroupInfo>>getField("groupCache").put("group_present", presentGroup);
        this.<Map<String, XGroupInfo>>getField("groupCache").put("group_absent", absentGroup);

        Map<String, Map<String, String>> sourceGroups = new HashMap<>();
        sourceGroups.put(presentGuid, attrs(presentGuid, "group_present"));

        invoke("computeDeletedGroups", Map.class, sourceGroups);

        Map<String, XGroupInfo> deletedGroups = getField("deletedGroups");
        assertFalse(deletedGroups.containsKey("group_present"), "group_present must not be hidden");
        assertTrue(deletedGroups.containsKey("group_absent"), "group_absent (genuinely gone) must be hidden");
        assertEquals(ISVISIBLE, presentGroup.getIsVisible());
        assertEquals(ISHIDDEN, absentGroup.getIsVisible());
    }

    @Test
    public void reconcileSweep_nullKeyInSourceGroups_doesNotThrow() throws Exception {
        String guid = "04040404-0000-1111-2222-333333333333";
        XGroupInfo cached = liveGroup("group_h", guid);
        this.<Map<String, XGroupInfo>>getField("groupCache").put("group_h", cached);

        Map<String, Map<String, String>> sourceGroups = new HashMap<>();
        sourceGroups.put(guid, attrs(guid, "group_h"));
        sourceGroups.put(null, attrs("ignored", "malformed_record")); // malformed upstream record

        invoke("computeDeletedGroups", Map.class, sourceGroups); // must not throw

        Map<String, XGroupInfo> deletedGroups = getField("deletedGroups");
        assertTrue(deletedGroups.isEmpty());
    }

    @Test
    public void perRecordDelete_nullKeyInDeletedSet_doesNotThrow() throws Exception {
        String guid = "05050505-2323-4545-6767-898989898989";
        XGroupInfo cached = liveGroup("group_i", guid);
        this.<Map<String, XGroupInfo>>getField("groupCache").put("group_i", cached);

        Set<String> deletedGroupFullNames = new HashSet<>();
        deletedGroupFullNames.add(null); // malformed @removed entry
        deletedGroupFullNames.add("06060606-2222-3333-4444-555555555555");

        invoke("markDeletedGroupsByFullName", Set.class, deletedGroupFullNames); // must not throw

        Map<String, XGroupInfo> deletedGroups = getField("deletedGroups");
        assertTrue(deletedGroups.isEmpty());
        assertEquals(ISVISIBLE, cached.getIsVisible());
    }
}
