/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ranger.entraid;

import org.apache.commons.lang3.StringUtils;
import org.apache.ranger.entraid.graph.EntraIdGraphClient;
import org.apache.ranger.entraid.graph.EntraIdGraphClientImpl;
import org.apache.ranger.entraid.graph.EntraIdGraphConfig;
import org.apache.ranger.entraid.graph.EntraIdGraphConfigLoader;
import org.apache.ranger.entraid.graph.GraphClientException;
import org.apache.ranger.ugsyncutil.model.EntraIdSyncSourceInfo;
import org.apache.ranger.ugsyncutil.model.UgsyncAuditInfo;
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
import org.apache.ranger.usergroupsync.UserGroupSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

public class EntraIdUserGroupSource implements UserGroupSource {
    private static final Logger LOG = LoggerFactory.getLogger(EntraIdUserGroupSource.class);
    private static final String GRAPH_ATTR_MAIL = "mail";
    private static final String GRAPH_ATTR_UPN = "userPrincipalName";

    private final UserGroupSyncConfig ugSyncConfig;

    private EntraIdGraphConfig config;
    private EntraIdGraphClient graphClient;
    private String currentSyncSource;

    // Delta state across cycles (in-process lifetime).
    private String userDeltaLink;
    private String groupDeltaLink;
    private boolean firstSyncDone;
    // Membership cache (group GUID -> user GUIDs). DIRECT: seeds incremental members@delta merges.
    // TRANSITIVE: tracks known groups so every cycle can refresh /transitiveMembers for all of them.
    private final Map<String, Set<String>> groupMembersCache = new HashMap<>();
    // Group attribute cache (group GUID -> sink attrs) so TRANSITIVE membership-only refreshes
    // can still upsert groups that did not appear in this cycle's group delta page.
    private final Map<String, Map<String, String>> groupAttrsCache = new HashMap<>();

    // Delete-cycle cadence, mirroring LdapUserGroupBuilder / UnixUserGroupBuilder.
    private int deleteCycles;

    public EntraIdUserGroupSource() {
        this(UserGroupSyncConfig.getInstance());
    }

    EntraIdUserGroupSource(UserGroupSyncConfig ugSyncConfig) {
        this.ugSyncConfig = ugSyncConfig;
    }

    EntraIdUserGroupSource(UserGroupSyncConfig ugSyncConfig, EntraIdGraphConfig config, EntraIdGraphClient graphClient, String currentSyncSource) {
        this.ugSyncConfig = ugSyncConfig;
        this.config = config;
        this.graphClient = graphClient;
        this.currentSyncSource = currentSyncSource;
        this.deleteCycles = 1;
    }

    @Override
    public void init() throws Throwable {
        deleteCycles = 1;
        currentSyncSource = ugSyncConfig.getCurrentSyncSource();
        this.config = new EntraIdGraphConfigLoader(ugSyncConfig).load();
        this.graphClient = new EntraIdGraphClientImpl();
        this.graphClient.init(config);
        LOG.info("EntraIdUserGroupSource initialized: syncSource={}, graph={}, authMode={}, membershipMode={}", currentSyncSource, config.getGraphBaseUrl(), config.getAuthMode(), config.getMembershipMode());
    }

    @Override
    public boolean isChanged() {
        // Graph delta determines actual change at sync time; always attempt a cycle.
        return true;
    }

    @Override
    public void updateSink(UserGroupSink sink) throws Throwable {
        // 1. Decide cycle type.
        //
        //    Normal cycle (the common case): incremental delta. Adds/updates ride the
        //    delta; deletions are applied per-record from Graph's @removed markers via
        //    sink.deleteUsersAndGroups() -- no full pull required.
        //
        //    Safety-net cycle (rare, every reconcileFrequency-th delete cycle): a full
        //    snapshot with computeDeletes=true, to converge on any deletions Graph's
        //    delta may have missed (documented @removed gaps, delta-token expiry).
        boolean deletesEnabled = ugSyncConfig.isUserSyncDeletesEnabled();
        boolean reconcileSweep = false;
        int membershipFetchSkips = 0;
        if (deletesEnabled && deleteCycles >= ugSyncConfig.getUserSyncDeletesFrequency()) {
            deleteCycles = 1;
            reconcileSweep = true;
            LOG.debug("Full reconciliation sweep enabled for this sync cycle");
        }
        if (deletesEnabled) {
            deleteCycles++;
        }
        // A full pull is needed for the first cycle and for a reconciliation sweep.
        boolean fullSync = reconcileSweep || !firstSyncDone;
        String userToken = fullSync ? null : userDeltaLink;
        String groupToken = fullSync ? null : groupDeltaLink;

        // 2. Pull users and groups from Graph.
        DeltaPage<GraphUser> userPage = orEmpty(graphClient.getUserDelta(userToken));
        // Group pull: in DIRECT mode, always use groups/delta with $select=...,members so
        // membership-only changes appear under members@delta. Full sync seeds from an empty
        // set; incremental sync passes groupMembersCache so the client merges adds/removes.
        // TRANSITIVE mode cannot use inline members (Graph returns direct members only), so it
        // uses getGroupDelta for group objects and refreshes /transitiveMembers for every
        // known group each cycle (not only groups on this delta page).
        boolean useInlineMembers = config.getMembershipMode() == MembershipMode.DIRECT;
        DeltaPage<GraphGroup> groupPage;
        GroupMembershipPage inlineMembers = null;
        if (useInlineMembers) {
            Map<String, Set<String>> priorMembers = fullSync ? null : groupMembersCache;
            inlineMembers = orEmpty(graphClient.getGroupDeltaWithMembers(groupToken, priorMembers));
            groupPage = new DeltaPage<>(inlineMembers.getGroups(), inlineMembers.getDeltaLink(), inlineMembers.isResynced());
        } else {
            groupPage = orEmpty(graphClient.getGroupDelta(groupToken));
        }
        // If the client had to fall back to a full resync (delta link expired/invalidated),
        // promote this cycle to a reconciliation sweep so deletions are computed by the
        // sink's snapshot-diff (a resync does not replay @removed tombstones for objects
        // deleted during the gap).
        //
        // CRITICAL: computeDeletes diffs the FULL cache against the passed snapshot, and a
        // single computeDeletes flag covers BOTH users and groups. So if only one of the two
        // delta links expired, the other is still an *incremental* page -- passing it with
        // computeDeletes=true would mark every cached object absent from that small page as
        // deleted (mass false-deletion). Therefore, if either resynced, force a full pull of
        // the other as well so both snapshots are complete before computing deletes.
        if (userPage.isResynced() || groupPage.isResynced()) {
            LOG.warn("Delta resync occurred; forcing a full pull of both users and groups and treating this cycle as a reconciliation sweep");
            reconcileSweep = true;
            if (!userPage.isResynced()) {
                userPage = orEmpty(graphClient.getUserDelta(null));
            }
            if (!groupPage.isResynced()) {
                // Force a full group pull, keeping membership and attributes consistent for
                // the snapshot-diff. Use inline membership only in DIRECT mode (see above);
                // in TRANSITIVE mode fetch groups only and resolve members per-group below.
                if (config.getMembershipMode() == MembershipMode.DIRECT) {
                    inlineMembers = graphClient.getGroupDeltaWithMembers(null, null);
                    groupPage = new DeltaPage<>(inlineMembers.getGroups(), inlineMembers.getDeltaLink(), inlineMembers.isResynced());
                } else {
                    groupPage = orEmpty(graphClient.getGroupDelta(null));
                }
            }
        }
        // 3. Build the user snapshot (upserts) and the per-record user delete map,
        //    both keyed by GUID.
        String nameAttr = chooseUserNameAttribute();
        Map<String, Map<String, String>> sourceUsers = new HashMap<>();
        Map<String, Map<String, String>> deletedUsers = new HashMap<>();
        long usersSynced = 0;
        for (DeltaEntry<GraphUser> entry : userPage.getEntries()) {
            GraphUser user = entry.getValue();
            if (user == null || StringUtils.isBlank(user.getId())) {
                continue;
            }
            if (entry.isRemoved()) {
                // @removed on /users/delta means the user was deleted/disabled in the
                // directory. Collect for per-record deletion (keyed by GUID, with the
                // minimal attrs the sink's scoping gate matches on). On a reconcile
                // sweep we let the full snapshot-diff compute deletes instead.
                if (!reconcileSweep) {
                    deletedUsers.put(user.getId(), buildDeleteAttributes(user.getId()));
                }
                continue;
            }
            String userName = resolveUserName(user, nameAttr);
            if (StringUtils.isBlank(userName)) {
                continue;
            }
            if (config.isSkipDisabledUsers() && !user.isAccountEnabled()) {
                // Operator opt-in: treat a disabled account as absent, the same way an LDAP
                // search filter can silently exclude one -- not a delete/hide signal, just an
                // exclusion. The existing reconcile-sweep snapshot-diff picks up the resulting
                // absence and hides it; re-enabling reverses that the same way any other
                // reappearance would (no auto-restore, consistent with every other source).
                continue;
            }
            sourceUsers.put(user.getId(), buildUserAttributes(user, userName));
            usersSynced++;
        }

        // 4. Build the group snapshot, group->member-GUID map, and per-record group delete map, all keyed by GUID.
        Map<String, Map<String, String>> sourceGroups = new HashMap<>();
        Map<String, Set<String>> sourceGroupUsers = new HashMap<>();
        Map<String, Map<String, String>> deletedGroups = new HashMap<>();
        long groupsSynced = 0;
        MembershipMode mode = config.getMembershipMode();
        for (DeltaEntry<GraphGroup> entry : groupPage.getEntries()) {
            GraphGroup group = entry.getValue();
            if (group == null || StringUtils.isBlank(group.getId())) {
                continue;
            }
            if (entry.isRemoved()) {
                if (!reconcileSweep) {
                    deletedGroups.put(group.getId(), buildDeleteAttributes(group.getId()));
                }
                continue;
            }
            String groupName = group.getDisplayName();
            if (StringUtils.isBlank(groupName)) {
                continue;
            }
            sourceGroups.put(group.getId(), buildGroupAttributes(group, groupName));
            // Membership: DIRECT inline path returns the full member set (full pull or
            // prior-cache + members@delta merge). TRANSITIVE defers member fetch to the
            // known-group refresh below so delta-absent groups are covered too.
            if (inlineMembers != null) {
                sourceGroupUsers.put(group.getId(), inlineMembers.getMembers(group.getId()));
            }
            groupsSynced++;
        }
        if (!useInlineMembers) {
            // TRANSITIVE: nested membership changes often do not surface the parent on
            // /groups/delta. On incremental cycles, re-fetch /transitiveMembers for every
            // known group (including those absent from this page). On a full/reconcile
            // snapshot, ONLY refresh groups in sourceGroups — never reinstate from
            // groupAttrsCache, or a vanished group would defeat sink computeDeletes.
            boolean reinstateFromCache = !fullSync && !reconcileSweep;
            membershipFetchSkips += refreshTransitiveMembershipForKnownGroups(sourceGroups, sourceGroupUsers, deletedGroups.keySet(), mode, reinstateFromCache);
            groupsSynced = sourceGroups.size();
        }
        LOG.debug("EntraID snapshot: users={}, groups={}, deletedUsers={}, deletedGroups={}, fullSync={}, reconcileSweep={}",
                sourceUsers.size(), sourceGroups.size(), deletedUsers.size(), deletedGroups.size(), fullSync, reconcileSweep);
        // 5. Apply to the sink; advance delta tokens only on full success.
        try {
            // Lowercase GUIDs on the copies passed to the sink. Delta caches stay keyed by the Graph id.
            sink.addOrUpdateUsersGroups(normalizeAttrMaps(sourceGroups), normalizeAttrMaps(sourceUsers),
                    normalizeMembership(sourceGroupUsers), reconcileSweep);
            // Per-record deletions (normal cycles only; the sweep computes deletes itself).
            if (!reconcileSweep && (!deletedUsers.isEmpty() || !deletedGroups.isEmpty())) {
                sink.deleteUsersAndGroups(normalizeAttrMaps(deletedUsers), normalizeAttrMaps(deletedGroups));
            }
            userDeltaLink = userPage.getDeltaLink();
            groupDeltaLink = groupPage.getDeltaLink();
            firstSyncDone = true;
            updateGroupMembersCache(sourceGroupUsers, deletedGroups.keySet(), fullSync || reconcileSweep);
            updateGroupAttrsCache(sourceGroups, deletedGroups.keySet(), fullSync || reconcileSweep);
            LOG.info("EntraID sync cycle complete: users+~{}, groups+~{}, usersDeleted~{}, groupsDeleted~{}, membershipSkips={}, fullSync={}, reconcileSweep={}",
                    sourceUsers.size(), sourceGroups.size(), deletedUsers.size(), deletedGroups.size(), membershipFetchSkips, fullSync, reconcileSweep);
        } catch (Throwable t) {
            // Do not advance tokens; next cycle retries with the same state.
            LOG.error("Failed to update ranger admin. Will retry in next sync cycle!!", t);
        }
        // 6. Emit audit (best-effort, like LDAP/Unix).
        try {
            sink.postUserGroupAuditInfo(buildAuditInfo(!fullSync, usersSynced, groupsSynced));
        } catch (Throwable t) {
            LOG.error("sink.postUserGroupAuditInfo failed with exception: ", t);
        }
    }

    private Map<String, String> buildDeleteAttributes(String guid) {
        Map<String, String> attrs = new HashMap<>();
        attrs.put(UgsyncCommonConstants.FULL_NAME, guid);
        attrs.put(UgsyncCommonConstants.SYNC_SOURCE, currentSyncSource);
        return attrs;
    }

    private String chooseUserNameAttribute() {
        Set<String> selected = config.getUserSelectAttrs();
        if (selected != null && selected.contains(GRAPH_ATTR_MAIL) && !selected.contains(GRAPH_ATTR_UPN)) {
            return GRAPH_ATTR_MAIL;
        }
        return GRAPH_ATTR_UPN;
    }

    private String resolveUserName(GraphUser user, String nameAttr) {
        String name = GRAPH_ATTR_MAIL.equals(nameAttr) ? user.getMail() : user.getUserPrincipalName();
        if (StringUtils.isBlank(name)) {
            name = GRAPH_ATTR_MAIL.equals(nameAttr) ? user.getUserPrincipalName() : user.getMail();
        }
        return name;
    }

    private Set<String> fetchMemberGuids(String groupId, MembershipMode mode) throws Throwable {
        Set<String> memberGuids = new HashSet<>();
        List<GraphMemberRef> refs = graphClient.getGroupMembers(groupId, mode);
        for (GraphMemberRef ref : refs) {
            // Only user members map to Ranger group members. In DIRECT mode, nested
            // GROUP members are not expanded (use TRANSITIVE for flattening).
            if (ref.getType() == MemberType.USER && ref.getId() != null && !ref.getId().isEmpty()) {
                memberGuids.add(ref.getId());
            }
        }
        return memberGuids;
    }

    private Map<String, String> buildUserAttributes(GraphUser user, String userName) {
        Map<String, String> attrs = new HashMap<>();
        // Extra Graph attrs first; contract keys below must win (sink matches full_name/GUID).
        attrs.putAll(user.getAdditionalAttributes());
        if (StringUtils.isNotBlank(user.getDisplayName())) {
            attrs.put("displayName", user.getDisplayName());
        }
        if (StringUtils.isNotBlank(user.getMail())) {
            attrs.put("email", user.getMail());
        }
        attrs.put("cloud_id", user.getId());
        // Contract-critical keys (read by the sink for add/update/delete reconciliation).
        attrs.put(UgsyncCommonConstants.ORIGINAL_NAME, userName);
        attrs.put(UgsyncCommonConstants.FULL_NAME, user.getId());     // GUID = stable identity
        attrs.put(UgsyncCommonConstants.SYNC_SOURCE, currentSyncSource);
        // NOTE: LDAP_URL intentionally NOT set (mirrors Unix; enables delete reconciliation).
        return attrs;
    }

    private Map<String, String> buildGroupAttributes(GraphGroup group, String groupName) {
        Map<String, String> attrs = new HashMap<>();
        attrs.putAll(group.getAdditionalAttributes());
        if (StringUtils.isNotBlank(group.getDisplayName())) {
            attrs.put("displayName", group.getDisplayName());
        }
        attrs.put("cloud_id", group.getId());
        attrs.put(UgsyncCommonConstants.ORIGINAL_NAME, groupName);
        attrs.put(UgsyncCommonConstants.FULL_NAME, group.getId());
        attrs.put(UgsyncCommonConstants.SYNC_SOURCE, currentSyncSource);
        return attrs;
    }

    private UgsyncAuditInfo buildAuditInfo(boolean incremental, long usersSynced, long groupsSynced) {
        EntraIdSyncSourceInfo sourceInfo = new EntraIdSyncSourceInfo();
        sourceInfo.setTenantId(config.getTenantId());
        sourceInfo.setGraphBaseUrl(config.getGraphBaseUrl());
        sourceInfo.setAuthMode(String.valueOf(config.getAuthMode()));
        sourceInfo.setMembershipMode(String.valueOf(config.getMembershipMode()));
        sourceInfo.setIncrementalSync(String.valueOf(incremental));
        sourceInfo.setGroupFilter(config.getGroupFilter());
        // Totals are authoritatively set by the sink (cache sizes + deletes) when it
        // recognizes the EntraID source; these are a reasonable fallback otherwise.
        sourceInfo.setTotalUsersSynced(usersSynced);
        sourceInfo.setTotalGroupsSynced(groupsSynced);
        UgsyncAuditInfo auditInfo = new UgsyncAuditInfo();
        auditInfo.setSyncSource(currentSyncSource);
        auditInfo.setEntraIdSyncSourceInfo(sourceInfo);
        return auditInfo;
    }

    private int refreshTransitiveMembershipForKnownGroups(Map<String, Map<String, String>> sourceGroups,
                                                          Map<String, Set<String>> sourceGroupUsers,
                                                          Set<String> deletedGroupIds,
                                                          MembershipMode mode,
                                                          boolean reinstateFromCache) throws Throwable {
        Set<String> toRefresh = new HashSet<>(sourceGroups.keySet());
        if (reinstateFromCache) {
            toRefresh.addAll(groupAttrsCache.keySet());
            if (deletedGroupIds != null) {
                toRefresh.removeAll(deletedGroupIds);
            }
        }
        int skips = 0;
        for (String groupId : toRefresh) {
            if (!sourceGroups.containsKey(groupId)) {
                // Only reachable when reinstateFromCache is true.
                Map<String, String> cachedAttrs = groupAttrsCache.get(groupId);
                if (cachedAttrs == null || cachedAttrs.isEmpty()) {
                    continue;
                }
                sourceGroups.put(groupId, cachedAttrs);
            }
            try {
                sourceGroupUsers.put(groupId, fetchMemberGuids(groupId, mode));
            } catch (GraphClientException e) {
                // A single group that 404s must not abort the whole cycle.
                if (e.getHttpStatus() == 404) {
                    LOG.warn("EntraID: skipping membership for group {} (not found during member fetch): {}", groupId, e.getMessage());
                    skips++;
                    sourceGroupUsers.put(groupId, Collections.emptySet());
                } else {
                    throw e;
                }
            }
        }
        return skips;
    }

    private void updateGroupMembersCache(Map<String, Set<String>> sourceGroupUsers, Set<String> deletedGroupIds, boolean replaceAll) {
        if (replaceAll) {
            groupMembersCache.clear();
        }
        for (Map.Entry<String, Set<String>> e : sourceGroupUsers.entrySet()) {
            Set<String> members = e.getValue();
            groupMembersCache.put(e.getKey(), (members == null) ? new HashSet<>() : new HashSet<>(members));
        }
        if (deletedGroupIds != null) {
            for (String id : deletedGroupIds) {
                groupMembersCache.remove(id);
            }
        }
    }

    private void updateGroupAttrsCache(Map<String, Map<String, String>> sourceGroups, Set<String> deletedGroupIds, boolean replaceAll) {
        if (replaceAll) {
            groupAttrsCache.clear();
        }
        for (Map.Entry<String, Map<String, String>> e : sourceGroups.entrySet()) {
            Map<String, String> attrs = e.getValue();
            groupAttrsCache.put(e.getKey(), (attrs == null) ? new HashMap<>() : new HashMap<>(attrs));
        }
        if (deletedGroupIds != null) {
            for (String id : deletedGroupIds) {
                groupAttrsCache.remove(id);
            }
        }
    }

    private static String normalizeGuid(String id) {
        return id == null ? null : id.toLowerCase(Locale.ROOT);
    }

    /** Lowercase GUID keys, full_name, and cloud_id on the copies passed to the sink. */
    private static Map<String, Map<String, String>> normalizeAttrMaps(Map<String, Map<String, String>> source) {
        Map<String, Map<String, String>> normalized = new HashMap<>();

        if (source == null) {
            return normalized;
        }

        for (Map.Entry<String, Map<String, String>> entry : source.entrySet()) {
            if (entry.getKey() == null) {
                continue;
            }

            Map<String, String> attrs = entry.getValue() == null ? new HashMap<>() : new HashMap<>(entry.getValue());
            String fullName = attrs.get(UgsyncCommonConstants.FULL_NAME);

            if (fullName != null) {
                attrs.put(UgsyncCommonConstants.FULL_NAME, normalizeGuid(fullName));
            }

            String cloudId = attrs.get("cloud_id");

            if (cloudId != null) {
                attrs.put("cloud_id", normalizeGuid(cloudId));
            }

            normalized.put(normalizeGuid(entry.getKey()), attrs);
        }

        return normalized;
    }

    private static Map<String, Set<String>> normalizeMembership(Map<String, Set<String>> source) {
        Map<String, Set<String>> normalized = new HashMap<>();

        if (source == null) {
            return normalized;
        }

        for (Map.Entry<String, Set<String>> entry : source.entrySet()) {
            if (entry.getKey() == null) {
                continue;
            }

            Set<String> members = new HashSet<>();

            if (entry.getValue() != null) {
                for (String memberId : entry.getValue()) {
                    if (memberId != null) {
                        members.add(normalizeGuid(memberId));
                    }
                }
            }

            normalized.put(normalizeGuid(entry.getKey()), members);
        }

        return normalized;
    }

    private static <T> DeltaPage<T> orEmpty(DeltaPage<T> page) {
        return page != null ? page : new DeltaPage<>(Collections.emptyList(), null);
    }

    private static GroupMembershipPage orEmpty(GroupMembershipPage page) {
        return page != null ? page : new GroupMembershipPage(Collections.emptyList(), new HashMap<>(), null, false);
    }
}
