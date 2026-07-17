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
        // Group pull: on a full sync in DIRECT membership mode, fetch groups WITH inline
        // membership in one delta enumeration (avoids one /members call per group -- the
        // first-sync bottleneck). The inline path is valid ONLY for DIRECT mode: Graph's
        // $select=members returns direct members only, with no transitive option on the
        // delta endpoint. For TRANSITIVE mode we must use the per-group /transitiveMembers
        // path even on full sync, otherwise a full sync would return direct members while
        // incremental cycles return transitive members -- inconsistent membership for the
        // same group. inlineMembers is non-null only when the inline path is used.
        boolean useInlineMembers = fullSync && config.getMembershipMode() == MembershipMode.DIRECT;
        DeltaPage<GraphGroup> groupPage;
        GroupMembershipPage inlineMembers = null;
        if (useInlineMembers) {
            inlineMembers = orEmpty(graphClient.getGroupDeltaWithMembers(groupToken));
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
                    inlineMembers = graphClient.getGroupDeltaWithMembers(null);
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
            // Membership: when the inline path was used (DIRECT full sync), read the member
            // set already merged across pages by the client (no per-group call). Otherwise
            // (incremental cycle, or TRANSITIVE mode) fetch the complete current member set
            // for this group. Either way the sink receives the full member set to diff.
            Set<String> memberGuids = null;
            if (inlineMembers != null) {
                memberGuids = inlineMembers.getMembers(group.getId());
            } else {
                try {
                    memberGuids = fetchMemberGuids(group.getId(), mode);
                } catch (GraphClientException e) {
                    // A single group that 404s mid-sync (e.g. deleted in Entra between the
                    // group-list pull and this member fetch) must not abort the whole cycle.
                    // Skip its membership this cycle and reconcile next cycle. Systemic
                    // failures (auth, throttling exhausted, network) carry a non-404 status
                    // or none, and are rethrown so a broken connection still fails loudly.
                    if (e.getHttpStatus() == 404) {
                        LOG.warn("EntraID: skipping membership for group {} (not found during member fetch): {}", group.getId(), e.getMessage());
                        membershipFetchSkips++;
                        memberGuids = Collections.emptySet();
                    } else {
                        throw e;   // systemic failure -> fail the cycle
                    }
                }
            }
            sourceGroupUsers.put(group.getId(), memberGuids);
            groupsSynced++;
        }
        LOG.debug("EntraID snapshot: users={}, groups={}, deletedUsers={}, deletedGroups={}, fullSync={}, reconcileSweep={}",
                sourceUsers.size(), sourceGroups.size(), deletedUsers.size(), deletedGroups.size(), fullSync, reconcileSweep);
        // 5. Apply to the sink; advance delta tokens only on full success.
        try {
            sink.addOrUpdateUsersGroups(sourceGroups, sourceUsers, sourceGroupUsers, reconcileSweep);
            // Per-record deletions (normal cycles only; the sweep computes deletes itself).
            if (!reconcileSweep && (!deletedUsers.isEmpty() || !deletedGroups.isEmpty())) {
                sink.deleteUsersAndGroups(deletedUsers, deletedGroups);
            }
            userDeltaLink = userPage.getDeltaLink();
            groupDeltaLink = groupPage.getDeltaLink();
            firstSyncDone = true;
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
        // Contract-critical keys (read by the sink for add/update/delete reconciliation).
        attrs.put(UgsyncCommonConstants.ORIGINAL_NAME, userName);
        attrs.put(UgsyncCommonConstants.FULL_NAME, user.getId());     // GUID = stable identity
        attrs.put(UgsyncCommonConstants.SYNC_SOURCE, currentSyncSource);
        // NOTE: LDAP_URL intentionally NOT set (mirrors Unix; enables delete reconciliation).
        // Extra attributes (stored as otherAttributes; affect modify-detection).
        attrs.put("cloud_id", user.getId());
        if (StringUtils.isNotBlank(user.getDisplayName())) {
            attrs.put("displayName", user.getDisplayName());
        }
        if (StringUtils.isNotBlank(user.getMail())) {
            attrs.put("email", user.getMail());
        }
        attrs.putAll(user.getAdditionalAttributes());
        return attrs;
    }

    private Map<String, String> buildGroupAttributes(GraphGroup group, String groupName) {
        Map<String, String> attrs = new HashMap<>();
        attrs.put(UgsyncCommonConstants.ORIGINAL_NAME, groupName);
        attrs.put(UgsyncCommonConstants.FULL_NAME, group.getId());
        attrs.put(UgsyncCommonConstants.SYNC_SOURCE, currentSyncSource);
        attrs.put("cloud_id", group.getId());
        if (StringUtils.isNotBlank(group.getDisplayName())) {
            attrs.put("displayName", group.getDisplayName());
        }
        attrs.putAll(group.getAdditionalAttributes());
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

    private static <T> DeltaPage<T> orEmpty(DeltaPage<T> page) {
        return page != null ? page : new DeltaPage<>(Collections.emptyList(), null);
    }

    private static GroupMembershipPage orEmpty(GroupMembershipPage page) {
        return page != null ? page : new GroupMembershipPage(Collections.emptyList(), new HashMap<>(), null, false);
    }
}
