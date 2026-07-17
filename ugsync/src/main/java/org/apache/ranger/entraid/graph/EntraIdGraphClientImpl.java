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

package org.apache.ranger.entraid.graph;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.ranger.plugin.util.RangerJersey2ClientBuilder;
import org.apache.ranger.ugsyncutil.model.graph.DeltaEntry;
import org.apache.ranger.ugsyncutil.model.graph.DeltaPage;
import org.apache.ranger.ugsyncutil.model.graph.GraphGroup;
import org.apache.ranger.ugsyncutil.model.graph.GraphMemberRef;
import org.apache.ranger.ugsyncutil.model.graph.GraphUser;
import org.apache.ranger.ugsyncutil.model.graph.GroupMembershipPage;
import org.apache.ranger.ugsyncutil.model.graph.MemberType;
import org.apache.ranger.ugsyncutil.model.graph.MembershipMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.client.Client;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public final class EntraIdGraphClientImpl implements EntraIdGraphClient {
    private static final Logger LOG = LoggerFactory.getLogger(EntraIdGraphClientImpl.class);

    private static final String API_VERSION     = "v1.0";
    private static final String ODATA_NEXTLINK  = "@odata.nextLink";
    private static final String ODATA_DELTALINK = "@odata.deltaLink";
    private static final String ODATA_REMOVED   = "@removed";
    private static final String MEMBERS_DELTA   = "members@delta";

    private final ObjectMapper objectMapper = new ObjectMapper();

    private EntraIdGraphConfig config;
    private Client httpClient;
    private TokenProvider tokenProvider;

    @Override
    public void init(EntraIdGraphConfig config) throws GraphClientException {
        if (config == null) {
            throw new GraphClientException("config must not be null");
        }
        this.config = config;
        this.httpClient = RangerJersey2ClientBuilder.createClient(config.getConnectTimeoutMs(), config.getReadTimeoutMs());
        switch (config.getAuthMode()) {
            case CERTIFICATE:
                this.tokenProvider = new CertificateTokenProvider();
                break;
            case CLIENT_SECRET:
                this.tokenProvider = new ClientSecretTokenProvider();
                break;
            default:
                throw new GraphClientException("Unsupported auth mode: " + config.getAuthMode());
        }
        this.tokenProvider.init(config, httpClient);
        this.tokenProvider.getAccessToken(false);
        LOG.info("Initialized Entra Graph client: graph={}, authMode={}, membershipMode={}", config.getGraphBaseUrl(), config.getAuthMode(), config.getMembershipMode());
    }

    @Override
    public DeltaPage<GraphUser> getUserDelta(String deltaLink) throws GraphClientException {
        String baseUrl = buildInitialDeltaUrl("users", config.getUserSelectAttrs());
        try {
            return pageUsers((deltaLink != null) ? deltaLink : baseUrl);
        } catch (DeltaResyncRequiredException e) {
            if (deltaLink == null) {
                throw e;   // already a full pull; nothing to fall back to
            }
            LOG.warn("User delta link expired; restarting with a full user sync. Cause: {}", e.getMessage());
            DeltaPage<GraphUser> full = pageUsers(baseUrl);
            return new DeltaPage<>(full.getEntries(), full.getDeltaLink(), true);  // resynced=true
        }
    }

    private DeltaPage<GraphUser> pageUsers(String startUrl) throws GraphClientException {
        List<DeltaEntry<GraphUser>> entries = new ArrayList<>();
        String nextDelta = null;
        String currentUrl = startUrl;
        while (currentUrl != null) {
            JsonNode page = executeGet(currentUrl);
            for (JsonNode node : valueArray(page)) {
                boolean removed = node.has(ODATA_REMOVED);
                GraphUser user = mapUser(node);
                entries.add(new DeltaEntry<>(user, removed));
            }
            currentUrl = textOrNull(page, ODATA_NEXTLINK);
            String dl = textOrNull(page, ODATA_DELTALINK);
            if (dl != null) {
                nextDelta = dl;
            }
        }
        return new DeltaPage<>(entries, nextDelta);
    }

    @Override
    public DeltaPage<GraphGroup> getGroupDelta(String deltaLink) throws GraphClientException {
        // Group membership is fetched separately and authoritatively via getGroupMembers
        // (the full member set, which the sink diffs against its cache). The group delta
        // is therefore used only to track group objects and their attributes, so the
        // group attribute $select always applies.
        String baseUrl = buildInitialDeltaUrl("groups", config.getGroupSelectAttrs());
        try {
            return pageGroups((deltaLink != null) ? deltaLink : baseUrl);
        } catch (DeltaResyncRequiredException e) {
            if (deltaLink == null) {
                throw e;
            }
            LOG.warn("Group delta link expired; restarting with a full group sync. Cause: {}", e.getMessage());
            DeltaPage<GraphGroup> full = pageGroups(baseUrl);
            return new DeltaPage<>(full.getEntries(), full.getDeltaLink(), true);
        }
    }

    private DeltaPage<GraphGroup> pageGroups(String startUrl) throws GraphClientException {
        List<DeltaEntry<GraphGroup>> entries = new ArrayList<>();
        String nextDelta = null;
        String currentUrl = startUrl;
        while (currentUrl != null) {
            JsonNode page = executeGet(currentUrl);
            for (JsonNode node : valueArray(page)) {
                boolean removed = node.has(ODATA_REMOVED);
                GraphGroup group = mapGroup(node);
                entries.add(new DeltaEntry<>(group, removed));
            }
            currentUrl = textOrNull(page, ODATA_NEXTLINK);
            String dl = textOrNull(page, ODATA_DELTALINK);
            if (dl != null) {
                nextDelta = dl;
            }
        }
        return new DeltaPage<>(entries, nextDelta);
    }

    @Override
    public GroupMembershipPage getGroupDeltaWithMembers(String deltaLink) throws GraphClientException {
        // Full-sync path: fetch groups AND their membership inline via $select=...,members,
        // so we avoid one /members call per group. Membership for a single large group can be
        // split across delta pages, so pageGroupsWithMembers merges per group id across pages.
        String baseUrl = buildGroupDeltaUrlWithMembers();
        try {
            return pageGroupsWithMembers((deltaLink != null) ? deltaLink : baseUrl, false);
        } catch (DeltaResyncRequiredException e) {
            if (deltaLink == null) {
                throw e;
            }
            LOG.warn("Group(+members) delta link expired; restarting with a full group sync. Cause: {}", e.getMessage());
            return pageGroupsWithMembers(baseUrl, true);
        }
    }

    private GroupMembershipPage pageGroupsWithMembers(String startUrl, boolean resynced) throws GraphClientException {
        // Dedup group objects across page splits (first occurrence defines attributes) and
        // accumulate each group's user-member ids across all pages.
        Map<String, GraphGroup> groupsById = new LinkedHashMap<>();
        Map<String, Boolean> removedById = new LinkedHashMap<>();
        Map<String, Set<String>> membersById = new HashMap<>();
        String nextDelta = null;
        String currentUrl = startUrl;
        while (currentUrl != null) {
            JsonNode page = executeGet(currentUrl);
            for (JsonNode node : valueArray(page)) {
                String id = textOrNull(node, "id");
                if (id == null || id.isEmpty()) {
                    continue;
                }
                // First occurrence defines the group's attributes and removed flag.
                groupsById.putIfAbsent(id, mapGroup(node));
                removedById.putIfAbsent(id, node.has(ODATA_REMOVED));
                // Accumulate this page's chunk of members for the group.
                Set<String> members = membersById.computeIfAbsent(id, k -> new LinkedHashSet<>());
                JsonNode inline = node.get(MEMBERS_DELTA);
                if (inline != null && inline.isArray()) {
                    for (JsonNode m : inline) {
                        String memberId = textOrNull(m, "id");
                        if (memberId == null || memberId.isEmpty()) {
                            continue;
                        }
                        if (m.has(ODATA_REMOVED)) {
                            members.remove(memberId); // defensive; rare on a full pull
                        } else if (isUserMember(m)) {
                            members.add(memberId); // users only (DIRECT mode)
                        }
                    }
                }
            }
            currentUrl = textOrNull(page, ODATA_NEXTLINK);
            String dl = textOrNull(page, ODATA_DELTALINK);
            if (dl != null) {
                nextDelta = dl;
            }
        }
        List<DeltaEntry<GraphGroup>> entries = new ArrayList<>();
        for (Map.Entry<String, GraphGroup> e : groupsById.entrySet()) {
            entries.add(new DeltaEntry<>(e.getValue(), Boolean.TRUE.equals(removedById.get(e.getKey()))));
        }
        return new GroupMembershipPage(entries, membersById, nextDelta, resynced);
    }

    /**
     * True when an inline members@delta entry is a user (by @odata.type).
     */
    private boolean isUserMember(JsonNode memberNode) {
        String type = textOrNull(memberNode, "@odata.type");
        // Inline members@delta carries @odata.type (unlike the type-cast /members segment).
        // Absent type is treated as non-user to avoid mis-adding directory objects.
        return type != null && type.toLowerCase().endsWith("user");
    }

    private String buildGroupDeltaUrlWithMembers() {
        StringBuilder sb = new StringBuilder(config.getGraphBaseUrl()).append("/").append(API_VERSION).append("/groups/delta");
        // $select must include 'members' to get inline membership; always include id, and
        // carry the configured group attributes so the group objects are fully populated.
        Set<String> attrs = new LinkedHashSet<>();
        attrs.add("id");
        if (config.getGroupSelectAttrs() != null) {
            attrs.addAll(config.getGroupSelectAttrs());
        }
        attrs.add("members");
        StringBuilder query = new StringBuilder();
        query.append("$select=").append(encode(String.join(",", attrs)));
        appendAmp(query).append("$top=").append(config.getPageSize());
        return sb.append("?").append(query).toString();
    }

    @Override
    public List<GraphMemberRef> getGroupMembers(String groupId, MembershipMode mode) throws GraphClientException {
        if (groupId == null || groupId.isEmpty()) {
            throw new GraphClientException("groupId must not be empty");
        }
        String relation = (mode == MembershipMode.TRANSITIVE) ? "transitiveMembers" : "members";
        String url = config.getGraphBaseUrl() + "/" + API_VERSION + "/groups/" + groupId + "/" + relation + "/microsoft.graph.user?$select=id&$top=" + config.getPageSize();
        List<GraphMemberRef> members = new ArrayList<>();
        String currentUrl = url;
        while (currentUrl != null) {
            JsonNode page = executeGet(currentUrl);
            for (JsonNode node : valueArray(page)) {
                String id = textOrNull(node, "id");
                if (id != null && !id.isEmpty()) {
                    members.add(new GraphMemberRef(id, MemberType.USER, node.has(ODATA_REMOVED)));
                }
            }
            currentUrl = textOrNull(page, ODATA_NEXTLINK);
        }
        return members;
    }

    @Override
    public void close() throws GraphClientException {
        if (httpClient != null) {
            try {
                httpClient.close();
            } catch (Exception e) {
                throw new GraphClientException("Failed to close Graph HTTP client", e);
            } finally {
                httpClient = null;
            }
        }
    }

    private String buildInitialDeltaUrl(String entity, Set<String> selectAttrs) throws GraphClientException {
        StringBuilder sb = new StringBuilder(config.getGraphBaseUrl()).append("/").append(API_VERSION).append("/").append(entity).append("/delta");
        StringBuilder query = new StringBuilder();
        if (selectAttrs != null && !selectAttrs.isEmpty()) {
            Set<String> attrs = new LinkedHashSet<>();
            attrs.add("id");
            attrs.addAll(selectAttrs);
            query.append("$select=").append(encode(String.join(",", attrs)));
        }
        // NOTE: Microsoft Graph does not support attribute-based $filter on the /delta
        // endpoints -- the only accepted $filter is "id eq {guid}" (max 50 ids). Sending
        // any other $filter to /users/delta or /groups/delta returns 400 Request_Unsupported
        // Query. Attribute-based scoping must therefore be applied client-side; it is not
        // attempted in this phase (see proposal: group scoping is a follow-up, implemented
        // client-side following the Microsoft Entra provisioning pattern of delta + post-filter).
        /*if (config.getGroupFilter() != null && !config.getGroupFilter().isEmpty() && "groups".equals(entity)) {
            appendAmp(query).append("$filter=").append(encode(config.getGroupFilter()));
        }*/
        appendAmp(query).append("$top=").append(config.getPageSize());
        return sb.append("?").append(query).toString();
    }

    private static StringBuilder appendAmp(StringBuilder sb) {
        if (!sb.isEmpty()) {
            sb.append("&");
        }
        return sb;
    }

    private static String encode(String value) {
        return URLEncoder.encode(value, StandardCharsets.UTF_8);
    }

    /**
     * Defense-in-depth against a poisoned @odata.nextLink / @odata.deltaLink: only follow
     * pagination/delta URLs whose host matches the configured Graph base URL. This prevents
     * the bearer token from being sent to an attacker-influenced host (e.g. a cloud metadata
     * endpoint) if a Graph response were ever tampered with. Comparison is host-only and
     * case-insensitive; scheme and port are also required to match the configured base.
     */
    private void assertSameHostAsGraph(String url) throws GraphClientException {
        final URI target;
        final URI base;
        try {
            target = new URI(url);
            base = new URI(config.getGraphBaseUrl());
        } catch (URISyntaxException e) {
            throw new GraphClientException("Refusing to fetch malformed URL: " + url, e);
        }
        String targetHost = target.getHost();
        String baseHost = base.getHost();
        if (targetHost == null || !targetHost.equalsIgnoreCase(baseHost) || !schemeEquals(base, target) || base.getPort() != target.getPort()) {
            throw new GraphClientException("Refusing to follow URL outside the configured Graph host (" + baseHost + "): " + url);
        }
    }

    private static boolean schemeEquals(URI a, URI b) {
        return a.getScheme() != null && a.getScheme().equalsIgnoreCase(b.getScheme());
    }

    private JsonNode executeGet(String url) throws GraphClientException {
        assertSameHostAsGraph(url);
        int attempt = 0;
        boolean refreshedOnce = false;
        while (true) {
            String token = tokenProvider.getAccessToken(false);
            try (Response response = httpClient.target(url).request(MediaType.APPLICATION_JSON).header("Authorization", "Bearer " + token)
                    // ConsistencyLevel: eventual opts into Entra ID's eventual-consistency read model and is
                    // required for advanced queries ($count, $search, advanced $filter, $orderby). It is also
                    // commonly used for directory-object delta queries and is harmless on the calls made here.
                    .header("ConsistencyLevel", "eventual").get()) {
                int status = response.getStatus();
                if (status >= 200 && status < 300) {
                    String body = response.hasEntity() ? response.readEntity(String.class) : "{}";
                    return objectMapper.readTree(body);
                }
                if (status == 401 && !refreshedOnce) {
                    refreshedOnce = true;
                    tokenProvider.getAccessToken(true);
                    continue;
                }
                if ((status == 429 || status == 503) && attempt < config.getMaxRetries()) {
                    long waitMs = backoffMillis(response, attempt);
                    LOG.warn("Graph throttled (HTTP {}); backing off {} ms (attempt {}/{})", status, waitMs, attempt + 1, config.getMaxRetries());
                    sleep(waitMs);
                    attempt++;
                    continue;
                }
                String body = response.hasEntity() ? response.readEntity(String.class) : "";
                // Delta link expiry / invalidation -> caller must restart with a full sync.
                // 410 Gone               : tenant maintenance / migration
                // 400 syncStateNotFound  : delta token expired (7-day limit for directory objects)
                // The Location header on a 410 is not always reliable, so recovery re-seeds
                // from our own base delta URL rather than following it.
                if (status == 410 || (status == 400 && body != null && (body.contains("syncStateNotFound") || body.contains("resyncRequired")))) {
                    throw new DeltaResyncRequiredException("Graph delta link expired/invalid: GET " + url + " -> HTTP " + status + " " + truncate(body), status);
                }
                throw new GraphClientException("Graph request failed: GET " + url + " -> HTTP " + status + " " + truncate(body), status);
            } catch (GraphClientException e) {
                throw e;
            } catch (Exception e) {
                if (attempt < config.getMaxRetries()) {
                    long waitMs = config.getRetryBaseBackoffMs() * (1L << attempt);
                    LOG.warn("Transient error on GET {} ({}); retrying in {} ms (attempt {}/{})", url, e.toString(), waitMs, attempt + 1, config.getMaxRetries());
                    sleep(waitMs);
                    attempt++;
                    continue;
                }
                throw new GraphClientException("Graph request failed after retries: GET " + url, e);
            }
        }
    }

    private long backoffMillis(Response response, int attempt) {
        String retryAfter = response.getHeaderString("Retry-After");
        if (retryAfter != null && !retryAfter.isEmpty()) {
            try {
                return Long.parseLong(retryAfter.trim()) * 1000L;
            } catch (NumberFormatException ignored) {
                // fall back to exponential backoff
            }
        }
        return config.getRetryBaseBackoffMs() * (1L << attempt);
    }

    private static void sleep(long millis) throws GraphClientException {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new GraphClientException("Interrupted while backing off", e);
        }
    }

    private GraphUser mapUser(JsonNode node) {
        GraphUser user = new GraphUser();
        user.setId(textOrNull(node, "id"));
        user.setUserPrincipalName(textOrNull(node, "userPrincipalName"));
        user.setMail(textOrNull(node, "mail"));
        user.setDisplayName(textOrNull(node, "displayName"));
        if (node.has("accountEnabled") && !node.get("accountEnabled").isNull()) {
            user.setAccountEnabled(node.get("accountEnabled").asBoolean(true));
        }
        applyAdditionalAttributes(node, config.getUserSelectAttrs(), user::putAdditionalAttribute);
        return user;
    }

    private GraphGroup mapGroup(JsonNode node) {
        GraphGroup group = new GraphGroup();
        group.setId(textOrNull(node, "id"));
        group.setDisplayName(textOrNull(node, "displayName"));
        group.setMailNickname(textOrNull(node, "mailNickname"));
        if (node.has("securityEnabled") && !node.get("securityEnabled").isNull()) {
            group.setSecurityEnabled(node.get("securityEnabled").asBoolean(true));
        }
        applyAdditionalAttributes(node, config.getGroupSelectAttrs(), group::putAdditionalAttribute);
        return group;
    }

    private interface AttrSink {
        void put(String key, String value);
    }

    private void applyAdditionalAttributes(JsonNode node, Set<String> selectAttrs, AttrSink sink) {
        if (selectAttrs == null || selectAttrs.isEmpty()) {
            return;
        }
        for (String attr : selectAttrs) {
            JsonNode v = node.get(attr);
            if (v != null && !v.isNull() && v.isValueNode()) {
                sink.put(attr, v.asText());
            }
        }
    }

    private Iterable<JsonNode> valueArray(JsonNode page) {
        JsonNode value = page.get("value");
        if (value == null || !value.isArray()) {
            return Collections.emptyList();
        }
        return value::elements;
    }

    private static String textOrNull(JsonNode node, String field) {
        JsonNode v = node.get(field);
        return (v == null || v.isNull()) ? null : v.asText();
    }

    private static String truncate(String s) {
        if (s == null) {
            return "";
        }
        return s.length() <= 500 ? s : s.substring(0, 500) + "...";
    }
}
