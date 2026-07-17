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

package org.apache.ranger.entraid.graph;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import org.apache.ranger.entraid.graph.EntraIdGraphConfig.AuthMode;
import org.apache.ranger.ugsyncutil.model.graph.DeltaEntry;
import org.apache.ranger.ugsyncutil.model.graph.DeltaPage;
import org.apache.ranger.ugsyncutil.model.graph.GraphGroup;
import org.apache.ranger.ugsyncutil.model.graph.GraphMemberRef;
import org.apache.ranger.ugsyncutil.model.graph.GraphUser;
import org.apache.ranger.ugsyncutil.model.graph.GroupMembershipPage;
import org.apache.ranger.ugsyncutil.model.graph.MemberType;
import org.apache.ranger.ugsyncutil.model.graph.MembershipMode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

@ExtendWith(MockitoExtension.class)
@TestMethodOrder(MethodOrderer.MethodName.class)
public class TestEntraIdGraphClientImpl {
    private static final String TENANT = "11111111-1111-1111-1111-111111111111";
    private static final String CLIENT = "22222222-2222-2222-2222-222222222222";
    private static final String RESOURCE_DIR = "src/test/resources/";
    private static final String BASE_URL_TAG = "__BASE_URL__";

    private HttpServer server;
    private String baseUrl;

    @BeforeEach
    public void startServer() throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        baseUrl = "http://127.0.0.1:" + server.getAddress().getPort();
        server.createContext("/" + TENANT + "/oauth2/v2.0/token", exchange ->
                respond(exchange, 200, "{\"access_token\":\"test-token\",\"expires_in\":3600}"));
        server.start();
    }

    @AfterEach
    public void stopServer() {
        if (server != null) {
            server.stop(0);
        }
    }

    private EntraIdGraphConfig stubConfig() {
        return new EntraIdGraphConfig.Builder()
                .tenantId(TENANT)
                .clientId(CLIENT)
                .authMode(AuthMode.CLIENT_SECRET)
                .clientSecret("test-secret".toCharArray())
                .authorityHost(baseUrl)
                .graphBaseUrl(baseUrl)
                .retryBaseBackoffMs(5)
                .maxRetries(3)
                .build();
    }

    private EntraIdGraphClientImpl newClient() throws Exception {
        EntraIdGraphClientImpl client = new EntraIdGraphClientImpl();
        client.init(stubConfig());
        return client;
    }

    private void context(String path, HttpHandler handler) {
        server.createContext(path, handler);
    }

    private static void respond(HttpExchange exchange, int status, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().add("Content-Type", "application/json");
        exchange.sendResponseHeaders(status, bytes.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(bytes);
        }
    }

    @Test
    public void test01_getUserDelta_mapsCoreUserFields() throws Exception {
        context("/v1.0/users/delta", exchange -> respond(exchange, 200,
                "{\"value\":[{\"id\":\"u-1\",\"userPrincipalName\":\"alice@example.com\","
                        + "\"mail\":\"alice@example.com\",\"displayName\":\"Alice\"}],"
                        + "\"@odata.deltaLink\":\"" + baseUrl + "/v1.0/users/delta?$deltatoken=D1\"}"));
        try (EntraIdGraphClientImpl client = newClient()) {
            DeltaPage<GraphUser> page = client.getUserDelta(null);
            Assertions.assertEquals(1, page.getEntries().size());
            GraphUser user = page.getEntries().get(0).getValue();
            Assertions.assertEquals("u-1", user.getId());
            Assertions.assertEquals("alice@example.com", user.getUserPrincipalName());
            Assertions.assertEquals("Alice", user.getDisplayName());
        }
    }

    @Test
    public void test02_getUserDelta_capturesDeltaLink() throws Exception {
        String deltaLink = baseUrl + "/v1.0/users/delta?$deltatoken=ABC";
        context("/v1.0/users/delta", exchange -> respond(exchange, 200, "{\"value\":[],\"@odata.deltaLink\":\"" + deltaLink + "\"}"));
        try (EntraIdGraphClientImpl client = newClient()) {
            DeltaPage<GraphUser> page = client.getUserDelta(null);
            Assertions.assertEquals(deltaLink, page.getDeltaLink());
        }
    }

    @Test
    public void test03_getUserDelta_followsNextLinkAndAggregates() throws Exception {
        context("/v1.0/users/delta", exchange -> respond(exchange, 200,
                "{\"value\":[{\"id\":\"u-1\",\"userPrincipalName\":\"a@x\"}],"
                        + "\"@odata.nextLink\":\"" + baseUrl + "/v1.0/users/delta/page2\"}"));
        context("/v1.0/users/delta/page2", exchange -> respond(exchange, 200,
                "{\"value\":[{\"id\":\"u-2\",\"userPrincipalName\":\"b@x\"}],"
                        + "\"@odata.deltaLink\":\"" + baseUrl + "/v1.0/users/delta?$deltatoken=END\"}"));
        try (EntraIdGraphClientImpl client = newClient()) {
            DeltaPage<GraphUser> page = client.getUserDelta(null);
            Assertions.assertEquals(2, page.getEntries().size());
            Assertions.assertTrue(page.getDeltaLink().contains("$deltatoken=END"));
        }
    }

    @Test
    public void test04_getUserDelta_marksRemovedEntries() throws Exception {
        context("/v1.0/users/delta", exchange -> respond(exchange, 200,
                "{\"value\":[{\"id\":\"u-9\",\"@removed\":{\"reason\":\"deleted\"}}],"
                        + "\"@odata.deltaLink\":\"" + baseUrl + "/v1.0/users/delta?$deltatoken=D\"}"));
        try (EntraIdGraphClientImpl client = newClient()) {
            DeltaPage<GraphUser> page = client.getUserDelta(null);
            Assertions.assertTrue(page.getEntries().get(0).isRemoved());
        }
    }

    @Test
    public void test05_getUserDelta_usesProvidedDeltaLinkVerbatim() throws Exception {
        context("/v1.0/users/delta/resume", exchange -> respond(exchange, 200,
                "{\"value\":[{\"id\":\"u-r\",\"userPrincipalName\":\"r@x\"}],"
                        + "\"@odata.deltaLink\":\"" + baseUrl + "/v1.0/users/delta?$deltatoken=D2\"}"));
        try (EntraIdGraphClientImpl client = newClient()) {
            DeltaPage<GraphUser> page = client.getUserDelta(baseUrl + "/v1.0/users/delta/resume");
            Assertions.assertEquals("u-r", page.getEntries().get(0).getValue().getId());
        }
    }

    @Test
    public void test06_throttling_retriesAfter429ThenSucceeds() throws Exception {
        AtomicInteger hits = new AtomicInteger();
        context("/v1.0/users/delta", exchange -> {
            if (hits.getAndIncrement() == 0) {
                respond(exchange, 429, "{\"error\":\"throttled\"}");
            } else {
                respond(exchange, 200, "{\"value\":[{\"id\":\"u-1\",\"userPrincipalName\":\"a@x\"}],"
                        + "\"@odata.deltaLink\":\"" + baseUrl + "/v1.0/users/delta?$deltatoken=D\"}");
            }
        });
        try (EntraIdGraphClientImpl client = newClient()) {
            DeltaPage<GraphUser> page = client.getUserDelta(null);
            Assertions.assertEquals(1, page.getEntries().size());
            Assertions.assertEquals(2, hits.get(), "client should have retried exactly once after the 429");
        }
    }

    @Test
    public void test07_throttling_exhaustsRetriesThenThrows() throws Exception {
        context("/v1.0/users/delta", exchange -> respond(exchange, 429, "{\"error\":\"throttled\"}"));
        try (EntraIdGraphClientImpl client = newClient()) {
            GraphClientException ex = Assertions.assertThrows(GraphClientException.class, () -> client.getUserDelta(null));
            Assertions.assertEquals(429, ex.getHttpStatus());
        }
    }

    @Test
    public void test08_nonRetryableError_throwsWithStatus() throws Exception {
        context("/v1.0/users/delta", exchange -> respond(exchange, 403, "{\"error\":\"forbidden\"}"));
        try (EntraIdGraphClientImpl client = newClient()) {
            GraphClientException ex = Assertions.assertThrows(GraphClientException.class, () -> client.getUserDelta(null));
            Assertions.assertEquals(403, ex.getHttpStatus());
        }
    }

    @Test
    public void test09_getGroupMembers_directUsesTypedUserSegment() throws Exception {
        // Client casts to microsoft.graph.user; Graph returns only users (no @odata.type),
        // and every result is mapped as USER by construction.
        context("/v1.0/groups/g-1/members/microsoft.graph.user", exchange -> respond(exchange, 200,
                "{\"value\":[{\"id\":\"u-1\"},{\"id\":\"u-2\"}]}"));
        try (EntraIdGraphClientImpl client = newClient()) {
            List<GraphMemberRef> members = client.getGroupMembers("g-1", MembershipMode.DIRECT);
            Assertions.assertEquals(2, members.size());
            Assertions.assertEquals(MemberType.USER, members.get(0).getType());
            Assertions.assertEquals(MemberType.USER, members.get(1).getType());
        }
    }

    @Test
    public void test10_getGroupMembers_transitiveUsesTransitiveEndpoint() throws Exception {
        context("/v1.0/groups/g-1/transitiveMembers/microsoft.graph.user", exchange -> respond(exchange, 200,
                "{\"value\":[{\"id\":\"u-1\",\"@odata.type\":\"#microsoft.graph.user\"}]}"));
        try (EntraIdGraphClientImpl client = newClient()) {
            List<GraphMemberRef> members = client.getGroupMembers("g-1", MembershipMode.TRANSITIVE);
            Assertions.assertEquals(1, members.size());
            Assertions.assertEquals("u-1", members.get(0).getId());
        }
    }

    @Test
    public void test11_getGroupDelta_mapsGroup() throws Exception {
        context("/v1.0/groups/delta", exchange -> respond(exchange, 200,
                "{\"value\":[{\"id\":\"g-1\",\"displayName\":\"Engineering\"}],"
                        + "\"@odata.deltaLink\":\"" + baseUrl + "/v1.0/groups/delta?$deltatoken=G1\"}"));
        try (EntraIdGraphClientImpl client = newClient()) {
            DeltaPage<GraphGroup> page = client.getGroupDelta(null);
            Assertions.assertEquals(1, page.getEntries().size());
            Assertions.assertEquals("Engineering", page.getEntries().get(0).getValue().getDisplayName());
        }
    }

    @Test
    public void test12_init_failsFastWhenTokenEndpointErrors() throws Exception {
        server.removeContext("/" + TENANT + "/oauth2/v2.0/token");
        context("/" + TENANT + "/oauth2/v2.0/token", exchange -> respond(exchange, 401, "{\"error\":\"invalid_client\"}"));
        EntraIdGraphClientImpl client = new EntraIdGraphClientImpl();
        Assertions.assertThrows(GraphClientException.class, () -> client.init(stubConfig()));
        client.close();
    }

    @Test
    public void test13_userDelta_mapsAllEntries() throws Exception {
        serve("/v1.0/users/delta", "users-delta-page.json");
        try (EntraIdGraphClientImpl client = newClient()) {
            DeltaPage<GraphUser> page = client.getUserDelta(null);
            // 3 users + 1 @removed entry in the loadGraphResponse.
            Assertions.assertEquals(4, page.getEntries().size());
        }
    }

    @Test
    public void test14_userDelta_parsesFieldsAndNullMail() throws Exception {
        serve("/v1.0/users/delta", "users-delta-page.json");
        try (EntraIdGraphClientImpl client = newClient()) {
            DeltaPage<GraphUser> page = client.getUserDelta(null);
            GraphUser alice = page.getEntries().get(0).getValue();
            Assertions.assertEquals("11111111-aaaa-bbbb-cccc-000000000001", alice.getId());
            Assertions.assertEquals("Alice Anderson", alice.getDisplayName());
            GraphUser carol = page.getEntries().get(2).getValue();
            Assertions.assertNull(carol.getMail());
        }
    }

    @Test
    public void test15_userDelta_flagsRemovedEntry() throws Exception {
        serve("/v1.0/users/delta", "users-delta-page.json");
        try (EntraIdGraphClientImpl client = newClient()) {
            DeltaPage<GraphUser> page = client.getUserDelta(null);
            DeltaEntry<GraphUser> last = page.getEntries().get(3);
            Assertions.assertTrue(last.isRemoved(), "the @removed entry must be flagged removed");
        }
    }

    @Test
    public void test16_userDelta_capturesDeltaToken() throws Exception {
        serve("/v1.0/users/delta", "users-delta-page.json");
        try (EntraIdGraphClientImpl client = newClient()) {
            DeltaPage<GraphUser> page = client.getUserDelta(null);
            Assertions.assertTrue(page.getDeltaLink().contains("SAMPLE_USER_DELTA_TOKEN"));
        }
    }

    @Test
    public void test17_groupDelta_mapsGroups() throws Exception {
        serve("/v1.0/groups/delta", "groups-delta-page.json");
        try (EntraIdGraphClientImpl client = newClient()) {
            DeltaPage<GraphGroup> page = client.getGroupDelta(null);
            Assertions.assertEquals(3, page.getEntries().size());
            Assertions.assertEquals("Engineering", page.getEntries().get(0).getValue().getDisplayName());
        }
    }

    @Test
    public void test18_groupDelta_flagsRemovedGroup() throws Exception {
        serve("/v1.0/groups/delta", "groups-delta-page.json");
        try (EntraIdGraphClientImpl client = newClient()) {
            DeltaPage<GraphGroup> page = client.getGroupDelta(null);
            Assertions.assertTrue(page.getEntries().get(2).isRemoved());
        }
    }

    @Test
    public void test19_members_mapsUsers() throws Exception {
        serve("/v1.0/groups/22222222-aaaa-bbbb-cccc-000000000001/members/microsoft.graph.user", "group-members.json");
        try (EntraIdGraphClientImpl client = newClient()) {
            List<GraphMemberRef> members = client.getGroupMembers("22222222-aaaa-bbbb-cccc-000000000001", MembershipMode.DIRECT);
            Assertions.assertEquals(2, members.size());
            Assertions.assertEquals(MemberType.USER, members.get(0).getType());
            Assertions.assertEquals(MemberType.USER, members.get(1).getType());
        }
    }

    @Test
    public void test20_paged_followNextLinkAndAggregate() throws Exception {
        serve("/v1.0/users/delta", "users-delta-page1.json");
        serve("/v1.0/users/delta/page2", "users-delta-page2.json");
        try (EntraIdGraphClientImpl client = newClient()) {
            DeltaPage<GraphUser> page = client.getUserDelta(null);
            // 2 users on page 1 + 1 on page 2 = 3 aggregated.
            Assertions.assertEquals(3, page.getEntries().size());
            Assertions.assertTrue(page.getDeltaLink().contains("PAGED_SAMPLE_TOKEN"));
        }
    }

    @Test
    public void test21_tokenExpired_intercepts401AndRefreshesToken() throws Exception {
        AtomicInteger hits = new AtomicInteger();
        context("/v1.0/users/delta", exchange -> {
            if (hits.getAndIncrement() == 0) {
                respond(exchange, 401, "{\"error\":{\"code\":\"InvalidAuthenticationToken\"}}");
            } else {
                respond(exchange, 200, "{\"value\":[{\"id\":\"u-refreshed\"}],\"@odata.deltaLink\":\"done\"}");
            }
        });
        try (EntraIdGraphClientImpl client = newClient()) {
            DeltaPage<GraphUser> page = client.getUserDelta(null);
            Assertions.assertEquals(1, page.getEntries().size());
            Assertions.assertEquals("u-refreshed", page.getEntries().get(0).getValue().getId());
            Assertions.assertEquals(2, hits.get());
        }
    }

    @Test
    public void test22_getGroupMembers_followsNextLinkAndAggregates() throws Exception {
        context("/v1.0/groups/g-paged/members/microsoft.graph.user", exchange -> respond(exchange, 200,
                "{\"value\":[{\"id\":\"u-p1\"}],\"@odata.nextLink\":\"" + baseUrl + "/v1.0/groups/g-paged/members/page2\"}"));
        context("/v1.0/groups/g-paged/members/page2", exchange -> respond(exchange, 200,
                "{\"value\":[{\"id\":\"u-p2\"}]}"));
        try (EntraIdGraphClientImpl client = newClient()) {
            List<GraphMemberRef> members = client.getGroupMembers("g-paged", MembershipMode.DIRECT);
            Assertions.assertEquals(2, members.size());
            Assertions.assertEquals("u-p1", members.get(0).getId());
            Assertions.assertEquals("u-p2", members.get(1).getId());
        }
    }

    @Test
    public void test23_userDelta_410Gone_resyncsFromBaseAndFlagsResynced() throws Exception {
        // Expired delta link returns 410; the base delta URL then returns a normal page.
        context("/v1.0/users/delta/expired", exchange ->
                respond(exchange, 410, "{\"error\":{\"code\":\"resyncRequired\"}}"));
        context("/v1.0/users/delta", exchange -> respond(exchange, 200,
                "{\"value\":[{\"id\":\"u-1\",\"userPrincipalName\":\"a@x\"}],"
                        + "\"@odata.deltaLink\":\"" + baseUrl + "/v1.0/users/delta?$deltatoken=NEW\"}"));
        try (EntraIdGraphClientImpl client = newClient()) {
            DeltaPage<GraphUser> page = client.getUserDelta(baseUrl + "/v1.0/users/delta/expired");
            Assertions.assertTrue(page.isResynced(), "a 410-triggered full pull must be flagged resynced");
            Assertions.assertEquals(1, page.getEntries().size());
            Assertions.assertTrue(page.getDeltaLink().contains("NEW"), "must capture the fresh delta token");
        }
    }

    @Test
    public void test24_userDelta_400SyncStateNotFound_resyncs() throws Exception {
        context("/v1.0/users/delta/expired", exchange ->
                respond(exchange, 400, "{\"error\":{\"code\":\"syncStateNotFound\",\"message\":\"token expired\"}}"));
        context("/v1.0/users/delta", exchange -> respond(exchange, 200,
                "{\"value\":[{\"id\":\"u-1\",\"userPrincipalName\":\"a@x\"}],"
                        + "\"@odata.deltaLink\":\"" + baseUrl + "/v1.0/users/delta?$deltatoken=NEW\"}"));
        try (EntraIdGraphClientImpl client = newClient()) {
            DeltaPage<GraphUser> page = client.getUserDelta(baseUrl + "/v1.0/users/delta/expired");
            Assertions.assertTrue(page.isResynced(), "400 syncStateNotFound must trigger a flagged resync");
            Assertions.assertEquals(1, page.getEntries().size());
        }
    }

    @Test
    public void test25_groupDelta_410Gone_resyncs() throws Exception {
        context("/v1.0/groups/delta/expired", exchange ->
                respond(exchange, 410, "{\"error\":{\"code\":\"resyncRequired\"}}"));
        context("/v1.0/groups/delta", exchange -> respond(exchange, 200,
                "{\"value\":[{\"id\":\"g-1\",\"displayName\":\"Engineering\"}],"
                        + "\"@odata.deltaLink\":\"" + baseUrl + "/v1.0/groups/delta?$deltatoken=NEW\"}"));
        try (EntraIdGraphClientImpl client = newClient()) {
            DeltaPage<GraphGroup> page = client.getGroupDelta(baseUrl + "/v1.0/groups/delta/expired");
            Assertions.assertTrue(page.isResynced());
            Assertions.assertEquals(1, page.getEntries().size());
        }
    }

    @Test
    public void test26_userDelta_resyncOnFullPull_propagatesException() throws Exception {
        // If the *base* delta URL itself returns expiry (deltaLink == null path), there is
        // nothing to fall back to -> the resync exception must propagate, not loop.
        context("/v1.0/users/delta", exchange -> respond(exchange, 410, "{\"error\":{\"code\":\"resyncRequired\"}}"));
        try (EntraIdGraphClientImpl client = newClient()) {
            Assertions.assertThrows(GraphClientException.class, () -> client.getUserDelta(null));
        }
    }

    @Test
    public void test27_userDelta_normalPage_isNotFlaggedResynced() throws Exception {
        // A normal (non-expired) delta must NOT be flagged resynced.
        context("/v1.0/users/delta", exchange -> respond(exchange, 200,
                "{\"value\":[{\"id\":\"u-1\",\"userPrincipalName\":\"a@x\"}],"
                        + "\"@odata.deltaLink\":\"" + baseUrl + "/v1.0/users/delta?$deltatoken=D\"}"));
        try (EntraIdGraphClientImpl client = newClient()) {
            DeltaPage<GraphUser> page = client.getUserDelta(null);
            Assertions.assertFalse(page.isResynced(), "a normal delta page must not be flagged resynced");
        }
    }

    @Test
    public void test28_membersMerge_accumulatesSameGroupAcrossPages() throws Exception {
        serve("/v1.0/groups/delta", "groups-members-page1.json");
        serve("/v1.0/groups/delta/page2", "groups-members-page2.json");
        try (EntraIdGraphClientImpl client = newClient()) {
            GroupMembershipPage page = client.getGroupDeltaWithMembers(null);
            // G1 members are split: u1,u2 on page1 and u4,u5 on page2 -> merged set of 4.
            Set<String> g1 = page.getMembers("G1");
            Assertions.assertEquals(4, g1.size(), "G1 membership must merge both pages");
            Assertions.assertTrue(g1.containsAll(java.util.Arrays.asList("u1", "u2", "u4", "u5")));
        }
    }

    @Test
    public void test29_membersMerge_excludesGroupTypeMembers() throws Exception {
        serve("/v1.0/groups/delta", "groups-members-page1.json");
        serve("/v1.0/groups/delta/page2", "groups-members-page2.json");
        try (EntraIdGraphClientImpl client = newClient()) {
            GroupMembershipPage page = client.getGroupDeltaWithMembers(null);
            // page1 G1 contains a #microsoft.graph.group member (nestedGroup1) which must be dropped.
            Assertions.assertFalse(page.getMembers("G1").contains("nestedGroup1"), "group-type members must be excluded in DIRECT mode");
        }
    }

    @Test
    public void test30_membersMerge_dedupsGroupObjectAcrossPages() throws Exception {
        serve("/v1.0/groups/delta", "groups-members-page1.json");
        serve("/v1.0/groups/delta/page2", "groups-members-page2.json");
        try (EntraIdGraphClientImpl client = newClient()) {
            GroupMembershipPage page = client.getGroupDeltaWithMembers(null);
            // G1, G2, G3 = three distinct groups even though G1 appeared on two pages.
            long g1Count = page.getGroups().stream().filter(e -> "G1".equals(e.getValue().getId())).count();
            Assertions.assertEquals(1, g1Count, "G1 must appear once despite spanning two pages");
            Assertions.assertEquals(3, page.getGroups().size(), "G1, G2, G3 expected");
        }
    }

    @Test
    public void test31_membersMerge_groupWithNoMembersHasEmptySet() throws Exception {
        serve("/v1.0/groups/delta", "groups-members-page1.json");
        serve("/v1.0/groups/delta/page2", "groups-members-page2.json");
        try (EntraIdGraphClientImpl client = newClient()) {
            GroupMembershipPage page = client.getGroupDeltaWithMembers(null);
            // G3 has no members@delta property at all -> empty set, not null.
            Assertions.assertTrue(page.getMembers("G3").isEmpty(), "a memberless group yields an empty set");
        }
    }

    @Test
    public void test32_membersMerge_secondGroupMembersIntact() throws Exception {
        serve("/v1.0/groups/delta", "groups-members-page1.json");
        serve("/v1.0/groups/delta/page2", "groups-members-page2.json");
        try (EntraIdGraphClientImpl client = newClient()) {
            GroupMembershipPage page = client.getGroupDeltaWithMembers(null);
            Set<String> g2 = page.getMembers("G2");
            Assertions.assertEquals(1, g2.size());
            Assertions.assertTrue(g2.contains("u3"));
        }
    }

    @Test
    public void test33_membersMerge_capturesDeltaLink() throws Exception {
        serve("/v1.0/groups/delta", "groups-members-page1.json");
        serve("/v1.0/groups/delta/page2", "groups-members-page2.json");
        try (EntraIdGraphClientImpl client = newClient()) {
            GroupMembershipPage page = client.getGroupDeltaWithMembers(null);
            Assertions.assertTrue(page.getDeltaLink().contains("MEMBERS_TOKEN"), "the deltaLink from the final page must be captured");
        }
    }

    @Test
    public void test34_malformedJsonOn200_throwsGraphClientException() throws Exception {
        // A 200 with a truncated/unparseable body must surface as GraphClientException,
        // never a silently-empty page.
        context("/v1.0/users/delta", exchange -> respond(exchange, 200, "{ \"value\": [ "));   // truncated JSON
        try (EntraIdGraphClientImpl client = newClient()) {
            Assertions.assertThrows(GraphClientException.class, () -> client.getUserDelta(null));
        }
    }

    @Test
    public void test35_doubleUnauthorized_refreshesOnceThenThrows() throws Exception {
        // First call 401 -> client force-refreshes the token and retries once. If the retry
        // ALSO 401s, the client must throw (not loop forever refreshing a bad token).
        AtomicInteger calls = new AtomicInteger();
        context("/v1.0/users/delta", exchange -> {
            calls.incrementAndGet();
            respond(exchange, 401, "{\"error\":{\"code\":\"InvalidAuthenticationToken\"}}");
        });
        try (EntraIdGraphClientImpl client = newClient()) {
            Assertions.assertThrows(GraphClientException.class, () -> client.getUserDelta(null));
            // Exactly two attempts: the original + one post-refresh retry. No infinite loop.
            Assertions.assertEquals(2, calls.get(), "client must retry once after refresh, then stop");
        }
    }

    @Test
    public void test36_missingDeltaLink_yieldsNullTokenNotCrash() throws Exception {
        // A successful page that omits @odata.deltaLink (and @odata.nextLink) must return a
        // page with a null delta link -- gracefully, without throwing. (The source treats a
        // null token as "full pull next cycle".)
        context("/v1.0/users/delta", exchange -> respond(exchange, 200, "{\"value\":[{\"id\":\"u1\",\"userPrincipalName\":\"a@example.com\"}]}"));
        try (EntraIdGraphClientImpl client = newClient()) {
            DeltaPage<GraphUser> page = client.getUserDelta(null);
            Assertions.assertEquals(1, page.getEntries().size(), "the user on the page must still be returned");
            Assertions.assertNull(page.getDeltaLink(), "missing deltaLink must yield a null token, not a crash");
        }
    }

    @Test
    public void test37_nextLinkToForeignHost_isRefused() throws Exception {
        // SSRF/defense-in-depth: a @odata.nextLink pointing off the configured Graph host must
        // NOT be followed (would leak the bearer token to an attacker-influenced host). The
        // client must throw rather than fetch it.
        context("/v1.0/users/delta", exchange -> respond(exchange, 200, "{\"value\":[{\"id\":\"u1\"}]," + "\"@odata.nextLink\":\"http://169.254.169.254/latest/meta-data/\"}"));
        try (EntraIdGraphClientImpl client = newClient()) {
            GraphClientException ex = Assertions.assertThrows(GraphClientException.class, () -> client.getUserDelta(null));
            Assertions.assertTrue(ex.getMessage().toLowerCase().contains("host") || ex.getMessage().toLowerCase().contains("refus"), "error should indicate the URL was outside the configured Graph host");
        }
    }

    private String loadGraphResponse(String fileName) throws IOException {
        String content = Files.readString(Paths.get(RESOURCE_DIR + fileName));
        return content.replace(BASE_URL_TAG, baseUrl);
    }

    private void serve(String path, String fileName) {
        context(path, exchange -> respond(exchange, 200, loadGraphResponse(fileName)));
    }
}
