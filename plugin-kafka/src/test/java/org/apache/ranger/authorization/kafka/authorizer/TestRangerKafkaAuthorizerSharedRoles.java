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

package org.apache.ranger.authorization.kafka.authorizer;

import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.server.authorizer.Action;
import org.apache.kafka.server.authorizer.AuthorizableRequestContext;
import org.apache.kafka.server.authorizer.AuthorizationResult;
import org.apache.ranger.authorization.hadoop.config.RangerPluginConfig;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerRole;
import org.apache.ranger.plugin.policyengine.RangerAccessRequest;
import org.apache.ranger.plugin.policyengine.RangerAccessResult;
import org.apache.ranger.plugin.policyengine.RangerPolicyEngineOptions;
import org.apache.ranger.plugin.service.RangerBasePlugin;
import org.apache.ranger.plugin.store.EmbeddedServiceDefsUtil;
import org.apache.ranger.plugin.util.RangerAccessRequestUtil;
import org.apache.ranger.plugin.util.RangerRoles;
import org.apache.ranger.plugin.util.ServicePolicies;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestRangerKafkaAuthorizerSharedRoles {
    @AfterEach
    void clearPlugin() throws Exception {
        setPlugin(null);
    }

    @Test
    void authorizeSharesOneRoleSetAcrossActions() throws Exception {
        RecordingPlugin plugin = new RecordingPlugin(pluginConfig());
        plugin.setPolicies(servicePolicies());
        plugin.setRoles(roles());
        setPlugin(plugin);

        AuthorizableRequestContext context = new RequestContext(new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "alice"));

        List<Action> actions = new ArrayList<>();
        actions.add(topicRead("orders"));
        actions.add(topicRead("payments"));
        actions.add(topicRead("shipping"));

        List<AuthorizationResult> results = new RangerKafkaAuthorizer().authorize(context, actions);

        assertEquals(Collections.nCopies(actions.size(), AuthorizationResult.ALLOWED), results);
        assertEquals(actions.size(), plugin.captured.size());

        Set<String> roles = null;

        for (RangerAccessRequest request : plugin.captured) {
            assertTrue(request.getUserRoles().contains("readers"));
            assertNull(RangerAccessRequestUtil.getBatchEvalContext(request.getContext()));

            if (roles == null) {
                roles = request.getUserRoles();
            } else {
                assertSame(roles, request.getUserRoles());
            }
        }
    }

    private static RangerPluginConfig pluginConfig() {
        RangerPolicyEngineOptions options = new RangerPolicyEngineOptions();
        options.disablePolicyRefresher = true;
        options.disableTagRetriever = true;
        options.disableUserStoreRetriever = true;
        options.disableGdsInfoRetriever = true;

        return new RangerPluginConfig("kafka", "svc", "app", null, null, options);
    }

    private static ServicePolicies servicePolicies() throws Exception {
        RangerPolicy.RangerPolicyItemAccess access = new RangerPolicy.RangerPolicyItemAccess();
        access.setType(RangerKafkaAuthorizer.ACCESS_TYPE_READ);
        access.setIsAllowed(true);

        RangerPolicy.RangerPolicyItem item = new RangerPolicy.RangerPolicyItem();
        item.setUsers(Collections.singletonList("alice"));
        item.setRoles(Collections.singletonList("readers"));
        item.setAccesses(Collections.singletonList(access));

        Map<String, RangerPolicy.RangerPolicyResource> resources = new HashMap<>();
        resources.put("topic", new RangerPolicy.RangerPolicyResource("*"));

        RangerPolicy policy = new RangerPolicy();
        policy.setId(1L);
        policy.setName("all-topics");
        policy.setService("svc");
        policy.setIsEnabled(true);
        policy.setResources(resources);
        policy.setPolicyItems(Collections.singletonList(item));

        ServicePolicies policies = new ServicePolicies();
        policies.setServiceName("svc");
        policies.setPolicyVersion(1L);
        policies.setServiceDef(EmbeddedServiceDefsUtil.instance().getEmbeddedServiceDef(EmbeddedServiceDefsUtil.EMBEDDED_SERVICEDEF_KAFKA_NAME));
        policies.setPolicies(Collections.singletonList(policy));

        return policies;
    }

    private static RangerRoles roles() {
        RangerRole readers = new RangerRole();
        readers.setName("readers");
        readers.setUsers(Collections.singletonList(new RangerRole.RoleMember("alice", false)));

        RangerRoles roles = new RangerRoles();
        roles.setRangerRoles(Collections.singleton(readers));

        return roles;
    }

    private static Action topicRead(String topic) {
        return new Action(AclOperation.READ, new ResourcePattern(ResourceType.TOPIC, topic, PatternType.LITERAL), 1, true, true);
    }

    private static void setPlugin(RangerBasePlugin plugin) throws Exception {
        Field field = RangerKafkaAuthorizer.class.getDeclaredField("rangerPlugin");

        field.setAccessible(true);
        field.set(null, plugin);
    }

    private static final class RequestContext implements AuthorizableRequestContext {
        private final KafkaPrincipal principal;

        private RequestContext(KafkaPrincipal principal) {
            this.principal = principal;
        }

        @Override
        public String listenerName() {
            return "SASL_PLAINTEXT";
        }

        @Override
        public SecurityProtocol securityProtocol() {
            return SecurityProtocol.SASL_PLAINTEXT;
        }

        @Override
        public KafkaPrincipal principal() {
            return principal;
        }

        @Override
        public InetAddress clientAddress() {
            return InetAddress.getLoopbackAddress();
        }

        @Override
        public int requestType() {
            return 0;
        }

        @Override
        public int requestVersion() {
            return 0;
        }

        @Override
        public String clientId() {
            return "cid";
        }

        @Override
        public int correlationId() {
            return 1;
        }
    }

    private static final class RecordingPlugin extends RangerBasePlugin {
        private Collection<RangerAccessRequest> captured;

        private RecordingPlugin(RangerPluginConfig config) {
            super(config);
        }

        @Override
        public Collection<RangerAccessResult> isAccessAllowed(Collection<RangerAccessRequest> requests) {
            captured = requests;

            return super.isAccessAllowed(requests);
        }
    }
}
