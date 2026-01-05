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

import org.apache.ranger.authorization.hadoop.config.RangerPluginConfig;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.policyengine.RangerAccessRequest;
import org.apache.ranger.plugin.policyengine.RangerAccessRequestImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessResourceImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessResult;
import org.apache.ranger.plugin.policyengine.RangerPolicyEngineOptions;
import org.apache.ranger.plugin.util.ServicePolicies;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.fail;

public class TestRangerBasePluginRaceCondition {
    private static final Logger LOG = LoggerFactory.getLogger(TestRangerBasePluginRaceCondition.class);

    private static final String SERVICE_TYPE = "hive";
    private static final String SERVICE_NAME = "test-hive";
    private static final String APP_ID = "test-app";
    private static final String USER = "bob";
    private static final int NUM_WORKERS = 10;
    private RangerBasePlugin plugin;
    private RangerAccessRequest request;

    @BeforeEach
    public void setUp() {
        System.setProperty("ranger.plugin.trino.policy.pollIntervalMs", "-1");
        System.setProperty("ranger.plugin.trino.policy.source.impl",
                "org.apache.ranger.plugin.service.TestRangerBasePluginRaceCondition$MockRangerAdminClient");
        System.setProperty("ranger.plugin.trino.policy.deltas", "false");
        System.setProperty("ranger.plugin.trino.in.place.policy.updates", "false");
        RangerPolicyEngineOptions peOptions = new RangerPolicyEngineOptions();
        RangerPluginConfig pluginConfig = new RangerPluginConfig(SERVICE_TYPE, SERVICE_NAME, APP_ID, "cl1", "on-perm", peOptions);
        pluginConfig.set("ranger.plugin.hive.policy.rest.url", "http://dummy:1234");

        plugin = new RangerBasePlugin(pluginConfig);
        plugin.init();
        request = createAccessRequest(USER, "table1", "select");
    }

    @Test
    public void testVisibilityOfPolicyEngine() throws Exception {
        plugin.setPolicies(createServicePolicies(true, 15_000, 1L));

        CountDownLatch ready = new CountDownLatch(NUM_WORKERS);
        CountDownLatch go = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(NUM_WORKERS);
        AtomicInteger sawDeny = new AtomicInteger(0);

        for (int i = 0; i < NUM_WORKERS; i++) {
            int id = i;
            new Thread(() -> {
                ready.countDown();
                try {
                    go.await();
                } catch (Exception ignored) {
                }
                boolean denied = false;
                for (int j = 0; j < 100_000_000; j++) {
                    RangerAccessResult r = plugin.isAccessAllowed(request);
                    if (r != null && !r.getIsAllowed()) {
                        sawDeny.incrementAndGet();
                        denied = true;
                        LOG.info("Worker-{} SAW DENY at {}.", id, j);
                        break;
                    }
                }
                if (!denied) {
                    LOG.info("Worker-{} NEVER SAW DENY!", id);
                }
                done.countDown();
            }).start();
        }
        ready.await();

        Thread updater = new Thread(() -> {
            LOG.info("UPDATING POLICY...");
            long start = System.nanoTime();
            plugin.setPolicies(createServicePolicies(false, 18_000, 2L));
            long duration = (System.nanoTime() - start) / 1_000_000;
            LOG.info("UPDATE TOOK {} ms", duration);
        });
        updater.start();
        go.countDown();
        updater.join();
        done.await(60, TimeUnit.SECONDS);
        int count = sawDeny.get();
        if (count < NUM_WORKERS) {
            fail("RACE! Only " + count + "/" + NUM_WORKERS + " saw DENY!");
        }
    }

    private RangerAccessRequest createAccessRequest(String user, String table, String accessType) {
        RangerAccessRequestImpl request = new RangerAccessRequestImpl();
        Map<String, Object> resource = new HashMap<>();
        resource.put("table", table);
        request.setResource(new RangerAccessResourceImpl(resource));
        request.setUser(user);
        request.setAccessType(accessType);
        request.setAction(accessType);
        return request;
    }

    private ServicePolicies createServicePolicies(boolean allowBob, int policyCount, long version) {
        ServicePolicies servicePolicies = new ServicePolicies();
        servicePolicies.setServiceName(SERVICE_NAME);
        servicePolicies.setServiceDef(createServiceDef());
        servicePolicies.setPolicyVersion(version);
        List<RangerPolicy> policies = new ArrayList<>(policyCount);

        for (int i = 0; i < policyCount; i++) {
            RangerPolicy p = new RangerPolicy();
            p.setId(1000L + i);
            p.setName("policy-" + i + "-v" + version);
            p.setService(SERVICE_NAME);
            p.setIsEnabled(true);

            Map<String, RangerPolicy.RangerPolicyResource> resMap = new HashMap<>();
            RangerPolicy.RangerPolicyResource res = new RangerPolicy.RangerPolicyResource();
            res.setValue("table1");
            res.setValues(Collections.singletonList("table1"));
            resMap.put("table", res);
            p.setResources(resMap);

            RangerPolicy.RangerPolicyItem item = new RangerPolicy.RangerPolicyItem();
            item.setUsers(Collections.singletonList(USER));
            RangerPolicy.RangerPolicyItemAccess acc = new RangerPolicy.RangerPolicyItemAccess();
            acc.setType("select");
            acc.setIsAllowed(allowBob);
            item.setAccesses(Collections.singletonList(acc));

            if (allowBob) {
                p.setPolicyItems(Collections.singletonList(item));
            } else {
                p.setDenyPolicyItems(Collections.singletonList(item));
            }
            policies.add(p);
        }
        servicePolicies.setPolicies(policies);
        return servicePolicies;
    }

    private RangerServiceDef createServiceDef() {
        RangerServiceDef def = new RangerServiceDef();
        def.setName(SERVICE_TYPE);
        return def;
    }
}
