/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ranger.plugin.policyengine;

import org.apache.ranger.authorization.hadoop.config.RangerPluginConfig;
import org.apache.ranger.plugin.contextenricher.RangerContextEnricher;
import org.apache.ranger.plugin.contextenricher.RangerTagEnricher;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.model.RangerServiceDef.RangerContextEnricherDef;
import org.apache.ranger.plugin.util.ServicePolicies;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

public class TestRangerPolicyRepositoryContextEnricher {
    private static RangerPolicyRepository buildRepository(String enricherClassName) {
        RangerServiceDef serviceDef = new RangerServiceDef();

        serviceDef.setName("test-enricher-svc");
        serviceDef.setContextEnrichers(Collections.singletonList(
                new RangerContextEnricherDef(1L, "testEnricher", enricherClassName, Collections.emptyMap())));

        ServicePolicies servicePolicies = new ServicePolicies();

        servicePolicies.setServiceName("test-enricher-svc-instance");
        servicePolicies.setServiceDef(serviceDef);
        servicePolicies.setPolicies(Collections.emptyList());

        RangerPluginContext pluginContext = new RangerPluginContext(new RangerPluginConfig("test-enricher-svc", "test-enricher-svc-instance", "test-enricher-svc", "cl1", "on-prem", null));

        return new RangerPolicyRepository(servicePolicies, pluginContext);
    }

    @Test
    public void testMaliciousEnricherClassIsNotInstantiated() {
        // java.lang.Thread has a public no-arg constructor and is on the classpath,
        // but does not implement RangerContextEnricher.
        RangerPolicyRepository repository = buildRepository(Thread.class.getName());

        List<RangerContextEnricher> enrichers = repository.getContextEnrichers();

        Assertions.assertTrue(enrichers == null || enrichers.isEmpty(),
                "a class not assignable to RangerContextEnricher must not be instantiated as one");
    }

    @Test
    public void testLegitimateEnricherClassIsStillInstantiated() {
        RangerPolicyRepository repository = buildRepository(RangerTagEnricher.class.getName());

        List<RangerContextEnricher> enrichers = repository.getContextEnrichers();

        Assertions.assertNotNull(enrichers);
        Assertions.assertEquals(1, enrichers.size());
        Assertions.assertEquals(RangerTagEnricher.class, enrichers.get(0).getClass());
    }
}
