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

package org.apache.ranger.plugin.model;

import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.apache.ranger.plugin.model.RangerServerHealth.RangerServerStatus.DOWN;
import static org.apache.ranger.plugin.model.RangerServerHealth.RangerServerStatus.UP;

public class TestRangerHealth {
    @Test
    public void testRangerStatusUP() {
        Map<String, Object> componentsMap = new HashMap<>();
        Map<String, Object> dbMap         = new LinkedHashMap<>();

        dbMap.put("status", UP);

        Map<String, Object> dbDetailsMap = new LinkedHashMap<>();

        dbDetailsMap.put("database", "Oracle 21.3c");
        dbDetailsMap.put("validationQuery", "SELECT banner from v$version where rownum<2");

        dbMap.put("details", dbDetailsMap);
        componentsMap.put("db", dbMap);

        Map<String, Object> auditProviderMap = new LinkedHashMap<>();

        auditProviderMap.put("status", UP);

        Map<String, Object> auditProviderDetailsMap = new LinkedHashMap<>();
        auditProviderDetailsMap.put("provider", "Elastic Search");
        auditProviderDetailsMap.put("providerHealthCheckEndpoint", "http://localhost:9200/_cluster/health?pretty");

        auditProviderMap.put("details", auditProviderDetailsMap);

        componentsMap.put("auditProvider", auditProviderMap);

        RangerServerHealth rangerHealth = RangerServerHealth.up().withDetail("components", componentsMap).build();

        Assert.assertEquals("RangerHealth.up()", UP, rangerHealth.getStatus());
        Assert.assertEquals("RangerHealth.getDetails()", 1, rangerHealth.getDetails().size());
        Assert.assertEquals("RangerHealth.getDetails('component')", 2, ((Map<?, ?>) rangerHealth.getDetails().get("components")).size());
    }

    @Test
    public void testRangerStatusDOWN() {
        Map<String, Object> componentsMap = new HashMap<>();
        Map<String, Object> dbMap         = new LinkedHashMap<>();

        dbMap.put("status", DOWN);

        Map<String, Object> dbDetailsMap = new LinkedHashMap<>();

        dbDetailsMap.put("database", "Oracle 21.3c");
        dbDetailsMap.put("validationQuery", "SELECT banner from v$version where rownum<2");

        dbMap.put("details", dbDetailsMap);
        componentsMap.put("db", dbMap);

        Map<String, Object> auditProviderMap = new LinkedHashMap<>();
        auditProviderMap.put("status", DOWN);

        Map<String, Object> auditProviderDetailsMap = new LinkedHashMap<>();

        auditProviderDetailsMap.put("provider", "Elastic Search");
        auditProviderDetailsMap.put("providerHealthCheckEndpoint", "http://localhost:9200/_cluster/health?pretty");

        auditProviderMap.put("details", auditProviderDetailsMap);
        componentsMap.put("auditProvider", auditProviderMap);

        RangerServerHealth rangerHealth = RangerServerHealth.down().withDetail("components", componentsMap).build();

        Assert.assertEquals("RangerHealth.down()", DOWN, rangerHealth.getStatus());
        Assert.assertEquals("RangerHealth.getDetails()", 1, rangerHealth.getDetails().size());
        Assert.assertEquals("RangerHealth.getDetails('component')", 2, ((Map<?, ?>) rangerHealth.getDetails().get("components")).size());
    }
}
