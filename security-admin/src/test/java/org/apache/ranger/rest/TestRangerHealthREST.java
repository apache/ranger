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

package org.apache.ranger.rest;

import org.apache.ranger.biz.RangerBizUtil;
import org.apache.ranger.plugin.model.RangerServerHealth;
import org.apache.ranger.util.RangerServerHealthUtil;
import org.junit.Assert;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.MethodSorters;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.apache.ranger.plugin.model.RangerServerHealth.RangerServerStatus.UP;

@RunWith(MockitoJUnitRunner.class)
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class TestRangerHealthREST {
    @Mock
    RangerServerHealthUtil rangerServerHealthUtil;
    @InjectMocks
    RangerHealthREST       rangerHealthREST = new RangerHealthREST();
    @Mock
    RangerBizUtil          xaBizUtil;

    @Test
    public void testHealthCheckStatusAPI() {
        String dbVersion = "23.2.0";
        Mockito.when(xaBizUtil.getDBVersion()).thenReturn(dbVersion);
        Mockito.when(rangerServerHealthUtil.getRangerServerHealth(dbVersion)).thenReturn(createRangerServerHealth());
        RangerServerHealth rangerServerHealth = rangerHealthREST.getRangerServerHealth();
        Assert.assertEquals("RangerHealth.down()", UP, rangerServerHealth.getStatus());
        Assert.assertEquals("RangerHealth.getDetails()", 1, rangerServerHealth.getDetails().size());
        Assert.assertEquals("RangerHealth.getDetails('component')", 2, ((Map<?, ?>) rangerServerHealth.getDetails().get("components")).size());
    }

    private RangerServerHealth createRangerServerHealth() {
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
        auditProviderDetailsMap.put("details", auditProviderDetailsMap);
        componentsMap.put("auditProvider", auditProviderMap);
        RangerServerHealth rangerRServerHealth = RangerServerHealth.up().withDetail("components", componentsMap).build();
        return rangerRServerHealth;
    }
}
