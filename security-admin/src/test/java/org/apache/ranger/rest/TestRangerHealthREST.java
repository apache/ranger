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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import javax.servlet.http.HttpServletRequest;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.apache.ranger.plugin.model.RangerServerHealth.RangerServerStatus.DOWN;
import static org.apache.ranger.plugin.model.RangerServerHealth.RangerServerStatus.UP;

@ExtendWith(MockitoExtension.class)
@TestMethodOrder(MethodOrderer.MethodName.class)
public class TestRangerHealthREST {
    @Mock
    RangerServerHealthUtil rangerServerHealthUtil;
    @InjectMocks
    RangerHealthREST       rangerHealthREST = new RangerHealthREST();
    @Mock
    RangerBizUtil          xaBizUtil;
    @Mock
    ServiceREST            serviceREST;

    @Test
    public void testHealthCheckStatusAPI() {
        String dbVersion = "23.2.0";
        Mockito.when(xaBizUtil.getDBVersion()).thenReturn(dbVersion);
        Mockito.when(rangerServerHealthUtil.getRangerServerHealth(dbVersion)).thenReturn(createRangerServerHealth());
        RangerServerHealth rangerServerHealth = rangerHealthREST.getRangerServerHealth();
        Assertions.assertEquals(UP, rangerServerHealth.getStatus(), "RangerHealth.down()");
        Assertions.assertEquals(1, rangerServerHealth.getDetails().size(), "RangerHealth.getDetails()");
        Assertions.assertEquals(2, ((Map<?, ?>) rangerServerHealth.getDetails().get("components")).size(), "RangerHealth.getDetails('component')");
    }

    @Test
    public void testLivenessReturnsUp() {
        Mockito.when(rangerServerHealthUtil.serviceUp()).thenReturn(RangerServerHealth.up().build());
        RangerServerHealth health = rangerHealthREST.getRangerServerLiveness();
        Assertions.assertEquals(UP, health.getStatus(), "liveness should report UP");
    }

    @Test
    public void testReadinessUpWhenServiceDefsExist() {
        HttpServletRequest request = Mockito.mock(HttpServletRequest.class);

        Mockito.when(serviceREST.getServiceDefNames()).thenReturn(Arrays.asList("hdfs"));
        Mockito.when(rangerServerHealthUtil.serviceUpWithAvailableServiceDefs(Arrays.asList("hdfs"))).thenReturn(RangerServerHealth.up().build());

        RangerServerHealth health = rangerHealthREST.getRangerServerReadiness();
        Assertions.assertEquals(UP, health.getStatus(), "readiness should be UP when service-defs exist");
        Mockito.verify(serviceREST).getServiceDefNames();
        Mockito.verify(rangerServerHealthUtil).serviceUpWithAvailableServiceDefs(Arrays.asList("hdfs"));
    }

    @Test
    public void testReadinessDownWhenNoServiceDefs() {
        HttpServletRequest request = Mockito.mock(HttpServletRequest.class);

        Mockito.when(serviceREST.getServiceDefNames()).thenReturn(Collections.emptyList());
        Mockito.when(rangerServerHealthUtil.serviceDown()).thenReturn(RangerServerHealth.down().build());

        RangerServerHealth health = rangerHealthREST.getRangerServerReadiness();
        Assertions.assertEquals(DOWN, health.getStatus(), "readiness should be DOWN when no service-defs exist");
        Mockito.verify(serviceREST).getServiceDefNames();
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
