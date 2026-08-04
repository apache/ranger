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
import org.apache.ranger.common.RESTErrorUtil;
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

import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.WebApplicationException;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.apache.ranger.plugin.model.RangerServerHealth.RangerServerStatus.INITIALIZATION_FAILURE;
import static org.apache.ranger.plugin.model.RangerServerHealth.RangerServerStatus.UP;
import static org.mockito.ArgumentMatchers.isNull;

@ExtendWith(MockitoExtension.class)
@TestMethodOrder(MethodOrderer.MethodName.class)
public class TestRangerHealthREST {
    @Mock
    RangerServerHealthUtil rangerServerHealthUtil;
    @InjectMocks
    RangerHealthREST       rangerHealthREST = new RangerHealthREST();
    @Mock
    RangerBizUtil          bizUtil;
    @Mock
    RESTErrorUtil          restErrorUtil;

    @Test
    public void testHealthCheckStatusAPI() {
        String dbVersion = "23.2.0";
        Mockito.when(bizUtil.getDBVersion()).thenReturn(dbVersion);
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
        Mockito.when(rangerServerHealthUtil.resolveAuthenticatedLoginId()).thenReturn(RangerBizUtil.HEALTHCHECK_USERNAME);
        Mockito.when(bizUtil.isHealthCheckUser(RangerBizUtil.HEALTHCHECK_USERNAME)).thenReturn(true);
        Mockito.when(rangerServerHealthUtil.getServiceDefNames()).thenReturn(Arrays.asList("hdfs"));
        Mockito.when(rangerServerHealthUtil.serviceUpWithAvailableServiceDefs(Arrays.asList("hdfs"))).thenReturn(RangerServerHealth.up().build());

        RangerServerHealth health = rangerHealthREST.getRangerServerReadiness();
        Assertions.assertEquals(UP, health.getStatus(), "readiness should be UP when service-defs exist");
        Mockito.verify(rangerServerHealthUtil).getServiceDefNames();
        Mockito.verify(rangerServerHealthUtil).serviceUpWithAvailableServiceDefs(Arrays.asList("hdfs"));
    }

    @Test
    public void testReadinessInitFailureWhenNoServiceDefs() {
        Mockito.when(rangerServerHealthUtil.resolveAuthenticatedLoginId()).thenReturn(RangerBizUtil.HEALTHCHECK_USERNAME);
        Mockito.when(bizUtil.isHealthCheckUser(RangerBizUtil.HEALTHCHECK_USERNAME)).thenReturn(true);
        Mockito.when(rangerServerHealthUtil.getServiceDefNames()).thenReturn(Collections.emptyList());
        Mockito.when(rangerServerHealthUtil.serviceInitFailure()).thenReturn(RangerServerHealth.initFailure().build());

        RangerServerHealth health = rangerHealthREST.getRangerServerReadiness();
        Assertions.assertEquals(INITIALIZATION_FAILURE, health.getStatus(),
                "readiness should report INITIALIZATION_FAILURE when no service-defs exist");
        Mockito.verify(rangerServerHealthUtil).getServiceDefNames();
    }

    @Test
    public void testReadinessForbiddenForOtherUser() {
        Mockito.when(rangerServerHealthUtil.resolveAuthenticatedLoginId()).thenReturn("admin");
        Mockito.when(bizUtil.isHealthCheckUser("admin")).thenReturn(false);
        Mockito.when(restErrorUtil.createRESTException(Mockito.eq(HttpServletResponse.SC_FORBIDDEN),
                Mockito.eq("Only the healthcheck user may query service-def names via this path."),
                Mockito.eq(true))).thenReturn(new WebApplicationException(HttpServletResponse.SC_FORBIDDEN));

        Assertions.assertThrows(WebApplicationException.class, () -> rangerHealthREST.getRangerServerReadiness());

        Mockito.verify(rangerServerHealthUtil, Mockito.never()).getServiceDefNames();
    }

    @Test
    public void testReadinessForbiddenWhenUnauthenticated() {
        Mockito.when(rangerServerHealthUtil.resolveAuthenticatedLoginId()).thenReturn(null);
        Mockito.when(bizUtil.isHealthCheckUser(isNull())).thenReturn(false);
        Mockito.when(restErrorUtil.createRESTException(Mockito.eq(HttpServletResponse.SC_FORBIDDEN),
                Mockito.eq("Only the healthcheck user may query service-def names via this path."),
                Mockito.eq(true))).thenReturn(new WebApplicationException(HttpServletResponse.SC_FORBIDDEN));

        Assertions.assertThrows(WebApplicationException.class, () -> rangerHealthREST.getRangerServerReadiness());

        Mockito.verify(rangerServerHealthUtil, Mockito.never()).getServiceDefNames();
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
