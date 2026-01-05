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

package org.apache.ranger.metrics;

import org.apache.ranger.biz.ServiceDBStore;
import org.apache.ranger.biz.XUserMgr;
import org.apache.ranger.common.RangerConstants;
import org.apache.ranger.service.XGroupService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class TestRangerMetricsFetcher {
    @InjectMocks
    RangerMetricsFetcher metricsFetcher;

    @Mock
    XUserMgr xUserMgr;

    @Mock
    ServiceDBStore svcStore;

    @Mock
    XGroupService groupService;

    @Test
    public void testGetGroupCount() {
        when(groupService.getAllGroupCount()).thenReturn(5L);
        Long result = metricsFetcher.getGroupCount();

        assertEquals(5L, result);
        verify(groupService).getAllGroupCount();
    }

    @Test
    public void testGetGroupCount_NullReturn() {
        when(groupService.getAllGroupCount()).thenReturn(null);

        Long result = metricsFetcher.getGroupCount();

        assertNull(result);
        verify(groupService).getAllGroupCount();
    }

    @Test
    public void testGetUserMetrics() {
        Map<String, Long> mockData = new HashMap<>();
        mockData.put(RangerConstants.ROLE_SYS_ADMIN, 2L);
        mockData.put(RangerConstants.ROLE_ADMIN_AUDITOR, 3L);
        mockData.put(RangerConstants.ROLE_KEY_ADMIN, 4L);
        mockData.put(RangerConstants.ROLE_KEY_ADMIN_AUDITOR, 5L);
        mockData.put(RangerConstants.ROLE_USER, 6L);
        mockData.put("INVALID_ROLE", 1L);

        when(xUserMgr.getUserCountByRole()).thenReturn(mockData);

        Map<String, Long> result = metricsFetcher.getUserMetrics();

        assertEquals(2L, result.get("SysAdmin"));
        assertEquals(3L, result.get("AdminAuditor"));
        assertEquals(4L, result.get("KeyAdmin"));
        assertEquals(5L, result.get("KeyAdminAuditor"));
        assertEquals(6L, result.get("User"));
        assertEquals(21L, result.get("Total"));

        assertFalse(result.containsKey("INVALID_ROLE"));
        verify(xUserMgr).getUserCountByRole();
    }

    @Test
    public void testGetUserMetrics_WithNullMap() {
        when(xUserMgr.getUserCountByRole()).thenReturn(null);

        assertThrows(NullPointerException.class, () -> {
            metricsFetcher.getUserMetrics();
        });
    }

    @Test
    public void testGetRangerServiceMetrics() {
        Map<String, Long> mockData = new HashMap<>();
        mockData.put("hdfs", 2L);
        mockData.put("hive", 3L);

        when(svcStore.getServiceCountByType()).thenReturn(mockData);

        Map<String, Long> result = metricsFetcher.getRangerServiceMetrics();

        assertEquals(2L, result.get("hdfs"));
        assertEquals(3L, result.get("hive"));
        assertEquals(5L, result.get("Total"));
        verify(svcStore).getServiceCountByType();
    }

    @Test
    public void testGetRangerServiceMetrics_EmptyMap() {
        when(svcStore.getServiceCountByType()).thenReturn(new HashMap<>());

        Map<String, Long> result = metricsFetcher.getRangerServiceMetrics();

        assertNotNull(result);
        assertEquals(0L, result.get("Total"));
    }

    @Test
    public void testGetPolicyMetrics() {
        int policyType = 0;
        Map<String, Long> mockData = new HashMap<>();
        mockData.put("hdfs", 4L);
        mockData.put("hive", 6L);

        when(svcStore.getPolicyCountByTypeAndServiceType(policyType)).thenReturn(mockData);

        Map<String, Long> result = metricsFetcher.getPolicyMetrics(policyType);

        assertEquals(4L, result.get("hdfs"));
        assertEquals(6L, result.get("hive"));
        assertEquals(10L, result.get("Total"));
        verify(svcStore).getPolicyCountByTypeAndServiceType(policyType);
    }

    @Test
    public void testGetPolicyMetrics_EmptyData() {
        int policyType = 0;
        when(svcStore.getPolicyCountByTypeAndServiceType(policyType)).thenReturn(new HashMap<>());

        Map<String, Long> result = metricsFetcher.getPolicyMetrics(policyType);

        assertNotNull(result);
        assertEquals(0L, result.get("Total"));
    }

    @Test
    public void testGetPolicyMetrics_NullType_ThrowsException() {
        NullPointerException ex = assertThrows(NullPointerException.class, () -> {
            metricsFetcher.getPolicyMetrics(null);
        });

        assertEquals("Policy type must not be null to get policy metrics.", ex.getMessage());
    }

    @Test
    public void testGetDenyConditionsMetrics() {
        Map<String, Long> mockData = new HashMap<>();
        mockData.put("hdfs", 2L);
        mockData.put("hive", 3L);

        when(svcStore.getPolicyCountByDenyConditionsAndServiceDef()).thenReturn(mockData);

        Map<String, Long> result = metricsFetcher.getDenyConditionsMetrics();

        assertEquals(5L, result.get("Total"));
        verify(svcStore).getPolicyCountByDenyConditionsAndServiceDef();
    }

    @Test
    public void testGetDenyConditionsMetrics_NullMap() {
        when(svcStore.getPolicyCountByDenyConditionsAndServiceDef()).thenReturn(null);

        assertThrows(NullPointerException.class, () -> {
            metricsFetcher.getDenyConditionsMetrics();
        });
    }

    @Test
    public void testGetContextEnrichersMetrics() {
        List<String> serviceDefs = Arrays.asList("hdfs", "hive", "kafka");

        when(svcStore.findAllServiceDefNamesHavingContextEnrichers()).thenReturn(serviceDefs);

        Map<String, Long> result = metricsFetcher.getContextEnrichersMetrics();

        assertEquals(1L, result.get("hdfs"));
        assertEquals(1L, result.get("hive"));
        assertEquals(1L, result.get("kafka"));
        assertEquals(3L, result.get("Total"));
        verify(svcStore).findAllServiceDefNamesHavingContextEnrichers();
    }

    @Test
    public void testGetContextEnrichersMetrics_EmptyList() {
        when(svcStore.findAllServiceDefNamesHavingContextEnrichers()).thenReturn(Collections.emptyList());

        Map<String, Long> result = metricsFetcher.getContextEnrichersMetrics();

        assertNotNull(result);
        assertEquals(1, result.size());
        assertTrue(result.containsKey("Total"));
        assertEquals(0L, result.get("Total"));
    }
}
