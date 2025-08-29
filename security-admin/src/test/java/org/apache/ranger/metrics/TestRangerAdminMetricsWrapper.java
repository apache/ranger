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

import org.apache.ranger.metrics.source.RangerAdminMetricsSourceContextEnricher;
import org.apache.ranger.metrics.source.RangerAdminMetricsSourceDenyConditions;
import org.apache.ranger.metrics.source.RangerAdminMetricsSourcePolicyMasking;
import org.apache.ranger.metrics.source.RangerAdminMetricsSourcePolicyResourceAccess;
import org.apache.ranger.metrics.source.RangerAdminMetricsSourcePolicyRowFiltering;
import org.apache.ranger.metrics.source.RangerAdminMetricsSourceService;
import org.apache.ranger.metrics.source.RangerAdminMetricsSourceUserGroup;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.anyList;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class TestRangerAdminMetricsWrapper {
    @InjectMocks
    RangerAdminMetricsWrapper metricsWrapper;

    @Mock
    RangerAdminMetricsSourceUserGroup userGroupSource;

    @Mock
    RangerAdminMetricsSourceService serviceSource;

    @Mock
    RangerAdminMetricsSourcePolicyResourceAccess policyResourceAccessSource;

    @Mock
    RangerAdminMetricsSourcePolicyRowFiltering policyRowFilteringSource;

    @Mock
    RangerAdminMetricsSourcePolicyMasking policyMaskingSource;

    @Mock
    RangerAdminMetricsSourceContextEnricher contextEnricherSource;

    @Mock
    RangerAdminMetricsSourceDenyConditions denyConditionSource;

    @Mock
    RangerMetricsSystemWrapper metricsSystemWrapper;

    @BeforeEach
    public void setUp() throws Exception {
        Field field = RangerAdminMetricsWrapper.class.getDeclaredField("rangerMetricsSystemWrapper");
        field.setAccessible(true);
        field.set(metricsWrapper, metricsSystemWrapper);
    }

    @Test
    public void testInit() {
        metricsWrapper.init();

        verify(metricsSystemWrapper).init(eq("admin"), anyList(), eq(Collections.emptyList()));
    }

    @Test
    public void testInitWithException() throws Exception {
        doThrow(new RuntimeException("Exception occured while initializing Metric Starter")).when(metricsSystemWrapper).init(anyString(), anyList(), anyList());

        metricsWrapper.init();

        verify(metricsSystemWrapper).init(anyString(), anyList(), anyList());
    }

    @Test
    public void testGetRangerMetricsInPrometheusFormat() throws Exception {
        String mockOutput = "mock_prometheus_metrics";
        when(metricsSystemWrapper.getRangerMetricsInPrometheusFormat()).thenReturn(mockOutput);

        String result = metricsWrapper.getRangerMetricsInPrometheusFormat();

        assertEquals(mockOutput, result);
        verify(metricsSystemWrapper).getRangerMetricsInPrometheusFormat();
    }

    @Test
    public void testGetRangerMetricsInPrometheusFormatWithException() throws Exception {
        when(metricsSystemWrapper.getRangerMetricsInPrometheusFormat()).thenThrow(new IOException("Exception occurred while getting metric."));

        IOException exception = assertThrows(IOException.class, () -> {
            metricsWrapper.getRangerMetricsInPrometheusFormat();
        });

        assertEquals("Exception occurred while getting metric.", exception.getMessage());
        verify(metricsSystemWrapper).getRangerMetricsInPrometheusFormat();
    }

    @Test
    public void testGetRangerMetricsInPrometheusFormat_NullReturn() throws Exception {
        when(metricsSystemWrapper.getRangerMetricsInPrometheusFormat()).thenReturn(null);

        String result = metricsWrapper.getRangerMetricsInPrometheusFormat();

        assertNull(result);
        verify(metricsSystemWrapper).getRangerMetricsInPrometheusFormat();
    }

    @Test
    public void testGetRangerMetrics() {
        Map<String, Map<String, Object>> mockMetrics = new HashMap<>();
        Map<String, Object> userGroupMetrics = new HashMap<>();
        userGroupMetrics.put("total_users", 100);
        userGroupMetrics.put("total_groups", 20);
        mockMetrics.put("UserGroup", userGroupMetrics);

        Map<String, Object> serviceMetrics = new HashMap<>();
        serviceMetrics.put("total_services", 5);
        serviceMetrics.put("active_services", 4);
        mockMetrics.put("Service", serviceMetrics);

        when(metricsSystemWrapper.getRangerMetrics()).thenReturn(mockMetrics);

        Map<String, Map<String, Object>> result = metricsWrapper.getRangerMetrics();

        assertNotNull(result);
        assertEquals(mockMetrics, result);
        assertEquals(2, result.size());
        assertTrue(result.containsKey("UserGroup"));
        assertTrue(result.containsKey("Service"));
        assertEquals(100, result.get("UserGroup").get("total_users"));
        assertEquals(5, result.get("Service").get("total_services"));
        verify(metricsSystemWrapper).getRangerMetrics();
    }

    @Test
    public void testGetRangerMetricsWithEmptyResult() {
        when(metricsSystemWrapper.getRangerMetrics()).thenReturn(new HashMap<>());

        Map<String, Map<String, Object>> result = metricsWrapper.getRangerMetrics();

        assertNotNull(result);
        assertTrue(result.isEmpty());
        verify(metricsSystemWrapper).getRangerMetrics();
    }

    @Test
    public void testGetRangerMetricsWithNullResult() {
        when(metricsSystemWrapper.getRangerMetrics()).thenReturn(null);

        Map<String, Map<String, Object>> result = metricsWrapper.getRangerMetrics();

        assertNull(result);
        verify(metricsSystemWrapper).getRangerMetrics();
    }
}
