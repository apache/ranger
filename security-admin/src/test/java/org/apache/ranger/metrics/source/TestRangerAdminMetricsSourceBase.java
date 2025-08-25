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

package org.apache.ranger.metrics.source;

import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.ranger.metrics.RangerMetricsInfo;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class TestRangerAdminMetricsSourceBase {
    private static final String TEST_CONTEXT = "test-context";
    private static final String TEST_RECORD = "test-record";

    @Mock
    MetricsCollector metricsCollector;

    @Mock
    MetricsRecordBuilder metricsRecordBuilder;

    private TestableRangerAdminMetricsSourceBase metricsSource;

    private static class TestableRangerAdminMetricsSourceBase extends RangerAdminMetricsSourceBase {
        public TestableRangerAdminMetricsSourceBase(String context, String record) {
            super(context, record);
        }

        @Override
        protected void refresh() {
            // No-op for testing
        }

        public void testAddMetricEntry(String prefix, String suffix, Long value) {
            addMetricEntry(prefix, suffix, value);
        }

        public void testAddMetricEntries(String prefix, Map<String, Long> metrics) {
            addMetricEntries(prefix, metrics);
        }

        public Map<String, Long> getMetricsMap() {
            return metricsMap;
        }

        public String getContext() {
            return context;
        }

        public String getRecord() {
            return record;
        }
    }

    @BeforeEach
    public void setUp() {
        metricsSource = new TestableRangerAdminMetricsSourceBase(TEST_CONTEXT, TEST_RECORD);
    }

    @Test
    public void testConstructor() {
        assertEquals(TEST_CONTEXT, metricsSource.getContext());
        assertEquals(TEST_RECORD, metricsSource.getRecord());
        assertNotNull(metricsSource.getMetricsMap());
        assertTrue(metricsSource.getMetricsMap().isEmpty());
    }

    @Test
    public void testAddMetricEntry_WithNormalPrefixAndSuffix() {
        String prefix = "TestMetric";
        String suffix = "Count";
        Long value = 100L;

        metricsSource.testAddMetricEntry(prefix, suffix, value);

        Map<String, Long> metricsMap = metricsSource.getMetricsMap();
        assertEquals(1, metricsMap.size());
        assertEquals(value, metricsMap.get("TestMetricCOUNT"));
    }

    @Test
    public void testAddMetricEntry_WithNullSuffix() {
        String prefix = "TestMetric";
        String suffix = null;
        Long value = 200L;

        metricsSource.testAddMetricEntry(prefix, suffix, value);

        Map<String, Long> metricsMap = metricsSource.getMetricsMap();
        assertEquals(1, metricsMap.size());
        assertEquals(value, metricsMap.get("TestMetric"));
    }

    @Test
    public void testAddMetricEntry_WithEmptySuffix() {
        String prefix = "TestMetric";
        String suffix = "";
        Long value = 300L;

        metricsSource.testAddMetricEntry(prefix, suffix, value);

        Map<String, Long> metricsMap = metricsSource.getMetricsMap();
        assertEquals(1, metricsMap.size());
        assertEquals(value, metricsMap.get("TestMetric"));
    }

    @Test
    public void testAddMetricEntry_WithTotalSuffix() {
        String prefix = "TestMetric";
        String suffix = "Total";
        Long value = 400L;

        metricsSource.testAddMetricEntry(prefix, suffix, value);

        Map<String, Long> metricsMap = metricsSource.getMetricsMap();
        assertEquals(1, metricsMap.size());
        assertEquals(value, metricsMap.get("TestMetric"));
    }

    @Test
    public void testAddMetricEntry_WithTotalSuffixCaseInsensitive() {
        String prefix = "TestMetric";
        String suffix = "total";
        Long value = 500L;

        metricsSource.testAddMetricEntry(prefix, suffix, value);

        Map<String, Long> metricsMap = metricsSource.getMetricsMap();
        assertEquals(1, metricsMap.size());
        assertEquals(value, metricsMap.get("TestMetric"));
    }

    @Test
    public void testAddMetricEntry_WithNullPrefix() {
        String prefix = null;
        String suffix = "Count";
        Long value = 600L;

        metricsSource.testAddMetricEntry(prefix, suffix, value);

        Map<String, Long> metricsMap = metricsSource.getMetricsMap();
        assertTrue(metricsMap.isEmpty());
    }

    @Test
    public void testAddMetricEntry_WithNullValue() {
        String prefix = "TestMetric";
        String suffix = "Count";
        Long value = null;

        metricsSource.testAddMetricEntry(prefix, suffix, value);

        Map<String, Long> metricsMap = metricsSource.getMetricsMap();
        assertEquals(1, metricsMap.size());
        assertNull(metricsMap.get("TestMetricCOUNT"));
    }

    @Test
    public void testAddMetricEntry_MultipleEntries() {
        metricsSource.testAddMetricEntry("Metric1", "Count", 10L);
        metricsSource.testAddMetricEntry("Metric2", "Size", 20L);
        metricsSource.testAddMetricEntry("Metric3", null, 30L);
        metricsSource.testAddMetricEntry("Metric4", "Total", 40L);

        Map<String, Long> metricsMap = metricsSource.getMetricsMap();
        assertEquals(4, metricsMap.size());
        assertEquals(10L, metricsMap.get("Metric1COUNT"));
        assertEquals(20L, metricsMap.get("Metric2SIZE"));
        assertEquals(30L, metricsMap.get("Metric3"));
        assertEquals(40L, metricsMap.get("Metric4"));
    }

    @Test
    public void testAddMetricEntries_WithValidMap() {
        Map<String, Long> metrics = new HashMap<>();
        metrics.put("User", 10L);
        metrics.put("Admin", 5L);
        metrics.put("Total", 15L);

        metricsSource.testAddMetricEntries("Count", metrics);

        Map<String, Long> metricsMap = metricsSource.getMetricsMap();
        assertTrue(metricsMap.containsKey("CountUSER"));
        assertTrue(metricsMap.containsKey("CountADMIN"));
        assertTrue(metricsMap.containsKey("Count"));
        assertEquals(3, metricsMap.size());
        assertEquals(10L, metricsMap.get("CountUSER"));
        assertEquals(5L,  metricsMap.get("CountADMIN"));
        assertEquals(15L, metricsMap.get("Count"));
    }

    @Test
    public void testAddMetricEntries_WithEmptyMap() {
        Map<String, Long> metrics = new HashMap<>();

        metricsSource.testAddMetricEntries("Count", metrics);

        Map<String, Long> metricsMap = metricsSource.getMetricsMap();
        assertTrue(metricsMap.isEmpty());
    }

    @Test
    public void testAddMetricEntries_WithNullMap() {
        assertThrows(NullPointerException.class, () -> {
            metricsSource.testAddMetricEntries("Count", null);
        });
    }

    @Test
    public void testAddMetricEntries_WithNullPrefix() {
        Map<String, Long> metrics = new HashMap<>();
        metrics.put("User", 10L);

        metricsSource.testAddMetricEntries(null, metrics);

        Map<String, Long> metricsMap = metricsSource.getMetricsMap();

        assertTrue(metricsMap.isEmpty());
    }

    @Test
    public void testUpdate_WithMetrics() {
        metricsSource.testAddMetricEntry("TestMetric1", "Count", 100L);
        metricsSource.testAddMetricEntry("TestMetric2", "Size", 200L);

        when(metricsCollector.addRecord(TEST_RECORD)).thenReturn(metricsRecordBuilder);
        when(metricsRecordBuilder.setContext(TEST_CONTEXT)).thenReturn(metricsRecordBuilder);
        when(metricsRecordBuilder.addGauge(any(RangerMetricsInfo.class), any(Long.class))).thenReturn(metricsRecordBuilder);

        metricsSource.update(metricsCollector, true);

        verify(metricsCollector).addRecord(TEST_RECORD);
        verify(metricsRecordBuilder).setContext(TEST_CONTEXT);
        verify(metricsRecordBuilder, times(2)).addGauge(any(RangerMetricsInfo.class), any(Long.class));

        ArgumentCaptor<RangerMetricsInfo> infoCaptor = ArgumentCaptor.forClass(RangerMetricsInfo.class);
        ArgumentCaptor<Long> valueCaptor = ArgumentCaptor.forClass(Long.class);
        verify(metricsRecordBuilder, times(2)).addGauge(infoCaptor.capture(), valueCaptor.capture());
        assertTrue(infoCaptor.getAllValues().stream().anyMatch(info -> "TestMetric1COUNT".equals(info.name())));
        assertTrue(infoCaptor.getAllValues().stream().anyMatch(info -> "TestMetric2SIZE".equals(info.name())));
        assertTrue(valueCaptor.getAllValues().contains(100L));
        assertTrue(valueCaptor.getAllValues().contains(200L));
    }

    @Test
    public void testUpdate_WithEmptyMetrics() {
        when(metricsCollector.addRecord(TEST_RECORD)).thenReturn(metricsRecordBuilder);
        when(metricsRecordBuilder.setContext(TEST_CONTEXT)).thenReturn(metricsRecordBuilder);

        metricsSource.update(metricsCollector, false);

        verify(metricsCollector).addRecord(TEST_RECORD);
        verify(metricsRecordBuilder).setContext(TEST_CONTEXT);
        verify(metricsRecordBuilder, never()).addGauge(any(RangerMetricsInfo.class), any(Long.class));
    }

    @Test
    public void testUpdate_BooleanParameterIgnored() {
        metricsSource.testAddMetricEntry("TestMetric", "Count", 50L);

        when(metricsCollector.addRecord(TEST_RECORD)).thenReturn(metricsRecordBuilder);
        when(metricsRecordBuilder.setContext(TEST_CONTEXT)).thenReturn(metricsRecordBuilder);
        when(metricsRecordBuilder.addGauge(any(RangerMetricsInfo.class), any(Long.class))).thenReturn(metricsRecordBuilder);

        metricsSource.update(metricsCollector, true);
        metricsSource.update(metricsCollector, false);

        verify(metricsCollector, times(2)).addRecord(TEST_RECORD);
        verify(metricsRecordBuilder, times(2)).setContext(TEST_CONTEXT);
        verify(metricsRecordBuilder, times(2)).addGauge(any(RangerMetricsInfo.class), eq(50L));
    }
}
