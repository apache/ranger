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

import org.apache.ranger.metrics.RangerMetricsFetcher;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class TestRangerAdminMetricsSourcePolicyMasking {
    private static final String EXPECTED_METRIC_PREFIX = "MaskingCount";

    @InjectMocks
    RangerAdminMetricsSourcePolicyMasking policyMaskingSource;

    @Mock
    RangerMetricsFetcher rangerMetricsFetcher;

    @Test
    public void testRefresh_WithValidMetrics() {
        RangerAdminMetricsSourcePolicyMasking spyPolicyMaskingSource = spy(policyMaskingSource);

        Map<String, Long> mockMetrics = new HashMap<>();
        mockMetrics.put("hdfs", 3L);
        mockMetrics.put("hive", 2L);
        mockMetrics.put("total", 5L);

        when(rangerMetricsFetcher.getPolicyMetrics(RangerPolicy.POLICY_TYPE_DATAMASK)).thenReturn(mockMetrics);

        spyPolicyMaskingSource.refresh();

        verify(spyPolicyMaskingSource).addMetricEntries(EXPECTED_METRIC_PREFIX, mockMetrics);
        verify(rangerMetricsFetcher).getPolicyMetrics(RangerPolicy.POLICY_TYPE_DATAMASK);
    }

    @Test
    public void testRefresh_WithEmptyMetrics() {
        RangerAdminMetricsSourcePolicyMasking spyPolicyMaskingSource = spy(policyMaskingSource);

        Map<String, Long> emptyMetrics = new HashMap<>();

        when(rangerMetricsFetcher.getPolicyMetrics(RangerPolicy.POLICY_TYPE_DATAMASK)).thenReturn(emptyMetrics);

        spyPolicyMaskingSource.refresh();

        verify(spyPolicyMaskingSource).addMetricEntries(EXPECTED_METRIC_PREFIX, emptyMetrics);
        verify(rangerMetricsFetcher).getPolicyMetrics(RangerPolicy.POLICY_TYPE_DATAMASK);
    }

    @Test
    public void testRefresh_WithNullMetrics() {
        when(rangerMetricsFetcher.getPolicyMetrics(RangerPolicy.POLICY_TYPE_DATAMASK)).thenReturn(null);

        assertThrows(NullPointerException.class, () -> {
            policyMaskingSource.refresh();
        });

        verify(rangerMetricsFetcher).getPolicyMetrics(RangerPolicy.POLICY_TYPE_DATAMASK);
    }
}
