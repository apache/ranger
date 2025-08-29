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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class TestRangerAdminMetricsSourceUserGroup {
    private static final String USER_COUNT_METRIC_PREFIX = "UserCount";
    private static final String GROUP_COUNT_METRIC_NAME = "GroupCount";

    @InjectMocks
    RangerAdminMetricsSourceUserGroup userGroupSource;

    @Mock
    RangerMetricsFetcher rangerMetricsFetcher;

    @Test
    public void testRefresh_WithValidUserGroupMetrics() {
        RangerAdminMetricsSourceUserGroup spyUserGroupSource = spy(userGroupSource);

        Map<String, Long> mockUserMetrics = new HashMap<>();
        mockUserMetrics.put("admin", 3L);
        mockUserMetrics.put("user", 2L);
        mockUserMetrics.put("total", 5L);

        long mockGroupCount = 7L;

        when(rangerMetricsFetcher.getUserMetrics()).thenReturn(mockUserMetrics);
        when(rangerMetricsFetcher.getGroupCount()).thenReturn(mockGroupCount);

        spyUserGroupSource.refresh();

        verify(spyUserGroupSource).addMetricEntries(USER_COUNT_METRIC_PREFIX, mockUserMetrics);
        verify(spyUserGroupSource).addMetricEntry(GROUP_COUNT_METRIC_NAME, "", mockGroupCount);
        verify(rangerMetricsFetcher).getUserMetrics();
        verify(rangerMetricsFetcher).getGroupCount();
    }

    @Test
    public void testRefresh_WithEmptyMetrics() {
        RangerAdminMetricsSourceUserGroup spyUserGroupSource = spy(userGroupSource);

        Map<String, Long> mockUserMetrics = new HashMap<>();

        long mockGroupCount = 7L;

        when(rangerMetricsFetcher.getUserMetrics()).thenReturn(mockUserMetrics);
        when(rangerMetricsFetcher.getGroupCount()).thenReturn(mockGroupCount);

        spyUserGroupSource.refresh();

        verify(spyUserGroupSource).addMetricEntries(USER_COUNT_METRIC_PREFIX, mockUserMetrics);
        verify(spyUserGroupSource).addMetricEntry(GROUP_COUNT_METRIC_NAME, "", mockGroupCount);
        verify(rangerMetricsFetcher).getUserMetrics();
        verify(rangerMetricsFetcher).getGroupCount();
    }

    @Test
    public void testRefresh_WithNullUserMetrics() {
        when(rangerMetricsFetcher.getUserMetrics()).thenReturn(null);

        assertThrows(NullPointerException.class, () -> {
            userGroupSource.refresh();
        });

        verify(rangerMetricsFetcher).getUserMetrics();
        verify(rangerMetricsFetcher, never()).getGroupCount();
    }
}
