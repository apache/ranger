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

package org.apache.ranger.biz;

import org.apache.ranger.authorization.hadoop.config.RangerAdminConfig;
import org.apache.ranger.common.MessageEnums;
import org.apache.ranger.common.RESTErrorUtil;
import org.apache.ranger.db.RangerDaoManager;
import org.apache.ranger.db.XXPolicyExportAuditDao;
import org.apache.ranger.db.XXTrxLogV2Dao;
import org.apache.ranger.solr.SolrAccessAuditsService;
import org.apache.ranger.view.RangerAuditAdminMetricsByDays;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import javax.ws.rs.WebApplicationException;

import java.lang.reflect.Field;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link AuditMetricsDBStore}.
 */
@ExtendWith(MockitoExtension.class)
@TestMethodOrder(MethodOrderer.MethodName.class)
class TestAuditMetricsDBStore {
    @Mock
    private RangerAdminConfig rangerAdminConfig;

    @Mock
    private RangerDaoManager daoMgr;

    @Mock
    private RESTErrorUtil restErrorUtil;

    @Mock
    private SolrAccessAuditsService solrAccessAuditsService;

    @InjectMocks
    private AuditMetricsDBStore auditMetricsDBStore;

    private void injectConfig(RangerAdminConfig cfg) throws Exception {
        Field configField = AuditMetricsDBStore.class.getDeclaredField("config");
        configField.setAccessible(true);
        configField.set(auditMetricsDBStore, cfg);
    }

    @Test
    void testGetRangerAuditAccessMetricsByDays_returnsSolrResult() throws Exception {
        injectConfig(rangerAdminConfig);
        when(rangerAdminConfig.getInt(anyString(), anyInt())).thenAnswer(invocation -> invocation.getArgument(1));

        List<Map<String, Object>> expected = new ArrayList<>();
        expected.add(new HashMap<>());
        when(solrAccessAuditsService.getAuditAccessMetricsByDays(10, "UTC")).thenReturn(expected);

        List<Map<String, Object>> result = auditMetricsDBStore.getRangerAuditAccessMetricsByDays(10, "UTC");

        assertSame(expected, result);
    }

    @Test
    void testGetRangerAuditAccessMetricsByDays_throwsWhenDaysOutOfRange() throws Exception {
        injectConfig(rangerAdminConfig);
        when(rangerAdminConfig.getInt(eq("ranger.audit.metrics.max.supported.days"), eq(90))).thenReturn(90);
        when(restErrorUtil.createRESTException(eq("Invalid parameter: olderThanInDays must be between 1 and 90"), eq(MessageEnums.INVALID_INPUT_DATA)))
                .thenReturn(new WebApplicationException());

        assertThrows(WebApplicationException.class, () -> auditMetricsDBStore.getRangerAuditAccessMetricsByDays(0, "UTC"));
        assertThrows(WebApplicationException.class, () -> auditMetricsDBStore.getRangerAuditAccessMetricsByDays(91, "UTC"));
    }

    @Test
    void testGetRangerAuditAccessMetricsByDays_throwsOnInvalidTimezone() throws Exception {
        injectConfig(rangerAdminConfig);
        when(rangerAdminConfig.getInt(anyString(), anyInt())).thenAnswer(invocation -> invocation.getArgument(1));
        when(restErrorUtil.createRESTException(eq("Invalid parameter: timezone"), eq(MessageEnums.INVALID_INPUT_DATA)))
                .thenReturn(new WebApplicationException());

        assertThrows(WebApplicationException.class, () -> auditMetricsDBStore.getRangerAuditAccessMetricsByDays(7, "Invalid/Zone"));
    }

    @Test
    void testGetRangerAuditAdminMetricsByDays_usesExplicitActionsAndTypes() throws Exception {
        injectConfig(rangerAdminConfig);
        when(rangerAdminConfig.getInt(anyString(), anyInt())).thenAnswer(invocation -> invocation.getArgument(1));

        XXTrxLogV2Dao xxTrxLogV2Dao = mock(XXTrxLogV2Dao.class);
        when(daoMgr.getXXTrxLogV2()).thenReturn(xxTrxLogV2Dao);
        List<RangerAuditAdminMetricsByDays> expected = new ArrayList<>();
        when(xxTrxLogV2Dao.getRangerAuditAdminMetricsByDays(eq(5), anySet(), anyList(), eq(ZoneId.of("Asia/Kolkata")))).thenReturn(expected);

        List<String> actions = new ArrayList<>(Arrays.asList("create", "update"));
        List<String> types = new ArrayList<>(Arrays.asList("1002", "1003"));

        List<RangerAuditAdminMetricsByDays> result = auditMetricsDBStore.getRangerAuditAdminMetricsByDays(5, types, actions, "Asia/Kolkata");

        assertSame(expected, result);
    }

    @Test
    void testGetRangerAuditAdminMetricsByDays_usesDefaultsWhenFiltersEmpty() throws Exception {
        injectConfig(rangerAdminConfig);
        when(rangerAdminConfig.getInt(anyString(), anyInt())).thenAnswer(invocation -> invocation.getArgument(1));

        XXTrxLogV2Dao xxTrxLogV2Dao = mock(XXTrxLogV2Dao.class);
        when(daoMgr.getXXTrxLogV2()).thenReturn(xxTrxLogV2Dao);
        when(xxTrxLogV2Dao.getRangerAuditAdminMetricsByDays(eq(2), anySet(), anyList(), eq(ZoneId.of("UTC")))).thenReturn(new ArrayList<>());

        auditMetricsDBStore.getRangerAuditAdminMetricsByDays(2, new ArrayList<>(), null, "UTC");

        Set<Integer> expectedTypes = new HashSet<>(Arrays.asList(1002, 1003, 1020, 1030, 1057));
        verify(xxTrxLogV2Dao).getRangerAuditAdminMetricsByDays(eq(2), eq(expectedTypes),
                argThat(actions -> actions.size() == 3
                        && actions.containsAll(Arrays.asList("create", "update", "delete"))), eq(ZoneId.of("UTC")));
    }

    @Test
    void testGetRangerAuditAdminMetricsByDays_throwsOnDisallowedAction() throws Exception {
        injectConfig(rangerAdminConfig);
        when(rangerAdminConfig.getInt(anyString(), anyInt())).thenAnswer(invocation -> invocation.getArgument(1));
        List<String> badActions = new ArrayList<>(Arrays.asList("truncate"));
        when(restErrorUtil.createRESTException(contains("Invalid parameter: actions"), eq(MessageEnums.INVALID_INPUT_DATA)))
                .thenReturn(new WebApplicationException());

        assertThrows(WebApplicationException.class, () -> auditMetricsDBStore.getRangerAuditAdminMetricsByDays(5, null, badActions, "UTC"));
    }

    @Test
    void testGetRangerAuditAdminMetricsByDays_throwsOnInvalidObjectClassToken() throws Exception {
        injectConfig(rangerAdminConfig);
        when(rangerAdminConfig.getInt(anyString(), anyInt())).thenAnswer(invocation -> invocation.getArgument(1));
        List<String> types = new ArrayList<>(Arrays.asList("not-a-number"));
        when(restErrorUtil.createRESTException(eq("Invalid parameter: objectClassTypes must contain numeric values only"), eq(MessageEnums.INVALID_INPUT_DATA)))
                .thenReturn(new WebApplicationException());

        assertThrows(WebApplicationException.class, () -> auditMetricsDBStore.getRangerAuditAdminMetricsByDays(4, types, null, "UTC"));
    }

    @Test
    void testGetRangerAuditAdminMetricsByDays_throwsOnUnsupportedObjectClassType() throws Exception {
        injectConfig(rangerAdminConfig);
        when(rangerAdminConfig.getInt(anyString(), anyInt())).thenAnswer(invocation -> invocation.getArgument(1));
        List<String> types = new ArrayList<>(Arrays.asList("2000"));
        when(restErrorUtil.createRESTException(contains("Invalid parameter: objectClassTypes"), eq(MessageEnums.INVALID_INPUT_DATA)))
                .thenReturn(new WebApplicationException());

        assertThrows(WebApplicationException.class, () -> auditMetricsDBStore.getRangerAuditAdminMetricsByDays(4, types, null, "UTC"));
    }

    @Test
    void testGetRangerAuditAdminMetricsByDays_throwsWhenDaysInvalid() throws Exception {
        injectConfig(rangerAdminConfig);
        when(rangerAdminConfig.getInt(eq("ranger.audit.metrics.max.supported.days"), eq(90))).thenReturn(90);
        when(restErrorUtil.createRESTException(eq("Invalid parameter: olderThanInDays must be between 1 and 90"), eq(MessageEnums.INVALID_INPUT_DATA)))
                .thenReturn(new WebApplicationException());

        assertThrows(WebApplicationException.class, () -> auditMetricsDBStore.getRangerAuditAdminMetricsByDays(0, null, null, "UTC"));
    }

    @Test
    void testGetRangerPluginPolicySyncMetricsByDays_returnsFromDao() throws Exception {
        injectConfig(rangerAdminConfig);
        when(rangerAdminConfig.getInt(anyString(), anyInt())).thenAnswer(invocation -> invocation.getArgument(1));

        XXPolicyExportAuditDao exportAuditDao = mock(XXPolicyExportAuditDao.class);
        when(daoMgr.getXXPolicyExportAudit()).thenReturn(exportAuditDao);
        List<Map<String, Object>> expected = new ArrayList<>();
        when(exportAuditDao.getRangerPluginPolicySyncMetricsByDays(eq(8), eq(ZoneId.of("Asia/Kolkata")))).thenReturn(expected);

        assertSame(expected, auditMetricsDBStore.getRangerPluginPolicySyncMetricsByDays(8, "Asia/Kolkata"));
    }

    @Test
    void testGetRangerPluginPolicySyncMetricsByDays_throwsWhenDaysInvalid() throws Exception {
        injectConfig(rangerAdminConfig);
        when(rangerAdminConfig.getInt(eq("ranger.audit.metrics.max.supported.days"), eq(90))).thenReturn(90);
        when(restErrorUtil.createRESTException(eq("Invalid parameter: olderThanInDays must be between 1 and 90"), eq(MessageEnums.INVALID_INPUT_DATA)))
                .thenReturn(new WebApplicationException());

        assertThrows(WebApplicationException.class, () -> auditMetricsDBStore.getRangerPluginPolicySyncMetricsByDays(-1, "UTC"));
    }

    @Test
    void testInitStore_loadsConfig() throws Exception {
        try (MockedStatic<RangerAdminConfig> mocked = Mockito.mockStatic(RangerAdminConfig.class)) {
            mocked.when(RangerAdminConfig::getInstance).thenReturn(rangerAdminConfig);
            auditMetricsDBStore.initStore();

            Field configField = AuditMetricsDBStore.class.getDeclaredField("config");
            configField.setAccessible(true);
            assertSame(rangerAdminConfig, configField.get(auditMetricsDBStore));
        }
    }

    @Test
    void testInitStore_doesNotThrow() {
        try (MockedStatic<RangerAdminConfig> mocked = Mockito.mockStatic(RangerAdminConfig.class)) {
            mocked.when(RangerAdminConfig::getInstance).thenReturn(rangerAdminConfig);
            assertDoesNotThrow(() -> auditMetricsDBStore.initStore());
        }
    }
}
