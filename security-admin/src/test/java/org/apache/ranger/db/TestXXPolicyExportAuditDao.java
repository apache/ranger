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

package org.apache.ranger.db;

import org.apache.ranger.biz.RangerBizUtil;
import org.apache.ranger.common.AppConstants;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;

import javax.persistence.EntityManager;
import javax.persistence.Query;

import java.sql.Date;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class TestXXPolicyExportAuditDao {
    @Mock
    private RangerDaoManager daoManager;

    @Mock
    private EntityManager entityManager;

    @Mock
    private Query query;

    @Test
    void testGetRangerPluginPolicySyncMetricsByDays_TimezoneAdjustedSql() {
        when(daoManager.getEntityManager()).thenReturn(entityManager);
        when(entityManager.createNativeQuery(anyString())).thenReturn(query);
        when(query.setParameter(anyInt(), any())).thenReturn(query);
        when(query.getResultList()).thenReturn(Arrays.asList(
                new Object[] {Date.valueOf("2024-06-21"), 3L},
                new Object[] {Date.valueOf("2024-06-22"), 5L}));

        List<Map<String, Object>> result;

        try (MockedStatic<RangerBizUtil> mocked = org.mockito.Mockito.mockStatic(RangerBizUtil.class)) {
            mocked.when(RangerBizUtil::getDBFlavor).thenReturn(AppConstants.DB_FLAVOR_POSTGRES);

            XXPolicyExportAuditDao dao = new XXPolicyExportAuditDao(daoManager);
            result = dao.getRangerPluginPolicySyncMetricsByDays(7, ZoneId.of("Asia/Kolkata"));
        }

        ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
        verify(entityManager).createNativeQuery(sqlCaptor.capture());

        String sql = sqlCaptor.getValue();
        assertTrue(sql.contains("((create_time + (? * INTERVAL '1 minute'))::date) AS auditDate"), sql);
        assertTrue(sql.contains("FROM x_policy_export_audit"), sql);
        assertTrue(sql.contains("WHERE create_time >= ?"), sql);
        assertTrue(sql.contains("COUNT(repository_name)"), sql);
        assertTrue(sql.contains("GROUP BY auditDate"), sql);

        verify(query).setParameter(1, 330);
        verify(query).setParameter(eq(2), isA(java.util.Date.class));

        assertEquals(2, result.size());
        assertEquals(Date.valueOf("2024-06-21").getTime(), result.get(0).get("auditDate"));
        assertEquals(3L, result.get(0).get("numberOfPolicySyncCount"));
        assertEquals(Date.valueOf("2024-06-22").getTime(), result.get(1).get("auditDate"));
        assertEquals(5L, result.get(1).get("numberOfPolicySyncCount"));
    }

    @Test
    void testGetRangerPluginPolicySyncMetricsByDays_MySqlBindsOffsetMinutes() {
        when(daoManager.getEntityManager()).thenReturn(entityManager);
        when(entityManager.createNativeQuery(anyString())).thenReturn(query);
        when(query.setParameter(anyInt(), any())).thenReturn(query);
        when(query.getResultList()).thenReturn(java.util.Collections.emptyList());

        try (MockedStatic<RangerBizUtil> mocked = org.mockito.Mockito.mockStatic(RangerBizUtil.class)) {
            mocked.when(RangerBizUtil::getDBFlavor).thenReturn(AppConstants.DB_FLAVOR_MYSQL);

            XXPolicyExportAuditDao dao = new XXPolicyExportAuditDao(daoManager);
            dao.getRangerPluginPolicySyncMetricsByDays(7, ZoneId.of("Asia/Kolkata"));
        }

        ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
        verify(entityManager).createNativeQuery(sqlCaptor.capture());

        assertTrue(sqlCaptor.getValue().contains("DATE(DATE_ADD(create_time, INTERVAL ? MINUTE)) AS auditDate"), sqlCaptor.getValue());

        verify(query).setParameter(1, 330);
        verify(query).setParameter(eq(2), isA(java.util.Date.class));
    }

    @Test
    void testGetRangerPluginPolicySyncMetricsByDays_OracleInlinesOffsetAndGroupByExpression() {
        when(daoManager.getEntityManager()).thenReturn(entityManager);
        when(entityManager.createNativeQuery(anyString())).thenReturn(query);
        when(query.setParameter(anyInt(), any())).thenReturn(query);
        when(query.getResultList()).thenReturn(java.util.Collections.emptyList());

        try (MockedStatic<RangerBizUtil> mocked = org.mockito.Mockito.mockStatic(RangerBizUtil.class)) {
            mocked.when(RangerBizUtil::getDBFlavor).thenReturn(AppConstants.DB_FLAVOR_ORACLE);

            XXPolicyExportAuditDao dao = new XXPolicyExportAuditDao(daoManager);
            dao.getRangerPluginPolicySyncMetricsByDays(7, ZoneId.of("Asia/Kolkata"));
        }

        ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
        verify(entityManager).createNativeQuery(sqlCaptor.capture());

        String sql = sqlCaptor.getValue();
        assertTrue(sql.contains("TRUNC(create_time + (330 / 1440.0)) AS auditDate"), sql);
        assertTrue(sql.contains("GROUP BY repository_name, agent_id, client_ip, http_ret_code, TRUNC(create_time + (330 / 1440.0))"), sql);
        assertTrue(sql.contains("GROUP BY auditDate"), sql);

        verify(query).setParameter(eq(1), isA(java.util.Date.class));
        verify(query, never()).setParameter(1, 330);
    }
}
