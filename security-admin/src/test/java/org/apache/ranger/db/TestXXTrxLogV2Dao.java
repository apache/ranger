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
import org.apache.ranger.view.RangerAuditAdminMetricsByDays;
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
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

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
class TestXXTrxLogV2Dao {
    @Mock
    private RangerDaoManager daoManager;

    @Mock
    private EntityManager entityManager;

    @Mock
    private Query query;

    @Test
    void testGetRangerAuditAdminMetricsByDays_PostgresTimezoneSql() {
        when(daoManager.getEntityManager()).thenReturn(entityManager);
        when(entityManager.createNativeQuery(anyString())).thenReturn(query);
        when(query.setParameter(anyInt(), any())).thenReturn(query);
        when(query.getResultList()).thenReturn(Collections.singletonList(
                new Object[] {1020, 1L, 2L, 0L, Date.valueOf("2024-06-21")}));

        Set<Integer> classTypes = new HashSet<>(Arrays.asList(1020, 1030));
        List<String> actions = Arrays.asList("create", "update");
        List<RangerAuditAdminMetricsByDays> result;

        try (MockedStatic<RangerBizUtil> mocked = org.mockito.Mockito.mockStatic(RangerBizUtil.class)) {
            mocked.when(RangerBizUtil::getDBFlavor).thenReturn(AppConstants.DB_FLAVOR_POSTGRES);

            XXTrxLogV2Dao dao = new XXTrxLogV2Dao(daoManager);
            result = dao.getRangerAuditAdminMetricsByDays(7, classTypes, actions, ZoneId.of("Asia/Kolkata"));
        }

        ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
        verify(entityManager).createNativeQuery(sqlCaptor.capture());

        String sql = sqlCaptor.getValue();
        assertTrue(sql.contains("FROM x_trx_log_v2"), sql);
        assertTrue(sql.contains("((create_time + (? * INTERVAL '1 minute'))::date) AS auditDate"), sql);
        assertTrue(sql.contains("class_type IN (?,?)"), sql);
        assertTrue(sql.contains("action IN (?,?)"), sql);

        verify(query).setParameter(1, 330);
        verify(query).setParameter(eq(2), isA(java.util.Date.class));

        assertEquals(1, result.size());
        assertEquals(1020, result.get(0).getObjectClassType());
        assertEquals(1L, result.get(0).getCreateCount());
        assertEquals(2L, result.get(0).getUpdateCount());
        assertEquals(Date.valueOf("2024-06-21").getTime(), result.get(0).getAuditDate());
    }

    @Test
    void testGetRangerAuditAdminMetricsByDays_MySqlBindsOffsetMinutes() {
        when(daoManager.getEntityManager()).thenReturn(entityManager);
        when(entityManager.createNativeQuery(anyString())).thenReturn(query);
        when(query.setParameter(anyInt(), any())).thenReturn(query);
        when(query.getResultList()).thenReturn(java.util.Collections.emptyList());

        Set<Integer> classTypes = new HashSet<>(Arrays.asList(1020));
        List<String> actions = Arrays.asList("create");

        try (MockedStatic<RangerBizUtil> mocked = org.mockito.Mockito.mockStatic(RangerBizUtil.class)) {
            mocked.when(RangerBizUtil::getDBFlavor).thenReturn(AppConstants.DB_FLAVOR_MYSQL);

            XXTrxLogV2Dao dao = new XXTrxLogV2Dao(daoManager);
            dao.getRangerAuditAdminMetricsByDays(7, classTypes, actions, ZoneId.of("Asia/Kolkata"));
        }

        ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
        verify(entityManager).createNativeQuery(sqlCaptor.capture());

        assertTrue(sqlCaptor.getValue().contains("DATE(DATE_ADD(create_time, INTERVAL ? MINUTE)) AS auditDate"), sqlCaptor.getValue());
        verify(query).setParameter(1, 330);
    }

    @Test
    void testGetRangerAuditAdminMetricsByDays_OracleInlinesOffset() {
        when(daoManager.getEntityManager()).thenReturn(entityManager);
        when(entityManager.createNativeQuery(anyString())).thenReturn(query);
        when(query.setParameter(anyInt(), any())).thenReturn(query);
        when(query.getResultList()).thenReturn(java.util.Collections.emptyList());

        Set<Integer> classTypes = new HashSet<>(Arrays.asList(1020));
        List<String> actions = Arrays.asList("create");

        try (MockedStatic<RangerBizUtil> mocked = org.mockito.Mockito.mockStatic(RangerBizUtil.class)) {
            mocked.when(RangerBizUtil::getDBFlavor).thenReturn(AppConstants.DB_FLAVOR_ORACLE);

            XXTrxLogV2Dao dao = new XXTrxLogV2Dao(daoManager);
            dao.getRangerAuditAdminMetricsByDays(7, classTypes, actions, ZoneId.of("Asia/Kolkata"));
        }

        ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
        verify(entityManager).createNativeQuery(sqlCaptor.capture());

        String sql = sqlCaptor.getValue();
        assertTrue(sql.contains("TRUNC(create_time + (330 / 1440.0)) AS auditDate"), sql);
        assertTrue(sql.contains("GROUP BY class_type, TRUNC(create_time + (330 / 1440.0))"), sql);

        verify(query).setParameter(eq(1), isA(java.util.Date.class));
        verify(query, never()).setParameter(1, 330);
    }
}
