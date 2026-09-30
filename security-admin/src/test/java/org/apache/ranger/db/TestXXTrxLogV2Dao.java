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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
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
    void testGetRangerAuditAdminMetricsByDays_TimezoneAdjustedSql() {
        when(daoManager.getEntityManager()).thenReturn(entityManager);
        when(entityManager.createNativeQuery(anyString())).thenReturn(query);
        when(query.setParameter(anyInt(), any())).thenReturn(query);
        when(query.getResultList()).thenReturn(Arrays.asList(
                new Object[] {AppConstants.CLASS_TYPE_RANGER_POLICY, 1L, 0L, 0L, Date.valueOf("2024-06-22")},
                new Object[] {AppConstants.CLASS_TYPE_RANGER_POLICY, 0L, 2L, 0L, Date.valueOf("2024-06-21")},
                new Object[] {AppConstants.CLASS_TYPE_XA_SERVICE, 3L, 4L, 5L, Date.valueOf("2024-06-20")},
                new Object[] {AppConstants.CLASS_TYPE_XA_USER, 0L, 6L, 7L, Date.valueOf("2024-06-19")},
                new Object[] {AppConstants.CLASS_TYPE_XA_GROUP, 8L, 0L, 9L, Date.valueOf("2024-06-18")}));

        List<RangerAuditAdminMetricsByDays> result;

        try (MockedStatic<RangerBizUtil> mocked = org.mockito.Mockito.mockStatic(RangerBizUtil.class)) {
            mocked.when(RangerBizUtil::getDBFlavor).thenReturn(AppConstants.DB_FLAVOR_POSTGRES);

            XXTrxLogV2Dao dao = new XXTrxLogV2Dao(daoManager);
            result = dao.getRangerAuditAdminMetricsByDays(7,
                    new HashSet<>(Arrays.asList(AppConstants.CLASS_TYPE_RANGER_POLICY, AppConstants.CLASS_TYPE_XA_SERVICE,
                            AppConstants.CLASS_TYPE_XA_USER, AppConstants.CLASS_TYPE_XA_GROUP)),
                    Arrays.asList("create", "update", "delete"),
                    ZoneId.of("Asia/Kolkata"));
        }

        ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
        verify(entityManager).createNativeQuery(sqlCaptor.capture());

        String sql = sqlCaptor.getValue();
        assertTrue(sql.contains("((create_time + (? * INTERVAL '1 minute'))::date) AS auditDate"));
        assertTrue(sql.contains("FROM x_trx_log_v2"));
        assertTrue(sql.contains("WHERE create_time >= ?"));
        assertTrue(sql.contains("GROUP BY class_type, auditDate"));

        verify(query).setParameter(1, 330);
        verify(query).setParameter(eq(2), isA(java.util.Date.class));

        assertEquals(5, result.size());
        assertMetric(result.get(0), AppConstants.CLASS_TYPE_RANGER_POLICY, 1L, 0L, 0L, Date.valueOf("2024-06-22").getTime());
        assertMetric(result.get(1), AppConstants.CLASS_TYPE_RANGER_POLICY, 0L, 2L, 0L, Date.valueOf("2024-06-21").getTime());
        assertMetric(result.get(2), AppConstants.CLASS_TYPE_XA_SERVICE, 3L, 4L, 5L, Date.valueOf("2024-06-20").getTime());
        assertMetric(result.get(3), AppConstants.CLASS_TYPE_XA_USER, 0L, 6L, 7L, Date.valueOf("2024-06-19").getTime());
        assertMetric(result.get(4), AppConstants.CLASS_TYPE_XA_GROUP, 8L, 0L, 9L, Date.valueOf("2024-06-18").getTime());
    }

    @Test
    void testGetRangerAuditAdminMetricsByDays_UsesDbSpecificDateExpression() {
        when(daoManager.getEntityManager()).thenReturn(entityManager);
        when(entityManager.createNativeQuery(anyString())).thenReturn(query);
        when(query.setParameter(anyInt(), any())).thenReturn(query);
        when(query.getResultList()).thenReturn(Collections.emptyList());

        int[] dbFlavors = {
                AppConstants.DB_FLAVOR_MYSQL,
                AppConstants.DB_FLAVOR_POSTGRES,
                AppConstants.DB_FLAVOR_ORACLE,
                AppConstants.DB_FLAVOR_SQLSERVER,
                AppConstants.DB_FLAVOR_SQLANYWHERE
        };
        String[] expectedExpressions = {
                "DATE(DATE_ADD(create_time, INTERVAL ? MINUTE)) AS auditDate",
                "((create_time + (? * INTERVAL '1 minute'))::date) AS auditDate",
                "TRUNC(create_time + (330 / 1440.0)) AS auditDate",
                "CAST(DATEADD(MINUTE, 330, create_time) AS date) AS auditDate",
                "CAST(DATEADD(MINUTE, 330, create_time) AS date) AS auditDate"
        };
        String[] expectedGroupBy = {
                "GROUP BY class_type, auditDate",
                "GROUP BY class_type, auditDate",
                "GROUP BY class_type, TRUNC(create_time + (330 / 1440.0))",
                "GROUP BY class_type, CAST(DATEADD(MINUTE, 330, create_time) AS date)",
                "GROUP BY class_type, CAST(DATEADD(MINUTE, 330, create_time) AS date)"
        };

        for (int dbFlavor : dbFlavors) {
            try (MockedStatic<RangerBizUtil> mocked = org.mockito.Mockito.mockStatic(RangerBizUtil.class)) {
                mocked.when(RangerBizUtil::getDBFlavor).thenReturn(dbFlavor);

                XXTrxLogV2Dao dao = new XXTrxLogV2Dao(daoManager);
                dao.getRangerAuditAdminMetricsByDays(7,
                        new HashSet<>(Arrays.asList(AppConstants.CLASS_TYPE_RANGER_POLICY, AppConstants.CLASS_TYPE_XA_SERVICE)),
                        Arrays.asList("create", "update", "delete"),
                        ZoneId.of("Asia/Kolkata"));
            }
        }

        ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
        verify(entityManager, times(dbFlavors.length)).createNativeQuery(sqlCaptor.capture());

        List<String> generatedSqls = sqlCaptor.getAllValues();
        for (int i = 0; i < expectedExpressions.length; i++) {
            assertTrue(generatedSqls.get(i).contains(expectedExpressions[i]), generatedSqls.get(i));
            assertTrue(generatedSqls.get(i).contains(expectedGroupBy[i]), generatedSqls.get(i));
            assertTrue(generatedSqls.get(i).contains("FROM x_trx_log_v2"), generatedSqls.get(i));
            assertTrue(generatedSqls.get(i).contains("WHERE create_time >= ?"), generatedSqls.get(i));
        }
    }

    @Test
    void testGetRangerAuditAdminMetricsByDays_BindsTimezoneParameterByDbFlavor() {
        when(daoManager.getEntityManager()).thenReturn(entityManager);
        when(entityManager.createNativeQuery(anyString())).thenReturn(query);
        when(query.setParameter(anyInt(), any())).thenReturn(query);
        when(query.getResultList()).thenReturn(Collections.emptyList());

        try (MockedStatic<RangerBizUtil> mocked = org.mockito.Mockito.mockStatic(RangerBizUtil.class)) {
            mocked.when(RangerBizUtil::getDBFlavor).thenReturn(AppConstants.DB_FLAVOR_MYSQL);

            XXTrxLogV2Dao dao = new XXTrxLogV2Dao(daoManager);
            dao.getRangerAuditAdminMetricsByDays(7,
                    new HashSet<>(Arrays.asList(AppConstants.CLASS_TYPE_RANGER_POLICY, AppConstants.CLASS_TYPE_XA_SERVICE)),
                    Arrays.asList("create", "update", "delete"),
                    ZoneId.of("Asia/Kolkata"));
        }

        verify(query).setParameter(1, 330);
        verify(query).setParameter(eq(2), isA(java.util.Date.class));

        reset(query);
        when(query.setParameter(anyInt(), any())).thenReturn(query);
        when(query.getResultList()).thenReturn(Collections.emptyList());

        try (MockedStatic<RangerBizUtil> mocked = org.mockito.Mockito.mockStatic(RangerBizUtil.class)) {
            mocked.when(RangerBizUtil::getDBFlavor).thenReturn(AppConstants.DB_FLAVOR_POSTGRES);

            XXTrxLogV2Dao dao = new XXTrxLogV2Dao(daoManager);
            dao.getRangerAuditAdminMetricsByDays(7,
                    new HashSet<>(Arrays.asList(AppConstants.CLASS_TYPE_RANGER_POLICY, AppConstants.CLASS_TYPE_XA_SERVICE)),
                    Arrays.asList("create", "update", "delete"),
                    ZoneId.of("Asia/Kolkata"));
        }

        verify(query).setParameter(1, 330);
        verify(query).setParameter(eq(2), isA(java.util.Date.class));

        int[] inlineOffsetFlavors = {
                AppConstants.DB_FLAVOR_ORACLE,
                AppConstants.DB_FLAVOR_SQLSERVER,
                AppConstants.DB_FLAVOR_SQLANYWHERE
        };

        for (int dbFlavor : inlineOffsetFlavors) {
            reset(query);
            when(query.setParameter(anyInt(), any())).thenReturn(query);
            when(query.getResultList()).thenReturn(Collections.emptyList());

            try (MockedStatic<RangerBizUtil> mocked = org.mockito.Mockito.mockStatic(RangerBizUtil.class)) {
                mocked.when(RangerBizUtil::getDBFlavor).thenReturn(dbFlavor);

                XXTrxLogV2Dao dao = new XXTrxLogV2Dao(daoManager);
                dao.getRangerAuditAdminMetricsByDays(7,
                        new HashSet<>(Arrays.asList(AppConstants.CLASS_TYPE_RANGER_POLICY, AppConstants.CLASS_TYPE_XA_SERVICE)),
                        Arrays.asList("create", "update", "delete"),
                        ZoneId.of("Asia/Kolkata"));
            }

            verify(query).setParameter(eq(1), isA(java.util.Date.class));
            verify(query, never()).setParameter(1, 330);
        }
    }

    private void assertMetric(RangerAuditAdminMetricsByDays metric, int objectClassType, long createCount,
            long updateCount, long deleteCount, long auditDate) {
        assertEquals(objectClassType, metric.getObjectClassType());
        assertEquals(createCount, metric.getCreateCount());
        assertEquals(updateCount, metric.getUpdateCount());
        assertEquals(deleteCount, metric.getDeleteCount());
        assertEquals(auditDate, metric.getAuditDate());
    }
}
