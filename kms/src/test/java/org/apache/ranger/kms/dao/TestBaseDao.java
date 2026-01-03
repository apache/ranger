/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ranger.kms.dao;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import javax.persistence.EntityManager;
import javax.persistence.EntityTransaction;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@Disabled
public class TestBaseDao {
    @Mock
    private DaoManager daoManager;

    @Mock
    private EntityManager entityManager;

    @Mock
    private EntityTransaction entityTransaction;

    private BaseDao<Object> baseDao;

    @BeforeEach
    public void setup() {
        when(daoManager.getEntityManager()).thenReturn(entityManager);
        baseDao = new BaseDao<Object>(daoManager) {};
    }

    @Test
    public void testRollbackTransaction_whenEntityManagerAndTransactionAreNotNull() {
        when(entityManager.getTransaction()).thenReturn(entityTransaction);

        baseDao.rollbackTransaction();

        verify(entityTransaction, times(1)).rollback();
    }

    @Test
    public void testRollbackTransaction_whenEntityManagerIsNull() {
        when(daoManager.getEntityManager()).thenReturn(null);

        baseDao.rollbackTransaction();

        // No rollback expected, verify no interaction
        verify(entityTransaction, never()).rollback();
    }

    @Test
    public void testRollbackTransaction_whenTransactionIsNull() {
        when(entityManager.getTransaction()).thenReturn(null);

        baseDao.rollbackTransaction();

        verify(entityTransaction, never()).rollback();
    }

    @Test
    public void testCreate_withException_shouldRollback() {
        Object entity = new Object();
        when(entityManager.getTransaction()).thenReturn(entityTransaction);
        when(entityTransaction.isActive()).thenReturn(false);
        doThrow(new RuntimeException("fail")).when(entityManager).persist(entity);

        Object result = baseDao.create(entity);

        assertNull(result);
        verify(entityTransaction).rollback();
    }
}
