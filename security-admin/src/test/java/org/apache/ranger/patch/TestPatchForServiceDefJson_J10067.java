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

package org.apache.ranger.patch;

import org.apache.ranger.db.RangerDaoManager;
import org.apache.ranger.db.XXServiceDefDao;
import org.apache.ranger.entity.XXServiceDef;
import org.apache.ranger.service.RangerServiceDefService;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionStatus;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;

@ExtendWith(MockitoExtension.class)
public class TestPatchForServiceDefJson_J10067 {
    private RangerDaoManager               daoMgr;
    private XXServiceDefDao                xServiceDefDao;
    private RangerServiceDefService        serviceDefService;
    private PatchForServiceDefJson_J10067  patch;

    @BeforeEach
    public void setUp() {
        daoMgr            = Mockito.mock(RangerDaoManager.class);
        xServiceDefDao    = Mockito.mock(XXServiceDefDao.class);
        serviceDefService = Mockito.mock(RangerServiceDefService.class);

        PlatformTransactionManager txManager = Mockito.mock(PlatformTransactionManager.class);

        Mockito.lenient().when(txManager.getTransaction(Mockito.any())).thenReturn(Mockito.mock(TransactionStatus.class));
        Mockito.when(daoMgr.getXXServiceDef()).thenReturn(xServiceDefDao);

        patch                   = new PatchForServiceDefJson_J10067();
        patch.daoMgr            = daoMgr;
        patch.serviceDefService = serviceDefService;
        patch.txManager         = txManager;
    }

    @Test
    public void testStoresJsonOnlyForServiceDefsWithoutIt() {
        XXServiceDef withoutJson = serviceDef(1L, "hive", null);
        XXServiceDef withJson    = serviceDef(2L, "hdfs", "{\"name\":\"hdfs\"}");

        Mockito.when(xServiceDefDao.getAll()).thenReturn(new ArrayList<>(Arrays.asList(withoutJson, withJson)));
        Mockito.when(xServiceDefDao.getById(1L)).thenReturn(withoutJson);
        Mockito.when(xServiceDefDao.getById(2L)).thenReturn(withJson);
        Mockito.when(serviceDefService.buildDefText(withoutJson, null)).thenReturn("{\"name\":\"hive\"}");

        patch.execLoad();

        Assertions.assertEquals("{\"name\":\"hive\"}", withoutJson.getDefText());
        Assertions.assertEquals("{\"name\":\"hdfs\"}", withJson.getDefText());
        Mockito.verify(xServiceDefDao).update(withoutJson);
        Mockito.verify(xServiceDefDao, Mockito.never()).update(withJson);
        Mockito.verify(serviceDefService, Mockito.never()).buildDefText(Mockito.eq(withJson), Mockito.any());
    }

    @Test
    public void testFailsWhenJsonCannotBeBuilt() {
        XXServiceDef withoutJson = serviceDef(1L, "hive", null);

        Mockito.when(xServiceDefDao.getAll()).thenReturn(new ArrayList<>(Collections.singletonList(withoutJson)));
        Mockito.when(xServiceDefDao.getById(1L)).thenReturn(withoutJson);
        Mockito.when(serviceDefService.buildDefText(withoutJson, null)).thenReturn(null);

        Assertions.assertThrows(RuntimeException.class, () -> patch.execLoad());
        Mockito.verify(xServiceDefDao, Mockito.never()).update(Mockito.any());
    }

    @Test
    public void testNoServiceDefs() {
        Mockito.when(xServiceDefDao.getAll()).thenReturn(new ArrayList<>());

        patch.execLoad();
        patch.printStats();

        Mockito.verify(xServiceDefDao, Mockito.never()).update(Mockito.any());
    }

    private static XXServiceDef serviceDef(Long id, String name, String defText) {
        XXServiceDef ret = new XXServiceDef();

        ret.setId(id);
        ret.setName(name);
        ret.setDefText(defText);

        return ret;
    }
}
