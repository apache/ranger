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

package org.apache.ranger.patch;

import org.apache.ranger.biz.ServiceDBStore;
import org.apache.ranger.common.JSONUtil;
import org.apache.ranger.common.RangerValidatorFactory;
import org.apache.ranger.db.RangerDaoManager;
import org.apache.ranger.db.XXServiceDefDao;
import org.apache.ranger.entity.XXServiceDef;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.model.RangerServiceDef.RangerResourceDef;
import org.apache.ranger.plugin.model.RangerServiceDef.RangerRowFilterDef;
import org.apache.ranger.plugin.model.validation.RangerServiceDefValidator;
import org.apache.ranger.plugin.model.validation.RangerValidator;
import org.apache.ranger.plugin.store.EmbeddedServiceDefsUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.security.Permission;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * @description Unit Test for PatchForHiveServiceDefUpdate_J10068 class
 */
@ExtendWith(MockitoExtension.class)
@TestMethodOrder(MethodOrderer.MethodName.class)
class TestPatchForHiveServiceDefUpdate_J10068 {
    private static final String HIVE_SERVICE_DEF_NAME = "hive";

    @Test
    void testExecLoadAndPrintStats() throws Exception {
        try (MockedStatic<EmbeddedServiceDefsUtil> utilMock = mockEmbeddedServiceDef(createEmbeddedHiveServiceDef())) {
            PatchForHiveServiceDefUpdate_J10068 patch = createPatchWithMocks();

            mockSuccessfulDbUpdate(patch);

            patch.execLoad();
            patch.printStats();

            Mockito.verify(patch.svcStore, Mockito.atLeastOnce()).updateServiceDef(Mockito.any(RangerServiceDef.class));
        }
    }

    @Test
    void testExecLoad_EmbeddedMissing_FailsAndExits() throws Exception {
        try (MockedStatic<EmbeddedServiceDefsUtil> utilMock = mockEmbeddedServiceDef(null)) {
            PatchForHiveServiceDefUpdate_J10068 patch = createPatchWithMocks();

            assertExecLoadFailsAndExits(patch);

            Mockito.verify(patch.svcStore, Mockito.never()).updateServiceDef(Mockito.any(RangerServiceDef.class));
        }
    }

    @Test
    void testExecLoad_UpdateReturnsNull_FailsAndExits() throws Exception {
        try (MockedStatic<EmbeddedServiceDefsUtil> utilMock = mockEmbeddedServiceDef(createEmbeddedHiveServiceDef())) {
            PatchForHiveServiceDefUpdate_J10068 patch = createPatchWithMocks();

            RangerServiceDef dbServiceDef = createDbHiveServiceDef();

            Mockito.doReturn(dbServiceDef).when(patch.svcStore).getServiceDefByName(HIVE_SERVICE_DEF_NAME);
            Mockito.doReturn(null).when(patch.svcStore).updateServiceDef(Mockito.any(RangerServiceDef.class));

            assertExecLoadFailsAndExits(patch);
        }
    }

    @Test
    void testUpdateHiveServiceDef_rowFilterDefMissing_throwsException() throws Exception {
        RangerServiceDef embedded = new RangerServiceDef();
        embedded.setName(HIVE_SERVICE_DEF_NAME);

        try (MockedStatic<EmbeddedServiceDefsUtil> utilMock = mockEmbeddedServiceDef(embedded)) {
            PatchForHiveServiceDefUpdate_J10068 patch = createPatchWithMocks();

            IllegalStateException ex = Assertions.assertThrows(IllegalStateException.class,
                    () -> invokeUpdateHiveServiceDef(patch));
            Assertions.assertTrue(ex.getMessage().contains("rowFilterDef"));
            Mockito.verify(patch.svcStore, Mockito.never()).updateServiceDef(Mockito.any(RangerServiceDef.class));
        }
    }

    @Test
    void testUpdateHiveServiceDef_copiesOnlyTableResourceFields() throws Exception {
        try (MockedStatic<EmbeddedServiceDefsUtil> utilMock = mockEmbeddedServiceDef(createEmbeddedHiveServiceDef())) {
            PatchForHiveServiceDefUpdate_J10068 patch = createPatchWithMocks();
            RangerServiceDefValidator validator = patch.validatorFactory.getServiceDefValidator(patch.svcStore);

            RangerServiceDef dbServiceDef = createDbHiveServiceDef();
            Mockito.doReturn(dbServiceDef).when(patch.svcStore).getServiceDefByName(HIVE_SERVICE_DEF_NAME);
            Mockito.doReturn(dbServiceDef).when(patch.svcStore).updateServiceDef(Mockito.any(RangerServiceDef.class));

            invokeUpdateHiveServiceDef(patch);

            ArgumentCaptor<RangerServiceDef> captor = ArgumentCaptor.forClass(RangerServiceDef.class);

            Mockito.verify(validator, Mockito.atLeastOnce()).validate(Mockito.any(RangerServiceDef.class),
                    Mockito.eq(RangerValidator.Action.UPDATE));
            Mockito.verify(patch.svcStore).updateServiceDef(captor.capture());

            RangerResourceDef tableResource = findRowFilterResource(captor.getValue(), "table");
            RangerResourceDef databaseResource = findRowFilterResource(captor.getValue(), "database");

            Assertions.assertNotNull(tableResource);
            Assertions.assertNotNull(tableResource.getMatcherOptions());
            Assertions.assertEquals("true", tableResource.getMatcherOptions().get("wildCard"));
            Assertions.assertEquals("", tableResource.getUiHint());

            Assertions.assertNotNull(databaseResource);
            Assertions.assertNotNull(databaseResource.getMatcherOptions());
            Assertions.assertEquals("false", databaseResource.getMatcherOptions().get("wildCard"));
            Assertions.assertEquals("{ \"singleValue\":true }", databaseResource.getUiHint());
        }
    }

    @Test
    void testUpdateHiveServiceDef_copiesMatcherOptionsDefensively() throws Exception {
        RangerServiceDef embedded = createEmbeddedHiveServiceDef();
        Map<String, String> embeddedMatcherOptions = findRowFilterResource(embedded, "table").getMatcherOptions();

        try (MockedStatic<EmbeddedServiceDefsUtil> utilMock = mockEmbeddedServiceDef(embedded)) {
            PatchForHiveServiceDefUpdate_J10068 patch = createPatchWithMocks();

            RangerServiceDef dbServiceDef = createDbHiveServiceDef();
            Mockito.doReturn(dbServiceDef).when(patch.svcStore).getServiceDefByName(HIVE_SERVICE_DEF_NAME);
            Mockito.doReturn(dbServiceDef).when(patch.svcStore).updateServiceDef(Mockito.any(RangerServiceDef.class));

            invokeUpdateHiveServiceDef(patch);

            ArgumentCaptor<RangerServiceDef> captor = ArgumentCaptor.forClass(RangerServiceDef.class);
            Mockito.verify(patch.svcStore).updateServiceDef(captor.capture());

            RangerResourceDef tableResource = findRowFilterResource(captor.getValue(), "table");

            Assertions.assertNotNull(tableResource.getMatcherOptions());
            Assertions.assertNotSame(embeddedMatcherOptions, tableResource.getMatcherOptions());
            Assertions.assertEquals(embeddedMatcherOptions, tableResource.getMatcherOptions());
        }
    }

    @Test
    void testUpdateHiveServiceDef_idempotentWhenAlreadyPatched() throws Exception {
        try (MockedStatic<EmbeddedServiceDefsUtil> utilMock = mockEmbeddedServiceDef(createEmbeddedHiveServiceDef())) {
            PatchForHiveServiceDefUpdate_J10068 patch = createPatchWithMocks();

            RangerServiceDef dbServiceDef = createEmbeddedHiveServiceDef();
            Mockito.doReturn(dbServiceDef).when(patch.svcStore).getServiceDefByName(HIVE_SERVICE_DEF_NAME);
            Mockito.doReturn(dbServiceDef).when(patch.svcStore).updateServiceDef(Mockito.any(RangerServiceDef.class));

            invokeUpdateHiveServiceDef(patch);
            invokeUpdateHiveServiceDef(patch);

            Mockito.verify(patch.svcStore, Mockito.times(2)).updateServiceDef(Mockito.any(RangerServiceDef.class));

            RangerResourceDef tableResource = findRowFilterResource(dbServiceDef, "table");
            Assertions.assertEquals("true", tableResource.getMatcherOptions().get("wildCard"));
            Assertions.assertEquals("", tableResource.getUiHint());
        }
    }

    @Test
    void testUpdateHiveServiceDef_restoresDefOptionsWhenInjected() throws Exception {
        try (MockedStatic<EmbeddedServiceDefsUtil> utilMock = mockEmbeddedServiceDef(createEmbeddedHiveServiceDef())) {
            PatchForHiveServiceDefUpdate_J10068 patch = createPatchWithMocks();

            XXServiceDefDao xxServiceDefDao = patch.daoMgr.getXXServiceDef();
            XXServiceDef xdefPre = Mockito.mock(XXServiceDef.class);
            XXServiceDef xdefPost = Mockito.mock(XXServiceDef.class);

            Mockito.when(xxServiceDefDao.findByName(HIVE_SERVICE_DEF_NAME)).thenReturn(xdefPre, xdefPost);
            Mockito.when(xdefPre.getDefOptions()).thenReturn("{}");

            Map<String, String> postMap = new HashMap<>();
            postMap.put(RangerServiceDef.OPTION_ENABLE_DENY_AND_EXCEPTIONS_IN_POLICIES, "true");
            Mockito.when(xdefPost.getDefOptions()).thenReturn(new JSONUtil().readMapToString(postMap));

            RangerServiceDef dbServiceDef = createDbHiveServiceDef();
            Mockito.doReturn(dbServiceDef).when(patch.svcStore).getServiceDefByName(HIVE_SERVICE_DEF_NAME);
            Mockito.doReturn(dbServiceDef).when(patch.svcStore).updateServiceDef(Mockito.any(RangerServiceDef.class));

            invokeUpdateHiveServiceDef(patch);

            Mockito.verify(xxServiceDefDao, Mockito.times(1)).update(Mockito.eq(xdefPost));
        }
    }

    @Test
    void testJsonStringToMap_withJson() {
        PatchForHiveServiceDefUpdate_J10068 patch = new PatchForHiveServiceDefUpdate_J10068();
        patch.jsonUtil = new JSONUtil();

        Map<String, String> result = patch.jsonStringToMap("{\"a\":\"1\",\"b\":\"2\"}");

        Assertions.assertEquals("1", result.get("a"));
        Assertions.assertEquals("2", result.get("b"));
    }

    @Test
    void testJsonStringToMap_withLegacyFormat() {
        PatchForHiveServiceDefUpdate_J10068 patch = new PatchForHiveServiceDefUpdate_J10068();
        patch.jsonUtil = new JSONUtil();

        Map<String, String> result = patch.jsonStringToMap("a=1;b=2;c=\n");

        Assertions.assertEquals("1", result.get("a"));
        Assertions.assertEquals("2", result.get("b"));
        Assertions.assertTrue(result.containsKey("c"));
    }

    @Test
    void testJsonStringToMap_nullOrEmpty() {
        PatchForHiveServiceDefUpdate_J10068 patch = new PatchForHiveServiceDefUpdate_J10068();
        patch.jsonUtil = new JSONUtil();

        Assertions.assertNull(patch.jsonStringToMap(null));
        Assertions.assertNull(patch.jsonStringToMap(""));
    }

    @Test
    void testUpdateHiveServiceDef_preservesExistingDefOptions() throws Exception {
        try (MockedStatic<EmbeddedServiceDefsUtil> utilMock = mockEmbeddedServiceDef(createEmbeddedHiveServiceDef())) {
            PatchForHiveServiceDefUpdate_J10068 patch = createPatchWithMocks();

            XXServiceDefDao xxServiceDefDao = patch.daoMgr.getXXServiceDef();
            XXServiceDef xdefPre = Mockito.mock(XXServiceDef.class);
            XXServiceDef xdefPost = Mockito.mock(XXServiceDef.class);

            Mockito.when(xxServiceDefDao.findByName(HIVE_SERVICE_DEF_NAME)).thenReturn(xdefPre, xdefPost);

            Map<String, String> preMap = new HashMap<>();
            preMap.put(RangerServiceDef.OPTION_ENABLE_DENY_AND_EXCEPTIONS_IN_POLICIES, "abc");
            Mockito.when(xdefPre.getDefOptions()).thenReturn(new JSONUtil().readMapToString(preMap));

            Map<String, String> postMap = new HashMap<>();
            postMap.put(RangerServiceDef.OPTION_ENABLE_DENY_AND_EXCEPTIONS_IN_POLICIES, "xyz");
            Mockito.when(xdefPost.getDefOptions()).thenReturn(new JSONUtil().readMapToString(postMap));

            RangerServiceDef dbServiceDef = createDbHiveServiceDef();
            Mockito.doReturn(dbServiceDef).when(patch.svcStore).getServiceDefByName(HIVE_SERVICE_DEF_NAME);
            Mockito.doReturn(dbServiceDef).when(patch.svcStore).updateServiceDef(Mockito.any(RangerServiceDef.class));

            invokeUpdateHiveServiceDef(patch);

            ArgumentCaptor<String> defOptionsCaptor = ArgumentCaptor.forClass(String.class);
            Mockito.verify(xdefPost).setDefOptions(defOptionsCaptor.capture());
            Mockito.verify(xxServiceDefDao, Mockito.times(1)).update(Mockito.eq(xdefPost));
            Assertions.assertTrue(defOptionsCaptor.getValue()
                    .contains("\"" + RangerServiceDef.OPTION_ENABLE_DENY_AND_EXCEPTIONS_IN_POLICIES + "\":\"abc\""));
        }
    }

    private void assertExecLoadFailsAndExits(PatchForHiveServiceDefUpdate_J10068 patch) {
        SecurityManager original = System.getSecurityManager();

        try {
            System.setSecurityManager(new SecurityManager() {
                @Override
                public void checkPermission(Permission perm) {
                }

                @Override
                public void checkExit(int status) {
                    throw new SecurityException("Intercepted System.exit");
                }
            });
            patch.execLoad();
            Assertions.fail("Expected SecurityException");
        } catch (SecurityException ignored) {
        } finally {
            System.setSecurityManager(original);
        }
    }

    private PatchForHiveServiceDefUpdate_J10068 createPatchWithMocks() {
        PatchForHiveServiceDefUpdate_J10068 patch = new PatchForHiveServiceDefUpdate_J10068();

        ServiceDBStore svcStore = Mockito.mock(ServiceDBStore.class);
        RangerValidatorFactory validatorFactory = Mockito.mock(RangerValidatorFactory.class);
        RangerServiceDefValidator validator = Mockito.mock(RangerServiceDefValidator.class);
        RangerDaoManager daoMgr = Mockito.mock(RangerDaoManager.class);
        XXServiceDefDao xxServiceDefDao = Mockito.mock(XXServiceDefDao.class);

        Mockito.lenient().when(validatorFactory.getServiceDefValidator(svcStore)).thenReturn(validator);
        Mockito.lenient().when(daoMgr.getXXServiceDef()).thenReturn(xxServiceDefDao);

        XXServiceDef xXServiceDef = Mockito.mock(XXServiceDef.class);
        Mockito.lenient().when(xxServiceDefDao.findByName(HIVE_SERVICE_DEF_NAME)).thenReturn(xXServiceDef);
        Mockito.lenient().when(xXServiceDef.getDefOptions()).thenReturn("{}");

        patch.svcStore = svcStore;
        patch.validatorFactory = validatorFactory;
        patch.daoMgr = daoMgr;
        patch.jsonUtil = new JSONUtil();

        return patch;
    }

    private void mockSuccessfulDbUpdate(PatchForHiveServiceDefUpdate_J10068 patch) throws Exception {
        RangerServiceDef dbServiceDef = createDbHiveServiceDef();

        Mockito.doReturn(dbServiceDef).when(patch.svcStore).getServiceDefByName(HIVE_SERVICE_DEF_NAME);
        Mockito.doReturn(dbServiceDef).when(patch.svcStore).updateServiceDef(Mockito.any(RangerServiceDef.class));
    }

    private MockedStatic<EmbeddedServiceDefsUtil> mockEmbeddedServiceDef(RangerServiceDef embedded) throws Exception {
        MockedStatic<EmbeddedServiceDefsUtil> utilMock = Mockito.mockStatic(EmbeddedServiceDefsUtil.class);
        EmbeddedServiceDefsUtil util = Mockito.mock(EmbeddedServiceDefsUtil.class);

        utilMock.when(EmbeddedServiceDefsUtil::instance).thenReturn(util);
        Mockito.doReturn(embedded).when(util).getEmbeddedServiceDef(HIVE_SERVICE_DEF_NAME);

        return utilMock;
    }

    private RangerServiceDef createEmbeddedHiveServiceDef() {
        RangerServiceDef embedded = new RangerServiceDef();
        embedded.setName(HIVE_SERVICE_DEF_NAME);
        embedded.setRowFilterDef(createEmbeddedRowFilterDef());
        return embedded;
    }

    private RangerServiceDef createDbHiveServiceDef() {
        RangerServiceDef dbServiceDef = new RangerServiceDef();
        dbServiceDef.setName(HIVE_SERVICE_DEF_NAME);
        dbServiceDef.setRowFilterDef(createDbRowFilterDef());
        return dbServiceDef;
    }

    private RangerRowFilterDef createEmbeddedRowFilterDef() {
        Map<String, String> tableMatcherOptions = new HashMap<>();
        tableMatcherOptions.put("wildCard", "true");

        RangerResourceDef databaseResource = new RangerResourceDef();
        databaseResource.setName("database");

        RangerResourceDef tableResource = new RangerResourceDef();
        tableResource.setName("table");
        tableResource.setMatcherOptions(tableMatcherOptions);
        tableResource.setUiHint("");

        RangerRowFilterDef rowFilterDef = new RangerRowFilterDef();
        rowFilterDef.setResources(Arrays.asList(databaseResource, tableResource));

        return rowFilterDef;
    }

    private RangerRowFilterDef createDbRowFilterDef() {
        Map<String, String> databaseMatcherOptions = new HashMap<>();
        databaseMatcherOptions.put("wildCard", "false");

        Map<String, String> tableMatcherOptions = new HashMap<>();
        tableMatcherOptions.put("wildCard", "false");

        RangerResourceDef databaseResource = new RangerResourceDef();
        databaseResource.setName("database");
        databaseResource.setMatcherOptions(databaseMatcherOptions);
        databaseResource.setUiHint("{ \"singleValue\":true }");

        RangerResourceDef tableResource = new RangerResourceDef();
        tableResource.setName("table");
        tableResource.setMatcherOptions(tableMatcherOptions);
        tableResource.setUiHint("{ \"singleValue\":true }");

        RangerRowFilterDef rowFilterDef = new RangerRowFilterDef();
        rowFilterDef.setResources(Arrays.asList(databaseResource, tableResource));

        return rowFilterDef;
    }

    private RangerResourceDef findRowFilterResource(RangerServiceDef serviceDef, String resourceName) {
        RangerResourceDef result = null;

        if (serviceDef != null && serviceDef.getRowFilterDef() != null && serviceDef.getRowFilterDef().getResources() != null) {
            for (RangerResourceDef resourceDef : serviceDef.getRowFilterDef().getResources()) {
                if (resourceName.equals(resourceDef.getName())) {
                    result = resourceDef;
                    break;
                }
            }
        }

        return result;
    }

    private void invokeUpdateHiveServiceDef(PatchForHiveServiceDefUpdate_J10068 patch) throws Exception {
        Method method = PatchForHiveServiceDefUpdate_J10068.class.getDeclaredMethod("updateHiveServiceDef");
        method.setAccessible(true);

        try {
            method.invoke(patch);
        } catch (InvocationTargetException e) {
            Throwable cause = e.getCause();

            if (cause instanceof Exception) {
                throw (Exception) cause;
            }

            if (cause instanceof Error) {
                throw (Error) cause;
            }

            throw e;
        }
    }
}
