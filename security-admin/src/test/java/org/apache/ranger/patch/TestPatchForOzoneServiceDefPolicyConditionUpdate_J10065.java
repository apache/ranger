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

import org.apache.ranger.authorization.hadoop.config.RangerAdminConfig;
import org.apache.ranger.biz.ServiceDBStore;
import org.apache.ranger.common.JSONUtil;
import org.apache.ranger.common.RangerValidatorFactory;
import org.apache.ranger.db.RangerDaoManager;
import org.apache.ranger.db.XXServiceDefDao;
import org.apache.ranger.entity.XXServiceDef;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.model.RangerServiceDef.RangerPolicyConditionDef;
import org.apache.ranger.plugin.model.validation.RangerServiceDefValidator;
import org.apache.ranger.plugin.model.validation.RangerValidator.Action;
import org.apache.ranger.plugin.store.EmbeddedServiceDefsUtil;
import org.apache.ranger.service.RangerServiceDefService;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@ExtendWith(MockitoExtension.class)
@TestMethodOrder(MethodOrderer.MethodName.class)
public class TestPatchForOzoneServiceDefPolicyConditionUpdate_J10065 {
    private static final String POLICY_CONDITION_ACTION_MATCHES = "action-matches";
    private static final String PROP_ENABLE_ACTION_MATCHER_IN_POLICIES_CONDITION =
            RangerServiceDefService.PROP_ENABLE_ACTION_MATCHER_IN_POLICIES_CONDITION;

    private static void setIfPresent(Object target, String fieldName, Object value) {
        try {
            Field f = target.getClass().getDeclaredField(fieldName);
            f.setAccessible(true);
            f.set(target, value);
        } catch (NoSuchFieldException ignored) {
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static RangerServiceDef buildEmbeddedOzoneServiceDefWithPolicyConditions() {
        final RangerServiceDef embedded = new RangerServiceDef();

        final List<RangerPolicyConditionDef> policyConditions = new ArrayList<>();

        final RangerPolicyConditionDef ipRange = new RangerPolicyConditionDef();
        ipRange.setName("ip-range");
        policyConditions.add(ipRange);

        final RangerPolicyConditionDef actionMatches = new RangerPolicyConditionDef();
        actionMatches.setName(POLICY_CONDITION_ACTION_MATCHES);
        policyConditions.add(actionMatches);

        embedded.setPolicyConditions(policyConditions);

        return embedded;
    }

    private static PatchForOzoneServiceDefPolicyConditionUpdate_J10065 buildPatch(
            RangerDaoManager daoMgr,
            ServiceDBStore svcStore,
            RangerServiceDef dbServiceDef,
            XXServiceDefDao xxServiceDefDao) throws Exception {
        final PatchForOzoneServiceDefPolicyConditionUpdate_J10065 patch =
                new PatchForOzoneServiceDefPolicyConditionUpdate_J10065();

        setIfPresent(patch, "daoMgr", daoMgr);
        setIfPresent(patch, "svcDBStore", svcStore);
        setIfPresent(patch, "svcStore", svcStore);
        setIfPresent(patch, "jsonUtil", new JSONUtil());

        final RangerValidatorFactory validatorFactory = Mockito.mock(RangerValidatorFactory.class);
        final RangerServiceDefValidator validator = Mockito.mock(RangerServiceDefValidator.class);
        Mockito.when(validatorFactory.getServiceDefValidator(Mockito.eq(svcStore))).thenReturn(validator);
        Mockito.doNothing().when(validator).validate(Mockito.any(RangerServiceDef.class), Mockito.eq(Action.UPDATE));
        setIfPresent(patch, "validatorFactory", validatorFactory);

        Mockito.when(svcStore.getServiceDefByName(Mockito.anyString())).thenReturn(dbServiceDef);
        Mockito.when(svcStore.updateServiceDef(Mockito.any(RangerServiceDef.class))).thenReturn(dbServiceDef);
        Mockito.doAnswer(invocation -> invocation.getArgument(0)).when(xxServiceDefDao).update(Mockito.any(XXServiceDef.class));

        return patch;
    }

    private static String getOptionFromDefOptions(String defOptionsJson, String optionKey) throws Exception {
        if (defOptionsJson == null) {
            return null;
        }

        final Map<String, String> options = new JSONUtil().jsonToMap(defOptionsJson);

        return options == null ? null : options.get(optionKey);
    }

    @Test
    public void testExecLoadAndPrintStats() throws Exception {
        try (MockedStatic<EmbeddedServiceDefsUtil> utilMock = Mockito.mockStatic(EmbeddedServiceDefsUtil.class)) {
            final EmbeddedServiceDefsUtil util = Mockito.mock(EmbeddedServiceDefsUtil.class);
            utilMock.when(EmbeddedServiceDefsUtil::instance).thenReturn(util);
            Mockito.when(util.getEmbeddedServiceDef(Mockito.anyString())).thenReturn(new RangerServiceDef());

            final PatchForOzoneServiceDefPolicyConditionUpdate_J10065 patch =
                    new PatchForOzoneServiceDefPolicyConditionUpdate_J10065();
            final RangerDaoManager daoMgr = Mockito.mock(RangerDaoManager.class);
            final XXServiceDefDao xxServiceDefDao = Mockito.mock(XXServiceDefDao.class);
            Mockito.when(daoMgr.getXXServiceDef()).thenReturn(xxServiceDefDao);
            Mockito.when(xxServiceDefDao.findByName(Mockito.anyString())).thenReturn(null);
            setIfPresent(patch, "daoMgr", daoMgr);

            patch.execLoad();
            patch.printStats();
        }
    }

    @Test
    public void testSetsEnableActionMatcherInPoliciesCondition_falseWhenFlagDisabledAndOptionMissingFromDb() throws Exception {
        RangerAdminConfig.getInstance().set(PROP_ENABLE_ACTION_MATCHER_IN_POLICIES_CONDITION, Boolean.FALSE.toString());

        try (MockedStatic<EmbeddedServiceDefsUtil> utilMock = Mockito.mockStatic(EmbeddedServiceDefsUtil.class)) {
            final EmbeddedServiceDefsUtil util = Mockito.mock(EmbeddedServiceDefsUtil.class);
            utilMock.when(EmbeddedServiceDefsUtil::instance).thenReturn(util);
            Mockito.when(util.getEmbeddedServiceDef(Mockito.anyString()))
                    .thenReturn(buildEmbeddedOzoneServiceDefWithPolicyConditions());

            final RangerDaoManager daoMgr = Mockito.mock(RangerDaoManager.class);
            final XXServiceDefDao xxServiceDefDao = Mockito.mock(XXServiceDefDao.class);
            final XXServiceDef xxServiceDef = new XXServiceDef();
            xxServiceDef.setDefOptions("{}");
            Mockito.when(daoMgr.getXXServiceDef()).thenReturn(xxServiceDefDao);
            Mockito.when(xxServiceDefDao.findByName(Mockito.anyString())).thenReturn(xxServiceDef);

            final ServiceDBStore svcStore = Mockito.mock(ServiceDBStore.class);
            final RangerServiceDef dbServiceDef = new RangerServiceDef();
            final PatchForOzoneServiceDefPolicyConditionUpdate_J10065 patch =
                    buildPatch(daoMgr, svcStore, dbServiceDef, xxServiceDefDao);

            invokeUpdateOzoneServiceDef(patch);

            final ArgumentCaptor<RangerServiceDef> serviceDefCaptor = ArgumentCaptor.forClass(RangerServiceDef.class);
            Mockito.verify(svcStore).updateServiceDef(serviceDefCaptor.capture());
            Assertions.assertFalse(hasActionMatchesCondition(serviceDefCaptor.getValue()));

            final ArgumentCaptor<XXServiceDef> xxCaptor = ArgumentCaptor.forClass(XXServiceDef.class);
            Mockito.verify(xxServiceDefDao).update(xxCaptor.capture());
            Assertions.assertEquals(
                    Boolean.FALSE.toString(),
                    getOptionFromDefOptions(
                            xxCaptor.getValue().getDefOptions(),
                            RangerServiceDefService.OPTION_ENABLE_ACTION_MATCHER_IN_POLICIES_CONDITION));
        } finally {
            RangerAdminConfig.getInstance().unset(PROP_ENABLE_ACTION_MATCHER_IN_POLICIES_CONDITION);
        }
    }

    @Test
    public void testSetsEnableActionMatcherInPoliciesCondition_trueWhenFlagEnabledAndOptionMissingFromDb() throws Exception {
        RangerAdminConfig.getInstance().set(PROP_ENABLE_ACTION_MATCHER_IN_POLICIES_CONDITION, Boolean.TRUE.toString());

        try (MockedStatic<EmbeddedServiceDefsUtil> utilMock = Mockito.mockStatic(EmbeddedServiceDefsUtil.class)) {
            final EmbeddedServiceDefsUtil util = Mockito.mock(EmbeddedServiceDefsUtil.class);
            utilMock.when(EmbeddedServiceDefsUtil::instance).thenReturn(util);
            Mockito.when(util.getEmbeddedServiceDef(Mockito.anyString()))
                    .thenReturn(buildEmbeddedOzoneServiceDefWithPolicyConditions());

            final RangerDaoManager daoMgr = Mockito.mock(RangerDaoManager.class);
            final XXServiceDefDao xxServiceDefDao = Mockito.mock(XXServiceDefDao.class);
            final XXServiceDef xxServiceDef = new XXServiceDef();
            xxServiceDef.setDefOptions(null);
            Mockito.when(daoMgr.getXXServiceDef()).thenReturn(xxServiceDefDao);
            Mockito.when(xxServiceDefDao.findByName(Mockito.anyString())).thenReturn(xxServiceDef);

            final ServiceDBStore svcStore = Mockito.mock(ServiceDBStore.class);
            final RangerServiceDef dbServiceDef = new RangerServiceDef();
            final PatchForOzoneServiceDefPolicyConditionUpdate_J10065 patch =
                    buildPatch(daoMgr, svcStore, dbServiceDef, xxServiceDefDao);

            invokeUpdateOzoneServiceDef(patch);

            final ArgumentCaptor<RangerServiceDef> serviceDefCaptor = ArgumentCaptor.forClass(RangerServiceDef.class);
            Mockito.verify(svcStore).updateServiceDef(serviceDefCaptor.capture());
            Assertions.assertTrue(hasActionMatchesCondition(serviceDefCaptor.getValue()));

            final ArgumentCaptor<XXServiceDef> xxCaptor = ArgumentCaptor.forClass(XXServiceDef.class);
            Mockito.verify(xxServiceDefDao).update(xxCaptor.capture());
            Assertions.assertEquals(
                    Boolean.TRUE.toString(),
                    getOptionFromDefOptions(
                            xxCaptor.getValue().getDefOptions(),
                            RangerServiceDefService.OPTION_ENABLE_ACTION_MATCHER_IN_POLICIES_CONDITION));
        } finally {
            RangerAdminConfig.getInstance().unset(PROP_ENABLE_ACTION_MATCHER_IN_POLICIES_CONDITION);
        }
    }

    @Test
    public void testSyncsDefOptionsFromAdminConfigWhenExistingValueDiffersInDb() throws Exception {
        RangerAdminConfig.getInstance().set(PROP_ENABLE_ACTION_MATCHER_IN_POLICIES_CONDITION, Boolean.FALSE.toString());

        try (MockedStatic<EmbeddedServiceDefsUtil> utilMock = Mockito.mockStatic(EmbeddedServiceDefsUtil.class)) {
            final EmbeddedServiceDefsUtil util = Mockito.mock(EmbeddedServiceDefsUtil.class);
            utilMock.when(EmbeddedServiceDefsUtil::instance).thenReturn(util);
            Mockito.when(util.getEmbeddedServiceDef(Mockito.anyString()))
                    .thenReturn(buildEmbeddedOzoneServiceDefWithPolicyConditions());

            final Map<String, String> preUpdateOptions = new HashMap<>();
            preUpdateOptions.put(RangerServiceDefService.OPTION_ENABLE_ACTION_MATCHER_IN_POLICIES_CONDITION, Boolean.TRUE.toString());

            final RangerDaoManager daoMgr = Mockito.mock(RangerDaoManager.class);
            final XXServiceDefDao xxServiceDefDao = Mockito.mock(XXServiceDefDao.class);
            final XXServiceDef xxServiceDef = new XXServiceDef();
            xxServiceDef.setDefOptions(new JSONUtil().readMapToString(preUpdateOptions));
            Mockito.when(daoMgr.getXXServiceDef()).thenReturn(xxServiceDefDao);
            Mockito.when(xxServiceDefDao.findByName(Mockito.anyString())).thenReturn(xxServiceDef);

            final ServiceDBStore svcStore = Mockito.mock(ServiceDBStore.class);
            final RangerServiceDef dbServiceDef = new RangerServiceDef();
            final PatchForOzoneServiceDefPolicyConditionUpdate_J10065 patch =
                    buildPatch(daoMgr, svcStore, dbServiceDef, xxServiceDefDao);

            invokeUpdateOzoneServiceDef(patch);

            final ArgumentCaptor<XXServiceDef> xxCaptor = ArgumentCaptor.forClass(XXServiceDef.class);
            Mockito.verify(xxServiceDefDao).update(xxCaptor.capture());
            Assertions.assertEquals(
                    Boolean.FALSE.toString(),
                    getOptionFromDefOptions(
                            xxCaptor.getValue().getDefOptions(),
                            RangerServiceDefService.OPTION_ENABLE_ACTION_MATCHER_IN_POLICIES_CONDITION));
        } finally {
            RangerAdminConfig.getInstance().unset(PROP_ENABLE_ACTION_MATCHER_IN_POLICIES_CONDITION);
        }
    }

    @Test
    public void testUpdateOzoneServiceDef_embeddedNull_returnsEarly() throws Exception {
        try (MockedStatic<EmbeddedServiceDefsUtil> utilMock = Mockito.mockStatic(EmbeddedServiceDefsUtil.class)) {
            final EmbeddedServiceDefsUtil util = Mockito.mock(EmbeddedServiceDefsUtil.class);
            utilMock.when(EmbeddedServiceDefsUtil::instance).thenReturn(util);
            Mockito.when(util.getEmbeddedServiceDef(Mockito.anyString())).thenReturn(null);

            final PatchForOzoneServiceDefPolicyConditionUpdate_J10065 patch =
                    new PatchForOzoneServiceDefPolicyConditionUpdate_J10065();
            final ServiceDBStore svcStore = Mockito.mock(ServiceDBStore.class);
            setIfPresent(patch, "svcStore", svcStore);

            invokeUpdateOzoneServiceDef(patch);

            Mockito.verify(svcStore, Mockito.never()).updateServiceDef(Mockito.any());
        }
    }

    private static void invokeUpdateOzoneServiceDef(PatchForOzoneServiceDefPolicyConditionUpdate_J10065 patch) throws Exception {
        final Method updateMethod =
                PatchForOzoneServiceDefPolicyConditionUpdate_J10065.class.getDeclaredMethod("updateOzoneServiceDef");
        updateMethod.setAccessible(true);
        updateMethod.invoke(patch);
    }

    private static boolean hasActionMatchesCondition(RangerServiceDef serviceDef) {
        if (serviceDef.getPolicyConditions() == null) {
            return false;
        }

        for (RangerPolicyConditionDef conditionDef : serviceDef.getPolicyConditions()) {
            if (POLICY_CONDITION_ACTION_MATCHES.equals(conditionDef.getName())) {
                return true;
            }
        }

        return false;
    }
}
