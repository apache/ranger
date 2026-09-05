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
import org.apache.ranger.db.RangerDaoManager;
import org.apache.ranger.db.XXServiceConfigDefDao;
import org.apache.ranger.db.XXServiceConfigMapDao;
import org.apache.ranger.db.XXServiceDao;
import org.apache.ranger.entity.XXService;
import org.apache.ranger.entity.XXServiceConfigMap;
import org.apache.ranger.plugin.util.PasswordUtils;
import org.apache.ranger.plugin.util.RangerSupportedCryptoAlgo;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Collections;
import java.util.List;

/**
 * @description Unit tests for PatchServicePasswordV2Migration_J10067, covering the gaps flagged in
 * review: the up-front config validation (see {@link PatchServicePasswordV2Migration_J10067#validateMigrationConfig()})
 * and the "already-v2 row is a no-op" idempotency guarantee.
 */
@ExtendWith(MockitoExtension.class)
public class TestPatchServicePasswordV2Migration_J10067 {
    @Test
    public void testMigrateServicePasswordsToV2_DefaultKeyConfig_FailsFastBeforeTouchingAnyRow() {
        // No test in this suite (or anywhere else in this module) overrides
        // ranger.password.encryption.key, so ServiceDBStore.ENCRYPT_KEY is - here, exactly like in
        // a freshly-installed, never-configured real deployment - still PasswordUtils.DEFAULT_ENCRYPT_KEY.
        // validateMigrationConfig() must refuse to proceed in that state, and it must do so BEFORE
        // even reading x_service_config_map - not burn every row as an individual per-row failure.
        Assertions.assertEquals(PasswordUtils.DEFAULT_ENCRYPT_KEY, ServiceDBStore.ENCRYPT_KEY,
                "test precondition: this suite never configures a real encryption key");

        PatchServicePasswordV2Migration_J10067 patch = new PatchServicePasswordV2Migration_J10067();
        RangerDaoManager daoMgr = Mockito.mock(RangerDaoManager.class);

        patch.daoMgr = daoMgr;

        Assertions.assertThrows(IllegalStateException.class, patch::migrateServicePasswordsToV2,
                "migration must refuse to run against an unset/default ranger.password.encryption.key");

        Mockito.verifyNoInteractions(daoMgr);
    }

    @Test
    public void testMigrateServicePasswordsToV2_AlreadyV2Row_IsNoOp() throws Exception {
        // Bypass only the key-configuration guard (already covered directly by
        // PasswordUtilsTest#testValidateEncryptionKeyConfiguredRejectsDefaultKey and by the
        // fail-fast test above) so the real per-row dispatch logic below it can be exercised
        // end-to-end against a v2-format row, the same way a re-run of this patch after a partial
        // prior run would encounter one.
        try (MockedStatic<PasswordUtils> pwdUtilsMock = Mockito.mockStatic(PasswordUtils.class, Mockito.CALLS_REAL_METHODS)) {
            pwdUtilsMock.when(() -> PasswordUtils.validateEncryptionKeyConfigured(Mockito.any())).thenAnswer(invocation -> null);

            PatchServicePasswordV2Migration_J10067 patch = new PatchServicePasswordV2Migration_J10067();

            RangerDaoManager daoMgr = Mockito.mock(RangerDaoManager.class);
            XXServiceConfigMapDao xServiceConfigMapDao = Mockito.mock(XXServiceConfigMapDao.class);
            XXServiceDao xServiceDao = Mockito.mock(XXServiceDao.class);
            XXServiceConfigDefDao xServiceConfigDefDao = Mockito.mock(XXServiceConfigDefDao.class);

            patch.daoMgr = daoMgr;

            String v2StoredValue = PasswordUtils.encryptPasswordV2("existingServicePassword", RangerSupportedCryptoAlgo.PBEWITHHMACSHA512ANDAES_128,
                    "some-operator-key".toCharArray(), "f77aLYLo".getBytes(), 1000);

            XXServiceConfigMap configMap = new XXServiceConfigMap();
            configMap.setId(1L);
            configMap.setServiceId(100L);
            configMap.setConfigkey("password");
            configMap.setConfigvalue(v2StoredValue);

            XXService xService = new XXService();
            xService.setId(100L);
            xService.setName("svc1");
            xService.setType(10L);

            Mockito.when(daoMgr.getXXServiceConfigMap()).thenReturn(xServiceConfigMapDao);
            Mockito.when(xServiceConfigMapDao.getAll()).thenReturn(Collections.singletonList(configMap));

            Mockito.when(daoMgr.getXXService()).thenReturn(xServiceDao);
            Mockito.when(xServiceDao.getById(100L)).thenReturn(xService);

            Mockito.when(daoMgr.getXXServiceConfigDef()).thenReturn(xServiceConfigDefDao);
            Mockito.when(xServiceConfigDefDao.findConfigNamesByServiceDefIdAndType(10L, ServiceDBStore.CONFIG_TYPE_PASSWORD))
                    .thenReturn(Collections.emptyList());

            patch.migrateServicePasswordsToV2();

            Mockito.verify(xServiceConfigMapDao, Mockito.never()).update(Mockito.any(XXServiceConfigMap.class));
            Assertions.assertEquals(v2StoredValue, configMap.getConfigvalue(), "an already-v2 row must be left byte-for-byte unchanged");
        }
    }

    @Test
    public void testMigrateServicePasswordsToV2_PlaintextWithCommaIsNotMisclassifiedAsLegacy() throws Exception {
        // Guards the fix for the old ".contains(\",\")" heuristic: a plaintext password that
        // merely contains a comma must be left alone (counted as "not encrypted"), never run
        // through decrypt/re-encrypt as if it were a genuine legacy-format value.
        try (MockedStatic<PasswordUtils> pwdUtilsMock = Mockito.mockStatic(PasswordUtils.class, Mockito.CALLS_REAL_METHODS)) {
            pwdUtilsMock.when(() -> PasswordUtils.validateEncryptionKeyConfigured(Mockito.any())).thenAnswer(invocation -> null);

            PatchServicePasswordV2Migration_J10067 patch = new PatchServicePasswordV2Migration_J10067();

            RangerDaoManager daoMgr = Mockito.mock(RangerDaoManager.class);
            XXServiceConfigMapDao xServiceConfigMapDao = Mockito.mock(XXServiceConfigMapDao.class);

            patch.daoMgr = daoMgr;

            XXServiceConfigMap configMap = new XXServiceConfigMap();
            configMap.setId(2L);
            configMap.setServiceId(101L);
            configMap.setConfigkey("password");
            configMap.setConfigvalue("plaintext,password,with,commas"); // first field is not a real algo name

            List<XXServiceConfigMap> allConfigMaps = Collections.singletonList(configMap);

            Mockito.when(daoMgr.getXXServiceConfigMap()).thenReturn(xServiceConfigMapDao);
            Mockito.when(xServiceConfigMapDao.getAll()).thenReturn(allConfigMaps);

            patch.migrateServicePasswordsToV2();

            // Never even reached the per-row XXService lookup - rejected up front by the
            // isLegacyFormat()/isV2Format() classification, before any DAO calls for this row.
            Mockito.verify(daoMgr, Mockito.never()).getXXService();
            Mockito.verify(xServiceConfigMapDao, Mockito.never()).update(Mockito.any(XXServiceConfigMap.class));
            Assertions.assertEquals("plaintext,password,with,commas", configMap.getConfigvalue());
        }
    }
}
