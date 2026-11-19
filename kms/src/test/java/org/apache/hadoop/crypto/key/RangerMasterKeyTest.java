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

package org.apache.hadoop.crypto.key;

import org.apache.hadoop.crypto.key.common.SupportedPBECryptoKDFSuite;
import org.apache.hadoop.crypto.key.kms.server.DerbyTestUtils;
import org.apache.hadoop.crypto.key.kms.server.KMSConfiguration;
import org.apache.ranger.entity.XXRangerMasterKey;
import org.apache.ranger.kms.dao.DaoManager;
import org.apache.ranger.kms.dao.RangerMasterKeyDao;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.Provider;
import java.security.Security;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * A test for the RangerMasterKey.
 */
public class RangerMasterKeyTest {
    private static final boolean UNRESTRICTED_POLICIES_INSTALLED;

    private RangerMasterKey rangerMasterKey;

    private DaoManager daoManager;

    @BeforeAll
    public static void startServers() throws Exception {
        if (!UNRESTRICTED_POLICIES_INSTALLED) {
            return;
        }
        DerbyTestUtils.startDerby();
    }

    @AfterAll
    public static void stopServers() throws Exception {
        if (UNRESTRICTED_POLICIES_INSTALLED) {
            DerbyTestUtils.stopDerby();
        }
    }

    @BeforeEach
    public void init() {
        Path configDir = Paths.get("src/test/resources/kms");
        System.setProperty(KMSConfiguration.KMS_CONFIG_DIR, configDir.toFile().getAbsolutePath());

        if (null == this.daoManager) {
            RangerKMSDB     rangerkmsDb     = new RangerKMSDB(RangerKeyStoreProvider.getDBKSConf());
            this.daoManager      = rangerkmsDb.getDaoManager();
        }

        Security.removeProvider("BC");
        RangerKMSCryptoConfigManager kmsCryptoConfigApi = new TestKMSCryptoAPIManager("PBEWithMD5AndTripleDES", "MD5", 20);
        ((TestKMSCryptoAPIManager) kmsCryptoConfigApi).setConfigForMK(true);
        this.rangerMasterKey = new RangerMasterKey(daoManager, kmsCryptoConfigApi);
    }

    @AfterEach
    public void tearDown() {
        RangerMasterKeyDao masterKeyDao = daoManager.getRangerMasterKeyDao();
        List<XXRangerMasterKey> masterKeys = masterKeyDao.getAll();
        if (null != masterKeys && !masterKeys.isEmpty()) {
            XXRangerMasterKey masterKey = masterKeys.get(0);
            masterKeyDao.remove(masterKey);
        }
        this.rangerMasterKey = null;
    }

    @Test
    public void testRangerMasterKeyGenerationAndReencryption() throws Throwable {
        if (!UNRESTRICTED_POLICIES_INSTALLED) {
            return;
        }

        String masterKeyPassword = "password0password0password0password0password0password0password0password0"
                + "password0password0password0password0password0password0password0password0password0password0"
                + "password0password0password0password0password0password0password0password0password0password0";

        Assertions.assertTrue(rangerMasterKey.generateMasterKey(masterKeyPassword));
        String masterKey = rangerMasterKey.getMasterKey(masterKeyPassword);
        Assertions.assertNotNull(masterKey);

        try {
            rangerMasterKey.getMasterKey("badpass");
            Assertions.fail("Failure expected on retrieving a key with the wrong password");
        } catch (Exception ex) {
            // expected
        }

        Assertions.assertNotNull(rangerMasterKey.getMasterSecretKey(masterKeyPassword));

        try {
            rangerMasterKey.getMasterSecretKey("badpass");
            Assertions.fail("Failure expected on retrieving a key with the wrong password");
        } catch (Exception ex) {
            // expected
        }

        /*
         * Now prepare env with required FIPS configurations
           Add BouncyCastleProvider at first position
           Change keystore type to bcfks
         */

        Provider provider = new BouncyCastleProvider();
        Security.insertProviderAt(provider, 1);

        RangerKMSCryptoConfigManager kmsCryptoConfigApi = new TestKMSCryptoAPIManager("PBKDF2WithHmacSHA256", "SHA-512", 1000);
        ((TestKMSCryptoAPIManager) kmsCryptoConfigApi).setConfigForMK(true);
        this.rangerMasterKey = new RangerMasterKey(daoManager, kmsCryptoConfigApi);

        Assertions.assertTrue(rangerMasterKey.reencryptOrUpdateMK(masterKeyPassword));

        // this checks the Algo name written in the DB.
        Assertions.assertEquals(SupportedPBECryptoKDFSuite.PBKDF2WITHHMACSHA256, rangerMasterKey.getMKEncryptionAlgoName());

        Assertions.assertEquals(masterKey, rangerMasterKey.getMasterKey(masterKeyPassword));

        Assertions.assertFalse(rangerMasterKey.reencryptOrUpdateMK(masterKeyPassword));

        // revert the FIPS specific changes so that other cases can execute with default provider.
        Security.removeProvider(provider.getName());
    }

    @Test
    public void testReencryptMKWithFipsAlgo() throws Exception {
        if (!UNRESTRICTED_POLICIES_INSTALLED) {
            return;
        }

        String masterKeyPassword = "password0password0password0password0password0password0password0password0"
                + "password0password0password0password0password0password0password0password0password0password0"
                + "password0password0password0password0password0password0password0password0password0password0";

        Assertions.assertFalse(rangerMasterKey.reencryptOrUpdateMK(masterKeyPassword));  // or assertTrue if expecting re-encryption to happen
    }

    @Test
    void testMKGenerateEncryptAndDecryptUsingAESGCMNoPadding() throws Throwable {
        if (!UNRESTRICTED_POLICIES_INSTALLED) {
            return;
        }

        String masterKeyPassword = "password0password0password0password0password0password0password0password0"
                + "password0password0password0password0password0password0password0password0password0password0"
                + "password0password0password0password0password0password0password0password0password0password0";

        RangerKMSCryptoConfigManager kmsCryptoConfigApi = new TestKMSCryptoAPIManager("PBKDF2WITHHMACSHA256", "AES/GCM/NOPADDING", "SHA-512", 1000);
        ((TestKMSCryptoAPIManager) kmsCryptoConfigApi).setConfigForMK(true);
        this.rangerMasterKey = new RangerMasterKey(daoManager, kmsCryptoConfigApi);

        Assertions.assertTrue(rangerMasterKey.generateMasterKey(masterKeyPassword));
        String masterKey = rangerMasterKey.getMasterKey(masterKeyPassword);
        Assertions.assertNotNull(masterKey);

        try {
            rangerMasterKey.getMasterKey("badpass");
            Assertions.fail("Failure expected on retrieving a key with the wrong password");
        } catch (Exception ex) {
            // expected
        }

        Assertions.assertNotNull(rangerMasterKey.getMasterSecretKey(masterKeyPassword));
    }

    @Test
    public void testExternalKeyAsMK() throws Throwable {
        if (!UNRESTRICTED_POLICIES_INSTALLED) {
            return;
        }

        String password = "password0password0password0password0password0password0password0password0"
                + "password0password0password0password0password0password0password0password0"
                + "password0password0password0password0password0password0password0";
        byte[] key = {0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07,
                0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f,
                0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17};
        rangerMasterKey.setExternalKeyAsMK(password, key);
        assertNotNull(rangerMasterKey.getMasterKey(password));
    }

    static {
        boolean ok = false;
        try {
            byte[] data = {0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07};

            SecretKey key192 = new SecretKeySpec(
                    new byte[] {0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07,
                            0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f,
                            0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17},
                    "AES");
            Cipher c = Cipher.getInstance("AES");
            c.init(Cipher.ENCRYPT_MODE, key192);
            c.doFinal(data);
            ok = true;
        } catch (Exception e) {
            //
        }
        UNRESTRICTED_POLICIES_INSTALLED = ok;
    }
}
