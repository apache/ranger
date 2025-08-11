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

import com.sun.jersey.core.util.Base64;
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

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
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
        this.rangerMasterKey = new RangerMasterKey(daoManager);
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

        // Before FIPS setup, default and selected crypto Algo should be PBEWithMD5AndTripleDES
        Assertions.assertEquals(SupportedPBECryptoAlgo.PBEWithMD5AndTripleDES, rangerMasterKey.getDefaultCryptoAlgorithm());

        Provider provider = new BouncyCastleProvider();
        Security.insertProviderAt(provider, 1);
        Security.setProperty("keystore.type", "bcfks");

        rangerMasterKey.init();

        Assertions.assertEquals(SupportedPBECryptoAlgo.PBKDF2WithHmacSHA256, rangerMasterKey.getDefaultCryptoAlgorithm());
        Assertions.assertEquals(SupportedPBECryptoAlgo.PBKDF2WithHmacSHA256, rangerMasterKey.getSelectedCryptoAlgorithm());

        Assertions.assertTrue(rangerMasterKey.reencryptMKWithFipsAlgo(masterKeyPassword));

        // this checks the Algo name written in the DB.
        Assertions.assertEquals(SupportedPBECryptoAlgo.PBKDF2WithHmacSHA256, rangerMasterKey.getMKEncryptionAlgoName());

        Assertions.assertEquals(masterKey, rangerMasterKey.getMasterKey(masterKeyPassword));

        Assertions.assertFalse(rangerMasterKey.reencryptMKWithFipsAlgo(masterKeyPassword));

        // revert the FIPS specific changes so that other cases can execute with default provider.
        Security.removeProvider(provider.getName());
        Security.setProperty("keystore.type", "jks");
        rangerMasterKey.resetDefaultMDAlgoAndEncrAlgo();
    }

    @Test
    public void testReencryptMKWithFipsAlgo() throws Exception {
        if (!UNRESTRICTED_POLICIES_INSTALLED) {
            return;
        }

        String masterKeyPassword = "password0password0password0password0password0password0password0password0"
                + "password0password0password0password0password0password0password0password0password0password0"
                + "password0password0password0password0password0password0password0password0password0password0";

        Assertions.assertFalse(rangerMasterKey.reencryptMKWithFipsAlgo(masterKeyPassword));  // or assertTrue if expecting re-encryption to happen
    }

    @Test
    public void testGenerateMKFromHSMMK() throws Throwable {
        if (!UNRESTRICTED_POLICIES_INSTALLED) {
            return;
        }

        String password = "password0password0password0password0password0password0password0password0"
                + "password0password0password0password0password0password0password0password0"
                + "password0password0password0password0password0password0password0";
        byte[] key = {0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07,
                0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f,
                0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17};
        rangerMasterKey.generateMKFromHSMMK(password, key);
    }

    @Test
    public void testGenerateMKFromKeySecureMK() throws Throwable {
        if (!UNRESTRICTED_POLICIES_INSTALLED) {
            return;
        }

        String password = "password0password0password0password0password0password0password0password0"
                + "password0password0password0password0password0password0password0";
        byte[] key = {0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07,
                0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f,
                0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17};
        rangerMasterKey.generateMKFromKeySecureMK(password, key);

        assertNotNull(rangerMasterKey.getMasterKey(password));
    }

    @Test
    public void testDecryptMasterKeySK() throws Exception {
        if (!UNRESTRICTED_POLICIES_INSTALLED) {
            return;
        }

        String password = "password0password0password0password0password0password0password0password0"
                + "password0password0password0password0password0password0password0";

        // Simulate a generated master key
        byte[] rawKey        = "myMockRawKey1234567890".getBytes();  // Simulated secret key material
        Method encryptMethod = RangerMasterKey.class.getDeclaredMethod("encryptMasterKey", String.class, byte[].class);
        encryptMethod.setAccessible(true);
        String encryptedStr = (String) encryptMethod.invoke(rangerMasterKey, password, rawKey);

        // Now decrypt using the method under test
        Method decryptMethod = RangerMasterKey.class.getDeclaredMethod(
                "decryptMasterKeySK", byte[].class, String.class, String.class);
        decryptMethod.setAccessible(true);

        byte[]    encryptedBytes = Base64.decode(encryptedStr);
        SecretKey secretKey      = (SecretKey) decryptMethod.invoke(rangerMasterKey, encryptedBytes, password, encryptedStr);

        assertNotNull(secretKey);
    }

    @Test
    public void testGetIntConfig_ReturnsDefaultWhenPropertyMissing() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        if (!UNRESTRICTED_POLICIES_INSTALLED) {
            return;
        }

        Method method = RangerMasterKey.class.getDeclaredMethod("getIntConfig", String.class, int.class);
        method.setAccessible(true);
        int defaultValue = 42;
        int result       = (int) method.invoke(rangerMasterKey, "non.existent.property", defaultValue);
        Assertions.assertEquals(defaultValue, result, "Expected default value when property is missing");
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
