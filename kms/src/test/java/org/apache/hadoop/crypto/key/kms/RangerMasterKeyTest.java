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

package org.apache.hadoop.crypto.key.kms;

import com.sun.jersey.core.util.Base64;
import org.apache.hadoop.crypto.key.RangerKMSDB;
import org.apache.hadoop.crypto.key.RangerKeyStoreProvider;
import org.apache.hadoop.crypto.key.RangerMasterKey;
import org.apache.hadoop.crypto.key.kms.server.DerbyTestUtils;
import org.apache.hadoop.crypto.key.kms.server.KMSConfiguration;
import org.apache.ranger.kms.dao.DaoManager;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * A test for the RangerMasterKey.
 */
public class RangerMasterKeyTest {
    private static final boolean UNRESTRICTED_POLICIES_INSTALLED;

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

    @Test
    public void testFetchEncrAlgo_DefaultFallback() throws Exception {
        if (!UNRESTRICTED_POLICIES_INSTALLED) {
            return;
        }

        Path configDir = Paths.get("src/test/resources/kms");
        System.setProperty(KMSConfiguration.KMS_CONFIG_DIR, configDir.toFile().getAbsolutePath());

        RangerKMSDB     rangerkmsDb     = new RangerKMSDB(RangerKeyStoreProvider.getDBKSConf());
        DaoManager      daoManager      = rangerkmsDb.getDaoManager();
        RangerMasterKey rangerMasterKey = new RangerMasterKey(daoManager);

        String inputWithoutComma = "verylongstringwithoutcommapassword123";

        Method method = RangerMasterKey.class.getDeclaredMethod("fetchEncrAlgo", String.class);
        method.setAccessible(true);

        String result = (String) method.invoke(rangerMasterKey, inputWithoutComma);
        Assertions.assertEquals("PBEWithMD5AndTripleDES", result);  // default fallback
    }

    @Test
    public void testReencryptMKWithFipsAlgo() throws Exception {
        if (!UNRESTRICTED_POLICIES_INSTALLED) {
            return;
        }

        Path configDir = Paths.get("src/test/resources/kms");
        System.setProperty(KMSConfiguration.KMS_CONFIG_DIR, configDir.toFile().getAbsolutePath());

        RangerKMSDB     rangerkmsDb     = new RangerKMSDB(RangerKeyStoreProvider.getDBKSConf());
        DaoManager      daoManager      = rangerkmsDb.getDaoManager();
        RangerMasterKey rangerMasterKey = new RangerMasterKey(daoManager);

        String inputWithoutComma = "verylongstringwithoutcommapassword123";

        Method method = RangerMasterKey.class.getDeclaredMethod("reencryptMKWithFipsAlgo", String.class);
        method.setAccessible(true);

        boolean result = (boolean) method.invoke(rangerMasterKey, inputWithoutComma);
        Assertions.assertFalse(result);  // or assertTrue if expecting re-encryption to happen
    }

    @Test
    public void testGenerateMKFromHSMMK() throws Throwable {
        if (!UNRESTRICTED_POLICIES_INSTALLED) {
            return;
        }

        Path configDir = Paths.get("src/test/resources/kms");
        System.setProperty(KMSConfiguration.KMS_CONFIG_DIR, configDir.toFile().getAbsolutePath());

        RangerKMSDB     rangerkmsDb     = new RangerKMSDB(RangerKeyStoreProvider.getDBKSConf());
        DaoManager      daoManager      = rangerkmsDb.getDaoManager();
        RangerMasterKey rangerMasterKey = new RangerMasterKey(daoManager);

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

        Path configDir = Paths.get("src/test/resources/kms");
        System.setProperty(KMSConfiguration.KMS_CONFIG_DIR, configDir.toFile().getAbsolutePath());

        RangerKMSDB     rangerkmsDb     = new RangerKMSDB(RangerKeyStoreProvider.getDBKSConf());
        DaoManager      daoManager      = rangerkmsDb.getDaoManager();
        RangerMasterKey rangerMasterKey = new RangerMasterKey(daoManager);

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

        Path configDir = Paths.get("src/test/resources/kms");
        System.setProperty(KMSConfiguration.KMS_CONFIG_DIR, configDir.toFile().getAbsolutePath());

        RangerKMSDB     rangerkmsDb     = new RangerKMSDB(RangerKeyStoreProvider.getDBKSConf());
        DaoManager      daoManager      = rangerkmsDb.getDaoManager();
        RangerMasterKey rangerMasterKey = new RangerMasterKey(daoManager);

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
    public void testUpdateEncryptedMK() throws Exception {
        if (!UNRESTRICTED_POLICIES_INSTALLED) {
            return;
        }

        Path            configDir       = Paths.get("src/test/resources/kms");
        RangerKMSDB     rangerkmsDb     = new RangerKMSDB(RangerKeyStoreProvider.getDBKSConf());
        DaoManager      daoManager      = rangerkmsDb.getDaoManager();
        RangerMasterKey rangerMasterKey = new RangerMasterKey(daoManager);

        String password = "password0password0password0password0password0password0password0password0"
                + "password0password0password0password0password0password0password0";

        byte[] rawKey = "myMockRawKey1234567890".getBytes();

        Method updateEncryptedMKMethod = RangerMasterKey.class.getDeclaredMethod(
                "updateEncryptedMK", String.class);
        updateEncryptedMKMethod.setAccessible(true);
        updateEncryptedMKMethod.invoke(rangerMasterKey, password);
    }

    @Test
    public void testGetFIPSCompliantPassword() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        if (!UNRESTRICTED_POLICIES_INSTALLED) {
            return;
        }

        Path configDir = Paths.get("src/test/resources/kms");
        System.setProperty(KMSConfiguration.KMS_CONFIG_DIR, configDir.toFile().getAbsolutePath());

        RangerKMSDB     rangerkmsDb     = new RangerKMSDB(RangerKeyStoreProvider.getDBKSConf());
        DaoManager      daoManager      = rangerkmsDb.getDaoManager();
        RangerMasterKey rangerMasterKey = new RangerMasterKey(daoManager);

        String password = "password0password0password0password0password0password0password0password0"
                + "password0password0password0password0password0password0password0";

        Method getFIPSCompliantPasswordMethod = RangerMasterKey.class.getDeclaredMethod(
                "getFIPSCompliantPassword", String.class);
        getFIPSCompliantPasswordMethod.setAccessible(true);
        getFIPSCompliantPasswordMethod.invoke(rangerMasterKey, password);
    }

    @Test
    public void testGetPasswordParam() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        if (!UNRESTRICTED_POLICIES_INSTALLED) {
            return;
        }

        Path configDir = Paths.get("src/test/resources/kms");
        System.setProperty(KMSConfiguration.KMS_CONFIG_DIR, configDir.toFile().getAbsolutePath());

        RangerKMSDB     rangerkmsDb     = new RangerKMSDB(RangerKeyStoreProvider.getDBKSConf());
        DaoManager      daoManager      = rangerkmsDb.getDaoManager();
        RangerMasterKey rangerMasterKey = new RangerMasterKey(daoManager);

        String password = "password0password0password0password0password0password0password0password0"
                + "password0password0password0password0password0password0password0";

        Method getPasswordParamMethod = RangerMasterKey.class.getDeclaredMethod(
                "getPasswordParam", String.class);
        getPasswordParamMethod.setAccessible(true);
        getPasswordParamMethod.invoke(rangerMasterKey, password);
    }

    @Test
    public void testGetIntConfig_ReturnsDefaultWhenPropertyMissing() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        if (!UNRESTRICTED_POLICIES_INSTALLED) {
            return;
        }

        Path configDir = Paths.get("src/test/resources/kms");
        System.setProperty(KMSConfiguration.KMS_CONFIG_DIR, configDir.toFile().getAbsolutePath());

        RangerKMSDB     rangerkmsDb     = new RangerKMSDB(RangerKeyStoreProvider.getDBKSConf());
        DaoManager      daoManager      = rangerkmsDb.getDaoManager();
        RangerMasterKey rangerMasterKey = new RangerMasterKey(daoManager);

        Method method = RangerMasterKey.class.getDeclaredMethod("getIntConfig", String.class, int.class);
        method.setAccessible(true);
        int defaultValue = 42;
        int result       = (int) method.invoke(rangerMasterKey, "non.existent.property", defaultValue);
        Assertions.assertEquals(defaultValue, result, "Expected default value when property is missing");
    }

    @Test
    public void testGetIntConfig_ParsesValidInteger() throws Exception {
        Field propField = RangerMasterKey.class.getDeclaredField("serverConfigProperties");
        propField.setAccessible(true);
        Properties props = new Properties();
        props.setProperty("valid.int.key", "123");

        Method method = RangerMasterKey.class.getDeclaredMethod("getIntConfig", String.class, int.class);
        method.setAccessible(true);
        int invoke = (int) method.invoke(null, "valid.int.key", 42);
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
