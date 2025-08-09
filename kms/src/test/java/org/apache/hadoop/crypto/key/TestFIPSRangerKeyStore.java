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

import org.apache.hadoop.crypto.key.kms.server.DerbyTestUtils;
import org.apache.hadoop.crypto.key.kms.server.KMSConfiguration;
import org.apache.ranger.kms.dao.DaoManager;
import org.apache.ranger.plugin.util.JsonUtilsV2;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.crypto.Cipher;
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.NoSuchAlgorithmException;
import java.security.Security;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

public class TestFIPSRangerKeyStore {
    private static final boolean UNRESTRICTED_POLICIES_INSTALLED;

    private DaoManager daoManager;

    private RangerKeyStore rangerKeyStore;

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

        // This is required. AzureKeyVault Authentication code adds BouncyCastleProvider in the provider list and creates problem if key is created using non-fips key.
        Security.removeProvider("BC");
    }

    @AfterEach
    public void tearDown() {
        this.rangerKeyStore = null;
    }

    /**
     * This test case creates Zone key with FIPS Algorithm, provider is BouncyCastleProvider
     * Scenario:
     *        Simple Zone key creation using FIPS algo
     * Assertions:
     *        Retrieved key from DB and after unseal operation, key material should match with the original key material
     *        On FIPS enabled env, Key Attribute map should contain the encryptionAlgo name, that is, PBKDF2WithHmacSHA256
     * @throws Throwable
     */
    @Test
    public void testZoneKeyEncryptionWithFipsAlgo() throws Throwable {
        if (!UNRESTRICTED_POLICIES_INSTALLED) {
            return;
        }

        BouncyCastleProvider bouncyCastleProvider = null;
        try {
            // Create a Zone key
            char[] masterKey = "masterkey".toCharArray();
            String keyName = "fipstestkey";
            String versionName = keyName + "@0";
            String cipher = "AES";
            int bitLength = 192;
            String attribute   = JsonUtilsV2.mapToJson(Collections.emptyMap());
            byte[] originalKeyMaterial = generateKey(bitLength, cipher);

            bouncyCastleProvider = new BouncyCastleProvider();
            Security.insertProviderAt(bouncyCastleProvider, 1);
            this.rangerKeyStore = new RangerKeyStore(true,  this.daoManager);

            this.rangerKeyStore.addKeyEntry(versionName, new SecretKeySpec(originalKeyMaterial, cipher), masterKey, cipher, bitLength, "fipstestkey", 1, attribute);
            this.rangerKeyStore.engineStore(null, masterKey);

            SecretKeySpec key = (SecretKeySpec) this.rangerKeyStore.engineGetKey(versionName, masterKey);
            Assertions.assertNotNull(key);
            Assertions.assertTrue(Arrays.equals(originalKeyMaterial, key.getEncoded()));
            Assertions.assertEquals(SupportedPBECryptoAlgo.PBKDF2WithHmacSHA256.getAlgoName(), this.getKeyAttributeMap(this.rangerKeyStore, versionName).get(RangerKeyStore.KEY_CRYPTO_ALGO_NAME));

            this.rangerKeyStore.engineDeleteEntry(versionName);
            Assertions.assertNull(this.rangerKeyStore.engineGetKey(versionName, masterKey));
        } finally {
            if (null != bouncyCastleProvider) {
                Security.removeProvider(bouncyCastleProvider.getName());
            }
        }
    }

    /**
     * This test case creates Zone key with Non-FIPS Algorithm, and then reencrypts the key using FIPS Algorithm, provider is BouncyCastleProvider.
     * Purpose of this test case is to verify key-reencryption logic.
     * Scenario:
     *        Simple Zone key creation using Non-FIPS algo
     *        Re-encrypt the zone key using FIPS-Algo
     * Assertions:
     *        Before reencryption: Retrieved key from DB and after unseal operation, key material should match with the original key material and Key Attribute map should be empty in this case.
     *        After reencryption: Retrieved key from DB and after unseal operation, key material should match with the original key material
     *                            And Key Attribute map should contain the encryptionAlgo name, that is, PBKDF2WithHmacSHA256
     * @throws Throwable
     */
    @Test
    public void testZoneKeyReencryptionWithFipsAlgo() throws Throwable {
        if (!UNRESTRICTED_POLICIES_INSTALLED) {
            return;
        }

        BouncyCastleProvider bouncyCastleProvider = null;
        try {
            // Create a Zone key
            char[] masterKey = "masterkey".toCharArray();
            String keyName = "fipstestkey";
            String versionName = keyName + "@0";
            String cipher = "AES";
            int bitLength = 192;

            this.rangerKeyStore = new RangerKeyStore(this.daoManager);
            String attribute   = JsonUtilsV2.mapToJson(Collections.emptyMap());
            byte[] originalKeyMaterial = generateKey(bitLength, cipher);

            this.rangerKeyStore.addKeyEntry(versionName, new SecretKeySpec(originalKeyMaterial, cipher), masterKey, cipher, bitLength, "fipstestkey", 1, attribute);
            this.rangerKeyStore.engineStore(null, masterKey);

            SecretKeySpec key = (SecretKeySpec) this.rangerKeyStore.engineGetKey(versionName, masterKey);
            Assertions.assertNotNull(key);
            Assertions.assertTrue(Arrays.equals(originalKeyMaterial, key.getEncoded()));
            Assertions.assertNull(this.getKeyAttributeMap(this.rangerKeyStore, versionName).get(RangerKeyStore.KEY_CRYPTO_ALGO_NAME));

            // Till now, ZoneKey was created using non-fips algo.
            // Now set FIPS parameters and invoke reencrypt.

            bouncyCastleProvider = new BouncyCastleProvider();
            Security.insertProviderAt(bouncyCastleProvider, 1);
            this.rangerKeyStore = new RangerKeyStore(true, this.daoManager);

            this.rangerKeyStore.reencryptZoneKeysWithNewAlgo(null, masterKey);

            key = (SecretKeySpec) this.rangerKeyStore.engineGetKey(versionName, masterKey);
            Assertions.assertNotNull(key);
            Assertions.assertTrue(Arrays.equals(originalKeyMaterial, key.getEncoded()));
            Assertions.assertEquals(SupportedPBECryptoAlgo.PBKDF2WithHmacSHA256.getAlgoName(), this.getKeyAttributeMap(this.rangerKeyStore, versionName).get(RangerKeyStore.KEY_CRYPTO_ALGO_NAME));

            this.rangerKeyStore.engineDeleteEntry(versionName);
            Assertions.assertNull(this.rangerKeyStore.engineGetKey(versionName, masterKey));
        } finally {
            if (null != bouncyCastleProvider) {
                Security.removeProvider(bouncyCastleProvider.getName());
            }
        }
    }

    private Map<String, String> getKeyAttributeMap(RangerKeyStore rangerKeyStore, String alias) throws Exception {
        Class<?> cls = rangerKeyStore.getClass();

        Method method = cls.getDeclaredMethod("getKeyEntry", String.class);
        method.setAccessible(true);
        Object secretEntry = (Object) method.invoke(rangerKeyStore, alias);

        Class<?>       secretKeyEntryCls = Class.forName("org.apache.hadoop.crypto.key.RangerKeyStore$SecretKeyEntry");
        Field secretkeyAttrField = secretKeyEntryCls.getDeclaredField("attributes");
        secretkeyAttrField.setAccessible(true);
        String attribute = (String) secretkeyAttrField.get(secretEntry);

        return JsonUtilsV2.jsonToMap(attribute);
    }

    private byte[] generateKey(int size, String algorithm) throws NoSuchAlgorithmException {
        KeyGenerator keyGenerator = KeyGenerator.getInstance(algorithm);
        keyGenerator.init(size);
        byte[] key = keyGenerator.generateKey().getEncoded();
        return key;
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
