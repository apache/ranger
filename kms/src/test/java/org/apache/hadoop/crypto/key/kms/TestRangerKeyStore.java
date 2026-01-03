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

package org.apache.hadoop.crypto.key.kms;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.key.KeyProvider;
import org.apache.hadoop.crypto.key.RangerAzureKeyVaultKeyGenerator;
import org.apache.hadoop.crypto.key.RangerKeyStore;
import org.apache.hadoop.crypto.key.RangerMasterKey;
import org.apache.ranger.entity.XXRangerKeyStore;
import org.apache.ranger.kms.dao.DaoManager;
import org.apache.ranger.kms.dao.RangerKMSDao;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import javax.crypto.KeyGenerator;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.security.Key;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.util.Collections;
import java.util.Date;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@TestMethodOrder(MethodOrderer.MethodName.class)
public class TestRangerKeyStore {
    String fileFormat       = "jceks";
    String keyStoreFileName = "KmsKeyStoreFile";
    char[] storePass        = "none".toCharArray();
    char[] keyPass          = "none".toCharArray();
    char[] masterKey        = "MasterPassword".toCharArray();

    @BeforeEach
    public void checkFileIfExists() {
        deleteKeyStoreFile();
    }

    @AfterEach
    public void cleanKeystoreFile() {
        deleteKeyStoreFile();
    }

    @Test
    public void testInvalidKey1() {
        DaoManager     daoManager     = mock(DaoManager.class);
        RangerKeyStore rangerKeyStore = new RangerKeyStore(daoManager);
        String         keyValue       = "enckey:1";
        Exception exception = Assertions.assertThrows(IOException.class, () -> {
            InputStream inputStream = generateKeyStoreFile(keyValue);
            rangerKeyStore.engineLoadKeyStoreFile(inputStream, storePass, keyPass, masterKey, fileFormat);
            inputStream.close();
        });
    }

    @Test
    public void testInvalidKey2() throws NoSuchAlgorithmException, CertificateException, IOException, KeyStoreException {
        DaoManager     daoManager     = mock(DaoManager.class);
        RangerKeyStore rangerKeyStore = new RangerKeyStore(daoManager);
        String         keyValue       = "1%enckey";
        Assertions.assertThrows(IOException.class, () -> {
            InputStream inputStream = generateKeyStoreFile(keyValue);
            rangerKeyStore.engineLoadKeyStoreFile(inputStream, storePass, keyPass, masterKey, fileFormat);
            inputStream.close();
        });
    }

    @Test
    public void testInvalidKey3() throws NoSuchAlgorithmException, CertificateException, IOException, KeyStoreException {
        DaoManager     daoManager     = mock(DaoManager.class);
        RangerKeyStore rangerKeyStore = new RangerKeyStore(daoManager);
        String         keyValue       = "1 enckey";
        Assertions.assertThrows(IOException.class, () -> {
            InputStream inputStream = generateKeyStoreFile(keyValue);
            rangerKeyStore.engineLoadKeyStoreFile(inputStream, storePass, keyPass, masterKey, fileFormat);
            inputStream.close();
        });
    }

    @Test
    public void testInvalidKey4() throws NoSuchAlgorithmException, CertificateException, IOException, KeyStoreException {
        DaoManager     daoManager     = mock(DaoManager.class);
        RangerKeyStore rangerKeyStore = new RangerKeyStore(daoManager);
        String         keyValue       = "_1-enckey";
        Assertions.assertThrows(IOException.class, () -> {
            InputStream inputStream = generateKeyStoreFile(keyValue);
            rangerKeyStore.engineLoadKeyStoreFile(inputStream, storePass, keyPass, masterKey, fileFormat);
            inputStream.close();
        });
    }

    @Test
    public void testValidKey1() throws NoSuchAlgorithmException, CertificateException, IOException, KeyStoreException {
        DaoManager     daoManager     = mock(DaoManager.class);
        RangerKeyStore rangerKeyStore = new RangerKeyStore(daoManager);
        String         keyValue       = "enckey_1-test";
        InputStream    inputStream    = generateKeyStoreFile(keyValue);
        rangerKeyStore.engineLoadKeyStoreFile(inputStream, storePass, keyPass, masterKey, fileFormat);
        inputStream.close();
    }

    @Test
    public void testValidKey2() throws NoSuchAlgorithmException, CertificateException, IOException, KeyStoreException {
        DaoManager     daoManager     = mock(DaoManager.class);
        RangerKeyStore rangerKeyStore = new RangerKeyStore(daoManager);
        String         keyValue       = "1-enckey_test";
        InputStream    inputStream    = generateKeyStoreFile(keyValue);
        rangerKeyStore.engineLoadKeyStoreFile(inputStream, storePass, keyPass, masterKey, fileFormat);
        inputStream.close();
    }

    @Test
    public void testEngineGetCertificateChain() {
        DaoManager     daoManager     = mock(DaoManager.class);
        RangerKeyStore rangerKeyStore = new RangerKeyStore(daoManager);

        Certificate[] result = rangerKeyStore.engineGetCertificateChain("enckey_1-test");
        assertNull(result, "Certificate chain should be null for a key that does not exist in the keystore");
    }

    @Test
    public void testEngineGetCertificate() {
        DaoManager     daoManager     = mock(DaoManager.class);
        RangerKeyStore rangerKeyStore = new RangerKeyStore(daoManager);

        Certificate result = rangerKeyStore.engineGetCertificate("enckey_1-test");
        assertNull(result, "Certificate should be null for a key that does not exist in the keystore");
    }

    @Test
    public void testEngineGetCreationDate_NoEntry() {
        DaoManager     daoManager     = mock(DaoManager.class);
        RangerKeyStore rangerKeyStore = new RangerKeyStore(daoManager);

        Date result = rangerKeyStore.engineGetCreationDate("missing-key");

        assertNull(result, "Creation date should be null for a key that does not exist");
    }

    @Test
    public void testEngineSize() {
        DaoManager     daoManager     = mock(DaoManager.class);
        RangerKeyStore rangerKeyStore = new RangerKeyStore(daoManager);

        int size = rangerKeyStore.engineSize();
        assertEquals(0, size, "Size should be 0 for an empty keystore");
    }

    @Test
    public void testEngineIsKeyEntry() {
        DaoManager     daoManager     = mock(DaoManager.class);
        RangerKeyStore rangerKeyStore = new RangerKeyStore(daoManager);

        boolean result = rangerKeyStore.engineIsKeyEntry("enckey_1-test");
        assertFalse(result, "isKeyEntry should return false for a key that does not exist in the keystore");
    }

    @Test
    public void testEngineIsCertificateEntry() {
        DaoManager     daoManager     = mock(DaoManager.class);
        RangerKeyStore rangerKeyStore = new RangerKeyStore(daoManager);

        boolean result = rangerKeyStore.engineIsCertificateEntry("enckey_1-test");
        assertFalse(result, "isCertificateEntry should return false for a key that does not exist in the keystore");
    }

    @Test
    public void testEngineGetCertificateAlias() {
        DaoManager     daoManager     = mock(DaoManager.class);
        RangerKeyStore rangerKeyStore = new RangerKeyStore(daoManager);
        Certificate    mockCert       = mock(Certificate.class);

        String result = rangerKeyStore.engineGetCertificateAlias(mockCert);
        assertNull(result, "engineGetCertificateAlias should return null for any certificate (default implementation)");
    }

    @Test
    public void testEngineStore_if() throws Exception {
        DaoManager     daoManager     = mock(DaoManager.class);
        RangerKeyStore rangerKeyStore = new RangerKeyStore(daoManager);

        Field keyVaultEnabledField = RangerKeyStore.class.getDeclaredField("keyVaultEnabled");
        keyVaultEnabledField.setAccessible(true);
        keyVaultEnabledField.set(rangerKeyStore, true);

        rangerKeyStore.engineStore(null, null);
    }

    @Test
    public void testEngineStore_ThrowsIllegalArgumentException_WhenPasswordIsNull() throws Exception {
        DaoManager     daoManager     = mock(DaoManager.class);
        RangerKeyStore rangerKeyStore = new RangerKeyStore(daoManager);

        Field keyVaultEnabledField = RangerKeyStore.class.getDeclaredField("keyVaultEnabled");
        keyVaultEnabledField.setAccessible(true);
        keyVaultEnabledField.set(rangerKeyStore, false);

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () -> {
            rangerKeyStore.engineStore(null, null);
        });

        assertTrue(ex.getMessage().contains("Ranger Master Key can't be null"));
    }

    @Test
    @Disabled
    public void testAddKeyEntry_throwsKeyStoreException_whenSealKeyFails() throws Exception {
        DaoManager     daoManager     = mock(DaoManager.class);
        RangerKeyStore rangerKeyStore = new RangerKeyStore(daoManager);

        String alias       = "testkey";
        char[] password    = "123".toCharArray();
        String cipher      = "AES";
        int    bitLength   = 128;
        String description = null;
        int    version     = 1;
        String attributes  = "key123";

        Key faultyKey = mock(Key.class);
        lenient().when(faultyKey.getEncoded()).thenReturn(null);

        assertThrows(KeyStoreException.class, () ->
                rangerKeyStore.addKeyEntry(alias, faultyKey, password, cipher, bitLength, description, version, attributes));
    }

    @Test
    @Disabled
    public void testDbOperationStore_whenUpdateThrowsException() {
        DaoManager     daoManager     = mock(DaoManager.class);
        RangerKeyStore rangerKeyStore = new RangerKeyStore(daoManager);
        RangerKMSDao   rangerKMSDao   = mock(RangerKMSDao.class);

        try {
            Field kmsDaoField = RangerKeyStore.class.getDeclaredField("kmsDao");
            kmsDaoField.setAccessible(true);
            kmsDaoField.set(rangerKeyStore, rangerKMSDao);
        } catch (Exception e) {
            //
        }

        XXRangerKeyStore input = new XXRangerKeyStore();
        input.setAlias("test-alias");

        when(rangerKMSDao.findByAlias(anyString())).thenReturn(new XXRangerKeyStore());

        doThrow(new RuntimeException("Simulated DB update error")).when(rangerKMSDao).update(any());
        rangerKeyStore.dbOperationStore(input);
        verify(rangerKMSDao).update(any());
        verify(rangerKMSDao, never()).create(any());
    }

    @Test
    public void testDbOperationStore_whenKeyStoreExists_shouldCallUpdate() throws Exception {
        DaoManager     daoManager     = mock(DaoManager.class);
        RangerKeyStore rangerKeyStore = new RangerKeyStore(daoManager);
        RangerKMSDao   rangerKMSDao   = mock(RangerKMSDao.class);

        XXRangerKeyStore input = new XXRangerKeyStore();
        input.setAlias("test-alias");

        XXRangerKeyStore existing = new XXRangerKeyStore();
        existing.setAlias("test-alias");

        Field kmsDaoField = RangerKeyStore.class.getDeclaredField("kmsDao");
        kmsDaoField.setAccessible(true);
        kmsDaoField.set(rangerKeyStore, rangerKMSDao);

        when(rangerKMSDao.findByAlias("test-alias")).thenReturn(existing);

        rangerKeyStore.dbOperationStore(input);

        verify(rangerKMSDao).update(any(XXRangerKeyStore.class));
        verify(rangerKMSDao, never()).create(any());
    }

    @Test
    @Disabled
    public void testDbOperationDelete() throws Exception {
        DaoManager     daoManager     = mock(DaoManager.class);
        RangerKeyStore rangerKeyStore = new RangerKeyStore(daoManager);
        RangerKMSDao   rangerKMSDao   = mock(RangerKMSDao.class);

        Field kmsDaoField = RangerKeyStore.class.getDeclaredField("kmsDao");
        kmsDaoField.setAccessible(true);
        kmsDaoField.set(rangerKeyStore, rangerKMSDao);

        doThrow(new RuntimeException("Simulated error")).when(rangerKMSDao).deleteByAlias("test-alias");

        Method method = RangerKeyStore.class.getDeclaredMethod("dbOperationDelete", String.class);
        method.setAccessible(true);

        assertDoesNotThrow(() -> method.invoke(rangerKeyStore, "test-alias"));

        verify(rangerKMSDao).deleteByAlias("test-alias");
    }

    @Test
    @Disabled
    public void testDbOperationLoad() throws Exception {
        DaoManager     daoManager     = mock(DaoManager.class);
        RangerKeyStore rangerKeyStore = new RangerKeyStore(daoManager);
        RangerKMSDao   rangerKMSDao   = mock(RangerKMSDao.class);

        Field kmsDaoField = RangerKeyStore.class.getDeclaredField("kmsDao");
        kmsDaoField.setAccessible(true);
        kmsDaoField.set(rangerKeyStore, rangerKMSDao);

        doThrow(new RuntimeException("Error")).when(rangerKMSDao).getAllKeys();

        Method method = RangerKeyStore.class.getDeclaredMethod("dbOperationLoad");
        method.setAccessible(true);

        assertDoesNotThrow(() -> method.invoke(rangerKeyStore));
        verify(rangerKMSDao).getAllKeys();
    }

    @Test
    public void testEngineGetCreationDate_WithoutKeyEntryDirectUse() throws Exception {
        DaoManager     daoManager     = mock(DaoManager.class);
        RangerKeyStore rangerKeyStore = new RangerKeyStore(daoManager);
        String         alias          = "alias";
        Date           expectedDate   = new Date();

        Class<?>       keyEntryClass = Class.forName("org.apache.hadoop.crypto.key.RangerKeyStore$KeyEntry");
        Constructor<?> constructor   = keyEntryClass.getDeclaredConstructor();
        constructor.setAccessible(true);
        Object keyEntryInstance = constructor.newInstance();

        Field dateField = keyEntryClass.getDeclaredField("date");
        dateField.setAccessible(true);
        dateField.set(keyEntryInstance, expectedDate);

        Field keyEntriesField = RangerKeyStore.class.getDeclaredField("keyEntries");
        keyEntriesField.setAccessible(true);
        @SuppressWarnings("unchecked")
        Map<String, Object> keyEntries = (Map<String, Object>) keyEntriesField.get(rangerKeyStore);
        keyEntries.put(alias, keyEntryInstance);

        Date result = rangerKeyStore.engineGetCreationDate(alias);

        assertNotNull(result);
        assertEquals(expectedDate.getTime(), result.getTime());
        assertNotSame(expectedDate, result);
    }

    @Test
    public void testRangerKeyStore_UsesAzureKeyVaultProvider() throws Exception {
        DaoManager   daoManager = mock(DaoManager.class);
        RangerKMSDao kmsDao     = mock(RangerKMSDao.class);
        when(daoManager.getRangerKMSDao()).thenReturn(kmsDao);

        Configuration conf = new Configuration();
        conf.set("ranger.kms.azure.keyvault.enabled", "true");

        RangerKeyStore rangerKeyStore = new RangerKeyStore(daoManager, conf, null);

        Field masterKeyProviderField = RangerKeyStore.class.getDeclaredField("masterKeyProvider");
        masterKeyProviderField.setAccessible(true);
        Object masterKeyProvider = masterKeyProviderField.get(rangerKeyStore);

        assertNotNull(masterKeyProvider);
        assertInstanceOf(RangerAzureKeyVaultKeyGenerator.class, masterKeyProvider, "Expected masterKeyProvider to be RangerAzureKeyVaultKeyGenerator");
    }

    @Test
    public void testEngineSetKeyEntry() {
        DaoManager     daoManager     = mock(DaoManager.class);
        RangerKeyStore rangerKeyStore = new RangerKeyStore(daoManager);

        String        alias       = "testKey";
        Key           key         = mock(Key.class);
        char[]        password    = "testPassword".toCharArray();
        Certificate[] certs       = new Certificate[] {mock(Certificate.class)};
        Certificate   certificate = mock(Certificate.class);
        byte[]        arg1        = "Hello, byte array!".getBytes(StandardCharsets.UTF_8);

        assertDoesNotThrow(() -> rangerKeyStore.engineSetKeyEntry(alias, key, password, certs));
        assertDoesNotThrow(() -> rangerKeyStore.engineSetKeyEntry(alias, arg1, certs));
        assertDoesNotThrow(() -> rangerKeyStore.engineSetCertificateEntry(alias, certificate));
    }

    @Test
    public void testGetAlgorithm() {
        DaoManager     daoManager     = mock(DaoManager.class);
        RangerKeyStore rangerKeyStore = new RangerKeyStore(daoManager);
        String         cipher         = "AES";

        String result = rangerKeyStore.getAlgorithm(cipher);

        assertEquals(cipher, result, "getAlgorithm should return the same cipher string when no override logic is applied");
    }

    @Test
    void testEngineGetDecryptedZoneKeyByte() throws Exception {
        DaoManager     daoManager     = mock(DaoManager.class);
        RangerKeyStore rangerKeyStore = new RangerKeyStore(daoManager);
        String         alias          = "testKey";

        assertNull(rangerKeyStore.engineGetDecryptedZoneKeyByte(alias));
    }

    @Test
    void testEngineGetDecryptedZoneKey() throws Exception {
        DaoManager     daoManager     = mock(DaoManager.class);
        RangerKeyStore rangerKeyStore = new RangerKeyStore(daoManager);
        String         alias          = "testKey";

        Key result = rangerKeyStore.engineGetDecryptedZoneKey(alias);
        assertNotNull(result);
    }

    @Test
    void testEngineGetKeyMetadata() throws Exception {
        DaoManager     daoManager     = mock(DaoManager.class);
        RangerKeyStore rangerKeyStore = new RangerKeyStore(daoManager);
        String         alias          = "testKey";

        KeyProvider.Metadata result = rangerKeyStore.engineGetKeyMetadata(alias);
        assertNull(result);
    }

    @Test
    @Disabled
    void testAddSecureKeyByteEntry_EncryptFails_ThrowsKeyStoreException() throws Exception {
        DaoManager     daoManager     = mock(DaoManager.class);
        RangerKeyStore rangerKeyStore = spy(new RangerKeyStore(daoManager));

        Key             mockKey               = mock(Key.class);
        RangerMasterKey mockMasterKeyProvider = mock(RangerMasterKey.class);

        Field field = RangerKeyStore.class.getDeclaredField("masterKeyProvider");
        field.setAccessible(true);
        field.set(rangerKeyStore, mockMasterKeyProvider);

        when(mockMasterKeyProvider.encryptZoneKey(any(Key.class)))
                .thenThrow(new RuntimeException("encryption failed"));

        KeyStoreException thrown = assertThrows(
                KeyStoreException.class,
                () -> rangerKeyStore.addSecureKeyByteEntry("testAlias", mockKey, "AES", 128, "desc", 1, null));
        assertEquals("encryption failed", thrown.getMessage());
    }

    @Test
    void testAddSecureKeyByteEntry() throws Exception {
        DaoManager     daoManager     = mock(DaoManager.class);
        RangerKeyStore rangerKeyStore = spy(new RangerKeyStore(daoManager));

        Key    key         = mock(Key.class);
        String alias       = "testKey";
        String cipher      = "AES";
        int    bitLength   = 128;
        String description = "Test Key";
        int    version     = 1;
        String attributes  = "key123";

        RangerMasterKey rangerMasterKey   = mock(RangerMasterKey.class);
        byte[]          encryptedKeyBytes = new byte[] {1, 2, 3, 4, 5};
        when(rangerMasterKey.encryptZoneKey(key)).thenReturn(encryptedKeyBytes);

        Field masterKeyProviderField = RangerKeyStore.class.getDeclaredField("masterKeyProvider");
        masterKeyProviderField.setAccessible(true);
        masterKeyProviderField.set(rangerKeyStore, rangerMasterKey);

        assertDoesNotThrow(() -> rangerKeyStore.addSecureKeyByteEntry(
                alias, key, cipher, bitLength, description, version, attributes));
    }

    @Test
    void testEngineLoadToKeyStoreFile_Success() throws Exception {
        DaoManager     daoManager     = mock(DaoManager.class);
        RangerKeyStore rangerKeyStore = spy(new RangerKeyStore(daoManager));
        String         alias          = "testkey";
        Key            dummyKey       = mock(Key.class);

        doReturn(Collections.enumeration(Collections.singletonList(alias))).when(rangerKeyStore).engineAliases();
        doReturn(dummyKey).when(rangerKeyStore).engineGetKey(eq(alias), any());
        doNothing().when(rangerKeyStore).engineLoad(any(), any());

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        char[]                storePass    = "storepass".toCharArray();
        char[]                keyPass      = "keypass".toCharArray();
        char[]                masterKey    = "masterpass".toCharArray();
        String                fileFormat   = "JCEKS";

        assertDoesNotThrow(() ->
                rangerKeyStore.engineLoadToKeyStoreFile(outputStream, storePass, keyPass, masterKey, fileFormat));
    }

    private InputStream generateKeyStoreFile(String keyValue) throws KeyStoreException, NoSuchAlgorithmException, CertificateException, IOException {
        FileOutputStream stream = new FileOutputStream(new File(keyStoreFileName));
        KeyStore         ks;
        try {
            ks = KeyStore.getInstance(fileFormat);
            if (ks != null) {
                ks.load(null, storePass);
                String alias = keyValue;

                KeyGenerator kg = KeyGenerator.getInstance("AES");
                kg.init(256);
                Key key = kg.generateKey();
                ks.setKeyEntry(alias, key, keyPass, null);
                ks.store(stream, storePass);
            }
            return new FileInputStream(new File(keyStoreFileName));
        } catch (Throwable t) {
            throw new IOException(t);
        } finally {
            stream.close();
        }
    }

    private void deleteKeyStoreFile() {
        File f = new File(keyStoreFileName);
        if (f.exists()) {
            boolean bol = f.delete();
            if (!bol) {
                System.out.println("Keystore File was not deleted successfully.");
            }
        }
    }
}
