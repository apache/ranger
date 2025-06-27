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

package org.apache.hadoop.crypto.key.kms.server;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.key.KeyProvider;
import org.apache.hadoop.crypto.key.KeyProvider.KeyVersion;
import org.apache.hadoop.crypto.key.KeyProvider.Options;
import org.apache.hadoop.crypto.key.RangerKMSMKI;
import org.apache.hadoop.crypto.key.RangerKeyStore;
import org.apache.hadoop.crypto.key.RangerKeyStoreProvider;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * A test for the RangerKeyStoreProvider, which is an implementation of the Hadoop KeyProvider interface, which stores keys in a database.
 * Apache Derby is used to create the relevant tables to store the keys in for this test.
 */
public class RangerKeyStoreProviderTest {
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

    @BeforeEach
    public void cleanUpKeyBeforeEachTest() throws Throwable {
        if (!UNRESTRICTED_POLICIES_INSTALLED) {
            return;
        }
        Path configDir = Paths.get("src/test/resources/kms");
        System.setProperty(KMSConfiguration.KMS_CONFIG_DIR, configDir.toFile().getAbsolutePath());

        Configuration conf = new Configuration();
        RangerKeyStoreProvider keyProvider = new RangerKeyStoreProvider(conf);

        try {
            keyProvider.deleteKey("newkey1");
            keyProvider.flush();
        } catch (IOException e) {
            // Ignore if the key doesn't exist yet
        }
    }

    @Test
    public void testCreateDeleteKey() throws Throwable {
        if (!UNRESTRICTED_POLICIES_INSTALLED) {
            return;
        }

        Path configDir = Paths.get("src/test/resources/kms");
        System.setProperty(KMSConfiguration.KMS_CONFIG_DIR, configDir.toFile().getAbsolutePath());

        Configuration conf = new Configuration();
        RangerKeyStoreProvider keyProvider = new RangerKeyStoreProvider(conf);

        // Create a key
        Options options = new Options(conf);
        options.setBitLength(128);
        options.setCipher("AES");
        KeyVersion keyVersion = keyProvider.createKey("newkey1", options);
        Assertions.assertEquals("newkey1", keyVersion.getName());
        Assertions.assertEquals(128 / 8, keyVersion.getMaterial().length);
        Assertions.assertEquals("newkey1@0", keyVersion.getVersionName());

        keyProvider.flush();
        Assertions.assertEquals(1, keyProvider.getKeys().size());
        keyProvider.deleteKey("newkey1");

        keyProvider.flush();
        Assertions.assertEquals(0, keyProvider.getKeys().size());
    }

    @Test
    public void testDeleteKey_EngineDeleteEntryThrowsForBaseKey() throws Throwable {
        Configuration          conf     = new Configuration();
        RangerKeyStoreProvider provider = spy(new RangerKeyStoreProvider(conf));
        RangerKeyStore         dbStore  = mock(RangerKeyStore.class);

        // Inject mocked dbStore
        Field dbStoreField = RangerKeyStoreProvider.class.getDeclaredField("dbStore");
        dbStoreField.setAccessible(true);
        dbStoreField.set(provider, dbStore);

        // Mock Metadata
        KeyProvider.Metadata metadata = mock(KeyProvider.Metadata.class);
        when(metadata.getAlgorithm()).thenReturn("AES");
        when(metadata.getBitLength()).thenReturn(128);
        when(metadata.getDescription()).thenReturn("test description");
        when(metadata.getVersions()).thenReturn(0); // No versions (only base key)
        when(metadata.getAttributes()).thenReturn(new HashMap<>());

        // Return mocked metadata
        doReturn(metadata).when(provider).getMetadata("testKey");

        // Simulate that base key alias exists
        doReturn(true).when(dbStore).engineContainsAlias("testKey");

        // Throw exception when trying to delete base key alias
        doThrow(new KeyStoreException("Delete failed")).when(dbStore).engineDeleteEntry("testKey");

        // Act & Assert
        IOException ex = assertThrows(IOException.class, () -> provider.deleteKey("testKey"));
        assertTrue(ex.getMessage().contains("Problem removing testKey from"));
    }

    @Test
    public void testCreateKey() throws Throwable {
        if (!UNRESTRICTED_POLICIES_INSTALLED) {
            return;
        }

        Path configDir = Paths.get("src/test/resources/kms");
        System.setProperty(KMSConfiguration.KMS_CONFIG_DIR, configDir.toFile().getAbsolutePath());

        Configuration          conf        = new Configuration();
        RangerKeyStoreProvider keyProvider = new RangerKeyStoreProvider(conf);

        // Create a key
        Options options = new Options(conf);
        options.setBitLength(256);
        options.setCipher("AES");
        KeyVersion keyVersion = keyProvider.createKey("newkey1", options);
        Assertions.assertEquals("newkey1", keyVersion.getName());
        Assertions.assertEquals(256 / 8, keyVersion.getMaterial().length);
        Assertions.assertEquals("newkey1@0", keyVersion.getVersionName());

        keyProvider.flush();

        // Validate the key exists
        List<String> keys = keyProvider.getKeys();
        Assertions.assertEquals(1, keys.size());
        Assertions.assertEquals("newkey1", keys.get(0));
    }

    @Test
    public void testRolloverKey() throws Throwable {
        if (!UNRESTRICTED_POLICIES_INSTALLED) {
            return;
        }

        Path configDir = Paths.get("src/test/resources/kms");
        System.setProperty(KMSConfiguration.KMS_CONFIG_DIR, configDir.toFile().getAbsolutePath());

        Configuration          conf        = new Configuration();
        RangerKeyStoreProvider keyProvider = new RangerKeyStoreProvider(conf);

        // Create a key
        Options options = new Options(conf);
        options.setBitLength(192);
        options.setCipher("AES");
        KeyVersion keyVersion = keyProvider.createKey("newkey1", options);
        Assertions.assertEquals("newkey1", keyVersion.getName());
        Assertions.assertEquals(192 / 8, keyVersion.getMaterial().length);
        Assertions.assertEquals("newkey1@0", keyVersion.getVersionName());

        keyProvider.flush();

        // Rollover a new key
        byte[] oldKey = keyVersion.getMaterial();
        keyVersion = keyProvider.rollNewVersion("newkey1");
        Assertions.assertEquals("newkey1", keyVersion.getName());
        Assertions.assertEquals(192 / 8, keyVersion.getMaterial().length);
        Assertions.assertEquals("newkey1@1", keyVersion.getVersionName());
        Assertions.assertFalse(Arrays.equals(oldKey, keyVersion.getMaterial()));

        keyProvider.deleteKey("newkey1");

        keyProvider.flush();
        Assertions.assertEquals(0, keyProvider.getKeys().size());
        try {
            keyProvider.deleteKey("newkey1");
            keyProvider.flush();
        } catch (IOException e) {
            // Ignore if key doesn't exist
        }
    }

    @Test
    public void testGetKeyVersion() throws Throwable {
        if (!UNRESTRICTED_POLICIES_INSTALLED) {
            return;
        }

        Path configDir = Paths.get("src/test/resources/kms");
        System.setProperty(KMSConfiguration.KMS_CONFIG_DIR, configDir.toFile().getAbsolutePath());

        Configuration          conf        = new Configuration();
        RangerKeyStoreProvider keyProvider = new RangerKeyStoreProvider(conf);

        // Create a key version
        Options options = new Options(conf);
        options.setBitLength(192);
        options.setCipher("AES");
        KeyVersion keyVersion = keyProvider.createKey("newkey1", options);

        Assertions.assertEquals("newkey1", keyVersion.getName());
        Assertions.assertEquals(192 / 8, keyVersion.getMaterial().length);
        Assertions.assertEquals("newkey1@0", keyVersion.getVersionName());

        keyProvider.flush();

        // Validate the key exists
        Assertions.assertEquals(1, keyProvider.getKeys().size());

        // Get key versions
        List<KeyVersion> keyVersions = keyProvider.getKeyVersions("newkey1");
        Assertions.assertEquals(1, keyVersions.size());

        KeyVersion kv = keyVersions.get(0);
        Assertions.assertEquals("newkey1", kv.getName());
        Assertions.assertEquals(192 / 8, kv.getMaterial().length);
        assertTrue(kv.getVersionName().startsWith("newkey1@"));

        keyProvider.flush();
        Assertions.assertNotEquals(0, keyProvider.getKeys().size());

        // Try to get key versions of a non-existent key
        try {
            List<KeyVersion> invalidVersions = keyProvider.getKeyVersions("newkey2");
            if (!invalidVersions.isEmpty()) {
                Assertions.fail("Unexpected key version found: " + invalidVersions.get(0).getName());
            }
        } catch (IOException ex) {
            // expected
        }
    }

    @Test
    public void testGetKeys() throws Throwable {
        if (!UNRESTRICTED_POLICIES_INSTALLED) {
            return;
        }

        Path configDir = Paths.get("src/test/resources/kms");
        System.setProperty(KMSConfiguration.KMS_CONFIG_DIR, configDir.toFile().getAbsolutePath());

        Configuration          conf        = new Configuration();
        RangerKeyStoreProvider keyProvider = new RangerKeyStoreProvider(conf);

        // Create a key version
        Options options = new Options(conf);
        options.setBitLength(192);
        options.setCipher("AES");
        KeyVersion keyVersion = keyProvider.createKey("newkey1", options);

        Assertions.assertEquals("newkey1", keyVersion.getName());
        Assertions.assertEquals(192 / 8, keyVersion.getMaterial().length);
        Assertions.assertEquals("newkey1@0", keyVersion.getVersionName());

        keyProvider.flush();

        List<String> getkeys = keyProvider.getKeys();
        Assertions.assertEquals(1, getkeys.size());
        Assertions.assertEquals("newkey1", getkeys.get(0));

        keyProvider.flush();
        Assertions.assertNotEquals(0, keyProvider.getKeys().size());
    }

    @Test
    public void testGetKeyVersionWithInvalidKey() throws Throwable {
        if (!UNRESTRICTED_POLICIES_INSTALLED) {
            return;
        }

        Path configDir = Paths.get("src/test/resources/kms");
        System.setProperty(KMSConfiguration.KMS_CONFIG_DIR, configDir.toFile().getAbsolutePath());

        Configuration          conf        = new Configuration();
        RangerKeyStoreProvider keyProvider = new RangerKeyStoreProvider(conf);

        // Try to get key versions of a non-existent key
        try {
            List<KeyVersion> invalidVersions = keyProvider.getKeyVersions("nonExistentKey");
            assertTrue(invalidVersions.isEmpty(), "Expected no key versions for non-existent key");
        } catch (IOException ex) {
            // expected
        }
    }

    @Test
    public void testGetKeyVersionWithInvalidVersion() throws Throwable {
        if (!UNRESTRICTED_POLICIES_INSTALLED) {
            return;
        }

        Path configDir = Paths.get("src/test/resources/kms");
        System.setProperty(KMSConfiguration.KMS_CONFIG_DIR, configDir.toFile().getAbsolutePath());

        Configuration          conf        = new Configuration();
        RangerKeyStoreProvider keyProvider = new RangerKeyStoreProvider(conf);

        // Create a key version
        Options options = new Options(conf);
        options.setBitLength(192);
        options.setCipher("AES");
        KeyVersion keyVersion = keyProvider.createKey("newkey1", options);

        Assertions.assertEquals("newkey1", keyVersion.getName());
        Assertions.assertEquals(192 / 8, keyVersion.getMaterial().length);
        Assertions.assertEquals("newkey1@0", keyVersion.getVersionName());

        keyProvider.flush();

        // Try to get an invalid version
        try {
            KeyVersion invalidVersion = keyProvider.getKeyVersion("newkey1@invalid");
            Assertions.assertNull(invalidVersion, "Expected null for invalid version");
        } catch (IOException ex) {
            // expected
        }
    }

    @Test
    public void testGetKeyVersions() throws Throwable {
        if (!UNRESTRICTED_POLICIES_INSTALLED) {
            return;
        }

        Path configDir = Paths.get("src/test/resources/kms");
        System.setProperty(KMSConfiguration.KMS_CONFIG_DIR, configDir.toFile().getAbsolutePath());

        Configuration          conf        = new Configuration();
        RangerKeyStoreProvider keyProvider = new RangerKeyStoreProvider(conf);

        // Create a key version
        Options options = new Options(conf);
        options.setBitLength(192);
        options.setCipher("AES");
        KeyVersion keyVersion = keyProvider.createKey("newkey1", options);

        Assertions.assertEquals("newkey1", keyVersion.getName());
        Assertions.assertEquals(192 / 8, keyVersion.getMaterial().length);
        Assertions.assertEquals("newkey1@0", keyVersion.getVersionName());

        keyProvider.flush();

        // Get key versions
        List<KeyVersion> keyVersions = keyProvider.getKeyVersions("newkey1");
        Assertions.assertEquals(1, keyVersions.size());

        KeyVersion kv = keyVersions.get(0);
        Assertions.assertEquals("newkey1", kv.getName());
        Assertions.assertEquals(192 / 8, kv.getMaterial().length);
        assertTrue(kv.getVersionName().startsWith("newkey1@"));
    }

    @Test
    public void testGetMetadata() throws Throwable {
        if (!UNRESTRICTED_POLICIES_INSTALLED) {
            return;
        }

        Path configDir = Paths.get("src/test/resources/kms");
        System.setProperty(KMSConfiguration.KMS_CONFIG_DIR, configDir.toFile().getAbsolutePath());

        Configuration          conf        = new Configuration();
        RangerKeyStoreProvider keyProvider = new RangerKeyStoreProvider(conf);

        // Create a key version
        Options options = new Options(conf);
        options.setBitLength(192);
        options.setCipher("AES");
        KeyVersion keyVersion = keyProvider.createKey("newkey1", options);
        Assertions.assertEquals("newkey1", keyVersion.getName());
        Assertions.assertEquals(192 / 8, keyVersion.getMaterial().length);
        Assertions.assertEquals("newkey1@0", keyVersion.getVersionName());

        keyProvider.flush();

        // Get metadata
        String metadata = String.valueOf(keyProvider.getMetadata("newkey1"));
        assertNotNull(metadata, "Metadata should not be null");
        assertTrue(metadata.contains("192"), "Metadata should contain key bit length");
        assertTrue(metadata.contains("AES"), "Metadata should contain key cipher");
    }

    @Test
    public void testGetKeyVersionWithInvalidKeyName() throws Throwable {
        if (!UNRESTRICTED_POLICIES_INSTALLED) {
            return;
        }

        Path configDir = Paths.get("src/test/resources/kms");
        System.setProperty(KMSConfiguration.KMS_CONFIG_DIR, configDir.toFile().getAbsolutePath());

        Configuration          conf        = new Configuration();
        RangerKeyStoreProvider keyProvider = new RangerKeyStoreProvider(conf);

        // Try to get key versions of a non-existent key
        try {
            KeyVersion invalidVersion = keyProvider.getKeyVersion("nonExistentKey@0");
            Assertions.assertNull(invalidVersion, "Expected null for non-existent key version");
        } catch (IOException ex) {
            // expected
        }
    }

    @Test
    public void testFlush() throws Throwable {
        if (!UNRESTRICTED_POLICIES_INSTALLED) {
            return;
        }

        Path configDir = Paths.get("src/test/resources/kms");
        System.setProperty(KMSConfiguration.KMS_CONFIG_DIR, configDir.toFile().getAbsolutePath());

        Configuration          conf        = new Configuration();
        RangerKeyStoreProvider keyProvider = new RangerKeyStoreProvider(conf);

        // Create a key version
        Options options = new Options(conf);
        options.setBitLength(192);
        options.setCipher("AES");
        KeyVersion keyVersion = keyProvider.createKey("newkey1", options);

        Assertions.assertEquals("newkey1", keyVersion.getName());
        Assertions.assertEquals(192 / 8, keyVersion.getMaterial().length);
        Assertions.assertEquals("newkey1@0", keyVersion.getVersionName());

        // Flush the provider
        keyProvider.flush();

        // Validate that the key is still present after flush
        List<KeyVersion> keyVersions = keyProvider.getKeyVersions("newkey1");
        Assertions.assertEquals(1, keyVersions.size());
    }

    @Test
    public void testGetConfiguration() {
        if (!UNRESTRICTED_POLICIES_INSTALLED) {
            return;
        }

        String originalConfDir = System.getProperty(KMSConfiguration.KMS_CONFIG_DIR);

        try {
            System.setProperty(KMSConfiguration.KMS_CONFIG_DIR, "relative/path");

            Configuration conf = new Configuration();

            RuntimeException ex = assertThrows(RuntimeException.class, () -> {
                new RangerKeyStoreProvider(conf); // Should internally call getConfiguration()
            });

            assertTrue(ex.getMessage().contains("must be an absolute path"));
        } finally {
            if (originalConfDir != null) {
                System.setProperty(KMSConfiguration.KMS_CONFIG_DIR, originalConfDir);
            } else {
                System.clearProperty(KMSConfiguration.KMS_CONFIG_DIR);
            }
        }
    }

    @Test
    public void testGetKeyVersionWithInvalidVersionName() throws Throwable {
        if (!UNRESTRICTED_POLICIES_INSTALLED) {
            return;
        }

        Path configDir = Paths.get("src/test/resources/kms");
        System.setProperty(KMSConfiguration.KMS_CONFIG_DIR, configDir.toFile().getAbsolutePath());

        Configuration          conf        = new Configuration();
        RangerKeyStoreProvider keyProvider = new RangerKeyStoreProvider(conf);

        // Try to get key version with an invalid version name
        try {
            KeyVersion invalidVersion = keyProvider.getKeyVersion("newkey1@invalid");
            Assertions.assertNull(invalidVersion, "Expected null for invalid version name");
        } catch (IOException ex) {
            // expected
        }
    }

    @Test
    public void testGetDBKSConf() throws Throwable {
        if (!UNRESTRICTED_POLICIES_INSTALLED) {
            return;
        }

        Path configDir = Paths.get("src/test/resources/kms");
        System.setProperty(KMSConfiguration.KMS_CONFIG_DIR, configDir.toFile().getAbsolutePath());

        Configuration          conf        = new Configuration();
        RangerKeyStoreProvider keyProvider = new RangerKeyStoreProvider(conf);

        // Get the DB configuration
        Configuration dbConf = RangerKeyStoreProvider.getDBKSConf();
        assertNotNull(dbConf, "DB configuration should not be null");
    }

    @Test
    public void testRollNewVersion_ThrowsWhenKeyNotFound() throws Throwable {
        Path configDir = Paths.get("src/test/resources/kms");
        System.setProperty(KMSConfiguration.KMS_CONFIG_DIR, configDir.toFile().getAbsolutePath());
        Configuration          conf     = new Configuration();
        RangerKeyStoreProvider provider = new RangerKeyStoreProvider(conf);

        byte[] dummyMaterial = new byte[16]; // 128-bit material

        IOException exception = assertThrows(IOException.class, () ->
                provider.rollNewVersion("nonExistingKey", dummyMaterial));

        assertTrue(exception.getMessage().contains("Key nonExistingKey not found"));
    }

    @Test
    public void testRollNewVersion_ThrowsWhenKeyLengthMismatch() throws Throwable {
        Path configDir = Paths.get("src/test/resources/kms");
        System.setProperty(KMSConfiguration.KMS_CONFIG_DIR, configDir.toFile().getAbsolutePath());

        Configuration          conf     = new Configuration();
        RangerKeyStoreProvider provider = new RangerKeyStoreProvider(conf);

        Options options = new Options(conf);
        options.setBitLength(128);
        options.setCipher("AES");
        provider.createKey("testKeyMismatch", options);
        provider.flush();

        // Use 192-bit material
        byte[] wrongMaterial = new byte[24];

        IOException exception = assertThrows(IOException.class, () ->
                provider.rollNewVersion("testKeyMismatch", wrongMaterial));

        assertTrue(exception.getMessage().contains("Wrong key length"));
    }

    @Test
    public void testDeleteKey_MetadataIsNull() throws Throwable {
        Path configDir = Paths.get("src/test/resources/kms");
        System.setProperty(KMSConfiguration.KMS_CONFIG_DIR, configDir.toFile().getAbsolutePath());

        Configuration          conf         = new Configuration();
        RangerKeyStoreProvider realProvider = new RangerKeyStoreProvider(conf);

        // Spy on the real provider to override getMetadata
        RangerKeyStoreProvider provider = spy(realProvider);

        // Simulate missing metadata
        doReturn(null).when(provider).getMetadata("testKey");

        // Act & Assert
        IOException ex = assertThrows(IOException.class, () -> provider.deleteKey("testKey"));

        // This now matches the real message thrown by deleteKey()
        assertTrue(ex.getMessage().contains("Key testKey does not exist"));
    }

    @Test
    public void testGetKeyVersion_DecryptKeyThrowsRuntimeException() throws Throwable {
        Configuration          conf     = new Configuration();
        RangerKeyStore         dbStore  = mock(RangerKeyStore.class);
        RangerKeyStoreProvider provider = spy(new RangerKeyStoreProvider(conf));

        // Inject dbStore
        Field dbStoreField = RangerKeyStoreProvider.class.getDeclaredField("dbStore");
        dbStoreField.setAccessible(true);
        dbStoreField.set(provider, dbStore);

        // Enable keyVault mode
        Field keyVaultField = RangerKeyStoreProvider.class.getDeclaredField("keyVaultEnabled");
        keyVaultField.setAccessible(true);
        keyVaultField.set(provider, true);

        // Setup mocks: alias exists, but decryption fails
        doReturn(true).when(dbStore).engineContainsAlias("testKey");
        doThrow(new RuntimeException("decryption failure")).when(dbStore).engineGetDecryptedZoneKeyByte("testKey");

        RuntimeException ex = assertThrows(RuntimeException.class, () -> provider.getKeyVersion("testKey"));
        assertTrue(ex.getMessage().contains("Error while getting decrypted key."));
        assertTrue(ex.getMessage().contains("decryption failure"));
    }

    @Test
    public void testGetKeyVersion_NoSuchAlgorithmException() throws Throwable {
        Configuration          conf     = new Configuration();
        RangerKeyStore         dbStore  = mock(RangerKeyStore.class);
        RangerKeyStoreProvider provider = spy(new RangerKeyStoreProvider(conf));

        // Inject dbStore
        Field dbStoreField = RangerKeyStoreProvider.class.getDeclaredField("dbStore");
        dbStoreField.setAccessible(true);
        dbStoreField.set(provider, dbStore);

        // Setup: alias exists, key fetch throws NoSuchAlgorithmException
        doReturn(true).when(dbStore).engineContainsAlias("testKey");
        doThrow(new NoSuchAlgorithmException()).when(dbStore).engineGetKey(eq("testKey"), any());

        IOException ex = assertThrows(IOException.class, () -> provider.getKeyVersion("testKey"));

        assertTrue(ex.getMessage().contains("Can't get algorithm for key"));

        // Setup: alias exists, key fetch throws UnrecoverableKeyException
        doReturn(true).when(dbStore).engineContainsAlias("testKey");
        doThrow(new UnrecoverableKeyException()).when(dbStore).engineGetKey(eq("testKey"), any());

        IOException ex1 = assertThrows(IOException.class, () -> provider.getKeyVersion("testKey"));

        assertTrue(ex1.getMessage().contains("Can't recover key "));
    }

    @Test
    public void testGetMetadata_GenericException() throws Throwable {
        Configuration          conf     = new Configuration();
        RangerKeyStoreProvider provider = spy(new RangerKeyStoreProvider(conf));
        RangerKeyStore         dbStore  = mock(RangerKeyStore.class);

        // Inject mocked dbStore
        Field dbStoreField = RangerKeyStoreProvider.class.getDeclaredField("dbStore");
        dbStoreField.setAccessible(true);
        dbStoreField.set(provider, dbStore);

        // Simulate exception during dbStore.engineContainsAlias for RuntimeException
        when(dbStore.engineContainsAlias("testKey")).thenThrow(new RuntimeException("DB failure"));

        IOException ex = assertThrows(IOException.class, () -> provider.getMetadata("testKey"));

        assertTrue(ex.getMessage().contains("Please try again"));
        assertTrue(ex.getCause().getMessage().contains("DB failure"));
    }

    @Test
    public void testGetConfiguration1() throws Throwable {
        if (!UNRESTRICTED_POLICIES_INSTALLED) {
            return;
        }

        Path configDir = Paths.get("src/test/resources/kms");
        System.setProperty(KMSConfiguration.KMS_CONFIG_DIR, configDir.toFile().getAbsolutePath());

        Configuration          conf        = new Configuration();
        RangerKeyStoreProvider keyProvider = new RangerKeyStoreProvider(conf);

        // Get the configuration
        Configuration keyProviderConf = keyProvider.getConf();
        assertNotNull(keyProviderConf, "Configuration should not be null");
    }

    @Test
    public void testSaveKey_ThrowsIOException() throws Throwable {
        Configuration          conf     = new Configuration();
        RangerKeyStoreProvider provider = spy(new RangerKeyStoreProvider(conf));
        RangerKeyStore         dbStore  = mock(RangerKeyStore.class);

        // Inject mocked dbStore
        Field dbStoreField = RangerKeyStoreProvider.class.getDeclaredField("dbStore");
        dbStoreField.setAccessible(true);
        dbStoreField.set(provider, dbStore);

        // Set keyVaultEnabled = false
        Field keyVaultField = RangerKeyStoreProvider.class.getDeclaredField("keyVaultEnabled");
        keyVaultField.setAccessible(true);
        keyVaultField.set(provider, false);

        // Mock Metadata with required getters
        KeyProvider.Metadata metadata = mock(KeyProvider.Metadata.class);
        when(metadata.getAlgorithm()).thenReturn("AES");
        when(metadata.getBitLength()).thenReturn(128);
        when(metadata.getDescription()).thenReturn("test description");
        when(metadata.getVersions()).thenReturn(1);
        when(metadata.getAttributes()).thenReturn(new HashMap<>());

        // Mock exception on addKeyEntry
        doThrow(new RuntimeException("decryption failure")).when(dbStore).addKeyEntry(
                eq("testKey"),
                any(),
                any(),
                eq("AES"),
                eq(128),
                eq("test description"),
                eq(1),
                any());

        Method saveKeyMethod = RangerKeyStoreProvider.class.getDeclaredMethod("saveKey", String.class, KeyProvider.Metadata.class);
        saveKeyMethod.setAccessible(true);

        try {
            saveKeyMethod.invoke(provider, "testKey", metadata);
            fail("Expected IOException to be thrown");
        } catch (InvocationTargetException e) {
            Throwable cause = e.getCause();
            assertInstanceOf(IOException.class, cause, "Cause should be IOException");
            assertInstanceOf(RuntimeException.class, cause.getCause(), "IOException cause should be RuntimeException");
            assertTrue(cause.getCause().getMessage().contains("decryption failure"));
        }
    }

    @Test
    public void testGetKeyVersion_KeyVaultTrue_SuccessPath() throws Throwable {
        Configuration          conf     = new Configuration();
        RangerKeyStoreProvider provider = spy(new RangerKeyStoreProvider(conf));
        RangerKeyStore         dbStore  = mock(RangerKeyStore.class);

        // Inject dbStore
        Field dbStoreField = RangerKeyStoreProvider.class.getDeclaredField("dbStore");
        dbStoreField.setAccessible(true);
        dbStoreField.set(provider, dbStore);

        // Set keyVaultEnabled = true
        Field keyVaultField = RangerKeyStoreProvider.class.getDeclaredField("keyVaultEnabled");
        keyVaultField.setAccessible(true);
        keyVaultField.set(provider, true);

        String versionedKey = "testKey@0";

        // Simulate engineContainsAlias returns false first, then true
        when(dbStore.engineContainsAlias(versionedKey)).thenReturn(false).thenReturn(true);

        // Simulate engineLoad
        doNothing().when(dbStore).engineLoad(isNull(), any());

        // Simulate decrypted key return
        byte[] decryptedKey = new byte[] {0x01, 0x02, 0x03};
        when(dbStore.engineGetDecryptedZoneKeyByte(versionedKey)).thenReturn(decryptedKey);

        // Act
        KeyVersion result = provider.getKeyVersion(versionedKey);

        // Assert
        assertNotNull(result);
        assertEquals(versionedKey, result.getVersionName());
        assertEquals("testKey", result.getName());
        assertArrayEquals(decryptedKey, result.getMaterial());
    }

    @Test
    public void testGenerateAndGetMasterKey_generateMasterKeyThrows() throws Throwable {
        Configuration          conf              = new Configuration();
        RangerKeyStoreProvider provider          = spy(new RangerKeyStoreProvider(conf));
        RangerKMSMKI           masterKeyProvider = mock(RangerKMSMKI.class);

        // simulate generateMasterKey throwing an exception
        doThrow(new RuntimeException("Simulated failure in generateMasterKey")).when(masterKeyProvider).generateMasterKey("abc123");

        Method method = RangerKeyStoreProvider.class.getDeclaredMethod("generateAndGetMasterKey", RangerKMSMKI.class, String.class);
        method.setAccessible(true);

        RuntimeException ex = assertThrows(RuntimeException.class, () -> {
            try {
                method.invoke(provider, masterKeyProvider, "abc123");
            } catch (InvocationTargetException e) {
                throw e.getCause();
            }
        });

        assertTrue(ex.getMessage().contains("Error while generating Ranger Master key"));
    }

    @Test
    void testFlush_EngineStoreThrowsIOException() throws Throwable {
        Configuration  conf    = new Configuration();
        RangerKeyStore dbStore = mock(RangerKeyStore.class);
        doThrow(new IOException("Flush failed")).when(dbStore).engineStore(any(), any());

        RangerKeyStoreProvider provider = spy(new RangerKeyStoreProvider(conf));

        // Inject mocks and changed = true
        Field dbStoreField = RangerKeyStoreProvider.class.getDeclaredField("dbStore");
        dbStoreField.setAccessible(true);
        dbStoreField.set(provider, dbStore);

        Field changedField = RangerKeyStoreProvider.class.getDeclaredField("changed");
        changedField.setAccessible(true);
        changedField.set(provider, true);

        IOException ex = assertThrows(IOException.class, provider::flush);
        assertTrue(ex.getMessage().contains("Flush failed"));

        verify(dbStore, times(1)).engineStore(any(), any());
    }

    @Test
    void testFlush_EngineStoreThrowsNoSuchAlgorithmException() throws Throwable {
        Configuration  conf    = new Configuration();
        RangerKeyStore dbStore = mock(RangerKeyStore.class);
        doThrow(new NoSuchAlgorithmException()).when(dbStore).engineStore(any(), any());

        RangerKeyStoreProvider provider = spy(new RangerKeyStoreProvider(conf));

        Field dbStoreField = RangerKeyStoreProvider.class.getDeclaredField("dbStore");
        dbStoreField.setAccessible(true);
        dbStoreField.set(provider, dbStore);

        Field changedField = RangerKeyStoreProvider.class.getDeclaredField("changed");
        changedField.setAccessible(true);
        changedField.set(provider, true);

        IOException ex = assertThrows(IOException.class, provider::flush);
        assertTrue(ex.getMessage().contains("No such algorithm storing key"));

        verify(dbStore, times(1)).engineStore(any(), any());
    }

    @Test
    void testFlush_EngineStoreThrowsCertificateException() throws Throwable {
        Configuration  conf    = new Configuration();
        RangerKeyStore dbStore = mock(RangerKeyStore.class);
        doThrow(new CertificateException()).when(dbStore).engineStore(any(), any());

        RangerKeyStoreProvider provider = spy(new RangerKeyStoreProvider(conf));

        Field dbStoreField = RangerKeyStoreProvider.class.getDeclaredField("dbStore");
        dbStoreField.setAccessible(true);
        dbStoreField.set(provider, dbStore);

        Field changedField = RangerKeyStoreProvider.class.getDeclaredField("changed");
        changedField.setAccessible(true);
        changedField.set(provider, true);

        IOException ex = assertThrows(IOException.class, provider::flush);
        assertTrue(ex.getMessage().contains("Certificate exception storing key"));

        verify(dbStore, times(1)).engineStore(any(), any());
    }

    @Test
    void testDeleteKey_ShouldThrowIOException() throws Throwable {
        Configuration          conf     = new Configuration();
        RangerKeyStore         dbStore  = mock(RangerKeyStore.class);
        RangerKeyStoreProvider provider = spy(new RangerKeyStoreProvider(conf));

        // Inject mocked dbStore
        Field dbStoreField = RangerKeyStoreProvider.class.getDeclaredField("dbStore");
        dbStoreField.setAccessible(true);
        dbStoreField.set(provider, dbStore);

        // Mock Metadata object with versions = 1
        KeyProvider.Metadata metadata = mock(KeyProvider.Metadata.class);
        when(metadata.getVersions()).thenReturn(1);

        // Stub getMetadata to return mocked metadata
        doReturn(metadata).when(provider).getMetadata("testKey");

        // Stub dbStore responses for key existence
        doReturn(true).when(dbStore).engineContainsAlias("testKey@0");
        doReturn(true).when(dbStore).engineContainsAlias("testKey");

        // Simulate KeyStoreException on deleting the version key
        doThrow(new KeyStoreException("forced exception")).when(dbStore).engineDeleteEntry("testKey@0");

        // Expect IOException because KeyStoreException is caught and wrapped
        IOException ex = assertThrows(IOException.class, () -> provider.deleteKey("testKey"));
        assertTrue(ex.getMessage().contains("Problem removing"));

        // Verify interactions with dbStore mocks
        verify(dbStore).engineContainsAlias("testKey@0");
        verify(dbStore).engineDeleteEntry("testKey@0");
    }

    @Test
    void testCreateKey_ShouldThrowIOException_WhenKeyAlreadyExists() throws Throwable {
        Configuration          conf     = new Configuration();
        RangerKeyStore         dbStore  = mock(RangerKeyStore.class);
        RangerKeyStoreProvider provider = spy(new RangerKeyStoreProvider(conf));

        // Inject mocked dbStore
        Field dbStoreField = RangerKeyStoreProvider.class.getDeclaredField("dbStore");
        dbStoreField.setAccessible(true);
        dbStoreField.set(provider, dbStore);

        String keyName  = "existingKey";
        byte[] material = new byte[16]; // 128 bits
        KeyProvider.Options options = new KeyProvider.Options(conf)
                .setCipher("AES")
                .setBitLength(128);

        // Simulate that key already exists
        when(dbStore.engineContainsAlias(keyName)).thenReturn(true);

        IOException ex = assertThrows(IOException.class, () -> provider.createKey(keyName, material, options));
        assertTrue(ex.getMessage().contains("Key " + keyName + " already exists"));
    }

    @Test
    void testCreateKey_ShouldThrowIOException_WhenKeyLengthIncorrect() throws Throwable {
        Configuration          conf     = new Configuration();
        RangerKeyStore         dbStore  = mock(RangerKeyStore.class);
        RangerKeyStoreProvider provider = spy(new RangerKeyStoreProvider(conf));

        // Inject mocked dbStore
        Field dbStoreField = RangerKeyStoreProvider.class.getDeclaredField("dbStore");
        dbStoreField.setAccessible(true);
        dbStoreField.set(provider, dbStore);

        String keyName  = "newKey";
        byte[] material = new byte[10]; // 80 bits
        KeyProvider.Options options = new KeyProvider.Options(conf)
                .setCipher("AES")
                .setBitLength(128); // But expects 128 bits

        // Simulate key does not exist
        when(dbStore.engineContainsAlias(keyName)).thenReturn(false);

        IOException ex = assertThrows(IOException.class, () -> provider.createKey(keyName, material, options));
        assertTrue(ex.getMessage().contains("Wrong key length. Required 128, but got 80"));
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
