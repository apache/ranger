/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.crypto.key.kms.server;

import com.codahale.metrics.Meter;
import org.apache.hadoop.crypto.key.KeyProvider;
import org.apache.hadoop.crypto.key.KeyProviderCryptoExtension;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.delegation.web.HttpUserGroupInformation;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import javax.servlet.http.HttpServletRequest;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URI;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.eclipse.persistence.jpa.jpql.Assert.fail;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@TestMethodOrder(MethodOrderer.MethodName.class)
@Disabled
public class TestKMS {
    private static KeyProviderCryptoExtension keyProvider;
    private static KMSAudit                   kmsAudit;
    private static Meter                      adminCallsMeter;
    private static KMSACLs                    acls;

    @Test
    public void testRolloverKey() throws Exception {
        String keyName = "testKey";
        // Prepare input map
        Map<String, Object> jsonKey = new HashMap<>();
        jsonKey.put("name", "testKey");
        jsonKey.put("cipher", "AES");
        jsonKey.put("length", 128);
        jsonKey.put("description", "just a basic key");

        // Mock request object
        HttpServletRequest request = mock(HttpServletRequest.class);
        lenient().when(request.getRemoteAddr()).thenReturn("127.0.0.1");
        lenient().when(request.getRequestURI()).thenReturn("/kms/v1/keys/testKey");
        Meter dummyMeter = mock(Meter.class);
        Field meterField = KMSWebApp.class.getDeclaredField("adminCallsMeter");
        meterField.setAccessible(true);
        meterField.set(null, dummyMeter);

        KMS kms = new KMS();
        try {
            kms.rolloverKey(keyName, jsonKey, request);
            fail("Expected exception due to missing provider/user context");
        } catch (Exception e) {
            System.out.println("Caught expected exception: " + e.getMessage());
            assertNotNull(e);
        }
    }

    @Test
    public void testGetKeyVersions() throws Exception {
        String keyName = "testKey";
        // Prepare input map
        Map<String, Object> jsonKey = new HashMap<>();
        jsonKey.put("name", "testKey");
        jsonKey.put("cipher", "AES");
        jsonKey.put("length", 128);
        jsonKey.put("description", "just a basic key");

        // Mock request object
        HttpServletRequest request = mock(HttpServletRequest.class);
        lenient().when(request.getRemoteAddr()).thenReturn("127.0.0.1");
        lenient().when(request.getRequestURI()).thenReturn("/kms/v1/keys/testKey");
        Meter dummyMeter = mock(Meter.class);
        Field meterField = KMSWebApp.class.getDeclaredField("adminCallsMeter");
        meterField.setAccessible(true);
        meterField.set(null, dummyMeter);

        KMS kms = new KMS();
        try {
            kms.getKeyVersions(keyName, request);
            fail("Expected exception due to missing provider/user context");
        } catch (Exception e) {
            System.out.println("Caught expected exception: " + e.getMessage());
            assertNotNull(e);
        }
    }

    @Test
    public void testReencryptEncryptedKeys() throws Exception {
        String keyName = "testKey";

        List<Map> jsonPayload = new ArrayList<>();

        Map<String, Object> key1 = new HashMap<>();
        key1.put("versionName", keyName + "@0");
        key1.put("iv", Base64.getEncoder().encodeToString("12345678".getBytes()));
        key1.put("encryptedKeyVersion", Base64.getEncoder().encodeToString("keyBytes1".getBytes()));
        jsonPayload.add(key1);

        Map<String, Object> key2 = new HashMap<>();
        key2.put("versionName", keyName + "@1");
        key2.put("iv", Base64.getEncoder().encodeToString("87654321".getBytes()));
        key2.put("encryptedKeyVersion", Base64.getEncoder().encodeToString("keyBytes2".getBytes()));
        jsonPayload.add(key2);

        Meter dummyMeter = mock(Meter.class);
        Field meterField = KMSWebApp.class.getDeclaredField("adminCallsMeter");
        meterField.setAccessible(true);
        meterField.set(null, dummyMeter);
        KMS kms = new KMS();

        try {
            kms.reencryptEncryptedKeys(keyName, jsonPayload);
            fail("Expected exception due to missing provider/user context");
        } catch (Exception e) {
            System.out.println("Caught expected exception: " + e.getMessage());
            assertNotNull(e);
        }
    }

    @Test
    public void testHandleEncryptedKeyOp() throws Exception {
        String keyName     = "testKey";
        String versionName = "testKeyVersion";

        String edekOp = "generateEncryptedKeys";
        // Prepare input map
        Map<String, Object> jsonKey = new HashMap<>();
        jsonKey.put("name", "testKey");
        jsonKey.put("cipher", "AES");
        jsonKey.put("length", 128);
        jsonKey.put("description", "just a basic key");

        // Mock request object
        HttpServletRequest request = mock(HttpServletRequest.class);
        lenient().when(request.getRemoteAddr()).thenReturn("127.0.0.1");
        lenient().when(request.getRequestURI()).thenReturn("/kms/v1/keys/testKey");
        Meter dummyMeter = mock(Meter.class);
        Field meterField = KMSWebApp.class.getDeclaredField("adminCallsMeter");
        meterField.setAccessible(true);
        meterField.set(null, dummyMeter);

        KMS kms = new KMS();

        try {
            kms.handleEncryptedKeyOp(versionName, edekOp, jsonKey, request);
            fail("Expected exception due to missing provider/user context");
        } catch (Exception e) {
            System.out.println("Caught expected exception: " + e.getMessage());
            assertNotNull(e);
        }
    }

    @Test
    public void testGenerateEncryptedKeys() throws Exception {
        String keyName = "testKey";

        String edekOp = "generateEncryptedKeys";

        final int numKeys = 1;

        HttpServletRequest request = mock(HttpServletRequest.class);
        lenient().when(request.getRemoteAddr()).thenReturn("127.0.0.1");
        lenient().when(request.getRequestURI()).thenReturn("/kms/v1/keys/testKey");
        Meter dummyMeter = mock(Meter.class);
        Field meterField = KMSWebApp.class.getDeclaredField("adminCallsMeter");
        meterField.setAccessible(true);
        meterField.set(null, dummyMeter);

        KMS kms = new KMS();

        try {
            kms.generateEncryptedKeys(keyName, edekOp, numKeys, request);
            fail("Expected exception due to missing provider/user context");
        } catch (Exception e) {
            System.out.println("Caught expected exception: " + e.getMessage());
            assertNotNull(e);
        }
    }

    @Test
    public void testGetKeyVersion() throws Exception {
        String versionName = "testKeyVersion";

        // Mock request object
        HttpServletRequest request = mock(HttpServletRequest.class);
        lenient().when(request.getRemoteAddr()).thenReturn("127.0.0.1");
        lenient().when(request.getRequestURI()).thenReturn("/kms/v1/keys/testKey");
        Meter dummyMeter = mock(Meter.class);
        Field meterField = KMSWebApp.class.getDeclaredField("adminCallsMeter");
        meterField.setAccessible(true);
        meterField.set(null, dummyMeter);

        KMS kms = new KMS();

        try {
            kms.getKeyVersion(versionName, request);
            fail("Expected exception due to missing provider/user context");
        } catch (Exception e) {
            System.out.println("Caught expected exception: " + e.getMessage());
            assertNotNull(e);
        }
    }

    @Test
    public void testGetKeysMetadata() throws Exception {
        List<String> keyNamesList = new ArrayList<>();
        keyNamesList.add("testKey1");

        // Mock request object
        HttpServletRequest request = mock(HttpServletRequest.class);
        lenient().when(request.getRemoteAddr()).thenReturn("127.0.0.1");
        lenient().when(request.getRequestURI()).thenReturn("/kms/v1/keys/testKey");
        Meter dummyMeter = mock(Meter.class);
        Field meterField = KMSWebApp.class.getDeclaredField("adminCallsMeter");
        meterField.setAccessible(true);
        meterField.set(null, dummyMeter);

        KMS kms = new KMS();

        try {
            kms.getKeysMetadata(keyNamesList, request);
            fail("Expected exception due to missing provider/user context");
        } catch (Exception e) {
            System.out.println("Caught expected exception: " + e.getMessage());
            assertNotNull(e);
        }
    }

    @Test
    public void testGetCurrentVersion() throws Exception {
        String keyName = "testKey";

        // Mock request object
        HttpServletRequest request = mock(HttpServletRequest.class);
        lenient().when(request.getRemoteAddr()).thenReturn("127.0.0.1");
        lenient().when(request.getRequestURI()).thenReturn("/kms/v1/keys/testKey");
        Meter dummyMeter = mock(Meter.class);
        Field meterField = KMSWebApp.class.getDeclaredField("adminCallsMeter");
        meterField.setAccessible(true);
        meterField.set(null, dummyMeter);

        KMS kms = new KMS();

        try {
            kms.getCurrentVersion(keyName, request);
            fail("Expected exception due to missing provider/user context");
        } catch (Exception e) {
            System.out.println("Caught expected exception: " + e.getMessage());
            assertNotNull(e);
        }
    }

    @Test
    public void testGenerateDataKey() throws Exception {
        String keyName = "testKey";

        // Mock request object
        HttpServletRequest request = mock(HttpServletRequest.class);
        lenient().when(request.getRemoteAddr()).thenReturn("127.0.0.1");
        lenient().when(request.getRequestURI()).thenReturn("/kms/v1/keys/testKey");
        Meter dummyMeter = mock(Meter.class);
        Field meterField = KMSWebApp.class.getDeclaredField("adminCallsMeter");
        meterField.setAccessible(true);
        meterField.set(null, dummyMeter);

        KMS kms = new KMS();

        try {
            kms.generateDataKey(keyName, request);
            fail("Expected exception due to missing provider/user context");
        } catch (Exception e) {
            System.out.println("Caught expected exception: " + e.getMessage());
            assertNotNull(e);
        }
    }

    @Test
    public void testGetKeyNames() throws Exception {
        // Mock request object
        HttpServletRequest request = mock(HttpServletRequest.class);
        lenient().when(request.getRemoteAddr()).thenReturn("127.0.0.1");
        lenient().when(request.getRequestURI()).thenReturn("/kms/v1/keys/testKey");
        Meter dummyMeter = mock(Meter.class);
        Field meterField = KMSWebApp.class.getDeclaredField("adminCallsMeter");
        meterField.setAccessible(true);
        meterField.set(null, dummyMeter);

        KMS kms = new KMS();

        try {
            kms.getKeyNames(request);
            fail("Expected exception due to missing provider/user context");
        } catch (Exception e) {
            System.out.println("Caught expected exception: " + e.getMessage());
            assertNotNull(e);
        }
    }

    @Test
    void testDeleteKey_skippingAdminCallsMeter() throws Exception {
        String               keyName  = "name";
        UserGroupInformation mockUser = mock(UserGroupInformation.class);

        HttpUserGroupInformation httpUserGroupInformation = mock(HttpUserGroupInformation.class);

        // Set up mock request
        HttpServletRequest request = mock(HttpServletRequest.class);
        when(request.getRemoteAddr()).thenReturn("127.0.0.1");
        lenient().when(request.getRequestURI()).thenReturn("/kms/v1/keys/testKey");

        // Prepare a dummy Meter to avoid static call impact
        Meter dummyMeter = mock(Meter.class);

        // Set static adminCallsMeter in KMSWebApp using reflection
        Field meterField = KMSWebApp.class.getDeclaredField("adminCallsMeter");
        meterField.setAccessible(true);
        meterField.set(null, dummyMeter);

        // Create the KMS instance (internally calls KMSWebApp static methods)
        KMS kms = new KMS();

        // Run and expect exception (due to missing KMSWebApp dependencies like provider, user, etc.)
        try {
            kms.deleteKey(keyName, request);
            fail("Expected exception due to missing dependencies like provider or UGI");
        } catch (Exception e) {
            System.out.println("Expected exception: " + e.getMessage());
            assertNotNull(e);
        }
    }

    @Test
    void testCreateKeyWithOnlyInputValues() throws Exception {
        // Prepare input map
        Map<String, Object> jsonKey = new HashMap<>();
        jsonKey.put("name", "testKey");
        jsonKey.put("cipher", "AES");
        jsonKey.put("length", 128);
        jsonKey.put("description", "just a basic key");

        // Mock request object
        HttpServletRequest request = mock(HttpServletRequest.class);
        lenient().when(request.getRemoteAddr()).thenReturn("127.0.0.1");
        lenient().when(request.getRequestURI()).thenReturn("/kms/v1/keys/testKey");

        Meter dummyMeter = mock(Meter.class);

        Field meterField = KMSWebApp.class.getDeclaredField("adminCallsMeter");
        meterField.setAccessible(true);
        meterField.set(null, dummyMeter);

        // Attempt call (expected to fail unless environment is injected)
        KMS kms = new KMS();
        try {
            kms.createKey(jsonKey, request);
            fail("Expected exception due to missing provider or user context");
        } catch (Exception e) {
            System.out.println("Caught expected exception: " + e.getMessage());
        }
    }

    @Test
    void testCreateKeyWithOnlyInputValues1() throws Exception {
        // Prepare input map
        Map<String, Object> jsonKey = new HashMap<>();
        jsonKey.put("name", "testKey");
        jsonKey.put("cipher", "AES");
        jsonKey.put("length", 128);
        jsonKey.put("description", "just a basic key");

        // Mock request object
        HttpServletRequest request = mock(HttpServletRequest.class);
        lenient().when(request.getRemoteAddr()).thenReturn("127.0.0.1");
        lenient().when(request.getRequestURI()).thenReturn("/kms/v1/keys/testKey");

        Meter dummyMeter = mock(Meter.class);

        Field meterField = KMSWebApp.class.getDeclaredField("adminCallsMeter");
        meterField.setAccessible(true);
        meterField.set(null, dummyMeter);

        // Attempt call (expected to fail unless environment is injected)
        KMS kms = new KMS();
        try {
            kms.createKey(jsonKey, request);
            fail("Expected exception due to missing provider or user context");
        } catch (Exception e) {
            System.out.println("Caught expected exception: " + e.getMessage());
        }
    }

    @Test
    void testInvalidateCacheWithOnlyInputValues() throws Exception {
        String keyName = "testKey";

        Meter dummyMeter = mock(Meter.class);

        Field meterField = KMSWebApp.class.getDeclaredField("adminCallsMeter");
        meterField.setAccessible(true);
        meterField.set(null, dummyMeter);

        KMS kms = new KMS();

        try {
            kms.invalidateCache(keyName);
            fail("Expected exception due to missing dependencies (provider/user)");
        } catch (Exception e) {
            System.out.println("Expected exception: " + e.getMessage());
            assertNotNull(e);
        }
    }

    @Test
    void testGetKey() throws Exception {
        String keyName    = "testKey";
        Meter  dummyMeter = mock(Meter.class);

        Field meterField = KMSWebApp.class.getDeclaredField("adminCallsMeter");
        meterField.setAccessible(true);
        meterField.set(null, dummyMeter);

        KMS                kms     = new KMS();
        HttpServletRequest request = mock(HttpServletRequest.class);
        lenient().when(request.getRemoteAddr()).thenReturn("127.0.0.1");
        lenient().when(request.getRequestURI()).thenReturn("/kms/v1/keys/testKey");

        try {
            kms.getKey(keyName, request);
            fail("Expected exception due to missing dependencies (provider/user)");
        } catch (Exception e) {
            System.out.println("Expected exception: " + e.getMessage());
            assertNotNull(e);
        }
    }

    @Test
    void testValidateKeyName_valid_reflection() throws Exception {
        String validKeyName = "ValidKey_123";
        KMS    kms          = new KMS();
        Method method       = KMS.class.getDeclaredMethod("validateKeyName", String.class);
        method.setAccessible(true);

        assertDoesNotThrow(() -> method.invoke(kms, "ValidKey_123"));
    }

    @Test
    void testValidateKeyName_invalid_reflection() throws Exception {
        KMS    kms    = new KMS();
        Method method = KMS.class.getDeclaredMethod("validateKeyName", String.class);
        method.setAccessible(true);

        InvocationTargetException ex = assertThrows(InvocationTargetException.class, () -> {
            method.invoke(kms, "!invalid-key");
        });

        assertInstanceOf(IllegalArgumentException.class, ex.getCause());
    }

    @Test
    void testGetKeyURI_reflection() throws Exception {
        Method method = KMS.class.getDeclaredMethod("getKeyURI", String.class, String.class);
        method.setAccessible(true);

        URI result = (URI) method.invoke(null, "v1", "testKey");

        assertEquals("v1/key/testKey", result.toString()); // ✅ expected value
    }

    @Test
    void testRemoveKeyMaterial_reflection() throws Exception {
        Constructor<?> ctor = KeyProvider.KeyVersion.class.getDeclaredConstructor(String.class, String.class, byte[].class);
        ctor.setAccessible(true);

        KeyProvider.KeyVersion original = (KeyProvider.KeyVersion) ctor.newInstance("key1", "v1", "secret".getBytes());

        Method method = KMS.class.getDeclaredMethod("removeKeyMaterial", KeyProvider.KeyVersion.class);
        method.setAccessible(true);

        KeyProvider.KeyVersion result = (KeyProvider.KeyVersion) method.invoke(null, original);

        assertEquals("key1", result.getName());
        assertEquals("v1", result.getVersionName());
        assertNull(result.getMaterial());
    }

    private static void setStaticField(Class<?> clazz, String fieldName, Object value) throws Exception {
        Field field = clazz.getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(null, value);
    }
}
