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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.key.KeyProvider;
import org.apache.hadoop.crypto.key.KeyProviderCryptoExtension;
import org.apache.hadoop.crypto.key.KeyProviderCryptoExtension.EncryptedKeyVersion;
import org.apache.hadoop.crypto.key.kms.KMSRESTConstants;
import org.apache.hadoop.crypto.key.kms.server.KMS.KMSOp;
import org.apache.hadoop.crypto.key.kms.server.KMSACLsType.Type;
import org.apache.hadoop.crypto.key.kms.server.KeyAuthorizationKeyProvider.KeyACLs;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.delegation.web.DelegationTokenAuthenticationFilter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Response;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URI;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@TestMethodOrder(MethodOrderer.MethodName.class)
public class TestKMS {
    private static final String CLIENT_IP     = "127.0.0.1";
    private static final String TEST_KEY      = "testKey";
    private static final String SENSITIVE_KEY = "sensitive_key";

    private static final String[] KMS_WEB_APP_MUTABLE_STATIC_FIELDS = {
            "kmsConf",
            "kmsAcls",
            "keyProviderCryptoExtension",
            "kmsAudit",
            "adminCallsMeter",
            "keyCallsMeter",
            "generateEEKCallsMeter",
            "decryptEEKCallsMeter",
            "reencryptEEKCallsMeter",
            "reencryptEEKBatchCallsMeter"
    };

    private KeyACLs                    mockAcls;
    private KeyProviderCryptoExtension mockProvider;
    private KMSAudit                   mockAudit;
    private Meter                      mockAdminCallsMeter;
    private Meter                      mockKeyCallsMeter;
    private Meter                      mockGenerateEEKMeter;
    private Meter                      mockDecryptEEKMeter;
    private Meter                      mockReencryptEEKMeter;
    private Meter                      mockReencryptEEKBatchMeter;
    private Map<String, Object>        kmsWebAppStaticsBefore;

    @BeforeEach
    public void setupKmsWebApp() throws Exception {
        System.setProperty("hadoop.home.dir", "./");

        mockAcls                   = mock(KeyACLs.class);
        mockProvider               = mock(KeyProviderCryptoExtension.class);
        mockAudit                  = mock(KMSAudit.class);
        mockAdminCallsMeter        = mock(Meter.class);
        mockKeyCallsMeter          = mock(Meter.class);
        mockGenerateEEKMeter       = mock(Meter.class);
        mockDecryptEEKMeter        = mock(Meter.class);
        mockReencryptEEKMeter      = mock(Meter.class);
        mockReencryptEEKBatchMeter = mock(Meter.class);

        kmsWebAppStaticsBefore = snapshotMutableKmsWebAppStatics();

        setStaticField(KMSWebApp.class, "kmsConf", new Configuration());
        setStaticField(KMSWebApp.class, "kmsAcls", mockAcls);
        setStaticField(KMSWebApp.class, "keyProviderCryptoExtension", mockProvider);
        setStaticField(KMSWebApp.class, "kmsAudit", mockAudit);
        setStaticField(KMSWebApp.class, "adminCallsMeter", mockAdminCallsMeter);
        setStaticField(KMSWebApp.class, "keyCallsMeter", mockKeyCallsMeter);
        setStaticField(KMSWebApp.class, "generateEEKCallsMeter", mockGenerateEEKMeter);
        setStaticField(KMSWebApp.class, "decryptEEKCallsMeter", mockDecryptEEKMeter);
        setStaticField(KMSWebApp.class, "reencryptEEKCallsMeter", mockReencryptEEKMeter);
        setStaticField(KMSWebApp.class, "reencryptEEKBatchCallsMeter", mockReencryptEEKBatchMeter);
    }

    @AfterEach
    public void tearDownKmsWebApp() throws Exception {
        clearHttpUserInContext();
        clearMdcContext();
        restoreKmsWebAppStatics(kmsWebAppStaticsBefore);
    }

    @Test
    public void testGetKeyVersion_passesBaseKeyNameToAssertAccess() throws Exception {
        String               versionName = SENSITIVE_KEY + "@0";
        String               baseKeyName = KeyProvider.getBaseName(versionName);
        UserGroupInformation user        = createTestUser();
        setHttpUserInContext(user);

        HttpServletRequest     request    = mockRequest();
        KeyProvider.KeyVersion keyVersion = createKeyVersion(baseKeyName, versionName, new byte[] {1, 2, 3});
        when(mockProvider.getKeyVersion(versionName)).thenReturn(keyVersion);

        KMS      kms      = newKms();
        Response response = kms.getKeyVersion(versionName, request);

        assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
        assertNotNull(response.getEntity());
        verify(mockAcls).assertAccess(
                eq(Type.GET), eq(user), eq(KMSOp.GET_KEY_VERSION), eq(baseKeyName), eq(CLIENT_IP));
        verify(mockProvider).getKeyVersion(versionName);
        verify(mockAudit).ok(user, KMSOp.GET_KEY_VERSION, baseKeyName, "");
    }

    @Test
    public void testGetKeyVersion_deniedWhenPerKeyAccessDenied() throws Exception {
        String               versionName = SENSITIVE_KEY + "@0";
        String               baseKeyName = KeyProvider.getBaseName(versionName);
        UserGroupInformation user        = createTestUser();
        setHttpUserInContext(user);

        HttpServletRequest request = mockRequest();
        doThrow(new AccessControlException("denied"))
                .when(mockAcls).assertAccess(
                        eq(Type.GET), eq(user), eq(KMSOp.GET_KEY_VERSION), eq(baseKeyName), eq(CLIENT_IP));

        KMS kms = newKms();
        assertThrows(AccessControlException.class, () -> kms.getKeyVersion(versionName, request));
        verify(mockProvider, never()).getKeyVersion(anyString());
        verify(mockAudit, never()).ok(any(), any(), anyString(), anyString());
    }

    @Test
    public void testGetKeyVersion_noVersionInKeyPathFails() throws Exception {
        UserGroupInformation user = createTestUser();
        setHttpUserInContext(user);

        HttpServletRequest request = mockRequest();
        KMS                kms     = newKms();

        assertThrows(IOException.class, () -> kms.getKeyVersion(SENSITIVE_KEY, request));
        verify(mockProvider, never()).getKeyVersion(anyString());
    }

    @Test
    public void testGetCurrentVersion_successPassesKeyNameToAssertAccess() throws Exception {
        UserGroupInformation user = createTestUser();
        setHttpUserInContext(user);

        HttpServletRequest     request    = mockRequest();
        KeyProvider.KeyVersion keyVersion = createKeyVersion(TEST_KEY, TEST_KEY + "@0", new byte[] {1});
        when(mockProvider.getCurrentKey(TEST_KEY)).thenReturn(keyVersion);

        KMS      kms      = newKms();
        Response response = kms.getCurrentVersion(TEST_KEY, request);

        assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
        verify(mockAcls).assertAccess(
                eq(Type.GET), eq(user), eq(KMSOp.GET_CURRENT_KEY), eq(TEST_KEY), eq(CLIENT_IP));
        verify(mockProvider).getCurrentKey(TEST_KEY);
        verify(mockAudit).ok(user, KMSOp.GET_CURRENT_KEY, TEST_KEY, "");
    }

    @Test
    public void testGetCurrentVersion_deniedBeforeProviderCall() throws Exception {
        UserGroupInformation user = createTestUser();
        setHttpUserInContext(user);

        HttpServletRequest request = mockRequest();
        doThrow(new AccessControlException("denied"))
                .when(mockAcls).assertAccess(
                        eq(Type.GET), eq(user), eq(KMSOp.GET_CURRENT_KEY), eq(TEST_KEY), eq(CLIENT_IP));

        KMS kms = newKms();
        assertThrows(AccessControlException.class, () -> kms.getCurrentVersion(TEST_KEY, request));
        verify(mockProvider, never()).getCurrentKey(anyString());
    }

    @Test
    public void testGetKeyVersions_successPassesKeyNameToAssertAccess() throws Exception {
        UserGroupInformation user = createTestUser();
        setHttpUserInContext(user);

        HttpServletRequest           request  = mockRequest();
        List<KeyProvider.KeyVersion> versions = new ArrayList<>();
        versions.add(createKeyVersion(TEST_KEY, TEST_KEY + "@0", new byte[] {1}));
        when(mockProvider.getKeyVersions(TEST_KEY)).thenReturn(versions);

        KMS      kms      = newKms();
        Response response = kms.getKeyVersions(TEST_KEY, request);

        assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
        verify(mockAcls).assertAccess(
                eq(Type.GET), eq(user), eq(KMSOp.GET_KEY_VERSIONS), eq(TEST_KEY), eq(CLIENT_IP));
        verify(mockProvider).getKeyVersions(TEST_KEY);
        verify(mockAudit).ok(user, KMSOp.GET_KEY_VERSIONS, TEST_KEY, "");
    }

    @Test
    public void testGetKeyVersions_deniedBeforeProviderCall() throws Exception {
        UserGroupInformation user = createTestUser();
        setHttpUserInContext(user);

        HttpServletRequest request = mockRequest();
        doThrow(new AccessControlException("denied"))
                .when(mockAcls).assertAccess(
                        eq(Type.GET), eq(user), eq(KMSOp.GET_KEY_VERSIONS), eq(TEST_KEY), eq(CLIENT_IP));

        KMS kms = newKms();
        assertThrows(AccessControlException.class, () -> kms.getKeyVersions(TEST_KEY, request));
        verify(mockProvider, never()).getKeyVersions(anyString());
    }

    @Test
    public void testGetMetadata_success() throws Exception {
        UserGroupInformation user = createTestUser();
        setHttpUserInContext(user);

        HttpServletRequest   request  = mockRequest();
        KeyProvider.Metadata metadata = mockMetadata();
        when(mockProvider.getMetadata(TEST_KEY)).thenReturn(metadata);

        KMS      kms      = newKms();
        Response response = kms.getMetadata(TEST_KEY, request);

        assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
        verify(mockAcls).assertAccess(
                eq(Type.GET_METADATA), eq(user), eq(KMSOp.GET_METADATA), eq(TEST_KEY), eq(CLIENT_IP));
        verify(mockProvider).getMetadata(TEST_KEY);
        verify(mockAudit).ok(user, KMSOp.GET_METADATA, TEST_KEY, "");
    }

    @Test
    public void testGetKey_delegatesToGetMetadata() throws Exception {
        UserGroupInformation user = createTestUser();
        setHttpUserInContext(user);

        HttpServletRequest   request  = mockRequest();
        KeyProvider.Metadata metadata = mockMetadata();
        when(mockProvider.getMetadata(TEST_KEY)).thenReturn(metadata);

        KMS      kms      = newKms();
        Response response = kms.getKey(TEST_KEY, request);

        assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
        verify(mockProvider).getMetadata(TEST_KEY);
    }

    @Test
    public void testCreateKey_successCreatesKeyAndAudits() throws Exception {
        UserGroupInformation user = createTestUser();
        setHttpUserInContext(user);
        setMdcContext(user, "http://localhost/kms/v1/keys");

        HttpServletRequest     request    = mockRequest();
        Map<String, Object>    jsonKey    = baseKeyJson(TEST_KEY);
        KeyProvider.KeyVersion keyVersion = createKeyVersion(TEST_KEY, TEST_KEY + "@0", new byte[] {1, 2, 3});

        when(mockProvider.createKey(eq(TEST_KEY), any(KeyProvider.Options.class))).thenReturn(keyVersion);
        when(mockAcls.hasAccess(eq(Type.GET), eq(user), eq(CLIENT_IP))).thenReturn(true);

        KMS      kms      = newKms();
        Response response = kms.createKey(jsonKey, request);

        assertEquals(Response.Status.CREATED.getStatusCode(), response.getStatus());
        verify(mockAcls).assertAccess(
                eq(Type.CREATE), eq(user), eq(KMSOp.CREATE_KEY), eq(TEST_KEY), eq(CLIENT_IP));
        verify(mockProvider).createKey(eq(TEST_KEY), any(KeyProvider.Options.class));
        verify(mockProvider).flush();
        verify(mockAudit).ok(eq(user), eq(KMSOp.CREATE_KEY), eq(TEST_KEY), anyString());
    }

    @Test
    public void testCreateKey_deniedBeforeProviderCall() throws Exception {
        UserGroupInformation user = createTestUser();
        setHttpUserInContext(user);

        HttpServletRequest  request = mockRequest();
        Map<String, Object> jsonKey = baseKeyJson(TEST_KEY);

        doThrow(new AccessControlException("denied"))
                .when(mockAcls).assertAccess(
                        eq(Type.CREATE), eq(user), eq(KMSOp.CREATE_KEY), eq(TEST_KEY), eq(CLIENT_IP));

        KMS kms = newKms();
        assertThrows(AccessControlException.class, () -> kms.createKey(jsonKey, request));
        verify(mockProvider, never()).createKey(anyString(), any(KeyProvider.Options.class));
    }

    @Test
    public void testCreateKey_withUserMaterialRequiresSetKeyMaterialAccess() throws Exception {
        UserGroupInformation user = createTestUser();
        setHttpUserInContext(user);
        setMdcContext(user, "http://localhost/kms/v1/keys");

        HttpServletRequest  request = mockRequest();
        Map<String, Object> jsonKey = baseKeyJson(TEST_KEY);
        jsonKey.put(KMSRESTConstants.MATERIAL_FIELD,
                Base64.getEncoder().encodeToString(new byte[] {1, 2, 3, 4}));

        KeyProvider.KeyVersion keyVersion = createKeyVersion(TEST_KEY, TEST_KEY + "@0", new byte[] {1});
        when(mockProvider.createKey(eq(TEST_KEY), any(byte[].class), any(KeyProvider.Options.class)))
                .thenReturn(keyVersion);
        when(mockAcls.hasAccess(eq(Type.GET), eq(user), eq(CLIENT_IP))).thenReturn(true);

        KMS kms = newKms();
        kms.createKey(jsonKey, request);

        verify(mockAcls).assertAccess(
                eq(Type.SET_KEY_MATERIAL), eq(user), eq(KMSOp.CREATE_KEY), eq(TEST_KEY), eq(CLIENT_IP));
    }

    @Test
    public void testDeleteKey_success() throws Exception {
        UserGroupInformation user = createTestUser();
        setHttpUserInContext(user);

        HttpServletRequest request  = mockRequest();
        KMS                kms      = newKms();
        Response           response = kms.deleteKey(TEST_KEY, request);

        assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
        verify(mockAcls).assertAccess(
                eq(Type.DELETE), eq(user), eq(KMSOp.DELETE_KEY), eq(TEST_KEY), eq(CLIENT_IP));
        verify(mockProvider).deleteKey(TEST_KEY);
        verify(mockProvider).flush();
        verify(mockAudit).ok(user, KMSOp.DELETE_KEY, TEST_KEY, "");
    }

    @Test
    public void testDeleteKey_deniedBeforeProviderCall() throws Exception {
        UserGroupInformation user = createTestUser();
        setHttpUserInContext(user);

        HttpServletRequest request = mockRequest();
        doThrow(new AccessControlException("denied"))
                .when(mockAcls).assertAccess(
                        eq(Type.DELETE), eq(user), eq(KMSOp.DELETE_KEY), eq(TEST_KEY), eq(CLIENT_IP));

        KMS kms = newKms();
        assertThrows(AccessControlException.class, () -> kms.deleteKey(TEST_KEY, request));
        verify(mockProvider, never()).deleteKey(anyString());
    }

    @Test
    public void testRolloverKey_success() throws Exception {
        UserGroupInformation user = createTestUser();
        setHttpUserInContext(user);

        HttpServletRequest     request      = mockRequest();
        Map<String, Object>    jsonMaterial = new HashMap<>();
        KeyProvider.KeyVersion keyVersion   = createKeyVersion(TEST_KEY, TEST_KEY + "@1", new byte[] {5});
        when(mockProvider.rollNewVersion(TEST_KEY)).thenReturn(keyVersion);
        when(mockAcls.hasAccess(eq(Type.GET), eq(user), eq(CLIENT_IP))).thenReturn(true);

        KMS      kms      = newKms();
        Response response = kms.rolloverKey(TEST_KEY, jsonMaterial, request);

        assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
        verify(mockAcls).assertAccess(
                eq(Type.ROLLOVER), eq(user), eq(KMSOp.ROLL_NEW_VERSION), eq(TEST_KEY), eq(CLIENT_IP));
        verify(mockProvider).rollNewVersion(TEST_KEY);
        verify(mockProvider).flush();
        verify(mockAudit).ok(eq(user), eq(KMSOp.ROLL_NEW_VERSION), eq(TEST_KEY), anyString());
    }

    @Test
    public void testRolloverKey_deniedBeforeProviderCall() throws Exception {
        UserGroupInformation user = createTestUser();
        setHttpUserInContext(user);

        HttpServletRequest request = mockRequest();
        doThrow(new AccessControlException("denied"))
                .when(mockAcls).assertAccess(
                        eq(Type.ROLLOVER), eq(user), eq(KMSOp.ROLL_NEW_VERSION), eq(TEST_KEY), eq(CLIENT_IP));

        KMS kms = newKms();
        assertThrows(AccessControlException.class,
                () -> kms.rolloverKey(TEST_KEY, new HashMap<>(), request));
        verify(mockProvider, never()).rollNewVersion(anyString());
    }

    @Test
    public void testInvalidateCache_success() throws Exception {
        UserGroupInformation user = createTestUser();
        setHttpUserInContext(user);

        HttpServletRequest request  = mockRequest();
        KMS                kms      = newKms();
        Response           response = kms.invalidateCache(TEST_KEY, request);

        assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
        verify(mockAcls).assertAccess(
                eq(Type.ROLLOVER), eq(user), eq(KMSOp.INVALIDATE_CACHE), eq(TEST_KEY), eq(CLIENT_IP));
        verify(mockProvider).invalidateCache(TEST_KEY);
        verify(mockProvider).flush();
        verify(mockAudit).ok(user, KMSOp.INVALIDATE_CACHE, TEST_KEY, "");
    }

    @Test
    public void testInvalidateCache_deniedBeforeProviderCall() throws Exception {
        UserGroupInformation user = createTestUser();
        setHttpUserInContext(user);

        HttpServletRequest request = mockRequest();
        doThrow(new AccessControlException("denied"))
                .when(mockAcls).assertAccess(
                        eq(Type.ROLLOVER), eq(user), eq(KMSOp.INVALIDATE_CACHE), eq(TEST_KEY), eq(CLIENT_IP));

        KMS kms = newKms();
        assertThrows(AccessControlException.class, () -> kms.invalidateCache(TEST_KEY, request));
        verify(mockProvider, never()).invalidateCache(anyString());
    }

    @Test
    public void testGetKeyNames_successUsesNullKeyNameInAssertAccess() throws Exception {
        UserGroupInformation user = createTestUser();
        setHttpUserInContext(user);

        HttpServletRequest request = mockRequest();
        List<String>       keys    = new ArrayList<>();
        keys.add(TEST_KEY);
        when(mockProvider.getKeys()).thenReturn(keys);

        KMS      kms      = newKms();
        Response response = kms.getKeyNames(request);

        assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
        verify(mockAcls).assertAccess(
                eq(Type.GET_KEYS), eq(user), eq(KMSOp.GET_KEYS), isNull(), eq(CLIENT_IP));
        verify(mockProvider).getKeys();
        verify(mockAudit).ok(user, KMSOp.GET_KEYS, "");
    }

    @Test
    public void testGetKeyNames_deniedBeforeProviderCall() throws Exception {
        UserGroupInformation user = createTestUser();
        setHttpUserInContext(user);

        HttpServletRequest request = mockRequest();
        doThrow(new AccessControlException("denied"))
                .when(mockAcls).assertAccess(
                        eq(Type.GET_KEYS), eq(user), eq(KMSOp.GET_KEYS), isNull(), eq(CLIENT_IP));

        KMS kms = newKms();
        assertThrows(AccessControlException.class, () -> kms.getKeyNames(request));
        verify(mockProvider, never()).getKeys();
    }

    @Test
    public void testGetKeysMetadata_successUsesNullKeyNameInAssertAccess() throws Exception {
        UserGroupInformation user = createTestUser();
        setHttpUserInContext(user);

        HttpServletRequest request      = mockRequest();
        List<String>       keyNamesList = new ArrayList<>();
        keyNamesList.add(TEST_KEY);
        KeyProvider.Metadata[] metas = {mockMetadata()};
        when(mockProvider.getKeysMetadata(any(String[].class))).thenReturn(metas);

        KMS      kms      = newKms();
        Response response = kms.getKeysMetadata(keyNamesList, request);

        assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
        verify(mockAcls).assertAccess(
                eq(Type.GET_METADATA), eq(user), eq(KMSOp.GET_KEYS_METADATA), isNull(), eq(CLIENT_IP));
        verify(mockProvider).getKeysMetadata(any(String[].class));
        verify(mockAudit).ok(user, KMSOp.GET_KEYS_METADATA, "");
    }

    @Test
    public void testGetKeysMetadata_deniedBeforeProviderCall() throws Exception {
        UserGroupInformation user = createTestUser();
        setHttpUserInContext(user);

        HttpServletRequest request      = mockRequest();
        List<String>       keyNamesList = new ArrayList<>();
        keyNamesList.add(TEST_KEY);

        doThrow(new AccessControlException("denied"))
                .when(mockAcls).assertAccess(
                        eq(Type.GET_METADATA), eq(user), eq(KMSOp.GET_KEYS_METADATA), isNull(), eq(CLIENT_IP));

        KMS kms = newKms();
        assertThrows(AccessControlException.class, () -> kms.getKeysMetadata(keyNamesList, request));
        verify(mockProvider, never()).getKeysMetadata(any(String[].class));
    }

    @Test
    public void testGenerateEncryptedKeys_successWithGenerateOp() throws Exception {
        UserGroupInformation user = createTestUser();
        setHttpUserInContext(user);

        HttpServletRequest  request = mockRequest();
        EncryptedKeyVersion ekv     = mockEncryptedKeyVersion(TEST_KEY, TEST_KEY + "@0");
        when(mockProvider.generateEncryptedKey(TEST_KEY)).thenReturn(ekv);

        KMS kms = newKms();
        Response response = kms.generateEncryptedKeys(
                TEST_KEY, KMSRESTConstants.EEK_GENERATE, 1, request);

        assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
        verify(mockAcls).assertAccess(
                eq(Type.GENERATE_EEK), eq(user), eq(KMSOp.GENERATE_EEK), eq(TEST_KEY), eq(CLIENT_IP));
        verify(mockProvider).generateEncryptedKey(TEST_KEY);
        verify(mockAudit).ok(user, KMSOp.GENERATE_EEK, TEST_KEY, "");
        verify(mockGenerateEEKMeter).mark();
    }

    @Test
    public void testGenerateEncryptedKeys_invalidEekOpFails() throws Exception {
        UserGroupInformation user = createTestUser();
        setHttpUserInContext(user);

        HttpServletRequest request = mockRequest();
        KMS                kms     = newKms();

        assertThrows(IllegalArgumentException.class, () ->
                kms.generateEncryptedKeys(TEST_KEY, "generateEncryptedKeys", 1, request));
        verify(mockProvider, never()).generateEncryptedKey(anyString());
    }

    // reencryptEncryptedKeys

    @Test
    public void testReencryptEncryptedKeys_success() throws Exception {
        UserGroupInformation user = createTestUser();
        setHttpUserInContext(user);

        HttpServletRequest request = mockRequest();
        List<Map>          payload = buildReencryptPayload(TEST_KEY, 1);

        KMS      kms      = newKms();
        Response response = kms.reencryptEncryptedKeys(TEST_KEY, payload, request);

        assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
        verify(mockAcls).assertAccess(
                eq(Type.GENERATE_EEK), eq(user), eq(KMSOp.REENCRYPT_EEK_BATCH), eq(TEST_KEY), eq(CLIENT_IP));
        verify(mockProvider).reencryptEncryptedKeys(any(List.class));
        verify(mockAudit).ok(eq(user), eq(KMSOp.REENCRYPT_EEK_BATCH), eq(TEST_KEY), anyString());
        verify(mockReencryptEEKBatchMeter).mark();
    }

    @Test
    public void testReencryptEncryptedKeys_deniedBeforeProviderCall() throws Exception {
        UserGroupInformation user = createTestUser();
        setHttpUserInContext(user);

        HttpServletRequest request = mockRequest();
        List<Map>          payload = buildReencryptPayload(TEST_KEY, 1);

        doThrow(new AccessControlException("denied"))
                .when(mockAcls).assertAccess(
                        eq(Type.GENERATE_EEK), eq(user), eq(KMSOp.REENCRYPT_EEK_BATCH), eq(TEST_KEY), eq(CLIENT_IP));

        KMS kms = newKms();
        assertThrows(AccessControlException.class,
                () -> kms.reencryptEncryptedKeys(TEST_KEY, payload, request));
        verify(mockProvider, never()).reencryptEncryptedKeys(any(List.class));
    }

    @Test
    public void testHandleEncryptedKeyOp_decryptSuccess() throws Exception {
        UserGroupInformation user = createTestUser();
        setHttpUserInContext(user);

        HttpServletRequest  request     = mockRequest();
        String              versionName = TEST_KEY + "@0";
        Map<String, Object> payload     = buildDecryptPayload(TEST_KEY);

        KeyProvider.KeyVersion decrypted = createKeyVersion(TEST_KEY, versionName, new byte[] {9});
        when(mockProvider.decryptEncryptedKey(any())).thenReturn(decrypted);

        KMS kms = newKms();
        Response response = kms.handleEncryptedKeyOp(
                versionName, KMSRESTConstants.EEK_DECRYPT, payload, request);

        assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
        verify(mockAcls).assertAccess(
                eq(Type.DECRYPT_EEK), eq(user), eq(KMSOp.DECRYPT_EEK), eq(TEST_KEY), eq(CLIENT_IP));
        verify(mockProvider).decryptEncryptedKey(any());
        verify(mockAudit).ok(user, KMSOp.DECRYPT_EEK, TEST_KEY, "");
        verify(mockDecryptEEKMeter).mark();
    }

    @Test
    public void testHandleEncryptedKeyOp_invalidEekOpFails() throws Exception {
        UserGroupInformation user = createTestUser();
        setHttpUserInContext(user);

        HttpServletRequest  request = mockRequest();
        Map<String, Object> payload = buildDecryptPayload(TEST_KEY);

        KMS kms = newKms();
        assertThrows(IllegalArgumentException.class, () ->
                kms.handleEncryptedKeyOp(TEST_KEY + "@0", "bad_op", payload, request));
    }

    @Test
    public void testGenerateDataKey_success() throws Exception {
        UserGroupInformation user = createTestUser();
        setHttpUserInContext(user);

        HttpServletRequest     request = mockRequest();
        EncryptedKeyVersion    ekv     = mockEncryptedKeyVersion(TEST_KEY, TEST_KEY + "@0");
        KeyProvider.KeyVersion dek     = createKeyVersion(TEST_KEY, TEST_KEY + "@0", new byte[] {7});

        when(mockProvider.generateEncryptedKey(TEST_KEY)).thenReturn(ekv);
        when(mockProvider.decryptEncryptedKey(any())).thenReturn(dek);

        KMS      kms      = newKms();
        Response response = kms.generateDataKey(TEST_KEY, request);

        assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
        verify(mockAcls).assertAccess(
                eq(Type.GENERATE_EEK), eq(user), eq(KMSOp.GENERATE_EEK), eq(TEST_KEY), eq(CLIENT_IP));
        verify(mockAcls).assertAccess(
                eq(Type.DECRYPT_EEK), eq(user), eq(KMSOp.DECRYPT_EEK), eq(TEST_KEY), eq(CLIENT_IP));
        verify(mockProvider).generateEncryptedKey(TEST_KEY);
        verify(mockProvider).decryptEncryptedKey(any());
    }

    @Test
    public void testGenerateDataKey_deniedOnGenerateBeforeDecrypt() throws Exception {
        UserGroupInformation user = createTestUser();
        setHttpUserInContext(user);

        HttpServletRequest request = mockRequest();
        doThrow(new AccessControlException("denied"))
                .when(mockAcls).assertAccess(
                        eq(Type.GENERATE_EEK), eq(user), eq(KMSOp.GENERATE_EEK), eq(TEST_KEY), eq(CLIENT_IP));

        KMS kms = newKms();
        IOException ex = assertThrows(IOException.class, () -> kms.generateDataKey(TEST_KEY, request));
        assertInstanceOf(AccessControlException.class, ex.getCause());
        verify(mockProvider, never()).generateEncryptedKey(anyString());
    }

    // Validation and helpers

    @Test
    public void testValidateKeyName_valid() throws Exception {
        KMS    kms    = newKms();
        Method method = KMS.class.getDeclaredMethod("validateKeyName", String.class);
        method.setAccessible(true);
        assertDoesNotThrow(() -> method.invoke(kms, "ValidKey_123"));
    }

    @Test
    public void testValidateKeyName_invalid() throws Exception {
        KMS    kms    = newKms();
        Method method = KMS.class.getDeclaredMethod("validateKeyName", String.class);
        method.setAccessible(true);

        InvocationTargetException ex = assertThrows(InvocationTargetException.class, () ->
                method.invoke(kms, "!invalid-key"));
        assertInstanceOf(IllegalArgumentException.class, ex.getCause());
    }

    @Test
    public void testGetKeyURI_buildsExpectedPath() throws Exception {
        Method method = KMS.class.getDeclaredMethod("getKeyURI", String.class, String.class);
        method.setAccessible(true);
        URI result = (URI) method.invoke(null, "v1", TEST_KEY);
        assertEquals("v1/key/testKey", result.toString());
    }

    @Test
    public void testRemoveKeyMaterial_stripsBytes() throws Exception {
        KeyProvider.KeyVersion original = createKeyVersion("key1", "v1", "secret".getBytes());
        Method                 method   = KMS.class.getDeclaredMethod("removeKeyMaterial", KeyProvider.KeyVersion.class);
        method.setAccessible(true);

        KeyProvider.KeyVersion result = (KeyProvider.KeyVersion) method.invoke(null, original);
        assertEquals("key1", result.getName());
        assertEquals("v1", result.getVersionName());
        assertNull(result.getMaterial());
    }

    @Test
    public void testCreateKey_withoutUserFails() throws Exception {
        HttpServletRequest  request = mockRequest();
        Map<String, Object> jsonKey = baseKeyJson(TEST_KEY);
        KMS                 kms     = newKms();

        assertThrows(NullPointerException.class, () -> kms.createKey(jsonKey, request));
    }

    // Helpers

    private KMS newKms() throws Exception {
        return new KMS();
    }

    private UserGroupInformation createTestUser() throws IOException {
        return UserGroupInformation.createRemoteUser("testuser");
    }

    private HttpServletRequest mockRequest() {
        HttpServletRequest request = mock(HttpServletRequest.class);
        lenient().when(request.getRemoteAddr()).thenReturn(CLIENT_IP);
        return request;
    }

    private Map<String, Object> baseKeyJson(String keyName) {
        Map<String, Object> jsonKey = new HashMap<>();
        jsonKey.put(KMSRESTConstants.NAME_FIELD, keyName);
        jsonKey.put(KMSRESTConstants.CIPHER_FIELD, "AES");
        jsonKey.put(KMSRESTConstants.LENGTH_FIELD, 128);
        jsonKey.put(KMSRESTConstants.DESCRIPTION_FIELD, "test key");
        return jsonKey;
    }

    private List<Map> buildReencryptPayload(String keyName, int count) {
        List<Map> payload = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            String              versionName = keyName + "@" + i;
            Map<String, Object> encInner    = new HashMap<>();
            encInner.put(KMSRESTConstants.VERSION_NAME_FIELD, versionName);
            encInner.put(KMSRESTConstants.MATERIAL_FIELD,
                    Base64.getEncoder().encodeToString("encMat".getBytes()));

            Map<String, Object> entry = new HashMap<>();
            entry.put(KMSRESTConstants.VERSION_NAME_FIELD, versionName);
            entry.put(KMSRESTConstants.IV_FIELD,
                    Base64.getEncoder().encodeToString("12345678".getBytes()));
            entry.put(KMSRESTConstants.ENCRYPTED_KEY_VERSION_FIELD, encInner);
            payload.add(entry);
        }
        return payload;
    }

    private Map<String, Object> buildDecryptPayload(String keyName) {
        Map<String, Object> payload = new HashMap<>();
        payload.put(KMSRESTConstants.NAME_FIELD, keyName);
        payload.put(KMSRESTConstants.IV_FIELD,
                Base64.getEncoder().encodeToString("12345678".getBytes()));
        payload.put(KMSRESTConstants.MATERIAL_FIELD,
                Base64.getEncoder().encodeToString("encMaterial".getBytes()));
        return payload;
    }

    private KeyProvider.Metadata mockMetadata() {
        KeyProvider.Metadata metadata = mock(KeyProvider.Metadata.class);
        when(metadata.getCipher()).thenReturn("AES/CTR/NoPadding");
        when(metadata.getBitLength()).thenReturn(128);
        when(metadata.getDescription()).thenReturn("test key");
        when(metadata.getAttributes()).thenReturn(null);
        when(metadata.getCreated()).thenReturn(new Date());
        when(metadata.getVersions()).thenReturn(1);
        return metadata;
    }

    private EncryptedKeyVersion mockEncryptedKeyVersion(String keyName, String versionName)
            throws Exception {
        KeyProvider.KeyVersion nested = createKeyVersion(keyName, versionName, new byte[] {1, 2, 3});
        EncryptedKeyVersion    ekv    = mock(EncryptedKeyVersion.class);
        lenient().when(ekv.getEncryptionKeyName()).thenReturn(keyName);
        when(ekv.getEncryptionKeyVersionName()).thenReturn(versionName);
        when(ekv.getEncryptedKeyIv()).thenReturn(new byte[8]);
        when(ekv.getEncryptedKeyVersion()).thenReturn(nested);
        return ekv;
    }

    private static KeyProvider.KeyVersion createKeyVersion(String name, String versionName, byte[] material)
            throws Exception {
        Constructor<?> ctor = KeyProvider.KeyVersion.class.getDeclaredConstructor(
                String.class, String.class, byte[].class);
        ctor.setAccessible(true);
        return (KeyProvider.KeyVersion) ctor.newInstance(name, versionName, material);
    }

    private static void setHttpUserInContext(UserGroupInformation ugi) throws Exception {
        Field ugiTlField = DelegationTokenAuthenticationFilter.class.getDeclaredField("UGI_TL");
        ugiTlField.setAccessible(true);
        @SuppressWarnings("unchecked")
        ThreadLocal<UserGroupInformation> ugiTl =
                (ThreadLocal<UserGroupInformation>) ugiTlField.get(null);
        ugiTl.set(ugi);
    }

    private static void clearHttpUserInContext() throws Exception {
        Field ugiTlField = DelegationTokenAuthenticationFilter.class.getDeclaredField("UGI_TL");
        ugiTlField.setAccessible(true);
        @SuppressWarnings("unchecked")
        ThreadLocal<UserGroupInformation> ugiTl =
                (ThreadLocal<UserGroupInformation>) ugiTlField.get(null);
        ugiTl.remove();
    }

    private static void setMdcContext(UserGroupInformation ugi, String url) throws Exception {
        Field dataTlField = KMSMDCFilter.class.getDeclaredField("DATA_TL");
        dataTlField.setAccessible(true);
        @SuppressWarnings("unchecked")
        ThreadLocal<Object> tl = (ThreadLocal<Object>) dataTlField.get(null);

        Class<?> dataClass = Class.forName(
                "org.apache.hadoop.crypto.key.kms.server.KMSMDCFilter$Data");
        Constructor<?> ctor = dataClass.getDeclaredConstructor(
                UserGroupInformation.class, String.class, String.class);
        ctor.setAccessible(true);
        Object data = ctor.newInstance(ugi, "POST", url);
        tl.set(data);
    }

    private static void clearMdcContext() throws Exception {
        Field dataTlField = KMSMDCFilter.class.getDeclaredField("DATA_TL");
        dataTlField.setAccessible(true);
        @SuppressWarnings("unchecked")
        ThreadLocal<?> tl = (ThreadLocal<?>) dataTlField.get(null);
        tl.remove();
    }

    private static Map<String, Object> snapshotMutableKmsWebAppStatics() throws Exception {
        Map<String, Object> snapshot = new HashMap<>();
        for (String fieldName : KMS_WEB_APP_MUTABLE_STATIC_FIELDS) {
            snapshot.put(fieldName, getStaticField(KMSWebApp.class, fieldName));
        }
        return snapshot;
    }

    private static void restoreKmsWebAppStatics(Map<String, Object> snapshot) throws Exception {
        for (Map.Entry<String, Object> entry : snapshot.entrySet()) {
            setStaticField(KMSWebApp.class, entry.getKey(), entry.getValue());
        }
    }

    private static Object getStaticField(Class<?> clazz, String fieldName) throws Exception {
        Field field = clazz.getDeclaredField(fieldName);
        field.setAccessible(true);
        return field.get(null);
    }

    private static void setStaticField(Class<?> clazz, String fieldName, Object value) throws Exception {
        Field field = clazz.getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(null, value);
    }
}
