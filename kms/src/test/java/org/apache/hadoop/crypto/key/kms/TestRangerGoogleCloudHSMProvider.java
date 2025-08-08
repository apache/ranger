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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.key.RangerGoogleCloudHSMProvider;
import org.bouncycastle.crypto.RuntimeCryptoException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;

import java.lang.reflect.Method;
import java.security.NoSuchAlgorithmException;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(MockitoExtension.class)
@TestMethodOrder(MethodOrderer.MethodName.class)
@Disabled
public class TestRangerGoogleCloudHSMProvider {
    private static final String        VALID_PROJECT_ID      = "test-project-id";
    private static final String        VALID_LOCATION_ID     = "us-central1";
    private static final String        VALID_KEYRING_ID      = "test-keyring";
    private static final String        VALID_MASTER_KEY_NAME = "test-master-key";
    private static final String        VALID_CRED_FILE       = "/path/to/credentials.json";
    private static final String        INVALID_CRED_FILE     = "/path/to/credentials.txt";
    private              Configuration configuration;

    @BeforeEach
    public void setUp() {
        configuration = new Configuration();
    }

    @Test
    public void testConstructorWithValidConfiguration() throws Exception {
        setValidConfiguration();

        RangerGoogleCloudHSMProvider provider = new RangerGoogleCloudHSMProvider(configuration);

        assertNotNull(provider);
    }

    @Test
    public void testConstructorWithNullConfiguration() {
        assertThrows(NullPointerException.class, () -> {
            RangerGoogleCloudHSMProvider provider = new RangerGoogleCloudHSMProvider(null);
        });
    }

    @Test
    public void testValidateGcpPropsWithValidConfiguration() throws Exception {
        setValidConfiguration();

        RangerGoogleCloudHSMProvider provider = new RangerGoogleCloudHSMProvider(configuration);

        Method validateMethod = RangerGoogleCloudHSMProvider.class.getDeclaredMethod("validateGcpProps");
        validateMethod.setAccessible(true);

        assertDoesNotThrow(() -> {
            validateMethod.invoke(provider);
        });
    }

    @Test
    public void testValidateGcpPropsWithInvalidCredentialFile() throws Exception {
        setValidConfiguration();
        configuration.set("ranger.kms.gcp.cred.file", INVALID_CRED_FILE);

        RangerGoogleCloudHSMProvider provider = new RangerGoogleCloudHSMProvider(configuration);

        Method validateMethod = RangerGoogleCloudHSMProvider.class.getDeclaredMethod("validateGcpProps");
        validateMethod.setAccessible(true);

        Exception exception = assertThrows(Exception.class, () ->
                validateMethod.invoke(provider));

        Throwable cause = exception.getCause();
        assertInstanceOf(RuntimeCryptoException.class, cause);
        assertTrue(cause.getMessage().contains("Invalid GCP app Credential JSON file"));
    }

    @Test
    public void testValidateGcpPropsWithEmptyCredentialFile() throws Exception {
        setValidConfiguration();
        configuration.set("ranger.kms.gcp.cred.file", "");

        RangerGoogleCloudHSMProvider provider = new RangerGoogleCloudHSMProvider(configuration);

        Method validateMethod = RangerGoogleCloudHSMProvider.class.getDeclaredMethod("validateGcpProps");
        validateMethod.setAccessible(true);

        Exception exception = assertThrows(Exception.class, () ->
                validateMethod.invoke(provider));

        Throwable cause = exception.getCause();
        assertInstanceOf(RuntimeCryptoException.class, cause);
        assertTrue(cause.getMessage().contains("Invalid GCP app Credential JSON file"));
    }

    @Test
    public void testValidateGcpPropsWithEmptyKeyRingId() throws Exception {
        setValidConfiguration();
        configuration.set("ranger.kms.gcp.keyring.id", "");

        RangerGoogleCloudHSMProvider provider = new RangerGoogleCloudHSMProvider(configuration);

        Method validateMethod = RangerGoogleCloudHSMProvider.class.getDeclaredMethod("validateGcpProps");
        validateMethod.setAccessible(true);

        Exception exception = assertThrows(Exception.class, () ->
                validateMethod.invoke(provider));

        Throwable cause = exception.getCause();
        assertInstanceOf(RuntimeCryptoException.class, cause);
        assertTrue(cause.getMessage().contains("Please provide GCP app KeyringId"));
    }

    @Test
    public void testValidateGcpPropsWithEmptyLocationId() throws Exception {
        setValidConfiguration();
        configuration.set("ranger.kms.gcp.location.id", "");

        RangerGoogleCloudHSMProvider provider = new RangerGoogleCloudHSMProvider(configuration);

        Method validateMethod = RangerGoogleCloudHSMProvider.class.getDeclaredMethod("validateGcpProps");
        validateMethod.setAccessible(true);

        Exception exception = assertThrows(Exception.class, () ->
                validateMethod.invoke(provider));

        Throwable cause = exception.getCause();
        assertInstanceOf(RuntimeCryptoException.class, cause);
        assertTrue(cause.getMessage().contains("Please provide the GCP app location Id"));
    }

    @Test
    public void testValidateGcpPropsWithEmptyProjectId() throws Exception {
        setValidConfiguration();
        configuration.set("ranger.kms.gcp.project.id", "");

        RangerGoogleCloudHSMProvider provider = new RangerGoogleCloudHSMProvider(configuration);

        Method validateMethod = RangerGoogleCloudHSMProvider.class.getDeclaredMethod("validateGcpProps");
        validateMethod.setAccessible(true);

        Exception exception = assertThrows(Exception.class, () ->
                validateMethod.invoke(provider));

        Throwable cause = exception.getCause();
        assertInstanceOf(RuntimeCryptoException.class, cause);
        assertTrue(cause.getMessage().contains("Please provide the GCP app project Id"));
    }

    @Test
    public void testValidateGcpPropsWithEmptyMasterKeyName() throws Exception {
        setValidConfiguration();
        configuration.set("ranger.kms.gcp.masterkey.name", "");

        RangerGoogleCloudHSMProvider provider = new RangerGoogleCloudHSMProvider(configuration);

        Method validateMethod = RangerGoogleCloudHSMProvider.class.getDeclaredMethod("validateGcpProps");
        validateMethod.setAccessible(true);

        Exception exception = assertThrows(Exception.class, () -> {
            validateMethod.invoke(provider);
        });

        Throwable cause = exception.getCause();
        assertInstanceOf(RuntimeCryptoException.class, cause);
        assertTrue(cause.getMessage().contains("Master key name must not be empty"));
    }

    @Test
    public void testValidateGcpPropsWithNullValues() throws Exception {
        RangerGoogleCloudHSMProvider provider = new RangerGoogleCloudHSMProvider(configuration);

        Method validateMethod = RangerGoogleCloudHSMProvider.class.getDeclaredMethod("validateGcpProps");
        validateMethod.setAccessible(true);

        Exception exception = assertThrows(Exception.class, () -> {
            validateMethod.invoke(provider);
        });

        Throwable cause = exception.getCause();
        assertInstanceOf(RuntimeCryptoException.class, cause);
        assertTrue(cause.getMessage().contains("Invalid GCP app Credential JSON file"));
    }

    @Test
    public void testGetMasterKeyReturnsNull() throws Throwable {
        setValidConfiguration();

        RangerGoogleCloudHSMProvider provider = new RangerGoogleCloudHSMProvider(configuration);

        String result = provider.getMasterKey("test-password");

        assertNull(result, "getMasterKey should return null as master key cannot be retrieved from GCP HSM");
    }

    @Test
    public void testGetMasterKeyWithNullPassword() throws Throwable {
        setValidConfiguration();

        RangerGoogleCloudHSMProvider provider = new RangerGoogleCloudHSMProvider(configuration);

        String result = provider.getMasterKey(null);

        assertNull(result, "getMasterKey should return null regardless of password input");
    }

    @Test
    public void testGetMasterKeyWithEmptyPassword() throws Throwable {
        setValidConfiguration();

        RangerGoogleCloudHSMProvider provider = new RangerGoogleCloudHSMProvider(configuration);

        String result = provider.getMasterKey("");

        assertNull(result, "getMasterKey should return null regardless of password input");
    }

    @Test
    public void testEncryptZoneKeyWithNullKey() throws Exception {
        setValidConfiguration();

        RangerGoogleCloudHSMProvider provider = new RangerGoogleCloudHSMProvider(configuration);

        assertThrows(Exception.class, () -> {
            provider.encryptZoneKey(null);
        });
    }

    @Test
    public void testDecryptZoneKeyWithNullBytes() throws Exception {
        setValidConfiguration();

        RangerGoogleCloudHSMProvider provider = new RangerGoogleCloudHSMProvider(configuration);

        assertThrows(Exception.class, () -> {
            provider.decryptZoneKey(null);
        });
    }

    @Test
    public void testDecryptZoneKeyWithEmptyBytes() throws Exception {
        setValidConfiguration();

        RangerGoogleCloudHSMProvider provider = new RangerGoogleCloudHSMProvider(configuration);

        assertThrows(Exception.class, () ->
                provider.decryptZoneKey(new byte[0]));
    }

    @Test
    public void testOnInitializationWithInvalidConfiguration() throws Exception {
        RangerGoogleCloudHSMProvider provider = new RangerGoogleCloudHSMProvider(configuration);

        RuntimeCryptoException exception = assertThrows(RuntimeCryptoException.class, () -> {
            provider.onInitialization();
        });

        assertNotNull(exception.getMessage());
        assertTrue(exception.getMessage().contains("Invalid GCP app Credential JSON file") ||
                exception.getMessage().contains("Please provide"));
    }

    @Test
    public void testGenerateMasterKeyWithValidConfiguration() throws Exception {
        setValidConfiguration();

        RangerGoogleCloudHSMProvider provider = new RangerGoogleCloudHSMProvider(configuration);

        assertThrows(Exception.class, () ->
                provider.generateMasterKey("test-password"));
    }

    @Test
    public void testGenerateMasterKeyWithNullPassword() throws Exception {
        setValidConfiguration();

        RangerGoogleCloudHSMProvider provider = new RangerGoogleCloudHSMProvider(configuration);

        assertThrows(Exception.class, () ->
                provider.generateMasterKey(null));
    }

    @Test
    public void testGenerateMasterKeyWithEmptyPassword() throws Exception {
        setValidConfiguration();

        RangerGoogleCloudHSMProvider provider = new RangerGoogleCloudHSMProvider(configuration);

        assertThrows(Exception.class, () ->
                provider.generateMasterKey(""));
    }

    @Test
    public void testEncryptZoneKeyWithValidKey() throws Exception {
        setValidConfiguration();

        RangerGoogleCloudHSMProvider provider = new RangerGoogleCloudHSMProvider(configuration);

        SecretKey testKey = generateTestKey();

        assertThrows(Exception.class, () ->
                provider.encryptZoneKey(testKey));
    }

    @Test
    public void testConfigurationPropertiesAreUsed() {
        setValidConfiguration();

        assertDoesNotThrow(() -> {
            RangerGoogleCloudHSMProvider provider = new RangerGoogleCloudHSMProvider(configuration);
            assertNotNull(provider);
        });
    }

    @Test
    public void testPrivateGetKeyClientMethod() throws Exception {
        setValidConfiguration();

        RangerGoogleCloudHSMProvider provider = new RangerGoogleCloudHSMProvider(configuration);

        Method getKeyClientMethod = RangerGoogleCloudHSMProvider.class.getDeclaredMethod("getKeyClient", String.class);
        getKeyClientMethod.setAccessible(true);

        Object result = getKeyClientMethod.invoke(provider, VALID_CRED_FILE);

        assertNull(result, "getKeyClient should return null when credentials are invalid or file doesn't exist");
    }

    @Test
    public void testUpdateEnvMethod() throws Exception {
        Method updateEnvMethod = RangerGoogleCloudHSMProvider.class.getDeclaredMethod("updateEnv", String.class, String.class);
        updateEnvMethod.setAccessible(true);

        assertDoesNotThrow(() -> {
            updateEnvMethod.invoke(null, "TEST_ENV_VAR", "test_value");
        });
    }

    private void setValidConfiguration() {
        configuration.set("ranger.kms.gcp.project.id", VALID_PROJECT_ID);
        configuration.set("ranger.kms.gcp.location.id", VALID_LOCATION_ID);
        configuration.set("ranger.kms.gcp.keyring.id", VALID_KEYRING_ID);
        configuration.set("ranger.kms.gcp.masterkey.name", VALID_MASTER_KEY_NAME);
        configuration.set("ranger.kms.gcp.cred.file", VALID_CRED_FILE);
    }

    private SecretKey generateTestKey() throws NoSuchAlgorithmException {
        KeyGenerator keyGenerator = KeyGenerator.getInstance("AES");
        keyGenerator.init(256);
        return keyGenerator.generateKey();
    }
}
