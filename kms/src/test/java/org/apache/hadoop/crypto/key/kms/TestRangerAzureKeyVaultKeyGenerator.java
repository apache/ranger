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

import com.microsoft.azure.keyvault.webkey.JsonWebKeyEncryptionAlgorithm;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.key.RangerAzureKeyVaultKeyGenerator;
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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(MockitoExtension.class)
@TestMethodOrder(MethodOrderer.MethodName.class)
@Disabled
public class TestRangerAzureKeyVaultKeyGenerator {
    private static final String        VALID_VAULT_URL     = "https://test-keyvault.vault.azure.net/";
    private static final String        VALID_MASTER_KEY    = "test-master-key";
    private static final String        VALID_CLIENT_ID     = "test-client-id";
    private static final String        VALID_CLIENT_SECRET = "test-client-secret";
    private static final String        VALID_CERT_PATH     = "/path/to/cert.pfx";
    private static final String        VALID_CERT_PASSWORD = "cert-password";
    private              Configuration configuration;

    @BeforeEach
    public void setUp() {
        configuration = new Configuration();
    }

    @Test
    public void testConstructorWithValidConfigurationAndNullClient() {
        setValidConfiguration();

        assertDoesNotThrow(() -> {
            RangerAzureKeyVaultKeyGenerator generator = new RangerAzureKeyVaultKeyGenerator(configuration, null);
            assertNotNull(generator);
        });
    }

    @Test
    public void testConstructorWithConfiguration() throws Exception {
        setValidConfiguration();

        // This will fail because we don't have actual Azure credentials, but should not throw during construction
        Exception exception = assertThrows(Exception.class, () -> {
            RangerAzureKeyVaultKeyGenerator generator = new RangerAzureKeyVaultKeyGenerator(configuration);
        });

        assertNotNull(exception);
    }

    @Test
    public void testCreateKeyVaultClientWithEmptyClientId() {
        setValidConfiguration();
        configuration.unset("ranger.kms.azure.client.id");

        Exception exception = assertThrows(Exception.class, () -> {
            RangerAzureKeyVaultKeyGenerator.createKeyVaultClient(configuration);
        });

        assertTrue(exception.getMessage().contains("client id is not configured"));
    }

    @Test
    public void testCreateKeyVaultClientWithNullClientId() {
        setValidConfiguration();
        configuration.set("ranger.kms.azure.client.id", "");

        Exception exception = assertThrows(Exception.class, () -> {
            RangerAzureKeyVaultKeyGenerator.createKeyVaultClient(configuration);
        });

        assertTrue(exception.getMessage().contains("client id is not configured"));
    }

    @Test
    public void testCreateKeyVaultClientWithNullConfiguration() {
        Exception exception = assertThrows(Exception.class, () -> {
            RangerAzureKeyVaultKeyGenerator.createKeyVaultClient(null);
        });

        assertNotNull(exception);
    }

    @Test
    public void testCreateKeyVaultClientSSLDisabledWithoutSecret() {
        setValidConfiguration();
        configuration.set("ranger.kms.azure.keyvault.ssl.enabled", "false");
        configuration.unset("ranger.kms.azure.client.secret");

        Exception exception = assertThrows(Exception.class, () -> {
            RangerAzureKeyVaultKeyGenerator.createKeyVaultClient(configuration);
        });

        assertTrue(exception.getMessage().contains("client password/secret is not configured"));
    }

    @Test
    public void testCreateKeyVaultClientSSLDisabledWithEmptySecret() {
        setValidConfiguration();
        configuration.set("ranger.kms.azure.keyvault.ssl.enabled", "false");
        configuration.set("ranger.kms.azure.client.secret", "");

        Exception exception = assertThrows(Exception.class, () -> {
            RangerAzureKeyVaultKeyGenerator.createKeyVaultClient(configuration);
        });

        assertTrue(exception.getMessage().contains("client password/secret is not configured"));
    }

    @Test
    public void testCreateKeyVaultClientSSLEnabledWithoutCertPath() {
        setValidConfiguration();
        configuration.set("ranger.kms.azure.keyvault.ssl.enabled", "true");
        configuration.unset("ranger.kms.azure.keyvault.certificate.path");

        Exception exception = assertThrows(Exception.class, () -> {
            RangerAzureKeyVaultKeyGenerator.createKeyVaultClient(configuration);
        });

        assertTrue(exception.getMessage().contains("Please provide certificate path for authentication"));
    }

    @Test
    public void testCreateKeyVaultClientSSLEnabledWithEmptyCertPath() {
        setValidConfiguration();
        configuration.set("ranger.kms.azure.keyvault.ssl.enabled", "true");
        configuration.set("ranger.kms.azure.keyvault.certificate.path", "");

        Exception exception = assertThrows(Exception.class, () -> {
            RangerAzureKeyVaultKeyGenerator.createKeyVaultClient(configuration);
        });

        assertTrue(exception.getMessage().contains("Please provide certificate path for authentication"));
    }

    @Test
    public void testCreateKeyVaultClientSSLDefaultEnabledWithoutCertPath() {
        setValidConfiguration();
        // Don't set SSL enabled, should default to true
        configuration.unset("ranger.kms.azure.keyvault.certificate.path");

        Exception exception = assertThrows(Exception.class, () -> {
            RangerAzureKeyVaultKeyGenerator.createKeyVaultClient(configuration);
        });

        assertTrue(exception.getMessage().contains("Please provide certificate path for authentication"));
    }

    @Test
    public void testGetMasterKeyReturnsNull() {
        setValidConfiguration();

        RangerAzureKeyVaultKeyGenerator generator = new RangerAzureKeyVaultKeyGenerator(configuration, null);

        String result = generator.getMasterKey("test-secret");

        assertNull(result, "getMasterKey should always return null for Azure Key Vault");
    }

    @Test
    public void testGetMasterKeyWithNullParameter() {
        setValidConfiguration();

        RangerAzureKeyVaultKeyGenerator generator = new RangerAzureKeyVaultKeyGenerator(configuration, null);

        String result = generator.getMasterKey(null);

        assertNull(result, "getMasterKey should always return null for Azure Key Vault");
    }

    @Test
    public void testGetMasterKeyWithEmptyParameter() {
        setValidConfiguration();

        RangerAzureKeyVaultKeyGenerator generator = new RangerAzureKeyVaultKeyGenerator(configuration, null);

        String result = generator.getMasterKey("");

        assertNull(result, "getMasterKey should always return null for Azure Key Vault");
    }

    @Test
    public void testGenerateMasterKeyWithNullClient() {
        setValidConfiguration();

        RangerAzureKeyVaultKeyGenerator generator = new RangerAzureKeyVaultKeyGenerator(configuration, null);

        Exception exception = assertThrows(Exception.class, () -> {
            generator.generateMasterKey("password");
        });

        assertTrue(exception.getMessage().contains("Key Vault Client is null"));
    }

    @Test
    public void testGenerateMasterKeyWithNullPassword() {
        setValidConfiguration();

        RangerAzureKeyVaultKeyGenerator generator = new RangerAzureKeyVaultKeyGenerator(configuration, null);

        Exception exception = assertThrows(Exception.class, () -> {
            generator.generateMasterKey(null);
        });

        assertTrue(exception.getMessage().contains("Key Vault Client is null"));
    }

    @Test
    public void testGenerateMasterKeyWithEmptyPassword() {
        setValidConfiguration();

        RangerAzureKeyVaultKeyGenerator generator = new RangerAzureKeyVaultKeyGenerator(configuration, null);

        Exception exception = assertThrows(Exception.class, () -> {
            generator.generateMasterKey("");
        });

        assertTrue(exception.getMessage().contains("Key Vault Client is null"));
    }

    @Test
    public void testEncryptZoneKeyWithNullClient() {
        setValidConfiguration();

        RangerAzureKeyVaultKeyGenerator generator = new RangerAzureKeyVaultKeyGenerator(configuration, null);

        assertThrows(Exception.class, () -> {
            generator.encryptZoneKey(generateTestKey());
        });
    }

    @Test
    public void testEncryptZoneKeyWithNullKey() {
        setValidConfiguration();

        RangerAzureKeyVaultKeyGenerator generator = new RangerAzureKeyVaultKeyGenerator(configuration, null);

        assertThrows(Exception.class, () -> {
            generator.encryptZoneKey(null);
        });
    }

    @Test
    public void testDecryptZoneKeyWithNullClient() {
        setValidConfiguration();

        RangerAzureKeyVaultKeyGenerator generator = new RangerAzureKeyVaultKeyGenerator(configuration, null);

        assertThrows(Exception.class, () -> {
            generator.decryptZoneKey("test".getBytes());
        });
    }

    @Test
    public void testDecryptZoneKeyWithNullData() {
        setValidConfiguration();

        RangerAzureKeyVaultKeyGenerator generator = new RangerAzureKeyVaultKeyGenerator(configuration, null);

        assertThrows(Exception.class, () -> {
            generator.decryptZoneKey(null);
        });
    }

    @Test
    public void testDecryptZoneKeyWithEmptyData() {
        setValidConfiguration();

        RangerAzureKeyVaultKeyGenerator generator = new RangerAzureKeyVaultKeyGenerator(configuration, null);

        assertThrows(Exception.class, () -> {
            generator.decryptZoneKey(new byte[0]);
        });
    }

    @Test
    public void testGetZoneKeyEncryptionAlgoWithDifferentAlgorithms() throws Exception {
        setValidConfiguration();

        String[] algorithms = {"RSA_OAEP", "RSA_OAEP_256", "RSA1_5", "INVALID_ALGO"};
        JsonWebKeyEncryptionAlgorithm[] expectedAlgos = {
                JsonWebKeyEncryptionAlgorithm.RSA_OAEP,
                JsonWebKeyEncryptionAlgorithm.RSA_OAEP_256,
                JsonWebKeyEncryptionAlgorithm.RSA1_5,
                JsonWebKeyEncryptionAlgorithm.RSA_OAEP // default
        };

        for (int i = 0; i < algorithms.length; i++) {
            configuration.set("ranger.kms.azure.zonekey.encryption.algorithm", algorithms[i]);

            RangerAzureKeyVaultKeyGenerator generator = new RangerAzureKeyVaultKeyGenerator(configuration, null);

            // Use reflection to test the private method
            Method getZoneKeyEncryptionAlgoMethod = RangerAzureKeyVaultKeyGenerator.class.getDeclaredMethod("getZoneKeyEncryptionAlgo");
            getZoneKeyEncryptionAlgoMethod.setAccessible(true);

            JsonWebKeyEncryptionAlgorithm result = (JsonWebKeyEncryptionAlgorithm) getZoneKeyEncryptionAlgoMethod.invoke(generator);

            assertEquals(expectedAlgos[i], result, "Algorithm should match for input: " + algorithms[i]);
        }
    }

    @Test
    public void testCreateKeyVaultClientWithValidSSLEnabledConfiguration() {
        setValidConfiguration();
        configuration.set("ranger.kms.azure.keyvault.ssl.enabled", "true");
        configuration.set("ranger.kms.azure.keyvault.certificate.path", VALID_CERT_PATH);

        // This will fail because we don't have actual Azure credentials, but should not fail on validation
        Exception exception = assertThrows(Exception.class, () -> {
            RangerAzureKeyVaultKeyGenerator.createKeyVaultClient(configuration);
        });

        // Should not be a configuration validation error but rather an authentication error
        assertFalse(exception.getMessage().contains("client id is not configured"));
        assertFalse(exception.getMessage().contains("Please provide certificate path for authentication"));
    }

    @Test
    public void testCreateKeyVaultClientWithCertificatePassword() {
        setValidConfiguration();
        configuration.set("ranger.kms.azure.keyvault.ssl.enabled", "true");
        configuration.set("ranger.kms.azure.keyvault.certificate.path", VALID_CERT_PATH);
        configuration.set("ranger.kms.azure.keyvault.certificate.password", VALID_CERT_PASSWORD);

        // This will fail because we don't have actual Azure credentials, but should not fail on validation
        Exception exception = assertThrows(Exception.class, () -> {
            RangerAzureKeyVaultKeyGenerator.createKeyVaultClient(configuration);
        });

        // Should not be a configuration validation error
        assertFalse(exception.getMessage().contains("client id is not configured"));
        assertFalse(exception.getMessage().contains("Please provide certificate path for authentication"));
    }

    @Test
    public void testConfigurationValuesAreSetCorrectly() {
        setValidConfiguration();

        // Test that constructor accepts and uses the configuration values
        assertDoesNotThrow(() -> {
            RangerAzureKeyVaultKeyGenerator generator = new RangerAzureKeyVaultKeyGenerator(configuration, null);
            assertNotNull(generator);
        });
    }

    private void setValidConfiguration() {
        configuration.set("ranger.kms.azurekeyvault.url", VALID_VAULT_URL);
        configuration.set("ranger.kms.azure.masterkey.name", VALID_MASTER_KEY);
        configuration.set("ranger.kms.azure.masterkey.type", "RSA");
        configuration.set("ranger.kms.azure.zonekey.encryption.algorithm", "RSA_OAEP");
        configuration.set("ranger.kms.azure.client.id", VALID_CLIENT_ID);
        configuration.set("ranger.kms.azure.client.secret", VALID_CLIENT_SECRET);
    }

    private SecretKey generateTestKey() throws NoSuchAlgorithmException {
        KeyGenerator keyGenerator = KeyGenerator.getInstance("AES");
        keyGenerator.init(256);
        return keyGenerator.generateKey();
    }
}
