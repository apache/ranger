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

import com.amazonaws.services.kms.AWSKMS;
import com.amazonaws.services.kms.model.AliasListEntry;
import com.amazonaws.services.kms.model.DecryptRequest;
import com.amazonaws.services.kms.model.DecryptResult;
import com.amazonaws.services.kms.model.DescribeKeyRequest;
import com.amazonaws.services.kms.model.DescribeKeyResult;
import com.amazonaws.services.kms.model.EncryptRequest;
import com.amazonaws.services.kms.model.EncryptResult;
import com.amazonaws.services.kms.model.KeyMetadata;
import com.amazonaws.services.kms.model.ListAliasesRequest;
import com.amazonaws.services.kms.model.ListAliasesResult;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.key.kms.server.KMSConfiguration;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import javax.crypto.spec.SecretKeySpec;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.Key;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@TestMethodOrder(MethodOrderer.MethodName.class)
@Disabled
public class TestRangerAWSKMSProvider {
    @Test
    public void testCreateKMSClient() throws Exception {
        Path configDir = Paths.get("src/test/resources/kms");
        System.setProperty(KMSConfiguration.KMS_CONFIG_DIR, configDir.toFile().getAbsolutePath());
        Configuration conf = new Configuration();
        conf.set("ranger.kms.awskms.masterkey.id", "your-master-key-id");
        conf.set("ranger.kms.aws.client.accesskey", "your-access-key");
        conf.set("ranger.kms.aws.client.secretkey", "your-secret-key");
        conf.set("ranger.kms.aws.client.region", "us-west-2");

        AWSKMS               client      = RangerAWSKMSProvider.createKMSClient(conf);
        RangerAWSKMSProvider kmsProvider = new RangerAWSKMSProvider(conf, client);

        RangerAWSKMSProvider.createKMSClient(conf);
    }

    @Test
    public void testGetMasterKey() throws Exception {
        Path configDir = Paths.get("src/test/resources/kms");
        System.setProperty(KMSConfiguration.KMS_CONFIG_DIR, configDir.toFile().getAbsolutePath());
        Configuration conf = new Configuration();
        conf.set("ranger.kms.awskms.masterkey.id", "your-master-key-id");
        conf.set("ranger.kms.aws.client.accesskey", "your-access-key");
        conf.set("ranger.kms.aws.client.secretkey", "your-secret-key");
        conf.set("ranger.kms.aws.client.region", "us-west-2");

        RangerAWSKMSProvider kmsProvider         = new RangerAWSKMSProvider(conf);
        String               masterKeySecretName = conf.get("ranger.kms.awskms.masterkey.id");
        kmsProvider.getMasterKey(masterKeySecretName);
    }

    @Test
    void testDecryptZoneKey_happyPath() throws Exception {
        /* ---------- 1.  Test data --------------------------------------------------- */
        byte[] ciphertext    = {0, 1, 2, 3, 4};
        byte[] expectedPlain = {10, 11, 12};

        /* ---------- 2.  Stub AWSKMS.decrypt(...) ------------------------------------ */
        AWSKMS kmsMock = mock(AWSKMS.class);
        DecryptResult decryptResult = new DecryptResult()
                .withPlaintext(ByteBuffer.wrap(expectedPlain));
        when(kmsMock.decrypt(Mockito.any(DecryptRequest.class)))
                .thenReturn(decryptResult);

        Path configDir = Paths.get("src/test/resources/kms");
        System.setProperty(KMSConfiguration.KMS_CONFIG_DIR, configDir.toFile().getAbsolutePath());

        /* ---------- 3.  Build provider with regular ctor --------------------------- */
        Configuration conf = new Configuration();
        conf.set("ranger.kms.awskms.masterkey.id", "your-master-key-id");
        conf.set("ranger.kms.aws.client.accesskey", "your-access-key");
        conf.set("ranger.kms.aws.client.secretkey", "your-secret-key");
        conf.set("ranger.kms.aws.client.region", "us-west-2");
        RangerAWSKMSProvider provider = new RangerAWSKMSProvider(conf);

        /* ---------- 4.  Inject mock client via reflection -------------------------- */
        Field clientField = null;
        for (Field f : RangerAWSKMSProvider.class.getDeclaredFields()) {
            if (AWSKMS.class.isAssignableFrom(f.getType())) {
                clientField = f;
                break;
            }
        }
        assertNotNull(clientField, "AWSKMS client field not found!");
        clientField.setAccessible(true);
        clientField.set(provider, kmsMock);

        /* ---------- 5.  Call and assert -------------------------------------------- */
        byte[] actualPlain = provider.decryptZoneKey(ciphertext);
        assertArrayEquals(expectedPlain, actualPlain,
                "Returned plaintext must match stubbed value");

        /* ---------- 6.  Verify interaction ----------------------------------------- */
        verify(kmsMock, times(1))
                .decrypt(Mockito.any(DecryptRequest.class));
    }

    @Test
    void testEncryptZoneKey_happyPath() throws Exception {
        /* ---------- 1. Test data -------------------------------------------------- */
        byte[] plainBytes     = {10, 11, 12, 13, 14, 15};
        Key    zoneKey        = new SecretKeySpec(plainBytes, "AES");
        byte[] expectedCipher = {1, 2, 3, 4};

        /* ---------- 2. Stub AWSKMS.encrypt(...) ---------------------------------- */
        AWSKMS kmsMock = mock(AWSKMS.class);
        EncryptResult encrypted = new EncryptResult()
                .withCiphertextBlob(ByteBuffer.wrap(expectedCipher));
        when(kmsMock.encrypt(any(EncryptRequest.class))).thenReturn(encrypted);

        Path configDir = Paths.get("src/test/resources/kms");
        System.setProperty(KMSConfiguration.KMS_CONFIG_DIR, configDir.toFile().getAbsolutePath());

        /* ---------- 3. Build provider & inject mock client ----------------------- */
        Configuration conf = new Configuration();
        conf.set("ranger.kms.awskms.masterkey.id", "your-master-key-id");
        conf.set("ranger.kms.aws.client.accesskey", "your-access-key");
        conf.set("ranger.kms.aws.client.secretkey", "your-secret-key");
        conf.set("ranger.kms.aws.client.region", "us-west-2");
        RangerAWSKMSProvider provider = new RangerAWSKMSProvider(conf);

        Field clientField = null;
        for (Field f : RangerAWSKMSProvider.class.getDeclaredFields()) {
            if (AWSKMS.class.isAssignableFrom(f.getType())) {
                clientField = f;
                break;
            }
        }
        assertNotNull(clientField, "AWSKMS client field not found");
        clientField.setAccessible(true);
        clientField.set(provider, kmsMock);

        /* ---------- 4. Call encryptZoneKey(...) ---------------------------------- */
        byte[] actualCipher = provider.encryptZoneKey(zoneKey);

        /* ---------- 5. Assertions & verifications -------------------------------- */
        assertArrayEquals(expectedCipher, actualCipher,
                "Returned ciphertext must match stubbed value");
        verify(kmsMock, times(1)).encrypt(any(EncryptRequest.class));
    }

    @Test
    void testGenerateMasterKey_success() throws Exception {
        // Dummy inputs and expected values
        String password    = "testPassword";
        String masterKeyId = "your-master-key-id";

        // 1. Stub DescribeKey → returns DescribeKeyResult with KeyMetadata
        KeyMetadata keyMetadata = new KeyMetadata()
                .withKeyId(masterKeyId)
                .withArn("dummy-arn")
                .withDescription("test key");
        DescribeKeyResult describeKeyResult = new DescribeKeyResult()
                .withKeyMetadata(keyMetadata);

        // 2. Stub ListAliases → returns alias that matches masterKeyId
        AliasListEntry aliasEntry = new AliasListEntry()
                .withAliasName(masterKeyId)
                .withTargetKeyId(masterKeyId);
        ListAliasesResult aliasesResult = new ListAliasesResult()
                .withAliases(aliasEntry);

        // 3. Mock AWSKMS client
        AWSKMS kmsMock = mock(AWSKMS.class);
        when(kmsMock.describeKey(any(DescribeKeyRequest.class))).thenReturn(describeKeyResult);
        when(kmsMock.listAliases(any(ListAliasesRequest.class))).thenReturn(aliasesResult);

        // 4. Prepare configuration and inject mocked client
        Configuration conf = new Configuration(false);
        conf.set("ranger.kms.awskms.masterkey.id", masterKeyId);
        conf.set("ranger.kms.aws.client.accesskey", "dummy-access-key");
        conf.set("ranger.kms.aws.client.secretkey", "dummy-secret-key");
        conf.set("ranger.kms.aws.client.region", "us-west-2");

        RangerAWSKMSProvider provider = new RangerAWSKMSProvider(conf);

        // Inject the mocked AWSKMS client into provider
        Field clientField = null;
        for (Field f : RangerAWSKMSProvider.class.getDeclaredFields()) {
            if (AWSKMS.class.isAssignableFrom(f.getType())) {
                clientField = f;
                break;
            }
        }
        assertNotNull(clientField, "AWSKMS client field not found");
        clientField.setAccessible(true);
        clientField.set(provider, kmsMock);

        // 5. Call generateMasterKey and assert result
        boolean result = provider.generateMasterKey(password);
        assertTrue(result, "Expected generateMasterKey to return true");

        // 6. Verify calls
        verify(kmsMock, times(1)).describeKey(any(DescribeKeyRequest.class));
        verify(kmsMock, times(1)).listAliases(any(ListAliasesRequest.class));
    }
}
