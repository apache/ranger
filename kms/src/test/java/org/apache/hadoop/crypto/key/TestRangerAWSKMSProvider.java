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
        byte[] ciphertext    = {0, 1, 2, 3, 4};
        byte[] expectedPlain = {10, 11, 12};

        AWSKMS kmsMock = mock(AWSKMS.class);
        DecryptResult decryptResult = new DecryptResult()
                .withPlaintext(ByteBuffer.wrap(expectedPlain));
        when(kmsMock.decrypt(Mockito.any(DecryptRequest.class)))
                .thenReturn(decryptResult);

        Path configDir = Paths.get("src/test/resources/kms");
        System.setProperty(KMSConfiguration.KMS_CONFIG_DIR, configDir.toFile().getAbsolutePath());

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
        assertNotNull(clientField, "AWSKMS client field not found!");
        clientField.setAccessible(true);
        clientField.set(provider, kmsMock);

        byte[] actualPlain = provider.decryptZoneKey(ciphertext);
        assertArrayEquals(expectedPlain, actualPlain,
                "Returned plaintext must match stubbed value");

        verify(kmsMock, times(1))
                .decrypt(Mockito.any(DecryptRequest.class));
    }

    @Test
    void testEncryptZoneKey_happyPath() throws Exception {
        byte[] plainBytes     = {10, 11, 12, 13, 14, 15};
        Key    zoneKey        = new SecretKeySpec(plainBytes, "AES");
        byte[] expectedCipher = {1, 2, 3, 4};

        AWSKMS kmsMock = mock(AWSKMS.class);
        EncryptResult encrypted = new EncryptResult()
                .withCiphertextBlob(ByteBuffer.wrap(expectedCipher));
        when(kmsMock.encrypt(any(EncryptRequest.class))).thenReturn(encrypted);

        Path configDir = Paths.get("src/test/resources/kms");
        System.setProperty(KMSConfiguration.KMS_CONFIG_DIR, configDir.toFile().getAbsolutePath());

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

        byte[] actualCipher = provider.encryptZoneKey(zoneKey);

        assertArrayEquals(expectedCipher, actualCipher,
                "Returned ciphertext must match stubbed value");
        verify(kmsMock, times(1)).encrypt(any(EncryptRequest.class));
    }

    @Test
    void testGenerateMasterKey_success() throws Exception {
        String password    = "testPassword";
        String masterKeyId = "your-master-key-id";

        KeyMetadata keyMetadata = new KeyMetadata()
                .withKeyId(masterKeyId)
                .withArn("dummy-arn")
                .withDescription("test key");
        DescribeKeyResult describeKeyResult = new DescribeKeyResult()
                .withKeyMetadata(keyMetadata);

        AliasListEntry aliasEntry = new AliasListEntry()
                .withAliasName(masterKeyId)
                .withTargetKeyId(masterKeyId);
        ListAliasesResult aliasesResult = new ListAliasesResult()
                .withAliases(aliasEntry);

        AWSKMS kmsMock = mock(AWSKMS.class);
        when(kmsMock.describeKey(any(DescribeKeyRequest.class))).thenReturn(describeKeyResult);
        when(kmsMock.listAliases(any(ListAliasesRequest.class))).thenReturn(aliasesResult);

        Configuration conf = new Configuration(false);
        conf.set("ranger.kms.awskms.masterkey.id", masterKeyId);
        conf.set("ranger.kms.aws.client.accesskey", "dummy-access-key");
        conf.set("ranger.kms.aws.client.secretkey", "dummy-secret-key");
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

        boolean result = provider.generateMasterKey(password);
        assertTrue(result, "Expected generateMasterKey to return true");

        verify(kmsMock, times(1)).describeKey(any(DescribeKeyRequest.class));
        verify(kmsMock, times(1)).listAliases(any(ListAliasesRequest.class));
    }
}
