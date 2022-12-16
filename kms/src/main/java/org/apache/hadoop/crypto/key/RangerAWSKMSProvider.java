/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.crypto.key;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.kms.AWSKMS;
import com.amazonaws.services.kms.AWSKMSClientBuilder;
import com.amazonaws.services.kms.model.KeyMetadata;
import com.amazonaws.services.kms.model.DescribeKeyRequest;
import com.amazonaws.services.kms.model.DescribeKeyResult;
import com.amazonaws.services.kms.model.ListAliasesRequest;
import com.amazonaws.services.kms.model.ListAliasesResult;
import com.amazonaws.services.kms.model.AliasListEntry;
import com.amazonaws.services.kms.model.EncryptRequest;
import com.amazonaws.services.kms.model.EncryptResult;
import com.amazonaws.services.kms.model.DecryptRequest;
import com.amazonaws.services.kms.model.DecryptResult;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.security.Key;

public class RangerAWSKMSProvider implements RangerKMSMKI {
    private static final Logger logger = LoggerFactory.getLogger(RangerAWSKMSProvider.class);

    static final String AWSKMS_MASTER_KEY_ID = "ranger.kms.awskms.masterkey.id";
    static final String AWS_CLIENT_ACCESSKEY = "ranger.kms.aws.client.accesskey";
    static final String AWS_CLIENT_SECRETKEY = "ranger.kms.aws.client.secretkey";
    static final String AWS_CLIENT_REGION    = "ranger.kms.aws.client.region";

    private String      masterKeyId;
    private KeyMetadata masterKeyMetadata;
    private AWSKMS      keyVaultClient;

    protected RangerAWSKMSProvider(Configuration conf, AWSKMS client) {
        this.masterKeyId    = conf.get(AWSKMS_MASTER_KEY_ID);
        this.keyVaultClient = client;
    }

    public RangerAWSKMSProvider(Configuration conf) throws Exception {
        this(conf, createKMSClient(conf));
    }

    public static AWSKMS createKMSClient(Configuration conf) throws Exception {
        String awskmsClientAccessKey = conf.get(AWS_CLIENT_ACCESSKEY);
        String awskmsClientSecretKey = conf.get(AWS_CLIENT_SECRETKEY);
        String awskmsClientRegion    = conf.get(AWS_CLIENT_REGION);

        AWSKMSClientBuilder builder = AWSKMSClientBuilder.standard();

        if (StringUtils.isNotEmpty(awskmsClientAccessKey) && StringUtils.isNotEmpty(awskmsClientSecretKey)) {
            builder.withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(awskmsClientAccessKey, awskmsClientSecretKey)));
        }

        if (StringUtils.isNotEmpty(awskmsClientRegion)) {
            builder.withRegion(awskmsClientRegion);
        }

        return builder.build();
    }

    @Override
    public boolean generateMasterKey(String password) throws Exception {
        if (keyVaultClient == null) {
            throw new Exception("Key Vault Client is null. Please check the aws related configuration.");
        }

        DescribeKeyRequest desckey_req = new DescribeKeyRequest();

        desckey_req.setKeyId(masterKeyId);

        DescribeKeyResult desckey_resp = keyVaultClient.describeKey(desckey_req);

        if (desckey_resp == null) {
            throw new Exception("Fetch KeyMetadata by describeKey failed");
        }

        { // verify whether alias or id match
            ListAliasesRequest listAliasesRequest = new ListAliasesRequest();

            listAliasesRequest.setKeyId(desckey_resp.getKeyMetadata().getKeyId());

            ListAliasesResult listAliasesResult = keyVaultClient.listAliases(listAliasesRequest);
            boolean           aliasMatched      = false;

            if (listAliasesResult != null) {
                for (AliasListEntry e : listAliasesResult.getAliases()) {
                    logger.info("keyalias: " + e);

                    if (e.getAliasName().equals(masterKeyId) && e.getTargetKeyId().equals(desckey_resp.getKeyMetadata().getKeyId())) {
                        aliasMatched = true;

                        break;
                    }
                }
            }

            if (!aliasMatched && !desckey_resp.getKeyMetadata().getKeyId().equals(masterKeyId)) {
                throw new Exception("KeyMetadata do not match masterKeyId");
            }
        }

        masterKeyMetadata = desckey_resp.getKeyMetadata();

        if (masterKeyMetadata == null) {
            throw new NoSuchMethodException("generateMasterKey is not implemented for AWS KMS");
        } else {
            logger.info("AWS Master key exist with KeyId: " + masterKeyId
                    + " with Arn: " + masterKeyMetadata.getArn()
                    + " with Description : " + masterKeyMetadata.getDescription());

            return true;
        }
    }

    @Override
    public byte[] encryptZoneKey(Key zoneKey) throws Exception {
        EncryptRequest req = new EncryptRequest();

        req.setKeyId(this.masterKeyId);
        req.setPlaintext(ByteBuffer.wrap(zoneKey.getEncoded()));

        EncryptResult resp = keyVaultClient.encrypt(req);
        ByteBuffer    buf  = resp.getCiphertextBlob();
        byte[]        arr  = new byte[buf.remaining()];

        buf.get(arr);

        return arr;
    }

    @Override
    public byte[] decryptZoneKey(byte[] encryptedByte) throws Exception {
        DecryptRequest req = new DecryptRequest();

        req.setCiphertextBlob(ByteBuffer.wrap(encryptedByte));

        DecryptResult resp = keyVaultClient.decrypt(req);
        ByteBuffer    buf  = resp.getPlaintext();
        byte[]        arr  = new byte[buf.remaining()];

        buf.get(arr);

        return arr;
    }

    @Override
    public String getMasterKey(String masterKeySecretName) {
        /*
         * This method is not require for AWS KMS because we can't get
         * key outside of KMS
         */
        return null;
    }
}
