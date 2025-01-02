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

package org.apache.hadoop.crypto.key;

import com.sun.org.apache.xml.internal.security.utils.Base64;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.bouncycastle.crypto.RuntimeCryptoException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.security.Key;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;

/**
 * This Class is for HSM Keystore
 */
public class RangerHSM implements RangerKMSMKI {
    static final Logger logger = LoggerFactory.getLogger(RangerHSM.class);

    private static final String MK_CIPHER          = "AES";
    private static final int    MK_KeySize         = 128;
    private static final String PARTITION_PASSWORD = "ranger.ks.hsm.partition.password";
    private static final String PARTITION_NAME     = "ranger.ks.hsm.partition.name";
    private static final String HSM_TYPE           = "ranger.ks.hsm.type";
    private static final String ALIAS              = "RangerKMSKey";

    private KeyStore myStore;
    private String   hsmKeystore;

    public RangerHSM() {
    }

    public RangerHSM(Configuration conf) {
        logger.info("RangerHSM provider");

        /*
         * We will log in to the HSM
         */
        String passwd        = conf.get(PARTITION_PASSWORD);
        String partitionName = conf.get(PARTITION_NAME);
        String errorMsg      = StringUtils.EMPTY;

        hsmKeystore = conf.get(HSM_TYPE);

        try {
            ByteArrayInputStream is1 = new ByteArrayInputStream(("tokenlabel:" + partitionName).getBytes());

            logger.debug("Loading HSM : Tokenlabel - '{}', Type - '{}' ", partitionName, hsmKeystore);

            myStore = KeyStore.getInstance("Luna");

            if (myStore == null) {
                logger.error("Luna not found. Please verify the Ranger KMS HSM configuration setup.");
            } else {
                myStore.load(is1, passwd.toCharArray());
            }
        } catch (KeyStoreException kse) {
            errorMsg = "Unable to create keystore object : " + kse.getMessage();
        } catch (NoSuchAlgorithmException nsae) {
            errorMsg = "Unexpected NoSuchAlgorithmException while loading keystore : " + nsae.getMessage();
        } catch (CertificateException e) {
            errorMsg = "Unexpected CertificateException while loading keystore : " + e.getMessage();
        } catch (IOException e) {
            errorMsg = "Unexpected IOException while loading keystore : " + e.getMessage();
        }

        if (StringUtils.isNotEmpty(errorMsg)) {
            throw new RuntimeCryptoException(errorMsg);
        }
    }

    @Override
    public boolean generateMasterKey(String password) throws Throwable {
        logger.debug("==> RangerHSM.generateMasterKey()");

        if (!this.myStore.containsAlias(ALIAS)) {
            try {
                logger.info("Generating AES Master Key for '{}' HSM Provider", hsmKeystore);

                KeyGenerator keyGen = KeyGenerator.getInstance(MK_CIPHER, hsmKeystore);

                keyGen.init(MK_KeySize);

                SecretKey aesKey = keyGen.generateKey();

                myStore.setKeyEntry(ALIAS, aesKey, password.toCharArray(), (java.security.cert.Certificate[]) null);

                return true;
            } catch (Exception e) {
                logger.error("generateMasterKey : Exception during Ranger Master Key Generation - {}", e.getMessage());
            }
        } else {
            logger.info("Master key with alias - '{}' already exists!", ALIAS);
        }

        logger.debug("<== RangerHSM.generateMasterKey()");

        return false;
    }

    @Override
    public String getMasterKey(String password) throws Throwable {
        logger.debug("==> RangerHSM.getMasterKey()");

        if (myStore != null) {
            try {
                logger.debug("Searching for Ranger Master Key in Luna Keystore");

                boolean result = myStore.containsAlias(ALIAS);

                if (result) {
                    logger.debug("Ranger Master Key is present in Keystore");

                    SecretKey key = (SecretKey) myStore.getKey(ALIAS, password.toCharArray());

                    return Base64.encode(key.getEncoded());
                }
            } catch (Exception e) {
                logger.error("getMasterKey : Exception searching for Ranger Master Key - {} ", e.getMessage());
            }
        }

        logger.debug("<== RangerHSM.getMasterKey()");

        return null;
    }

    public boolean setMasterKey(String password, byte[] key) {
        if (myStore != null) {
            try {
                Key aesKey = new SecretKeySpec(key, MK_CIPHER);

                myStore.setKeyEntry(ALIAS, aesKey, password.toCharArray(), (java.security.cert.Certificate[]) null);

                return true;
            } catch (KeyStoreException e) {
                logger.error("setMasterKey : Exception while setting Master Key, Error - {} ", e.getMessage());
            }
        }

        return false;
    }
}
