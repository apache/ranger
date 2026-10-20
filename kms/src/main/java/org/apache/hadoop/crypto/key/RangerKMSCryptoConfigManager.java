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

import org.apache.hadoop.crypto.key.common.KeySpecStrategy;
import org.apache.hadoop.crypto.key.common.RangerCipherSuite;
import org.apache.hadoop.crypto.key.common.RangerCryptoKDFSuite;
import org.apache.hadoop.crypto.key.common.RangerKMSCryptoConfigApi;
import org.apache.hadoop.crypto.key.common.RangerKMSKeyCryptoAPI;
import org.apache.hadoop.crypto.key.common.SaltGenerationStrategy;
import org.apache.hadoop.crypto.key.common.SupportedCipherSuite;
import org.apache.hadoop.crypto.key.common.SupportedPBECryptoKDFSuite;
import org.apache.ranger.plugin.util.XMLUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.KeyStore;
import java.util.Optional;
import java.util.Properties;

import static org.apache.hadoop.crypto.key.RangerKMSCryptoConfigManager.KMSCryptoConfigParams.RANGER_KMS_MD_ALGO;
import static org.apache.hadoop.crypto.key.RangerKMSCryptoConfigManager.KMSCryptoConfigParams.RANGER_KMS_MK_CRYPTO_ALGO;
import static org.apache.hadoop.crypto.key.RangerKMSCryptoConfigManager.KMSCryptoConfigParams.RANGER_KMS_MK_PWD_CIPHER;
import static org.apache.hadoop.crypto.key.RangerKMSCryptoConfigManager.KMSCryptoConfigParams.RANGER_KMS_MK_PWD_ITERATION_COUNT;
import static org.apache.hadoop.crypto.key.RangerKMSCryptoConfigManager.KMSCryptoConfigParams.RANGER_KMS_MK_PWD_SALT;
import static org.apache.hadoop.crypto.key.RangerKMSCryptoConfigManager.KMSCryptoConfigParams.RANGER_KMS_MK_PWD_SALT_SIZE;
import static org.apache.hadoop.crypto.key.RangerKMSCryptoConfigManager.KMSCryptoConfigParams.RANGER_KMS_MK_PWD_SIZE;
import static org.apache.hadoop.crypto.key.common.RangerKMSKeyCryptoAPI.KMSCryptoParams.KMSCryptoParamsBuilder;

public class RangerKMSCryptoConfigManager implements RangerKMSCryptoConfigApi {
    private static final Logger logger = LoggerFactory.getLogger(RangerKMSCryptoConfigManager.class);

    private       String        cryptoConfigFile;
    private final Properties    serverConfigProperties = new Properties();

    protected RangerCryptoKDFSuite cryptoKDF;
    protected RangerCipherSuite cipher;

    protected  int     keySize;
    protected  Integer saltSize;
    protected  String salt;
    protected  String  mdAlgo;
    protected  Integer iterationCount;

    public RangerKMSCryptoConfigManager(String configFile) {
        this.cryptoConfigFile = configFile;
        init();
    }

    // This default constructor is being provided only to be overridden by Test implementations.
    protected RangerKMSCryptoConfigManager() {}

    @Override
    public RangerCipherSuite getCipher() {
        return this.cipher;
    }

    @Override
    public int getKeySize() {
        return this.keySize;
    }

    @Override
    public int getSaltSize() {
        return this.saltSize;
    }

    @Override
    public String getSalt() {
        return this.salt;
    }

    @Override
    public RangerCryptoKDFSuite getCryptoAlgorithm() {
        return this.cryptoKDF;
    }

    @Override
    public String getMessageDigestAlgorithm() {
        return this.mdAlgo;
    }

    @Override
    public int getIterationCount() {
        return this.iterationCount;
    }

    private void init() {
        logger.debug("==> RangerKMSCryptoConfigManager.init()");

        XMLUtils.loadConfig(cryptoConfigFile, serverConfigProperties);

        String pbeAlgo      = getConfig(RANGER_KMS_MK_CRYPTO_ALGO.getPropKey(), RANGER_KMS_MK_CRYPTO_ALGO.getStringDefaultValue());
        cryptoKDF           = SupportedPBECryptoKDFSuite.convert(pbeAlgo);

        String strCipher    = getConfig(RANGER_KMS_MK_PWD_CIPHER.getPropKey(), RANGER_KMS_MK_PWD_CIPHER.getStringDefaultValue());
        this.cipher         = SupportedCipherSuite.convert(strCipher);

        keySize             = getIntConfig(RANGER_KMS_MK_PWD_SIZE.getPropKey(), RANGER_KMS_MK_PWD_SIZE.getIntDefaultValue());
        saltSize            = RangerKMSCryptoConfigApi.calculateCompliantSaltSize(getIntConfig(RANGER_KMS_MK_PWD_SALT_SIZE.getPropKey(), RANGER_KMS_MK_PWD_SALT_SIZE.getIntDefaultValue()), cryptoKDF);
        salt                = getConfig(RANGER_KMS_MK_PWD_SALT.getPropKey(), RANGER_KMS_MK_PWD_SALT.getStringDefaultValue());
        mdAlgo              = getConfig(RANGER_KMS_MD_ALGO.getPropKey(), RANGER_KMS_MD_ALGO.getStringDefaultValue());
        iterationCount      = getIntConfig(RANGER_KMS_MK_PWD_ITERATION_COUNT.getPropKey(), RANGER_KMS_MK_PWD_ITERATION_COUNT.getIntDefaultValue());

        logger.info("Selected MD_ALGO={}", mdAlgo);
        logger.info("Selected CRYPTO_KDF_ALGO={}", cryptoKDF);
        logger.info("Selected CRYPTO_CIPHER={}", cipher.getCipherTransformation());
        logger.info("<== RangerMasterKey.init()");
    }

    public String getConfig(String key, String defaultValue) {
        String value = serverConfigProperties.getProperty(key);

        if (value == null || value.trim().isEmpty()) {
            //value not found in properties file, let's try to get from system property
            value = System.getProperty(key);
        }

        if (value == null || value.trim().isEmpty()) {
            value = defaultValue;
        }

        return value;
    }

    public int getIntConfig(String key, int defaultValue) {
        int    ret    = defaultValue;
        String retStr = serverConfigProperties.getProperty(key);

        try {
            if (retStr != null) {
                ret = Integer.parseInt(retStr);
            }
        } catch (Exception err) {
            logger.warn("Key can not be parsed to int due to NumberFormatException");
        }

        return ret;
    }

    enum KMSCryptoConfigParams
    {
        RANGER_KS_FILE_TYPE("ranger.keystore.file.type", KeyStore.getDefaultType()),
        RANGER_KMS_MK_PWD_CIPHER("ranger.kms.service.masterkey.password.cipher", "AES/CTR/NoPadding"),
        RANGER_KMS_MK_PWD_SIZE("ranger.kms.service.masterkey.password.size", 256),
        RANGER_KMS_MK_PWD_SALT_SIZE("ranger.kms.service.masterkey.password.salt.size", 8),
        RANGER_KMS_MK_PWD_SALT("ranger.kms.service.masterkey.password.salt", "abcdefghijklmnopqrstuvwxyz01234567890"),
        RANGER_KMS_MK_CRYPTO_ALGO("ranger.kms.service.masterkey.password.encryption.algorithm", SupportedPBECryptoKDFSuite.PBKDF2WITHHMACSHA256.toString()),
        RANGER_KMS_MD_ALGO("ranger.kms.service.masterkey.password.md.algorithm", "SHA-512"),
        RANGER_KMS_MK_PWD_ITERATION_COUNT("ranger.kms.service.masterkey.password.iteration.count", 1000);

        private String propKey;

        private Optional<String> propStrDefaultVal;

        private Optional<Integer> propIntDefaultVal;

        KMSCryptoConfigParams(String propKey, String strDefaultVal) {
            this.propKey = propKey;
            this.propStrDefaultVal = Optional.of(strDefaultVal);
            this.propIntDefaultVal = Optional.empty();
        }

        KMSCryptoConfigParams(String propKey, int intDefaultVal) {
            this.propKey = propKey;
            this.propIntDefaultVal = Optional.of(intDefaultVal);
            this.propStrDefaultVal = Optional.empty();
        }

        public String getPropKey() {
            return this.propKey;
        }

        public String getStringDefaultValue() {
            return this.propStrDefaultVal.get();
        }

        public int getIntDefaultValue() {
            return this.propIntDefaultVal.get();
        }
    }

    public KMSCryptoParamsBuilder prepareCryptoParamsBuilder(SaltGenerationStrategy saltGenerationStrategy, KeySpecStrategy keySpecStrategy) {
        KMSCryptoParamsBuilder builder = new KMSCryptoParamsBuilder(saltGenerationStrategy, keySpecStrategy);
        return builder.kdfAlgo(this.getCryptoAlgorithm())
                .cipher(this.getCipher())
                .keySize(this.getKeySize())
                .mdAlgo(this.getMessageDigestAlgorithm())
                .iterationCount(this.getIterationCount())
                .saltSeed(this.getSalt())
                .saltSize(this.getSaltSize());
    }

    @Override
    public RangerKMSKeyCryptoAPI.KMSCryptoParams getCryptoParams() {
        KeySpecStrategy keySpecStrategy = this.getCryptoAlgorithm().getKeyLength().isPresent() ? KeySpecStrategy.PASSWORD_SALT_ITERATIONCOUNT_KEYLENGTH : KeySpecStrategy.PASSWORD_SALT_ITERATIONCOUNT;
        return  prepareCryptoParamsBuilder(SaltGenerationStrategy.DETERMINISTIC, keySpecStrategy)
                .build();
    }
}
