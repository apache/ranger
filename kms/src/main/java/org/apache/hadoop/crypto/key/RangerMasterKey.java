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

import com.google.common.annotations.VisibleForTesting;
import com.sun.org.apache.xml.internal.security.utils.Base64;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.crypto.key.common.KeyEncryptor;
import org.apache.hadoop.crypto.key.common.KeySpecStrategy;
import org.apache.hadoop.crypto.key.common.RangerCipherSuite;
import org.apache.hadoop.crypto.key.common.RangerCryptoKDFSuite;
import org.apache.hadoop.crypto.key.common.RangerKMSCryptoConfigApi;
import org.apache.hadoop.crypto.key.common.RangerKMSCryptoException;
import org.apache.hadoop.crypto.key.common.RangerKMSCryptoManager;
import org.apache.hadoop.crypto.key.common.RangerKMSKeyCryptoAPI;
import org.apache.hadoop.crypto.key.common.SaltGenerationStrategy;
import org.apache.hadoop.crypto.key.common.SupportedCipherSuite;
import org.apache.hadoop.crypto.key.common.SupportedPBECryptoKDFSuite;
import org.apache.hadoop.thirdparty.com.google.common.base.Joiner;
import org.apache.hadoop.thirdparty.com.google.common.base.Splitter;
import org.apache.hadoop.thirdparty.com.google.common.collect.Lists;
import org.apache.ranger.entity.XXRangerMasterKey;
import org.apache.ranger.kms.dao.DaoManager;
import org.apache.ranger.kms.dao.RangerMasterKeyDao;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.apache.hadoop.crypto.key.RangerKMSCryptoConfigManager.KMSCryptoConfigParams;
import static org.apache.hadoop.crypto.key.common.RangerKMSKeyCryptoAPI.KMSCryptoParams.KMSCryptoParamsBuilder;

public class RangerMasterKey implements RangerKMSMKI {
    private static final Logger logger = LoggerFactory.getLogger(RangerMasterKey.class);

    public static final int         PADDING_STRING_ELEM_COUNT           = 7;
    private static final int        PADDING_STRING_ELEM_COUNT_WITH_IV  = 9;
    private  static final String    MK_META_INFO_SEPARATOR     = ",";
    private  static final RangerCryptoKDFSuite LEGACY_DEFAULT_CRYPTO_ALGO = SupportedPBECryptoKDFSuite.PBEWITHMD5ANDTRIPLEDES;

    // Right now, it is fixed as there is only one MK (or it's versions)
    // Once MK rotation is supported, MK_AAD should use the corresponding MK version, Like MasterKey@1
    // MK_AAD should be unique.
    public  static final byte[]   MK_AAD_FOR_GCM_MODE    = "ApacheRangerMasterKey".getBytes(StandardCharsets.UTF_8);

    public RangerCipherSuite        mkCipher;
    public Integer                  mkKeySize = 0;
    public   Integer                saltSize  = 0;
    public   int                    saltSizeFromDB;
    public   String                 salt;
    public   String                 iv;
    public   RangerCryptoKDFSuite   pbeKDFAlgo;
    public   String                 mdAlgo;
    public   Integer                iterationCount = 0;
    public   String                 paddingString;
    private  String                 password;

    private final RangerMasterKeyDao masterKeyDao;

    private final RangerKMSCryptoConfigApi cryptoConfigApi;

    private final RangerKMSKeyCryptoAPI kmsKeyCryptoAPI;

    public RangerMasterKey(DaoManager daoManager, RangerKMSCryptoConfigApi kmsCryptoConfigApi) {
        this.masterKeyDao = daoManager != null ? daoManager.getRangerMasterKeyDao() : null;
        this.cryptoConfigApi = kmsCryptoConfigApi;
        this.kmsKeyCryptoAPI = new RangerKMSCryptoManager(kmsCryptoConfigApi);
        paddingString = prepareMKMetaInfoInitials(this.cryptoConfigApi.getCryptoParams());
    }

    public String prepareMKMetaInfoInitials(RangerKMSKeyCryptoAPI.KMSCryptoParams cryptoParams) {
        return Joiner.on(MK_META_INFO_SEPARATOR).skipNulls()
                .join(cryptoParams.getCipher(), cryptoParams.getKeySize(), cryptoParams.getSaltSize(),
                        cryptoParams.getKDFAlgo().toString(), cryptoParams.getMdAlgo(), cryptoParams.getIterationCount(),
                        cryptoParams.getSalt());
    }

    private String buildMKMetaInfo(byte[] iv, byte[] encryptedKeyBytes) {
        return new StringBuilder(paddingString).append(MK_META_INFO_SEPARATOR)
                .append(null != iv ? Base64.encode(iv) : "")
                .append(MK_META_INFO_SEPARATOR)
                .append(Base64.encode(encryptedKeyBytes))
                .toString();
    }

    public void getPasswordParam(String paddedEncryptedPwd) {
        String[] encryptedPwd = null;

        if (paddedEncryptedPwd != null && paddedEncryptedPwd.contains(",")) {
            encryptedPwd = Lists.newArrayList(Splitter.on(",").split(paddedEncryptedPwd)).toArray(new String[0]);
        }

        if (encryptedPwd != null && encryptedPwd.length >= PADDING_STRING_ELEM_COUNT) {
            int index = 0;

            mkCipher            = SupportedCipherSuite.convert(encryptedPwd[index]);
            mkKeySize           = Integer.parseInt(encryptedPwd[++index]);
            saltSizeFromDB  = Integer.parseInt(encryptedPwd[++index]);
            pbeKDFAlgo          = SupportedPBECryptoKDFSuite.convert(encryptedPwd[++index]);
            saltSize            = RangerKMSCryptoConfigApi.calculateCompliantSaltSize(saltSizeFromDB, pbeKDFAlgo);
            mdAlgo              = encryptedPwd[++index];
            iterationCount      = Integer.parseInt(encryptedPwd[++index]);
            salt                = encryptedPwd[++index];
            if (encryptedPwd.length >= PADDING_STRING_ELEM_COUNT_WITH_IV) {
                String tempIv   = encryptedPwd[++index];
                iv              = StringUtils.isEmpty(tempIv) ? null : tempIv;
            }
            password       = encryptedPwd[++index];
        } else {
            mkCipher  = SupportedCipherSuite.convert("AES");
            mkKeySize = KMSCryptoConfigParams.RANGER_KMS_MK_PWD_SIZE.getIntDefaultValue();
            saltSize   = KMSCryptoConfigParams.RANGER_KMS_MK_PWD_SALT_SIZE.getIntDefaultValue();
            pbeKDFAlgo = LEGACY_DEFAULT_CRYPTO_ALGO;
            mdAlgo     = KMSCryptoConfigParams.RANGER_KMS_MD_ALGO.getStringDefaultValue();
            password  = paddedEncryptedPwd;
            salt      = password;
            iv        = null;

            if (password != null) {
                iterationCount = password.toCharArray().length + 1;
            }
        }
    }

    public SecretKey getMasterSecretKey(String password) throws Throwable {
        logger.debug("==> RangerMasterKey.getMasterSecretKey()");
        logger.info("Getting Master Key");

        List   result              = getEncryptedMK();
        String encryptedPassString = null;
        byte[] masterKeyByte       = null;

        if (CollectionUtils.isNotEmpty(result) && result.size() == 2) {
            masterKeyByte       = (byte[]) result.get(0);
            encryptedPassString = (String) result.get(1);
        } else if (CollectionUtils.isNotEmpty(result)) {
            masterKeyByte = (byte[]) result.get(0);
        }

        if (masterKeyByte != null && masterKeyByte.length > 0) {
            logger.debug("<== RangerMasterKey.getMasterSecretKey()");

            return decryptMasterKeySK(masterKeyByte, password, encryptedPassString);
        } else {
            throw new Exception("No Master Key Found");
        }
    }

    /**
     * Generate the master key, encrypt it and save it in the database
     *
     * @return true if the master key was successfully created false if master
     * key generation was unsuccessful or the master key already exists
     */
    @Override
    public boolean generateMasterKey(String password) throws Throwable {
        logger.debug("==> RangerMasterKey.generateMasterKey()");
        logger.info("Generating Master Key...");

        if (!checkMKExistence(this.masterKeyDao)) {
            logger.info("Master Key doesn't exist in DB, Generating the Master Key");

            KeyEncryptor.EncryptKeyResponse<byte[]> response = this.kmsKeyCryptoAPI.generateAndEncryptKey(password, Optional.of(MK_AAD_FOR_GCM_MODE));

            String mkToDB = buildMKMetaInfo(response.getIv(), response.getEncryptedContent());

            String savedKey           = saveEncryptedMK(mkToDB);

            if (savedKey != null && !savedKey.trim().equals("")) {
                logger.debug("Master Key Created with id = {}", savedKey);
                logger.debug("<== RangerMasterKey.generateMasterKey()");

                return true;
            }
        } else {
            logger.debug("Ranger Master Key already exists in the DB, returning.");
        }

        logger.debug("<== RangerMasterKey.generateMasterKey()");

        return false;
    }

    /**
     * To get Master Key
     *
     * @param password password to be used for decryption
     * @return Decrypted Master Key
     * @throws Throwable
     */
    @Override
    public String getMasterKey(String password) throws Throwable {
        logger.debug("==> RangerMasterKey.getMasterKey()");
        logger.info("Getting Master Key");

        List   result              = getEncryptedMK();
        String encryptedPassString = null;
        byte[] masterKeyByte       = null;

        if (CollectionUtils.isNotEmpty(result) && result.size() == 2) {
            masterKeyByte       = (byte[]) result.get(0);
            encryptedPassString = (String) result.get(1);
        } else if (CollectionUtils.isNotEmpty(result)) {
            masterKeyByte = (byte[]) result.get(0);
        }

        if (masterKeyByte != null && masterKeyByte.length > 0) {
            logger.debug("<== RangerMasterKey.getMasterKey()");

            return decryptMasterKey(masterKeyByte, password, encryptedPassString);
        } else {
            throw new Exception("No Master Key Found");
        }
    }

    /**
     * Generate the master key, encrypt it and save it in the database
     *
     * @return true if the master key was successfully created false if master
     * key generation was unsuccessful or the master key already exists
     */
    @Override
    public boolean reencryptOrUpdateMK(String mkPassword) {
        logger.debug("==> RangerMasterKey.reencryptOrUpdateMK");

        boolean isMKReencrypted = false;
        // Fetch MK and check the last CryptoAlgo used for encryption
        List result = getEncryptedMK();
        String encryptedPassString = null;
        byte[] masterKeyByte = null;
        if (CollectionUtils.isNotEmpty(result) && result.size() == 2) {
            masterKeyByte = (byte[]) result.get(0);
            encryptedPassString = (String) result.get(1);
        } else if (CollectionUtils.isNotEmpty(result)) {
            masterKeyByte = (byte[]) result.get(0);
        }

        if (encryptedPassString == null) {
            getPasswordParam(mkPassword);
        }

        RangerCryptoKDFSuite encrCryptoAlgo = this.cryptoConfigApi.getCryptoAlgorithm();
        RangerCipherSuite cipherSuite       = this.cryptoConfigApi.getCipher();

        if (null != masterKeyByte &&
                (!encrCryptoAlgo.getKeyDerivationAlgoName().equalsIgnoreCase((this.pbeKDFAlgo.getKeyDerivationAlgoName()))
                        || !cipherSuite.getCipherTransformation().equalsIgnoreCase(this.mkCipher.getCipherTransformation()))) {
            logger.info("MasterKey key material was encrypted using KDF {} and Cipher {} , going to re-encrypt using KDF {} and Cipher {}", this.pbeKDFAlgo, this.mkCipher, encrCryptoAlgo, cipherSuite);
            try {
                KeyEncryptor.EncryptKeyResponse<byte[]> response = this.kmsKeyCryptoAPI.reencryptKey(masterKeyByte, mkPassword, prepareCryptoParamsFromCurrentState(), Optional.of(MK_AAD_FOR_GCM_MODE));

                updateEncryptedMK(buildMKMetaInfo(response.getIv(), response.getEncryptedContent()));
                isMKReencrypted = true;
                logger.info("MasterKey key material got re-encrypted and saved to the DB");
            } catch (Throwable e) {
                logger.error(" Error while re-encrypting the  MasterKey", e);
                throw new RuntimeException(e);
            }
        }

        logger.debug("<== RangerMasterKey.reencryptMKWithFipsAlgo");

        return isMKReencrypted;
    }

    @Override
    public boolean setExternalKeyAsMK(String password, byte[] key)throws Throwable {
        logger.debug("==> RangerMasterKey.setExternalKeyAsMK()");

        boolean keySetAsMK = false;

        if (!checkMKExistence(this.masterKeyDao)) {
            logger.info("Master Key doesn't exist in DB, encrypting and storing the provided Master Key");

            KeyEncryptor.EncryptKeyResponse<byte[]> response = this.kmsKeyCryptoAPI.encryptKey(key, password, Optional.of(MK_AAD_FOR_GCM_MODE));

            String savedKey           = saveEncryptedMK(buildMKMetaInfo(response.getIv(), response.getEncryptedContent()));

            if (savedKey != null && !savedKey.trim().equals("")) {
                keySetAsMK = true;
                logger.info("Master Key Created with id = {}", savedKey);
                logger.debug("<== RangerMasterKey.setExternalKeyAsMK()");
            }
        } else {
            String errMsg = "Ranger Master Key already exists in the DB, returning.";
            logger.warn(errMsg);
        }

        logger.debug("<== RangerMasterKey.setExternalKeyAsMK()");

        return keySetAsMK;
    }

    private String decryptMasterKey(byte[] masterKey, String password, String encryptedPassString) throws Throwable {
        logger.debug("==> RangerMasterKey.decryptMasterKey()");
        logger.debug("Decrypting Master Key...");

        SecretKey masterKeyFromDB = decryptMasterKeySK(masterKey, password, encryptedPassString);

        logger.debug("<== RangerMasterKey.decryptMasterKey()");

        return Base64.encode(masterKeyFromDB.getEncoded());
    }

    private SecretKey decryptMasterKeySK(byte[] masterKey, String password, String encryptedPassString) throws Throwable {
        logger.debug("==> RangerMasterKey.decryptMasterKeySK()");

        if (encryptedPassString == null) {
            getPasswordParam(password);
        }

        byte[] masterKeyFromDBDecrypted = this.kmsKeyCryptoAPI.decryptKey(masterKey, password, prepareCryptoParamsFromCurrentState(), Optional.of(MK_AAD_FOR_GCM_MODE));

        logger.debug("<== RangerMasterKey.decryptMasterKeySK()");

        return getMasterKeyFromBytes(masterKeyFromDBDecrypted);
    }

    private List getEncryptedMK() {
        logger.debug("==> RangerMasterKey.getEncryptedMK()");

        try {
            if (masterKeyDao != null) {
                ArrayList               ret                = new ArrayList<>();
                List<XXRangerMasterKey> lstRangerMasterKey = masterKeyDao.getAll();

                if (lstRangerMasterKey.size() < 1) {
                    throw new Exception("No Master Key exists");
                } else if (lstRangerMasterKey.size() > 1) {
                    throw new Exception("More than one Master Key exists");
                } else {
                    XXRangerMasterKey rangerMasterKey = masterKeyDao.getById(lstRangerMasterKey.get(0).getId());
                    String            masterKeyStr    = rangerMasterKey.getMasterKey();

                    if (masterKeyStr.contains(",")) {
                        getPasswordParam(masterKeyStr);

                        ret.add(Base64.decode(password));
                        ret.add(masterKeyStr);
                    } else {
                        ret.add(Base64.decode(masterKeyStr));
                    }

                    logger.debug("<== RangerMasterKey.getEncryptedMK()");

                    return ret;
                }
            }
        } catch (Exception e) {
            logger.error("Unable to retrieve Master Key from the database!!!", e);
        }

        logger.debug("<== RangerMasterKey.getEncryptedMK()");

        return null;
    }

    private String saveEncryptedMK(String encryptedMasterKey) {
        logger.debug("==> RangerMasterKey.saveEncryptedMK()");

        XXRangerMasterKey xxRangerMasterKey = new XXRangerMasterKey();

        xxRangerMasterKey.setCipher(this.cryptoConfigApi.getCipher().getCipherTransformation());
        xxRangerMasterKey.setBitLength(this.cryptoConfigApi.getKeySize());
        xxRangerMasterKey.setMasterKey(encryptedMasterKey);

        try {
            if (masterKeyDao != null) {
                XXRangerMasterKey rangerMasterKey = masterKeyDao.create(xxRangerMasterKey);

                logger.debug("<== RangerMasterKey.saveEncryptedMK()");

                return rangerMasterKey.getId().toString();
            }
        } catch (Exception e) {
            logger.error("Error while saving master key in Database!!! ", e);
        }

        logger.debug("<== RangerMasterKey.saveEncryptedMK()");

        return null;
    }

    private void updateEncryptedMK(String encryptedMasterKey) throws Exception {
        logger.debug("==> RangerMasterKey.updateEncryptedMK()");
        try {
            if (masterKeyDao != null) {
                XXRangerMasterKey rangerMasterKey = masterKeyDao.getAll().get(0);
                if (rangerMasterKey != null) {
                    rangerMasterKey.setMasterKey(encryptedMasterKey);
                    masterKeyDao.update(rangerMasterKey);
                }

                logger.debug("<== RangerMasterKey.updateEncryptedMK()");
            }
        } catch (Exception e) {
            String errorMsg = "Error while updating master key in Database!!! ";
            logger.error(errorMsg, e);
            throw new Exception("Error while updating master key in Database!!! ", e);
        }

        logger.debug("<== RangerMasterKey.updateEncryptedMK()");
    }

    /*
        Returns:
        true: if Master Key exists
        fasle: If Master key doesn't exist.
     */
    private boolean checkMKExistence(RangerMasterKeyDao rangerMKDao) {
        boolean mkExists = false;

        if (rangerMKDao != null) {
            mkExists = rangerMKDao.getAllCount() < 1 ? false : true;
        }

        return mkExists;
    }

    private RangerKMSKeyCryptoAPI.KMSCryptoParams prepareCryptoParamsFromCurrentState() throws RangerKMSCryptoException {
        return prepareCryptoParamsBuilderFromCurrentState().build();
    }

    // To be used while decrypt operation
    private KMSCryptoParamsBuilder prepareCryptoParamsBuilderFromCurrentState() throws RangerKMSCryptoException {
        try {
            RangerCryptoKDFSuite kdfSuite = pbeKDFAlgo;
            KeySpecStrategy keySpecStrategy = kdfSuite.getKeyLength().isPresent() ? KeySpecStrategy.PASSWORD_SALT_ITERATIONCOUNT_KEYLENGTH : KeySpecStrategy.PASSWORD_SALT_ITERATIONCOUNT;
            RangerCipherSuite mappedCipherSuite = mapToCorrectCipher(kdfSuite.getKeyDerivationAlgoName(), mkCipher);
            String generatedSalt = generateSalt(mdAlgo, salt, saltSize);

            return new KMSCryptoParamsBuilder(SaltGenerationStrategy.DETERMINISTIC, keySpecStrategy)
                    .kdfAlgo(kdfSuite)
                    .keySize(mkKeySize)
                    .cipher(mappedCipherSuite)
                    .saltSeed(generatedSalt)
                    .saltSize(saltSize)
                    .mdAlgo(mdAlgo)
                    .iterationCount(iterationCount)
                    .iv(null != iv ? Base64.decode(iv) : null);
        } catch (Exception e) {
            String errMsg = "Error while preparing cryptoParams from current state";
            logger.error(errMsg, e);
            throw new RangerKMSCryptoException(errMsg, e);
        }
    }

    /*
        Earlier, only PBEWith<MessageDigestFunction>And<Cipher> type of algorithm was being used and there same algo name is used for key derivation as well as Cipher.
        Actually, exact key derivation and cipher is derived and used by the SecurityProviders. It was not explicit.
        Now, KMS has started supporting separate KeyDerivationFunction (KDF) and separate Cipher.
        This is the recommended approach.
        Following mapping is required to support backward-compatibility
         1. Upgrade Case:
            Example 1:
             Cipher :           AES
             EncryptionAlgo:    PBEWithMD5AndTripleDES

             Mapped value :     PBEWithMD5AndTripleDES

           2. Fresh Cluster:
           Fresh cluster after this code is expected to use complete format for Cipher, that is, AES/<MODE>/<PADDING>
           Example:  AES/CTR/NoPadding
           In fresh cluster, if only "AES" is used as cipher with KDF that requires separate Cipher, it would fail at runtime.
           Like, only AES can't be used with PBKDF2WithHmacSHA256. It requires proper Cipher.
           It would work with PBEWith<MessageDigestFunction>And<Cipher> type of algorithms.
         */
    RangerCipherSuite mapToCorrectCipher(String kdfAlgo, RangerCipherSuite defaultCipherSuite) {
        RangerCipherSuite mappedCipherSuite = defaultCipherSuite;

        if (RangerCryptoKDFSuite.canKDFAlgoBeUsedAsCipher(kdfAlgo)) { // that is, the KDF is of type PBEWith<MD>And<Cipher>
            mappedCipherSuite = SupportedCipherSuite.convert(kdfAlgo);
        }

        return mappedCipherSuite;
    }

    private SecretKey getMasterKeyFromBytes(byte[] keyData) {
        return new SecretKeySpec(keyData, mkCipher.getCipherAlgoName());
    }

    private String generateSalt(String mdAlgoName, String initialSalt, int saltSize) throws RangerKMSCryptoException {
        return Base64.encode(((RangerKMSCryptoManager) this.kmsKeyCryptoAPI).getSaltGenerator(SaltGenerationStrategy.DETERMINISTIC, mdAlgoName, initialSalt).generateSalt(saltSize));
    }

    // Following methods MUST NOT BE USED FOR PROD CODE. IT HAS BEEN EXPOSED ONLY FOR UnitTesting.

    @VisibleForTesting
    RangerCryptoKDFSuite getMKEncryptionAlgoName() {
        List result = getEncryptedMK();
        String encryptedPassString = null;
        if (CollectionUtils.isNotEmpty(result) && result.size() == 2) {
            encryptedPassString = (String) result.get(1);
        }

        if (StringUtils.isEmpty(encryptedPassString)) {
            getPasswordParam("masterKeyStr");
        }

        return  this.pbeKDFAlgo;
    }
}
