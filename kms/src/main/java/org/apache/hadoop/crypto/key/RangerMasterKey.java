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
import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.thirdparty.com.google.common.base.Joiner;
import org.apache.hadoop.thirdparty.com.google.common.base.Splitter;
import org.apache.hadoop.thirdparty.com.google.common.collect.Lists;
import org.apache.ranger.entity.XXRangerMasterKey;
import org.apache.ranger.kms.dao.DaoManager;
import org.apache.ranger.kms.dao.RangerMasterKeyDao;
import org.apache.ranger.plugin.util.XMLUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.Cipher;
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.PBEParameterSpec;
import javax.crypto.spec.SecretKeySpec;

import java.security.Key;
import java.security.KeyStore;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class RangerMasterKey implements RangerKMSMKI {
    private static final Logger logger = LoggerFactory.getLogger(RangerMasterKey.class);

    public  static final String     DBKS_SITE_XML           = "dbks-site.xml";
    private static final String     DEFAULT_MK_CIPHER       = "AES";
    private static final int        DEFAULT_MK_KeySize      = 256;
    private static final int        DEFAULT_SALT_SIZE       = 8;
    private static final String     DEFAULT_SALT            = "abcdefghijklmnopqrstuvwxyz01234567890";
    private static final String     DEFAULT_CRYPT_ALGO      = "PBEWithMD5AndTripleDES";
    private static final int        DEFAULT_ITERATION_COUNT = 1000;
    private static final Properties serverConfigProperties  = new Properties();

    public  static String  mkCipher;
    public  static Integer mkKeySize = 0;
    public  static Integer saltSize  = 0;
    public  static String salt;
    public  static String  pbeAlgo;
    public  static String  mdAlgo;
    public  static Integer iterationCount = 0;
    public  static String  paddingString;
    private static String password;
    private static String defaultMdAlgo;

    private final RangerMasterKeyDao masterKeyDao;

    public RangerMasterKey() {
        this.masterKeyDao = null;
    }

    public RangerMasterKey(DaoManager daoManager) {
        this.masterKeyDao = daoManager != null ? daoManager.getRangerMasterKeyDao() : null;
    }

    public static void getPasswordParam(String paddedEncryptedPwd) {
        String[] encryptedPwd = null;

        if (paddedEncryptedPwd != null && paddedEncryptedPwd.contains(",")) {
            encryptedPwd = Lists.newArrayList(Splitter.on(",").split(paddedEncryptedPwd)).toArray(new String[0]);
        }

        if (encryptedPwd != null && encryptedPwd.length >= 7) {
            int index = 0;

            mkCipher       = encryptedPwd[index];
            mkKeySize      = Integer.parseInt(encryptedPwd[++index]);
            saltSize       = Integer.parseInt(encryptedPwd[++index]);
            pbeAlgo        = encryptedPwd[++index];
            mdAlgo         = encryptedPwd[++index];
            iterationCount = Integer.parseInt(encryptedPwd[++index]);
            salt           = encryptedPwd[++index];
            password       = encryptedPwd[++index];
        } else {
            mkCipher  = DEFAULT_MK_CIPHER;
            mkKeySize = DEFAULT_MK_KeySize;
            saltSize  = DEFAULT_SALT_SIZE;
            pbeAlgo   = DEFAULT_CRYPT_ALGO;
            mdAlgo    = defaultMdAlgo;
            password  = paddedEncryptedPwd;
            salt      = password;

            if (password != null) {
                iterationCount = password.toCharArray().length + 1;
            }
        }
    }

    protected static String getConfig(String key, String defaultValue) {
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

    protected static int getIntConfig(String key, int defaultValue) {
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

    public void init() {
        logger.debug("==> RangerMasterKey.init()");

        XMLUtils.loadConfig(DBKS_SITE_XML, serverConfigProperties);

        defaultMdAlgo  = getConfig("ranger.keystore.file.type", KeyStore.getDefaultType()).equalsIgnoreCase("bcfks") ? "SHA-512" : "MD5";
        mkCipher       = getConfig("ranger.kms.service.masterkey.password.cipher", DEFAULT_MK_CIPHER);
        mkKeySize      = getIntConfig("ranger.kms.service.masterkey.password.size", DEFAULT_MK_KeySize);
        saltSize       = getIntConfig("ranger.kms.service.masterkey.password.salt.size", DEFAULT_SALT_SIZE);
        salt           = getConfig("ranger.kms.service.masterkey.password.salt", DEFAULT_SALT);
        pbeAlgo        = getConfig("ranger.kms.service.masterkey.password.encryption.algorithm", DEFAULT_CRYPT_ALGO);
        mdAlgo         = getConfig("ranger.kms.service.masterkey.password.md.algorithm", defaultMdAlgo);
        iterationCount = getIntConfig("ranger.kms.service.masterkey.password.iteration.count", DEFAULT_ITERATION_COUNT);
        paddingString  = Joiner.on(",").skipNulls().join(mkCipher, mkKeySize, saltSize, pbeAlgo, mdAlgo, iterationCount, salt);

        logger.debug("<== RangerMasterKey.init()");
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

        init();

        if (!checkMKExistence(this.masterKeyDao)) {
            logger.info("Master Key doesn't exist in DB, Generating the Master Key");

            String encryptedMasterKey = encryptMasterKey(password);
            String savedKey           = saveEncryptedMK(paddingString + "," + encryptedMasterKey);

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

    public void generateMKFromHSMMK(String password, byte[] key) throws Throwable {
        logger.debug("==> RangerMasterKey.generateMKFromHSMMK()");

        init();

        if (!checkMKExistence(this.masterKeyDao)) {
            logger.info("Master Key doesn't exist in DB, Generating the Master Key");

            String encryptedMasterKey = encryptMasterKey(password, key);
            String savedKey           = saveEncryptedMK(paddingString + "," + encryptedMasterKey);

            if (savedKey != null && !savedKey.trim().equals("")) {
                logger.debug("Master Key Created with id = {}", savedKey);
                logger.debug("<== RangerMasterKey.generateMKFromHSMMK()");
            }
        } else {
            logger.debug("Ranger Master Key already exists in the DB, returning.");
        }

        logger.debug("<== RangerMasterKey.generateMKFromHSMMK()");
    }

    public void generateMKFromKeySecureMK(String password, byte[] key) throws Throwable {
        logger.debug("==> RangerMasterKey.generateMKFromKeySecureMK()");

        init();

        if (!checkMKExistence(this.masterKeyDao)) {
            logger.info("Master Key doesn't exist in DB, Generating the Master Key");

            String encryptedMasterKey = encryptMasterKey(password, key);
            String savedKey           = saveEncryptedMK(paddingString + "," + encryptedMasterKey);

            if (savedKey != null && !savedKey.trim().equals("")) {
                logger.debug("Master Key Created with id = " + savedKey);
            }
        } else {
            logger.debug("Ranger Master Key already exists in the DB, returning.");
        }

        logger.debug("<== RangerMasterKey.generateMKFromKeySecureMK()");
    }

    private String decryptMasterKey(byte[] masterKey, String password, String encryptedPassString) throws Throwable {
        logger.debug("==> RangerMasterKey.decryptMasterKey()");
        logger.debug("Decrypting Master Key...");

        if (encryptedPassString == null) {
            getPasswordParam(password);
        }

        PBEKeySpec pbeKeyspec               = getPBEParameterSpec(password);
        byte[]     masterKeyFromDBDecrypted = decryptKey(masterKey, pbeKeyspec);
        SecretKey  masterKeyFromDB          = getMasterKeyFromBytes(masterKeyFromDBDecrypted);

        logger.debug("<== RangerMasterKey.decryptMasterKey()");

        return Base64.encode(masterKeyFromDB.getEncoded());
    }

    private SecretKey decryptMasterKeySK(byte[] masterKey, String password, String encryptedPassString) throws Throwable {
        logger.debug("==> RangerMasterKey.decryptMasterKeySK()");

        if (encryptedPassString == null) {
            getPasswordParam(password);
        }

        PBEKeySpec pbeKeyspec               = getPBEParameterSpec(password);
        byte[]     masterKeyFromDBDecrypted = decryptKey(masterKey, pbeKeyspec);

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

        xxRangerMasterKey.setCipher(mkCipher);
        xxRangerMasterKey.setBitLength(mkKeySize);
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

    private String encryptMasterKey(String password) throws Throwable {
        logger.debug("==> RangerMasterKey.encryptMasterKey()");

        Key        secretKey     = generateMasterKey();
        PBEKeySpec pbeKeySpec    = getPBEParameterSpec(password);
        byte[]     masterKeyToDB = encryptKey(secretKey.getEncoded(), pbeKeySpec);

        logger.debug("<== RangerMasterKey.encryptMasterKey()");

        return Base64.encode(masterKeyToDB);
    }

    private String encryptMasterKey(String password, byte[] secretKey) throws Throwable {
        logger.debug("==> RangerMasterKey.encryptMasterKey()");

        PBEKeySpec pbeKeySpec    = getPBEParameterSpec(password);
        byte[]     masterKeyToDB = encryptKey(secretKey, pbeKeySpec);

        logger.debug("<== RangerMasterKey.encryptMasterKey()");

        return Base64.encode(masterKeyToDB);
    }

    private Key generateMasterKey() throws NoSuchAlgorithmException {
        logger.debug("==> RangerMasterKey.generateMasterKey()");

        KeyGenerator kg = KeyGenerator.getInstance(mkCipher);

        kg.init(mkKeySize);

        return kg.generateKey();
    }

    private PBEKeySpec getPBEParameterSpec(String password) throws Throwable {
        logger.debug("==> RangerMasterKey.getPBEParameterSpec()");

        MessageDigest md      = MessageDigest.getInstance(mdAlgo);
        byte[]        saltGen = md.digest(salt.getBytes());
        byte[]        salt    = new byte[saltSize];

        System.arraycopy(saltGen, 0, salt, 0, saltSize);

        return new PBEKeySpec(password.toCharArray(), salt, iterationCount);
    }

    private byte[] encryptKey(byte[] data, PBEKeySpec keyspec) throws Throwable {
        logger.debug("==> RangerMasterKey.encryptKey()");

        SecretKey key = getPasswordKey(keyspec);

        if (keyspec.getSalt() != null) {
            PBEParameterSpec paramSpec = new PBEParameterSpec(keyspec.getSalt(), keyspec.getIterationCount());
            Cipher           c         = Cipher.getInstance(key.getAlgorithm());

            c.init(Cipher.ENCRYPT_MODE, key, paramSpec);

            logger.debug("<== RangerMasterKey.encryptKey()");

            return c.doFinal(data);
        }

        logger.debug("<== RangerMasterKey.encryptKey()");

        return null;
    }

    private SecretKey getPasswordKey(PBEKeySpec keyspec) throws Throwable {
        logger.debug("==> RangerMasterKey.getPasswordKey()");

        SecretKeyFactory factory = SecretKeyFactory.getInstance(pbeAlgo);

        logger.debug("<== RangerMasterKey.getPasswordKey()");

        return factory.generateSecret(keyspec);
    }

    private byte[] decryptKey(byte[] encrypted, PBEKeySpec keySpec) throws Throwable {
        SecretKey key = getPasswordKey(keySpec);

        if (keySpec.getSalt() != null) {
            PBEParameterSpec paramSpec = new PBEParameterSpec(keySpec.getSalt(), keySpec.getIterationCount());
            Cipher           c         = Cipher.getInstance(key.getAlgorithm());

            c.init(Cipher.DECRYPT_MODE, key, paramSpec);

            return c.doFinal(encrypted);
        }

        return null;
    }

    private SecretKey getMasterKeyFromBytes(byte[] keyData) {
        return new SecretKeySpec(keyData, mkCipher);
    }
}
