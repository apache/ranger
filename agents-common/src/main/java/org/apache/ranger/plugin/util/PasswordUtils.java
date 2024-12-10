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
package org.apache.ranger.plugin.util;

import com.sun.jersey.core.util.Base64;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.thirdparty.com.google.common.base.Splitter;
import org.apache.hadoop.thirdparty.com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.PBEParameterSpec;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Map;

public class PasswordUtils {
    private static final Logger LOG = LoggerFactory.getLogger(PasswordUtils.class);

    public static final String PBE_SHA512_AES_128      = "PBEWITHHMACSHA512ANDAES_128";
    public static final String DEFAULT_CRYPT_ALGO      = "PBEWithMD5AndDES";
    public static final String DEFAULT_ENCRYPT_KEY     = "tzL1AKl5uc4NKYaoQ4P3WLGIBFPXWPWdu1fRm9004jtQiV";
    public static final String DEFAULT_SALT            = "f77aLYLo";
    public static final int    DEFAULT_ITERATION_COUNT = 17;
    public static final byte[] DEFAULT_INITIAL_VECTOR  = new byte[16];

    private static final String LEN_SEPARATOR_STR = ":";

    private final String cryptAlgo;
    private final int    iterationCount;
    private final char[] encryptKey;
    private final byte[] salt;
    private final byte[] iv;
    private       String password;

    PasswordUtils(String aPassword) {
        String[] cryptAlgoArray = null;
        byte[]   lSalt;
        char[]   lEncryptKey;

        if (aPassword != null && aPassword.contains(",")) {
            cryptAlgoArray = Lists.newArrayList(Splitter.on(",").split(aPassword)).toArray(new String[0]);
        }

        if (cryptAlgoArray != null && cryptAlgoArray.length > 4) {
            int index = 0;

            cryptAlgo      = cryptAlgoArray[index++]; // 0
            lEncryptKey    = cryptAlgoArray[index++].toCharArray(); // 1
            lSalt          = cryptAlgoArray[index++].getBytes(); // 2
            iterationCount = Integer.parseInt(cryptAlgoArray[index++]); // 3

            if (needsIv(cryptAlgo)) {
                iv = Base64.decode(cryptAlgoArray[index++]);
            } else {
                iv = DEFAULT_INITIAL_VECTOR;
            }

            password = cryptAlgoArray[index++];

            if (cryptAlgoArray.length > index) {
                for (int i = index; i < cryptAlgoArray.length; i++) {
                    password = password + "," + cryptAlgoArray[i];
                }
            }
        } else {
            cryptAlgo      = DEFAULT_CRYPT_ALGO;
            lEncryptKey    = DEFAULT_ENCRYPT_KEY.toCharArray();
            lSalt          = DEFAULT_SALT.getBytes();
            iterationCount = DEFAULT_ITERATION_COUNT;
            iv             = DEFAULT_INITIAL_VECTOR;
            password       = aPassword;
        }

        Map<String, String> env           = System.getenv();
        String              encryptKeyStr = env.get("lEncryptKey");

        if (encryptKeyStr == null) {
            encryptKey = lEncryptKey;
        } else {
            encryptKey = encryptKeyStr.toCharArray();
        }

        String saltStr = env.get("ENCRYPT_SALT");

        if (saltStr == null) {
            salt = lSalt;
        } else {
            salt = saltStr.getBytes();
        }
    }

    public static String encryptPassword(String aPassword) throws IOException {
        return build(aPassword).encrypt();
    }

    public static PasswordUtils build(String aPassword) {
        return new PasswordUtils(aPassword);
    }

    public static String decryptPassword(String aPassword) throws IOException {
        return build(aPassword).decrypt();
    }

    public static boolean needsIv(String cryptoAlgo) {
        if (StringUtils.isEmpty(cryptoAlgo)) {
            return false;
        }

        return PBE_SHA512_AES_128.equalsIgnoreCase(cryptoAlgo)
                || cryptoAlgo.toLowerCase().contains("aes_128") || cryptoAlgo.toLowerCase().contains("aes_256");
    }

    public static String generateIvIfNeeded(String cryptAlgo) throws NoSuchAlgorithmException {
        if (!needsIv(cryptAlgo)) {
            return null;
        }
        return generateBase64EncodedIV();
    }

    public static String getDecryptPassword(String password) {
        String decryptedPwd = null;

        try {
            decryptedPwd = decryptPassword(password);
        } catch (Exception ex) {
            LOG.warn("Password decryption failed, trying original password string.");
        } finally {
            if (decryptedPwd == null) {
                decryptedPwd = password;
            }
        }

        return decryptedPwd;
    }

    public String getCryptAlgo() {
        return cryptAlgo;
    }

    public String getPassword() {
        return password;
    }

    public int getIterationCount() {
        return iterationCount;
    }

    public char[] getEncryptKey() {
        return encryptKey;
    }

    public byte[] getSalt() {
        return salt;
    }

    public byte[] getIv() {
        return iv;
    }

    public String getIvAsString() {
        return new String(Base64.encode(getIv()));
    }

    private String encrypt() throws IOException {
        String ret;
        String strToEncrypt;

        if (password == null) {
            strToEncrypt = "";
        } else {
            strToEncrypt = password.length() + LEN_SEPARATOR_STR + password;
        }

        try {
            Cipher           engine  = Cipher.getInstance(cryptAlgo);
            PBEKeySpec       keySpec = new PBEKeySpec(encryptKey);
            SecretKeyFactory skf     = SecretKeyFactory.getInstance(cryptAlgo);
            SecretKey        key     = skf.generateSecret(keySpec);

            engine.init(Cipher.ENCRYPT_MODE, key, new PBEParameterSpec(salt, iterationCount, new IvParameterSpec(iv)));

            byte[] encryptedStr = engine.doFinal(strToEncrypt.getBytes());

            ret = new String(Base64.encode(encryptedStr));
        } catch (Throwable t) {
            LOG.error("Unable to encrypt password due to error", t);

            throw new IOException("Unable to encrypt password due to error", t);
        }

        return ret;
    }

    private String decrypt() throws IOException {
        String ret;

        try {
            byte[]           decodedPassword = Base64.decode(password);
            Cipher           engine          = Cipher.getInstance(cryptAlgo);
            PBEKeySpec       keySpec         = new PBEKeySpec(encryptKey);
            SecretKeyFactory skf             = SecretKeyFactory.getInstance(cryptAlgo);
            SecretKey        key             = skf.generateSecret(keySpec);

            engine.init(Cipher.DECRYPT_MODE, key, new PBEParameterSpec(salt, iterationCount, new IvParameterSpec(iv)));

            String decrypted = new String(engine.doFinal(decodedPassword));
            int    foundAt   = decrypted.indexOf(LEN_SEPARATOR_STR);

            if (foundAt > -1) {
                if (decrypted.length() > foundAt) {
                    ret = decrypted.substring(foundAt + 1);
                } else {
                    ret = "";
                }
            } else {
                ret = null;
            }
        } catch (Throwable t) {
            LOG.error("Unable to decrypt password due to error", t);

            throw new IOException("Unable to decrypt password due to error", t);
        }

        return ret;
    }

    private static String generateBase64EncodedIV() throws NoSuchAlgorithmException {
        byte[] iv = new byte[16];

        SecureRandom.getInstance("NativePRNGNonBlocking").nextBytes(iv);

        return new String(Base64.encode(iv));
    }
}
