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

package org.apache.hadoop.crypto.key.common;

import com.sun.org.apache.xml.internal.security.utils.Base64;
import org.apache.hadoop.crypto.key.common.RangerCipherSuite.AlgoParamSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.SealedObject;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.SecretKeySpec;

import java.io.IOException;
import java.io.Serializable;
import java.security.AlgorithmParameters;
import java.security.Key;
import java.security.NoSuchAlgorithmException;
import java.security.spec.AlgorithmParameterSpec;
import java.util.Optional;

import static org.apache.hadoop.crypto.key.common.SupportedCipherSuite.AES_GCM_NOPADDING;

public class RangerKMSCryptoManager  implements RangerKMSKeyCryptoAPI  {
    private static final Logger logger = LoggerFactory.getLogger(RangerKMSCryptoManager.class);

    private final RangerKMSCryptoConfigApi cryptoConfig;
    private final RangerKMSKeyGenerator kmsKeyGenerator;

    public RangerKMSCryptoManager(RangerKMSCryptoConfigApi cryptoConfig) {
        this.cryptoConfig = cryptoConfig;
        this.kmsKeyGenerator = new DefaultKMSKeyGenerator();
    }

    @Override
    public EncryptKeyResponse<byte[]> generateAndEncryptKey(String password, Optional<byte[]> aad) throws RangerKMSCryptoException {
        try {
            // First, Generate the cryptographic key
            SecretKey secretKey = kmsKeyGenerator.generateKey(cryptoConfig.getCipher().getCipherAlgoName(), cryptoConfig.getKeySize());

            // Now encrypt it using configured crypto algo
            return this.encryptKey(secretKey.getEncoded(), password, aad);
        } catch (NoSuchAlgorithmException e) {
            String errMsg = "Error while generating & decrypting key";
            logger.error(errMsg, e);
            throw new RangerKMSCryptoException(errMsg, e);
        }
    }

    @Override
    public EncryptKeyResponse<byte[]> reencryptKey(byte[] data, String password, KMSCryptoParams oldCryptoParams, Optional<byte[]> aad) throws RangerKMSCryptoException {
        // Fist it needs to be decrypted
        byte[] oldKeyMaterial = decryptKey(data, password, oldCryptoParams, aad);

        // Now encrypt it using current selected algorithm
        EncryptKeyResponse<byte[]> encryptKeyResponse =  encryptKey(oldKeyMaterial, password, aad);

        // This is just a sanity check but important to ensure that reencrypted key material is same as old MK key material
        RangerKMSKeyCryptoAPI.KMSCryptoParams cryptoParams = new RangerKMSKeyCryptoAPI.KMSCryptoParams.KMSCryptoParamsBuilder(SaltGenerationStrategy.DETERMINISTIC, this.cryptoConfig.getCryptoParams().getKeySpecStrategy())
                .iterationCount(this.cryptoConfig.getIterationCount())
                .saltSeed(Base64.encode(encryptKeyResponse.getSalt()))
                .saltSize(this.cryptoConfig.getSaltSize())
                .kdfAlgo(this.cryptoConfig.getCryptoAlgorithm())
                .cipher(this.cryptoConfig.getCipher())
                .iv(encryptKeyResponse.getIv())
                .mdAlgo(this.cryptoConfig.getMessageDigestAlgorithm())
                .build();

        byte[] materialAfterReencryption = decryptKey(encryptKeyResponse.getEncryptedContent(), password, cryptoParams, aad);

        if (!Base64.encode(oldKeyMaterial).equals(Base64.encode(materialAfterReencryption))) {
            String errMsg = "After re-encryption, Latest decrypted MasterKey material is different than original.Aborting the re-encryption, DB is not updated with new encrypted material.";
            logger.error(errMsg);
            throw new RangerKMSCryptoException(errMsg);
        }

        return encryptKeyResponse;
    }

    @Override
    public EncryptKeyResponse<byte[]> encryptKey(byte[] data, String password, Optional<byte[]> aad) throws RangerKMSCryptoException {
        KMSCryptoParams      cryptoParams = cryptoConfig.getCryptoParams();
        RangerCryptoKDFSuite kdfAlgoSuite = cryptoParams.getKDFAlgo();

        RangerCipherSuite cipherSuite     = mapToCorrectCipherTranformation(kdfAlgoSuite.getKeyDerivationAlgoName(), cryptoParams.getCipher());

        try {
            // Generate Salt
            byte[] salt = getSaltGenerator(cryptoParams.getSaltGenerationStrategy(), cryptoParams.getMdAlgo(), cryptoParams.getSalt()).generateSalt(cryptoParams.getSaltSize());

            // Generate IV if required
            byte[] iv = generateIVIfRequired(cipherSuite);

            AlgoParamSpec algoParamSpec = new AlgoParamSpec(salt, cryptoParams.getIterationCount(), iv, kdfAlgoSuite.getKeyLength(), cipherSuite.getCipherTagLengthInBits());
            PBEKeySpec pbeKeySpec    = getPBEParameterSpec(password, kdfAlgoSuite, algoParamSpec, cryptoParams.getKeySpecStrategy());

            SecretKey deriveKey = this.kmsKeyGenerator.generateKey(pbeKeySpec, kdfAlgoSuite.getKeyDerivationAlgoName());

            SecretKey cipherKey = createSecretKeyFromDerivedSecretKey(deriveKey, cipherSuite);

            byte[] encryptedResponse = null;
            if (pbeKeySpec.getSalt() != null) {
                Cipher c = Cipher.getInstance(cipherSuite.getCipherTransformation());

                c.init(Cipher.ENCRYPT_MODE, cipherKey, cipherSuite.getAlgoParamSpec(algoParamSpec));

                iv = c.getIV();

                if (AES_GCM_NOPADDING.getCipherTransformation().equalsIgnoreCase(cipherSuite.getCipherTransformation()) && aad.isPresent()) {
                    c.updateAAD(aad.get());
                }

                encryptedResponse = c.doFinal(data);
            }

            logger.debug("<== RangerMasterKey.encryptKey()");
            return new EncryptKeyResponse<>(encryptedResponse, salt, iv);
        } catch (Exception e) {
            String errMsg = "Error while encrypting key";
            logger.error(errMsg, e);
            throw new RangerKMSCryptoException(errMsg, e);
        }
    }

    @Override
    public byte[] decryptKey(byte[] encryptedData, String password, KMSCryptoParams cryptoParams, Optional<byte[]> aad) throws RangerKMSCryptoException {
        byte[]               decryptedBytes = null;
        RangerCryptoKDFSuite kdf       = cryptoParams.getKDFAlgo();
        RangerCipherSuite cipherSuite       = cryptoParams.getCipher();

        try {
            byte[]              salt                = Base64.decode(cryptoParams.getSalt());
            int iterationCount = cryptoParams.getIterationCount();
            AlgoParamSpec algoParamSpec = new AlgoParamSpec(salt, iterationCount, cryptoParams.getIv(), kdf.getKeyLength(), cipherSuite.getCipherTagLengthInBits());

            PBEKeySpec          pbeKeySpec          = getPBEParameterSpec(new String(password), kdf, algoParamSpec, cryptoParams.getKeySpecStrategy());

            SecretKey deriveKey = this.kmsKeyGenerator.generateKey(pbeKeySpec, kdf.getKeyDerivationAlgoName());

            SecretKey cipherKey = createSecretKeyFromDerivedSecretKey(deriveKey, cipherSuite);

            if (pbeKeySpec.getSalt() != null) {
                AlgorithmParameterSpec algoParameterSpec = cipherSuite.getAlgoParamSpec(algoParamSpec);
                Cipher                 c             = Cipher.getInstance(cipherSuite.getCipherTransformation());

                c.init(Cipher.DECRYPT_MODE, cipherKey, algoParameterSpec);

                if (AES_GCM_NOPADDING.getCipherTransformation().equalsIgnoreCase(cipherSuite.getCipherTransformation()) && aad.isPresent()) {
                    c.updateAAD(aad.get());
                }

                decryptedBytes = c.doFinal(encryptedData);
            }
        } catch (Exception  e) {
            String errMsg = "Error while decrypting key";
            logger.error(errMsg, e);
            throw new RangerKMSCryptoException(errMsg, e);
        }

        return decryptedBytes;
    }

    @Override
    public EncryptKeyResponse<SealedObject> sealKey(Key key, char[] password, Optional<byte[]> aad) throws RangerKMSCryptoException {
        logger.debug("==> RangerKeyStore.sealKey()");
        KMSCryptoParams cryptoParams = cryptoConfig.getCryptoParams();
        // Create SecretKey
        RangerCryptoKDFSuite encrAlgo = cryptoParams.getKDFAlgo();
        RangerCipherSuite cipherSuite = mapToCorrectCipherTranformation(encrAlgo.getKeyDerivationAlgoName(), cryptoParams.getCipher());

        try {
            SecretKeyFactory            secretKeyFactory  = SecretKeyFactory.getInstance(encrAlgo.getKeyDerivationAlgoName());

            // Generate Salt
            byte[] salt = getSaltGenerator(cryptoParams.getSaltGenerationStrategy(), cryptoParams.getMdAlgo(), cryptoParams.getSalt()).generateSalt(cryptoParams.getSaltSize());

            // Generate IV if required
            byte[] iv = generateIVIfRequired(cipherSuite);

            AlgoParamSpec algoParamSpec = new AlgoParamSpec(salt, cryptoParams.getIterationCount(), iv, encrAlgo.getKeyLength(), cipherSuite.getCipherTagLengthInBits());

            PBEKeySpec   pbeKeySpec = getPBEParameterSpec(new String(password), encrAlgo, algoParamSpec, cryptoParams.getKeySpecStrategy());

            SecretKey deriveKey = secretKeyFactory.generateSecret(pbeKeySpec);
            pbeKeySpec.clearPassword();

            SecretKey cipherKey = createSecretKeyFromDerivedSecretKey(deriveKey, cipherSuite);

            // Seal the Key
            Cipher cipher = Cipher.getInstance(cipherSuite.getCipherTransformation());
            cipher.init(Cipher.ENCRYPT_MODE, cipherKey, cipherSuite.getAlgoParamSpec(algoParamSpec));

            iv = cipher.getIV();

            if (AES_GCM_NOPADDING.getCipherTransformation().equalsIgnoreCase(cipherSuite.getCipherTransformation()) && aad.isPresent()) {
                cipher.updateAAD(aad.get());
            }

            SealedObject sealedKeyResponse = new SealedKeyObject(key, cipher);

            logger.debug("<== RangerKeyStore.sealKey()");

            return new EncryptKeyResponse<>(sealedKeyResponse, salt, iv);
        } catch (Exception e) {
            String errMsg = "Error while sealing key";
            logger.error(errMsg, e);
            throw new RangerKMSCryptoException(errMsg, e);
        }
    }

    @Override
    public Key unsealKey(SealedObject sealedKey, char[] password, RangerKMSKeyCryptoAPI.KMSCryptoParams cryptoParams, Optional<byte[]> aad) throws RangerKMSCryptoException {
        logger.debug("==> RangerKeyStore.unsealKey()");

        RangerCryptoKDFSuite cryptoAlgoSuite = cryptoParams.getKDFAlgo();
        RangerCipherSuite cipherSuite = cryptoParams.getCipher();

        try {
            // Get the AlgorithmParameters from RangerSealedObject for Cipher
            AlgorithmParameters algorithmParameters = null;

            if (sealedKey instanceof SealedKeyObject) {
                algorithmParameters = ((SealedKeyObject) sealedKey).getParameters(cipherSuite.getAlgoNameForAlgoParameter());
            } else {
                algorithmParameters = new SealedKeyObject(sealedKey).getParameters(cipherSuite.getAlgoNameForAlgoParameter());
            }

            byte[]              salt                = Base64.decode(cryptoParams.getSalt());
            int iterationCount = cryptoParams.getIterationCount();
            AlgoParamSpec algoParamSpec = new AlgoParamSpec(salt, iterationCount, null, cryptoAlgoSuite.getKeyLength(), cipherSuite.getCipherTagLengthInBits());
            PBEKeySpec          pbeKeySpec          = getPBEParameterSpec(new String(password), cryptoAlgoSuite, algoParamSpec, cryptoParams.getKeySpecStrategy());

            // Create SecretKey
            SecretKeyFactory secretKeyFactory = SecretKeyFactory.getInstance(cryptoAlgoSuite.getKeyDerivationAlgoName());

            SecretKey        deriveKey        = secretKeyFactory.generateSecret(pbeKeySpec);
            pbeKeySpec.clearPassword();

            SecretKey cipherKey = createSecretKeyFromDerivedSecretKey(deriveKey, cipherSuite);

            // Unseal the Key
            Cipher cipher = Cipher.getInstance(cipherSuite.getCipherTransformation());
            cipher.init(Cipher.DECRYPT_MODE, cipherKey, algorithmParameters);

            if (AES_GCM_NOPADDING.getCipherTransformation().equalsIgnoreCase(cipherSuite.getCipherTransformation()) && aad.isPresent()) {
                cipher.updateAAD(aad.get());
            }

            logger.debug("<== RangerKeyStore.unsealKey()");
            return (Key) sealedKey.getObject(cipher);
        } catch (Exception e) {
            String errMsg = "Error while unsealing key";
            logger.error(errMsg, e);
            throw new RangerKMSCryptoException(errMsg, e);
        }
    }

    private byte[] generateIVIfRequired(RangerCipherSuite cipherSuite) {
        byte[] iv = null;

        if (cipherSuite.isIVRequired()) {
            iv =  new RandomSaltGenerator().generateSalt(cipherSuite.getCipherIVLengthInBytes());
        }

        return iv;
    }

    private PBEKeySpec getPBEParameterSpec(String password, RangerCryptoKDFSuite kdfSuite, AlgoParamSpec algoParamSpec, KeySpecStrategy keySpecStrategy) throws RangerKMSCryptoException {
        logger.debug("==> RangerMasterKey.getPBEParameterSpec()");

        char[] compliantPwd = getCompliantPassword(password, kdfSuite).toCharArray();
        return keySpecStrategy.createPBEKeySpec(compliantPwd, algoParamSpec);
    }

    private String getCompliantPassword(String password, RangerCryptoKDFSuite kdf) {
        String newPwd = password;

        if (kdf.getMinPwdLength().isPresent()) {
            int requiredPwdLength = kdf.getMinPwdLength().get();
            while (newPwd.length() < requiredPwdLength) {
                newPwd = newPwd.concat(password);
            }
        }

        return newPwd;
    }

    public SaltGenerator getSaltGenerator(SaltGenerationStrategy saltGenerationStrategy, String mdAlgo, String initialSalt) {
        switch (saltGenerationStrategy) {
            case RANDOM:
                return new RandomSaltGenerator();
            case DETERMINISTIC:
                return new DeterministicSaltGenerator(mdAlgo, initialSalt);
            default:
                throw new IllegalArgumentException("Unknown saltGenerationStrategy. Only RANDOM or DETERMINISTIC is supported");
        }
    }

    // For PBEWith<MD>And<Cipher> based KDF algorithms, KDF name is used for Cipher transformation as well.
    // So this mapping is required.
    //Provided Cipher, like AES/CTR/NoPadding will be used for key generation and cipher transformation for PBKDF2 kind of KDF algorithms.
    private RangerCipherSuite mapToCorrectCipherTranformation(String kdfAlgo, RangerCipherSuite cipherSuite) {
        if (RangerCryptoKDFSuite.canKDFAlgoBeUsedAsCipher(kdfAlgo)) {
            cipherSuite = SupportedCipherSuite.convert(kdfAlgo);
        }

        return cipherSuite;
    }

    // Wrapping of derivedKey using AES algo is required for AES/GCM/NoPadding.
    private SecretKey createSecretKeyFromDerivedSecretKey(SecretKey derivedKey, RangerCipherSuite cipherSuite) {
        SecretKey cipherKey = derivedKey;
        if (cipherSuite.isAESWrappingRequiredForCipherTransformation()) {
            cipherKey = new SecretKeySpec(derivedKey.getEncoded(), "AES");
        }

        return cipherKey;
    }

    /**
     * Encapsulate the encrypted key, so that we can retrieve the AlgorithmParameters object on the decryption side
     */
    public static class SealedKeyObject extends SealedObject {
        protected SealedKeyObject(Serializable object, Cipher cipher) throws IllegalBlockSizeException, IOException {
            super(object, cipher);
        }

        protected SealedKeyObject(SealedObject object) throws IllegalBlockSizeException, IOException {
            super(object);
        }

        public AlgorithmParameters getParameters(String algorithm) throws NoSuchAlgorithmException, IOException {
            AlgorithmParameters algorithmParameters = AlgorithmParameters.getInstance(algorithm);

            algorithmParameters.init(super.encodedParams);

            return algorithmParameters;
        }
    }
}
