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
import org.apache.hadoop.crypto.key.common.RangerKMSKeyCryptoAPI;
import org.apache.hadoop.crypto.key.common.SaltGenerationStrategy;
import org.apache.hadoop.crypto.key.common.SupportedCipherSuite;
import org.apache.hadoop.crypto.key.common.SupportedPBECryptoKDFSuite;

import java.util.Optional;

public class TestKMSCryptoAPIManager extends  RangerKMSCryptoConfigManager {
    private           RangerCryptoKDFSuite encrCryptoAlgo;
    private           RangerCipherSuite    cipher    = SupportedCipherSuite.AES_CTR_NOPADDING;
    private     final int                   keySize = 256;
    private           int                   saltSize = 8;
    private           String                salt = "RangerKMSKeySalt";
    private           String                mdAlgo;
    private           Integer               iterationCount;

    // To be used to create different KMSCryptoParams for MK and ZK.
    // By default, it is set to false
    private boolean isConfigForMK;

    public TestKMSCryptoAPIManager(String cryptoAlgo, String mdAlgo, int iterationCount) {
        this.encrCryptoAlgo = SupportedPBECryptoKDFSuite.convert(cryptoAlgo);
        this.mdAlgo = mdAlgo;
        this.iterationCount = iterationCount;
    }

    public TestKMSCryptoAPIManager(String cryptoAlgo, String cipher, String mdAlgo, int iterationCount) {
        this(cryptoAlgo, mdAlgo, iterationCount);
        this.cipher = SupportedCipherSuite.convert(cipher);
    }

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
        Optional<Integer> minSaltSize = this.encrCryptoAlgo.getMinSaltSize();
        return minSaltSize.orElseGet(() -> this.saltSize);
    }

    @Override
    public String getSalt() {
        return this.salt;
    }

    @Override
    public RangerCryptoKDFSuite getCryptoAlgorithm() {
        return encrCryptoAlgo;
    }

    @Override
    public String getMessageDigestAlgorithm() {
        return this.mdAlgo;
    }

    @Override
    public int getIterationCount() {
        return this.iterationCount;
    }

    public void setConfigForMK(boolean isConfigForMK) {
        this.isConfigForMK = isConfigForMK;
    }

    @Override
    public RangerKMSKeyCryptoAPI.KMSCryptoParams getCryptoParams() {
        SaltGenerationStrategy saltGenerationStrategy = SaltGenerationStrategy.RANDOM;
        KeySpecStrategy        keySpecStrategy        = KeySpecStrategy.PASSWORD_ONLY;

        if (this.isConfigForMK) {
            saltGenerationStrategy = SaltGenerationStrategy.DETERMINISTIC;
            keySpecStrategy = this.getCryptoAlgorithm().getKeyLength().isPresent() ? KeySpecStrategy.PASSWORD_SALT_ITERATIONCOUNT_KEYLENGTH : KeySpecStrategy.PASSWORD_SALT_ITERATIONCOUNT;
        }

        RangerKMSKeyCryptoAPI.KMSCryptoParams.KMSCryptoParamsBuilder builder = new RangerKMSKeyCryptoAPI.KMSCryptoParams.KMSCryptoParamsBuilder(saltGenerationStrategy, keySpecStrategy);
        return builder.kdfAlgo(this.getCryptoAlgorithm())
                .cipher(this.getCipher())
                .keySize(this.getKeySize())
                .mdAlgo(this.getMessageDigestAlgorithm())
                .iterationCount(this.getIterationCount())
                .saltSeed(this.getSalt())
                .saltSize(this.getSaltSize())
                .build();
    }
}
