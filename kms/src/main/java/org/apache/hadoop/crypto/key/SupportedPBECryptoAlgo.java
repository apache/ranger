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

import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.PBEParameterSpec;

import java.security.spec.AlgorithmParameterSpec;
import java.util.Optional;
import java.util.function.Function;

public enum SupportedPBECryptoAlgo {
    @Deprecated
    PBEWithMD5AndTripleDES("PBEWithMD5AndTripleDES",
      "PBEWithMD5AndTripleDES",
      0, Optional.empty(),  Optional.empty(), keySpec -> new PBEParameterSpec(keySpec.getSalt(), keySpec.getIterationCount())),
    @Deprecated
    PBEWithMD5AndDES("PBEWithMD5AndDES",
      "PBEWithMD5AndDES",
      0, Optional.empty(),  Optional.empty(), keySpec -> new PBEParameterSpec(keySpec.getSalt(), keySpec.getIterationCount())),
    PBKDF2WithHmacSHA256("PBKDF2WithHmacSHA256",
      "AES/CBC/PKCS7Padding",
      64 * 4, Optional.of(16), Optional.of(14),  keySpec -> new IvParameterSpec(keySpec.getSalt()));
    private final String encrAlgoName;
    private final String  cipherTransformation;
    private final int               keyLength;
    private final Optional<Integer> minSaltSize;
    private final Optional<Integer> minPwdLength;
    private final Function<PBEKeySpec, AlgorithmParameterSpec> algoParamSpecFunc;

    SupportedPBECryptoAlgo(String encrAlgoName, String cipherTransformation, int keyLength, Optional<Integer> minSaltSize, Optional<Integer> minPwdLength, Function<PBEKeySpec, AlgorithmParameterSpec> algoParamSpecFunc) {
        this.encrAlgoName         = encrAlgoName;
        this.cipherTransformation = cipherTransformation;
        this.keyLength            = keyLength;
        this.minSaltSize          = minSaltSize;
        this.minPwdLength         = minPwdLength;
        this.algoParamSpecFunc    = algoParamSpecFunc;
    }

    public int getKeyLength() {
        return this.keyLength;
    }

    public String getAlgoName() {
        return this.encrAlgoName;
    }

    public String getCipherTransformation() {
        return this.cipherTransformation;
    }

    public Optional<Integer> getMinSaltSize() {
        return this.minSaltSize;
    }

    public Optional<Integer> getMinPwdLength() {
        return this.minPwdLength;
    }

    public AlgorithmParameterSpec getAlgoParamSpec(PBEKeySpec keySpec) {
        return this.algoParamSpecFunc.apply(keySpec);
    }

    public static SupportedPBECryptoAlgo getFIPSCompliantAlgorithm() {
        return SupportedPBECryptoAlgo.PBKDF2WithHmacSHA256;
    }

    public static boolean isFIPSCompliantAlgorithm(SupportedPBECryptoAlgo encrAlgo) {
        return SupportedPBECryptoAlgo.PBKDF2WithHmacSHA256.equals(encrAlgo);
    }
}
