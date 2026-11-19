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

import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.PBEParameterSpec;

import java.security.spec.AlgorithmParameterSpec;
import java.util.Optional;
import java.util.function.Function;

public interface RangerCipherSuite {
    Function<AlgoParamSpec, AlgorithmParameterSpec> SALT_ITERATION_COUNT_ALGO_PARAM_SPEC = algoParamSpec -> new PBEParameterSpec(algoParamSpec.getSalt(), algoParamSpec.getIterationCount());

    Function<AlgoParamSpec, AlgorithmParameterSpec> SALT_ITERATION_COUNT_IV_ALGO_PARAM_SPEC = algoParamSpec -> new PBEParameterSpec(algoParamSpec.getSalt(), algoParamSpec.getIterationCount(), new IvParameterSpec(algoParamSpec.getIv()));

    Function<RangerCipherSuite.AlgoParamSpec, AlgorithmParameterSpec> IV_ALGO_PARAM_SPEC = algoParamSpec -> new IvParameterSpec(algoParamSpec.getIv());

    Function<AlgoParamSpec, AlgorithmParameterSpec> GCM_ALGO_PARAM_SPEC = algoParamSpec -> new GCMParameterSpec(algoParamSpec.getCipherTagLength(), algoParamSpec.getIv());

    String getCipherTransformation();

    boolean isIVRequired();

    int getCipherIVLengthInBytes();

    int getCipherTagLengthInBits();

    boolean isDataIntegrityCheckSupported();

    AlgorithmParameterSpec getAlgoParamSpec(AlgoParamSpec algoParamSpec);

    /*
     Some SecurityProviders like SunJCE, explicitly verifies the algorithmName while calling Cipher.init(SecretKey).
     SecretKey's algorithm should be either AES or Rijndael, otherwise it throws RuntimeException.
     Like if SecretKey is generated using PBKDF2WithHmacSHA256, algorithm name comes as "PBKDF2" and fails.

     Some latest SecurityProviders checks the content instead of algorithm name. If the bytes length is of AES supported length, it doesn't fail.

     So to make the solution provider-agnostic, it is good to always wrap the SecretKey using AES.
     But to keep the solution backward-compatible, no AES wrapping will happen for the SecretKeys generated using older supported algorithms (3DES).
     */
    boolean isAESWrappingRequiredForCipherTransformation();

    String getAlgoNameForAlgoParameter();

    default String getCipherAlgoName() {
        String cipherTransformation = getCipherTransformation();
        return cipherTransformation.split("/")[0];
    }

    class AlgoParamSpec {
        private byte[] salt;
        private int iterationCount;
        private byte[]            iv;
        private Optional<Integer> keyLength;
        private int               cipherTagLength;

        public AlgoParamSpec(byte[] salt, int iterationCount, byte[] iv, Optional<Integer> keyLength, int cipherTagLength) {
            this.salt = salt;
            this.iterationCount = iterationCount;
            this.iv = iv;
            this.keyLength = keyLength;
            this.cipherTagLength = cipherTagLength;
        }

        public byte[] getSalt() {
            return salt;
        }

        public int getIterationCount() {
            return iterationCount;
        }

        public byte[] getIv() {
            return iv;
        }

        public Optional<Integer> getKeyLength() {
            return this.keyLength;
        }

        public int getCipherTagLength() {
            return this.cipherTagLength;
        }
    }
}
