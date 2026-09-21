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

import java.util.Arrays;
import java.util.Optional;

public interface RangerKMSKeyCryptoAPI extends KeyEncryptor, KeyDecryptor {
    EncryptKeyResponse<byte[]> generateAndEncryptKey(String password, Optional<byte[]> aad) throws RangerKMSCryptoException;

    EncryptKeyResponse<byte[]> reencryptKey(byte[] data, String password, RangerKMSKeyCryptoAPI.KMSCryptoParams oldCryptoParams, Optional<byte[]> aad) throws RangerKMSCryptoException;

    class KMSCryptoParams {
        private  RangerCipherSuite  cipher;
        private  int keySize;
        private  int                   saltSize;
        private String               salt;
        private RangerCryptoKDFSuite kdfAlgoSuite;
        private String               mdAlgo;
        private  int iterationCount;
        private SaltGenerationStrategy saltGenerationStrategy;
        private KeySpecStrategy keySpecStrategy;
        private byte[] iv;

        public KMSCryptoParams(KMSCryptoParamsBuilder cryptoParamsBuilder) {
            this.cipher                 = cryptoParamsBuilder.cipher;
            this.keySize                = cryptoParamsBuilder.keySize;
            this.iv                     = cryptoParamsBuilder.iv;
            this.saltSize               = cryptoParamsBuilder.saltSize;
            this.salt                   = cryptoParamsBuilder.saltSeed;
            this.kdfAlgoSuite           = cryptoParamsBuilder.kdfAlgo;
            this.mdAlgo                 = cryptoParamsBuilder.mdAlgo;
            this.iterationCount         = cryptoParamsBuilder.iterationCount;
            this.saltGenerationStrategy = cryptoParamsBuilder.saltGenerationStrategy;
            this.keySpecStrategy        = cryptoParamsBuilder.keySpecStrategy;
        }

        public RangerCipherSuite getCipher() {
            return cipher;
        }

        public int getKeySize() {
            return keySize;
        }

        public Integer getSaltSize() {
            return saltSize;
        }

        public String getSalt() {
            return salt;
        }

        public RangerCryptoKDFSuite getKDFAlgo() {
            return kdfAlgoSuite;
        }

        public String getMdAlgo() {
            return mdAlgo;
        }

        public int getIterationCount() {
            return iterationCount;
        }

        public SaltGenerationStrategy getSaltGenerationStrategy() {
            return this.saltGenerationStrategy;
        }

        public KeySpecStrategy getKeySpecStrategy() {
            return this.keySpecStrategy;
        }

        public byte[] getIv() {
            return this.iv;
        }

        @Override
        public String toString() {
            return "KMSCryptoParams{" +
                    "cipher=" + cipher +
                    ", keySize=" + keySize +
                    ", saltSize=" + saltSize +
                    ", salt='" + salt + '\'' +
                    ", kdfAlgoSuite=" + kdfAlgoSuite +
                    ", mdAlgo='" + mdAlgo + '\'' +
                    ", iterationCount=" + iterationCount +
                    ", saltGenerationStrategy=" + saltGenerationStrategy +
                    ", keySpecStrategy=" + keySpecStrategy +
                    ", iv=" + Arrays.toString(iv) +
                    '}';
        }

        public static class KMSCryptoParamsBuilder {
            private  RangerCipherSuite  cipher;
            private  int keySize;
            private  int                  saltSize;
            private String               saltSeed;
            private RangerCryptoKDFSuite kdfAlgo;
            private String               mdAlgo;
            private  int iterationCount;
            private SaltGenerationStrategy saltGenerationStrategy;
            private KeySpecStrategy keySpecStrategy;
            private byte[] iv;

            public KMSCryptoParamsBuilder(SaltGenerationStrategy saltGenerationStrategy, KeySpecStrategy keySpecStrategy) {
                this.saltGenerationStrategy = saltGenerationStrategy;
                this.keySpecStrategy = keySpecStrategy;
            }

            public KMSCryptoParamsBuilder cipher(RangerCipherSuite cipher) {
                this.cipher = cipher;
                return this;
            }

            public KMSCryptoParamsBuilder keySize(int keySize) {
                this.keySize = keySize;
                return this;
            }

            public KMSCryptoParamsBuilder saltSize(int saltSize) {
                this.saltSize = saltSize;
                return this;
            }

            public KMSCryptoParamsBuilder saltSeed(String saltSeed) {
                this.saltSeed = saltSeed;
                return this;
            }

            public KMSCryptoParamsBuilder kdfAlgo(String cryptoAlgo) {
                return this.kdfAlgo(SupportedPBECryptoKDFSuite.convert(cryptoAlgo));
            }

            public KMSCryptoParamsBuilder kdfAlgo(RangerCryptoKDFSuite cryptoAlgo) {
                this.kdfAlgo = cryptoAlgo;
                return this;
            }

            public KMSCryptoParamsBuilder mdAlgo(String mdAlgo) {
                this.mdAlgo = mdAlgo;
                return this;
            }

            public KMSCryptoParamsBuilder iterationCount(int iterationCount) {
                this.iterationCount = iterationCount;
                return this;
            }

            public KMSCryptoParamsBuilder saltGenerationStrategy(SaltGenerationStrategy saltGenerationStrategy) {
                this.saltGenerationStrategy = saltGenerationStrategy;
                return this;
            }

            public KMSCryptoParamsBuilder iv(byte[] iv) {
                if (null != iv) {
                    this.iv = new byte[iv.length];
                    System.arraycopy(iv, 0, this.iv, 0, iv.length);
                }

                return this;
            }

            public KMSCryptoParams build() {
                return new KMSCryptoParams(this);
            }
        }
    }
}
