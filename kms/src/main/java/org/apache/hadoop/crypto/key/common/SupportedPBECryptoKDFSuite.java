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

import java.util.Optional;

public enum SupportedPBECryptoKDFSuite implements RangerCryptoKDFSuite {
    @Deprecated
    PBEWITHMD5ANDTRIPLEDES("PBEWithMD5AndTripleDES",
            Optional.empty(),
            Optional.empty(), Optional.empty()),

    PBKDF2WITHHMACSHA256("PBKDF2WithHmacSHA256",
            Optional.of(256),
            Optional.of(16), Optional.of(14)),

    UNKNOWN("Unknown",
            Optional.empty(),
            Optional.empty(),  Optional.empty());

    private final String keyDerivationAlgoName;
    private final Optional<Integer> keyLength;
    private final Optional<Integer> minSaltSize;
    private final Optional<Integer> minPwdLength;

    SupportedPBECryptoKDFSuite(String keyDerivationAlgoName, Optional<Integer> keyLength, Optional<Integer> minSaltSize, Optional<Integer> minPwdLength) {
        this.keyDerivationAlgoName = keyDerivationAlgoName;
        this.keyLength            = keyLength;
        this.minSaltSize          = minSaltSize;
        this.minPwdLength         = minPwdLength;
    }

    @Override
    public Optional<Integer> getKeyLength() {
        return this.keyLength;
    }

    @Override
    public String getKeyDerivationAlgoName() {
        return this.keyDerivationAlgoName;
    }

    @Override
    public Optional<Integer> getMinSaltSize() {
        return this.minSaltSize;
    }

    @Override
    public Optional<Integer> getMinPwdLength() {
        return this.minPwdLength;
    }

    @Override
    public String toString() {
        return this.getKeyDerivationAlgoName();
    }

    public static RangerCryptoKDFSuite convert(String cryptoKDFName) {
        RangerCryptoKDFSuite rangerKDF;
        try {
            rangerKDF = SupportedPBECryptoKDFSuite.valueOf(cryptoKDFName.toUpperCase());
        } catch (IllegalArgumentException e) {
            // means, an unknown or not listed/tested crypto algorithm is being used.
            // It may work but it's not tested.
            // This support is being added just to make it backward compatible, not to break if some different algo is being used
            // in any environment.

            rangerKDF = new RangerCryptoKDFSuite() {
                @Override
                public String getKeyDerivationAlgoName() {
                    return cryptoKDFName;
                }

                @Override
                public Optional<Integer> getKeyLength() {
                    return UNKNOWN.keyLength;
                }

                @Override
                public Optional<Integer> getMinSaltSize() {
                    return UNKNOWN.minSaltSize;
                }

                @Override
                public Optional<Integer> getMinPwdLength() {
                    return UNKNOWN.minPwdLength;
                }

                @Override
                public String toString() {
                    return this.getKeyDerivationAlgoName();
                }
            };
        }

        return rangerKDF;
    }
}
