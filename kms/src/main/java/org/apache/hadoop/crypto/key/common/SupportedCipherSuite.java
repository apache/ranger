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

import java.security.spec.AlgorithmParameterSpec;
import java.util.function.Function;

public enum SupportedCipherSuite implements  RangerCipherSuite {
    AES_CBC_PKCS7PADDING("AES/CBC/PKCS7Padding",
            true, 16, 0,
            false, true,
            IV_ALGO_PARAM_SPEC),

    AES_CBC_PKCS5PADDING("AES/CBC/PKCS5Padding",
            true, 16, 0,
            false, true,
            IV_ALGO_PARAM_SPEC),

    AES_CTR_NOPADDING("AES/CTR/NoPadding",
            true, 16, 0,
            false, true,
            IV_ALGO_PARAM_SPEC),

    AES_GCM_NOPADDING("AES/GCM/NoPadding",
            true, 12, 128,
            true, true,
            GCM_ALGO_PARAM_SPEC),

    @Deprecated
    PBEWITHMD5ANDTRIPLEDES("PBEWithMD5AndTripleDES",
            false, 8, 0,
            false, false,
            SALT_ITERATION_COUNT_ALGO_PARAM_SPEC),

    /*
        This is being kept only to support any older PBE based algorithm other than PBEWithMD5AndTripleDES.
        Example, PBEWithMD5AndDES etc.

        Earlier it was configurable and KDF and Cipher both used to be the same, hence to keep current implementation
        backward compatible, UNKNOWN is being introduced. Here it will return the default values and the provided KDF name as cipher name.

        This is not supposed to be used for new setup.
     */
    UNKNOWN("Unknown",
            false, 8, 0,
            false, false,
            SALT_ITERATION_COUNT_ALGO_PARAM_SPEC);

    private final String                                          cipherTransformation;
    private final boolean                                         isIVRequired;
    private final int                                             cipherIvLengthBytes;
    private final int                                             cipherTagLengthBits;
    private final boolean                                         isDataIntegrityCheckSupported;
    private final boolean                                         isAESWrappingRequiredForCipherTransformation;
    private final Function<AlgoParamSpec, AlgorithmParameterSpec> algoParamSpecFunc;

    SupportedCipherSuite(String cipherTransformation, boolean isIVRequired, int cipherIvLengthBytes, int cipherTagLengthBits, boolean isDataIntegrityCheckSupported,
            boolean isAESWrappingRequiredForCipherTransformation, Function<AlgoParamSpec, AlgorithmParameterSpec> algoParamSpecFunc) {
        this.cipherTransformation                         = cipherTransformation;
        this.isIVRequired                                 = isIVRequired;
        this.cipherIvLengthBytes                          = cipherIvLengthBytes;
        this.cipherTagLengthBits                          = cipherTagLengthBits;
        this.isDataIntegrityCheckSupported                = isDataIntegrityCheckSupported;
        this.isAESWrappingRequiredForCipherTransformation = isAESWrappingRequiredForCipherTransformation;
        this.algoParamSpecFunc                            = algoParamSpecFunc;
    }

    @Override
    public String getCipherTransformation() {
        return this.cipherTransformation;
    }

    @Override
    public boolean isIVRequired() {
        return this.isIVRequired;
    }

    @Override
    public int getCipherIVLengthInBytes() {
        return this.cipherIvLengthBytes;
    }

    @Override
    public int getCipherTagLengthInBits() {
        return this.cipherTagLengthBits;
    }

    @Override
    public boolean isDataIntegrityCheckSupported() {
        return this.isDataIntegrityCheckSupported;
    }

    @Override
    public boolean isAESWrappingRequiredForCipherTransformation() {
        return this.isAESWrappingRequiredForCipherTransformation;
    }

    @Override
    public String getAlgoNameForAlgoParameter() {
        String cipherTransformation = getCipherTransformation();

        String[] splits = cipherTransformation.split("/");

        String cipherAlgoName = splits[0];
        if (splits.length > 1) {
            String mode = splits[1];

            if ("GCM".equalsIgnoreCase(mode)) {
                cipherAlgoName = "GCM";
            }
        }

        return cipherAlgoName;
    }

    @Override
    public AlgorithmParameterSpec getAlgoParamSpec(AlgoParamSpec algoParamSpec) {
        return this.algoParamSpecFunc.apply(algoParamSpec);
    }

    @Override
    public String toString() {
        return this.getCipherTransformation();
    }

    public static RangerCipherSuite convert(String cipherTransformation) {
        RangerCipherSuite cipherSuite;
        try {
            cipherSuite = SupportedCipherSuite.valueOf(cipherTransformation.toUpperCase().replace("/", "_"));
        } catch (IllegalArgumentException e) {
            // means, an unknown or not listed/tested crypto algorithm is being used.
            // It may work but it's not tested.
            // This support is being added just to make it backward compatible, not to break if some different PBE based algo (like PBEWithMD5AndDES) was being used
            // in any environment.

            cipherSuite = new RangerCipherSuite() {
                @Override
                public String getCipherTransformation() {
                    return cipherTransformation;
                }

                @Override
                public boolean isIVRequired() {
                    return UNKNOWN.isIVRequired();
                }

                @Override
                public int getCipherIVLengthInBytes() {
                    return UNKNOWN.getCipherIVLengthInBytes();
                }

                @Override
                public int getCipherTagLengthInBits() {
                    return UNKNOWN.getCipherTagLengthInBits();
                }

                @Override
                public boolean isDataIntegrityCheckSupported() {
                    return UNKNOWN.isDataIntegrityCheckSupported();
                }

                @Override
                public AlgorithmParameterSpec getAlgoParamSpec(AlgoParamSpec algoSpec) {
                    AlgorithmParameterSpec algoParamSpec;
                    if (algoSpec.getIv() != null) {
                        algoParamSpec = SALT_ITERATION_COUNT_IV_ALGO_PARAM_SPEC.apply(algoSpec);
                    } else {
                        algoParamSpec = UNKNOWN.getAlgoParamSpec(algoSpec);
                    }
                    return algoParamSpec;
                }

                @Override
                public boolean isAESWrappingRequiredForCipherTransformation() {
                    return UNKNOWN.isAESWrappingRequiredForCipherTransformation();
                }

                @Override
                public String getAlgoNameForAlgoParameter() {
                    String cipherTransformation = getCipherTransformation();
                    return cipherTransformation.split("/")[0];
                }

                @Override
                public String toString() {
                    return this.getCipherTransformation();
                }
            };
        }
        return cipherSuite;
    }
}
