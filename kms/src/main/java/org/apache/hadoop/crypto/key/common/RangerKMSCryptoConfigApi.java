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

public interface RangerKMSCryptoConfigApi {
    RangerCipherSuite getCipher();

    int getKeySize();

    int getSaltSize();

    String getSalt();

    RangerCryptoKDFSuite getCryptoAlgorithm();

    String getMessageDigestAlgorithm();

    int getIterationCount();

    RangerKMSKeyCryptoAPI.KMSCryptoParams getCryptoParams();

    // For FIPS, salt size must be at least 128 bits, that is, at least 16 in length.
    static int calculateCompliantSaltSize(int saltSize, RangerCryptoKDFSuite encrAlgoSuite) {
        int compliantSaltSize = saltSize;
        if (encrAlgoSuite.getMinSaltSize().isPresent()) {
            int minSaltSize = encrAlgoSuite.getMinSaltSize().get();
            while (compliantSaltSize < minSaltSize) {
                compliantSaltSize = compliantSaltSize * 2;
            }
        }

        return compliantSaltSize;
    }
}
