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

import javax.crypto.SecretKey;
import javax.crypto.spec.PBEKeySpec;

import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.spec.InvalidKeySpecException;

public interface RangerKMSKeyGenerator {
    /**
     * This method can be used to generate a key that doesn't need to be derived (or re-created) from any input source.
     * @param cipher
     * @param keySize
     * @return
     * @throws NoSuchAlgorithmException
     */
    SecretKey generateKey(String cipher, int keySize) throws NoSuchAlgorithmException;

    SecretKey generateKey(String cipher, int keySize, String provider) throws NoSuchAlgorithmException, NoSuchProviderException;

    /**
     * This method can be used to generate a key that needs to be derived ( or re-created) from some input source.
     * @param pbeKeySpec
     * @param cryptoAlgo
     * @return
     * @throws NoSuchAlgorithmException
     * @throws InvalidKeySpecException
     */
    SecretKey generateKey(PBEKeySpec pbeKeySpec, String cryptoAlgo) throws NoSuchAlgorithmException, InvalidKeySpecException; // SecretKeyFactory factory =
}
