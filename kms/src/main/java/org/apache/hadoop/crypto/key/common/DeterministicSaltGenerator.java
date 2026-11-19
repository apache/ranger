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

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class DeterministicSaltGenerator implements SaltGenerator {
    private String mdAlgo;
    private String initialSalt;

    DeterministicSaltGenerator(String mdAlgo, String initialSalt) {
        this.mdAlgo = mdAlgo;
        this.initialSalt = initialSalt;
    }

    @Override
    public byte[] generateSalt(int saltSize) throws RangerKMSCryptoException {
        byte[] salt = null;
        try {
            MessageDigest md = MessageDigest.getInstance(mdAlgo);
            byte[]        saltGen = md.digest(initialSalt.getBytes());
            salt    = new byte[saltSize];

            System.arraycopy(saltGen, 0, salt, 0, saltSize);
        } catch (NoSuchAlgorithmException e) {
            throw new RangerKMSCryptoException("Error while generating salt", e);
        }

        return salt;
    }
}
