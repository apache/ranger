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

import javax.crypto.SealedObject;

import java.security.Key;
import java.util.Optional;

public interface KeyEncryptor {
    EncryptKeyResponse<byte[]> encryptKey(byte[] data, String password, Optional<byte[]> aad) throws RangerKMSCryptoException;

    EncryptKeyResponse<SealedObject> sealKey(Key key, char[] password, Optional<byte[]> aad) throws RangerKMSCryptoException;

    class EncryptKeyResponse<R> {
        private final byte[] salt;
        private final byte[] iv;
        private final R encryptedContent;

        public EncryptKeyResponse(R encryptedContent, byte[] salt, byte[] iv) {
            this.encryptedContent = encryptedContent;
            this.salt = salt;
            this.iv = iv;
        }

        public byte[] getSalt() {
            return salt;
        }

        public byte[] getIv() {
            return iv;
        }

        public R getEncryptedContent() {
            return this.encryptedContent;
        }
    }
}
