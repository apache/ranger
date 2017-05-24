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

package org.apache.ranger.plugin.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.io.IOException;

import org.junit.Test;

public class PasswordUtilsTest {

    @Test
    public void testEncrypt() throws IOException {
        String string0 = PasswordUtils.encryptPassword("secretPasswordNoOneWillEverKnow");
        assertNotNull(string0);
        assertEquals("ljoJ3gf4T018Xr+BujPAqBDW8Onp1PqprsLKmxus8pGGBETtAVU6OQ==", string0);
    }

    @Test
    public void testDecrypt() throws IOException {
        String string0 = PasswordUtils.decryptPassword("ljoJ3gf4T018Xr+BujPAqBDW8Onp1PqprsLKmxus8pGGBETtAVU6OQ==");
        assertNotNull(string0);
        assertEquals("secretPasswordNoOneWillEverKnow", string0);
    }

    @Test
    public void testEncryptWithExplicitDefaultWeakAlgorithm() throws IOException {
        String string0 = PasswordUtils
                .encryptPassword("PBEWithMD5AndDES,ENCRYPT_KEY,SALTSALT,4,secretPasswordNoOneWillEverKnow");
        assertNotNull(string0);
        String string1 = PasswordUtils.decryptPassword("PBEWithMD5AndDES,ENCRYPT_KEY,SALTSALT,4," + string0);
        assertNotNull("secretPasswordNoOneWillEverKnow", string1);
    }

    @Test
    public void testEncryptWithSHA1AndDESede() throws IOException {
        String string0 = PasswordUtils
                .encryptPassword("PBEWithSHA1AndDESede,ENCRYPT_KEY,SALTSALT,4,secretPasswordNoOneWillEverKnow");
        assertNotNull(string0);
        String string1 = PasswordUtils.decryptPassword("PBEWithSHA1AndDESede,ENCRYPT_KEY,SALTSALT,4," + string0);
        assertNotNull("secretPasswordNoOneWillEverKnow", string1);
    }

    @Test
    public void testDecryptEmptyResultInNull() throws Throwable {
        String string0 = PasswordUtils.decryptPassword("");
        assertNull(string0);
    }
}
