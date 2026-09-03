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

import org.apache.hadoop.thirdparty.com.google.common.base.Joiner;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class PasswordUtilsTest {
    @Test
    public void testEncrypt() throws IOException {
        // encryption of password that contains no configuration info is using legacy
        // cryptography algorithm for backward compatibility.
        String encryptedPassword = PasswordUtils.encryptPassword("secretPasswordNoOneWillEverKnow");

        assertNotNull(encryptedPassword);
        assertEquals("ljoJ3gf4T018Xr+BujPAqBDW8Onp1PqprsLKmxus8pGGBETtAVU6OQ==", encryptedPassword);
    }

    @Test
    public void testDecrypt() throws IOException {
        String decryptedPassword = PasswordUtils.decryptPassword("ljoJ3gf4T018Xr+BujPAqBDW8Onp1PqprsLKmxus8pGGBETtAVU6OQ==");

        assertNotNull(decryptedPassword);
        assertEquals("secretPasswordNoOneWillEverKnow", decryptedPassword);
    }

    @Test
    public void testEncryptWithExplicitDefaultWeakAlgorithm() throws IOException {
        String freeTextPasswordMetaData = join("PBEWithMD5AndDES", "ENCRYPT_KEY", "SALTSALT", "4");
        String encryptedPassword        = PasswordUtils.encryptPassword(join(freeTextPasswordMetaData, "secretPasswordNoOneWillEverKnow"));

        assertNotNull(encryptedPassword);

        String decryptedPassword = PasswordUtils.decryptPassword(join(freeTextPasswordMetaData, encryptedPassword));

        assertEquals("secretPasswordNoOneWillEverKnow", decryptedPassword);
    }

    @Test
    public void testEncryptWithSHA1AndDESede() throws IOException {
        String freeTextPasswordMetaData = join("PBEWithSHA1AndDESede", "ENCRYPT_KEY", "SALTSALT", "4");
        String encryptedPassword        = PasswordUtils.encryptPassword(join(freeTextPasswordMetaData, "secretPasswordNoOneWillEverKnow"));

        assertNotNull(encryptedPassword);

        String decryptedPassword = PasswordUtils.decryptPassword(join(freeTextPasswordMetaData, encryptedPassword));

        assertEquals("secretPasswordNoOneWillEverKnow", decryptedPassword);
    }

    @Test
    public void testEncryptWithSHA512AndAES128() throws IOException, NoSuchAlgorithmException {
        String freeTextPasswordMetaData = join("PBEWITHHMACSHA512ANDAES_128", "ENCRYPT_KEY", "SALTSALT", "4", PasswordUtils.generateIvIfNeeded("PBEWITHHMACSHA512ANDAES_128"));
        String encryptedPassword        = PasswordUtils.encryptPassword(join(freeTextPasswordMetaData, "secretPasswordNoOneWillEverKnow"));

        assertNotNull(encryptedPassword);

        String decryptedPassword = PasswordUtils.decryptPassword(join(freeTextPasswordMetaData, encryptedPassword));

        assertEquals("secretPasswordNoOneWillEverKnow", decryptedPassword);
    }

    @Test
    public void testEncryptWithSHA512AndAES128WithMultipleComasInPass() throws IOException {
        String freeTextPasswordMetaData = "PBEWITHHMACSHA512ANDAES_128,tzL1AKl5uc4NKYaoQ4P3WLGIBFPXWPWdu1fRm9004jtQiV,f77aLYLo,1000,9f3vNL0ijeHF4RWN/yUo0A==";
        String freeTextPassword         = "asd,qwe,123";
        String encryptedPassword        = PasswordUtils.encryptPassword(join(freeTextPasswordMetaData, freeTextPassword));

        assertNotNull(encryptedPassword);

        String decryptedPassword = PasswordUtils.decryptPassword(join(freeTextPasswordMetaData, encryptedPassword));

        assertEquals(freeTextPassword, decryptedPassword);
    }

    @Test
    public void testEncryptWithSHA512AndAES128WithSingleComaInPass() throws IOException {
        String freeTextPasswordMetaData = "PBEWITHHMACSHA512ANDAES_128,tzL1AKl5uc4NKYaoQ4P3WLGIBFPXWPWdu1fRm9004jtQiV,f77aLYLo,1000,9f3vNL0ijeHF4RWN/yUo0A==";
        String freeTextPassword         = "asd,123";
        String encryptedPassword        = PasswordUtils.encryptPassword(join(freeTextPasswordMetaData, freeTextPassword));

        assertNotNull(encryptedPassword);

        String decryptedPassword = PasswordUtils.decryptPassword(join(freeTextPasswordMetaData, encryptedPassword));

        assertEquals(freeTextPassword, decryptedPassword);
    }

    @Test
    public void testEncryptWithSHA512AndAES128EndingWithSingleComa() throws IOException {
        String freeTextPasswordMetaData = "PBEWITHHMACSHA512ANDAES_128,tzL1AKl5uc4NKYaoQ4P3WLGIBFPXWPWdu1fRm9004jtQiV,f77aLYLo,1000,9f3vNL0ijeHF4RWN/yUo0A==";
        String freeTextPassword         = "asd,";
        String encryptedPassword        = PasswordUtils.encryptPassword(join(freeTextPasswordMetaData, freeTextPassword));

        assertNotNull(encryptedPassword);

        String decryptedPassword = PasswordUtils.decryptPassword(join(freeTextPasswordMetaData, encryptedPassword));

        assertEquals(freeTextPassword, decryptedPassword);
    }

    @Test
    public void testEncryptWithSHA512AndAES128StartingWithSingleComa() throws IOException {
        String freeTextPasswordMetaData = "PBEWITHHMACSHA512ANDAES_128,tzL1AKl5uc4NKYaoQ4P3WLGIBFPXWPWdu1fRm9004jtQiV,f77aLYLo,1000,9f3vNL0ijeHF4RWN/yUo0A==";
        String freeTextPassword         = ",asd";
        String encryptedPassword        = PasswordUtils.encryptPassword(join(freeTextPasswordMetaData, freeTextPassword));

        assertNotNull(encryptedPassword);

        String decryptedPassword = PasswordUtils.decryptPassword(join(freeTextPasswordMetaData, encryptedPassword));

        assertEquals(freeTextPassword, decryptedPassword);
    }

    @Test
    public void testEncryptWithSHA512AndAES128MultipleComasInTheEnd() throws IOException {
        String freeTextPasswordMetaData = "PBEWITHHMACSHA512ANDAES_128,tzL1AKl5uc4NKYaoQ4P3WLGIBFPXWPWdu1fRm9004jtQiV,f77aLYLo,1000,9f3vNL0ijeHF4RWN/yUo0A==";
        String freeTextPassword         = "asd,,";
        String encryptedPassword        = PasswordUtils.encryptPassword(join(freeTextPasswordMetaData, freeTextPassword));

        assertNotNull(encryptedPassword);

        String decryptedPassword = PasswordUtils.decryptPassword(join(freeTextPasswordMetaData, encryptedPassword));

        assertEquals(freeTextPassword, decryptedPassword);
    }

    @Test
    public void testEncryptWithSHA512AndAES128MultipleComasSurroundingText() throws IOException {
        String freeTextPasswordMetaData = "PBEWITHHMACSHA512ANDAES_128,tzL1AKl5uc4NKYaoQ4P3WLGIBFPXWPWdu1fRm9004jtQiV,f77aLYLo,1000,9f3vNL0ijeHF4RWN/yUo0A==";
        String freeTextPassword         = ",,a,,";
        String encryptedPassword        = PasswordUtils.encryptPassword(join(freeTextPasswordMetaData, freeTextPassword));

        assertNotNull(encryptedPassword);

        String decryptedPassword = PasswordUtils.decryptPassword(join(freeTextPasswordMetaData, encryptedPassword));

        assertEquals(freeTextPassword, decryptedPassword);
    }

    @Test
    public void testEncryptWithSHA512AndAES128MultipleComasBeforeText() throws IOException {
        String freeTextPasswordMetaData = "PBEWITHHMACSHA512ANDAES_128,tzL1AKl5uc4NKYaoQ4P3WLGIBFPXWPWdu1fRm9004jtQiV,f77aLYLo,1000,9f3vNL0ijeHF4RWN/yUo0A==";
        String freeTextPassword         = ",,,a";
        String encryptedPassword        = PasswordUtils.encryptPassword(join(freeTextPasswordMetaData, freeTextPassword));

        assertNotNull(encryptedPassword);

        String decryptedPassword = PasswordUtils.decryptPassword(join(freeTextPasswordMetaData, encryptedPassword));

        assertEquals(freeTextPassword, decryptedPassword);
    }

    @Test
    public void testEncryptWithSHA512AndAES128MultipleComasOnlyPassword() throws IOException {
        String freeTextPasswordMetaData = "PBEWITHHMACSHA512ANDAES_128,tzL1AKl5uc4NKYaoQ4P3WLGIBFPXWPWdu1fRm9004jtQiV,f77aLYLo,1000,9f3vNL0ijeHF4RWN/yUo0A==";
        String freeTextPassword         = ",,,";
        String encryptedPassword        = PasswordUtils.encryptPassword(join(freeTextPasswordMetaData, freeTextPassword));

        assertNotNull(encryptedPassword);

        String decryptedPassword = PasswordUtils.decryptPassword(join(freeTextPasswordMetaData, encryptedPassword));

        assertEquals(freeTextPassword, decryptedPassword);
    }

    @Test
    public void testEncryptWithSHA512AndAES128SingleComaOnlyPassword() throws IOException {
        String freeTextPasswordMetaData = "PBEWITHHMACSHA512ANDAES_128,tzL1AKl5uc4NKYaoQ4P3WLGIBFPXWPWdu1fRm9004jtQiV,f77aLYLo,1000,9f3vNL0ijeHF4RWN/yUo0A==";
        String freeTextPassword         = ",";
        String encryptedPassword        = PasswordUtils.encryptPassword(join(freeTextPasswordMetaData, freeTextPassword));

        assertNotNull(encryptedPassword);

        String decryptedPassword = PasswordUtils.decryptPassword(join(freeTextPasswordMetaData, encryptedPassword));

        assertEquals(freeTextPassword, decryptedPassword);
    }

    @Test
    public void testDecryptEmptyResultInNull() throws Throwable {
        String string0 = PasswordUtils.decryptPassword("");

        assertNull(string0);
    }

    @Test
    public void testEncryptDecryptV2RoundTrip() throws Exception {
        char[] key = "operator-configured-key-2026".toCharArray();
        String storedValue = PasswordUtils.encryptPasswordV2("secretPasswordNoOneWillEverKnow", RangerSupportedCryptoAlgo.PBEWITHHMACSHA512ANDAES_128,
                key, "f77aLYLo".getBytes(), 1000);
        assertNotNull(storedValue);
        assertEquals("secretPasswordNoOneWillEverKnow", PasswordUtils.decryptPasswordV2(storedValue, key));
    }

    @Test
    public void testEncryptV2NeverEmbedsKeyInStoredValue() throws Exception {
        char[] key = "operator-configured-key-2026".toCharArray();
        String storedValue = PasswordUtils.encryptPasswordV2("secretPasswordNoOneWillEverKnow", RangerSupportedCryptoAlgo.PBEWITHHMACSHA512ANDAES_128,
                key, "f77aLYLo".getBytes(), 1000);
        assertFalse(storedValue.contains(new String(key)), "the encryption key must never appear in the stored value");
    }

    @Test
    public void testIsV2FormatDetection() throws Exception {
        char[] key = "operator-configured-key-2026".toCharArray();
        String v2Value = PasswordUtils.encryptPasswordV2("secretPasswordNoOneWillEverKnow", RangerSupportedCryptoAlgo.PBEWITHHMACSHA512ANDAES_128,
                key, "f77aLYLo".getBytes(), 1000);
        String v1Value = PasswordUtils.encryptPassword("secretPasswordNoOneWillEverKnow");
        assertTrue(PasswordUtils.isV2Format(v2Value));
        assertFalse(PasswordUtils.isV2Format(v1Value));
        assertFalse(PasswordUtils.isV2Format("secretPasswordNoOneWillEverKnow")); // plaintext, no format at all
        assertFalse(PasswordUtils.isV2Format(""));
        assertFalse(PasswordUtils.isV2Format(null));
    }

    @Test
    public void testDecryptV2WithWrongKeyThrows() throws Exception {
        String storedValue = PasswordUtils.encryptPasswordV2("secretPasswordNoOneWillEverKnow", RangerSupportedCryptoAlgo.PBEWITHHMACSHA512ANDAES_128,
                "operator-configured-key-2026".toCharArray(), "f77aLYLo".getBytes(), 1000);
        assertThrows(Exception.class, () -> PasswordUtils.decryptPasswordV2(storedValue, "a-completely-different-key".toCharArray()),
                "decrypting a v2 value under the wrong key must throw, never silently return garbage or the original ciphertext");
    }

    @Test
    public void testDecryptV2RejectsLegacyV1Value() throws Exception {
        String v1Value = PasswordUtils.encryptPassword(join("PBEWITHHMACSHA512ANDAES_128", "tzL1AKl5uc4NKYaoQ4P3WLGIBFPXWPWdu1fRm9004jtQiV", "f77aLYLo", "1000",
                PasswordUtils.generateIvIfNeeded("PBEWITHHMACSHA512ANDAES_128"), "secretPasswordNoOneWillEverKnow"));
        String v1StoredValue = join("PBEWITHHMACSHA512ANDAES_128", "tzL1AKl5uc4NKYaoQ4P3WLGIBFPXWPWdu1fRm9004jtQiV", "f77aLYLo", "1000",
                PasswordUtils.generateIvIfNeeded("PBEWITHHMACSHA512ANDAES_128"), v1Value);
        assertFalse(PasswordUtils.isV2Format(v1StoredValue));
        assertThrows(Exception.class, () -> PasswordUtils.decryptPasswordV2(v1StoredValue, "operator-configured-key-2026".toCharArray()));
    }

    @Test
    public void testEncryptV2CommaInKeyDoesNotCorruptFormat() throws Exception {
        char[] keyWithComma = "abc,def-key-with-a-comma".toCharArray();
        String storedValue = PasswordUtils.encryptPasswordV2("secretPasswordNoOneWillEverKnow", RangerSupportedCryptoAlgo.PBEWITHHMACSHA512ANDAES_128,
                keyWithComma, "f77aLYLo".getBytes(), 1000);
        assertTrue(PasswordUtils.isV2Format(storedValue));
        assertEquals("secretPasswordNoOneWillEverKnow", PasswordUtils.decryptPasswordV2(storedValue, keyWithComma));
    }

    @Test
    public void testMigrationScenarioLegacyDecryptThenV2Encrypt() throws Exception {
        // Mirrors PatchServicePasswordV2Migration_J10067.migrateValue(): decrypt an existing
        // legacy-format row (self-contained, no external key needed), then re-encrypt it as v2
        // under the operator's configured key, and confirm the round trip reproduces the original.
        char[] newKey = "operator-configured-key-2026".toCharArray();

        // generateIvIfNeeded() returns a fresh/random IV on every call, so it must be generated
        // once and reused for both the inner encrypted value and the outer metadata string below
        // — a real legacy row always has the two agree (that's how it was originally written).
        String iv = PasswordUtils.generateIvIfNeeded("PBEWITHHMACSHA512ANDAES_128");
        String legacyValue = PasswordUtils.encryptPassword(join("PBEWITHHMACSHA512ANDAES_128", "tzL1AKl5uc4NKYaoQ4P3WLGIBFPXWPWdu1fRm9004jtQiV", "f77aLYLo", "1000",
                iv, "existingServicePassword"));
        String legacyStoredValue = join("PBEWITHHMACSHA512ANDAES_128", "tzL1AKl5uc4NKYaoQ4P3WLGIBFPXWPWdu1fRm9004jtQiV", "f77aLYLo", "1000",
                iv, legacyValue);
        String decryptedPwd = PasswordUtils.decryptPassword(legacyStoredValue);
        assertEquals("existingServicePassword", decryptedPwd);
        String migratedValue = PasswordUtils.encryptPasswordV2(decryptedPwd, RangerSupportedCryptoAlgo.PBEWITHHMACSHA512ANDAES_128, newKey, "f77aLYLo".getBytes(), 1000);
        assertTrue(PasswordUtils.isV2Format(migratedValue));
        assertNotEquals(legacyStoredValue, migratedValue);
        assertEquals("existingServicePassword", PasswordUtils.decryptPasswordV2(migratedValue, newKey));
    }

    @Test
    public void testDecryptV2RejectsEmptyOrNullKey() {
        String storedValue = "v2,PBEWITHHMACSHA512ANDAES_128,c29tZXNhbHQ=,1000,someciphertext";
        assertThrows(IllegalArgumentException.class, () -> PasswordUtils.decryptPasswordV2(storedValue, null), "a null key must never be silently accepted for a v2 decrypt");
        assertThrows(IllegalArgumentException.class, () -> PasswordUtils.decryptPasswordV2(storedValue, new char[0]), "an empty key must never be silently accepted for a v2 decrypt");
    }

    @Test
    public void testEncryptV2RejectsEmptyOrNullKey() {
        assertThrows(IllegalArgumentException.class, () -> PasswordUtils.encryptPasswordV2("secretPasswordNoOneWillEverKnow",
                        RangerSupportedCryptoAlgo.PBEWITHHMACSHA512ANDAES_128, null, "f77aLYLo".getBytes(), 1000),
                "a null key must never be silently accepted for a v2 encrypt");
        assertThrows(IllegalArgumentException.class, () -> PasswordUtils.encryptPasswordV2("secretPasswordNoOneWillEverKnow",
                        RangerSupportedCryptoAlgo.PBEWITHHMACSHA512ANDAES_128, new char[0], "f77aLYLo".getBytes(), 1000),
                "an empty key must never be silently accepted for a v2 encrypt");
    }

    @Test
    public void testValidateEncryptionKeyConfiguredRejectsDefaultKey() {
        IllegalStateException ex = assertThrows(IllegalStateException.class,
                () -> PasswordUtils.validateEncryptionKeyConfigured(PasswordUtils.DEFAULT_ENCRYPT_KEY.toCharArray()),
                "the well-known default key must never be accepted as 'configured'");
        assertTrue(ex.getMessage().contains("ranger.password.encryption.key"), "the error should name the property an operator needs to set");
    }

    @Test
    public void testValidateEncryptionKeyConfiguredRejectsNullOrEmptyKey() {
        assertThrows(IllegalArgumentException.class, () -> PasswordUtils.validateEncryptionKeyConfigured(null));
        assertThrows(IllegalArgumentException.class, () -> PasswordUtils.validateEncryptionKeyConfigured(new char[0]));
    }

    @Test
    public void testValidateEncryptionKeyConfiguredAcceptsRealKey() {
        assertDoesNotThrow(() -> PasswordUtils.validateEncryptionKeyConfigured("a-real-operator-configured-key".toCharArray()));
    }

    @Test
    public void testIsLegacyFormatDetectsRealLegacyValue() throws Exception {
        String iv = PasswordUtils.generateIvIfNeeded("PBEWITHHMACSHA512ANDAES_128");
        String legacyMetadata = join("PBEWITHHMACSHA512ANDAES_128", "tzL1AKl5uc4NKYaoQ4P3WLGIBFPXWPWdu1fRm9004jtQiV", "f77aLYLo", "1000", iv);
        String encryptedPassword = PasswordUtils.encryptPassword(join(legacyMetadata, "secretPasswordNoOneWillEverKnow"));
        String legacyStoredValue = join(legacyMetadata, encryptedPassword);
        assertTrue(PasswordUtils.isLegacyFormat(legacyStoredValue));
    }

    @Test
    public void testIsLegacyFormatRejectsPlaintextWithComma() {
        assertFalse(PasswordUtils.isLegacyFormat("my,password,with,commas"));
        assertFalse(PasswordUtils.isLegacyFormat("plaintext-no-comma-at-all"));
        assertFalse(PasswordUtils.isLegacyFormat(""));
        assertFalse(PasswordUtils.isLegacyFormat(null));
    }

    @Test
    public void testIsLegacyFormatRejectsV2Value() throws Exception {
        String v2Value = PasswordUtils.encryptPasswordV2("secretPasswordNoOneWillEverKnow", RangerSupportedCryptoAlgo.PBEWITHHMACSHA512ANDAES_128,
                "operator-configured-key-2026".toCharArray(), "f77aLYLo".getBytes(), 1000);
        assertFalse(PasswordUtils.isLegacyFormat(v2Value), "a v2-format value is not legacy-format, even though it also contains commas");
    }

    @Test
    public void testIsLegacyFormatRejectsTooFewFields() {
        // Fewer than 5 comma-separated fields can't be a real "ALGO,KEY,SALT,ITER,CIPHERTEXT" value.
        assertFalse(PasswordUtils.isLegacyFormat("PBEWithMD5AndDES,key,salt,4"));
    }

    private String join(String... strings) {
        return Joiner.on(",").skipNulls().join(strings);
    }
}
