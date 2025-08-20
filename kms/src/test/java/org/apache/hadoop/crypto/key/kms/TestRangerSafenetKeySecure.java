/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.crypto.key.kms;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.key.RangerSafenetKeySecure;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.security.KeyStore;
import java.security.Provider;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Answers.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;

@ExtendWith(MockitoExtension.class)
@TestMethodOrder(MethodOrderer.MethodName.class)
@Disabled
public class TestRangerSafenetKeySecure {
    @Test
    public void testGenerateMasterKey_WithNullKeystore_ShouldReturnFalse() throws Exception {
        RangerSafenetKeySecure secure = mock(RangerSafenetKeySecure.class, CALLS_REAL_METHODS);

        Field storeField = RangerSafenetKeySecure.class.getDeclaredField("myStore");
        storeField.setAccessible(true);
        storeField.set(secure, null);  // force myStore to null

        boolean result = secure.generateMasterKey("password");
        assertFalse(result);
    }

    @Test
    public void testConstructorWithMissingConfigFile_ShouldLogError() {
        Configuration conf = new Configuration();
        conf.set("ranger.kms.keysecure.sunpkcs11.cfg.filepath", "non-existent.cfg");
        conf.set("ranger.kms.keysecure.masterkey.size", "256");
        conf.set("ranger.kms.keysecure.masterkey.name", "RANGERMK");
        conf.set("ranger.kms.keysecure.provider.type", "SunPKCS11");
        conf.set("ranger.kms.keysecure.login", "testpass");

        assertThrows(Exception.class, () -> new RangerSafenetKeySecure(conf));
    }

    @Test
    public void testGetMasterKey_WithNullKeystore_ShouldReturnNull() throws Throwable {
        RangerSafenetKeySecure secure = mock(RangerSafenetKeySecure.class, CALLS_REAL_METHODS);

        Field storeField = RangerSafenetKeySecure.class.getDeclaredField("myStore");
        storeField.setAccessible(true);
        storeField.set(secure, null);

        String key = secure.getMasterKey("password");
        assertNull(key);
    }

    @Test
    public void testSetMasterKey_WithNullKeystore_ShouldReturnFalse() throws Exception {
        RangerSafenetKeySecure secure = mock(RangerSafenetKeySecure.class, CALLS_REAL_METHODS);

        Field storeField = RangerSafenetKeySecure.class.getDeclaredField("myStore");
        storeField.setAccessible(true);
        storeField.set(secure, null);

        boolean result = secure.setMasterKey("pass", "mockKey".getBytes(), new Configuration());
        assertFalse(result);
    }

    @Test
    public void testGetJavaVersion() throws Exception {
        RangerSafenetKeySecure secure = mock(RangerSafenetKeySecure.class, CALLS_REAL_METHODS);

        Method getJavaVersionMethod = RangerSafenetKeySecure.class.getDeclaredMethod("getJavaVersion");
        getJavaVersionMethod.setAccessible(true);

        int version = (int) getJavaVersionMethod.invoke(secure);
        assertTrue(version >= 8);
    }

    @Test
    public void testGetMasterKey() throws Throwable {
        RangerSafenetKeySecure secure = mock(RangerSafenetKeySecure.class, CALLS_REAL_METHODS);

        Field storeField = RangerSafenetKeySecure.class.getDeclaredField("myStore");
        storeField.setAccessible(true);
        storeField.set(secure, null);

        String key = secure.getMasterKey("password");
        assertNull(key);
    }

    @Test
    public void testAddProviderManually_ShouldHitLoadAndLog() throws Exception {
        Configuration conf = new Configuration();
        conf.set("ranger.kms.keysecure.sunpkcs11.cfg.filepath", "dummy.cfg");
        conf.set("ranger.kms.keysecure.masterkey.name", "RANGERMK");
        conf.set("ranger.kms.keysecure.login", "dummy");
        conf.set("ranger.kms.keysecure.provider.type", "DummyProvider");

        RangerSafenetKeySecure secure = mock(RangerSafenetKeySecure.class, Mockito.CALLS_REAL_METHODS);

        Provider dummyProvider = new Provider("DummyProvider", 1.0, "dummy desc") {};
        KeyStore dummyKeystore = mock(KeyStore.class);

        Field providerField = RangerSafenetKeySecure.class.getDeclaredField("provider");
        providerField.setAccessible(true);
        providerField.set(secure, dummyProvider);

        Field storeField = RangerSafenetKeySecure.class.getDeclaredField("myStore");
        storeField.setAccessible(true);
        storeField.set(secure, dummyKeystore);

        // Now call methods that use provider & keystore
        boolean result = secure.generateMasterKey("pass");
        assertFalse(result); // because it won't actually generate anything, but still executes lines
    }
}
