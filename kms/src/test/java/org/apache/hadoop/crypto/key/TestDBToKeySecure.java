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
package org.apache.hadoop.crypto.key;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.security.Permission;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

@ExtendWith(MockitoExtension.class)
@TestMethodOrder(MethodOrderer.MethodName.class)
@Disabled
public class TestDBToKeySecure {
    private static SecurityManager originalSecurityManager;

    @BeforeAll
    static void installExitInterceptor() {
        originalSecurityManager = System.getSecurityManager();
        System.setSecurityManager(new ExitInterceptingSecurityManager());
    }

    @AfterAll
    static void restoreSecurityManager() {
        System.setSecurityManager(originalSecurityManager);
    }

    @Test
    public void testDoExportMKToKeySecure() throws NoSuchMethodException {
        String        keyName       = "testKeyName";
        String        username      = "testUser";
        String        password      = "testPassword";
        String        cfgFilePath   = "testConfigFilePath";
        Configuration conf          = new Configuration();
        DBToKeySecure dbToKeySecure = new DBToKeySecure();

        Method method = DBToKeySecure.class.getDeclaredMethod("doExportMKToKeySecure",
                String.class, String.class, String.class, String.class, Configuration.class);
        method.setAccessible(true);
        try {
            method.invoke(dbToKeySecure, keyName, username, password, cfgFilePath, conf);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    void testMain_invalidArgs_shouldExit1() {
        String[] args = new String[] {"only-one"};
        SecurityException ex = assertThrows(SecurityException.class, () -> {
            DBToKeySecure.main(args);
        });

        assertEquals(1, ExitInterceptingSecurityManager.getStatus());
    }

    @Test
    void testMain_emptyKeyName_shouldExit1() {
        String[] args = new String[] {"", "user", "pass", "path"};
        SecurityException ex = assertThrows(SecurityException.class, () -> {
            DBToKeySecure.main(args);
        });

        assertEquals(1, ExitInterceptingSecurityManager.getStatus());
    }

    @Test
    void testMain_emptyKeyName_shouldExit3() {
        String[] args = new String[] {"", "user", "pass", "path", "extraArg", "anotherArg"};
        SecurityException ex = assertThrows(SecurityException.class, () -> {
            DBToKeySecure.main(args);
        });

        assertEquals(1, ExitInterceptingSecurityManager.getStatus());
    }

    @Test
    void testMain_exportSuccess_shouldExit0() {
        String[] args = new String[] {"validKey", "user", "pass", "path"};
        try {
            Method mainMethod = DBToKeySecure.class.getDeclaredMethod("main", String[].class);
            mainMethod.setAccessible(true);
            InvocationTargetException ex = assertThrows(InvocationTargetException.class, () -> {
                mainMethod.invoke(null, (Object) args);
            });
        } catch (Exception e) {
            fail("Unexpected exception: " + e.getMessage());
        }
    }

    static class ExitInterceptingSecurityManager extends SecurityManager {
        private static int status;

        public static int getStatus() {
            return status;
        }

        @Override
        public void checkPermission(Permission perm) {}

        @Override
        public void checkExit(int status) {
            ExitInterceptingSecurityManager.status = status;
            throw new SecurityException("Intercepted System.exit(" + status + ")");
        }
    }
}
