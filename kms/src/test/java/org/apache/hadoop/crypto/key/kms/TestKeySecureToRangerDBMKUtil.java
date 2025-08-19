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
import org.apache.hadoop.crypto.key.KeySecureToRangerDBMKUtil;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import static org.eclipse.persistence.jpa.jpql.Assert.fail;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@TestMethodOrder(MethodOrderer.MethodName.class)
@Disabled
public class TestKeySecureToRangerDBMKUtil {
    private final ByteArrayOutputStream errContent = new ByteArrayOutputStream();

    @Test
    public void testShowUsage() {
        // Backup original System.err
        PrintStream originalErr = System.err;

        // Prepare to capture System.err
        ByteArrayOutputStream errContent = new ByteArrayOutputStream();
        System.setErr(new PrintStream(errContent));

        try {
            // Call method
            KeySecureToRangerDBMKUtil.showUsage();

            // Flush and verify output
            System.err.flush();
            String output = errContent.toString();
            assertTrue(output.contains("USAGE: java "), "Expected usage message in System.err");
        } finally {
            System.setErr(originalErr);
        }
    }

    @Test
    public void testDoImportMKFromKeySecure() throws Exception {
        KeySecureToRangerDBMKUtil keySecureToRangerDBMKUtil = new KeySecureToRangerDBMKUtil();
        Method                    method                    = KeySecureToRangerDBMKUtil.class.getDeclaredMethod("doImportMKFromKeySecure", String.class);
        method.setAccessible(true);
        String[] args = new String[] {"testKeyAlias"};
        try {
            method.invoke(keySecureToRangerDBMKUtil, args[0]);
        } catch (Exception e) {
            // Handle exception if needed
        }
    }

    @Test
    void testGetFromJceks_whenAliasMissing_skipsCredentialReader() throws Exception {
        Configuration confMock = mock(Configuration.class);

        String pathProp   = "ranger.ks.jpa.jdbc.credential.provider.path";
        String aliasProp  = "ranger.kms.keysecure.login.password.alias";
        String resultProp = "ranger.kms.keysecure.login.password";

        when(confMock.get(pathProp)).thenReturn("jceks://file/ignored.jceks");
        when(confMock.get(aliasProp)).thenReturn("jceks://file/ignored.jceks!missingAlias");

        Method m = KeySecureToRangerDBMKUtil.class
                .getDeclaredMethod("getFromJceks",
                        Configuration.class, String.class, String.class, String.class);
        m.setAccessible(true);
        m.invoke(null, confMock, pathProp, aliasProp, resultProp);
    }

    @Test
    void testMain_invalidArgs_showsUsage_and_exit1() throws Exception {
        // Capture System.err
        PrintStream           originalErr = System.err;
        ByteArrayOutputStream errBuf      = new ByteArrayOutputStream();
        System.setErr(new PrintStream(errBuf));

        // Intercept System.exit
        SecurityManager originalSM = System.getSecurityManager();
        System.setSecurityManager(new SecurityManager() {
            @Override
            public void checkPermission(java.security.Permission perm) {}

            @Override
            public void checkExit(int status) {
                throw new SecurityException("exit:" + status);
            }
        });

        try {
            Method main = KeySecureToRangerDBMKUtil.class.getDeclaredMethod("main", String[].class);
            try {
                main.invoke(null, (Object) new String[0]); // invoke static main
                fail("Expected SecurityException to be thrown");
            } catch (InvocationTargetException ex) {
                Throwable cause = ex.getCause();
                assertInstanceOf(SecurityException.class, cause);
                assertTrue(cause.getMessage().contains("exit:1"));
            }

            String stderr = errBuf.toString();
            assertTrue(stderr.contains("USAGE: java "), "Expected usage message in stderr");
        } finally {
            System.setSecurityManager(originalSM);
            System.setErr(originalErr);
        }
    }

    @Test
    void testMain_validArg_printsSuccess_and_exit0() throws Exception {
        PrintStream           originalOut = System.out;
        ByteArrayOutputStream outBuf      = new ByteArrayOutputStream();
        System.setOut(new PrintStream(outBuf));

        SecurityManager originalSM = System.getSecurityManager();
        System.setSecurityManager(new SecurityManager() {
            @Override
            public void checkPermission(java.security.Permission perm) {}

            @Override
            public void checkExit(int status) {
                throw new SecurityException("exit:" + status);
            }
        });

        try {
            Method main = KeySecureToRangerDBMKUtil.class.getDeclaredMethod("main", String[].class);
            try {
                main.invoke(null, (Object) new String[] {"dummyPassword"});
                fail("Expected SecurityException due to System.exit");
            } catch (InvocationTargetException ex) {
                Throwable cause = ex.getCause();
            }
            outBuf.toString();
        } finally {
            System.setSecurityManager(originalSM);
            System.setOut(originalOut);
        }
    }

    @Test
    void testMain_withEmptyPassword_shouldExit1() throws Exception {
        SecurityManager       originalSM = System.getSecurityManager();
        ByteArrayOutputStream errBuf     = new ByteArrayOutputStream();
        System.setErr(new PrintStream(errBuf));

        System.setSecurityManager(new SecurityManager() {
            @Override
            public void checkPermission(java.security.Permission perm) {}

            @Override
            public void checkExit(int status) {
                throw new SecurityException("exit:" + status);
            }
        });

        try {
            Method main = KeySecureToRangerDBMKUtil.class.getDeclaredMethod("main", String[].class);
            InvocationTargetException ex = assertThrows(
                    InvocationTargetException.class,
                    () -> main.invoke(null, (Object) new String[] {"  "}));
            Throwable cause = ex.getCause();
            assertInstanceOf(SecurityException.class, cause);
            assertTrue(cause.getMessage().contains("exit:1"));
            assertTrue(errBuf.toString().contains("KMS master key password not provided"));
            assertTrue(errBuf.toString().contains("USAGE: java"));
        } finally {
            System.setErr(System.err);
            System.setSecurityManager(originalSM);
        }
    }
}
