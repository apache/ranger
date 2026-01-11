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
import org.apache.hadoop.crypto.key.JKS2RangerUtil;
import org.apache.hadoop.crypto.key.RangerMasterKey;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.security.Permission;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;

@ExtendWith(MockitoExtension.class)
@TestMethodOrder(MethodOrderer.MethodName.class)
@Disabled
public class TestJKS2RangerUtil {
    @Test
    public void testGetFromJceks() throws Exception {
        Configuration conf = new org.apache.hadoop.conf.Configuration();
        conf.set("keystore.path", "mock/path/to/jceks.jceks");
        conf.set("keystore.alias", "test-alias");

        JKS2RangerUtil jKS2RangerUtil = new JKS2RangerUtil();

        Method method = JKS2RangerUtil.class.getDeclaredMethod("getFromJceks", Configuration.class, String.class, String.class, String.class);
        method.setAccessible(true);

        String targetKey = "db.password";

        method.invoke(jKS2RangerUtil, conf, "keystore.path", "keystore.alias", targetKey);
    }

    @Test
    public void testDoImportKeysFromJKS_elseBranch() throws Exception {
        JKS2RangerUtil util = new JKS2RangerUtil();

        // Dummy config with no KeySecure or Azure enabled
        Configuration conf = mock(Configuration.class);
        lenient().when(conf.get("ranger.plugin.kms.encryption.key.password")).thenReturn("testpass");
        lenient().when(conf.get(anyString())).thenReturn(null); // ensures keysecure/azure conditions fail

        // Create a dummy keystore file
        File dummyJceks = File.createTempFile("dummy", ".jceks");
        dummyJceks.deleteOnExit();

        Method method = JKS2RangerUtil.class.getDeclaredMethod("doImportKeysFromJKS", String.class, String.class);
        method.setAccessible(true);

        try {
            method.invoke(util, dummyJceks.getAbsolutePath(), "jceks");
            //
        } catch (InvocationTargetException e) {
            Throwable cause = e.getCause();
            assertNotNull(cause);
        }
    }

    @Test
    public void testDoImportKeysFromJKS_if() throws Throwable {
        JKS2RangerUtil util = new JKS2RangerUtil();

        RangerMasterKey rangerMasterKey = mock(RangerMasterKey.class);
        Method          method          = JKS2RangerUtil.class.getDeclaredMethod("doImportKeysFromJKS", String.class, String.class);
        method.setAccessible(true);

        try {
            method.invoke(util, "nonexistent.jceks", "jceks");
        } catch (InvocationTargetException e) {
        }
    }

    @Test
    public void testShowUsage() {
        PrintStream originalErr = System.err;

        ByteArrayOutputStream errContent = new ByteArrayOutputStream();
        System.setErr(new PrintStream(errContent));

        JKS2RangerUtil.showUsage();

        System.setErr(originalErr);

        String output = errContent.toString();

        assertTrue(output.contains("USAGE: java " + JKS2RangerUtil.class.getName()));
        assertTrue(output.contains("KeyStoreType"));
        assertTrue(output.contains("keystore password"));
        assertTrue(output.contains("key password"));
    }

    @Test
    public void testMain_NoArgs_ShouldPrintUsageAndExit() {
        SecurityManager originalSM  = System.getSecurityManager();
        PrintStream     originalErr = System.err;

        ByteArrayOutputStream errContent = new ByteArrayOutputStream();
        System.setErr(new PrintStream(errContent));

        System.setSecurityManager(new SecurityManager() {
            public void checkPermission(Permission perm) {}

            @Override
            public void checkExit(int status) {
                throw new SecurityException("Intercepted System.exit(" + status + ")");
            }
        });

        try {
            JKS2RangerUtil.main(new String[0]);
            //
        } catch (SecurityException ex) {
            assertTrue(ex.getMessage().contains("Intercepted System.exit(1)"));
        } finally {
            System.setSecurityManager(originalSM);
            System.setErr(originalErr);
        }

        String output = errContent.toString();
        assertTrue(output.contains("Invalid number of parameters found."));
        assertTrue(output.contains("USAGE: java " + JKS2RangerUtil.class.getName()));
    }

    @Test
    public void testMain_FileNotExists_ShouldExit1() {
        SecurityManager originalSM = System.getSecurityManager();
        System.setSecurityManager(new SecurityManager() {
            @Override
            public void checkPermission(Permission perm) {}

            @Override
            public void checkExit(int status) {
                throw new SecurityException("exit:" + status);
            }
        });

        try {
            JKS2RangerUtil.main(new String[] {"nonexistent.jceks", "jceks", "testpass", "testkeypass"});
            //
        } catch (SecurityException ex) {
            assertTrue(ex.getMessage().contains("exit:1"));
        } finally {
            System.setSecurityManager(originalSM);
        }
    }

    @Test
    public void testDoImportKeysFromJKS_elseBranchcc() throws Exception {
        JKS2RangerUtil util = new JKS2RangerUtil();

        // Dummy config with no KeySecure or Azure enabled
        Configuration conf = mock(Configuration.class);
        lenient().when(conf.get("ranger.plugin.kms.encryption.key.password")).thenReturn("testpass");
        lenient().when(conf.get(anyString())).thenReturn(null); // ensures keysecure/azure conditions fail

        // Create a dummy keystore file
        File dummyJceks = File.createTempFile("dummy", ".jceks");
        dummyJceks.deleteOnExit();

        Method method = JKS2RangerUtil.class.getDeclaredMethod("doImportKeysFromJKS", String.class, String.class);
        method.setAccessible(true);

        try {
            method.invoke(util, dummyJceks.getAbsolutePath(), "jceks");
            //
        } catch (InvocationTargetException e) {
            Throwable cause = e.getCause();
            assertNotNull(cause);
        }
    }
}
