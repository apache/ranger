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

import org.apache.hadoop.crypto.key.MigrateDBMKeyToGCP;
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
import java.security.Permission;

import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(MockitoExtension.class)
@TestMethodOrder(MethodOrderer.MethodName.class)
@Disabled
public class TestMigrateDBMKeyToGCP {
    @Test
    public void testShowUsage() throws InvocationTargetException, IllegalAccessException, NoSuchMethodException {
        MigrateDBMKeyToGCP migrateDBMKeyToGCP = new MigrateDBMKeyToGCP();

        Method method = MigrateDBMKeyToGCP.class.getDeclaredMethod("showUsage");
        method.setAccessible(true);
        method.invoke(migrateDBMKeyToGCP);
    }

    @Test
    public void testMain_NoArgs_ShouldPrintUsageAndExit() {
        SecurityManager originalSM  = System.getSecurityManager();
        PrintStream     originalErr = System.err;

        ByteArrayOutputStream errContent = new ByteArrayOutputStream();
        System.setErr(new PrintStream(errContent));

        System.setSecurityManager(new SecurityManager() {
            @Override
            public void checkPermission(Permission perm) {}

            @Override
            public void checkExit(int status) {
                throw new SecurityException("Intercepted System.exit(" + status + ")");
            }
        });

        try {
            MigrateDBMKeyToGCP.main(new String[0]);
            //
        } catch (Exception ex) {
            assertTrue(ex.getMessage().contains("Intercepted System.exit(1)"));
        } finally {
            System.setSecurityManager(originalSM);
            System.setErr(originalErr);
        }

        String output = errContent.toString();
        assertTrue(output.contains("Invalid number of parameters found."));
        assertTrue(output.contains("USAGE: java " + MigrateDBMKeyToGCP.class.getName()));
    }

    @Test
    public void testMain() throws Exception {
        SecurityManager originalSM  = System.getSecurityManager();
        PrintStream     originalErr = System.err;

        ByteArrayOutputStream errContent = new ByteArrayOutputStream();
        System.setErr(new PrintStream(errContent));

        System.setSecurityManager(new SecurityManager() {
            @Override
            public void checkPermission(Permission perm) {}

            @Override
            public void checkExit(int status) {
                throw new SecurityException("Intercepted System.exit(" + status + ")");
            }
        });

        String[] args = {
                "gcpMasterKeyName",
                "gcpProjectId",
                "gcpKeyRingId",
                "gcpKeyRingLocationId",
                "pathOfJsonCredFile"
        };
        try {
            MigrateDBMKeyToGCP.main(args);
            //
        } catch (Exception ex) {
        } finally {
            System.setSecurityManager(originalSM);
            System.setErr(originalErr);
        }
    }
}
