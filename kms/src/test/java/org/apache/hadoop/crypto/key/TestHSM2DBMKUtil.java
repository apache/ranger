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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.lang.reflect.Method;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(MockitoExtension.class)
@TestMethodOrder(MethodOrderer.MethodName.class)
@Disabled
public class TestHSM2DBMKUtil {
    private final PrintStream     originalOut             = System.out;
    private final PrintStream     originalErr             = System.err;
    private final SecurityManager originalSecurityManager = System.getSecurityManager();

    private ByteArrayOutputStream outContent;
    private ByteArrayOutputStream errContent;

    @BeforeEach
    public void setUp() {
        outContent = new ByteArrayOutputStream();
        errContent = new ByteArrayOutputStream();
        System.setOut(new PrintStream(outContent));
        System.setErr(new PrintStream(errContent));
        System.setSecurityManager(new NoExitSecurityManager());
    }

    @AfterEach
    public void tearDown() {
        System.setOut(originalOut);
        System.setErr(originalErr);
        System.setSecurityManager(originalSecurityManager);
    }

    @Test
    public void testMain_WithLessThan2Args_ShouldExit1() {
        String[]             args = {"onlyOneArg"};
        ExitTrappedException ex   = assertThrows(ExitTrappedException.class, () -> HSM2DBMKUtil.main(args));
        assertEquals(1, ex.status);
        assertTrue(errContent.toString().contains("Invalid number of parameters"));
    }

    @Test
    public void testMain_WithEmptyHSMType_ShouldExit1() {
        String[]             args = {"", "partition1"};
        ExitTrappedException ex   = assertThrows(ExitTrappedException.class, () -> HSM2DBMKUtil.main(args));
        assertEquals(1, ex.status);
        assertTrue(errContent.toString().contains("HSM Type does not exists"));
    }

    @Test
    public void testMain_WithEmptyPartition_ShouldExit1() {
        String[]             args = {"HSMType", ""};
        ExitTrappedException ex   = assertThrows(ExitTrappedException.class, () -> HSM2DBMKUtil.main(args));
        assertEquals(1, ex.status);
        assertTrue(errContent.toString().contains("Partition name does not exists"));
    }

    @Test
    public void testShowUsage() {
        HSM2DBMKUtil ranger2DBMKUtil = new HSM2DBMKUtil();
        ranger2DBMKUtil.getClass();
        HSM2DBMKUtil.showUsage();
    }

    @Test
    public void testDoExportMKToDB() throws NoSuchMethodException {
        HSM2DBMKUtil ranger2DBMKUtil = new HSM2DBMKUtil();
        String[]     args            = {"-jks", "test.jks", "-out", "output.txt"};

        Method method = HSM2DBMKUtil.class.getDeclaredMethod(
                "doImportMKFromHSM", String.class, String.class);
        method.setAccessible(true);

        try {
            method.invoke(ranger2DBMKUtil, args[1], args[3]);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    static class ExitTrappedException extends SecurityException {
        final int status;

        ExitTrappedException(int status) {
            super("System.exit(" + status + ") called");
            this.status = status;
        }
    }

    private static class NoExitSecurityManager extends SecurityManager {
        @Override
        public void checkPermission(java.security.Permission perm) {
            // Allow all
        }

        @Override
        public void checkExit(int status) {
            throw new ExitTrappedException(status);
        }
    }
}
