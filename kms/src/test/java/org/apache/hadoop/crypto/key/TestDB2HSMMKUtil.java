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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(MockitoExtension.class)
@TestMethodOrder(MethodOrderer.MethodName.class)
@Disabled
class TestDB2HSMMKUtil {
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
    void testMain_WithLessThan2Args_ShouldExit1() {
        String[]             args = {"onlyOneArg"};
        ExitTrappedException ex   = assertThrows(ExitTrappedException.class, () -> DB2HSMMKUtil.main(args));
        assertEquals(1, ex.status);
        assertTrue(errContent.toString().contains("Invalid number of parameters"));
    }

    @Test
    void testMain_WithEmptyHSMType_ShouldExit1() {
        String[]             args = {"", "partition1"};
        ExitTrappedException ex   = assertThrows(ExitTrappedException.class, () -> DB2HSMMKUtil.main(args));
        assertEquals(1, ex.status);
        assertTrue(errContent.toString().contains("HSM Type does not exists"));
    }

    @Test
    void testMain_WithEmptyPartition_ShouldExit1() {
        String[]             args = {"HSMType", ""};
        ExitTrappedException ex   = assertThrows(ExitTrappedException.class, () -> DB2HSMMKUtil.main(args));
        assertEquals(1, ex.status);
        assertTrue(errContent.toString().contains("Partition name does not exists"));
    }

    @Test
    void testShowUsage() {
        DB2HSMMKUtil.showUsage();
        String errOutput = errContent.toString();
        assertTrue(errOutput.contains("USAGE:"));
        assertTrue(errOutput.contains("HSMType"));
        assertTrue(errOutput.contains("partitionName"));
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
