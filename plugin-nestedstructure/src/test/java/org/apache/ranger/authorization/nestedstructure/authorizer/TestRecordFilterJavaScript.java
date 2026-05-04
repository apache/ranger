/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.ranger.authorization.nestedstructure.authorizer;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

import javax.script.ScriptEngineManager;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestRecordFilterJavaScript {
    @Test
    public void testArbitraryCommand() {
        assertThrows(MaskingException.class, () ->
                RecordFilterJavaScript.filterRow("user", "this.engine.factory.scriptEngine.eval('java.lang.Runtime.getRuntime().exec(\"/Applications/Spotify.app/Contents/MacOS/Spotify\")')", TestJsonManipulator.bigTester));
    }

    /**
     * Required security contract: by default, record-filter scripts must not be able to use Java/host APIs to create
     * or modify local files. Do not add a parallel test that asserts a file is created when host access is opt-in; that
     * normalizes filesystem side effects in CI. The opt-in flag exists for exceptional deployments only.
     */
    @Test
    public void testJavaInteropCannotWriteFileWhenHostAccessDisabledByDefault() throws Exception {
        Assumptions.assumeTrue(
                new ScriptEngineManager().getEngineByName("graal.js") != null,
                "GraalJS not on this classpath: skip (Nashorn allows Java by default, so the secure-default contract is not asserted here).");

        Files.deleteIfExists(Paths.get("omg.txt"));
        System.clearProperty(RecordFilterJavaScript.RANGER_RECORDFILTER_JS_ALLOW_HOST_ACCESS);

        assertThrows(MaskingException.class, () -> RecordFilterJavaScript.filterRow("user", "bufferedWriter = new java.io.BufferedWriter(new java.io.FileWriter('omg.txt'));\n" +
                "            bufferedWriter.write(\"Writing line one to file\"); bufferedWriter.close;", TestJsonManipulator.bigTester));
        assertFalse(Files.exists(Paths.get("omg.txt")));
    }

    @AfterEach
    public void cleanupTest() throws IOException {
        System.clearProperty(RecordFilterJavaScript.RANGER_RECORDFILTER_JS_ALLOW_HOST_ACCESS);
        Files.deleteIfExists(Paths.get("omg.txt"));
    }

    @Test
    public void testBasicFilters() {
        assertTrue(RecordFilterJavaScript.filterRow("user", "jsonAttr.partner.equals('dance')", TestJsonManipulator.testString1));
        assertTrue(RecordFilterJavaScript.filterRow("user", "jsonAttr.address.zipCode.equals('19019')", TestJsonManipulator.testString1));
        assertTrue(RecordFilterJavaScript.filterRow("user", "jsonAttr.aMap.mapNumber > 5", TestJsonManipulator.bigTester));

        assertFalse(RecordFilterJavaScript.filterRow("user", "jsonAttr.partner.equals('cox')", TestJsonManipulator.testString1));
    }
}
