/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ranger.audit.utils;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;
import java.util.HashMap;
import java.util.Properties;
import java.util.Collections;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.when;

public class RangerJSONAuditWriterTest {

    public Properties props;
    public Map<String, String> auditConfigs;

    public void setup(){
        props = new Properties();
        props.setProperty("test.dir", "/tmp");
        auditConfigs = new HashMap<>();
        auditConfigs.put(FileSystem.FS_DEFAULT_NAME_KEY, FileSystem.DEFAULT_FS);
        auditConfigs.put("fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem");
    }

    @Test
    public void verifyAppendToFileWhenEnabledWithConfig() throws Exception {
        RangerJSONAuditWriter jsonAuditWriter = spy(new RangerJSONAuditWriter());

        setup();
        props.setProperty("test.file.append.enabled", "true");
        jsonAuditWriter.init(props, "test", "localfs", auditConfigs);

        assertTrue(jsonAuditWriter.logJSON(Collections.singleton("This event will be logged in write(create) mode!")));
        assertTrue(jsonAuditWriter.reUseLastLogFile);

        when(jsonAuditWriter.getLogFileStream()).thenThrow(new IOException("Unable to fetch log file stream!"));
        assertFalse(jsonAuditWriter.logJSON(Collections.singleton("This event will not be logged due to exception!")));

        assertNull(jsonAuditWriter.ostream);
        assertNull(jsonAuditWriter.logWriter);
        assertNotNull(jsonAuditWriter.auditPath);
        assertNotNull(jsonAuditWriter.fullPath);

        reset(jsonAuditWriter);
        assertTrue(jsonAuditWriter.logJSON(Collections.singleton("Last log file will be opened in append mode and this event will be written")));
        assertTrue(jsonAuditWriter.logJSON(Collections.singleton("This event will also be written in append mode")));

        jsonAuditWriter.fileSystem.deleteOnExit(jsonAuditWriter.auditPath);
    }

    @Test
    public void verifyFileRolloverWithAppend() throws Exception {
        RangerJSONAuditWriter jsonAuditWriter = spy(new RangerJSONAuditWriter());

        setup();
        props.setProperty("test.file.rollover.enable.periodic.rollover", "true");
        props.setProperty("test.file.rollover.periodic.rollover.check.sec", "2");
        props.setProperty("test.file.append.enabled", "true");
        // rollover log file after this interval
        jsonAuditWriter.fileRolloverSec = 5; // in seconds
        jsonAuditWriter.init(props, "test", "localfs", auditConfigs);

        assertTrue(jsonAuditWriter.reUseLastLogFile);
        assertTrue(jsonAuditWriter.logJSON(Collections.singleton("This event will be logged in write(create) mode!")));

        when(jsonAuditWriter.getLogFileStream()).thenThrow(new IOException("Unable to fetch log file stream!"));
        assertFalse(jsonAuditWriter.logJSON(Collections.singleton("This event will not be logged due to exception!")));

        assertNull(jsonAuditWriter.ostream);
        assertNull(jsonAuditWriter.logWriter);
        assertNotNull(jsonAuditWriter.auditPath);
        assertNotNull(jsonAuditWriter.fullPath);

        reset(jsonAuditWriter);

        assertTrue(jsonAuditWriter.logJSON(Collections.singleton("Last log file will be opened in append mode and this event will be written")));
        assertTrue(jsonAuditWriter.logJSON(Collections.singleton("This event will also be written in append mode")));
        Path auditPath1 = jsonAuditWriter.auditPath;

        Thread.sleep(6000);

        // rollover should have happened
        assertTrue(jsonAuditWriter.reUseLastLogFile);
        assertNull(jsonAuditWriter.ostream);
        assertNull(jsonAuditWriter.logWriter);
        assertNull(jsonAuditWriter.auditPath);
        assertNull(jsonAuditWriter.fullPath);

        assertTrue(jsonAuditWriter.logJSON(Collections.singleton("Second file created since rollover happened!")));

        // ensure the same rolled over file is not used for append
        assertNotEquals(auditPath1, jsonAuditWriter.auditPath);

        // cleanup
        jsonAuditWriter.fileSystem.deleteOnExit(auditPath1);
        jsonAuditWriter.fileSystem.deleteOnExit(jsonAuditWriter.auditPath);
        jsonAuditWriter.closeWriter();
    }

    @Test
    public void verifyNoAppendToFileWhenDisabledWithConfig() throws Exception {
        RangerJSONAuditWriter jsonAuditWriter = spy(new RangerJSONAuditWriter());

        setup();

        props.setProperty("test.file.append.enabled", "false");
        jsonAuditWriter.init(props, "test", "localfs", auditConfigs);
        jsonAuditWriter.createFileSystemFolders();
        // File creation should fail with an exception which will trigger append next time.

        when(jsonAuditWriter.fileSystem.create(jsonAuditWriter.auditPath)).thenThrow(new IOException("Creation not allowed at this time!"));
        assertFalse(jsonAuditWriter.logJSON(Collections.singleton("This event will not be logged!")));
        assertFalse(jsonAuditWriter.reUseLastLogFile);
        assertNull(jsonAuditWriter.ostream);
        assertNull(jsonAuditWriter.logWriter);
        assertNotNull(jsonAuditWriter.auditPath);
        assertNotNull(jsonAuditWriter.fullPath);

        Path auditPath1 = jsonAuditWriter.auditPath;

        reset(jsonAuditWriter);
        assertTrue(jsonAuditWriter.logJSON(Collections.singleton("This event should be written to a newly created file in write mode!")));
        assertTrue(jsonAuditWriter.logJSON(Collections.singleton("This event should also be written to the previous file")));
        assertFalse(jsonAuditWriter.reUseLastLogFile);

        // cleanup
        jsonAuditWriter.fileSystem.deleteOnExit(auditPath1);
        jsonAuditWriter.fileSystem.deleteOnExit(jsonAuditWriter.auditPath);
    }


    @Test
    public void verifyFileRolloverAfterThreshold() throws Exception {
        RangerJSONAuditWriter jsonAuditWriter = spy(new RangerJSONAuditWriter());

        setup();
        props.setProperty("test.file.rollover.enable.periodic.rollover", "true");
        props.setProperty("test.file.rollover.periodic.rollover.check.sec", "2");
        // rollover log file after this interval
        jsonAuditWriter.fileRolloverSec = 5; // in seconds
        jsonAuditWriter.init(props, "test", "localfs", auditConfigs);

        assertTrue(jsonAuditWriter.logJSON(Collections.singleton("First file created and added this line!")));
        Path auditPath1 = jsonAuditWriter.auditPath;

        Thread.sleep(6000);

        assertNull(jsonAuditWriter.ostream);
        assertNull(jsonAuditWriter.logWriter);
        assertTrue(jsonAuditWriter.logJSON(Collections.singleton("Second file created since rollover happened!")));

        // cleanup
        jsonAuditWriter.fileSystem.deleteOnExit(auditPath1);
        jsonAuditWriter.fileSystem.deleteOnExit(jsonAuditWriter.auditPath);
        jsonAuditWriter.closeWriter();
    }
}
