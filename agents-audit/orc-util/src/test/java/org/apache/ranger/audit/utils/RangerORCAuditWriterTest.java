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

package org.apache.ranger.audit.utils;

import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.ColumnStatistics;
import org.apache.orc.OrcProto;
import org.apache.orc.StripeInformation;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.apache.ranger.audit.model.AuthzAuditEvent;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

/** * Unit Test cases for RangerORCAuditWriter */
class RangerORCAuditWriterTest {
    static class TestWriter implements Writer {
        @Override
        public void addRowBatch(VectorizedRowBatch batch) throws IOException {}

        @Override
        public void addUserMetadata(String name, ByteBuffer value) {}

        @Override
        public long getRawDataSize() {
            return 0;
        }

        @Override
        public long getNumberOfRows() {
            return 0;
        }

        @Override
        public long writeIntermediateFooter() throws IOException {
            return 0;
        }

        @Override
        public void appendStripe(byte[] data, int offset, int length,
                                           StripeInformation stripeInfo,
                                           OrcProto.StripeStatistics stripeStatistics) throws IOException {}

        @Override
        public void appendUserMetadata(List<OrcProto.UserMetadataItem> metadata) {}

        @Override
        public ColumnStatistics[] getStatistics() throws IOException {
            return null;
        }

        @Override
        public TypeDescription getSchema() {
            return null;
        }

        @Override
        public void close() throws IOException {}
    }

    // -----------------------------------------------------------------------
    // 1. Constants & default field values
    // -----------------------------------------------------------------------
    @Test
    void testOrcFileExtensionConstant() {
        assertEquals(".orc", RangerORCAuditWriter.ORC_FILE_EXTENSION);
    }

    @Test
    void testFileTypeDefaultValue() {
        RangerORCAuditWriter writer = new RangerORCAuditWriter();
        assertEquals("orc", writer.fileType);
    }

    @Test
    void testDefaultBufferAndStripeSize() {
        RangerORCAuditWriter writer = new RangerORCAuditWriter();
        assertEquals(100000, writer.defaultbufferSize);
        assertEquals(100000L, writer.defaultStripeSize);
    }

    // -----------------------------------------------------------------------
    // 2. flush / start – no-ops
    // -----------------------------------------------------------------------
    @Test
    void testFlushAndStartAreNoOps() {
        RangerORCAuditWriter writer = new RangerORCAuditWriter();
        writer.flush();
        writer.start();
        // No exception means pass
    }

    // -----------------------------------------------------------------------
    // 3. logFile – always false
    // -----------------------------------------------------------------------
    @Test
    void testLogFileAlwaysReturnsFalse() throws Exception {
        RangerORCAuditWriter writer = new RangerORCAuditWriter();
        assertFalse(writer.logFile(new File("test")));
    }

    // -----------------------------------------------------------------------
    // 4. stop() – orcLogWriter is null → no interaction with orcFileUtil
    // -----------------------------------------------------------------------
    @Test
    void testStopDoesNothingWhenWriterIsNull() throws Exception {
        RangerORCAuditWriter writer = new RangerORCAuditWriter();
        ORCFileUtil mockOrcUtil = mock(ORCFileUtil.class);
        writer.orcFileUtil = mockOrcUtil;
        writer.orcLogWriter = null;
        writer.stop();
        verify(mockOrcUtil, never()).close(any());
    }

    // -----------------------------------------------------------------------
    // 5. stop() – closes writer and nullifies it
    // -----------------------------------------------------------------------
    @Test
    void testStopClosesOrcLogWriter() throws Exception {
        RangerORCAuditWriter writer = new RangerORCAuditWriter();
        Writer testWriter = new TestWriter();
        ORCFileUtil mockOrcUtil = mock(ORCFileUtil.class);
        writer.orcLogWriter = testWriter;
        writer.orcFileUtil = mockOrcUtil;
        writer.stop();
        verify(mockOrcUtil).close(testWriter);
        assertNull(writer.orcLogWriter);
    }

    // -----------------------------------------------------------------------
    // 6. stop() – exception during close is swallowed, writer still nullified
    // -----------------------------------------------------------------------
    @Test
    void testStopHandlesExceptionAndNullifiesWriter() throws Exception {
        RangerORCAuditWriter writer = new RangerORCAuditWriter();
        Writer testWriter = new TestWriter();
        ORCFileUtil mockOrcUtil = mock(ORCFileUtil.class);
        doThrow(new RuntimeException("close error")).when(mockOrcUtil).close(any(Writer.class));
        writer.orcLogWriter = testWriter;
        writer.orcFileUtil = mockOrcUtil;
        writer.stop();
        assertNull(writer.orcLogWriter);
    }

    // -----------------------------------------------------------------------
    // 7. logAsORC – delegates to logAuditAsORC
    // -----------------------------------------------------------------------
    @Test
    void testLogAsORCDelegatesToLogAuditAsORC() throws Exception {
        RangerORCAuditWriter writer = spy(new RangerORCAuditWriter());
        doReturn(Collections.singletonList(new AuthzAuditEvent())).when(writer).getAuthzAuditEvents(any());
        doReturn(true).when(writer).logAuditAsORC(any());
        assertTrue(writer.logAsORC(Collections.singletonList("event")));
        verify(writer).logAuditAsORC(any());
    }

    // -----------------------------------------------------------------------
    // 8. log(Collection<String>) – delegates to logAsORC
    // -----------------------------------------------------------------------
    @Test
    void testLogDelegatesToLogAsORC() throws Exception {
        RangerORCAuditWriter writer = spy(new RangerORCAuditWriter());
        List<String> events = Collections.singletonList("event");
        doReturn(true).when(writer).logAsORC(events);
        assertTrue(writer.log(events));
        verify(writer).logAsORC(events);
    }

    // -----------------------------------------------------------------------
    // 9. getAuthzAuditEvents – valid JSON
    // -----------------------------------------------------------------------
    @Test
    void testGetAuthzAuditEventsWithValidJson() {
        RangerORCAuditWriter writer = new RangerORCAuditWriter();
        String validJson = "{\"repoType\":1,\"repo\":\"hdfs\",\"reqUser\":\"test\"}";
        Collection<AuthzAuditEvent> result = writer.getAuthzAuditEvents(Collections.singletonList(validJson));
        assertNotNull(result);
        assertEquals(1, result.size());
    }

    // -----------------------------------------------------------------------
    // 10. getAuthzAuditEvents – empty collection
    // -----------------------------------------------------------------------
    @Test
    void testGetAuthzAuditEventsWithEmptyCollection() {
        RangerORCAuditWriter writer = new RangerORCAuditWriter();
        Collection<AuthzAuditEvent> result = writer.getAuthzAuditEvents(Collections.emptyList());
        assertNotNull(result);
        assertTrue(result.isEmpty());
    }

    // -----------------------------------------------------------------------
    // 11. getAuthzAuditEvents – invalid JSON throws exception
    // -----------------------------------------------------------------------
    @Test
    void testGetAuthzAuditEventsThrowsOnInvalidJson() {
        RangerORCAuditWriter writer = new RangerORCAuditWriter();
        Collection<AuthzAuditEvent> result = writer.getAuthzAuditEvents(
                Collections.singletonList("not-valid-json{{"));
        assertNotNull(result);
        assertEquals(1, result.size());
        assertNull(result.iterator().next());
    }

    // -----------------------------------------------------------------------
    // 12. getAuthzAuditEvents – multiple events
    // -----------------------------------------------------------------------
    @Test
    void testGetAuthzAuditEventsWithMultipleEvents() {
        RangerORCAuditWriter writer = new RangerORCAuditWriter();
        String json1 = "{\"repoType\":1,\"repo\":\"hdfs\"}";
        String json2 = "{\"repoType\":2,\"repo\":\"hive\"}";
        Collection<AuthzAuditEvent> result = writer.getAuthzAuditEvents(Arrays.asList(json1, json2));
        assertEquals(2, result.size());
    }

    // -----------------------------------------------------------------------
    // 13. getORCFileWrite – returns existing writer when not null
    // -----------------------------------------------------------------------
    @Test
    void testGetORCFileWriteReturnsExistingWriter() throws Exception {
        RangerORCAuditWriter writer = new RangerORCAuditWriter();
        Writer existingWriter = new TestWriter();
        writer.orcLogWriter = existingWriter;
        Writer result = writer.getORCFileWrite();
        assertEquals(existingWriter, result);
    }

    // -----------------------------------------------------------------------
    // 14. compression field default is null
    // -----------------------------------------------------------------------
    @Test
    void testCompressionDefaultIsNull() {
        RangerORCAuditWriter writer = new RangerORCAuditWriter();
        assertNull(writer.compression);
    }

    // -----------------------------------------------------------------------
    // 15. orcLogWriter default is null
    // -----------------------------------------------------------------------
    @Test
    void testOrcLogWriterDefaultIsNull() {
        RangerORCAuditWriter writer = new RangerORCAuditWriter();
        assertNull(writer.orcLogWriter);
    }

    // -----------------------------------------------------------------------
    // 16. logAuditAsORC – exception during execute nullifies orcLogWriter
    //     and re-throws
    // -----------------------------------------------------------------------
    @Test
    void testLogAuditAsORCExceptionNullifiesWriterAndRethrows() throws Exception {
        RangerORCAuditWriter writer = spy(new RangerORCAuditWriter());
        Writer existingWriter = new TestWriter();
        writer.orcLogWriter = existingWriter;
        // Force the privileged action to throw by making getORCFileWrite() throw
        doThrow(new RuntimeException("write error")).when(writer).getORCFileWrite();
        assertThrows(Exception.class, () ->
                writer.logAuditAsORC(Collections.singletonList(new AuthzAuditEvent())));
        assertNull(writer.orcLogWriter);
    }
}
