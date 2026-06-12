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

package org.apache.ranger.audit.dispatcher.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.HttpEntity;
import org.apache.http.StatusLine;
import org.apache.ranger.audit.model.AuthzAuditEvent;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class TestAuditOpenSearchDispatcher {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Mock
    private RestClient mockRestClient;

    @Mock
    private Response mockResponse;

    @Mock
    private StatusLine mockStatusLine;

    private AuditOpenSearchDispatcher dispatcher;

    @BeforeEach
    void setUp() throws Exception {
        Field unsafeField = sun.misc.Unsafe.class.getDeclaredField("theUnsafe");
        unsafeField.setAccessible(true);
        sun.misc.Unsafe unsafe = (sun.misc.Unsafe) unsafeField.get(null);
        dispatcher = (AuditOpenSearchDispatcher) unsafe.allocateInstance(AuditOpenSearchDispatcher.class);

        setField(dispatcher, "openSearchClient", mockRestClient);
        setField(dispatcher, "openSearchIndex", "ranger_audits");
    }

    @Test
    void processMessageBatch_sendsBulkRequestWithDocumentId() throws Exception {
        AuthzAuditEvent event = new AuthzAuditEvent();
        event.setEventId("test-id-001");
        event.setAccessType("read");
        event.setUser("testuser");
        event.setRepositoryName("dev_hdfs");
        event.setEventTime(new Date(1700000000000L));

        String auditJson = MAPPER.writeValueAsString(event);
        setupSuccessResponse();

        invokeProcessMessageBatch(Collections.singletonList(auditJson));

        ArgumentCaptor<Request> requestCaptor = ArgumentCaptor.forClass(Request.class);
        verify(mockRestClient).performRequest(requestCaptor.capture());

        Request request = requestCaptor.getValue();
        assertEquals("POST", request.getMethod());
        assertEquals("/_bulk", request.getEndpoint());

        String[] lines = requestBody(request).split("\n");
        Map<String, Object> indexMeta = indexMetadata(lines[0]);
        Map<String, Object> doc = MAPPER.readValue(lines[1], Map.class);

        assertEquals("ranger_audits", indexMeta.get("_index"));
        assertEquals("test-id-001", indexMeta.get("_id"));
        assertEquals("test-id-001", doc.get("id"));
        assertEquals("read", doc.get("access"));
        assertEquals("testuser", doc.get("reqUser"));
        assertEquals("dev_hdfs", doc.get("repo"));
    }

    @Test
    void processMessageBatch_generatesDocumentIdWhenAuditIdMissing() throws Exception {
        AuthzAuditEvent event = new AuthzAuditEvent();
        event.setAccessType("read");
        event.setUser("testuser");

        String auditJson = MAPPER.writeValueAsString(event);
        setupSuccessResponse();

        invokeProcessMessageBatch(Collections.singletonList(auditJson));

        ArgumentCaptor<Request> requestCaptor = ArgumentCaptor.forClass(Request.class);
        verify(mockRestClient).performRequest(requestCaptor.capture());

        String[] lines = requestBody(requestCaptor.getValue()).split("\n");
        Map<String, Object> indexMeta = indexMetadata(lines[0]);
        Map<String, Object> doc = MAPPER.readValue(lines[1], Map.class);
        Object generatedId = indexMeta.get("_id");

        assertNotNull(generatedId);
        assertFalse(generatedId.toString().trim().isEmpty());
        assertEquals(generatedId, doc.get("id"));
    }

    @Test
    void processMessageBatch_throwsOnNullInput() {
        assertThrows(Exception.class, () -> invokeProcessMessageBatch(null));
    }

    @Test
    void processMessageBatch_throwsOnEmptyInput() {
        assertThrows(Exception.class, () -> invokeProcessMessageBatch(Collections.emptyList()));
    }

    @Test
    void processMessageBatch_throwsOnHttpError() throws Exception {
        AuthzAuditEvent event = new AuthzAuditEvent();
        event.setEventId("test-id-002");
        event.setUser("testuser");

        String auditJson = MAPPER.writeValueAsString(event);

        when(mockRestClient.performRequest(any(Request.class))).thenReturn(mockResponse);
        when(mockResponse.getStatusLine()).thenReturn(mockStatusLine);
        when(mockStatusLine.getStatusCode()).thenReturn(500);

        assertThrows(Exception.class, () -> invokeProcessMessageBatch(Collections.singletonList(auditJson)));
    }

    @Test
    void processMessageBatch_throwsOnBulkItemErrors() throws Exception {
        AuthzAuditEvent event = new AuthzAuditEvent();
        event.setEventId("test-id-003");
        event.setUser("testuser");

        String auditJson = MAPPER.writeValueAsString(event);
        String errorResponse = "{\"errors\":true,\"items\":[{\"index\":{\"status\":400,\"error\":\"mapping error\"}}]}";
        HttpEntity entity = mock(HttpEntity.class);
        when(entity.getContent()).thenReturn(new ByteArrayInputStream(errorResponse.getBytes(StandardCharsets.UTF_8)));
        when(entity.getContentLength()).thenReturn((long) errorResponse.length());

        when(mockRestClient.performRequest(any(Request.class))).thenReturn(mockResponse);
        when(mockResponse.getStatusLine()).thenReturn(mockStatusLine);
        when(mockStatusLine.getStatusCode()).thenReturn(200);
        when(mockResponse.getEntity()).thenReturn(entity);

        assertThrows(Exception.class, () -> invokeProcessMessageBatch(Collections.singletonList(auditJson)));
    }

    private void setupSuccessResponse() throws Exception {
        String successBody = "{\"errors\":false,\"items\":[]}";
        HttpEntity entity = mock(HttpEntity.class);
        when(entity.getContent()).thenReturn(new ByteArrayInputStream(successBody.getBytes(StandardCharsets.UTF_8)));
        when(entity.getContentLength()).thenReturn((long) successBody.length());

        when(mockRestClient.performRequest(any(Request.class))).thenReturn(mockResponse);
        when(mockResponse.getStatusLine()).thenReturn(mockStatusLine);
        when(mockStatusLine.getStatusCode()).thenReturn(200);
        when(mockResponse.getEntity()).thenReturn(entity);
    }

    private String requestBody(Request request) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        request.getEntity().writeTo(out);

        return out.toString(StandardCharsets.UTF_8.name());
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> indexMetadata(String line) throws Exception {
        Map<String, Object> meta = MAPPER.readValue(line, Map.class);

        return (Map<String, Object>) meta.get("index");
    }

    private void invokeProcessMessageBatch(Collection<String> audits) throws Exception {
        Method method = AuditOpenSearchDispatcher.class.getDeclaredMethod("processMessageBatch", Collection.class);
        method.setAccessible(true);
        try {
            method.invoke(dispatcher, audits);
        } catch (java.lang.reflect.InvocationTargetException e) {
            if (e.getCause() instanceof Exception) {
                throw (Exception) e.getCause();
            }
            throw e;
        }
    }

    private void setField(Object target, String fieldName, Object value) throws Exception {
        Class<?> clazz = target.getClass();
        while (clazz != null) {
            try {
                Field field = clazz.getDeclaredField(fieldName);
                field.setAccessible(true);
                field.set(target, value);
                return;
            } catch (NoSuchFieldException e) {
                clazz = clazz.getSuperclass();
            }
        }
        throw new NoSuchFieldException(fieldName + " not found in class hierarchy");
    }
}
