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

package org.apache.ranger.audit.dispatcher;

import org.apache.ranger.audit.model.AuthzAuditEvent;
import org.junit.jupiter.api.Test;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

public class TestAuditEventOpenSearchDocMapper {
    @Test
    void toDoc_mapsAllFields() {
        Date eventTime = new Date(1700000000000L);

        AuthzAuditEvent event = new AuthzAuditEvent(
                1, "dev_hdfs", "testuser", eventTime, "read",
                "/tmp/test", "path", "read", (short) 1, "hdfs-agent",
                42L, "allowed by policy", "ranger-acl", "sess-123",
                "hive", "192.168.1.1", "/tmp/test", "cl1", "zone1", 5L);
        event.setEventId("evt-001");
        event.setSeqNum(10L);
        event.setEventCount(1L);
        event.setEventDurationMS(50L);
        event.setAgentHostname("host1.example.com");

        Set<String> tags = new HashSet<>();
        tags.add("PII");
        event.setTags(tags);

        Map<String, Object> doc = AuditEventOpenSearchDocMapper.toDoc(event);

        assertNotNull(doc);
        assertEquals("evt-001", doc.get("id"));
        assertEquals("read", doc.get("access"));
        assertEquals("ranger-acl", doc.get("enforcer"));
        assertEquals("hdfs-agent", doc.get("agent"));
        assertEquals("dev_hdfs", doc.get("repo"));
        assertEquals("sess-123", doc.get("sess"));
        assertEquals("testuser", doc.get("reqUser"));
        assertEquals("/tmp/test", doc.get("reqData"));
        assertEquals("/tmp/test", doc.get("resource"));
        assertEquals("192.168.1.1", doc.get("cliIP"));
        assertEquals("hive", doc.get("cliType"));
        assertEquals((short) 1, doc.get("result"));
        assertEquals(42L, doc.get("policy"));
        assertEquals(1, doc.get("repoType"));
        assertEquals("path", doc.get("resType"));
        assertEquals("allowed by policy", doc.get("reason"));
        assertEquals("read", doc.get("action"));
        assertEquals(10L, doc.get("seq_num"));
        assertEquals(1L, doc.get("event_count"));
        assertEquals(50L, doc.get("event_dur_ms"));
        assertEquals("cl1", doc.get("cluster"));
        assertEquals("zone1", doc.get("zoneName"));
        assertEquals("host1.example.com", doc.get("agentHost"));
        assertEquals(5L, doc.get("policyVersion"));

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
        sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
        assertEquals(sdf.format(eventTime), doc.get("evtTime"));
    }

    @Test
    void toDoc_handlesNullEventTime() {
        AuthzAuditEvent event = new AuthzAuditEvent();
        event.setEventId("evt-002");
        event.setEventTime(null);

        Map<String, Object> doc = AuditEventOpenSearchDocMapper.toDoc(event);

        assertNotNull(doc);
        assertEquals("evt-002", doc.get("id"));
        assertNull(doc.get("evtTime"));
    }

    @Test
    void toDoc_handlesNullFields() {
        AuthzAuditEvent event = new AuthzAuditEvent();

        Map<String, Object> doc = AuditEventOpenSearchDocMapper.toDoc(event);

        assertNotNull(doc);
        assertNull(doc.get("id"));
        assertNull(doc.get("access"));
        assertNull(doc.get("enforcer"));
        assertNull(doc.get("agent"));
        assertNull(doc.get("repo"));
        assertNull(doc.get("reqUser"));
        assertNull(doc.get("resource"));
        assertNull(doc.get("cliIP"));
        assertEquals(0, doc.get("repoType"));
        assertEquals(0L, doc.get("policy"));
    }

    @Test
    void toDoc_mapsDatasets() {
        AuthzAuditEvent event = new AuthzAuditEvent();
        event.setEventId("evt-003");

        Set<String> datasets = new HashSet<>();
        datasets.add("dataset1");
        datasets.add("dataset2");
        event.setDatasets(datasets);

        Set<String> projects = new HashSet<>();
        projects.add("project1");
        event.setProjects(projects);

        Set<Long> datasetIds = new HashSet<>();
        datasetIds.add(100L);
        datasetIds.add(200L);
        event.setDatasetIds(datasetIds);

        Map<String, Object> doc = AuditEventOpenSearchDocMapper.toDoc(event);

        assertEquals(datasets, doc.get("datasets"));
        assertEquals(projects, doc.get("projects"));
        assertEquals(datasetIds, doc.get("datasetIds"));
    }
}
