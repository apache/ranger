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

package org.apache.ranger.audit.destination;

import org.apache.ranger.audit.model.AuthzAuditEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Date;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestOpenSearchAuditDestination {
    private OpenSearchAuditDestination destination;

    @BeforeEach
    void setUp() {
        destination = new OpenSearchAuditDestination();
    }

    @Test
    void toDoc_mapsAllFieldsCorrectly() {
        AuthzAuditEvent event = new AuthzAuditEvent();
        event.setEventId("test-event-001");
        event.setAccessType("read");
        event.setAclEnforcer("ranger-acl");
        event.setAgentId("hdfs-agent");
        event.setRepositoryName("dev_hdfs");
        event.setSessionId("sess-123");
        event.setUser("testuser");
        event.setRequestData("/tmp/testfile");
        event.setResourcePath("/data/warehouse");
        event.setClientIP("192.168.1.10");
        event.setLogType("RangerAudit");
        event.setAccessResult((short) 1);
        event.setPolicyId(42L);
        event.setRepositoryType(1);
        event.setResourceType("path");
        event.setResultReason("Allowed by policy");
        event.setAction("read");
        event.setEventTime(new Date(1700000000000L));
        event.setSeqNum(10L);
        event.setEventCount(1L);
        event.setEventDurationMS(50L);
        event.setClusterName("test-cluster");
        event.setZoneName("zone1");
        event.setAgentHostname("host1.example.com");
        event.setPolicyVersion(5L);

        Map<String, Object> doc = destination.toDoc(event);

        assertEquals("test-event-001", doc.get("id"));
        assertEquals("read", doc.get("access"));
        assertEquals("ranger-acl", doc.get("enforcer"));
        assertEquals("hdfs-agent", doc.get("agent"));
        assertEquals("dev_hdfs", doc.get("repo"));
        assertEquals("sess-123", doc.get("sess"));
        assertEquals("testuser", doc.get("reqUser"));
        assertEquals("/tmp/testfile", doc.get("reqData"));
        assertEquals("/data/warehouse", doc.get("resource"));
        assertEquals("192.168.1.10", doc.get("cliIP"));
        assertEquals("RangerAudit", doc.get("logType"));
        assertEquals((short) 1, doc.get("result"));
        assertEquals(42L, doc.get("policy"));
        assertEquals(1, doc.get("repoType"));
        assertEquals("path", doc.get("resType"));
        assertEquals("Allowed by policy", doc.get("reason"));
        assertEquals("read", doc.get("action"));
        assertNotNull(doc.get("evtTime"));
        assertTrue(doc.get("evtTime").toString().contains("2023-11-14"));
        assertEquals(10L, doc.get("seq_num"));
        assertEquals(1L, doc.get("event_count"));
        assertEquals(50L, doc.get("event_dur_ms"));
        assertEquals("test-cluster", doc.get("cluster"));
        assertEquals("zone1", doc.get("zoneName"));
        assertEquals("host1.example.com", doc.get("agentHost"));
        assertEquals(5L, doc.get("policyVersion"));
    }

    @Test
    void toDoc_nullEventId_remainsNull() {
        AuthzAuditEvent event = new AuthzAuditEvent();
        event.setUser("testuser");

        Map<String, Object> doc = destination.toDoc(event);

        assertNull(doc.get("id"));
    }

    @Test
    void toDoc_nullEventTime_formatsAsNull() {
        AuthzAuditEvent event = new AuthzAuditEvent();
        event.setEventId("evt-1");
        event.setEventTime(null);

        Map<String, Object> doc = destination.toDoc(event);

        assertNull(doc.get("evtTime"));
    }

    @Test
    void getClient_noneUrls_returnsNull() {
        java.util.Properties props = new java.util.Properties();
        props.setProperty(OpenSearchAuditDestination.CONFIG_PREFIX + ".urls", "NONE");

        destination.init(props, OpenSearchAuditDestination.CONFIG_PREFIX);

        assertNull(destination.getClient());
    }

    @Test
    void log_nullClient_returnsFalse() {
        java.util.Properties props = new java.util.Properties();
        props.setProperty(OpenSearchAuditDestination.CONFIG_PREFIX + ".urls", "NONE");
        destination.init(props, OpenSearchAuditDestination.CONFIG_PREFIX);

        AuthzAuditEvent event = new AuthzAuditEvent();
        event.setEventId("test-1");
        event.setUser("user1");

        java.util.Collection<org.apache.ranger.audit.model.AuditEventBase> events = java.util.List.of(event);
        boolean result = destination.log(events);

        assertFalse(result);
    }

    @Test
    void log_emptyEvents_returnsTrue() {
        java.util.Collection<org.apache.ranger.audit.model.AuditEventBase> events = java.util.Collections.emptyList();
        boolean result = destination.log(events);

        assertTrue(result);
    }

    @Test
    void log_nullEvents_returnsTrue() {
        boolean result = destination.log((java.util.Collection<org.apache.ranger.audit.model.AuditEventBase>) null);

        assertTrue(result);
    }

    @Test
    void configConstants_matchExpectedValues() {
        assertEquals("ranger.audit.opensearch", OpenSearchAuditDestination.CONFIG_PREFIX);
        assertEquals("urls", OpenSearchAuditDestination.CONFIG_URLS);
        assertEquals("port", OpenSearchAuditDestination.CONFIG_PORT);
        assertEquals("user", OpenSearchAuditDestination.CONFIG_USER);
        assertEquals("password", OpenSearchAuditDestination.CONFIG_PASSWORD);
        assertEquals("protocol", OpenSearchAuditDestination.CONFIG_PROTOCOL);
        assertEquals("index", OpenSearchAuditDestination.CONFIG_INDEX);
        assertEquals("ranger_audits", OpenSearchAuditDestination.DEFAULT_INDEX);
    }
}
