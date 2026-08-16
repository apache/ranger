/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ranger.audit.destination;

import org.apache.ranger.plugin.util.PluginHeaderAuthConfig;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class RangerAuditServerDestinationTest {
    private static final String AUDIT_DEST_PREFIX = "xasecure.audit.destination.auditserver";

    @Test
    public void buildTrustedAuthHeadersUsesAuditDestinationPrefix() {
        Properties props = new Properties();
        props.setProperty(AUDIT_DEST_PREFIX + ".authn.header.enabled", "true");
        props.setProperty(AUDIT_DEST_PREFIX + ".authn.header.X-Spiffe-Id",
                "spiffe://prod-cluster.k8s.example.com/ns/ranger/sa/hive");

        Map<String, String> headers = PluginHeaderAuthConfig.buildTrustedAuthHeaders(props, AUDIT_DEST_PREFIX);

        assertEquals(1, headers.size());
        assertEquals("spiffe://prod-cluster.k8s.example.com/ns/ranger/sa/hive", headers.get("X-Spiffe-Id"));
    }

    @Test
    public void buildTrustedAuthHeadersEmptyWhenAuditDestinationDisabled() {
        Properties props = new Properties();
        props.setProperty(AUDIT_DEST_PREFIX + ".authn.header.enabled", "false");
        props.setProperty(AUDIT_DEST_PREFIX + ".authn.header.X-Spiffe-Id",
                "spiffe://prod-cluster.k8s.example.com/ns/ranger/sa/hive");

        Map<String, String> headers = PluginHeaderAuthConfig.buildTrustedAuthHeaders(props, AUDIT_DEST_PREFIX);

        assertTrue(headers.isEmpty());
    }
}
