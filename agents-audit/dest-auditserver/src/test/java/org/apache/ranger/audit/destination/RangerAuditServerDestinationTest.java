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

import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class RangerAuditServerDestinationTest {
    @Test
    public void resolvePluginHeaderAuthPrefixUsesExplicitAuditProperty() {
        Properties props = new Properties();
        props.setProperty("xasecure.audit.destination.auditserver.authn.header.config.prefix", "ranger.ozone");

        assertEquals("ranger.ozone",
                RangerAuditServerDestination.resolvePluginHeaderAuthPrefix(props, "xasecure.audit.destination.auditserver"));
    }

    @Test
    public void resolvePluginHeaderAuthPrefixScansPluginSiteProperties() {
        Properties props = new Properties();
        props.setProperty("ranger.hive.authn.header.enabled", "true");

        assertEquals("ranger.hive",
                RangerAuditServerDestination.resolvePluginHeaderAuthPrefix(props, "xasecure.audit.destination.auditserver"));
    }

    @Test
    public void resolvePluginHeaderAuthPrefixNullWhenNotEnabled() {
        Properties props = new Properties();
        props.setProperty("ranger.hive.authn.header.enabled", "false");

        assertNull(RangerAuditServerDestination.resolvePluginHeaderAuthPrefix(props, "xasecure.audit.destination.auditserver"));
    }
}
