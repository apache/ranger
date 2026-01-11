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

package org.apache.ranger.authorization.kms.authorizer;

import org.apache.ranger.plugin.audit.RangerDefaultAuditHandler;
import org.apache.ranger.plugin.service.RangerBasePlugin;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

class RangerKMSPluginTest {
    private RangerKMSPlugin plugin;

    @BeforeEach
    void setUp() {
        plugin = new RangerKMSPlugin();
    }

    @Test
    void testConstructor_SetsServiceTypeAndAppId() {
        assertEquals("kms", plugin.getServiceType());
        assertEquals("kms", plugin.getAppId());
    }

    @Test
    void testInitSets_ResultProcessor() {
        RangerKMSPlugin spyPlugin = spy(new RangerKMSPlugin());
        doNothing().when((RangerBasePlugin) spyPlugin).setResultProcessor(any());

        spyPlugin.init();

        verify((RangerBasePlugin) spyPlugin).setResultProcessor(any(RangerDefaultAuditHandler.class));
    }
}
