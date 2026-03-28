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

package org.apache.ranger.pdp.config;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class RangerPdpConfigTest {
    @AfterEach
    public void clearSystemOverrides() {
        System.clearProperty(RangerPdpConstants.PROP_AUTHN_HEADER_USERNAME);
        System.clearProperty(RangerPdpConstants.PROP_PORT);
    }

    @Test
    public void testHeaderUserNameCanBeOverriddenBySystemProperty() {
        System.setProperty(RangerPdpConstants.PROP_AUTHN_HEADER_USERNAME, "X-Test-User");

        RangerPdpConfig config = new RangerPdpConfig();

        assertEquals("X-Test-User", config.getHeaderAuthnUsername());
    }

    @Test
    public void testInvalidPortFallsBackToDefault() {
        System.setProperty(RangerPdpConstants.PROP_PORT, "not-a-number");

        RangerPdpConfig config = new RangerPdpConfig();

        assertEquals(6500, config.getPort());
    }
}
