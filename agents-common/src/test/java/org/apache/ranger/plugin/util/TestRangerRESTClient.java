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

package org.apache.ranger.plugin.util;

import org.apache.ranger.authorization.hadoop.config.RangerPluginConfig;
import org.apache.ranger.plugin.policyengine.RangerPolicyEngineOptions;
import org.apache.ranger.plugin.service.RangerBasePlugin;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestRangerRESTClient {
    private static final String SERVICE_TYPE = "hive";
    private static final String SERVICE_NAME = "test-service";
    private static final String APP_ID = "test-app";
    private static final String ERR_MESSAGE = "Ranger URL is null or empty.";

    @Test
    public void testPluginInit_WithNoUrl_ThrowsException() {
        RangerBasePlugin plugin = new RangerBasePlugin(SERVICE_TYPE, SERVICE_NAME, APP_ID);
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, plugin::init);
        assertTrue(exception.getMessage().contains(ERR_MESSAGE));
    }

    @Test
    public void testPluginInit_WithValidUrl_Succeeds() {
        RangerPolicyEngineOptions peOptions = new RangerPolicyEngineOptions();
        RangerPluginConfig pluginConfig = new RangerPluginConfig(SERVICE_TYPE, SERVICE_NAME, APP_ID, "cl1", "on-perm", peOptions);
        pluginConfig.set("ranger.plugin.hive.policy.rest.url", "http://dummy:1234");
        RangerBasePlugin plugin = new RangerBasePlugin(pluginConfig);
        plugin.init();
        assertNotNull(plugin, "RangerBasePlugin should be initialized successfully");
    }
}
