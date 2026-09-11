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

package org.apache.ranger.plugin.policyengine;

import org.apache.hadoop.conf.Configuration;
import org.apache.ranger.authorization.hadoop.config.RangerPluginConfig;
import org.apache.ranger.plugin.authn.DefaultJwtProvider;
import org.junit.jupiter.api.Test;

import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class TestRangerPluginContextJwtProvider {
    @Test
    public void usesDefaultProviderWhenNotConfigured() {
        RangerPluginConfig  config = newConfig();
        RangerPluginContext ctx    = new RangerPluginContext(config);

        assertEquals(DefaultJwtProvider.class, ctx.getTokenSupplier().getClass());
    }

    @Test
    public void usesDefaultProviderWhenExplicitlyConfigured() {
        RangerPluginConfig config = newConfig();

        config.set(jwtProviderProperty(config), DefaultJwtProvider.class.getName());

        RangerPluginContext ctx = new RangerPluginContext(config);

        assertEquals(DefaultJwtProvider.class, ctx.getTokenSupplier().getClass());
    }

    @Test
    public void usesCustomProviderWithNoArgConstructor() {
        RangerPluginConfig config = newConfig();

        config.set(jwtProviderProperty(config), NoArgJwtProvider.class.getName());

        RangerPluginContext ctx = new RangerPluginContext(config);

        assertEquals(NoArgJwtProvider.class, ctx.getTokenSupplier().getClass());
        assertEquals("no-arg-token", ctx.getTokenSupplier().get());
    }

    @Test
    public void usesCustomProviderWithConfigConstructor() {
        RangerPluginConfig config = newConfig();

        config.set(jwtProviderProperty(config), ConfigJwtProvider.class.getName());

        RangerPluginContext ctx = new RangerPluginContext(config);

        assertEquals(ConfigJwtProvider.class, ctx.getTokenSupplier().getClass());
        assertEquals("config-token", ctx.getTokenSupplier().get());
    }

    @Test
    public void throwsForClassThatIsNotSupplier() {
        RangerPluginConfig config = newConfig();

        config.set(jwtProviderProperty(config), NotASupplier.class.getName());

        assertThrows(IllegalArgumentException.class, () -> new RangerPluginContext(config));
    }

    @Test
    public void throwsForUnknownClass() {
        RangerPluginConfig config = newConfig();

        config.set(jwtProviderProperty(config), "org.apache.ranger.NoSuchJwtProvider");

        assertThrows(IllegalArgumentException.class, () -> new RangerPluginContext(config));
    }

    private RangerPluginConfig newConfig() {
        return new RangerPluginConfig("hive", "test-service", "test-app", "cl1", "on-perm", new RangerPolicyEngineOptions());
    }

    private String jwtProviderProperty(RangerPluginConfig config) {
        return config.getPropertyPrefix() + ".policy.rest.client" + DefaultJwtProvider.JWT_PROVIDER;
    }

    public static class NoArgJwtProvider implements Supplier<String> {
        @Override
        public String get() {
            return "no-arg-token";
        }
    }

    public static class ConfigJwtProvider implements Supplier<String> {
        public ConfigJwtProvider(Configuration config) {
            // custom providers may accept the Ranger Configuration
        }

        @Override
        public String get() {
            return "config-token";
        }
    }

    public static class NotASupplier {
        public String get() {
            return "not-a-supplier";
        }
    }
}
