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
import org.apache.ranger.plugin.authn.DefaultTokenSupplier;
import org.junit.jupiter.api.Test;

import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class TestRangerPluginContextTokenSupplier {
    @Test
    public void noTokenSupplierWhenNotConfigured() {
        RangerPluginConfig  config = newConfig();
        RangerPluginContext ctx    = new RangerPluginContext(config);

        assertNull(ctx.getTokenSupplier());
    }

    @Test
    public void usesDefaultProviderWhenJwtSourceConfigured() {
        RangerPluginConfig config = newConfig();

        config.set(jwtSourceProperty(config), "env");

        RangerPluginContext ctx = new RangerPluginContext(config);

        assertEquals(DefaultTokenSupplier.class, ctx.getTokenSupplier().getClass());
    }

    @Test
    public void usesDefaultProviderWhenExplicitlyConfigured() {
        RangerPluginConfig config = newConfig();

        config.set(tokenSupplierProperty(config), DefaultTokenSupplier.class.getName());

        RangerPluginContext ctx = new RangerPluginContext(config);

        assertEquals(DefaultTokenSupplier.class, ctx.getTokenSupplier().getClass());
    }

    @Test
    public void usesCustomProviderWithNoArgConstructor() {
        RangerPluginConfig config = newConfig();

        config.set(tokenSupplierProperty(config), NoArgTokenSupplier.class.getName());

        RangerPluginContext ctx = new RangerPluginContext(config);

        assertEquals(NoArgTokenSupplier.class, ctx.getTokenSupplier().getClass());
        assertEquals("no-arg-token", ctx.getTokenSupplier().get());
    }

    @Test
    public void usesCustomProviderWithConfigConstructor() {
        RangerPluginConfig config = newConfig();

        config.set(tokenSupplierProperty(config), ConfigTokenSupplier.class.getName());

        RangerPluginContext ctx = new RangerPluginContext(config);

        assertEquals(ConfigTokenSupplier.class, ctx.getTokenSupplier().getClass());
        assertEquals("config-token", ctx.getTokenSupplier().get());
    }

    @Test
    public void fallsBackToDefaultForClassThatIsNotSupplier() {
        RangerPluginConfig config = newConfig();

        config.set(tokenSupplierProperty(config), NotASupplier.class.getName());
        config.set(jwtSourceProperty(config), "env");

        RangerPluginContext ctx = new RangerPluginContext(config);

        assertEquals(DefaultTokenSupplier.class, ctx.getTokenSupplier().getClass());
    }

    @Test
    public void fallsBackToDefaultForUnknownClass() {
        RangerPluginConfig config = newConfig();

        config.set(tokenSupplierProperty(config), "org.apache.ranger.NoSuchTokenSupplier");
        config.set(jwtSourceProperty(config), "env");

        RangerPluginContext ctx = new RangerPluginContext(config);

        assertEquals(DefaultTokenSupplier.class, ctx.getTokenSupplier().getClass());
    }

    @Test
    public void fallsBackToDefaultWhenProviderConstructorThrows() {
        RangerPluginConfig config = newConfig();

        config.set(tokenSupplierProperty(config), FailingTokenSupplier.class.getName());
        config.set(jwtSourceProperty(config), "env");

        RangerPluginContext ctx = new RangerPluginContext(config);

        assertEquals(DefaultTokenSupplier.class, ctx.getTokenSupplier().getClass());
    }

    @Test
    public void noTokenSupplierWhenCustomProviderFailsAndNoJwtSource() {
        RangerPluginConfig config = newConfig();

        config.set(tokenSupplierProperty(config), "org.apache.ranger.NoSuchTokenSupplier");

        RangerPluginContext ctx = new RangerPluginContext(config);

        assertNull(ctx.getTokenSupplier());
    }

    public static class NoArgTokenSupplier implements Supplier<String> {
        @Override
        public String get() {
            return "no-arg-token";
        }
    }

    public static class ConfigTokenSupplier implements Supplier<String> {
        public ConfigTokenSupplier(Configuration config) {
            /* custom providers may accept the Ranger Configuration */
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

    public static class FailingTokenSupplier implements Supplier<String> {
        public FailingTokenSupplier() {
            throw new IllegalStateException("token source unavailable");
        }

        @Override
        public String get() {
            return "never-returned";
        }
    }

    private RangerPluginConfig newConfig() {
        return new RangerPluginConfig("hive", "test-service", "test-app", "cl1", "on-perm", new RangerPolicyEngineOptions());
    }

    private String tokenSupplierProperty(RangerPluginConfig config) {
        return config.getPropertyPrefix() + ".policy.rest.client" + DefaultTokenSupplier.JWT_SUPPLIER;
    }

    private String jwtSourceProperty(RangerPluginConfig config) {
        return config.getPropertyPrefix() + ".policy.rest.client" + DefaultTokenSupplier.JWT_SOURCE;
    }
}
