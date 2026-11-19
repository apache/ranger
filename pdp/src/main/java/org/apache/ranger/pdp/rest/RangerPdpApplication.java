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

package org.apache.ranger.pdp.rest;

import org.apache.ranger.authz.api.RangerAuthorizer;
import org.apache.ranger.pdp.config.RangerPdpConfig;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.glassfish.jersey.server.ResourceConfig;

/**
 * Jersey 2.x {@link ResourceConfig} for the Ranger PDP application.
 *
 * <p>Registers:
 * <ul>
 *   <li>{@link JacksonFeature} – Jackson 2.x JSON provider (honours all {@code @Json*}
 *       annotations on the {@code authz-api} model classes)
 *   <li>{@link AuthorizerBinder} – HK2 binder that makes the {@link RangerAuthorizer}
 *       instance injectable into {@link RangerPdpREST} via {@code @Inject}
 *   <li>{@link RangerPdpREST} – the JAX-RS resource class
 * </ul>
 */
public class RangerPdpApplication extends ResourceConfig {
    public RangerPdpApplication(RangerAuthorizer authorizer, RangerPdpConfig config) {
        register(JacksonFeature.class);
        register(new AuthorizerBinder(authorizer, config));
        register(RangerPdpREST.class);
    }

    private static class AuthorizerBinder extends AbstractBinder {
        private final RangerAuthorizer authorizer;
        private final RangerPdpConfig  config;

        AuthorizerBinder(RangerAuthorizer authorizer, RangerPdpConfig config) {
            this.authorizer = authorizer;
            this.config     = config;
        }

        @Override
        protected void configure() {
            bind(authorizer).to(RangerAuthorizer.class);
            bind(config).to(RangerPdpConfig.class);
        }
    }
}
