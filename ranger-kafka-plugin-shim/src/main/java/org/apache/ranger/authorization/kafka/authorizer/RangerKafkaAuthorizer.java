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

package org.apache.ranger.authorization.kafka.authorizer;

import org.apache.kafka.common.Endpoint;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.server.authorizer.AclCreateResult;
import org.apache.kafka.server.authorizer.AclDeleteResult;
import org.apache.kafka.server.authorizer.Action;
import org.apache.kafka.server.authorizer.AuthorizableRequestContext;
import org.apache.kafka.server.authorizer.AuthorizationResult;
import org.apache.kafka.server.authorizer.Authorizer;
import org.apache.kafka.server.authorizer.AuthorizerServerInfo;
import org.apache.ranger.plugin.classloader.RangerPluginClassLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;

public class RangerKafkaAuthorizer implements Authorizer {
    private static final Logger logger = LoggerFactory.getLogger(RangerKafkaAuthorizer.class);

    private static final String RANGER_PLUGIN_TYPE                     = "kafka";
    private static final String RANGER_KAFKA_AUTHORIZER_IMPL_CLASSNAME = "org.apache.ranger.authorization.kafka.authorizer.RangerKafkaAuthorizer";

    private Authorizer              rangerKafkaAuthorizerImpl;
    private RangerPluginClassLoader rangerPluginClassLoader;

    public RangerKafkaAuthorizer() {
        logger.debug("==> RangerKafkaAuthorizer.RangerKafkaAuthorizer()");

        this.init();

        logger.debug("<== RangerKafkaAuthorizer.RangerKafkaAuthorizer()");
    }

    @Override
    public void configure(Map<String, ?> configs) {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator("configure")) {
            rangerKafkaAuthorizerImpl.configure(configs);
        }
    }

    @Override
    public Map<Endpoint, ? extends CompletionStage<Void>> start(AuthorizerServerInfo authorizerServerInfo) {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator("start")) {
            return rangerKafkaAuthorizerImpl.start(authorizerServerInfo);
        }
    }

    @Override
    public List<AuthorizationResult> authorize(AuthorizableRequestContext requestContext, List<Action> actions) {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator("authorize")) {
            return rangerKafkaAuthorizerImpl.authorize(requestContext, actions);
        }
    }

    @Override
    public List<? extends CompletionStage<AclCreateResult>> createAcls(AuthorizableRequestContext requestContext, List<AclBinding> aclBindings) {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator("createAcls")) {
            return rangerKafkaAuthorizerImpl.createAcls(requestContext, aclBindings);
        }
    }

    @Override
    public List<? extends CompletionStage<AclDeleteResult>> deleteAcls(AuthorizableRequestContext requestContext, List<AclBindingFilter> aclBindingFilters) {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator("deleteAcls")) {
            return rangerKafkaAuthorizerImpl.deleteAcls(requestContext, aclBindingFilters);
        }
    }

    @Override
    public Iterable<AclBinding> acls(AclBindingFilter filter) {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator("acls")) {
            return rangerKafkaAuthorizerImpl.acls(filter);
        }
    }

    @Override
    public AuthorizationResult authorizeByResourceType(AuthorizableRequestContext requestContext, AclOperation op, ResourceType resourceType) {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator("authorizeByResourceType")) {
            return rangerKafkaAuthorizerImpl.authorizeByResourceType(requestContext, op, resourceType);
        }
    }

    @Override
    public void close() throws IOException {
        try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator("close")) {
            rangerKafkaAuthorizerImpl.close();
        }
    }

    private void init() {
        logger.debug("==> RangerKafkaAuthorizer.init()");

        try {
            rangerPluginClassLoader = RangerPluginClassLoader.getInstance(RANGER_PLUGIN_TYPE, this.getClass());

            @SuppressWarnings("unchecked")
            Class<Authorizer> cls = (Class<Authorizer>) Class.forName(RANGER_KAFKA_AUTHORIZER_IMPL_CLASSNAME, true, rangerPluginClassLoader);

            try (PluginClassLoaderActivator ignored = new PluginClassLoaderActivator("init")) {
                rangerKafkaAuthorizerImpl = cls.newInstance();
            }
        } catch (Exception e) {
            logger.error("Error Enabling RangerKafkaPlugin", e);
            throw new IllegalStateException("Error Enabling RangerKafkaPlugin", e);
        }

        logger.debug("<== RangerKafkaAuthorizer.init()");
    }

    private class PluginClassLoaderActivator implements AutoCloseable {
        private final String methodName;

        PluginClassLoaderActivator(String methodName) {
            logger.debug("==> RangerKafkaAuthorizer.{}()", methodName);

            this.methodName = methodName;

            if (rangerPluginClassLoader != null) {
                rangerPluginClassLoader.activate();
            }
        }

        @Override
        public void close() {
            if (rangerPluginClassLoader != null) {
                rangerPluginClassLoader.deactivate();
            }

            logger.debug("<== RangerKafkaAuthorizer.{}()", methodName);
        }
    }
}
