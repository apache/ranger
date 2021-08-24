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
import org.apache.kafka.server.authorizer.AclCreateResult;
import org.apache.kafka.server.authorizer.AclDeleteResult;
import org.apache.kafka.server.authorizer.Action;
import org.apache.kafka.server.authorizer.AuthorizableRequestContext;
import org.apache.kafka.server.authorizer.AuthorizationResult;
import org.apache.kafka.server.authorizer.Authorizer;
import org.apache.kafka.server.authorizer.AuthorizerServerInfo;
import org.apache.log4j.Logger;
import org.apache.ranger.plugin.classloader.RangerPluginClassLoader;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;


public class RangerKafkaAuthorizer implements Authorizer {
    private static final Logger LOG = Logger.getLogger(RangerKafkaAuthorizer.class);

    private static final String RANGER_PLUGIN_TYPE = "kafka";
    private static final String RANGER_KAFKA_AUTHORIZER_IMPL_CLASSNAME = "org.apache.ranger.authorization.kafka.authorizer.RangerKafkaAuthorizer";
    private static RangerPluginClassLoader rangerPluginClassLoader = null;

    private Authorizer rangerKafkaAuthorizerImpl = null;

    public RangerKafkaAuthorizer() {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> RangerKafkaAuthorizer.RangerKafkaAuthorizer()");
        }

        this.init();

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== RangerKafkaAuthorizer.RangerKafkaAuthorizer()");
        }
    }

    @Override
    public void configure(Map<String, ?> configs) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> RangerKafkaAuthorizer.configure(Map<String, ?>)");
        }

        try {
            activatePluginClassLoader();

            rangerKafkaAuthorizerImpl.configure(configs);
        } finally {
            deactivatePluginClassLoader();
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== RangerKafkaAuthorizer.configure(Map<String, ?>)");
        }
    }

    @Override
    public void close() throws IOException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> RangerKafkaAuthorizer.close()");
        }

        try {
            activatePluginClassLoader();

            rangerKafkaAuthorizerImpl.close();
        } finally {
            deactivatePluginClassLoader();
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== RangerKafkaAuthorizer.close()");
        }

    }

    @Override
    public Map<Endpoint, ? extends CompletionStage<Void>> start(AuthorizerServerInfo authorizerServerInfo) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> RangerKafkaAuthorizer.start()");
        }

        Map<Endpoint, ? extends CompletionStage<Void>> ret;

        try {
            activatePluginClassLoader();

            ret = rangerKafkaAuthorizerImpl.start(authorizerServerInfo);

        } finally {
            deactivatePluginClassLoader();
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== RangerKafkaAuthorizer.start()");
        }

        return ret;
    }

    @Override
    public List<AuthorizationResult> authorize(AuthorizableRequestContext authorizableRequestContext, List<Action> list) {

        if (LOG.isDebugEnabled()) {
            String operations = list.stream().map(action -> action.operation().name()).collect(Collectors.joining(","));
            String resources = list.stream().map(action -> action.resourcePattern().toString()).collect(Collectors.joining(","));
            LOG.debug(String.format("==> RangerKafkaAuthorizer.authorize(Principal=%s, ClientAddress=%s, Operations=%s Resources=%s)",
                    authorizableRequestContext.principal(), authorizableRequestContext.clientAddress(), operations, resources));
        }

        List<AuthorizationResult> ret;

        try {
            activatePluginClassLoader();

            ret = rangerKafkaAuthorizerImpl.authorize(authorizableRequestContext, list);
        } finally {
            deactivatePluginClassLoader();
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== RangerKafkaAuthorizer.authorize: " + ret);
        }

        return ret;
    }

    @Override
    public List<? extends CompletionStage<AclCreateResult>> createAcls(AuthorizableRequestContext authorizableRequestContext, List<AclBinding> list) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> RangerKafkaAuthorizer.createAcls(AuthorizableRequestContext, List<AclBinding>)");
        }

        List<? extends CompletionStage<AclCreateResult>> ret;

        try {
            activatePluginClassLoader();
            ret = rangerKafkaAuthorizerImpl.createAcls(authorizableRequestContext, list);
        } finally {
            deactivatePluginClassLoader();
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== RangerKafkaAuthorizer.createAcls(AuthorizableRequestContext, List<AclBinding>)");
        }

        return ret;
    }

    @Override
    public List<? extends CompletionStage<AclDeleteResult>> deleteAcls(AuthorizableRequestContext authorizableRequestContext,
                                                                       List<AclBindingFilter> list) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> RangerKafkaAuthorizer.deleteAcls(AuthorizableRequestContext, List<AclBindingFilter>)");
        }

        List<? extends CompletionStage<AclDeleteResult>> ret;
        try {
            activatePluginClassLoader();

            ret = rangerKafkaAuthorizerImpl.deleteAcls(authorizableRequestContext, list);
        } finally {
            deactivatePluginClassLoader();
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== RangerKafkaAuthorizer.deleteAcls(AuthorizableRequestContext, List<AclBindingFilter>)");
        }

        return ret;
    }

    @Override
    public Iterable<AclBinding> acls(AclBindingFilter aclBindingFilter) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> RangerKafkaAuthorizer.acls(AclBindingFilter)");
        }

        Iterable<AclBinding> ret;

        try {
            activatePluginClassLoader();
            ret = rangerKafkaAuthorizerImpl.acls(aclBindingFilter);
        } finally {
            deactivatePluginClassLoader();
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== RangerKafkaAuthorizer.acls(AclBindingFilter)");
        }

        return ret;
    }


    private void activatePluginClassLoader() {
        if (rangerPluginClassLoader != null) {
            rangerPluginClassLoader.activate();
        }
    }

    private void deactivatePluginClassLoader() {
        if (rangerPluginClassLoader != null) {
            rangerPluginClassLoader.deactivate();
        }
    }

    private void init() {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> RangerKafkaAuthorizer.init()");
        }

        try {

            rangerPluginClassLoader = RangerPluginClassLoader.getInstance(RANGER_PLUGIN_TYPE, this.getClass());

            @SuppressWarnings("unchecked")
            Class<Authorizer> cls = (Class<Authorizer>) Class.forName(RANGER_KAFKA_AUTHORIZER_IMPL_CLASSNAME, true, rangerPluginClassLoader);

            activatePluginClassLoader();

            rangerKafkaAuthorizerImpl = cls.newInstance();
        } catch (Exception e) {
            // check what need to be done
            LOG.error("Error Enabling RangerKafkaPlugin", e);
        } finally {
            deactivatePluginClassLoader();
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== RangerKafkaAuthorizer.init()");
        }
    }
}
