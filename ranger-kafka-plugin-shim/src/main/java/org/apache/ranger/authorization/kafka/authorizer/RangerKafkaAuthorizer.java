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
import org.apache.kafka.server.authorizer.*;
import org.apache.ranger.plugin.classloader.RangerPluginClassLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;

public class RangerKafkaAuthorizer implements Authorizer {
  private static final Logger logger = LoggerFactory.getLogger(RangerKafkaAuthorizer.class);

  private static final String RANGER_PLUGIN_TYPE = "kafka";
  private static final String RANGER_KAFKA_AUTHORIZER_IMPL_CLASSNAME = "org.apache.ranger.authorization.kafka.authorizer.RangerKafkaAuthorizer";

  private Authorizer rangerKafkaAuthorizerImpl = null;
  private RangerPluginClassLoader rangerPluginClassLoader = null;

  public RangerKafkaAuthorizer() {
    logger.debug("==> RangerKafkaAuthorizer.RangerKafkaAuthorizer()");

    this.init();

    logger.debug("<== RangerKafkaAuthorizer.RangerKafkaAuthorizer()");
  }

  private static String toString(AuthorizableRequestContext requestContext) {
    return requestContext == null ? null :
        String.format("AuthorizableRequestContext{principal=%s, clientAddress=%s, clientId=%s}",
            requestContext.principal(), requestContext.clientAddress(), requestContext.clientId());
  }

  private void init() {
    logger.debug("==> RangerKafkaAuthorizer.init()");

    try {

      rangerPluginClassLoader = RangerPluginClassLoader.getInstance(RANGER_PLUGIN_TYPE, this.getClass());

      @SuppressWarnings("unchecked")
      Class<Authorizer> cls = (Class<Authorizer>) Class.forName(RANGER_KAFKA_AUTHORIZER_IMPL_CLASSNAME, true, rangerPluginClassLoader);

      activatePluginClassLoader();

      rangerKafkaAuthorizerImpl = cls.newInstance();
    } catch (Exception e) {
      logger.error("Error Enabling RangerKafkaPlugin", e);
      throw new IllegalStateException("Error Enabling RangerKafkaPlugin", e);
    } finally {
      deactivatePluginClassLoader();
    }

    logger.debug("<== RangerKafkaAuthorizer.init()");
  }

  @Override
  public void configure(Map<String, ?> configs) {
    logger.debug("==> RangerKafkaAuthorizer.configure(Map<String, ?>)");

    try {
      activatePluginClassLoader();

      rangerKafkaAuthorizerImpl.configure(configs);
    } finally {
      deactivatePluginClassLoader();
    }

    logger.debug("<== RangerKafkaAuthorizer.configure(Map<String, ?>)");
  }

  @Override
  public Map<Endpoint, ? extends CompletionStage<Void>> start(AuthorizerServerInfo authorizerServerInfo) {
    logger.debug("==> RangerKafkaAuthorizer.start(AuthorizerServerInfo)");

    try {
      activatePluginClassLoader();

      return rangerKafkaAuthorizerImpl.start(authorizerServerInfo);
    } finally {
      deactivatePluginClassLoader();
      logger.debug("<== RangerKafkaAuthorizer.start(AuthorizerServerInfo)");
    }
  }

  @Override
  public void close() throws IOException {
    logger.debug("==> RangerKafkaAuthorizer.close()");

    try {
      activatePluginClassLoader();

      rangerKafkaAuthorizerImpl.close();
    } finally {
      deactivatePluginClassLoader();
    }

    logger.debug("<== RangerKafkaAuthorizer.close()");
  }

  @Override
  public List<AuthorizationResult> authorize(AuthorizableRequestContext requestContext, List<Action> actions) {
    logger.debug("==> RangerKafkaAuthorizer.authorize(AuthorizableRequestContext={}, List<Action>={})", toString(requestContext), actions);

    List<AuthorizationResult> ret;

    try {
      activatePluginClassLoader();

      ret = rangerKafkaAuthorizerImpl.authorize(requestContext, actions);
    } finally {
      deactivatePluginClassLoader();
    }

    logger.debug("<== RangerKafkaAuthorizer.authorize: {}", ret);

    return ret;
  }

  @Override
  public List<? extends CompletionStage<AclCreateResult>> createAcls(AuthorizableRequestContext requestContext, List<AclBinding> aclBindings) {
    logger.debug("==> RangerKafkaAuthorizer.createAcls(AuthorizableRequestContext, List<AclBinding>)");

    try {
      activatePluginClassLoader();

      return rangerKafkaAuthorizerImpl.createAcls(requestContext, aclBindings);
    } finally {
      deactivatePluginClassLoader();
      logger.debug("<== RangerKafkaAuthorizer.createAcls(AuthorizableRequestContext, List<AclBinding>)");
    }
  }

  @Override
  public List<? extends CompletionStage<AclDeleteResult>> deleteAcls(AuthorizableRequestContext requestContext, List<AclBindingFilter> aclBindingFilters) {
    logger.debug("==> RangerKafkaAuthorizer.deleteAcls(AuthorizableRequestContext, List<AclBindingFilter>)");

    try {
      activatePluginClassLoader();

      return rangerKafkaAuthorizerImpl.deleteAcls(requestContext, aclBindingFilters);
    } finally {
      deactivatePluginClassLoader();
      logger.debug("<== RangerKafkaAuthorizer.deleteAcls(AuthorizableRequestContext, List<AclBindingFilter>)");
    }
  }

  @Override
  public Iterable<AclBinding> acls(AclBindingFilter filter) {
    logger.debug("==> RangerKafkaAuthorizer.acls(AclBindingFilter)");

    try {
      activatePluginClassLoader();

      return rangerKafkaAuthorizerImpl.acls(filter);
    } finally {
      deactivatePluginClassLoader();
      logger.debug("<== RangerKafkaAuthorizer.acls(AclBindingFilter)");
    }
  }

  @Override
  public AuthorizationResult authorizeByResourceType(AuthorizableRequestContext requestContext, AclOperation op, ResourceType resourceType) {
    if (logger.isDebugEnabled()) {
      logger.debug("==> RangerKafkaAuthorizer.authorizeByResourceType(AuthorizableRequestContext={}, AclOperation={}, ResourceType={})",
          toString(requestContext), op, resourceType);
    }

    AuthorizationResult ret;

    try {
      activatePluginClassLoader();

      ret = rangerKafkaAuthorizerImpl.authorizeByResourceType(requestContext, op, resourceType);
    } finally {
      deactivatePluginClassLoader();
    }

    if (logger.isDebugEnabled()) {
      logger.debug("<== RangerKafkaAuthorizer.authorizeByResourceType: {}", ret);
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
}
