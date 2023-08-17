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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.kafka.common.Endpoint;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.security.JaasContext;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.utils.SecurityUtils;
import org.apache.kafka.server.authorizer.AclCreateResult;
import org.apache.kafka.server.authorizer.AclDeleteResult;
import org.apache.kafka.server.authorizer.Action;
import org.apache.kafka.server.authorizer.AuthorizableRequestContext;
import org.apache.kafka.server.authorizer.AuthorizationResult;
import org.apache.kafka.server.authorizer.Authorizer;
import org.apache.kafka.server.authorizer.AuthorizerServerInfo;
import org.apache.ranger.audit.provider.MiscUtil;
import org.apache.ranger.plugin.policyengine.RangerAccessRequest;
import org.apache.ranger.plugin.policyengine.RangerAccessRequestImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessResourceImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessResult;
import org.apache.ranger.plugin.service.RangerBasePlugin;
import org.apache.ranger.plugin.util.RangerPerfTracer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RangerKafkaAuthorizer implements Authorizer {
  private static final Logger logger = LoggerFactory.getLogger(RangerKafkaAuthorizer.class);
  private static final Logger PERF_KAFKAAUTH_REQUEST_LOG = RangerPerfTracer.getPerfLogger("kafkaauth.request");

  public static final String ACCESS_TYPE_ALTER_CONFIGS = "alter_configs";
  public static final String KEY_TOPIC = "topic";
  public static final String KEY_CLUSTER = "cluster";
  public static final String KEY_CONSUMER_GROUP = "consumergroup";
  public static final String KEY_TRANSACTIONALID = "transactionalid";
  public static final String KEY_DELEGATIONTOKEN = "delegationtoken";
  public static final String ACCESS_TYPE_READ = "consume";
  public static final String ACCESS_TYPE_WRITE = "publish";
  public static final String ACCESS_TYPE_CREATE = "create";
  public static final String ACCESS_TYPE_DELETE = "delete";
  public static final String ACCESS_TYPE_CONFIGURE = "configure";
  public static final String ACCESS_TYPE_DESCRIBE = "describe";
  public static final String ACCESS_TYPE_DESCRIBE_CONFIGS = "describe_configs";
  public static final String ACCESS_TYPE_CLUSTER_ACTION = "cluster_action";
  public static final String ACCESS_TYPE_IDEMPOTENT_WRITE = "idempotent_write";

  private static volatile RangerBasePlugin rangerPlugin = null;
  RangerKafkaAuditHandler auditHandler = null;

  public RangerKafkaAuthorizer() {
  }

  private static String mapToRangerAccessType(AclOperation operation) {
    switch (operation) {
      case READ:
        return ACCESS_TYPE_READ;
      case WRITE:
        return ACCESS_TYPE_WRITE;
      case ALTER:
        return ACCESS_TYPE_CONFIGURE;
      case DESCRIBE:
        return ACCESS_TYPE_DESCRIBE;
      case CLUSTER_ACTION:
        return ACCESS_TYPE_CLUSTER_ACTION;
      case CREATE:
        return ACCESS_TYPE_CREATE;
      case DELETE:
        return ACCESS_TYPE_DELETE;
      case DESCRIBE_CONFIGS:
        return ACCESS_TYPE_DESCRIBE_CONFIGS;
      case ALTER_CONFIGS:
        return ACCESS_TYPE_ALTER_CONFIGS;
      case IDEMPOTENT_WRITE:
        return ACCESS_TYPE_IDEMPOTENT_WRITE;
      case UNKNOWN:
      case ANY:
      case ALL:
      default:
        return null;
    }
  }

  private static String mapToResourceType(ResourceType resourceType) {
    switch (resourceType) {
      case TOPIC:
        return KEY_TOPIC;
      case CLUSTER:
        return KEY_CLUSTER;
      case GROUP:
        return KEY_CONSUMER_GROUP;
      case TRANSACTIONAL_ID:
        return KEY_TRANSACTIONALID;
      case DELEGATION_TOKEN:
        return KEY_DELEGATIONTOKEN;
      case ANY:
      case UNKNOWN:
      default:
        return null;
    }
  }

  private static RangerAccessResourceImpl createRangerAccessResource(String resourceTypeKey, String resourceName) {
    RangerAccessResourceImpl rangerResource = new RangerAccessResourceImpl();
    rangerResource.setValue(resourceTypeKey, resourceName);
    return rangerResource;
  }

  private static RangerAccessRequestImpl createRangerAccessRequest(String userName,
                                                                   Set<String> userGroups,
                                                                   String ip,
                                                                   Date eventTime,
                                                                   String resourceTypeKey,
                                                                   String resourceName,
                                                                   String accessType) {
    RangerAccessRequestImpl rangerRequest = new RangerAccessRequestImpl();
    rangerRequest.setResource(createRangerAccessResource(resourceTypeKey, resourceName));
    rangerRequest.setUser(userName);
    rangerRequest.setUserGroups(userGroups);
    rangerRequest.setClientIPAddress(ip);
    rangerRequest.setAccessTime(eventTime);
    rangerRequest.setAccessType(accessType);
    rangerRequest.setAction(accessType);
    rangerRequest.setRequestData(resourceName);
    return rangerRequest;
  }

  private static List<AuthorizationResult> denyAll(List<Action> actions) {
    return actions.stream().map(a -> AuthorizationResult.DENIED).collect(Collectors.toList());
  }

  private static List<AuthorizationResult> mapResults(List<Action> actions, Collection<RangerAccessResult> results) {
    if (CollectionUtils.isEmpty(results)) {
      logger.error("Ranger Plugin returned null or empty. Returning Denied for all");
      return denyAll(actions);
    }
    return results.stream()
        .map(r -> r != null && r.getIsAllowed() ? AuthorizationResult.ALLOWED : AuthorizationResult.DENIED)
        .collect(Collectors.toList());
  }

  private static String toString(AuthorizableRequestContext requestContext) {
    return requestContext == null ? null :
        String.format("AuthorizableRequestContext{principal=%s, clientAddress=%s, clientId=%s}",
            requestContext.principal(), requestContext.clientAddress(), requestContext.clientId());
  }

  @Override
  public void close() {
    logger.info("close() called on authorizer.");
    try {
      if (rangerPlugin != null) {
        rangerPlugin.cleanup();
      }
    } catch (Throwable t) {
      logger.error("Error closing RangerPlugin.", t);
    }
  }

  @Override
  public void configure(Map<String, ?> configs) {
    RangerBasePlugin me = rangerPlugin;
    if (me == null) {
      synchronized (RangerKafkaAuthorizer.class) {
        me = rangerPlugin;
        if (me == null) {
          try {
            // Possible to override JAAS configuration which is used by Ranger, otherwise
            // SASL_PLAINTEXT is used, which force Kafka to use 'sasl_plaintext.KafkaServer',
            // if it's not defined, then it reverts to 'KafkaServer' configuration.
            final Object jaasContext = configs.get("ranger.jaas.context");
            final String listenerName = (jaasContext instanceof String
                && StringUtils.isNotEmpty((String) jaasContext)) ? (String) jaasContext
                : SecurityProtocol.SASL_PLAINTEXT.name();
            final String saslMechanism = SaslConfigs.GSSAPI_MECHANISM;
            JaasContext context = JaasContext.loadServerContext(new ListenerName(listenerName), saslMechanism, configs);
            MiscUtil.setUGIFromJAASConfig(context.name());
            UserGroupInformation loginUser = MiscUtil.getUGILoginUser();
            logger.info("LoginUser={}", loginUser);
          } catch (Throwable t) {
            logger.error("Error getting principal.", t);
          }
          rangerPlugin = new RangerBasePlugin("kafka", "kafka");
          logger.info("Calling plugin.init()");
          rangerPlugin.init();
          auditHandler = new RangerKafkaAuditHandler();
          rangerPlugin.setResultProcessor(auditHandler);
        }
      }
    }
  }

  @Override
  public Map<Endpoint, ? extends CompletionStage<Void>> start(AuthorizerServerInfo serverInfo) {
    return serverInfo.endpoints().stream()
        .collect(Collectors.toMap(endpoint -> endpoint, endpoint -> CompletableFuture.completedFuture(null), (a, b) -> b));
  }

  @Override
  public List<AuthorizationResult> authorize(AuthorizableRequestContext requestContext, List<Action> actions) {
    if (rangerPlugin == null) {
      MiscUtil.logErrorMessageByInterval(logger, "Authorizer is still not initialized");
      return denyAll(actions);
    }

    RangerPerfTracer perf = null;
    if (RangerPerfTracer.isPerfTraceEnabled(PERF_KAFKAAUTH_REQUEST_LOG)) {
      perf = RangerPerfTracer.getPerfTracer(PERF_KAFKAAUTH_REQUEST_LOG, "RangerKafkaAuthorizer.authorize(actions=" + actions + ")");
    }
    try {
      return wrappedAuthorization(requestContext, actions);
    } finally {
      RangerPerfTracer.log(perf);
    }
  }

  private List<AuthorizationResult> wrappedAuthorization(AuthorizableRequestContext requestContext, List<Action> actions) {
    if (CollectionUtils.isEmpty(actions)) {
      return Collections.emptyList();
    }
    String userName = requestContext.principal() == null ? null : requestContext.principal().getName();
    Set<String> userGroups = MiscUtil.getGroupsForRequestUser(userName);
    String hostAddress = requestContext.clientAddress() == null ? null : requestContext.clientAddress().getHostAddress();
    String ip = StringUtils.isNotEmpty(hostAddress) && hostAddress.charAt(0) == '/' ? hostAddress.substring(1) : hostAddress;
    Date eventTime = new Date();

    List<RangerAccessRequest> rangerRequests = new ArrayList<>();
    for (Action action : actions) {
      String accessType = mapToRangerAccessType(action.operation());
      if (accessType == null) {
        MiscUtil.logErrorMessageByInterval(logger, "Unsupported access type, requestContext=" + toString(requestContext) +
            ", actions=" + actions + ", operation=" + action.operation());
        return denyAll(actions);
      }
      String resourceTypeKey = mapToResourceType(action.resourcePattern().resourceType());
      if (resourceTypeKey == null) {
        MiscUtil.logErrorMessageByInterval(logger, "Unsupported resource type, requestContext=" + toString(requestContext) +
            ", actions=" + actions + ", resourceType=" + action.resourcePattern().resourceType());
        return denyAll(actions);
      }

      RangerAccessRequestImpl rangerAccessRequest = createRangerAccessRequest(
          userName,
          userGroups,
          ip,
          eventTime,
          resourceTypeKey,
          action.resourcePattern().name(),
          accessType);
      rangerRequests.add(rangerAccessRequest);
    }

    Collection<RangerAccessResult> results = callRangerPlugin(rangerRequests);

    List<AuthorizationResult> authorizationResults = mapResults(actions, results);

    logger.debug("rangerRequests={}, return={}", rangerRequests, authorizationResults);
    return authorizationResults;
  }

  private Collection<RangerAccessResult> callRangerPlugin(List<RangerAccessRequest> rangerRequests) {
    try {
      return rangerPlugin.isAccessAllowed(rangerRequests);
    } catch (Throwable t) {
      logger.error("Error while calling isAccessAllowed(). requests={}", rangerRequests, t);
      return null;
    } finally {
      auditHandler.flushAudit();
    }
  }

  @Override
  public List<? extends CompletionStage<AclCreateResult>> createAcls(AuthorizableRequestContext requestContext, List<AclBinding> aclBindings) {
    logger.error("createAcls is not supported by Ranger for Kafka");

    return aclBindings.stream()
        .map(ab -> {
          CompletableFuture<AclCreateResult> completableFuture = new CompletableFuture<>();
          completableFuture.completeExceptionally(new UnsupportedOperationException("createAcls is not supported by Ranger for Kafka"));
          return completableFuture;
        })
        .collect(Collectors.toList());
  }

  @Override
  public List<? extends CompletionStage<AclDeleteResult>> deleteAcls(AuthorizableRequestContext requestContext, List<AclBindingFilter> aclBindingFilters) {
    logger.error("deleteAcls is not supported by Ranger for Kafka");
    return aclBindingFilters.stream()
        .map(ab -> {
          CompletableFuture<AclDeleteResult> completableFuture = new CompletableFuture<>();
          completableFuture.completeExceptionally(new UnsupportedOperationException("deleteAcls is not supported by Ranger for Kafka"));
          return completableFuture;
        })
        .collect(Collectors.toList());
  }

  // TODO: provide a real implementation (RANGER-3809)
  // Currently we return a dummy implementation because KAFKA-13598 makes producers idempotent by default and this causes
  // a failure in the InitProducerId API call on the broker side because of the missing acls() method implementation.
  // Overriding this with a dummy impl will make Kafka return an authorization error instead of an exception if the
  // IDEMPOTENT_WRITE permission wasn't set on the producer.
  @Override
  public AuthorizationResult authorizeByResourceType(AuthorizableRequestContext requestContext, AclOperation op, ResourceType resourceType) {
    SecurityUtils.authorizeByResourceTypeCheckArgs(op, resourceType);

    logger.debug("authorizeByResourceType call is not supported by Ranger for Kafka yet");
    return AuthorizationResult.DENIED;
  }

  @Override
  public Iterable<AclBinding> acls(AclBindingFilter filter) {
    logger.error("(getting) acls is not supported by Ranger for Kafka");
    throw new UnsupportedOperationException("(getting) acls is not supported by Ranger for Kafka");
  }
}
