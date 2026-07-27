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

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.kafka.common.Endpoint;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.errors.ApiException;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.security.JaasContext;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
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
import org.apache.ranger.authorization.kafka.authorizer.utils.RangerKafkaCheckAccess;
import org.apache.ranger.authorization.kafka.authorizer.utils.RangerKafkaGrantAccess;
import org.apache.ranger.authorization.kafka.authorizer.utils.RangerKafkaListAccess;
import org.apache.ranger.authorization.kafka.authorizer.utils.RangerKafkaRevokeAccess;
import org.apache.ranger.authorization.kafka.authorizer.utils.RangerKafkaUtils;
import org.apache.ranger.plugin.service.RangerBasePlugin;
import org.apache.ranger.plugin.util.RangerPerfTracer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;

public class RangerKafkaAuthorizer implements Authorizer {
    private static final Logger logger                      = LoggerFactory.getLogger(RangerKafkaAuthorizer.class);
    private static final Logger PERF_KAFKAAUTH_REQUEST_LOG  = RangerPerfTracer.getPerfLogger("kafkaauth.request");

    private static final String KAFKA_SUPER_USERS_PROP = "super.users";

    private static volatile RangerBasePlugin rangerPlugin;

    RangerKafkaAuditHandler auditHandler;
    RangerKafkaUtils rangerKafkaUtils = new RangerKafkaUtils();

    public RangerKafkaAuthorizer() {
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
                        final String listenerName = (jaasContext instanceof String && StringUtils.isNotEmpty((String) jaasContext)) ? (String) jaasContext : SecurityProtocol.SASL_PLAINTEXT.name();
                        final String saslMechanism = SaslConfigs.GSSAPI_MECHANISM;
                        JaasContext  context       = JaasContext.loadServerContext(new ListenerName(listenerName), saslMechanism, configs);

                        MiscUtil.setUGIFromJAASConfig(context.name());

                        UserGroupInformation loginUser = MiscUtil.getUGILoginUser();

                        logger.info("LoginUser = {}", loginUser);
                    } catch (Throwable t) {
                        logger.error("Error getting principal.", t);
                    }

                    me = new RangerBasePlugin("kafka", "kafka");

                    logger.info("Calling plugin.init()");

                    me.init();

                    Set<String> superUsersFromKafkaConfig = parseSuperUsersFromKafkaConfig(configs);

                    me.getPluginContext().getConfig().addSuperUsers(superUsersFromKafkaConfig);

                    logger.info("Super users added from Kafka config: {}", superUsersFromKafkaConfig);

                    auditHandler = new RangerKafkaAuditHandler();

                    me.setResultProcessor(auditHandler);

                    rangerPlugin = me;
                }
            }
        }
    }

    @Override
    public Map<Endpoint, ? extends CompletionStage<Void>> start(AuthorizerServerInfo serverInfo) {
        return serverInfo.endpoints().stream().collect(Collectors.toMap(endpoint -> endpoint, endpoint -> CompletableFuture.completedFuture(null), (a, b) -> b));
    }

    @Override
    public List<AuthorizationResult> authorize(AuthorizableRequestContext requestContext, List<Action> actions) {
        if (rangerPlugin == null) {
            MiscUtil.logErrorMessageByInterval(logger, "Authorizer is still not initialized");

            return RangerKafkaUtils.denyAll(actions);
        }

        RangerPerfTracer perf = null;

        if (RangerPerfTracer.isPerfTraceEnabled(PERF_KAFKAAUTH_REQUEST_LOG)) {
            perf = RangerPerfTracer.getPerfTracer(PERF_KAFKAAUTH_REQUEST_LOG, "RangerKafkaAuthorizer.authorize(actions=" + actions + ")");
        }

        try {
            return rangerKafkaUtils.wrappedAuthorization(requestContext, actions, rangerPlugin, auditHandler);
        } finally {
            RangerPerfTracer.log(perf);
        }
    }

    @Override
    public List<? extends CompletionStage<AclCreateResult>> createAcls(AuthorizableRequestContext requestContext, List<AclBinding> aclBindings) {
        List<? extends CompletionStage<AclCreateResult>> ret;

        if (logger.isDebugEnabled()) {
            logger.debug("==> RangerKafkaAuthorizer.createAcls(): AuthorizableRequestContext: {} aclBindings: {}", requestContext, aclBindings);
        }

        RangerKafkaGrantAccess rangerKafkaGrantAccess = new RangerKafkaGrantAccess();

        ret = aclBindings.stream()
                .map(aclBinding -> {
                    CompletableFuture<AclCreateResult> completableFuture = new CompletableFuture<>();
                    completableFuture.complete(rangerKafkaGrantAccess.grant(requestContext, aclBinding, rangerPlugin, auditHandler));
                    return completableFuture;
                })
                .collect(Collectors.toList());

        if (logger.isDebugEnabled()) {
            logger.debug("<== RangerKafkaAuthorizer.createAcls() result: {}", ret);
        }

        return ret;
    }

    @Override
    public List<? extends CompletionStage<AclDeleteResult>> deleteAcls(AuthorizableRequestContext requestContext, List<AclBindingFilter> aclBindingFilters) {
        if (logger.isDebugEnabled()) {
            logger.debug("==> RangerKafkaAuthorizer.deleteAcls(): AuthorizableRequestContext: {} aclBindingFilters : {}", requestContext, aclBindingFilters);
        }

        List<? extends CompletionStage<AclDeleteResult>> ret;
        List<CompletableFuture<AclDeleteResult>> completableFutures = new ArrayList<>();

        for (AclBindingFilter filter : aclBindingFilters) {
            RangerKafkaRevokeAccess rangerKafkaRevokeAccess = new RangerKafkaRevokeAccess();
            CompletableFuture<AclDeleteResult> completableFuture = new CompletableFuture<>();
            try {
                AclDeleteResult aclDeleteResult = rangerKafkaRevokeAccess.revoke(filter, rangerPlugin, auditHandler, requestContext);
                completableFuture.complete(aclDeleteResult);
            } catch (Exception e) {
                completableFuture.completeExceptionally(new ApiException(e.getMessage()));
            }
            completableFutures.add(completableFuture);
        }

        ret = completableFutures.stream().collect(Collectors.toList());

        if (logger.isDebugEnabled()) {
            logger.debug("<== RangerKafkaAuthorizer.deleteAcls() result: {}", ret);
        }

        return ret;
    }

    @Override
    public Iterable<AclBinding> acls(AclBindingFilter filter) {
        if (logger.isDebugEnabled()) {
            logger.debug("==> RangerKafkaAuthorizer.acls(): AclBindingFilter: {}", filter);
        }

        RangerKafkaListAccess rangerKafkaListAccess = new RangerKafkaListAccess();
        Iterable<AclBinding> ret = rangerKafkaListAccess.getAclBindings(filter, rangerPlugin);

        if (logger.isDebugEnabled()) {
            logger.debug("<== RangerKafkaAuthorizer.acls(): AclBindingFilter: {}", ret);
        }

        return ret;
    }

    /**
     *
     * @param requestContext Request context including request resourceType, security protocol and listener name
     * @param op             The ACL operation to check
     * @param resourceType   The resource type to check
     * @return               Return {@link AuthorizationResult#ALLOWED} if the caller is authorized
     *                       to perform the given ACL operation on at least one resource of the
     *                       given type. Return {@link AuthorizationResult#DENIED} otherwise.
     */
    @Override
    public AuthorizationResult authorizeByResourceType(AuthorizableRequestContext requestContext, AclOperation op, ResourceType resourceType) {
        AuthorizationResult ret = AuthorizationResult.DENIED;

        SecurityUtils.authorizeByResourceTypeCheckArgs(op, resourceType);

        if (logger.isDebugEnabled()) {
            logger.debug("==> RangerKafkaAuthorizer.authorizeByResourceType() requestContext: {} AclOperation : {} ResourceType : {}", requestContext, op, resourceType);
        }

        RangerKafkaCheckAccess rangerKafkaCheckAccess = new RangerKafkaCheckAccess();

        ret = rangerKafkaCheckAccess.authorizeByResourceType(requestContext, op, resourceType, rangerPlugin, auditHandler);

        if (logger.isDebugEnabled()) {
            logger.debug("<== RangerKafkaAuthorizer.authorizeByResourceType() requestContext: {} AclOperation : {} ResourceType : {} AuthorizationResult : {}", requestContext, op, resourceType, ret);
        }

        return ret;
    }

    private Set<String> parseSuperUsersFromKafkaConfig(Map<String, ?> configs) {
        if (configs == null) {
            return Collections.emptySet();
        }

        Object kafkaSuperUsersConfig = configs.get(KAFKA_SUPER_USERS_PROP);

        if (kafkaSuperUsersConfig == null) {
            return Collections.emptySet();
        }

        if (!(kafkaSuperUsersConfig instanceof String)) {
            logger.warn("super.users in Kafka config could not be parsed");

            return Collections.emptySet();
        }

        String      kafkaSuperUsers = (String) kafkaSuperUsersConfig;
        String[]    principals      = kafkaSuperUsers.split(";");
        Set<String> superUserNames  = new HashSet<>();

        for (String principal : principals) {
            try {
                KafkaPrincipal parsedPrincipal = SecurityUtils.parseKafkaPrincipal(principal.trim());
                String         userName        = parsedPrincipal.getName();

                if (KafkaPrincipal.USER_TYPE.equals(parsedPrincipal.getPrincipalType()) && StringUtils.isNotEmpty(userName)) {
                    superUserNames.add(userName);
                }
            } catch (Exception e) {
                logger.warn("Kafka principal: \"{}\" could not be parsed and will not be added to the authorized super users list", principal, e);
            }
        }

        return Collections.unmodifiableSet(superUserNames);
    }
}
