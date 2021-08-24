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

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.common.Endpoint;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.security.JaasContext;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.server.authorizer.AclCreateResult;
import org.apache.kafka.server.authorizer.AclDeleteResult;
import org.apache.kafka.server.authorizer.Action;
import org.apache.kafka.server.authorizer.AuthorizableRequestContext;
import org.apache.kafka.server.authorizer.AuthorizationResult;
import org.apache.kafka.server.authorizer.Authorizer;
import org.apache.kafka.server.authorizer.AuthorizerServerInfo;
import org.apache.ranger.audit.provider.MiscUtil;
import org.apache.ranger.plugin.policyengine.RangerAccessRequestImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessResourceImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessResult;
import org.apache.ranger.plugin.service.RangerBasePlugin;
import org.apache.ranger.plugin.util.RangerPerfTracer;

import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;

public class RangerKafkaAuthorizer implements Authorizer {
    private static final Log logger = LogFactory.getLog(RangerKafkaAuthorizer.class);
    private static final Log PERF_KAFKAAUTH_REQUEST_LOG = RangerPerfTracer.getPerfLogger("kafkaauth.request");
    private static volatile RangerBasePlugin rangerPlugin = null;

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
    public static final String ACCESS_TYPE_ALTER_CONFIGS = "alter_configs";
    public static final String ACCESS_TYPE_IDEMPOTENT_WRITE = "idempotent_write";
    public static final String ACCESS_TYPE_CLUSTER_ACTION = "cluster_action";

    private RangerKafkaAuditHandler auditHandler = null;

    public RangerKafkaAuthorizer() {
    }

    /*
     * (non-Javadoc)
     *
     * @see kafka.security.auth.Authorizer#configure(Map<String, Object>)
     */
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
                        logger.info("LoginUser=" + MiscUtil.getUGILoginUser());
                    } catch (Throwable t) {
                        logger.error("Error getting principal.", t);
                    }
                    rangerPlugin = new RangerBasePlugin("kafka", "kafka");
                }
            }
        }
        logger.info("Calling plugin.init()");
        rangerPlugin.init();
        auditHandler = new RangerKafkaAuditHandler();
        rangerPlugin.setResultProcessor(auditHandler);
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
    public Map<Endpoint, ? extends CompletionStage<Void>> start(AuthorizerServerInfo serverInfo) {
        return Collections.emptyMap();
    }

    @Override
    public List<AuthorizationResult> authorize(AuthorizableRequestContext requestContext, List<Action> actions) {
        if (rangerPlugin == null) {
            MiscUtil.logErrorMessageByInterval(logger,
                    "Authorizer is still not initialized");
            return actions.stream().map(a -> AuthorizationResult.DENIED).collect(Collectors.toList());
        }

        String userName = requestContext.principal() == null ? null : requestContext.principal().getName();
        Set<String> userGroups = MiscUtil.getGroupsForRequestUser(userName);
        String hostAddress = requestContext.clientAddress().getHostAddress();
        String ip = StringUtils.isNotEmpty(hostAddress) && hostAddress.charAt(0) == '/' ? hostAddress.substring(1) : hostAddress;

        return actions.stream().map(action -> {
            RangerPerfTracer perf = perfTracer(action.resourcePattern());
            Date eventTime = new Date();
            String accessType = mapToRangerAccessType(action.operation());
            boolean validationFailed = false;
            String validationStr = "";

            if (StringUtils.isEmpty(accessType)) {
                if (MiscUtil.logErrorMessageByInterval(logger,
                        "Unsupported access type. operation=" + action.operation())) {
                    logger.fatal("Unsupported access type. principal=" + requestContext.principal()
                            + ", clientAddress=" + requestContext.clientAddress()
                            + ", operation=" + action.operation() + ", resource=" + action.resourcePattern());
                }
                validationFailed = true;
                validationStr += "Unsupported access type. operation=" + action.operation();
            }

            RangerAccessRequestImpl rangerRequest = new RangerAccessRequestImpl();
            rangerRequest.setUser(userName);
            rangerRequest.setUserGroups(userGroups);
            rangerRequest.setClientIPAddress(ip);
            rangerRequest.setAccessTime(eventTime);

            RangerAccessResourceImpl rangerResource = new RangerAccessResourceImpl();
            rangerRequest.setResource(rangerResource);
            rangerRequest.setAccessType(accessType);
            rangerRequest.setAction(accessType);
            rangerRequest.setRequestData(action.resourcePattern().name());

            String resourceTypeString = mapToResourceType(action.resourcePattern().resourceType());
            if (StringUtils.isEmpty(resourceTypeString)) {
                logger.fatal("Unsupported resourceType=" + action.resourcePattern().resourceType());
                validationFailed = true;
            } else {
                rangerResource.setValue(resourceTypeString, action.resourcePattern().name());
            }

            boolean returnValue = false;
            if (validationFailed) {
                MiscUtil.logErrorMessageByInterval(logger, validationStr + ", request=" + rangerRequest);
            } else {
                try {
                    RangerAccessResult result = rangerPlugin.isAccessAllowed(rangerRequest);
                    if (result == null) {
                        logger.error("Ranger Plugin returned null. Returning false");
                    } else {
                        returnValue = result.getIsAllowed();
                    }
                } catch (Throwable t) {
                    logger.error("Error while calling isAccessAllowed(). request=" + rangerRequest, t);
                } finally {
                    auditHandler.flushAudit();
                }
            }
            RangerPerfTracer.log(perf);

            if (logger.isDebugEnabled()) {
                logger.debug("rangerRequest=" + rangerRequest + ", return=" + returnValue);
            }
            return returnValue ? AuthorizationResult.ALLOWED : AuthorizationResult.DENIED;
        }).collect(Collectors.toList());
    }

    @Override
    public List<? extends CompletionStage<AclCreateResult>> createAcls(AuthorizableRequestContext requestContext, List<AclBinding> aclBindings) {
        logger.error("createAcls(AuthorizableRequestContext, List<AclBinding>) is not supported by Ranger for Kafka");
        return Collections.emptyList();
    }

    @Override
    public List<? extends CompletionStage<AclDeleteResult>> deleteAcls(AuthorizableRequestContext requestContext, List<AclBindingFilter> aclBindingFilters) {
        logger.error("deleteAcls(AuthorizableRequestContext, List<AclBindingFilter>) is not supported by Ranger for Kafka");
        return Collections.emptyList();
    }

    @Override
    public Iterable<AclBinding> acls(AclBindingFilter filter) {
        logger.error("acls(AclBindingFilter) is not supported by Ranger for Kafka");
        return Collections.emptyList();
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
            default:
                return null;
        }
    }

    private static RangerPerfTracer perfTracer(ResourcePattern resourcePattern) {
        if (RangerPerfTracer.isPerfTraceEnabled(PERF_KAFKAAUTH_REQUEST_LOG)) {
            return RangerPerfTracer.getPerfTracer(PERF_KAFKAAUTH_REQUEST_LOG,
                    "RangerKafkaAuthorizer.authorize(resource=" + resourcePattern + ")");
        } else {
            return null;
        }
    }
}
