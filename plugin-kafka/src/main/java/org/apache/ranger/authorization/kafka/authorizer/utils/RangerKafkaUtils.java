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

package org.apache.ranger.authorization.kafka.authorizer.utils;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AccessControlEntryFilter;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourcePatternFilter;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.server.authorizer.Action;
import org.apache.kafka.server.authorizer.AuthorizableRequestContext;
import org.apache.kafka.server.authorizer.AuthorizationResult;
import org.apache.ranger.audit.provider.MiscUtil;
import org.apache.ranger.authorization.kafka.authorizer.RangerKafkaAuditHandler;
import org.apache.ranger.authorization.kafka.authorizer.RangerKafkaAuthorizer;
import org.apache.ranger.plugin.policyengine.RangerAccessRequest;
import org.apache.ranger.plugin.policyengine.RangerAccessRequestImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessResourceImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessResult;
import org.apache.ranger.plugin.policyengine.RangerPolicyEngine;
import org.apache.ranger.plugin.policyengine.RangerResourceACLs;
import org.apache.ranger.plugin.service.RangerBasePlugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class RangerKafkaUtils {
    private static final Logger logger = LoggerFactory.getLogger(RangerKafkaAuthorizer.class);

    public static final String ACCESS_TYPE_ALTER_CONFIGS = "alter_configs";
    public static final String KEY_TOPIC = "topic";
    public static final String KEY_CLUSTER = "cluster";
    public static final String KEY_CONSUMER_GROUP = "consumergroup";
    public static final String KEY_TRANSACTIONALID = "transactionalid";
    public static final String KEY_DELEGATIONTOKEN = "delegationtoken";
    public static final String ACCESS_TYPE_READ = "consume";
    public static final String ACCESS_TYPE_WRITE = "publish";
    public static final String ACCESS_TYPE_CREATE = "create";
    public static final String ACCESS_TYPE_ALTER = "alter";
    public static final String ACCESS_TYPE_DELETE = "delete";
    public static final String ACCESS_TYPE_CONFIGURE = "configure";
    public static final String ACCESS_TYPE_DESCRIBE = "describe";
    public static final String ACCESS_TYPE_DESCRIBE_CONFIGS = "describe_configs";
    public static final String ACCESS_TYPE_CLUSTER_ACTION = "cluster_action";
    public static final String ACCESS_TYPE_IDEMPOTENT_WRITE = "idempotent_write";
    public static final String ACCESS_TYPE_ADMIN = "_admin";
    public static final String ACCESS_TYPE_KAFKA_ADMIN = "kafka_admin";
    public static final String ACCESS_TYPE_ANY = "any";
    public static final String ACCESS_TYPE_UNKNOWN = "UNKNOWN";
    public static final String WILDCARD_ASTERIX = "*";

    public enum AclType { LIST_ACCES, CHECK_ACCESS, GRANT_ACCESS, REVOKE_ACCESS }

    public static String mapToRangerAccessType(AclOperation operation) {
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

    public static String mapToResourceType(ResourceType resourceType) {
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

    public Iterable<AclBinding> getAclBindingsForUserOrGroup(AclBindingFilter filter, RangerBasePlugin rangerPlugin, RangerKafkaUtils.AclType aclType) throws Exception {
        List<AclBinding> ret = new ArrayList<>();
        if (logger.isDebugEnabled()) {
            logger.debug("==> RangerKafkaListAccess.getAclBindingsForUserOrGroup() AclBindingFilter :{}", filter);
        }

        List<RangerAccessRequest> accessRequests = createRangerRequests(filter);
        for (RangerAccessRequest accessRequest : accessRequests) {
            RangerResourceACLs rangerResourceACLs = rangerPlugin.getResourceACLs(accessRequest);
            ret.addAll(getKafkaAclBindings(rangerResourceACLs, filter, aclType));
        }
        if (logger.isDebugEnabled()) {
            logger.debug("<== RangerKafkaListAccess.getAclBindingsForUserOrGroup() AclBindingFilter :{}", ret);
        }
        return ret;
    }

    public List<RangerAccessRequest> createRangerRequests(AclBindingFilter aclBindingFilter) throws Exception {
        if (logger.isDebugEnabled()) {
            logger.debug("==> RangerKafkaUtils.createRangerRequests() : aclBindingFilter {}", aclBindingFilter);
        }

        List<RangerAccessRequest> ret = new ArrayList<>();

        ResourcePatternFilter resourcePatternFilter = aclBindingFilter.patternFilter();
        ResourceType resourceType = getResourceType(resourcePatternFilter);
        PatternType patternType = getResourcePatternType(resourcePatternFilter);
        String resourceName = getResourceName(resourcePatternFilter);

        // Build Ranger resource
        Set<ResourceType> resourceTypes = new HashSet<>();
        if (RangerKafkaUtils.ACCESS_TYPE_ANY.equalsIgnoreCase(resourceType.name())) {
            resourceTypes = new HashSet<>(Arrays.asList(ResourceType.values()));
        } else if (!RangerKafkaUtils.ACCESS_TYPE_UNKNOWN.equalsIgnoreCase(resourceType.name())) {
            resourceTypes.add(resourceType);
        }

        // Build request for each of the permission for the user
        for (ResourceType resType : resourceTypes) {
            String resourceTypeKey;
            if (ResourceType.UNKNOWN.equals(resType) || ResourceType.ANY.equals(resType)) {
                throw new Exception("Unsupported ResoureType " + resType + " in the request..");
            }

            Map<String, String> mapResource = new HashMap<>();
            resourceTypeKey = RangerKafkaUtils.mapToResourceType(resType);
            mapResource.put(resourceTypeKey, getResourceValue(patternType, resourceName));

            AccessControlEntryFilter accessControlEntryFilter = aclBindingFilter.entryFilter();
            AclOperation aclOperation = accessControlEntryFilter.operation();
            String accessType = RangerKafkaUtils.mapToRangerAccessType(aclOperation);
            if (StringUtils.isBlank(accessType)) {
                accessType = "_any";
            }

            RangerAccessResourceImpl kafkaResource = createRangerAccessResource(resourceTypeKey, resourceName);
            String user = getPolicyUser(accessControlEntryFilter.principal());
            String group = getPolicyGroup(accessControlEntryFilter.principal());
            String ip = accessControlEntryFilter.host();
            RangerAccessRequestImpl request = new RangerAccessRequestImpl();
            request.setResource(kafkaResource);
            request.setAccessType(accessType);
            request.setUser(user);
            request.setUserGroups((group != null) ? new HashSet<>(Arrays.asList(group)) : null);
            request.setAccessTime(new Date());
            request.setClientIPAddress(ip);
            ret.add(request);
        }

        if (logger.isDebugEnabled()) {
            logger.debug("<==  RangerKafkaUtils.createRangerRequests() {}", ret);
        }

        return ret;
    }

    public RangerAccessResourceImpl createRangerAccessResource(String resourceTypeKey, String resourceName) {
        RangerAccessResourceImpl rangerResource = new RangerAccessResourceImpl();
        rangerResource.setValue(resourceTypeKey, resourceName);
        return rangerResource;
    }

    public RangerAccessRequestImpl createRangerAccessRequest(String userName,
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

    public static List<AuthorizationResult> mapResults(List<Action> actions, Collection<RangerAccessResult> results) {
        if (CollectionUtils.isEmpty(results)) {
            logger.error("Ranger Plugin returned null or empty. Returning Denied for all");
            return denyAll(actions);
        }
        return results.stream()
                .map(r -> r != null && r.getIsAllowed() ? AuthorizationResult.ALLOWED : AuthorizationResult.DENIED)
                .collect(Collectors.toList());
    }

    public static List<AuthorizationResult> denyAll(List<Action> actions) {
        return actions.stream().map(a -> AuthorizationResult.DENIED).collect(Collectors.toList());
    }

    public static String toString(AuthorizableRequestContext requestContext) {
        return requestContext == null ? null :
                String.format("AuthorizableRequestContext{principal=%s, clientAddress=%s, clientId=%s}",
                        requestContext.principal(), requestContext.clientAddress(), requestContext.clientId());
    }

    public String getPolicyUser(String principal) {
        String ret = null;
        if (logger.isDebugEnabled()) {
            logger.debug("getPolicyUser(): Principal : {}", principal);
        }

        if (principal != null) {
            if (principal.contains(":")) {
                String[] userData = principal.split(":");
                if (ArrayUtils.isNotEmpty(userData)) {
                    if ("User".equalsIgnoreCase(userData[0])) {
                        ret = userData[1];
                    }
                }
            } else if (principal.equals("<any>")) {
                // Access check is for any user so returning public so group can be public
                ret = "any";
            }
        } else {
            return StringUtils.EMPTY;
        }
        return ret;
    }

    protected String getPolicyGroup(String principal) {
        String ret = null;
        if (logger.isDebugEnabled()) {
            logger.debug("getPolicyGroup(): Principal : {}", principal);
        }
        logger.info("#### getPolicyGroup(): Principal : {}", principal);

        if (principal != null && principal.contains(":")) {
            String[] userData = principal.split(":");
            if (ArrayUtils.isNotEmpty(userData)) {
                if ("Group".equalsIgnoreCase(userData[0])) {
                    ret = userData[1];
                }
            }
        }
        return ret;
    }

    protected Set<String> getUserGroups(String userName) throws Exception {
        return MiscUtil.getGroupsForRequestUser(userName);
    }

    protected String getAllowedHost(String host) {
        String ret = host;
        if ("*".equals(host)) {
            ret = "";
        }
        return ret;
    }

    protected String getResourceValue(PatternType patternType, String resource) {
        String ret = resource;
        switch (patternType) {
            case LITERAL:
                ret = resource;
                break;
            case PREFIXED:
                ret = resource + "*";
                break;
            case ANY:
            case MATCH:
                ret = resource + "*";
                break;
        }
        return ret;
    }

    public List<AuthorizationResult> wrappedAuthorization(AuthorizableRequestContext requestContext, List<Action> actions, RangerBasePlugin rangerPlugin, RangerKafkaAuditHandler auditHandler) {
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
            String accessType = RangerKafkaUtils.mapToRangerAccessType(action.operation());
            if (accessType == null) {
                MiscUtil.logErrorMessageByInterval(logger, "Unsupported access type, requestContext=" + RangerKafkaUtils.toString(requestContext) +
                        ", actions=" + actions + ", operation=" + action.operation());
                return RangerKafkaUtils.denyAll(actions);
            }
            String resourceTypeKey = RangerKafkaUtils.mapToResourceType(action.resourcePattern().resourceType());
            if (resourceTypeKey == null) {
                MiscUtil.logErrorMessageByInterval(logger, "Unsupported resource type, requestContext=" + RangerKafkaUtils.toString(requestContext) +
                        ", actions=" + actions + ", resourceType=" + action.resourcePattern().resourceType());
                return RangerKafkaUtils.denyAll(actions);
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

        Collection<RangerAccessResult> results = callRangerPlugin(rangerRequests, rangerPlugin, auditHandler);

        List<AuthorizationResult> authorizationResults = RangerKafkaUtils.mapResults(actions, results);

        if (logger.isDebugEnabled()) {
            logger.debug("rangerRequests={}, return={}", rangerRequests, authorizationResults);
        }
        return authorizationResults;
    }

    private Collection<RangerAccessResult> callRangerPlugin(List<RangerAccessRequest> rangerRequests, RangerBasePlugin rangerPlugin, RangerKafkaAuditHandler auditHandler) {
        try {
            return rangerPlugin.isAccessAllowed(rangerRequests);
        } catch (Throwable t) {
            logger.error("Error while calling isAccessAllowed(). requests={}", rangerRequests, t);
            return null;
        } finally {
            auditHandler.flushAudit();
        }
    }

    public List<AclBinding> getKafkaAclBindings(RangerResourceACLs rangerResourceACLs, AclBindingFilter filter, RangerKafkaUtils.AclType aclType) {
        List<AclBinding> ret = new ArrayList<>();
        if (rangerResourceACLs != null) {
            Map<String, Map<String, RangerResourceACLs.AccessResult>> userRangerACLs = rangerResourceACLs.getUserACLs();
            if (logger.isDebugEnabled()) {
                logger.debug("RangerKafkaUtils.getKafkaAclBindings() userRangerAcls: {}", userRangerACLs);
            }
            List<AclBinding> userAclBindings = getAclBindingsForUserAcls(userRangerACLs, filter, aclType);
            if (CollectionUtils.isNotEmpty(userAclBindings)) {
                ret.addAll(userAclBindings);
            }
            Map<String, Map<String, RangerResourceACLs.AccessResult>> groupRangerACLs = rangerResourceACLs.getGroupACLs();
            if (logger.isDebugEnabled()) {
                logger.debug("RangerKafkaUtils.getKafkaAclBindings() groupRangerAcls: {}", groupRangerACLs);
            }
            List<AclBinding> groupAclBindins = getAclBindingsForGroupAcls(groupRangerACLs, filter, aclType);
            if (CollectionUtils.isNotEmpty(groupAclBindins)) {
                ret.addAll(groupAclBindins);
            }
        }
        return ret;
    }

    public List<AclBinding> getAclBindingsForUserAcls(Map<String, Map<String, RangerResourceACLs.AccessResult>> userRangerACLs, AclBindingFilter filter, RangerKafkaUtils.AclType aclType) {
        List<AclBinding> ret = new ArrayList<>();
        String user = getPolicyUser(filter.entryFilter().principal());
        if (StringUtils.isNotBlank(user) && MapUtils.isNotEmpty(userRangerACLs)) {
            for (Map.Entry<String, Map<String, RangerResourceACLs.AccessResult>> entry : userRangerACLs.entrySet()) {
                String userName = entry.getKey();
                if (StringUtils.isNotBlank(user)) {
                    if (!"any".equals(user)
                            && !user.equals(userName) && !"*".equals(userName)) {
                        continue;
                    }
                }
                for (Map.Entry<String, RangerResourceACLs.AccessResult> privilege : entry.getValue().entrySet()) {
                    if (StringUtils.equals(RangerPolicyEngine.ADMIN_ACCESS, privilege.getKey())) {
                        continue;
                    }
                    ResourcePattern resourcePattern = getKafkaResourcePattern(filter);
                    AccessControlEntry accessControlEntry = getKafkaAccessControlEntry("USER:" + userName, filter.entryFilter().host(), privilege);
                    ret.add(new AclBinding(resourcePattern, accessControlEntry));
                }
            }
        } else if (StringUtils.isBlank(user)
                && MapUtils.isNotEmpty(userRangerACLs)
                && aclType.equals(RangerKafkaUtils.AclType.LIST_ACCES)) {
            for (Map.Entry<String, Map<String, RangerResourceACLs.AccessResult>> entry : userRangerACLs.entrySet()) {
                String userName = entry.getKey();
                for (Map.Entry<String, RangerResourceACLs.AccessResult> privilege : entry.getValue().entrySet()) {
                    if (StringUtils.equals(RangerPolicyEngine.ADMIN_ACCESS, privilege.getKey())) {
                        continue;
                    }
                    ResourcePattern resourcePattern = getKafkaResourcePattern(filter);
                    AccessControlEntry accessControlEntry = getKafkaAccessControlEntry("USER:" + userName, filter.entryFilter().host(), privilege);
                    ret.add(new AclBinding(resourcePattern, accessControlEntry));
                }
            }
        }
        return ret;
    }

    public List<AclBinding> getAclBindingsForGroupAcls(Map<String, Map<String, RangerResourceACLs.AccessResult>> groupRangerACLs, AclBindingFilter filter, RangerKafkaUtils.AclType aclType) {
        List<AclBinding> ret = new ArrayList<>();
        String filterGroup = getPolicyGroup(filter.entryFilter().principal());
        logger.info("####GROUP: {}", filterGroup);
        if (StringUtils.isNotBlank(filterGroup) && MapUtils.isNotEmpty(groupRangerACLs)) {
            for (Map.Entry<String, Map<String, RangerResourceACLs.AccessResult>> entry : groupRangerACLs.entrySet()) {
                String groupName = entry.getKey();
                if (StringUtils.isNotBlank(filterGroup) && !"public".equals(groupName)
                        && !filterGroup.equals(groupName) && !"*".equals(groupName)) {
                    continue;
                }
                for (Map.Entry<String, RangerResourceACLs.AccessResult> privilege : entry.getValue().entrySet()) {
                    if (StringUtils.equals(RangerPolicyEngine.ADMIN_ACCESS, privilege.getKey())) {
                        continue;
                    }
                    ResourcePattern resourcePattern = getKafkaResourcePattern(filter);
                    AccessControlEntry accessControlEntry = getKafkaAccessControlEntry("GROUP:" + groupName, filter.entryFilter().host(), privilege);
                    ret.add(new AclBinding(resourcePattern, accessControlEntry));
                }
            }
        } else if (StringUtils.isBlank(filterGroup)
                && MapUtils.isNotEmpty(groupRangerACLs)
                && aclType.equals(RangerKafkaUtils.AclType.LIST_ACCES)) {
            for (Map.Entry<String, Map<String, RangerResourceACLs.AccessResult>> entry : groupRangerACLs.entrySet()) {
                String groupName = entry.getKey();
                for (Map.Entry<String, RangerResourceACLs.AccessResult> privilege : entry.getValue().entrySet()) {
                    if (StringUtils.equals(RangerPolicyEngine.ADMIN_ACCESS, privilege.getKey())) {
                        continue;
                    }
                    ResourcePattern resourcePattern = getKafkaResourcePattern(filter);
                    AccessControlEntry accessControlEntry = getKafkaAccessControlEntry("GROUP:" + groupName, filter.entryFilter().host(), privilege);
                    ret.add(new AclBinding(resourcePattern, accessControlEntry));
                }
            }
        }
        return ret;
    }

    public ResourcePattern getKafkaResourcePattern(AclBindingFilter filter) {
        ResourcePatternFilter resourcePatternFilter = filter.patternFilter();
        ResourceType resourceType = getResourceType(resourcePatternFilter);
        String resourceName = getResourceName(resourcePatternFilter);
        PatternType resourcePatternType = getResourcePatternType(resourcePatternFilter);
        return new ResourcePattern(resourceType, resourceName, resourcePatternType);
    }

    public AccessControlEntry getKafkaAccessControlEntry(String userOrGroupName, String hostIP, Map.Entry<String, RangerResourceACLs.AccessResult> privilege) {
        String principal = userOrGroupName;
        String host = (hostIP != null) ? hostIP : ACCESS_TYPE_ANY;
        AclOperation aclOperation = getKafkAclOperation(privilege.getKey());
        AclPermissionType aclPermissionType = getKafkaAclPermissionType(privilege.getValue());
        return new AccessControlEntry(principal, host, aclOperation, aclPermissionType);
    }

    public ResourceType getResourceType(ResourcePatternFilter resourcePatternFilter) {
        return resourcePatternFilter.resourceType();
    }

    public String getResourceName(ResourcePatternFilter resourcePatternFilter) {
        return resourcePatternFilter.name();
    }

    public PatternType getResourcePatternType(ResourcePatternFilter resourcePatternFilter) {
        return resourcePatternFilter.patternType();
    }

    public AclOperation getKafkAclOperation(String rangerOperation) {
        AclOperation ret = null;
        switch (rangerOperation) {
            case ACCESS_TYPE_READ:
                ret = AclOperation.READ;
                break;
            case ACCESS_TYPE_WRITE:
                ret = AclOperation.WRITE;
                break;
            case ACCESS_TYPE_CONFIGURE:
                ret = AclOperation.DESCRIBE_CONFIGS;
                break;
            case ACCESS_TYPE_DESCRIBE:
                ret = AclOperation.DESCRIBE;
                break;
            case ACCESS_TYPE_CLUSTER_ACTION:
                ret = AclOperation.CLUSTER_ACTION;
                break;
            case ACCESS_TYPE_CREATE:
                ret = AclOperation.CREATE;
                break;
            case ACCESS_TYPE_ALTER:
                ret = AclOperation.ALTER;
                break;
            case ACCESS_TYPE_DELETE:
                ret = AclOperation.DELETE;
                break;
            case ACCESS_TYPE_DESCRIBE_CONFIGS:
                ret = AclOperation.DESCRIBE_CONFIGS;
                break;
            case ACCESS_TYPE_ALTER_CONFIGS:
                ret = AclOperation.ALTER_CONFIGS;
                break;
            case ACCESS_TYPE_IDEMPOTENT_WRITE:
                ret = AclOperation.IDEMPOTENT_WRITE;
                break;
            case ACCESS_TYPE_ADMIN:
            case ACCESS_TYPE_KAFKA_ADMIN:
                ret = AclOperation.ALL;
                break;
        }
        return ret;
    }

    public AclPermissionType getKafkaAclPermissionType(RangerResourceACLs.AccessResult accessResult) {
        return (accessResult.getResult() == 1) ? AclPermissionType.ALLOW : AclPermissionType.DENY;
    }

    public AclBinding getAclBindingForFilter(AclBindingFilter filter) {
        ResourcePattern resourcePattern = getKafkaResourcePattern(filter);
        AccessControlEntryFilter accessControlEntryFilter = filter.entryFilter();
        AccessControlEntry accessControlEntry = new AccessControlEntry(accessControlEntryFilter.principal(), accessControlEntryFilter.host(), accessControlEntryFilter.operation(), accessControlEntryFilter.permissionType());
        return new AclBinding(resourcePattern, accessControlEntry);
    }
}
