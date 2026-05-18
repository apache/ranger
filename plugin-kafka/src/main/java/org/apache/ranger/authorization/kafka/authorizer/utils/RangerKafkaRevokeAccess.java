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
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.security.AccessControlException;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.errors.ApiException;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.server.authorizer.AclDeleteResult;
import org.apache.kafka.server.authorizer.AuthorizableRequestContext;
import org.apache.ranger.audit.provider.MiscUtil;
import org.apache.ranger.authorization.kafka.authorizer.RangerKafkaAuditHandler;
import org.apache.ranger.plugin.service.RangerBasePlugin;
import org.apache.ranger.plugin.util.GrantRevokeRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class RangerKafkaRevokeAccess {
    private static final Logger logger = LoggerFactory.getLogger(RangerKafkaRevokeAccess.class);

    RangerKafkaUtils rangerKafkaUtils = new RangerKafkaUtils();

    public AclDeleteResult revoke(AclBindingFilter filter, RangerBasePlugin rangerPlugin, RangerKafkaAuditHandler auditHandler, AuthorizableRequestContext requestContext) throws Exception {
        if (logger.isDebugEnabled()) {
            logger.debug("==> RangerKafkaRevokeAccess.revoke(): AuthorizableRequestContext: {} aclBindingFilter : {}", requestContext, filter);
        }

        Collection<AclDeleteResult.AclBindingDeleteResult> ret = new ArrayList<>();
        Iterable<AclBinding> aclBindingsForUserOrGroup = rangerKafkaUtils.getAclBindingsForUserOrGroup(filter, rangerPlugin, RangerKafkaUtils.AclType.REVOKE_ACCESS);
        if (aclBindingsForUserOrGroup != null && !aclBindingsForUserOrGroup.iterator().hasNext()) {
            ret.add(new AclDeleteResult.AclBindingDeleteResult(rangerKafkaUtils.getAclBindingForFilter(filter)));
        } else {
            AclDeleteResult.AclBindingDeleteResult aclBindingDeleteResult = getAclDeleteResultForUserOrGroupAclBinding(aclBindingsForUserOrGroup, rangerPlugin, auditHandler, requestContext);
            ret.add(aclBindingDeleteResult);
        }

        if (logger.isDebugEnabled()) {
            logger.debug("<== RangerKafkaRevokeAccess.revoke(): AclDeleteResult.AclBindingDeleteResults: {}", ret);
        }

        return new AclDeleteResult(ret);
    }

    public AclDeleteResult.AclBindingDeleteResult getAclDeleteResultForUserOrGroupAclBinding(Iterable<AclBinding> aclBindings, RangerBasePlugin rangerPlugin, RangerKafkaAuditHandler auditHandler, AuthorizableRequestContext requestContext) {
        AclDeleteResult.AclBindingDeleteResult ret = null;
        GrantRevokeRequest grantRevokeRequest = null;
        AclBinding aclBinding = null;
        try {
            // Here aclBindings are for a single user/group hence fetch the first aclbinding
            aclBinding = getAclBindingForUserOrGroup(aclBindings);
            // Fetch all the operations to be revoked from the aclBindings
            Set<String> rangerOperations = getRangerOperationsFromAclBindings(aclBindings);
            if (CollectionUtils.isEmpty(rangerOperations)) {
                throw new IllegalArgumentException("Unsupported accessType in the request..");
            }
            grantRevokeRequest = createRevokeData(requestContext, aclBinding, rangerOperations);
            if (logger.isDebugEnabled()) {
                logger.debug("RevokeAccessRequest: {}", grantRevokeRequest);
            }
            if (rangerPlugin != null) {
                rangerPlugin.revokeAccess(grantRevokeRequest, auditHandler);
                ret = new AclDeleteResult.AclBindingDeleteResult(aclBinding);
            }
        } catch (AccessControlException excp) {
            logger.error("revoke() failed", excp);
            ret = new AclDeleteResult.AclBindingDeleteResult(aclBinding, new ApiException(excp));
        } catch (IOException excp) {
            logger.error("revoke() failed", excp);
            ret = new AclDeleteResult.AclBindingDeleteResult(aclBinding, new ApiException(excp));
        } catch (Exception excp) {
            logger.error("revoke() failed", excp);
            ret = new AclDeleteResult.AclBindingDeleteResult(aclBinding, new ApiException(excp));
        } finally {
            auditHandler.flushAudit();
        }
        return ret;
    }

    public GrantRevokeRequest createRevokeData(AuthorizableRequestContext requestContext, AclBinding aclBinding, Set<String> operations) throws Exception {
        GrantRevokeRequest ret = new GrantRevokeRequest();

        // Revoker Info
        String revoker = null;
        Set<String> revokerGroups = null;
        String principalType = requestContext.principal().getPrincipalType();
        if ("User".equals(principalType)) {
            revoker = requestContext.principal().getName();
            revokerGroups = MiscUtil.getGroupsForRequestUser(revoker);
        }

        // Client details
        String hostAddress     = requestContext.clientAddress() == null ? null : requestContext.clientAddress().getHostAddress();
        String clientIPAddress = StringUtils.isNotEmpty(hostAddress) && hostAddress.charAt(0) == '/' ? hostAddress.substring(1) : hostAddress;

        // Kafka Resource
        ResourcePattern resourcePattern = aclBinding.pattern();
        ResourceType resourceType = resourcePattern.resourceType();
        String resourceName = resourcePattern.name();
        PatternType patternType = resourcePattern.patternType();

        // Build resource
        String resourceTypeKey = RangerKafkaUtils.mapToResourceType(resourceType);
        Map<String, String> mapResource = new HashMap<>();
        switch (resourceType) {
            case TOPIC:
            case CLUSTER:
            case GROUP:
            case DELEGATION_TOKEN:
            case TRANSACTIONAL_ID:
                mapResource.put(resourceTypeKey, rangerKafkaUtils.getResourceValue(patternType, resourceName));
                break;
            case ANY:
            case UNKNOWN:
                // TODO: throw exception?
                break;
        }

        // Kafka Ranger Policy User / Group
        AccessControlEntry accessControlEntry = aclBinding.entry();
        String policyUser  = rangerKafkaUtils.getPolicyUser(accessControlEntry.principal());
        String policyGroup = rangerKafkaUtils.getPolicyGroup(accessControlEntry.principal());

        // Kafka Ranger Policy Permissions
        String operation = RangerKafkaUtils.mapToRangerAccessType(accessControlEntry.operation());
        if (operation == null) {
            throw new IllegalArgumentException("Unsupported accessType in the request..");
        }

        // Create GrantRevokeRequest
        ret.setGrantor(revoker);
        ret.setGrantorGroups(revokerGroups);
        ret.setDelegateAdmin(Boolean.TRUE); // remove delegateAdmin privilege as well
        ret.setEnableAudit(Boolean.TRUE);
        ret.setReplaceExistingPermissions(Boolean.TRUE);
        ret.setResource(mapResource);
        ret.setClientIPAddress(clientIPAddress);
        ret.setForwardedAddresses(null);
        ret.setRemoteIPAddress(clientIPAddress);

        StringBuilder sb = new StringBuilder("DeleteAcls:");
        sb.append(" ResourceType: ").append(resourceTypeKey);
        sb.append(" ResourceName: ").append(resourceName);
        sb.append(" Operation: ").append(operations);
        ret.setRequestData(sb.toString());

        Set<String> users = new HashSet<>();
        if (policyUser != null) {
            users.add(policyUser);
            ret.setUsers(users);
        }

        Set<String> groups = new HashSet<>();
        if (policyGroup != null) {
            groups.add(policyGroup);
            ret.setGroups(groups);
        }

        Set<String> accessTypes = new HashSet<>();
        accessTypes.addAll(operations);
        ret.setAccessTypes(accessTypes);

        ret.setDelegateAdmin(false);

        if (logger.isDebugEnabled()) {
            logger.debug("CREATED REVOKE Request: {}", ret);
        }

        return ret;
    }

    private AclBinding getAclBindingForUserOrGroup(Iterable<AclBinding> aclBindings) {
        AclBinding ret = null;
        if (aclBindings.iterator().hasNext()) {
            ret = aclBindings.iterator().next();
        }
        return ret;
    }

    private Set<String> getRangerOperationsFromAclBindings(Iterable<AclBinding> aclBindings) {
        Set<String> ret = new HashSet<>();
        for (AclBinding aclBinding : aclBindings) {
            AccessControlEntry accessControlEntry = aclBinding.entry();
            ret.add(RangerKafkaUtils.mapToRangerAccessType(accessControlEntry.operation()));
        }
        return ret;
    }
}
