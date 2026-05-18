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

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.security.AccessControlException;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.errors.ApiException;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.server.authorizer.AclCreateResult;
import org.apache.kafka.server.authorizer.AuthorizableRequestContext;
import org.apache.ranger.audit.provider.MiscUtil;
import org.apache.ranger.authorization.kafka.authorizer.RangerKafkaAuditHandler;
import org.apache.ranger.plugin.service.RangerBasePlugin;
import org.apache.ranger.plugin.util.GrantRevokeRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class RangerKafkaGrantAccess {
    private static final Logger logger = LoggerFactory.getLogger(RangerKafkaGrantAccess.class);

    RangerKafkaUtils rangerKafkaUtils = new RangerKafkaUtils();

    // Create GrantData for createACLs
    public AclCreateResult grant(AuthorizableRequestContext requestContext, AclBinding aclBinding, RangerBasePlugin rangerPlugin, RangerKafkaAuditHandler auditHandler) {
        AclCreateResult ret = null;
        GrantRevokeRequest grantRevokeRequest = null;

        if (logger.isDebugEnabled()) {
            logger.debug("==> RangerKafkaGrantAccess.grant() AuthorizableRequestContext : {} AclBinding : {}", requestContext, aclBinding);
        }

        try {
            grantRevokeRequest = createGrantData(requestContext, aclBinding);
            logger.info("GrantAccess: {}", grantRevokeRequest);
            if (rangerPlugin != null) {
                rangerPlugin.grantAccess(grantRevokeRequest, auditHandler);
                ret = AclCreateResult.SUCCESS;
            }
        } catch (AccessControlException excp) {
            logger.warn("grant() failed", excp);
            ret = new AclCreateResult(new ApiException(excp.getMessage()));
        } catch (IOException excp) {
            logger.warn("grant() failed", excp);
            ret = new AclCreateResult(new ApiException(excp.getMessage()));
        } catch (Exception excp) {
            logger.warn("grant() failed", excp);
            ret = new AclCreateResult(new ApiException(excp.getMessage()));
        } finally {
            auditHandler.flushAudit();
        }

        if (logger.isDebugEnabled()) {
            logger.debug("<== RangerKafkaGrantAccess.grant() AuthorizableRequestContext : {} AclBinding : {} AclCreateResult: {}", requestContext, aclBinding, ret);
        }

        return ret;
    }

    private GrantRevokeRequest createGrantData(AuthorizableRequestContext requestContext, AclBinding aclBinding) throws Exception {
        if (logger.isDebugEnabled()) {
            logger.debug("==> RangerKafkaGrantAccess.createGrantData()");
        }

        GrantRevokeRequest ret = new GrantRevokeRequest();

        // Grantor Info
        String grantor = null;
        Set<String> grantorGroups = null;
        String principalType = requestContext.principal().getPrincipalType();
        if ("User".equals(principalType)) {
            grantor = requestContext.principal().getName();
            grantorGroups = MiscUtil.getGroupsForRequestUser(grantor);
        }

        // Client details
        String hostAddress     = requestContext.clientAddress() == null ? null : requestContext.clientAddress().getHostAddress();
        String clientIPAddress = StringUtils.isNotEmpty(hostAddress) && hostAddress.charAt(0) == '/' ? hostAddress.substring(1) : hostAddress;

        // Kafka Resource
        ResourcePattern resourcePattern = aclBinding.pattern();
        ResourceType resourceType       = resourcePattern.resourceType();
        String resourceName             = resourcePattern.name();
        PatternType patternType         = resourcePattern.patternType();

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
        AccessControlEntry entry = aclBinding.entry();
        String policyUser  = rangerKafkaUtils.getPolicyUser(entry.principal());
        String policyGroup = rangerKafkaUtils.getPolicyGroup(entry.principal());

        // Kafka Ranger Policy Permissions
        AclOperation operation = entry.operation();

        // Create GrantRevokeRequest
        ret.setGrantor(grantor);
        ret.setGrantorGroups(grantorGroups);
        ret.setDelegateAdmin(Boolean.TRUE); // remove delegateAdmin privilege as well
        ret.setEnableAudit(Boolean.TRUE);
        ret.setReplaceExistingPermissions(Boolean.TRUE);
        ret.setResource(mapResource);
        ret.setClientIPAddress(clientIPAddress);
        ret.setForwardedAddresses(null); // TODO: Need to check with Knox proxy how they handle forwarded address.
        ret.setRemoteIPAddress(clientIPAddress);

        StringBuilder sb = new StringBuilder("CreateACL: ");
        sb.append("ResourceType: ").append(resourceTypeKey);
        sb.append("resourceName: ").append(resourceName);
        sb.append("operation: ").append(operation);
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

        String accessType = RangerKafkaUtils.mapToRangerAccessType(operation);
        if (accessType == null) {
            throw new IllegalArgumentException("Unsupported accessType in the request..");
        }

        Set<String> accessTypes = new HashSet<>();
        accessTypes.add(accessType);
        ret.setAccessTypes(accessTypes);

        if (logger.isDebugEnabled()) {
            logger.debug("<== RangerKafkaGrantAccess.createGrantData(): {}", ret);
        }

        return ret;
    }
}
