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
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.server.authorizer.AuthorizableRequestContext;
import org.apache.kafka.server.authorizer.AuthorizationResult;
import org.apache.ranger.authorization.kafka.authorizer.RangerKafkaAuditHandler;
import org.apache.ranger.authorization.kafka.authorizer.RangerKafkaAuthorizer;
import org.apache.ranger.plugin.policyengine.RangerAccessRequest;
import org.apache.ranger.plugin.policyengine.RangerAccessRequestImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessResult;
import org.apache.ranger.plugin.service.RangerBasePlugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class RangerKafkaCheckAccess {
    private static final Logger logger = LoggerFactory.getLogger(RangerKafkaAuthorizer.class);

    RangerKafkaUtils rangerKafkaUtils = new RangerKafkaUtils();

    public AuthorizationResult authorizeByResourceType(AuthorizableRequestContext requestContext, AclOperation op, ResourceType resourceType, RangerBasePlugin rangerPlugin, RangerKafkaAuditHandler auditHandler) {
        AuthorizationResult ret = null;

        if (logger.isDebugEnabled()) {
            logger.debug("==> RangerKafkaCheckAccess.authorizeByResourceType() AuthorizableRequestContext : {} AclOperation : {} ResourceType : {}", requestContext, op, resourceType);
        }

        String principal        = requestContext.principal().getName();
        String userName         = rangerKafkaUtils.getPolicyUser(principal);
        String group            = rangerKafkaUtils.getPolicyGroup(principal);
        Set<String> userGroups  = (group != null) ? new HashSet<>(Arrays.asList(group)) : null;
        String resourceTypeKey  = RangerKafkaUtils.mapToResourceType(resourceType);
        String accessType       = RangerKafkaUtils.mapToRangerAccessType(op);
        String resourceName     = StringUtils.EMPTY; // Set Resource to Empty as the access is for any resource of give request type
        String hostAddress      = requestContext.clientAddress() == null ? null : requestContext.clientAddress().getHostAddress();
        String clientIPAddress  = StringUtils.isNotEmpty(hostAddress) && hostAddress.charAt(0) == '/' ? hostAddress.substring(1) : hostAddress;

        RangerAccessRequestImpl rangerAccessRequest = rangerKafkaUtils.createRangerAccessRequest(
                userName,
                userGroups,
                clientIPAddress,
                new Date(),
                resourceTypeKey,
                resourceName,
                accessType);

        // Resource Matching Scope is set to SELF_OR_PREFIX for this uses case
        Map<String, RangerAccessRequest.ResourceElementMatchingScope> resourceElementMatchingScopes = rangerAccessRequest.getResourceElementMatchingScopes();
        resourceElementMatchingScopes.put(resourceTypeKey, RangerAccessRequest.ResourceElementMatchingScope.SELF_OR_PREFIX);

        RangerAccessResult result;
        try {
            result = rangerPlugin.isAccessAllowed(rangerAccessRequest);
        } catch (Throwable t) {
            logger.error("Error while calling isAccessAllowed(). requests=" + rangerAccessRequest, t);
            return AuthorizationResult.DENIED;
        } finally {
            auditHandler.flushAudit();
        }

        if (result.getIsAllowed()) {
            ret = AuthorizationResult.ALLOWED;
        }

        if (logger.isDebugEnabled()) {
            logger.debug("<== RangerKafkaCheckAccess.authorizeByResourceType() AuthorizableRequestContext : {} AclOperation : {} ResourceType : {} isAccessAllowed: {}", requestContext, op, resourceType, ret);
        }

        return ret;
    }
}
