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

import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.ranger.plugin.policyengine.RangerAccessRequest;
import org.apache.ranger.plugin.policyengine.RangerResourceACLs;
import org.apache.ranger.plugin.service.RangerBasePlugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class RangerKafkaListAccess {
    private static final Logger logger = LoggerFactory.getLogger(RangerKafkaListAccess.class);
    RangerKafkaUtils rangerKafkaUtils = new RangerKafkaUtils();

    public RangerKafkaListAccess() {
    }

    public Iterable<AclBinding> getAclBindings(AclBindingFilter filter, RangerBasePlugin rangerPlugin) {
        if (logger.isDebugEnabled()) {
            logger.debug("==> RangerKafkaListAccess.getAclBindings() AclBindingFilter : {}", filter);
        }

        List<AclBinding> ret = new ArrayList<>();
        try {
            List<RangerAccessRequest> accessRequests = rangerKafkaUtils.createRangerRequests(filter);
            for (RangerAccessRequest accessRequest : accessRequests) {
                RangerResourceACLs rangerResourceACLs = rangerPlugin.getResourceACLs(accessRequest);
                ret.addAll(rangerKafkaUtils.getKafkaAclBindings(rangerResourceACLs, filter, RangerKafkaUtils.AclType.LIST_ACCES));
            }
        } catch (Exception e) {
            logger.error("Unable to fetch ACLs for {}", filter, e);
        }

        if (logger.isDebugEnabled()) {
            logger.debug("<== RangerKafkaListAccess.getAclBindings() AclBindingFilter : {}", ret);
        }

        return ret;
    }
}
