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

package org.apache.ranger.admin.client;

import org.apache.ranger.plugin.model.RangerRole;
import org.apache.ranger.plugin.util.*;

import java.util.List;

public abstract class AbstractRangerAdminClient implements RangerAdminClient {

    @Override
    public void init(String serviceName, String appId, String configPropertyPrefix) {

    }

    @Override
    public ServicePolicies getServicePoliciesIfUpdated(long lastKnownVersion, long lastActivationTimeInMillis) throws Exception {
        return null;
    }

    @Override
    public RangerRole createRole(RangerRole request) throws Exception {
        return null;
    }

    @Override
    public void dropRole(String execUser, String roleName) throws Exception {

    }

    @Override
    public List<String> getAllRoles(String execUser) throws Exception {
        return null;
    }

    @Override
    public List<String> getUserRoles(String execUser) throws Exception {
        return null;
    }

    @Override
    public RangerRole getRole(String execUser, String roleName) throws Exception {
        return null;
    }

    @Override
    public void grantRole(GrantRevokeRoleRequest request) throws Exception {

    }

    @Override
    public void revokeRole(GrantRevokeRoleRequest request) throws Exception {

    }

    @Override
    public void grantAccess(GrantRevokeRequest request) throws Exception {

    }

    @Override
    public void revokeAccess(GrantRevokeRequest request) throws Exception {

    }

    @Override
    public ServiceTags getServiceTagsIfUpdated(long lastKnownVersion, long lastActivationTimeInMillis) throws Exception {
        return null;
    }

    @Override
    public List<String> getTagTypes(String tagTypePattern) throws Exception {
        return null;
    }
}
