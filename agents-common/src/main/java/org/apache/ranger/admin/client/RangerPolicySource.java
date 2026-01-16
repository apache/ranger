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

import org.apache.commons.lang3.NotImplementedException;
import org.apache.ranger.plugin.model.RangerRole;
import org.apache.ranger.plugin.util.GrantRevokeRequest;
import org.apache.ranger.plugin.util.GrantRevokeRoleRequest;

import java.util.List;

// Implementations of this abstract class should implement methods that supply
// following details used by policy engine: policies, tags, roles, userstore, gds
// Other methods in RangerAdminClient throw NotImplementedException
public abstract class RangerPolicySource extends AbstractRangerAdminClient {
    public static final String SUFFIX_POLICIES_FILE  = ".json";
    public static final String SUFFIX_ROLES_FILE     = "_roles.json";
    public static final String SUFFIX_USERSTORE_FILE = "_userstore.json";
    public static final String SUFFIX_TAG_FILE       = "_tag.json";
    public static final String SUFFIX_GDS_FILE       = "_gds.json";

    @Override
    public RangerRole createRole(RangerRole request) throws Exception {
        throw new NotImplementedException();
    }

    @Override
    public void dropRole(String execUser, String roleName) throws Exception {
        throw new NotImplementedException();
    }

    @Override
    public List<String> getAllRoles(String execUser) throws Exception {
        throw new NotImplementedException();
    }

    @Override
    public List<String> getUserRoles(String execUser) throws Exception {
        throw new NotImplementedException();
    }

    @Override
    public RangerRole getRole(String execUser, String roleName) throws Exception {
        throw new NotImplementedException();
    }

    @Override
    public void grantRole(GrantRevokeRoleRequest request) throws Exception {
        throw new NotImplementedException();
    }

    @Override
    public void revokeRole(GrantRevokeRoleRequest request) throws Exception  {
        throw new NotImplementedException();
    }

    @Override
    public void grantAccess(GrantRevokeRequest request) throws Exception {
        throw new NotImplementedException();
    }

    @Override
    public void revokeAccess(GrantRevokeRequest request) throws Exception {
        throw new NotImplementedException();
    }

    @Override
    public List<String> getTagTypes(String tagTypePattern) throws Exception {
        throw new NotImplementedException();
    }
}
