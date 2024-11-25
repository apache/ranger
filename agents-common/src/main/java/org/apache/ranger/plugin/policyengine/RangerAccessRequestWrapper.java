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

package org.apache.ranger.plugin.policyengine;

import org.apache.commons.lang.StringUtils;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class RangerAccessRequestWrapper implements RangerAccessRequest {

    private final RangerAccessRequest request;
    private final String              accessType;
    private final boolean             isAccessTypeAny;
    private final boolean             isAccessTypeDelegatedAdmin;


    public RangerAccessRequestWrapper(RangerAccessRequest request, String accessType) {
        this.request                    = request;
        this.accessType                 = accessType;
        this.isAccessTypeAny            = StringUtils.equals(accessType, RangerPolicyEngine.ANY_ACCESS);
        this.isAccessTypeDelegatedAdmin = StringUtils.equals(accessType, RangerPolicyEngine.ADMIN_ACCESS);
    }

    @Override
    public RangerAccessResource getResource() { return request.getResource(); }

    @Override
    public String getAccessType() { return accessType; }

    @Override
    public boolean ignoreDescendantDeny() { return request.ignoreDescendantDeny(); }

    @Override
    public boolean isAccessTypeAny() { return isAccessTypeAny; }

    @Override
    public boolean isAccessTypeDelegatedAdmin() { return isAccessTypeDelegatedAdmin; }

    @Override
    public String getUser() { return request.getUser(); }

    @Override
    public Set<String> getUserGroups() { return request.getUserGroups(); }

    @Override
    public Set<String> getUserRoles() {return request.getUserRoles(); }

    @Override
    public Date getAccessTime() { return request.getAccessTime(); }

    @Override
    public String getClientIPAddress() { return request.getClientIPAddress(); }

    @Override
    public String getRemoteIPAddress() { return request.getRemoteIPAddress(); }

    @Override
    public List<String> getForwardedAddresses() { return request.getForwardedAddresses(); }

    @Override
    public String getClientType() { return request.getClientType(); }

    @Override
    public String getAction() { return request.getAction(); }

    @Override
    public String getRequestData() { return request.getRequestData(); }

    @Override
    public String getSessionId() { return request.getSessionId(); }

    @Override
    public String getClusterName() { return request.getClusterName(); }

    @Override
    public String getClusterType() { return request.getClusterType(); }

    @Override
    public Map<String, Object> getContext() { return request.getContext(); }

    @Override
    public RangerAccessRequest getReadOnlyCopy() { return request.getReadOnlyCopy(); }

    @Override
    public ResourceMatchingScope getResourceMatchingScope() { return request.getResourceMatchingScope(); }

    @Override
    public Map<String, ResourceElementMatchingScope> getResourceElementMatchingScopes() { return request.getResourceElementMatchingScopes(); }

}

