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

package org.apache.ranger.authz.api;

import org.apache.commons.lang3.StringUtils;
import org.apache.ranger.authz.model.RangerAccessContext;
import org.apache.ranger.authz.model.RangerAccessInfo;
import org.apache.ranger.authz.model.RangerAuthzRequest;
import org.apache.ranger.authz.model.RangerAuthzResult;
import org.apache.ranger.authz.model.RangerMultiAuthzRequest;
import org.apache.ranger.authz.model.RangerMultiAuthzResult;
import org.apache.ranger.authz.model.RangerResourceInfo;
import org.apache.ranger.authz.model.RangerResourcePermissions;
import org.apache.ranger.authz.model.RangerUserInfo;

import java.util.HashMap;
import java.util.Properties;

import static org.apache.ranger.authz.api.RangerAuthzApiErrorCode.INVALID_REQUEST_ACCESS_CONTEXT_MISSING;
import static org.apache.ranger.authz.api.RangerAuthzApiErrorCode.INVALID_REQUEST_ACCESS_INFO_MISSING;
import static org.apache.ranger.authz.api.RangerAuthzApiErrorCode.INVALID_REQUEST_NAME_MATCH_SCOPE_INVALID;
import static org.apache.ranger.authz.api.RangerAuthzApiErrorCode.INVALID_REQUEST_PERMISSIONS_EMPTY;
import static org.apache.ranger.authz.api.RangerAuthzApiErrorCode.INVALID_REQUEST_RESOURCE_INFO_MISSING;
import static org.apache.ranger.authz.api.RangerAuthzApiErrorCode.INVALID_REQUEST_RESOURCE_NAME_MISSING;
import static org.apache.ranger.authz.api.RangerAuthzApiErrorCode.INVALID_REQUEST_USER_INFO_MISSING;

public abstract class RangerAuthorizer {
    protected final Properties properties;

    protected RangerAuthorizer(Properties properties) {
        this.properties = properties;
    }

    public abstract void init() throws RangerAuthzException;

    public abstract void close() throws RangerAuthzException;

    public abstract RangerAuthzResult authorize(RangerAuthzRequest request) throws RangerAuthzException;

    public abstract RangerMultiAuthzResult authorize(RangerMultiAuthzRequest request) throws RangerAuthzException;

    public abstract RangerResourcePermissions getResourcePermissions(RangerResourceInfo resource, RangerAccessContext context) throws RangerAuthzException;

    protected void validateRequest(RangerAuthzRequest request) throws RangerAuthzException {
        validateUserInfo(request.getUser());
        validateAccessInfo(request.getAccess());
        validateAccessContext(request.getContext());
    }

    protected void validateRequest(RangerMultiAuthzRequest request) throws RangerAuthzException {
        validateUserInfo(request.getUser());

        if (request.getAccesses() == null || request.getAccesses().isEmpty()) {
            throw new RangerAuthzException(INVALID_REQUEST_ACCESS_INFO_MISSING);
        }

        for (RangerAccessInfo access : request.getAccesses()) {
            validateAccessInfo(access);
        }

        validateAccessContext(request.getContext());
    }

    protected void validateUserInfo(RangerUserInfo user) throws RangerAuthzException {
        if (user == null || StringUtils.isBlank(user.getName())) {
            throw new RangerAuthzException(INVALID_REQUEST_USER_INFO_MISSING);
        }
    }

    protected void validateAccessInfo(RangerAccessInfo access) throws RangerAuthzException {
        if (access == null) {
            throw new RangerAuthzException(INVALID_REQUEST_ACCESS_INFO_MISSING);
        }

        validateResourceInfo(access.getResource());

        if (access.getPermissions() == null || access.getPermissions().isEmpty()) {
            throw new RangerAuthzException(INVALID_REQUEST_PERMISSIONS_EMPTY);
        }
    }

    protected void validateResourceInfo(RangerResourceInfo resource) throws RangerAuthzException {
        if (resource == null) {
            throw new RangerAuthzException(INVALID_REQUEST_RESOURCE_INFO_MISSING);
        }

        if (resource.getName() == null || resource.getName().isEmpty()) {
            throw new RangerAuthzException(INVALID_REQUEST_RESOURCE_NAME_MISSING);
        }

        if (resource.getSubResources() != null && !resource.getSubResources().isEmpty()) {
            if (resource.getNameMatchScope() != null && !RangerResourceInfo.ResourceMatchScope.SELF.equals(resource.getNameMatchScope())) {
                throw new RangerAuthzException(INVALID_REQUEST_NAME_MATCH_SCOPE_INVALID, resource.getNameMatchScope());
            }
        }
    }

    protected void validateAccessContext(RangerAccessContext context) throws RangerAuthzException {
        if (context == null) {
            throw new RangerAuthzException(INVALID_REQUEST_ACCESS_CONTEXT_MISSING);
        }

        if (context.getAccessTime() <= 0) {
            context.setAccessTime(System.currentTimeMillis());
        }

        if (context.getAdditionalInfo() == null) {
            context.setAdditionalInfo(new HashMap<>());
        }
    }
}
