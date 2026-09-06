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

package org.apache.ranger.authz.model;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
@JsonInclude(JsonInclude.Include.NON_EMPTY)
@JsonIgnoreProperties(ignoreUnknown = true)
public class RangerFilterResourcesRequest {
    private String                   requestId;
    private RangerUserInfo           user;
    private List<RangerResourceInfo> resources;
    private Set<String>              permissions;
    private String                   action;
    private RangerAccessContext      context;

    public RangerFilterResourcesRequest() {
    }

    public RangerFilterResourcesRequest(String userName, List<String> resources, String permission, String serviceType, String serviceName) {
        this(null, new RangerUserInfo(userName), toResourceInfo(resources), Collections.singleton(permission), permission, new RangerAccessContext(serviceType, serviceName));
    }

    public RangerFilterResourcesRequest(RangerUserInfo user, List<RangerResourceInfo> resources, Set<String> permissions, String action, RangerAccessContext context) {
        this(null, user, resources, permissions, action, context);
    }

    public RangerFilterResourcesRequest(String requestId, RangerUserInfo user, List<RangerResourceInfo> resources, Set<String> permissions, String action, RangerAccessContext context) {
        this.requestId   = requestId;
        this.user        = user;
        this.resources   = resources;
        this.permissions = permissions;
        this.action      = action;
        this.context     = context;
    }

    public String getRequestId() {
        return requestId;
    }

    public void setRequestId(String requestId) {
        this.requestId = requestId;
    }

    public RangerUserInfo getUser() {
        return user;
    }

    public void setUser(RangerUserInfo user) {
        this.user = user;
    }

    public List<RangerResourceInfo> getResources() {
        return resources;
    }

    public void setResources(List<RangerResourceInfo> resources) {
        this.resources = resources;
    }

    public Set<String> getPermissions() {
        return permissions;
    }

    public void setPermissions(Set<String> permissions) {
        this.permissions = permissions;
    }

    public String getAction() {
        return action;
    }

    public void setAction(String action) {
        this.action = action;
    }

    public RangerAccessContext getContext() {
        return context;
    }

    public void setContext(RangerAccessContext context) {
        this.context = context;
    }

    @Override
    public int hashCode() {
        return Objects.hash(requestId, user, resources, permissions, action, context);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        } else if (o == null || getClass() != o.getClass()) {
            return false;
        }

        RangerFilterResourcesRequest that = (RangerFilterResourcesRequest) o;

        return Objects.equals(requestId, that.requestId) &&
                Objects.equals(user, that.user) &&
                Objects.equals(resources, that.resources) &&
                Objects.equals(permissions, that.permissions) &&
                Objects.equals(action, that.action) &&
                Objects.equals(context, that.context);
    }

    @Override
    public String toString() {
        return "RangerFilterResourcesRequest{" +
                "requestId='" + requestId + '\'' +
                ", user=" + user +
                ", resources=" + resources +
                ", permissions=" + permissions +
                ", action=" + action +
                ", context=" + context +
                '}';
    }

    private static List<RangerResourceInfo> toResourceInfo(List<String> resources) {
        return resources == null ? null : resources.stream().map(RangerResourceInfo::new).collect(Collectors.toList());
    }
}
