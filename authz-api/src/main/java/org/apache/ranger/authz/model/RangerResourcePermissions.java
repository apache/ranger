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
import org.apache.ranger.authz.model.RangerAuthzResult.PermissionResult;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
@JsonInclude(JsonInclude.Include.NON_EMPTY)
@JsonIgnoreProperties(ignoreUnknown = true)
public class RangerResourcePermissions {
    private RangerResourceInfo                         resource;
    private Map<String, Map<String, PermissionResult>> users;
    private Map<String, Map<String, PermissionResult>> groups;
    private Map<String, Map<String, PermissionResult>> roles;

    public RangerResourcePermissions() {
    }

    public RangerResourcePermissions(RangerResourceInfo resource, Map<String, Map<String, PermissionResult>> users, Map<String, Map<String, PermissionResult>> groups, Map<String, Map<String, PermissionResult>> roles) {
        this.resource = resource;
        this.users    = users;
        this.groups   = groups;
        this.roles    = roles;
    }

    public RangerResourceInfo getResource() {
        return resource;
    }

    public void setResource(RangerResourceInfo resource) {
        this.resource = resource;
    }

    public Map<String, Map<String, PermissionResult>> getUsers() {
        return users;
    }

    public void setUsers(Map<String, Map<String, PermissionResult>> users) {
        this.users = users;
    }

    public Map<String, Map<String, PermissionResult>> getGroups() {
        return groups;
    }

    public void setGroups(Map<String, Map<String, PermissionResult>> groups) {
        this.groups = groups;
    }

    public Map<String, Map<String, PermissionResult>> getRoles() {
        return roles;
    }

    public void setRoles(Map<String, Map<String, PermissionResult>> roles) {
        this.roles = roles;
    }

    public Map<String, PermissionResult> getUserPermissions(String user) {
        return users == null ? null : users.get(user);
    }

    public void setUserPermissions(String user, Map<String, PermissionResult> permissions) {
        if (permissions == null) {
            if (users != null) {
                users.remove(user);
            }
        } else {
            if (users == null) {
                users = new HashMap<>();
            }

            users.put(user, permissions);
        }
    }

    public Map<String, PermissionResult> getGroupPermissions(String group) {
        return groups == null ? null : groups.get(group);
    }

    public void setGroupPermissions(String group, Map<String, PermissionResult> permissions) {
        if (permissions == null) {
            if (groups != null) {
                groups.remove(group);
            }
        } else {
            if (groups == null) {
                groups = new HashMap<>();
            }

            groups.put(group, permissions);
        }
    }

    public Map<String, PermissionResult> getRolePermissions(String role) {
        return roles == null ? null : roles.get(role);
    }

    public void setRolePermissions(String role, Map<String, PermissionResult> permissions) {
        if (permissions == null) {
            if (roles != null) {
                roles.remove(role);
            }
        } else {
            if (roles == null) {
                roles = new HashMap<>();
            }

            roles.put(role, permissions);
        }
    }

    public PermissionResult getUserPermission(String user, String permission) {
        Map<String, PermissionResult> userPermissions = getUserPermissions(user);

        return userPermissions == null ? null : userPermissions.get(permission);
    }

    public void setUserPermission(String user, String permission, PermissionResult result) {
        Map<String, PermissionResult> userPermissions = getUserPermissions(user);

        if (result == null) {
            if (userPermissions != null) {
                userPermissions.remove(permission);
            }
        } else {
            if (userPermissions == null) {
                userPermissions = new HashMap<>();

                userPermissions.put(permission, result);
                setUserPermissions(user, userPermissions);
            } else {
                userPermissions.put(permission, result);
            }
        }
    }

    public PermissionResult getGroupPermission(String group, String permission) {
        Map<String, PermissionResult> groupPermissions = getGroupPermissions(group);

        return groupPermissions == null ? null : groupPermissions.get(permission);
    }

    public void setGroupPermission(String group, String permission, PermissionResult result) {
        Map<String, PermissionResult> groupPermissions = getGroupPermissions(group);

        if (result == null) {
            if (groupPermissions != null) {
                groupPermissions.remove(permission);
            }
        } else {
            if (groupPermissions == null) {
                groupPermissions = new HashMap<>();

                groupPermissions.put(permission, result);
                setGroupPermissions(group, groupPermissions);
            } else {
                groupPermissions.put(permission, result);
            }
        }
    }

    public PermissionResult getRolePermission(String role, String permission) {
        Map<String, PermissionResult> rolePermissions = getRolePermissions(role);

        return rolePermissions == null ? null : rolePermissions.get(permission);
    }

    public void setRolePermission(String role, String permission, PermissionResult result) {
        Map<String, PermissionResult> rolePermissions = getRolePermissions(role);

        if (result == null) {
            if (rolePermissions != null) {
                rolePermissions.remove(permission);
            }
        } else {
            if (rolePermissions == null) {
                rolePermissions = new HashMap<>();

                rolePermissions.put(permission, result);
                setRolePermissions(role, rolePermissions);
            } else {
                rolePermissions.put(permission, result);
            }
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(resource, users, groups, roles);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        } else if (o == null || getClass() != o.getClass()) {
            return false;
        }

        RangerResourcePermissions that = (RangerResourcePermissions) o;

        return Objects.equals(resource, that.resource) &&
                Objects.equals(users, that.users) &&
                Objects.equals(groups, that.groups) &&
                Objects.equals(roles, that.roles);
    }

    @Override
    public String toString() {
        return "RangerResourcePermissions{" +
                "resource=" + resource +
                ", users=" + users +
                ", groups=" + groups +
                ", roles=" + roles +
                '}';
    }
}
