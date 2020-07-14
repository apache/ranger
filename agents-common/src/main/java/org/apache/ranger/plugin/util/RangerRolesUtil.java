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

package org.apache.ranger.plugin.util;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.ranger.plugin.model.RangerRole;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class RangerRolesUtil {
    private final long                     roleVersion;
    private final Map<String, Set<String>> userRoleMapping = new HashMap<>();
    private final Map<String, Set<String>> groupRoleMapping = new HashMap<>();

    public RangerRolesUtil(RangerRoles roles) {
        if (roles != null) {
            roleVersion = roles.getRoleVersion();

            if (CollectionUtils.isNotEmpty(roles.getRangerRoles())) {
                for (RangerRole role : roles.getRangerRoles()) {
                    Set<RangerRole> containedRoles = getAllContainedRoles(roles.getRangerRoles(), role);

                    buildMap(userRoleMapping, role, containedRoles, true);
                    buildMap(groupRoleMapping, role, containedRoles, false);
                }
            }
        } else {
            roleVersion = -1L;
        }
    }

    public long getRoleVersion() { return roleVersion; }

    public Map<String, Set<String>> getUserRoleMapping() {
        return this.userRoleMapping;
    }

    public Map<String, Set<String>> getGroupRoleMapping() {
        return this.groupRoleMapping;
    }

    private Set<RangerRole> getAllContainedRoles(Set<RangerRole> roles, RangerRole role) {
        Set<RangerRole> allRoles = new HashSet<>();

        allRoles.add(role);
        addContainedRoles(allRoles, roles, role);

        return allRoles;
    }

    private void addContainedRoles(Set<RangerRole> allRoles, Set<RangerRole> roles, RangerRole role) {
        List<RangerRole.RoleMember> roleMembers = role.getRoles();

        for (RangerRole.RoleMember roleMember : roleMembers) {
            RangerRole containedRole = getContainedRole(roles, roleMember.getName());

            if (containedRole!= null && !allRoles.contains(containedRole)) {
                allRoles.add(containedRole);
                addContainedRoles(allRoles, roles, containedRole);
            }
        }
    }

    private void buildMap(Map<String, Set<String>> map, RangerRole role, Set<RangerRole> containedRoles, boolean isUser) {
        buildMap(map, role, role.getName(), isUser);

        for (RangerRole containedRole : containedRoles) {
            buildMap(map, containedRole, role.getName(), isUser);
        }
    }

    private void buildMap(Map<String, Set<String>> map, RangerRole role, String roleName, boolean isUser) {
        for (RangerRole.RoleMember userOrGroup : isUser ? role.getUsers() : role.getGroups()) {
            if (StringUtils.isNotEmpty(userOrGroup.getName())) {
                Set<String> roleNames = map.get(userOrGroup.getName());

                if (roleNames == null) {
                    roleNames = new HashSet<>();

                    map.put(userOrGroup.getName(), roleNames);
                }

                roleNames.add(roleName);
            }
        }
    }

    private RangerRole getContainedRole(Set<RangerRole> roles, String role) {
        return (roles
                .stream()
                .filter(containedRole -> role.equals(containedRole.getName()))
                .findAny()
                .orElse(null));
    }
}


