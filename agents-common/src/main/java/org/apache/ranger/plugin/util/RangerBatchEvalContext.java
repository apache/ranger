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

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Not thread-safe. One instance per evaluation call; do not mutate a groups set during that call, because the hot key matches the same set instance.
 */
public class RangerBatchEvalContext {
    private final Map<String, String> userNameMappings = new HashMap<>();
    private final Map<IdentityKey, Set<String>> userGroupsMappings = new HashMap<>();
    private final Map<IdentityKey, Set<String>> userRolesMappings = new HashMap<>();

    private IdentityKey hotKey;
    private String hotUser;
    private Set<String> hotGroupsRef;

    public boolean hasMappingForUserName(String user) {
        return userNameMappings.containsKey(user);
    }

    public String getMappedUserName(String user) {
        return userNameMappings.get(user);
    }

    public void setUserNameMapping(String user, String normalizedUser) {
        userNameMappings.put(user, normalizedUser);
    }

    public boolean hasMappingForUserGroups(String user, Set<String> groups) {
        return userGroupsMappings.containsKey(keyFor(user, groups));
    }

    public Set<String> getMappedUserGroups(String user, Set<String> groups) {
        return userGroupsMappings.get(keyFor(user, groups));
    }

    public void setUserGroupsMapping(String user, Set<String> groups, Set<String> normalizedGroups) {
        userGroupsMappings.put(storedKey(user, groups), normalizedGroups);
    }

    public boolean hasMappingForUserRoles(String user, Set<String> groups) {
        return userRolesMappings.containsKey(keyFor(user, groups));
    }

    public Set<String> getMappedUserRoles(String user, Set<String> groups) {
        return userRolesMappings.get(keyFor(user, groups));
    }

    public void setUserRolesMapping(String user, Set<String> groups, Set<String> roles) {
        userRolesMappings.put(storedKey(user, groups), roles);
    }

    private IdentityKey keyFor(String user, Set<String> groups) {
        if (hotKey != null && Objects.equals(user, hotUser) && groups == hotGroupsRef) {
            return hotKey;
        }

        IdentityKey key = new IdentityKey(user, groups, false);

        hotKey = key;
        hotUser = user;
        hotGroupsRef = groups;

        return key;
    }

    private IdentityKey storedKey(String user, Set<String> groups) {
        IdentityKey key = new IdentityKey(user, groups, true);

        hotKey = key;
        hotUser = user;
        hotGroupsRef = groups;

        return key;
    }

    private static final class IdentityKey {
        private final String user;
        private final Set<String> groups;
        private final int hash;

        private IdentityKey(String user, Set<String> groups, boolean copy) {
            this.user = user;
            this.groups = copy || CollectionUtils.isEmpty(groups) ? copyGroups(groups) : groups;
            this.hash = Objects.hash(user, this.groups);
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }

            if (!(other instanceof IdentityKey)) {
                return false;
            }

            IdentityKey that = (IdentityKey) other;

            return Objects.equals(user, that.user) && groups.equals(that.groups);
        }

        @Override
        public int hashCode() {
            return hash;
        }

        private static Set<String> copyGroups(Set<String> groups) {
            if (CollectionUtils.isEmpty(groups)) {
                return Collections.emptySet();
            }

            return Collections.unmodifiableSet(new HashSet<>(groups));
        }
    }
}
