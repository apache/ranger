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

package org.apache.ranger.common;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Configuration-based Ranger Admin super users and super groups.
 * When {@code ranger.admin.super.users} or
 * {@code ranger.admin.super.groups} are set, matching authenticated users
 * receive full Ranger administrative privileges (system admin and key admin
 * capabilities) without requiring corresponding roles in the Ranger database.
 */
public final class RangerSuperUserConfig {
    private static volatile RangerSuperUserConfig instance;

    private final Set<String> superUsers;
    private final Set<String> superUserGroups;
    private final boolean     superUsersConfigured;
    private final boolean     superGroupsConfigured;
    private final boolean     enabled;

    private RangerSuperUserConfig() {
        superUsers            = buildConfiguredSet(
                PropertiesUtil.getPropertyStringList(
                        RangerConstants.RANGER_ADMIN_SUPER_USERS));
        superUserGroups       = buildConfiguredSet(
                PropertiesUtil.getPropertyStringList(
                        RangerConstants.RANGER_ADMIN_SUPER_GROUPS));
        superUsersConfigured  = !superUsers.isEmpty();
        superGroupsConfigured = !superUserGroups.isEmpty();
        enabled               = superUsersConfigured || superGroupsConfigured;
    }

    /**
     * Clears cached config snapshot. For unit tests only after
     * {@link PropertiesUtil} properties change.
     */
    public static synchronized void resetForTests() {
        instance = null;
    }

    /**
     * @return true when {@code ranger.admin.super.users} or
     *         {@code ranger.admin.super.groups} has at least one non-blank
     *         entry
     */
    public static boolean isEnabled() {
        return getInstance().enabled;
    }

    /**
     * @return true when {@code ranger.admin.super.users} has at least one
     *         non-blank entry
     */
    public static boolean isSuperUsersConfigured() {
        return getInstance().superUsersConfigured;
    }

    /**
     * @return true when {@code ranger.admin.super.groups} has at least one
     *         non-blank entry
     */
    public static boolean isSuperGroupsConfigured() {
        return getInstance().superGroupsConfigured;
    }

    /**
     * @param loginId authenticated login id
     * @return true when loginId matches configured super users
     */
    public static boolean isSuperUser(final String loginId) {
        if (StringUtils.isBlank(loginId) || !isEnabled()) {
            return false;
        }

        return getInstance().matchesUser(loginId);
    }

    /**
     * @param loginId authenticated login id
     * @param userGroups group names from Ranger user store
     * @return true when loginId or groups match configured super users/groups
     */
    public static boolean isSuperUser(final String loginId,
            final Set<String> userGroups) {
        if (StringUtils.isBlank(loginId) || !isEnabled()) {
            return false;
        }

        RangerSuperUserConfig cfg = getInstance();

        if (cfg.superUsersConfigured && cfg.matchesUser(loginId)) {
            return true;
        }

        if (cfg.superGroupsConfigured) {
            return cfg.matchesGroups(userGroups);
        }

        return false;
    }

    /**
     * Merges config super-user admin roles with existing portal roles.
     * Used for Spring Security authentication and session role lists.
     *
     * @param existingRoles DB portal roles (may be null)
     * @param includeRoleUser when true, adds {@code ROLE_USER} (session lists)
     * @return merged role list with stable ordering
     */
    public static List<String> mergeConfigSuperUserRoles(
            final Collection<String> existingRoles,
            final boolean includeRoleUser) {
        LinkedHashSet<String> merged = new LinkedHashSet<>();

        merged.add(RangerConstants.ROLE_SYS_ADMIN);
        merged.add(RangerConstants.ROLE_KEY_ADMIN);

        if (includeRoleUser) {
            merged.add(RangerConstants.ROLE_USER);
        }

        if (existingRoles != null) {
            merged.addAll(existingRoles);
        }

        return new ArrayList<>(merged);
    }

    /**
     * Admin roles exposed on {@code GET /user/profile} for config super-users.
     */
    public static List<String> getConfigSuperUserProfileRoles() {
        return mergeConfigSuperUserRoles(Collections.emptyList(), false);
    }

    private static RangerSuperUserConfig getInstance() {
        RangerSuperUserConfig cfg = instance;

        if (cfg == null) {
            synchronized (RangerSuperUserConfig.class) {
                cfg = instance;

                if (cfg == null) {
                    cfg = new RangerSuperUserConfig();
                    instance = cfg;
                }
            }
        }

        return cfg;
    }

    private static Set<String> buildConfiguredSet(final String[] values) {
        Set<String> configured = new HashSet<>();

        if (values != null) {
            for (String value : values) {
                if (StringUtils.isNotBlank(value)) {
                    configured.add(value.trim());
                }
            }
        }

        return Collections.unmodifiableSet(configured);
    }

    private boolean matchesUser(final String loginId) {
        for (String configuredUser : superUsers) {
            if ("*".equals(configuredUser)
                    || configuredUser.equalsIgnoreCase(loginId)) {
                return true;
            }
        }

        return false;
    }

    private boolean matchesGroups(final Set<String> userGroups) {
        if (CollectionUtils.isEmpty(userGroups)) {
            return false;
        }

        for (String configuredGroup : superUserGroups) {
            if (RangerConstants.GROUP_PUBLIC.equalsIgnoreCase(configuredGroup)
                    || containsGroupIgnoreCase(userGroups, configuredGroup)) {
                return true;
            }
        }

        return false;
    }

    private static boolean containsGroupIgnoreCase(final Set<String> userGroups,
            final String configuredGroup) {
        for (String userGroup : userGroups) {
            if (userGroup != null
                    && userGroup.equalsIgnoreCase(configuredGroup)) {
                return true;
            }
        }

        return false;
    }
}
