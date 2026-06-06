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

import org.apache.ranger.entity.XXAuthSession;
import org.apache.ranger.entity.XXPortalUser;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArraySet;

public class UserSessionBase implements Serializable {
    private static final long serialVersionUID = 1L;

    XXPortalUser                 xXPortalUser;
    XXAuthSession                xXAuthSession;
    int                          clientTimeOffsetInMinute;
    private boolean              userAdmin;
    private boolean              userAuditAdmin;
    private boolean              auditKeyAdmin;
    private boolean              keyAdmin;
    /** Set at login when login matches {@code ranger.admin.super.users} / super.groups. */
    private boolean              configSuperUser;
    private int                  authProvider   = RangerConstants.USER_APP;
    private List<String>         userRoleList   = new ArrayList<>();
    private RangerUserPermission rangerUserPermission;
    private Boolean              isSSOEnabled;
    private Boolean              isSpnegoEnabled = Boolean.FALSE;

    public Long getUserId() {
        if (xXPortalUser != null) {
            return xXPortalUser.getId();
        }

        return null;
    }

    public String getLoginId() {
        if (xXPortalUser != null) {
            return xXPortalUser.getLoginId();
        }

        return null;
    }

    public Long getSessionId() {
        if (xXAuthSession != null) {
            return xXAuthSession.getId();
        }

        return null;
    }

    public boolean isUserAdmin() {
        return userAdmin;
    }

    public void setUserAdmin(boolean userAdmin) {
        this.userAdmin = userAdmin;
    }

    public boolean isAuditUserAdmin() {
        return userAuditAdmin;
    }

    public void setAuditUserAdmin(boolean userAuditAdmin) {
        this.userAuditAdmin = userAuditAdmin;
    }

    public XXPortalUser getXXPortalUser() {
        return xXPortalUser;
    }

    public void setXXPortalUser(XXPortalUser gjUser) {
        this.xXPortalUser = gjUser;
    }

    public void setXXAuthSession(XXAuthSession gjAuthSession) {
        this.xXAuthSession = gjAuthSession;
    }

    public List<String> getUserRoleList() {
        return this.userRoleList;
    }

    public void setUserRoleList(List<String> strRoleList) {
        this.userRoleList = strRoleList;
    }

    public int getAuthProvider() {
        return this.authProvider;
    }

    public void setAuthProvider(int userSource) {
        this.authProvider = userSource;
    }

    public int getClientTimeOffsetInMinute() {
        return clientTimeOffsetInMinute;
    }

    public void setClientTimeOffsetInMinute(int clientTimeOffsetInMinute) {
        this.clientTimeOffsetInMinute = clientTimeOffsetInMinute;
    }

    public boolean isKeyAdmin() {
        return keyAdmin;
    }

    public void setKeyAdmin(boolean keyAdmin) {
        this.keyAdmin = keyAdmin;
    }

    public boolean isConfigSuperUser() {
        return configSuperUser;
    }

    public void setConfigSuperUser(final boolean configSuperUser) {
        this.configSuperUser = configSuperUser;
    }

    /**
     * True when the portal DB role is only {@code ROLE_USER} and the session
     * is not an effective Ranger admin ({@link #isEffectiveRangerAdmin()}).
     */
    public boolean isSingleRoleUserSession() {
        return userRoleList != null
                && userRoleList.size() == 1
                && userRoleList.contains(RangerConstants.ROLE_USER)
                && !isEffectiveRangerAdmin();
    }

    /**
     * True when the session should be treated as a full Ranger admin for
     * authorization bypasses (e.g. {@link #isSingleRoleUserSession()},
     * role REST access).
     * <p>
     * Both operands are required:
     * <ul>
     *   <li>{@code userAdmin} — stock DB admins ({@code ROLE_SYS_ADMIN}) never
     *       get {@link #isConfigSuperUser()} but always have {@code userAdmin}.</li>
     *   <li>{@code configSuperUser} — config super-user grant
     *       ({@code ranger.admin.super.users} / super.groups); grant also
     *       sets {@code userAdmin}, but this flag keeps the config path explicit.</li>
     * </ul>
     * {@code keyAdmin} is intentionally excluded: key admin is not full sys admin.
     */
    public boolean isEffectiveRangerAdmin() {
        return userAdmin || configSuperUser;
    }

    public boolean isAuditKeyAdmin() {
        return auditKeyAdmin;
    }

    public void setAuditKeyAdmin(boolean auditKeyAdmin) {
        this.auditKeyAdmin = auditKeyAdmin;
    }

    /**
     * @return the rangerUserPermission
     */
    public RangerUserPermission getRangerUserPermission() {
        return rangerUserPermission;
    }

    /**
     * @param rangerUserPermission the rangerUserPermission to set
     */
    public void setRangerUserPermission(RangerUserPermission rangerUserPermission) {
        this.rangerUserPermission = rangerUserPermission;
    }

    public Boolean isSSOEnabled() {
        return isSSOEnabled;
    }

    public void setSSOEnabled(Boolean isSSOEnabled) {
        this.isSSOEnabled = isSSOEnabled;
    }

    public Boolean isSpnegoEnabled() {
        return isSpnegoEnabled;
    }

    public void setSpnegoEnabled(Boolean isSpnegoEnabled) {
        this.isSpnegoEnabled = isSpnegoEnabled;
    }

    public static class RangerUserPermission implements Serializable {
        private static final long serialVersionUID = 1L;

        protected CopyOnWriteArraySet<String> userPermissions;
        protected Long                        lastUpdatedTime;

        /**
         * @return the userPermissions
         */
        public CopyOnWriteArraySet<String> getUserPermissions() {
            return userPermissions;
        }

        /**
         * @param userPermissions the userPermissions to set
         */
        public void setUserPermissions(CopyOnWriteArraySet<String> userPermissions) {
            this.userPermissions = userPermissions;
        }

        /**
         * @return the lastUpdatedTime
         */
        public Long getLastUpdatedTime() {
            return lastUpdatedTime;
        }

        /**
         * @param lastUpdatedTime the lastUpdatedTime to set
         */
        public void setLastUpdatedTime(Long lastUpdatedTime) {
            this.lastUpdatedTime = lastUpdatedTime;
        }
    }
}
