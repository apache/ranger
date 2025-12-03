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

package org.apache.ranger.entity.view;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.IdClass;
import javax.persistence.Table;

import java.io.Serializable;
import java.util.Objects;

@Entity
@Table(name = "vx_security_zone_user")
@IdClass(VXSecurityZoneUser.VXSecurityZoneUserId.class)
public class VXSecurityZoneUser implements Serializable {
    /**
     * Access Type Constants:
     * 0 = Direct User Access
     * 1 = Group Membership Access
     * 2 = Role Membership Access
     * 3 = Role-Group Access
     * 4 = Public Group Access
     */
    public static final  int     ACCESS_TYPE_DIRECT_USER       = 0;
    public static final  int     ACCESS_TYPE_GROUP_MEMBER      = 1;
    public static final  int     ACCESS_TYPE_ROLE_MEMBER       = 2;
    public static final  int     ACCESS_TYPE_ROLE_GROUP_MEMBER = 3;
    public static final  int     ACCESS_TYPE_PUBLIC_GROUP      = 4;
    private static final long    serialVersionUID              = 1L;
    @Id
    @Column(name = "ZONE_ID", nullable = false)
    protected            Long    zoneId;
    @Column(name = "ZONE_NAME", nullable = false)
    protected            String  zoneName;
    @Id
    @Column(name = "USER_ID", nullable = false)
    protected            Long    userId;
    @Id
    @Column(name = "ACCESS_TYPE", nullable = false)
    protected            Integer accessType;

    /**
     * @return the zoneId
     */
    public Long getZoneId() {
        return zoneId;
    }

    /**
     * @param zoneId the zoneId to set
     */
    public void setZoneId(Long zoneId) {
        this.zoneId = zoneId;
    }

    /**
     * @return the zoneName
     */
    public String getZoneName() {
        return zoneName;
    }

    /**
     * @param zoneName the zoneName to set
     */
    public void setZoneName(String zoneName) {
        this.zoneName = zoneName;
    }

    /**
     * @return the userId
     */
    public Long getUserId() {
        return userId;
    }

    /**
     * @param userId the userId to set
     */
    public void setUserId(Long userId) {
        this.userId = userId;
    }

    /**
     * @return the accessType
     */
    public Integer getAccessType() {
        return accessType;
    }

    /**
     * @param accessType the accessType to set
     */
    public void setAccessType(Integer accessType) {
        this.accessType = accessType;
    }

    /**
     * Composite Primary Key class for VXSecurityZoneUser
     */
    public static class VXSecurityZoneUserId implements Serializable {
        private static final long serialVersionUID = 1L;

        protected Long    zoneId;
        protected Long    userId;
        protected Integer accessType;

        public VXSecurityZoneUserId() {
        }

        public VXSecurityZoneUserId(Long zoneId, Long userId, Integer accessType) {
            this.zoneId     = zoneId;
            this.userId     = userId;
            this.accessType = accessType;
        }

        @Override
        public int hashCode() {
            int result = zoneId != null ? zoneId.hashCode() : 0;
            result = 31 * result + (userId != null ? userId.hashCode() : 0);
            result = 31 * result + (accessType != null ? accessType.hashCode() : 0);
            return result;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            VXSecurityZoneUserId that = (VXSecurityZoneUserId) o;

            if (!Objects.equals(zoneId, that.zoneId)) {
                return false;
            }
            if (!Objects.equals(userId, that.userId)) {
                return false;
            }
            return Objects.equals(accessType, that.accessType);
        }
    }
}
