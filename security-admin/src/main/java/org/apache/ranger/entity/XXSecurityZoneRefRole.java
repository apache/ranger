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

package org.apache.ranger.entity;

import javax.persistence.Cacheable;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.SequenceGenerator;
import javax.persistence.Table;
import java.util.Objects;

@Entity
@Cacheable
@Table(name = "x_security_zone_ref_role")
public class XXSecurityZoneRefRole extends XXDBBase implements java.io.Serializable{
	private static final long serialVersionUID = 1L;
  	@Id
    @SequenceGenerator(name = "x_sec_zone_ref_role_SEQ", sequenceName = "x_sec_zone_ref_role_SEQ", allocationSize = 1)
    @GeneratedValue(strategy = GenerationType.AUTO, generator = "x_sec_zone_ref_role_SEQ")
    @Column(name = "id")
    protected Long id;

  	/**
	 * zoneId of the XXSecurityZoneRefRole
	 * <ul>
	 * </ul>
	 *
	 */
	@Column(name = "zone_id")
	protected Long zoneId;

  	/**
	 * roleId of the XXSecurityZoneRefRole
	 * <ul>
	 * </ul>
	 *
	 */
	@Column(name = "role_id")
	protected Long roleId;

	/**
	 * roleName of the XXSecurityZoneRefRole
	 * <ul>
	 * </ul>
	 *
	 */
	@Column(name = "role_name")
	protected String roleName;

	/**
	 * roleType of the XXSecurityZoneRefRole , 1 for admin,0 for audit user.
	 * <ul>
	 * </ul>
	 *
	 */
    @Override
    public void setId(Long id) {
        this.id = id;
    }

    @Override
    public Long getId() {
        return id;
    }

    /**
	 * This method sets the value to the member attribute <b> zoneId</b> .
	 * You cannot set null to the attribute.
	 *
	 * @param zoneId
	 *            Value to set member attribute <b> zoneId</b>
	 */
	public void setZoneId(Long zoneId) {
		this.zoneId = zoneId;
	}

	/**
	 * Returns the value for the member attribute <b>zoneId</b>
	 *
	 * @return Date - value of member attribute <b>zoneId</b> .
	 */
	public Long getZoneId() {
		return this.zoneId;
	}

	/**
	 * This method sets the value to the member attribute <b> roleId</b> .
	 * You cannot set null to the attribute.
	 *
	 * @param roleId
	 *            Value to set member attribute <b> roleId</b>
	 */
	public void setRoleId(Long roleId) {
		this.roleId = roleId;
	}

	/**
	 * Returns the value for the member attribute <b>roleId</b>
	 *
	 * @return Date - value of member attribute <b>roleId</b> .
	 */
	public Long getRoleId() {
		return roleId;
	}

	/**
	 * This method sets the value to the member attribute <b> roleName</b> .
	 * You cannot set null to the attribute.
	 *
	 * @param roleName
	 *            Value to set member attribute <b> roleName</b>
	 */
	public void setRoleName(String roleName) {
		this.roleName = roleName;
	}

	/**
	 * Returns the value for the member attribute <b>roleName</b>
	 *
	 * @return Date - value of member attribute <b>roleName</b> .
	 */
	public String getRoleName() {
		return roleName;
	}

	@Override
	public int hashCode() {
		return Objects.hash(super.hashCode(), id, zoneId, roleId, roleName);
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}

		if (getClass() != obj.getClass()) {
			return false;
		}

		XXSecurityZoneRefRole other = (XXSecurityZoneRefRole) obj;

		return super.equals(obj) &&
			   Objects.equals(id, other.id) &&
			   Objects.equals(zoneId, other.zoneId) &&
			   Objects.equals(roleId, other.roleId) &&
			   Objects.equals(roleName, other.roleName);
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "XXSecurityZoneRefRole [" + super.toString() + " id=" + id + ", zoneId=" + zoneId + ", roleId="
				+ roleId + ", roleName=" + roleName + "]";
	}
}
