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
@Table(name = "x_access_type_def_grants")
public class XXAccessTypeDefGrants extends XXDBBase implements java.io.Serializable {
    private static final long serialVersionUID = 1L;

    /**
     * id of the XXAccessTypeDefGrants
     * <ul>
     * </ul>
     */
    @Id
    @SequenceGenerator(name = "x_access_type_def_grants_SEQ", sequenceName = "x_access_type_def_grants_SEQ", allocationSize = 1)
    @GeneratedValue(strategy = GenerationType.AUTO, generator = "x_access_type_def_grants_SEQ")
    @Column(name = "id")
    protected Long id;

    /**
     * atdId of the XXAccessTypeDefGrants
     * <ul>
     * </ul>
     */
    @Column(name = "atd_id")
    protected Long atdId;

    /**
     * impliedGrant of the XXAccessTypeDefGrants
     * <ul>
     * </ul>
     */
    @Column(name = "implied_grant")
    protected String impliedGrant;

    /**
     * Returns the value for the member attribute <b>id</b>
     *
     * @return Date - value of member attribute <b>id</b> .
     */
    public Long getId() {
        return this.id;
    }

    /**
     * This method sets the value to the member attribute <b> id</b> . You
     * cannot set null to the attribute.
     *
     * @param id Value to set member attribute <b> id</b>
     */
    public void setId(Long id) {
        this.id = id;
    }

    @Override
    public int hashCode() {
        return super.hashCode();
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
        } else if (!super.equals(obj)) {
            return false;
        }

        XXAccessTypeDefGrants other = (XXAccessTypeDefGrants) obj;

        return Objects.equals(atdId, other.atdId) &&
                Objects.equals(id, other.id) &&
                Objects.equals(impliedGrant, other.impliedGrant);
    }

    /*
     * (non-Javadoc)
     *
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return "XXAccessTypeDefGrants [" + super.toString() + " id=" + id + ", atdId=" + atdId + ", impliedGrant=" + impliedGrant + "]";
    }

    /**
     * Returns the value for the member attribute <b>atdId</b>
     *
     * @return Date - value of member attribute <b>atdId</b> .
     */
    public Long getAtdId() {
        return this.atdId;
    }

    /**
     * This method sets the value to the member attribute <b> atdId</b> . You
     * cannot set null to the attribute.
     *
     * @param atdId Value to set member attribute <b> atdId</b>
     */
    public void setAtdId(Long atdId) {
        this.atdId = atdId;
    }

    /**
     * Returns the value for the member attribute <b>impliedGrant</b>
     *
     * @return Date - value of member attribute <b>impliedGrant</b> .
     */
    public String getImpliedGrant() {
        return this.impliedGrant;
    }

    /**
     * This method sets the value to the member attribute <b> impliedGrant</b> .
     * You cannot set null to the attribute.
     *
     * @param impliedGrant Value to set member attribute <b> impliedGrant</b>
     */
    public void setImpliedGrant(String impliedGrant) {
        this.impliedGrant = impliedGrant;
    }
}
