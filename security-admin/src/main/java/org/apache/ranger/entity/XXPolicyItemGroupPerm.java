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
@Table(name = "x_policy_item_group_perm")
public class XXPolicyItemGroupPerm extends XXDBBase implements java.io.Serializable {
    private static final long serialVersionUID = 1L;

    /**
     * id of the XXPolicyItemGroupPerm
     * <ul>
     * </ul>
     */
    @Id
    @SequenceGenerator(name = "x_policy_item_group_perm_SEQ", sequenceName = "x_policy_item_group_perm_SEQ", allocationSize = 1)
    @GeneratedValue(strategy = GenerationType.AUTO, generator = "x_policy_item_group_perm_SEQ")
    @Column(name = "id")
    protected Long id;

    /**
     * policyItemId of the XXPolicyItemGroupPerm
     * <ul>
     * </ul>
     */
    @Column(name = "policy_item_id")
    protected Long policyItemId;

    /**
     * groupId of the XXPolicyItemGroupPerm
     * <ul>
     * </ul>
     */
    @Column(name = "group_id")
    protected Long groupId;

    /**
     * order of the XXPolicyItemGroupPerm
     * <ul>
     * </ul>
     */
    @Column(name = "sort_order")
    protected Integer order;

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

        XXPolicyItemGroupPerm other = (XXPolicyItemGroupPerm) obj;

        return Objects.equals(groupId, other.groupId) &&
                Objects.equals(id, other.id) &&
                Objects.equals(order, other.order) &&
                Objects.equals(policyItemId, other.policyItemId);
    }

    /*
     * (non-Javadoc)
     *
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return "XXPolicyItemGroupPerm [" + super.toString() + " id=" + id
                + ", policyItemId=" + policyItemId + ", groupId=" + groupId
                + ", order=" + order + "]";
    }

    /**
     * This method sets the value to the member attribute <b> policyItemId</b> .
     * You cannot set null to the attribute.
     *
     * @param policyItemId Value to set member attribute <b> policyItemId</b>
     */
    public void setPolicyItemId(Long policyItemId) {
        this.policyItemId = policyItemId;
    }

    /**
     * Returns the value for the member attribute <b>policyItemId</b>
     *
     * @return Date - value of member attribute <b>policyItemId</b> .
     */
    public Long getPolicyitemid() {
        return this.policyItemId;
    }

    /**
     * This method sets the value to the member attribute <b> groupId</b> . You
     * cannot set null to the attribute.
     *
     * @param groupId Value to set member attribute <b> groupId</b>
     */
    public void setGroupId(Long groupId) {
        this.groupId = groupId;
    }

    /**
     * Returns the value for the member attribute <b>groupId</b>
     *
     * @return Date - value of member attribute <b>groupId</b> .
     */
    public Long getGroupid() {
        return this.groupId;
    }

    /**
     * Returns the value for the member attribute <b>order</b>
     *
     * @return Date - value of member attribute <b>order</b> .
     */
    public Integer getOrder() {
        return this.order;
    }

    /**
     * This method sets the value to the member attribute <b> order</b> . You
     * cannot set null to the attribute.
     *
     * @param order Value to set member attribute <b> order</b>
     */
    public void setOrder(Integer order) {
        this.order = order;
    }
}
