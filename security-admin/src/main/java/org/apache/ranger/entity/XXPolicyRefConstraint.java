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

/**
 * Constraint used by a constraint policy (policy type 4). constraintDefs are stored only in x_service_def.def_text,
 * so a reference holds the constraint name and the service-def ID, instead of the ID of a constraint-def row.
 */
@Entity
@Cacheable
@Table(name = "x_policy_ref_constraint")
public class XXPolicyRefConstraint implements java.io.Serializable {
    private static final long serialVersionUID = 1L;

    @Id
    @SequenceGenerator(name = "x_policy_ref_constraint_SEQ", sequenceName = "x_policy_ref_constraint_SEQ", allocationSize = 1)
    @GeneratedValue(strategy = GenerationType.AUTO, generator = "x_policy_ref_constraint_SEQ")
    @Column(name = "id")
    protected Long id;

    @Column(name = "policy_id")
    protected Long policyId;

    @Column(name = "service_def_id")
    protected Long serviceDefId;

    @Column(name = "constraint_name")
    protected String constraintName;

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public Long getPolicyId() {
        return policyId;
    }

    public void setPolicyId(Long policyId) {
        this.policyId = policyId;
    }

    public Long getServiceDefId() {
        return serviceDefId;
    }

    public void setServiceDefId(Long serviceDefId) {
        this.serviceDefId = serviceDefId;
    }

    public String getConstraintName() {
        return constraintName;
    }

    public void setConstraintName(String constraintName) {
        this.constraintName = constraintName;
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, policyId, serviceDefId, constraintName);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        } else if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        XXPolicyRefConstraint other = (XXPolicyRefConstraint) obj;

        return Objects.equals(id, other.id) &&
                Objects.equals(policyId, other.policyId) &&
                Objects.equals(serviceDefId, other.serviceDefId) &&
                Objects.equals(constraintName, other.constraintName);
    }

    @Override
    public String toString() {
        return "XXPolicyRefConstraint [id=" + id + ", policyId=" + policyId + ", serviceDefId=" + serviceDefId + ", constraintName=" + constraintName + "]";
    }
}
