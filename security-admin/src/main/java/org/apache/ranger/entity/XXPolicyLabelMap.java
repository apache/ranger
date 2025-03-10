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

import java.io.Serializable;
import java.util.Objects;

@Entity
@Cacheable
@Table(name = "x_policy_label_map")
public class XXPolicyLabelMap extends XXDBBase implements Serializable {
    private static final long serialVersionUID = 1L;

    /**
     * id of the XXPolicyLabelMap
     * <ul>
     * </ul>
     */
    @Id
    @SequenceGenerator(name = "X_POLICY_LABEL_MAP_SEQ", sequenceName = "X_POLICY_LABEL_MAP_SEQ", allocationSize = 1)
    @GeneratedValue(strategy = GenerationType.AUTO, generator = "X_POLICY_LABEL_MAP_SEQ")
    @Column(name = "id")
    protected Long id;

    /**
     * Global Id for the object
     * <ul>
     * <li>The maximum length for this attribute is <b>512</b>.
     * </ul>
     */
    @Column(name = "guid", unique = true, nullable = false, length = 512)
    protected String guid;

    /**
     * policyId of the XXPolicyLabelMap
     * <ul>
     * </ul>
     */
    @Column(name = "policy_id")
    protected Long policyId;

    /**
     * policyLabelId of the XXPolicyLabelMap
     * <ul>
     * </ul>
     */
    @Column(name = "policy_label_id")
    protected Long policyLabelId;

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        } else if (!super.equals(obj)) {
            return false;
        }

        XXPolicyLabelMap other = (XXPolicyLabelMap) obj;

        return Objects.equals(id, other.id) &&
                Objects.equals(guid, other.guid) &&
                Objects.equals(policyId, other.policyId) &&
                Objects.equals(policyLabelId, other.policyLabelId);
    }

    @Override
    public String toString() {
        String str = "XXPolicyLabelMap={[id=" + id + "]";
        str += super.toString();
        str += " [guid=" + guid + ", policyId=" + policyId + ", policyLabelId=" + policyLabelId + "]";
        str += "}";
        return str;
    }

    /**
     * @return the gUID
     */
    public String getGuid() {
        return guid;
    }

    /**
     * @param gUID the gUID to set
     */
    public void setGuid(String gUID) {
        guid = gUID;
    }

    /**
     * @return the policyId
     */
    public Long getPolicyId() {
        return policyId;
    }

    /**
     * @param policyId the policyId to set
     */
    public void setPolicyId(Long policyId) {
        this.policyId = policyId;
    }

    /**
     * @return the policyLabelId
     */
    public Long getPolicyLabelId() {
        return policyLabelId;
    }

    /**
     * @param policyLabelId the policyLabelId to set
     */
    public void setPolicyLabelId(Long policyLabelId) {
        this.policyLabelId = policyLabelId;
    }
}
