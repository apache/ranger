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

import org.apache.ranger.common.AppConstants;

import javax.persistence.Cacheable;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.SequenceGenerator;
import javax.persistence.Table;
import javax.xml.bind.annotation.XmlRootElement;

import java.io.Serializable;
import java.util.Objects;

@Entity
@Cacheable
@Table(name = "x_gds_dataset_policy_map")
@XmlRootElement
public class XXGdsDatasetPolicyMap implements Serializable {
    private static final long serialVersionUID = 1L;

    @Id
    @SequenceGenerator(name = "X_GDS_DATASET_POLICY_MAP_SEQ", sequenceName = "X_GDS_DATASET_POLICY_MAP_SEQ", allocationSize = 1)
    @GeneratedValue(strategy = GenerationType.AUTO, generator = "X_GDS_DATASET_POLICY_MAP_SEQ")
    @Column(name = "id")
    protected Long id;

    @Column(name = "dataset_id")
    protected Long datasetId;

    @Column(name = "policy_id")
    protected Long policyId;

    public XXGdsDatasetPolicyMap() {}

    public XXGdsDatasetPolicyMap(Long datasetId, Long policyId) {
        setDatasetId(datasetId);
        setPolicyId(policyId);
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public Long getDatasetId() {
        return datasetId;
    }

    public void setDatasetId(Long datasetId) {
        this.datasetId = datasetId;
    }

    public Long getPolicyId() {
        return policyId;
    }

    public void setPolicyId(Long policyId) {
        this.policyId = policyId;
    }

    public int getMyClassType() {
        return AppConstants.CLASS_TYPE_GDS_DATASET_POLICY_MAP;
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, datasetId, policyId);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        } else if (!super.equals(obj)) {
            return false;
        }

        XXGdsDatasetPolicyMap other = (XXGdsDatasetPolicyMap) obj;

        return Objects.equals(id, other.id) &&
                Objects.equals(datasetId, other.datasetId) &&
                Objects.equals(policyId, other.policyId);
    }

    @Override
    public String toString() {
        return toString(new StringBuilder()).toString();
    }

    public StringBuilder toString(StringBuilder sb) {
        sb.append("XXGdsDatasetPolicyMap={ ")
                .append(super.toString() + "} ")
                .append("id={").append(id).append("} ")
                .append("datasetId={").append(datasetId).append("} ")
                .append("policyId={").append(policyId).append("} ")
                .append(" }");

        return sb;
    }
}
