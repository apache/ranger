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
import javax.persistence.Version;
import javax.xml.bind.annotation.XmlRootElement;

import java.io.Serializable;
import java.util.Objects;

@Entity
@Cacheable
@Table(name = "x_gds_data_share_in_dataset")
@XmlRootElement
public class XXGdsDataShareInDataset extends XXDBBase implements Serializable {
    private static final long serialVersionUID = 1L;

    @Id
    @SequenceGenerator(name = "X_GDS_DATA_SHARE_IN_DATASET_SEQ", sequenceName = "X_GDS_DATA_SHARE_IN_DATASET_SEQ", allocationSize = 1)
    @GeneratedValue(strategy = GenerationType.AUTO, generator = "X_GDS_DATA_SHARE_IN_DATASET_SEQ")
    @Column(name = "id")
    protected Long id;

    @Column(name = "guid", unique = true, nullable = false, length = 512)
    protected String guid;

    @Version
    @Column(name = "version")
    protected Long version;

    @Column(name = "is_enabled")
    protected Boolean isEnabled;

    @Column(name = "description")
    protected String description;

    @Column(name = "data_share_id")
    protected Long dataShareId;

    @Column(name = "dataset_id")
    protected Long datasetId;

    @Column(name = "status")
    protected Short status;

    @Column(name = "validity_period")
    protected String validityPeriod;

    @Column(name = "profiles")
    protected String profiles;

    @Column(name = "options")
    protected String options;

    @Column(name = "additional_info")
    protected String additionalInfo;

    @Column(name = "approver_id")
    protected Long approverId;

    public String getGuid() {
        return guid;
    }

    public void setGuid(String guid) {
        this.guid = guid;
    }

    public Long getVersion() {
        return version;
    }

    public void setVersion(Long version) {
        this.version = version;
    }

    public Boolean getIsEnabled() {
        return isEnabled;
    }

    public void setIsEnabled(Boolean isEnabled) {
        this.isEnabled = isEnabled;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public Long getDataShareId() {
        return dataShareId;
    }

    public void setDataShareId(Long dataShareId) {
        this.dataShareId = dataShareId;
    }

    public Long getDatasetId() {
        return datasetId;
    }

    public void setDatasetId(Long datasetId) {
        this.datasetId = datasetId;
    }

    public Short getStatus() {
        return status;
    }

    public void setStatus(Short status) {
        this.status = status;
    }

    public String getValidityPeriod() {
        return validityPeriod;
    }

    public void setValidityPeriod(String validityPeriod) {
        this.validityPeriod = validityPeriod;
    }

    public String getProfiles() {
        return profiles;
    }

    public void setProfiles(String profiles) {
        this.profiles = profiles;
    }

    public String getOptions() {
        return options;
    }

    public void setOptions(String options) {
        this.options = options;
    }

    public String getAdditionalInfo() {
        return additionalInfo;
    }

    public void setAdditionalInfo(String additionalInfo) {
        this.additionalInfo = additionalInfo;
    }

    public Long getApproverId() {
        return approverId;
    }

    public void setApproverId(Long approverId) {
        this.approverId = approverId;
    }

    @Override
    public int getMyClassType() {
        return AppConstants.CLASS_TYPE_GDS_DATA_SHARE_IN_DATASET;
    }

    @Override
    public Long getId() {
        return id;
    }

    @Override
    public void setId(Long id) {
        this.id = id;
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, guid, dataShareId, datasetId, status, validityPeriod, profiles, options, additionalInfo, approverId);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        } else if (!super.equals(obj)) {
            return false;
        }

        XXGdsDataShareInDataset other = (XXGdsDataShareInDataset) obj;

        return Objects.equals(id, other.id) &&
                Objects.equals(guid, other.guid) &&
                Objects.equals(version, other.version) &&
                Objects.equals(isEnabled, other.isEnabled) &&
                Objects.equals(dataShareId, other.dataShareId) &&
                Objects.equals(datasetId, other.datasetId) &&
                Objects.equals(status, other.status) &&
                Objects.equals(validityPeriod, other.validityPeriod) &&
                Objects.equals(profiles, other.profiles) &&
                Objects.equals(options, other.options) &&
                Objects.equals(additionalInfo, other.additionalInfo) &&
                Objects.equals(approverId, other.approverId);
    }

    @Override
    public String toString() {
        return toString(new StringBuilder()).toString();
    }

    public StringBuilder toString(StringBuilder sb) {
        sb.append("XXDataShareInDataset={ ")
                .append(super.toString() + "} ")
                .append("id={").append(id).append("} ")
                .append("guid={").append(guid).append("} ")
                .append("version={").append(version).append("} ")
                .append("isEnabled={").append(isEnabled).append("} ")
                .append("dataShareId={").append(dataShareId).append("} ")
                .append("datasetId={").append(datasetId).append("} ")
                .append("status={").append(status).append("} ")
                .append("validityPeriod={").append(validityPeriod).append("} ")
                .append("profiles={").append(profiles).append("} ")
                .append("options={").append(options).append("} ")
                .append("additionalInfo={").append(additionalInfo).append("} ")
                .append("approverId={").append(approverId).append("} ")
                .append(" }");

        return sb;
    }
}
