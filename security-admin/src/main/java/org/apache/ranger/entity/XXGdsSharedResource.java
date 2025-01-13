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
@Table(name = "x_gds_shared_resource")
@XmlRootElement
public class XXGdsSharedResource extends XXDBBase implements Serializable {
    private static final long serialVersionUID = 1L;

    @Id
    @SequenceGenerator(name = "X_GDS_SHARED_RESOURCE_SEQ", sequenceName = "X_GDS_SHARED_RESOURCE_SEQ", allocationSize = 1)
    @GeneratedValue(strategy = GenerationType.AUTO, generator = "X_GDS_SHARED_RESOURCE_SEQ")
    @Column(name = "id")
    protected Long id;

    @Column(name = "guid", unique = true, nullable = false, length = 512)
    protected String guid;

    @Version
    @Column(name = "version")
    protected Long version;

    @Column(name = "is_enabled")
    protected Boolean isEnabled;

    @Column(name = "name")
    protected String name;

    @Column(name = "description")
    protected String description;

    @Column(name = "data_share_id")
    protected Long dataShareId;

    @Column(name = "resource")
    protected String resource;

    @Column(name = "sub_resource")
    protected String subResource;

    @Column(name = "sub_resource_type")
    protected String subResourceType;

    @Column(name = "resource_signature")
    protected String resourceSignature;

    @Column(name = "condition_expr")
    protected String conditionExpr;

    @Column(name = "access_types")
    protected String accessTypes;

    @Column(name = "row_filter")
    protected String rowFilter;

    @Column(name = "sub_resource_masks")
    protected String subResourceMasks;

    @Column(name = "profiles")
    protected String profiles;

    @Column(name = "options")
    protected String options;

    @Column(name = "additional_info")
    protected String additionalInfo;

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

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
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

    public String getResource() {
        return resource;
    }

    public void setResource(String resource) {
        this.resource = resource;
    }

    public String getSubResource() {
        return subResource;
    }

    public void setSubResource(String subResource) {
        this.subResource = subResource;
    }

    public String getSubResourceType() {
        return subResourceType;
    }

    public void setSubResourceType(String subResourceType) {
        this.subResourceType = subResourceType;
    }

    public String getResourceSignature() {
        return resourceSignature;
    }

    public void setResourceSignature(String resourceSignature) {
        this.resourceSignature = resourceSignature;
    }

    public String getConditionExpr() {
        return conditionExpr;
    }

    public void setConditionExpr(String conditionExpr) {
        this.conditionExpr = conditionExpr;
    }

    public String getAccessTypes() {
        return accessTypes;
    }

    public void setAccessTypes(String accessTypes) {
        this.accessTypes = accessTypes;
    }

    public String getRowFilter() {
        return rowFilter;
    }

    public void setRowFilter(String rowFilter) {
        this.rowFilter = rowFilter;
    }

    public String getSubResourceMasks() {
        return subResourceMasks;
    }

    public void setSubResourceMasks(String subResourceMasks) {
        this.subResourceMasks = subResourceMasks;
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

    @Override
    public int getMyClassType() {
        return AppConstants.CLASS_TYPE_GDS_SHARED_RESOURCE;
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
        return Objects.hash(id, guid, version, isEnabled, name, description, dataShareId, resource, subResource, subResourceType, resourceSignature, conditionExpr, accessTypes, rowFilter, subResourceMasks, profiles, options, additionalInfo);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        } else if (!super.equals(obj)) {
            return false;
        }

        XXGdsSharedResource other = (XXGdsSharedResource) obj;

        return Objects.equals(id, other.id) &&
                Objects.equals(guid, other.guid) &&
                Objects.equals(version, other.version) &&
                Objects.equals(isEnabled, other.isEnabled) &&
                Objects.equals(name, other.name) &&
                Objects.equals(description, other.description) &&
                Objects.equals(dataShareId, other.dataShareId) &&
                Objects.equals(resource, other.resource) &&
                Objects.equals(subResource, other.subResource) &&
                Objects.equals(subResourceType, other.subResourceType) &&
                Objects.equals(resourceSignature, other.resourceSignature) &&
                Objects.equals(conditionExpr, other.conditionExpr) &&
                Objects.equals(accessTypes, other.accessTypes) &&
                Objects.equals(rowFilter, other.rowFilter) &&
                Objects.equals(subResourceMasks, other.subResourceMasks) &&
                Objects.equals(profiles, other.profiles) &&
                Objects.equals(options, other.options) &&
                Objects.equals(additionalInfo, other.additionalInfo);
    }

    @Override
    public String toString() {
        return toString(new StringBuilder()).toString();
    }

    public StringBuilder toString(StringBuilder sb) {
        sb.append("XXGdsSharedResource={ ")
                .append(super.toString()).append(" ")
                .append("id={").append(id).append("} ")
                .append("guid={").append(guid).append("} ")
                .append("version={").append(version).append("} ")
                .append("isEnabled={").append(isEnabled).append("} ")
                .append("description={").append(description).append("} ")
                .append("name={").append(name).append("} ")
                .append("description={").append(description).append("} ")
                .append("dataShareId={").append(dataShareId).append("} ")
                .append("resource={").append(resource).append("} ")
                .append("subResource={").append(subResource).append("} ")
                .append("subResourceType={").append(subResourceType).append("} ")
                .append("conditionExpr={").append(conditionExpr).append("} ")
                .append("accessTypes={").append(accessTypes).append("} ")
                .append("rowFilter={").append(rowFilter).append("} ")
                .append("subResourceMasks={").append(subResourceMasks).append("} ")
                .append("profiles={").append(profiles).append("} ")
                .append("options={").append(options).append("} ")
                .append("additionalInfo={").append(additionalInfo).append("} ")
                .append(" }");

        return sb;
    }
}
