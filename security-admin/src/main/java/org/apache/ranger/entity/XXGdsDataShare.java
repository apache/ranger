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
@Table(name = "x_gds_data_share")
@XmlRootElement
public class XXGdsDataShare extends XXDBBase implements Serializable {
    private static final long serialVersionUID = 1L;

    @Id
    @SequenceGenerator(name = "X_GDS_DATA_SHARE_SEQ", sequenceName = "X_GDS_DATA_SHARE_SEQ", allocationSize = 1)
    @GeneratedValue(strategy = GenerationType.AUTO, generator = "X_GDS_DATA_SHARE_SEQ")
    @Column(name = "id")
    protected Long id;

    @Column(name = "guid", unique = true, nullable = false, length = 512)
    protected String guid;

    @Version
    @Column(name = "version")
    protected Long version;

    @Column(name = "is_enabled")
    protected Boolean isEnabled;

    @Column(name = "service_id")
    protected Long serviceId;

    @Column(name = "zone_id")
    protected Long zoneId;

    @Column(name = "name")
    protected String name;

    @Column(name = "description")
    protected String description;

    @Column(name = "acl")
    protected String acl;

    @Column(name = "condition_expr")
    protected String conditionExpr;

    @Column(name = "default_access_types")
    protected String defaultAccessTypes;

    @Column(name = "default_tag_masks")
    protected String defaultTagMasks;

    @Column(name = "terms_of_use")
    protected String termsOfUse;

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

    public Long getServiceId() {
        return serviceId;
    }

    public void setServiceId(Long serviceId) {
        this.serviceId = serviceId;
    }

    public Long getZoneId() {
        return zoneId;
    }

    public void setZoneId(Long zoneId) {
        this.zoneId = zoneId;
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

    public String getAcl() {
        return acl;
    }

    public void setAcl(String acl) {
        this.acl = acl;
    }

    public String getConditionExpr() {
        return conditionExpr;
    }

    public void setConditionExpr(String conditionExpr) {
        this.conditionExpr = conditionExpr;
    }

    public String getDefaultAccessTypes() {
        return defaultAccessTypes;
    }

    public void setDefaultAccessTypes(String defaultAccessTypes) {
        this.defaultAccessTypes = defaultAccessTypes;
    }

    public String getDefaultTagMasks() {
        return defaultTagMasks;
    }

    public void setDefaultTagMasks(String defaultMasks) {
        this.defaultTagMasks = defaultMasks;
    }

    public String getTermsOfUse() {
        return termsOfUse;
    }

    public void setTermsOfUse(String termsOfUse) {
        this.termsOfUse = termsOfUse;
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
        return AppConstants.CLASS_TYPE_GDS_DATA_SHARE;
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
        return Objects.hash(id, guid, version, isEnabled, serviceId, zoneId, name, description, acl, conditionExpr, defaultAccessTypes, defaultTagMasks, termsOfUse, options, additionalInfo);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        } else if (!super.equals(obj)) {
            return false;
        }

        XXGdsDataShare other = (XXGdsDataShare) obj;

        return Objects.equals(id, other.id) &&
                Objects.equals(guid, other.guid) &&
                Objects.equals(version, other.version) &&
                Objects.equals(isEnabled, other.isEnabled) &&
                Objects.equals(serviceId, other.serviceId) &&
                Objects.equals(zoneId, other.zoneId) &&
                Objects.equals(name, other.name) &&
                Objects.equals(description, other.description) &&
                Objects.equals(acl, other.acl) &&
                Objects.equals(conditionExpr, other.conditionExpr) &&
                Objects.equals(defaultAccessTypes, other.defaultAccessTypes) &&
                Objects.equals(defaultTagMasks, other.defaultTagMasks) &&
                Objects.equals(termsOfUse, other.termsOfUse) &&
                Objects.equals(options, other.options) &&
                Objects.equals(additionalInfo, other.additionalInfo);
    }

    @Override
    public String toString() {
        return toString(new StringBuilder()).toString();
    }

    public StringBuilder toString(StringBuilder sb) {
        sb.append("XXGdsDataShare={ ")
                .append(super.toString()).append(" ")
                .append("id={").append(id).append("} ")
                .append("guid={").append(guid).append("} ")
                .append("version={").append(version).append("} ")
                .append("isEnabled={").append(isEnabled).append("} ")
                .append("serviceId={").append(serviceId).append("} ")
                .append("zoneId={").append(zoneId).append("} ")
                .append("name={").append(name).append("} ")
                .append("description={").append(description).append("} ")
                .append("acl={").append(acl).append("} ")
                .append("conditionExpr={").append(conditionExpr).append("} ")
                .append("defaultAccessTypes={").append(defaultAccessTypes).append("} ")
                .append("defaultMasks={").append(defaultTagMasks).append("} ")
                .append("termsOfUse={").append(termsOfUse).append("} ")
                .append("options={").append(options).append("} ")
                .append("additionalInfo={").append(additionalInfo).append("} ")
                .append(" }");

        return sb;
    }
}
