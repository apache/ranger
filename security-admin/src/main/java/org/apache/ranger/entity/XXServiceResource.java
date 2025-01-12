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

import java.io.Serializable;
import java.util.Objects;

@Entity
@Cacheable
@Table(name = "x_service_resource")
public class XXServiceResource extends XXDBBase implements Serializable {
    private static final long serialVersionUID = 1L;

    @Id
    @SequenceGenerator(name = "X_SERVICE_RESOURCE_SEQ", sequenceName = "X_SERVICE_RESOURCE_SEQ", allocationSize = 1)
    @GeneratedValue(strategy = GenerationType.AUTO, generator = "X_SERVICE_RESOURCE_SEQ")
    @Column(name = "id")
    protected Long id;

    @Column(name = "guid", unique = true, nullable = false, length = 512)
    protected String guid;

    @Version
    @Column(name = "version")
    protected Long version;

    @Column(name = "is_enabled")
    protected Boolean isEnabled;

    @Column(name = "resource_signature")
    protected String resourceSignature;

    @Column(name = "service_id")
    protected Long serviceId;

    @Column(name = "service_resource_elements_text")
    protected String serviceResourceElements;

    @Column(name = "tags_text")
    protected String tags;

    /**
     * @return the guid
     */
    public String getGuid() {
        return guid;
    }

    /**
     * @param guid the guid to set
     */
    public void setGuid(String guid) {
        this.guid = guid;
    }

    /**
     * @return the serviceId
     */
    public Long getServiceId() {
        return serviceId;
    }

    /**
     * @param serviceId the serviceId to set
     */
    public void setServiceId(Long serviceId) {
        this.serviceId = serviceId;
    }

    /**
     * @return the resourceSignature
     */
    public String getResourceSignature() {
        return resourceSignature;
    }

    /**
     * @param resourceSignature the resourceSignature to set
     */
    public void setResourceSignature(String resourceSignature) {
        this.resourceSignature = resourceSignature;
    }

    /**
     * @return the version
     */
    public Long getVersion() {
        return version;
    }

    /**
     * @param version the version to set
     */
    public void setVersion(Long version) {
        this.version = version;
    }

    /**
     * @return the isEnabled
     */
    public Boolean getIsEnabled() {
        return isEnabled;
    }

    /**
     * @param isEnabled the isEnabled to set
     */
    public void setIsEnabled(Boolean isEnabled) {
        this.isEnabled = isEnabled;
    }

    public String getServiceResourceElements() {
        return serviceResourceElements;
    }

    public void setServiceResourceElements(String serviceResourceElements) {
        this.serviceResourceElements = serviceResourceElements;
    }

    public String getTags() {
        return tags;
    }

    public void setTags(String tags) {
        this.tags = tags;
    }

    @Override
    public int getMyClassType() {
        return AppConstants.CLASS_TYPE_XA_SERVICE_RESOURCE;
    }

    @Override
    public Long getId() {
        return id;
    }

    @Override
    public void setId(Long id) {
        this.id = id;
    }

    /*
     * (non-Javadoc)
     *
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), id, guid, version, isEnabled, resourceSignature, serviceId, serviceResourceElements, tags);
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

        XXServiceResource other = (XXServiceResource) obj;

        return Objects.equals(resourceSignature, other.resourceSignature) &&
                Objects.equals(guid, other.guid) &&
                Objects.equals(id, other.id) &&
                Objects.equals(isEnabled, other.isEnabled) &&
                Objects.equals(serviceId, other.serviceId) &&
                Objects.equals(version, other.version) &&
                Objects.equals(serviceResourceElements, other.serviceResourceElements) &&
                Objects.equals(tags, other.tags);
    }

    /*
     * (non-Javadoc)
     *
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        toString(sb);
        return sb.toString();
    }

    public StringBuilder toString(StringBuilder sb) {
        sb.append("{ ");
        sb.append(super.toString() + "} ");
        sb.append("id={").append(id).append("} ");
        sb.append("guid={").append(guid).append("} ");
        sb.append("version={").append(version).append("} ");
        sb.append("isEnabled={").append(isEnabled).append("} ");
        sb.append("resourceSignature={").append(resourceSignature).append("} ");
        sb.append("serviceId={").append(serviceId).append("} ");
        sb.append("serviceResourceElements={").append(serviceResourceElements).append("} ");
        sb.append("tags={").append(tags).append("} ");
        sb.append(" }");

        return sb;
    }
}
