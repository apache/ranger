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

import java.io.Serializable;
import java.util.Objects;

@Entity
@Cacheable
@Table(name = "x_service_resource_element")
public class XXServiceResourceElement extends XXDBBase implements Serializable {
    private static final long serialVersionUID = 1L;

    @Id
    @SequenceGenerator(name = "X_SERVICE_RESOURCE_ELEMENT_SEQ", sequenceName = "X_SERVICE_RESOURCE_ELEMENT_SEQ", allocationSize = 1)
    @GeneratedValue(strategy = GenerationType.AUTO, generator = "X_SERVICE_RESOURCE_ELEMENT_SEQ")
    @Column(name = "id")
    protected Long id;

    @Column(name = "res_def_id")
    protected Long resDefId;

    @Column(name = "res_id")
    protected Long resourceId;

    @Column(name = "is_excludes")
    protected Boolean isExcludes;

    @Column(name = "is_recursive")
    protected Boolean isRecursive;

    /**
     * @return the resDefId
     */
    public Long getResDefId() {
        return resDefId;
    }

    /**
     * @param resDefId the resDefId to set
     */
    public void setResDefId(Long resDefId) {
        this.resDefId = resDefId;
    }

    /**
     * @return the isExcludes
     */
    public Boolean getIsExcludes() {
        return isExcludes;
    }

    /**
     * @param isExcludes the isExcludes to set
     */
    public void setIsExcludes(Boolean isExcludes) {
        this.isExcludes = isExcludes;
    }

    /**
     * @return the isRecursive
     */
    public Boolean getIsRecursive() {
        return isRecursive;
    }

    /**
     * @param isRecursive the isRecursive to set
     */
    public void setIsRecursive(Boolean isRecursive) {
        this.isRecursive = isRecursive;
    }

    /**
     * @return the resourceId
     */
    public Long getResourceId() {
        return resourceId;
    }

    /**
     * @param resourceId the resourceId to set
     */
    public void setResourceId(Long resourceId) {
        this.resourceId = resourceId;
    }

    @Override
    public int getMyClassType() {
        return AppConstants.CLASS_TYPE_XA_SERVICE_RESOURCE_ELEMENT;
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
        return Objects.hash(super.hashCode(), id, isExcludes, isRecursive, resDefId, resourceId);
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

        XXServiceResourceElement other = (XXServiceResourceElement) obj;

        return Objects.equals(id, other.id) &&
                Objects.equals(isExcludes, other.isExcludes) &&
                Objects.equals(isRecursive, other.isRecursive) &&
                Objects.equals(resDefId, other.resDefId) &&
                Objects.equals(resourceId, other.resourceId);
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
        sb.append(super.toString()).append("} ");
        sb.append("id={").append(id).append("} ");
        sb.append("resDefId={").append(resDefId).append("} ");
        sb.append("resourceId={").append(resourceId).append("} ");
        sb.append("isExcludes={").append(isExcludes).append("} ");
        sb.append("isRecursive={").append(isRecursive).append("} ");
        sb.append(" }");

        return sb;
    }
}
