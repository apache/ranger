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
@Table(name = "x_tag")
public class XXTag extends XXDBBase implements Serializable {
    private static final long serialVersionUID = 1L;

    @Id
    @SequenceGenerator(name = "X_TAG_SEQ", sequenceName = "X_TAG_SEQ", allocationSize = 1)
    @GeneratedValue(strategy = GenerationType.AUTO, generator = "X_TAG_SEQ")
    @Column(name = "id")
    protected Long id;

    @Column(name = "guid", unique = true, nullable = false, length = 512)
    protected String guid;

    @Version
    @Column(name = "version")
    protected Long version;

    @Column(name = "type")
    protected Long type;

    @Column(name = "owned_by")
    protected Short owner;

    @Column(name = "policy_options")
    protected String options;

    @Column(name = "tag_attrs_text")
    protected String tagAttrs;

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
     * @return the type
     */
    public Long getType() {
        return type;
    }

    /**
     * @param type the type to set
     */
    public void setType(Long type) {
        this.type = type;
    }

    public Short getOwner() {
        return owner;
    }

    public void setOwner(Short owner) {
        this.owner = owner;
    }

    public String getOptions() {
        return this.options;
    }

    public void setOptions(String options) {
        this.options = options;
    }

    public String getTagAttrs() {
        return tagAttrs;
    }

    public void setTagAttrs(String tagAttrs) {
        this.tagAttrs = tagAttrs;
    }

    @Override
    public int getMyClassType() {
        return AppConstants.CLASS_TYPE_XA_TAG;
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
        return Objects.hash(super.hashCode(), version, guid, id, type, owner, options, tagAttrs);
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

        XXTag other = (XXTag) obj;

        return Objects.equals(version, other.version) &&
                Objects.equals(guid, other.guid) &&
                Objects.equals(id, other.id) &&
                Objects.equals(type, other.type) &&
                Objects.equals(owner, other.owner) &&
                Objects.equals(options, other.options) &&
                Objects.equals(tagAttrs, other.tagAttrs);
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
        sb.append("guid={").append(guid).append("} ");
        sb.append("type={").append(type).append("} ");
        sb.append("owned_by={").append(owner).append("} ");
        sb.append("options={").append(options).append("} ");
        sb.append("tagAttrs={").append(tagAttrs).append("} ");
        sb.append(" }");

        return sb;
    }
}
