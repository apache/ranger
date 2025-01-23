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
@Table(name = "x_tag_attr_def")
public class XXTagAttributeDef extends XXDBBase implements Serializable {
    private static final long serialVersionUID = 1L;

    @Id
    @SequenceGenerator(name = "X_TAG_ATTR_DEF_SEQ", sequenceName = "X_TAG_ATTR_DEF_SEQ", allocationSize = 1)
    @GeneratedValue(strategy = GenerationType.AUTO, generator = "X_TAG_ATTR_DEF_SEQ")
    @Column(name = "id")
    protected Long id;

    @Column(name = "tag_def_id")
    protected Long tagDefId;

    @Column(name = "name")
    protected String name;

    @Column(name = "type")
    protected String type;

    /**
     * @return the tagDefId
     */
    public Long getTagDefId() {
        return tagDefId;
    }

    /**
     * @param tagDefId the tagDefId to set
     */
    public void setTagDefId(Long tagDefId) {
        this.tagDefId = tagDefId;
    }

    /**
     * @return the name
     */
    public String getName() {
        return name;
    }

    /**
     * @param name the name to set
     */
    public void setName(String name) {
        this.name = name;
    }

    /**
     * @return the type
     */
    public String getType() {
        return type;
    }

    /**
     * @param type the type to set
     */
    public void setType(String type) {
        this.type = type;
    }

    @Override
    public int getMyClassType() {
        return AppConstants.CLASS_TYPE_XA_TAG_ATTR_DEF;
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
        return Objects.hash(super.hashCode(), id, name, tagDefId, type);
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

        XXTagAttributeDef other = (XXTagAttributeDef) obj;

        return Objects.equals(id, other.id) &&
                Objects.equals(name, other.name) &&
                Objects.equals(tagDefId, other.tagDefId) &&
                Objects.equals(type, other.type);
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
        sb.append("tagDefId={").append(tagDefId).append("} ");
        sb.append("name={").append(name).append("} ");
        sb.append("type={").append(type).append("} ");
        sb.append(" }");

        return sb;
    }
}
