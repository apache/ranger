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

@Entity
@Cacheable
@Table(name = "x_constraint_def")
public class XXConstraintDef extends XXDBBase implements java.io.Serializable {
    private static final long serialVersionUID = 1L;

    @Id
    @SequenceGenerator(name = "x_constraint_def_SEQ", sequenceName = "x_constraint_def_SEQ", allocationSize = 1)
    @GeneratedValue(strategy = GenerationType.AUTO, generator = "x_constraint_def_SEQ")
    @Column(name = "id")
    protected Long id;

    @Column(name = "def_id")
    protected Long defId;

    @Column(name = "item_id")
    protected Long itemId;

    @Column(name = "name")
    protected String name;

    @Column(name = "label")
    protected String label;

    @Column(name = "description")
    protected String description;

    @Column(name = "rb_key_label")
    protected String rbKeyLabel;

    @Column(name = "rb_key_description")
    protected String rbKeyDescription;

    @Column(name = "resources")
    protected String resources;

    @Column(name = "access_types")
    protected String accessTypes;

    @Column(name = "parameters")
    protected String parameters;

    @Column(name = "options")
    protected String options;

    @Column(name = "sort_order")
    protected Integer order;

    public Long getId() {
        return this.id;
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

        XXConstraintDef other = (XXConstraintDef) obj;

        return Objects.equals(defId, other.defId) &&
                Objects.equals(itemId, other.itemId) &&
                Objects.equals(name, other.name) &&
                Objects.equals(label, other.label) &&
                Objects.equals(description, other.description) &&
                Objects.equals(rbKeyLabel, other.rbKeyLabel) &&
                Objects.equals(rbKeyDescription, other.rbKeyDescription) &&
                Objects.equals(resources, other.resources) &&
                Objects.equals(accessTypes, other.accessTypes) &&
                Objects.equals(parameters, other.parameters) &&
                Objects.equals(options, other.options) &&
                Objects.equals(id, other.id) &&
                Objects.equals(order, other.order);
    }

    @Override
    public String toString() {
        String str = "XXConstraintDef={";

        str += super.toString();
        str += " [id=" + id + ", defId=" + defId + ", itemId=" + itemId
                + ", name=" + name + ", label=" + label
                + ", description=" + description + ", rbKeyLabel=" + rbKeyLabel
                + ", rbKeyDescription=" + rbKeyDescription
                + ", resources=" + resources + ", accessTypes=" + accessTypes
                + ", parameters=" + parameters + ", options=" + options
                + ", order=" + order + "]";
        str += "}";

        return str;
    }

    public Long getDefid() {
        return this.defId;
    }

    public void setDefid(Long defId) {
        this.defId = defId;
    }

    public Long getItemId() {
        return this.itemId;
    }

    public void setItemId(Long itemId) {
        this.itemId = itemId;
    }

    public String getName() {
        return this.name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getLabel() {
        return this.label;
    }

    public void setLabel(String label) {
        this.label = label;
    }

    public String getDescription() {
        return this.description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getRbkeylabel() {
        return this.rbKeyLabel;
    }

    public void setRbkeylabel(String rbKeyLabel) {
        this.rbKeyLabel = rbKeyLabel;
    }

    public String getRbkeydescription() {
        return this.rbKeyDescription;
    }

    public void setRbkeydescription(String rbKeyDescription) {
        this.rbKeyDescription = rbKeyDescription;
    }

    public String getResources() {
        return this.resources;
    }

    public void setResources(String resources) {
        this.resources = resources;
    }

    public String getAccessTypes() {
        return this.accessTypes;
    }

    public void setAccessTypes(String accessTypes) {
        this.accessTypes = accessTypes;
    }

    public String getParameters() {
        return this.parameters;
    }

    public void setParameters(String parameters) {
        this.parameters = parameters;
    }

    public String getOptions() {
        return this.options;
    }

    public void setOptions(String options) {
        this.options = options;
    }

    public Integer getOrder() {
        return this.order;
    }

    public void setOrder(Integer order) {
        this.order = order;
    }
}
