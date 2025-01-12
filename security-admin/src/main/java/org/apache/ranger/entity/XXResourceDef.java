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
@Table(name = "x_resource_def")
public class XXResourceDef extends XXDBBase implements java.io.Serializable {
    private static final long serialVersionUID = 1L;

    /**
     * id of the XXResourceDef
     * <ul>
     * </ul>
     */
    @Id
    @SequenceGenerator(name = "x_resource_def_SEQ", sequenceName = "x_resource_def_SEQ", allocationSize = 1)
    @GeneratedValue(strategy = GenerationType.AUTO, generator = "x_resource_def_SEQ")
    @Column(name = "id")
    protected Long id;

    /**
     * defId of the XXResourceDef
     * <ul>
     * </ul>
     */
    @Column(name = "def_id")
    protected Long defId;

    /**
     * itemId of the XXResourceDef
     * <ul>
     * </ul>
     */
    @Column(name = "item_id")
    protected Long itemId;

    /**
     * name of the XXResourceDef
     * <ul>
     * </ul>
     */
    @Column(name = "name")
    protected String name;

    /**
     * type of the XXResourceDef
     * <ul>
     * </ul>
     */
    @Column(name = "type")
    protected String type;

    /**
     * level of the XXResourceDef
     * <ul>
     * </ul>
     */
    @Column(name = "res_level")
    protected Integer level;

    /**
     * parent of the XXResourceDef
     * <ul>
     * </ul>
     */
    @Column(name = "parent")
    protected Long parent;

    /**
     * mandatory of the XXResourceDef
     * <ul>
     * </ul>
     */
    @Column(name = "mandatory")
    protected boolean mandatory;

    /**
     * lookUpSupported of the XXResourceDef
     * <ul>
     * </ul>
     */
    @Column(name = "look_up_supported")
    protected boolean lookUpSupported;

    /**
     * recursiveSupported of the XXResourceDef
     * <ul>
     * </ul>
     */
    @Column(name = "recursive_supported")
    protected boolean recursiveSupported;

    /**
     * excludesSupported of the XXResourceDef
     * <ul>
     * </ul>
     */
    @Column(name = "excludes_supported")
    protected boolean excludesSupported;

    /**
     * matcher of the XXResourceDef
     * <ul>
     * </ul>
     */
    @Column(name = "matcher")
    protected String matcher;

    /**
     * matcherOptions of the XXResourceDef
     * <ul>
     * </ul>
     */
    @Column(name = "matcher_options")
    protected String matcherOptions;

    /**
     * validationRegEx of the XXResourceDef
     * <ul>
     * </ul>
     */
    @Column(name = "validation_reg_ex")
    protected String validationRegEx;

    /**
     * validationMessage of the XXResourceDef
     * <ul>
     * </ul>
     */
    @Column(name = "validation_message")
    protected String validationMessage;

    /**
     * uiHint of the XXResourceDef
     * <ul>
     * </ul>
     */
    @Column(name = "ui_hint")
    protected String uiHint;

    /**
     * label of the XXResourceDef
     * <ul>
     * </ul>
     */
    @Column(name = "label")
    protected String label;

    /**
     * description of the XXResourceDef
     * <ul>
     * </ul>
     */
    @Column(name = "description")
    protected String description;

    /**
     * rbKeyLabel of the XXResourceDef
     * <ul>
     * </ul>
     */
    @Column(name = "rb_key_label")
    protected String rbKeyLabel;

    /**
     * rbKeyDescription of the XXResourceDef
     * <ul>
     * </ul>
     */
    @Column(name = "rb_key_description")
    protected String rbKeyDescription;

    /**
     * rbKeyValidationMessage of the XXResourceDef
     * <ul>
     * </ul>
     */
    @Column(name = "rb_key_validation_message")
    protected String rbKeyValidationMessage;

    /**
     * order of the XXResourceDef
     * <ul>
     * </ul>
     */
    @Column(name = "sort_order")
    protected Integer order;

    /**
     * dataMaskOptions of the XXResourceDef
     * <ul>
     * </ul>
     */
    @Column(name = "datamask_options")
    protected String dataMaskOptions;

    /**
     * rowFilterOptions of the XXAccessTypeDef
     * <ul>
     * </ul>
     */
    @Column(name = "rowfilter_options")
    protected String rowFilterOptions;

    /**
     * Returns the value for the member attribute <b>id</b>
     *
     * @return Date - value of member attribute <b>id</b> .
     */
    public Long getId() {
        return this.id;
    }

    /**
     * This method sets the value to the member attribute <b> id</b> . You
     * cannot set null to the attribute.
     *
     * @param id Value to set member attribute <b> id</b>
     */
    public void setId(Long id) {
        this.id = id;
    }

    @Override
    public int hashCode() {
        return super.hashCode();
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

        XXResourceDef other = (XXResourceDef) obj;

        return Objects.equals(defId, other.defId) &&
                Objects.equals(itemId, other.itemId) &&
                Objects.equals(description, other.description) &&
                Objects.equals(excludesSupported, other.excludesSupported) &&
                Objects.equals(id, other.id) &&
                Objects.equals(validationRegEx, other.validationRegEx) &&
                Objects.equals(validationMessage, other.validationMessage) &&
                Objects.equals(uiHint, other.uiHint) &&
                Objects.equals(rbKeyValidationMessage, other.rbKeyValidationMessage) &&
                Objects.equals(label, other.label) &&
                Objects.equals(lookUpSupported, other.lookUpSupported) &&
                Objects.equals(mandatory, other.mandatory) &&
                Objects.equals(matcher, other.matcher) &&
                Objects.equals(matcherOptions, other.matcherOptions) &&
                Objects.equals(name, other.name) &&
                Objects.equals(order, other.order) &&
                Objects.equals(parent, other.parent) &&
                Objects.equals(rbKeyDescription, other.rbKeyDescription) &&
                Objects.equals(rbKeyLabel, other.rbKeyLabel) &&
                Objects.equals(recursiveSupported, other.recursiveSupported) &&
                Objects.equals(type, other.type) &&
                Objects.equals(dataMaskOptions, other.dataMaskOptions) &&
                Objects.equals(rowFilterOptions, other.rowFilterOptions);
    }

    /*
     * (non-Javadoc)
     *
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return "XXResourceDef [" + super.toString() + " id=" + id + ", defId="
                + defId + ", itemId=" + itemId + ", name=" + name + ", type=" + type + ", level="
                + level + ", parent=" + parent + ", mandatory=" + mandatory
                + ", lookUpSupported=" + lookUpSupported
                + ", recursiveSupported=" + recursiveSupported
                + ", excludesSupported=" + excludesSupported + ", matcher=" + matcher
                + ", matcherOptions=" + matcherOptions
                + ", validationRegEx=" + validationRegEx
                + ", validationMessage=" + validationMessage
                + ", uiHint=" + uiHint
                + ", label=" + label + ", description=" + description
                + ", rbKeyLabel=" + rbKeyLabel
                + ", rbKeyDescription=" + rbKeyDescription
                + ", rbKeyValidationMessage=" + rbKeyValidationMessage
                + ", order=" + order
                + ", dataMaskOptions=" + dataMaskOptions
                + ", rowFilterOptions=" + rowFilterOptions
                + "]";
    }

    /**
     * Returns the value for the member attribute <b>itemId</b>
     *
     * @return Long - value of member attribute <b>itemId</b> .
     */
    public Long getItemId() {
        return this.itemId;
    }

    /**
     * This method sets the value to the member attribute <b> itemId</b> . You
     * cannot set null to the attribute.
     *
     * @param itemId Value to set member attribute <b> itemId</b>
     */
    public void setItemId(Long itemId) {
        this.itemId = itemId;
    }

    /**
     * Returns the value for the member attribute <b>defId</b>
     *
     * @return Date - value of member attribute <b>defId</b> .
     */
    public Long getDefid() {
        return this.defId;
    }

    /**
     * This method sets the value to the member attribute <b> defId</b> . You
     * cannot set null to the attribute.
     *
     * @param defId Value to set member attribute <b> defId</b>
     */
    public void setDefid(Long defId) {
        this.defId = defId;
    }

    /**
     * Returns the value for the member attribute <b>name</b>
     *
     * @return Date - value of member attribute <b>name</b> .
     */
    public String getName() {
        return this.name;
    }

    /**
     * This method sets the value to the member attribute <b> name</b> . You
     * cannot set null to the attribute.
     *
     * @param name Value to set member attribute <b> name</b>
     */
    public void setName(String name) {
        this.name = name;
    }

    /**
     * Returns the value for the member attribute <b>type</b>
     *
     * @return Date - value of member attribute <b>type</b> .
     */
    public String getType() {
        return this.type;
    }

    /**
     * This method sets the value to the member attribute <b> type</b> . You
     * cannot set null to the attribute.
     *
     * @param type Value to set member attribute <b> type</b>
     */
    public void setType(String type) {
        this.type = type;
    }

    /**
     * Returns the value for the member attribute <b>level</b>
     *
     * @return Date - value of member attribute <b>level</b> .
     */
    public Integer getLevel() {
        return this.level;
    }

    /**
     * This method sets the value to the member attribute <b> level</b> . You
     * cannot set null to the attribute.
     *
     * @param level Value to set member attribute <b> level</b>
     */
    public void setLevel(Integer level) {
        this.level = level;
    }

    /**
     * Returns the value for the member attribute <b>parent</b>
     *
     * @return Date - value of member attribute <b>parent</b> .
     */
    public Long getParent() {
        return this.parent;
    }

    /**
     * This method sets the value to the member attribute <b> parent</b> . You
     * cannot set null to the attribute.
     *
     * @param parent Value to set member attribute <b> parent</b>
     */
    public void setParent(Long parent) {
        this.parent = parent;
    }

    /**
     * Returns the value for the member attribute <b>mandatory</b>
     *
     * @return Date - value of member attribute <b>mandatory</b> .
     */
    public boolean getMandatory() {
        return this.mandatory;
    }

    /**
     * This method sets the value to the member attribute <b> mandatory</b> .
     * You cannot set null to the attribute.
     *
     * @param mandatory Value to set member attribute <b> mandatory</b>
     */
    public void setMandatory(boolean mandatory) {
        this.mandatory = mandatory;
    }

    /**
     * Returns the value for the member attribute <b>lookUpSupported</b>
     *
     * @return Date - value of member attribute <b>lookUpSupported</b> .
     */
    public boolean getLookupsupported() {
        return this.lookUpSupported;
    }

    /**
     * This method sets the value to the member attribute <b>
     * lookUpSupported</b> . You cannot set null to the attribute.
     *
     * @param lookUpSupported Value to set member attribute <b> lookUpSupported</b>
     */
    public void setLookupsupported(boolean lookUpSupported) {
        this.lookUpSupported = lookUpSupported;
    }

    /**
     * Returns the value for the member attribute <b>recursiveSupported</b>
     *
     * @return Date - value of member attribute <b>recursiveSupported</b> .
     */
    public boolean getRecursivesupported() {
        return this.recursiveSupported;
    }

    /**
     * This method sets the value to the member attribute <b>
     * recursiveSupported</b> . You cannot set null to the attribute.
     *
     * @param recursiveSupported Value to set member attribute <b> recursiveSupported</b>
     */
    public void setRecursivesupported(boolean recursiveSupported) {
        this.recursiveSupported = recursiveSupported;
    }

    /**
     * Returns the value for the member attribute <b>excludesSupported</b>
     *
     * @return Date - value of member attribute <b>excludesSupported</b> .
     */
    public boolean getExcludessupported() {
        return this.excludesSupported;
    }

    /**
     * This method sets the value to the member attribute <b>
     * excludesSupported</b> . You cannot set null to the attribute.
     *
     * @param excludesSupported Value to set member attribute <b> excludesSupported</b>
     */
    public void setExcludessupported(boolean excludesSupported) {
        this.excludesSupported = excludesSupported;
    }

    /**
     * Returns the value for the member attribute <b>matcher</b>
     *
     * @return Date - value of member attribute <b>matcher</b> .
     */
    public String getMatcher() {
        return this.matcher;
    }

    /**
     * This method sets the value to the member attribute <b> matcher</b> . You
     * cannot set null to the attribute.
     *
     * @param matcher Value to set member attribute <b> matcher</b>
     */
    public void setMatcher(String matcher) {
        this.matcher = matcher;
    }

    /**
     * Returns the value for the member attribute <b>matcherOptions</b>
     *
     * @return Date - value of member attribute <b>matcherOptions</b> .
     */
    public String getMatcheroptions() {
        return this.matcherOptions;
    }

    /**
     * This method sets the value to the member attribute <b> matcherOptions</b>
     * . You cannot set null to the attribute.
     *
     * @param matcherOptions Value to set member attribute <b> matcherOptions</b>
     */
    public void setMatcheroptions(String matcherOptions) {
        this.matcherOptions = matcherOptions;
    }

    /**
     * @return the validationRegEx
     */
    public String getValidationRegEx() {
        return validationRegEx;
    }

    /**
     * @param validationRegEx the validationRegEx to set
     */
    public void setValidationRegEx(String validationRegEx) {
        this.validationRegEx = validationRegEx;
    }

    /**
     * @return the validationMessage
     */
    public String getValidationMessage() {
        return validationMessage;
    }

    /**
     * @param validationMessage the validationMessage to set
     */
    public void setValidationMessage(String validationMessage) {
        this.validationMessage = validationMessage;
    }

    /**
     * @return the uiHint
     */
    public String getUiHint() {
        return uiHint;
    }

    /**
     * @param uiHint the uiHint to set
     */
    public void setUiHint(String uiHint) {
        this.uiHint = uiHint;
    }

    /**
     * Returns the value for the member attribute <b>label</b>
     *
     * @return Date - value of member attribute <b>label</b> .
     */
    public String getLabel() {
        return this.label;
    }

    /**
     * This method sets the value to the member attribute <b> label</b> . You
     * cannot set null to the attribute.
     *
     * @param label Value to set member attribute <b> label</b>
     */
    public void setLabel(String label) {
        this.label = label;
    }

    /**
     * Returns the value for the member attribute <b>description</b>
     *
     * @return Date - value of member attribute <b>description</b> .
     */
    public String getDescription() {
        return this.description;
    }

    /**
     * This method sets the value to the member attribute <b> description</b> .
     * You cannot set null to the attribute.
     *
     * @param description Value to set member attribute <b> description</b>
     */
    public void setDescription(String description) {
        this.description = description;
    }

    /**
     * Returns the value for the member attribute <b>rbKeyLabel</b>
     *
     * @return Date - value of member attribute <b>rbKeyLabel</b> .
     */
    public String getRbkeylabel() {
        return this.rbKeyLabel;
    }

    /**
     * This method sets the value to the member attribute <b> rbKeyLabel</b> .
     * You cannot set null to the attribute.
     *
     * @param rbKeyLabel Value to set member attribute <b> rbKeyLabel</b>
     */
    public void setRbkeylabel(String rbKeyLabel) {
        this.rbKeyLabel = rbKeyLabel;
    }

    /**
     * Returns the value for the member attribute <b>rbKeyDescription</b>
     *
     * @return Date - value of member attribute <b>rbKeyDescription</b> .
     */
    public String getRbkeydescription() {
        return this.rbKeyDescription;
    }

    /**
     * This method sets the value to the member attribute <b>
     * rbKeyDescription</b> . You cannot set null to the attribute.
     *
     * @param rbKeyDescription Value to set member attribute <b> rbKeyDescription</b>
     */
    public void setRbkeydescription(String rbKeyDescription) {
        this.rbKeyDescription = rbKeyDescription;
    }

    /**
     * @return the rbKeyValidationMessage
     */
    public String getRbKeyValidationMessage() {
        return rbKeyValidationMessage;
    }

    /**
     * @param rbKeyValidationMessage the rbKeyValidationMessage to set
     */
    public void setRbKeyValidationMessage(String rbKeyValidationMessage) {
        this.rbKeyValidationMessage = rbKeyValidationMessage;
    }

    /**
     * Returns the value for the member attribute <b>order</b>
     *
     * @return Date - value of member attribute <b>order</b> .
     */
    public Integer getOrder() {
        return this.order;
    }

    /**
     * This method sets the value to the member attribute <b> order</b> . You
     * cannot set null to the attribute.
     *
     * @param order Value to set member attribute <b> order</b>
     */
    public void setOrder(Integer order) {
        this.order = order;
    }

    public String getDataMaskOptions() {
        return dataMaskOptions;
    }

    public void setDataMaskOptions(String dataMaskOptions) {
        this.dataMaskOptions = dataMaskOptions;
    }

    public String getRowFilterOptions() {
        return rowFilterOptions;
    }

    public void setRowFilterOptions(String rowFilterOptions) {
        this.rowFilterOptions = rowFilterOptions;
    }
}
