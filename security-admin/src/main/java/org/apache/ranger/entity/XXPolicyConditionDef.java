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
@Table(name = "x_policy_condition_def")
public class XXPolicyConditionDef extends XXDBBase implements java.io.Serializable {
    private static final long serialVersionUID = 1L;

    /**
     * id of the XXPolicyConditionDef
     * <ul>
     * </ul>
     */
    @Id
    @SequenceGenerator(name = "x_policy_condition_def_SEQ", sequenceName = "x_policy_condition_def_SEQ", allocationSize = 1)
    @GeneratedValue(strategy = GenerationType.AUTO, generator = "x_policy_condition_def_SEQ")
    @Column(name = "id")
    protected Long id;

    /**
     * defId of the XXPolicyConditionDef
     * <ul>
     * </ul>
     */
    @Column(name = "def_id")
    protected Long defId;

    /**
     * itemId of the XXPolicyConditionDef
     * <ul>
     * </ul>
     */
    @Column(name = "item_id")
    protected Long itemId;

    /**
     * name of the XXPolicyConditionDef
     * <ul>
     * </ul>
     */
    @Column(name = "name")
    protected String name;

    /**
     * evaluator of the XXPolicyConditionDef
     * <ul>
     * </ul>
     */
    @Column(name = "evaluator")
    protected String evaluator;

    /**
     * evaluatorOptions of the XXPolicyConditionDef
     * <ul>
     * </ul>
     */
    @Column(name = "evaluator_options")
    protected String evaluatorOptions;

    /**
     * validationRegEx of the XXPolicyConditionDef
     * <ul>
     * </ul>
     */
    @Column(name = "validation_reg_ex")
    protected String validationRegEx;

    /**
     * validationMessage of the XXPolicyConditionDef
     * <ul>
     * </ul>
     */
    @Column(name = "validation_message")
    protected String validationMessage;

    /**
     * uiHint of the XXPolicyConditionDef
     * <ul>
     * </ul>
     */
    @Column(name = "ui_hint")
    protected String uiHint;

    /**
     * label of the XXPolicyConditionDef
     * <ul>
     * </ul>
     */
    @Column(name = "label")
    protected String label;

    /**
     * description of the XXPolicyConditionDef
     * <ul>
     * </ul>
     */
    @Column(name = "description")
    protected String description;

    /**
     * rbKeyLabel of the XXPolicyConditionDef
     * <ul>
     * </ul>
     */
    @Column(name = "rb_key_label")
    protected String rbKeyLabel;

    /**
     * rbKeyDescription of the XXPolicyConditionDef
     * <ul>
     * </ul>
     */
    @Column(name = "rb_key_description")
    protected String rbKeyDescription;

    /**
     * rbKeyValidationMessage of the XXPolicyConditionDef
     * <ul>
     * </ul>
     */
    @Column(name = "rb_key_validation_message")
    protected String rbKeyValidationMessage;

    /**
     * order of the XXPolicyConditionDef
     * <ul>
     * </ul>
     */
    @Column(name = "sort_order")
    protected Integer order;

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

        XXPolicyConditionDef other = (XXPolicyConditionDef) obj;

        return Objects.equals(defId, other.defId) &&
                Objects.equals(itemId, other.itemId) &&
                Objects.equals(description, other.description) &&
                Objects.equals(evaluator, other.evaluator) &&
                Objects.equals(evaluatorOptions, other.evaluatorOptions) &&
                Objects.equals(id, other.id) &&
                Objects.equals(label, other.label) &&
                Objects.equals(name, other.name) &&
                Objects.equals(order, other.order) &&
                Objects.equals(rbKeyDescription, other.rbKeyDescription) &&
                Objects.equals(rbKeyLabel, other.rbKeyLabel) &&
                Objects.equals(validationRegEx, other.validationRegEx) &&
                Objects.equals(validationMessage, other.validationMessage) &&
                Objects.equals(uiHint, other.uiHint) &&
                Objects.equals(rbKeyValidationMessage, other.rbKeyValidationMessage);
    }

    /*
     * (non-Javadoc)
     *
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        String str = "XXPolicyConditionDef={";
        str += super.toString();
        str += " [id=" + id + ", defId=" + defId + ", itemId=" + itemId
                + ", name=" + name + ", evaluator=" + evaluator
                + ", evaluatorOptions=" + evaluatorOptions + ", label=" + label
                + ", validationRegEx=" + validationRegEx
                + ", validationMessage=" + validationMessage
                + ", uiHint=" + uiHint
                + ", description=" + description + ", rbKeyLabel=" + rbKeyLabel
                + ", rbKeyValidationMessage=" + rbKeyValidationMessage
                + ", rbKeyDescription=" + rbKeyDescription + ", order=" + order
                + "]";
        str += "}";
        return str;
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
     * Returns the value for the member attribute <b>evaluator</b>
     *
     * @return Date - value of member attribute <b>evaluator</b> .
     */
    public String getEvaluator() {
        return this.evaluator;
    }

    /**
     * This method sets the value to the member attribute <b> evaluator</b> .
     * You cannot set null to the attribute.
     *
     * @param evaluator Value to set member attribute <b> evaluator</b>
     */
    public void setEvaluator(String evaluator) {
        this.evaluator = evaluator;
    }

    /**
     * Returns the value for the member attribute <b>evaluatorOptions</b>
     *
     * @return Date - value of member attribute <b>evaluatorOptions</b> .
     */
    public String getEvaluatoroptions() {
        return this.evaluatorOptions;
    }

    /**
     * This method sets the value to the member attribute <b>
     * evaluatorOptions</b> . You cannot set null to the attribute.
     *
     * @param evaluatorOptions Value to set member attribute <b> evaluatorOptions</b>
     */
    public void setEvaluatoroptions(String evaluatorOptions) {
        this.evaluatorOptions = evaluatorOptions;
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
}
