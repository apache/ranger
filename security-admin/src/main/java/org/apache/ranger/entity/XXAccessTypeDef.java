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

import javax.persistence.*;
import java.util.Objects;

@Entity
@Cacheable
@Table(name = "x_access_type_def")
public class XXAccessTypeDef extends XXDBBase implements java.io.Serializable {
	private static final long serialVersionUID = 1L;
	/**
	 * id of the XXAccessTypeDef
	 * <ul>
	 * </ul>
	 *
	 */
	@Id
	@SequenceGenerator(name = "x_access_type_def_SEQ", sequenceName = "x_access_type_def_SEQ", allocationSize = 1)
	@GeneratedValue(strategy = GenerationType.AUTO, generator = "x_access_type_def_SEQ")
	@Column(name = "id")
	protected Long id;

	/**
	 * defId of the XXAccessTypeDef
	 * <ul>
	 * </ul>
	 *
	 */
	@Column(name = "def_id")
	protected Long defId;

	/**
	 * itemId of the XXAccessTypeDef
	 * <ul>
	 * </ul>
	 *
	 */
	@Column(name = "item_id")
	protected Long itemId;

	/**
	 * name of the XXAccessTypeDef
	 * <ul>
	 * </ul>
	 *
	 */
	@Column(name = "name")
	protected String name;

	/**
	 * label of the XXAccessTypeDef
	 * <ul>
	 * </ul>
	 *
	 */
	@Column(name = "label")
	protected String label;

	/**
	 * rbKeyLabel of the XXAccessTypeDef
	 * <ul>
	 * </ul>
	 *
	 */
	@Column(name = "rb_key_label")
	protected String rbKeyLabel;

	/**
	 * category of the XXAccessTypeDef
	 */
	@Column(name = "category")
	protected Short category;

	/**
	 * order of the XXAccessTypeDef
	 * <ul>
	 * </ul>
	 *
	 */
	@Column(name = "sort_order")
	protected Integer order;

	/**
	 * dataMaskOptions of the XXAccessTypeDef
	 * <ul>
	 * </ul>
	 *
	 */
	@Column(name = "datamask_options")
	protected String dataMaskOptions;

	/**
	 * rowFilterOptions of the XXAccessTypeDef
	 * <ul>
	 * </ul>
	 *
	 */
	@Column(name = "rowfilter_options")
	protected String rowFilterOptions;

	/**
	 * This method sets the value to the member attribute <b> id</b> . You
	 * cannot set null to the attribute.
	 *
	 * @param id
	 *            Value to set member attribute <b> id</b>
	 */
	public void setId(Long id) {
		this.id = id;
	}

	/**
	 * Returns the value for the member attribute <b>id</b>
	 *
	 * @return Date - value of member attribute <b>id</b> .
	 */
	public Long getId() {
		return this.id;
	}

	/**
	 * This method sets the value to the member attribute <b> defId</b> . You
	 * cannot set null to the attribute.
	 *
	 * @param defId
	 *            Value to set member attribute <b> defId</b>
	 */
	public void setDefid(Long defId) {
		this.defId = defId;
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
	 * This method sets the value to the member attribute <b> itemId</b> . You
	 * cannot set null to the attribute.
	 *
	 * @param itemId
	 *            Value to set member attribute <b> itemId</b>
	 */
	public void setItemId(Long itemId) {
		this.itemId = itemId;
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
	 * This method sets the value to the member attribute <b> name</b> . You
	 * cannot set null to the attribute.
	 *
	 * @param name
	 *            Value to set member attribute <b> name</b>
	 */
	public void setName(String name) {
		this.name = name;
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
	 * This method sets the value to the member attribute <b> label</b> . You
	 * cannot set null to the attribute.
	 *
	 * @param label
	 *            Value to set member attribute <b> label</b>
	 */
	public void setLabel(String label) {
		this.label = label;
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
	 * This method sets the value to the member attribute <b> rbKeyLabel</b> .
	 * You cannot set null to the attribute.
	 *
	 * @param rbKeyLabel
	 *            Value to set member attribute <b> rbKeyLabel</b>
	 */
	public void setRbkeylabel(String rbKeyLabel) {
		this.rbKeyLabel = rbKeyLabel;
	}

	/**
	 * Returns the value for the member attribute <b>rbKeyLabel</b>
	 *
	 * @return Date - value of member attribute <b>rbKeyLabel</b> .
	 */
	public String getRbkeylabel() {
		return this.rbKeyLabel;
	}

	public Short getCategory() {
		return category;
	}

	public void setCategory(Short category) {
		this.category = category;
	}

	/**
	 * This method sets the value to the member attribute <b> order</b> . You
	 * cannot set null to the attribute.
	 *
	 * @param order
	 *            Value to set member attribute <b> order</b>
	 */
	public void setOrder(Integer order) {
		this.order = order;
	}

	/**
	 * Returns the value for the member attribute <b>order</b>
	 *
	 * @return Date - value of member attribute <b>order</b> .
	 */
	public Integer getOrder() {
		return this.order;
	}

	public String getDataMaskOptions() {
		return dataMaskOptions;
	}

	public void setDataMaskOptions(String dataMaskOptions) {
		this.dataMaskOptions = dataMaskOptions;
	}

	public String getRowFilterOptions() { return rowFilterOptions; }

	public void setRowFilterOptions(String rowFilterOptions) { this.rowFilterOptions = rowFilterOptions; }

	/*
	 * (non-Javadoc)
	 *
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		} else if (getClass() != obj.getClass()) {
			return false;
		} else if (!super.equals(obj)) {
			return false;
		}

		XXAccessTypeDef other = (XXAccessTypeDef) obj;

		return Objects.equals(defId, other.defId) &&
		       Objects.equals(itemId, other.itemId) &&
		       Objects.equals(id, other.id) &&
		       Objects.equals(name, other.name) &&
		       Objects.equals(label, other.label) &&
		       Objects.equals(rbKeyLabel, other.rbKeyLabel) &&
		       Objects.equals(category, other.category) &&
		       Objects.equals(dataMaskOptions, other.dataMaskOptions) &&
		       Objects.equals(rowFilterOptions, other.rowFilterOptions) &&
		       Objects.equals(order, other.order);
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "XXAccessTypeDef [" + super.toString() + " id=" + id
				+ ", defId=" + defId + ", itemId=" + itemId + ", name=" + name + ", label=" + label
				+ ", rbKeyLabel=" + rbKeyLabel + ", category=" + category + ", dataMaskOptions=" + dataMaskOptions
				+ ", rowFilterOptions=" + rowFilterOptions + ", order=" + order + "]";
	}

}
