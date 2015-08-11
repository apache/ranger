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

import java.io.Serializable;

import javax.persistence.Cacheable;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.SequenceGenerator;
import javax.persistence.Table;
import javax.xml.bind.annotation.XmlRootElement;

import org.apache.ranger.common.AppConstants;

@Entity
@Cacheable
@Table(name="x_tagged_resource_value_map")
@XmlRootElement
public class XXTaggedResourceValueMap extends XXDBBase implements Serializable {
	private static final long serialVersionUID = 1L;

	@Id
	@SequenceGenerator(name = "XXTaggedResourceValueMapSEQ", sequenceName = "XXTaggedResourceValueMapSEQ", allocationSize = 1)
	@GeneratedValue(strategy = GenerationType.AUTO, generator = "XXTaggedResourceValueMapSEQ")
	@Column(name = "id")
	protected Long id;

	@Column(name = "guid", unique = true, nullable = false, length = 512)
	protected String guid;

	@Column(name = "res_value_id")
	protected Long resValueId;

	@Column(name = "value")
	protected String value;

	@Column(name = "sort_order")
	protected Integer sortOrder;

	@Override
	public void setId(Long id) {
		this.id = id;
	}

	@Override
	public Long getId() {
		return id;
	}

	/**
	 * @return the guid
	 */
	public String getGuid() {
		return guid;
	}

	/**
	 * @param guid
	 *            the guid to set
	 */
	public void setGuid(String guid) {
		this.guid = guid;
	}

	/**
	 * @return the resValueId
	 */
	public Long getResValueId() {
		return resValueId;
	}

	/**
	 * @param resValueId
	 *            the resValueId to set
	 */
	public void setResValueId(Long resValueId) {
		this.resValueId = resValueId;
	}

	/**
	 * @return the value
	 */
	public String getValue() {
		return value;
	}

	/**
	 * @param value
	 *            the value to set
	 */
	public void setValue(String value) {
		this.value = value;
	}

	/**
	 * @return the sortOrder
	 */
	public Integer getSortOrder() {
		return sortOrder;
	}

	/**
	 * @param sortOrder
	 *            the sortOrder to set
	 */
	public void setSortOrder(Integer sortOrder) {
		this.sortOrder = sortOrder;
	}

	@Override
	public int getMyClassType() {
		return AppConstants.CLASS_TYPE_XA_TAGGED_RESOURCE_VALUE_MAP;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((guid == null) ? 0 : guid.hashCode());
		result = prime * result + ((id == null) ? 0 : id.hashCode());
		result = prime * result + ((resValueId == null) ? 0 : resValueId.hashCode());
		result = prime * result + ((sortOrder == null) ? 0 : sortOrder.hashCode());
		result = prime * result + ((value == null) ? 0 : value.hashCode());
		return result;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (!super.equals(obj))
			return false;
		if (getClass() != obj.getClass())
			return false;
		XXTaggedResourceValueMap other = (XXTaggedResourceValueMap) obj;
		if (guid == null) {
			if (other.guid != null)
				return false;
		} else if (!guid.equals(other.guid))
			return false;
		if (id == null) {
			if (other.id != null)
				return false;
		} else if (!id.equals(other.id))
			return false;
		if (resValueId == null) {
			if (other.resValueId != null)
				return false;
		} else if (!resValueId.equals(other.resValueId))
			return false;
		if (sortOrder == null) {
			if (other.sortOrder != null)
				return false;
		} else if (!sortOrder.equals(other.sortOrder))
			return false;
		if (value == null) {
			if (other.value != null)
				return false;
		} else if (!value.equals(other.value))
			return false;
		return true;
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
		sb.append("resValueId={").append(resValueId).append("} ");
		sb.append("value={").append(value).append("} ");
		sb.append("sortOrder={").append(sortOrder).append("} ");
		sb.append(" }");

		return sb;
	}

}
