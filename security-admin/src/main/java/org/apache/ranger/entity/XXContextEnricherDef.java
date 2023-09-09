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

@Entity
@Cacheable
@Table(name = "x_context_enricher_def")
public class XXContextEnricherDef extends XXDBBase implements
		java.io.Serializable {
	private static final long serialVersionUID = 1L;
	/**
	 * id of the XXContextEnricherDef
	 * <ul>
	 * </ul>
	 *
	 */
	@Id
	@SequenceGenerator(name = "x_context_enricher_def_SEQ", sequenceName = "x_context_enricher_def_SEQ", allocationSize = 1)
	@GeneratedValue(strategy = GenerationType.AUTO, generator = "x_context_enricher_def_SEQ")
	@Column(name = "id")
	protected Long id;

	/**
	 * defId of the XXContextEnricherDef
	 * <ul>
	 * </ul>
	 *
	 */
	@Column(name = "def_id")
	protected Long defId;

	/**
	 * itemId of the XXContextEnricherDef
	 * <ul>
	 * </ul>
	 *
	 */
	@Column(name = "item_id")
	protected Long itemId;

	/**
	 * name of the XXContextEnricherDef
	 * <ul>
	 * </ul>
	 *
	 */
	@Column(name = "name")
	protected String name;

	/**
	 * enricher of the XXContextEnricherDef
	 * <ul>
	 * </ul>
	 *
	 */
	@Column(name = "enricher")
	protected String enricher;

	/**
	 * enricherOptions of the XXContextEnricherDef
	 * <ul>
	 * </ul>
	 *
	 */
	@Column(name = "enricher_options")
	protected String enricherOptions;

	/**
	 * order of the XXContextEnricherDef
	 * <ul>
	 * </ul>
	 *
	 */
	@Column(name = "sort_order")
	protected Integer order;

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
	 * Returns the value for the member attribute <b>itemId</b>
	 *
	 * @return Long - value of member attribute <b>itemId</b> .
	 */
	public Long getItemId() {
		return this.itemId;
	}

	/**
	 * This method sets the value to the member attribute <b> defId</b> . You
	 * cannot set null to the attribute.
	 *
	 * @param defId
	 *            Value to set member attribute <b> defId</b>
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
	 * This method sets the value to the member attribute <b> enricher</b> .
	 * You cannot set null to the attribute.
	 *
	 * @param enricher
	 *            Value to set member attribute <b> enricher</b>
	 */
	public void setEnricher(String enricher) {
		this.enricher = enricher;
	}

	/**
	 * Returns the value for the member attribute <b>enricher</b>
	 *
	 * @return String - value of member attribute <b>enricher</b> .
	 */
	public String getEnricher() {
		return this.enricher;
	}

	/**
	 * This method sets the value to the member attribute <b>
	 * enricherOptions</b> . You cannot set null to the attribute.
	 *
	 * @param enricherOptions
	 *            Value to set member attribute <b> enricherOptions</b>
	 */
	public void setEnricherOptions(String enricherOptions) {
		this.enricherOptions = enricherOptions;
	}

	/**
	 * Returns the value for the member attribute <b>evaluatorOptions</b>
	 *
	 * @return Date - value of member attribute <b>evaluatorOptions</b> .
	 */
	public String getEnricherOptions() {
		return this.enricherOptions;
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
	 * @return Integer - value of member attribute <b>order</b> .
	 */
	public Integer getOrder() {
		return this.order;
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object obj) {
		if (!super.equals(obj)) {
			return false;
		}
		if (this == obj) {
			return true;
		}
		if (!super.equals(obj)) {
			return false;
		}
		if (getClass() != obj.getClass()) {
			return false;
		}
		XXContextEnricherDef other = (XXContextEnricherDef) obj;
		if (defId == null) {
			if (other.defId != null) {
				return false;
			}
		} else if (!defId.equals(other.defId)) {
			return false;
		}
		if (itemId == null) {
			if (other.itemId != null) {
				return false;
			}
		} else if (!itemId.equals(other.itemId)) {
			return false;
		}
		if (enricher == null) {
			if (other.enricher != null) {
				return false;
			}
		} else if (!enricher.equals(other.enricher)) {
			return false;
		}
		if (enricherOptions == null) {
			if (other.enricherOptions != null) {
				return false;
			}
		} else if (!enricherOptions.equals(other.enricherOptions)) {
			return false;
		}
		if (id == null) {
			if (other.id != null) {
				return false;
			}
		} else if (!id.equals(other.id)) {
			return false;
		}
		if (name == null) {
			if (other.name != null) {
				return false;
			}
		} else if (!name.equals(other.name)) {
			return false;
		}
		if (order == null) {
			if (other.order != null) {
				return false;
			}
		} else if (!order.equals(other.order)) {
			return false;
		}
		return true;
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		String str = "XXContextEnricherDef={";
		str += super.toString();
		str+=" [id=" + id + ", defId=" + defId + ", itemId=" + itemId
				+ ", name=" + name + ", enricher=" + enricherOptions
				+ ", enricherOptions=" + enricherOptions + ", order=" + order
				+ "]";
		str += "}";
		return str;
	}

}
