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
import javax.xml.bind.annotation.XmlRootElement;

@Entity
@Cacheable
@XmlRootElement
@Table(name = "x_policy")
public class XXPolicy extends XXDBBase implements java.io.Serializable {
	private static final long serialVersionUID = 1L;
	/**
	 * id of the XXPolicy
	 * <ul>
	 * </ul>
	 *
	 */
	@Id
	@SequenceGenerator(name = "x_policy_SEQ", sequenceName = "x_policy_SEQ", allocationSize = 1)
	@GeneratedValue(strategy = GenerationType.AUTO, generator = "x_policy_SEQ")
	@Column(name = "id")
	protected Long id;

	/**
	 * Global Id for the object
	 * <ul>
	 * <li>The maximum length for this attribute is <b>512</b>.
	 * </ul>
	 *
	 */
	@Column(name = "guid", unique = true, nullable = false, length = 512)
	protected String guid;
	
	/**
	 * version of the XXPolicy
	 * <ul>
	 * </ul>
	 *
	 */
	@Column(name = "version")
	protected Long version;

	/**
	 * service of the XXPolicy
	 * <ul>
	 * </ul>
	 *
	 */
	@Column(name = "service")
	protected Long service;

	/**
	 * name of the XXPolicy
	 * <ul>
	 * </ul>
	 *
	 */
	@Column(name = "name")
	protected String name;

	/**
	 * description of the XXPolicy
	 * <ul>
	 * </ul>
	 *
	 */
	@Column(name = "description")
	protected String description;

	/**
	 * isEnabled of the XXPolicy
	 * <ul>
	 * </ul>
	 *
	 */
	@Column(name = "is_enabled")
	protected boolean isEnabled;

	/**
	 * isAuditEnabled of the XXPolicy
	 * <ul>
	 * </ul>
	 *
	 */
	@Column(name = "is_audit_enabled")
	protected boolean isAuditEnabled;

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
	 * @return the gUID
	 */
	public String getGuid() {
		return guid;
	}

	/**
	 * @param gUID
	 *            the gUID to set
	 */
	public void setGuid(String gUID) {
		guid = gUID;
	}

	/**
	 * This method sets the value to the member attribute <b> version</b> . You
	 * cannot set null to the attribute.
	 * 
	 * @param version
	 *            Value to set member attribute <b> version</b>
	 */
	public void setVersion(Long version) {
		this.version = version;
	}

	/**
	 * Returns the value for the member attribute <b>version</b>
	 * 
	 * @return Date - value of member attribute <b>version</b> .
	 */
	public Long getVersion() {
		return this.version;
	}

	/**
	 * This method sets the value to the member attribute <b> service</b> . You
	 * cannot set null to the attribute.
	 * 
	 * @param service
	 *            Value to set member attribute <b> service</b>
	 */
	public void setService(Long service) {
		this.service = service;
	}

	/**
	 * Returns the value for the member attribute <b>service</b>
	 * 
	 * @return Date - value of member attribute <b>service</b> .
	 */
	public Long getService() {
		return this.service;
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
	 * This method sets the value to the member attribute <b> description</b> .
	 * You cannot set null to the attribute.
	 * 
	 * @param description
	 *            Value to set member attribute <b> description</b>
	 */
	public void setDescription(String description) {
		this.description = description;
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
	 * This method sets the value to the member attribute <b> isEnabled</b> .
	 * You cannot set null to the attribute.
	 * 
	 * @param isEnabled
	 *            Value to set member attribute <b> isEnabled</b>
	 */
	public void setIsEnabled(boolean isEnabled) {
		this.isEnabled = isEnabled;
	}

	/**
	 * Returns the value for the member attribute <b>isEnabled</b>
	 * 
	 * @return Date - value of member attribute <b>isEnabled</b> .
	 */
	public boolean getIsEnabled() {
		return this.isEnabled;
	}

	/**
	 * This method sets the value to the member attribute <b> isAuditEnabled</b>
	 * . You cannot set null to the attribute.
	 * 
	 * @param isAuditEnabled
	 *            Value to set member attribute <b> isAuditEnabled</b>
	 */
	public void setIsAuditEnabled(boolean isAuditEnabled) {
		this.isAuditEnabled = isAuditEnabled;
	}

	/**
	 * Returns the value for the member attribute <b>isAuditEnabled</b>
	 * 
	 * @return Date - value of member attribute <b>isAuditEnabled</b> .
	 */
	public boolean getIsAuditEnabled() {
		return this.isAuditEnabled;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object obj) {
		if(!super.equals(obj)) {
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
		XXPolicy other = (XXPolicy) obj;
		if (guid == null) {
			if (other.guid != null) {
				return false;
			}
		} else if (!guid.equals(other.guid)) {
			return false;
		}
		if (description == null) {
			if (other.description != null) {
				return false;
			}
		} else if (!description.equals(other.description)) {
			return false;
		}
		if (id == null) {
			if (other.id != null) {
				return false;
			}
		} else if (!id.equals(other.id)) {
			return false;
		}
		if (isAuditEnabled != other.isAuditEnabled) {
			return false;
		}
		if (isEnabled != other.isEnabled) {
			return false;
		}
		if (name == null) {
			if (other.name != null) {
				return false;
			}
		} else if (!name.equals(other.name)) {
			return false;
		}
		if (service == null) {
			if (other.service != null) {
				return false;
			}
		} else if (!service.equals(other.service)) {
			return false;
		}
		if (version == null) {
			if (other.version != null) {
				return false;
			}
		} else if (!version.equals(other.version)) {
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
		return "XXPolicy [" + super.toString() + " id=" + id + ", guid=" + guid
				+ ", version=" + version + ", service=" + service + ", name="
				+ name + ", description=" + description + ", isEnabled="
				+ isEnabled + ", isAuditEnabled=" + isAuditEnabled + "]";
	}

}